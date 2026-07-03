# Untreated issues & proposed `reflex_doctor()`

Status: **draft spec** — captured from a production incident on 2026-07-02 (extension
`1.10.7`, tenants `omc` / `nvg`, shared multi-tenant DB). Every item below is a real
failure mode observed or directly implied by that incident. None are yet fixed in the
extension; the workarounds column is what an operator must do today by hand.

The through-line: pg_reflex has strong *primitives* for partitioned-IMV maintenance
(`reflex_flush_partitions`, `reflex_sync_partitions`, `reflex_reconcile_partition`,
`reflex_rebuild_imv`, `reflex_audit`, `reflex_ivm_status`) but **no single operator
entrypoint that detects an inconsistent state and repairs it safely**. Recovery today
is a hand-assembled sequence of catalog queries + targeted calls, and several failure
modes are *silent* — the IMV is wrong and nothing surfaces it. `reflex_doctor()`
(spec at the end) is the proposed remedy.

---

## The incident, in one paragraph

`ALTER TABLE omc.sales_simulation ATTACH PARTITION …_p_473` (an archived demand plan,
1.69M base rows) left `omc.sop_forecast_view` and every downstream IMV empty for
dp 473. Investigation showed: the `__reflex_partition_pending` queue held two rows
(`omc.sales_simulation` from today, `nvg.sales_simulation` stuck since **2026-06-26**)
that the commit-time auto-drain never cleared, because per-root
`reflex_flush_partition_source` was failing on **shape drift** (an IMV mirror child
left a plain table while the source gained a sub-partition level). Draining manually
(`reflex_flush_partitions()`) healed `sop_forecast_view` (473 → 1.69M rows, exact
match to source). That reconcile then cascaded downstream and **aborted on a
`would overlap partition` error** caused by a `drop_orphans=false` orphan partition,
leaving `forecast_analysis_view` and `analysis_dp_year_agg` stale. Even after
rebuilding those, dp 473 stayed empty in `forecast_analysis_view` because it lists
`sop_forecast_view` in `ignore_sources` and 473 had been archived *while its source
partition was empty* — an unhealable-by-incremental residue that only an explicit
per-partition reconcile (or full chain rebuild) fixes. Every one of these steps was
manual, and each failure mode was silent until someone went looking.

---

## Findings

Severity: **S1** = silent data-correctness loss; **S2** = operational wedge / stall;
**S3** = usability / observability.

### F1 — Pending-queue re-arm hole: a failed flush permanently suppresses future flushes — S1

**Symptom.** After one `reflex_flush_partition_source(root)` failure, *every subsequent*
`ATTACH`/`DETACH`/`CREATE … PARTITION OF` on that root silently stops maintaining the
IMV — no error, no retry.

**Root cause.** `__reflex_on_ddl_command_end` enqueues the partition root with:

```sql
INSERT INTO public.__reflex_partition_pending (source_root)
VALUES (_part_root)
ON CONFLICT (source_root) DO NOTHING;
```

and the drain is an `AFTER INSERT` constraint trigger. When a prior flush failed, the
root's row is still present, so the next enqueue is a **no-op** — which means the
`AFTER INSERT` trigger **never fires**, so no new flush is attempted. The queue is a
dirty-set, but a stuck entry converts "dirty" into "permanently ignored."

**Evidence.** `nvg.sales_simulation` sat in `__reflex_partition_pending` for 6 days
across many attaches; its IMV never re-synced in that window.

**Impact.** This is the mechanism behind "data sometimes not updated after swap." It is
S1 because the IMV is wrong and nothing signals it.

**Proposed fix.**
- Change the enqueue to bump a retry/`enqueued_at` on conflict (`ON CONFLICT (source_root)
  DO UPDATE SET enqueued_at = statement_timestamp(), attempts = attempts + 1`) so the
  `AFTER INSERT`/`AFTER UPDATE` drain re-fires on every DDL even when a row already
  exists.
- Add an `attempts INT` and `last_error TEXT` column to `__reflex_partition_pending`;
  after N failed attempts, raise the failure to the configured
  `pg_reflex.alter_source_policy` (warn/error) instead of swallowing it.

### F2 — A stuck pending row is invisible: no age, no alert, no auto-retry — S2

**Symptom.** A wedged root is discoverable only by manually `SELECT * FROM
public.__reflex_partition_pending` and eyeballing `enqueued_at`.

**Root cause.** No surfacing. `reflex_ivm_status()` reports `last_error` per IMV but does
not join the pending-queue backlog, and nothing ages-out or retries a stuck row.

**Proposed fix.**
- Surface pending-queue depth + oldest `enqueued_at` per root in `reflex_ivm_status()`
  (or a new `reflex_partition_pending_status()`).
- A `reflex_scheduled_reconcile`-style background retry that re-attempts stuck roots
  with backoff and escalates after a threshold.

### F3 — `drop_orphans=false` orphans cause later `would overlap partition` swap aborts — S1

**Symptom.** A reconcile/sync fails with
`partition "…__reflex_swap_tgt_…" would overlap partition "…__cte_for_<hash>"`, aborts
the auto-sync, and leaves the IMV (and everything downstream) stale.

**Root cause.** The safe default `drop_orphans=false` never deletes an IMV partition when
its source partition is detached/rebuilt. The orphan lingers with live partition bounds;
the *next* reconcile builds a swap target with the same bounds and `ATTACH` collides.
Correctness-safe as a default (never deletes user data on a source `DETACH`) but it
converts a benign leftover into a hard blocker for the next maintenance cycle.

**Evidence.** `forecast_analysis_view_forecast_analysis_view__cte_for_38e6d6c1` was
reported `orphan target partition … preserved (drop_orphans=false)` on one flush, then
on the next reconcile blocked the swap with an overlap error.

**Proposed fix.**
- When a swap target's bounds exactly match an existing **orphan** (confirmed to have no
  live source partition), auto-adopt or auto-drop-and-refill it inside the same swap —
  the reconcile immediately repopulates, so no data is lost. This is a non-breaking heal
  analogous to the 1.10.0 shape-drift heal.
- Failing that, at minimum emit an actionable single-line error that names the exact
  `reflex_sync_partitions('<imv>', true)` call needed, and record the blocked IMV as
  known-stale (see F4).

### F4 — Cascade auto-sync failures are WARNING-only and leave no durable stale flag — S1

**Symptom.** When auto-sync of a downstream IMV fails mid-cascade, it raises a `WARNING`,
the outer transaction commits, and the IMV is silently stale. The only trace is transient
log output.

**Root cause.** `__reflex_on_ddl_command_end` wraps the per-IMV
`reflex_sync_partitions` in `EXCEPTION WHEN OTHERS THEN RAISE WARNING …`. Correct for
availability (one bad IMV shouldn't abort the DDL) but it drops the fact on the floor.

**Proposed fix.**
- Persist a durable `known_stale` flag + reason + timestamp per IMV (extend
  `__reflex_ivm_reference` or a companion health table), set on any caught cascade
  failure and cleared on a successful reconcile.
- Expose it in `reflex_ivm_status()` (`known_stale BOOLEAN`, `stale_reason TEXT`) so a
  single query answers "is anything wrong right now?"

### F4b — Reconcile primitives cannot self-heal a corrupted *decomposed* chain; only a CASCADE rebuild recovers — S1

**Symptom.** On a CTE-decomposed IMV (`forecast_analysis_view` → `__cte_forecast_bounds`
/ `__cte_date_limits` / `__cte_forecast_sales` / `__cte_history_sales` → top) whose
sub-IMVs accumulated structural damage during a wedge, bottom-up
`reflex_reconcile_partition` per sub-IMV **does not converge**:

- `__cte_forecast_bounds` (chain root) fails hard:
  `ERROR: reconcile_partition: missing intermediate bound for child
  '__reflex_intermediate_…_19286f5b'` — its own internal partition structure is
  corrupt, so nothing above it can fill.
- higher levels abort on `drop_orphans=false` overlap (F3) at *every* tier
  (`__cte_history_sales`, top view), each `WARNING`-swallowed (F4).

Result: after reconciling all five levels + the aggregate, the target is **still 0
rows** for the affected key.

**Root cause.** The reconcile/sync primitives assume a structurally-sound partition tree;
they repair *data*, not deep structural corruption (missing intermediate bounds, mutually
overlapping orphan targets across a decomposed chain). Once several tiers are damaged, no
sequence of per-partition reconciles digs out.

**Only reliable recovery.** Drop the whole chain and recreate from the registry:
`drop_reflex_ivm('<top>', TRUE)` (CASCADE removes every decomposed sub-IMV) then recreate
each spec forward-topo — `create_reflex_ivm` materializes from the (now complete) source
at build time. In this codebase that is
`base-db-manager recreate-views <tenant> --only <top> --with-downstream`.

**Proposed fix.** `reflex_doctor()` (below) MUST treat "per-partition reconcile errored or
failed to converge after K attempts" as an escalation trigger to a **chain rebuild**
(`drop_reflex_ivm CASCADE` + recreate), rather than looping reconciles that cannot
succeed. A standalone `reflex_rebuild_chain(view)` primitive that does the CASCADE-drop +
ordered recreate inside the extension (no external tooling) would let the doctor perform
this repair itself.

### F5 — `reflex_rebuild_imv` on a partitioned IMV only swaps *structurally-changed* children — S1

**Symptom.** `reflex_rebuild_imv('omc.forecast_analysis_view')` returned
`RECONCILED (3 children swapped)` yet dp 473's partition stayed empty — its structure was
unchanged, only its *data* was stale.

**Root cause.** For a partitioned IMV, "rebuild" reconciles by structural diff of the
partition tree; a partition whose child already exists (even if empty/stale) is not
re-derived. The name `reflex_rebuild_imv` implies a full recompute and misleads operators
into thinking data is guaranteed fresh afterward.

**Proposed fix.**
- Add a `force_full BOOLEAN DEFAULT FALSE` (or a `reflex_rebuild_imv_full`) that
  re-derives **every** partition's data regardless of structural diff.
- Document the structural-diff semantics prominently on
  [`reflex_rebuild_imv`](api/reflex_reconcile.md) and cross-link
  [`reflex_reconcile_partition`](api/reflex_reconcile_partition.md) as the per-partition
  data-forcing path.

### F6 — `ignore_sources` + archive residue: DPs archived while their source was empty are permanently empty — S1

**Symptom.** `forecast_analysis_view` shows 0 rows for an archived DP even though its
anchor `sop_forecast_view` now holds all rows for it.

**Root cause.** `forecast_analysis_view` lists `sop_forecast_view` in `ignore_sources`,
so filling the source does not incrementally propagate. Archived DPs are materialized
only (a) at archive time by the factory flow's explicit
`reflex_reconcile_partition`, or (b) by a full chain drop+recreate
(`scripts/backfill_forecast_analysis_archives.py`). If a DP is archived while its source
partition is empty (e.g. during an F1 wedge), it materializes an **empty** partition that
neither incremental maintenance nor `reflex_rebuild_imv` (F5) will ever repair.

**Impact.** Silent, indefinite under-reporting for archived demand plans. The residue is
detectable only by the operator-authored "source has rows, IMV partition empty" query.

**Proposed fix.**
- A first-class detector: for each partitioned IMV, list partition keys where the
  authoritative source has rows but the IMV partition is empty (respecting the IMV's own
  `WHERE` predicate as far as feasible). Fold into `reflex_audit`/`reflex_doctor`.
- Document the `ignore_sources` maintenance contract: which mutations require an explicit
  reconcile and which are incremental, so the archive flow's obligations are discoverable
  from the docs, not only from a view header comment.

### F7 — Snapshot vs live-leaf divergence when DDL events are missed — S2

**Symptom.** `__reflex_source_partition_snapshot` can disagree with the live partition
tree (extension installed *after* partitions were created; bulk DDL done in a path the
event trigger didn't tag; a crash between snapshot refresh and commit).

**Root cause.** The snapshot is maintained incrementally off the DDL event stream; there
is no periodic reconciliation of snapshot ⇄ `pg_inherits`/`pg_class` oids.

**Proposed fix.** A snapshot self-heal: oid-diff `__reflex_source_partition_snapshot`
against the live leaf set per root and refresh via
`__reflex_refresh_partition_snapshot(root)` when they diverge. Non-breaking — snapshot is
derived state.

### F8 — Bare-vs-qualified name matching in `depends_on`/registry is collision-prone in shared multi-tenant DBs — S1

**Symptom / context.** Registry matching does
`depends_on @> ARRAY[root] OR depends_on @> ARRAY[split_part(root,'.',2)]`, and IMVs can
be registered under **bare** names. In a shared DB with hundreds of tenant schemas
(`omc`, `nvg`, `alp`, …) a bare name (`sales_simulation`) is ambiguous — the first
tenant wins and the rest can silently mis-target or no-op. (Already documented downstream
as an app-side rule to always pass `schema=`.)

**Proposed fix.** A doctor check that flags any `__reflex_ivm_reference` row whose `name`
or `depends_on` entries are unqualified when the DB contains the same relation name in
more than one schema, with a remediation hint. Optionally a strict GUC that rejects
bare-name registration.

### F9 — `REFLEX-DBG resolve_anchor …` NOTICE spam in production — S3

**Symptom.** Every reconcile/flush emits multiple `NOTICE: REFLEX-DBG resolve_anchor …`
lines. On a multi-IMV cascade this is hundreds of lines that bury the real WARNINGs.

**Proposed fix.** Gate `REFLEX-DBG` output behind a GUC (e.g.
`pg_reflex.debug_resolve_anchor = off` default) or `DEBUG1` log level.

### F10 — No single "diagnose + repair" entrypoint — S2

**Root cause.** Recovery required, in sequence and by hand: inspect
`__reflex_partition_pending`; read `pg_event_trigger`; compare parent vs partition
`pg_trigger`; `reflex_flush_partitions()`; interpret NOTICEs; `reflex_sync_partitions(…,
true)`; `reflex_rebuild_imv(…)` up the chain; author a "source-has-rows / IMV-empty"
blast-radius query; per-DP `reflex_reconcile_partition`. This is expert-only and error
-prone. → see `reflex_doctor()`.

---

## Proposed function: `reflex_doctor()`

A single operator entrypoint that **detects** every inconsistency class above and
**applies only non-breaking repairs**, returning a structured report of what it found and
what it did. It is the auto-remediating companion to the detect-only
[`reflex_audit`](api/reflex_audit.md) and the read-only
[`reflex_ivm_status`](api/reflex_ivm_status.md).

> **Design contract — "non-breaking."** `reflex_doctor()` may only make repairs that are
> *idempotent and additive*: they can lose no committed IMV data and change no user
> object. Concretely it MAY: drain/retry the pending queue, refresh derived snapshots,
> reconcile empty/stale partitions from an authoritative source, rebuild drifted mirror
> children, re-emit trigger bodies, install a missing capture trigger. It MUST NOT, unless
> explicitly authorized by a separate flag: drop any data-bearing relation, drop an orphan
> that still has a matching live source partition, alter a `base_query`, or change
> partition bounds. Anything outside the safe class is **reported, not performed**.

### Signature

```sql
reflex_doctor(
    target       TEXT    DEFAULT NULL,   -- one IMV or source root; NULL = whole DB
    fix          BOOLEAN DEFAULT FALSE,  -- FALSE = report only (dry run)
    drop_orphans BOOLEAN DEFAULT FALSE,  -- authorize the one destructive-ish repair (F3)
    max_attempts INT     DEFAULT 3       -- retry budget per stuck root (F1/F2)
) RETURNS TABLE(
    check_id     TEXT,     -- 'F1' … 'F8'
    severity     TEXT,     -- 'S1' | 'S2' | 'S3'
    object       TEXT,     -- IMV / root / partition the finding is about
    finding      TEXT,     -- one-line description
    action       TEXT,     -- what was done, or the exact SQL to run if fix=FALSE
    outcome      TEXT      -- 'fixed' | 'reported' | 'skipped(needs drop_orphans)' | 'failed:<err>'
)
```

Default invocation is a **dry run** (`fix => FALSE`): it prints the diagnosis and the
exact remediation SQL for each finding, mutating nothing — safe to run on prod any time.

### Checks → repairs

| # | Check | Detection | Non-breaking repair (`fix => TRUE`) |
|---|---|---|---|
| F1/F2 | Wedged pending queue | rows in `__reflex_partition_pending` older than a threshold, or with `attempts >= max_attempts` | re-run `reflex_flush_partition_source(root)` per root in its own subtransaction; on success the row drains; on repeated failure, report with `last_error` |
| F3 | Orphan overlap | IMV target/intermediate partition whose bounds match no live source partition **and** collides with a pending swap target | report by default; **with `drop_orphans => TRUE`** drop the confirmed orphan then `reflex_sync_partitions(imv, true)` (reconcile refills) |
| F4 | Known-stale IMVs | durable stale flag (F4) set, or `reflex_ivm_status.last_error` non-null | `reflex_reconcile_partition` (partitioned) or `reflex_rebuild_imv` (unpartitioned); clear the flag on success |
| F5 | Data-stale partitions | partition child exists but `count(*)=0` while the authoritative source has rows for those keys (respecting the IMV predicate) | `reflex_reconcile_partition(imv, keys)` — the force-data path, not structural rebuild |
| F6 | Archive residue | archived/known DP present in source, empty in a dependent that `ignore_sources` the source | same as F5, per affected key; report the set first (blast radius) |
| F7 | Snapshot drift | `__reflex_source_partition_snapshot` oid-diff vs live `pg_inherits` | `__reflex_refresh_partition_snapshot(root)` |
| (misc) | Missing capture triggers | an **unpartitioned** source of an enabled IMV lacking its `__reflex_trigger_*` set | `reflex_rebuild_triggers(source)` |
| F8 | Bare-name ambiguity | registry `name`/`depends_on` unqualified while the relation name exists in >1 schema | report only (never auto-rename — could mis-target) |

### Report format

Human-readable when called bare in psql; the `TABLE` shape lets tooling filter by
`severity`/`outcome`. Example dry-run over the whole DB:

```
 check_id | severity | object                         | finding                                   | outcome
----------+----------+--------------------------------+-------------------------------------------+---------
 F1       | S1       | nvg.sales_simulation           | pending 6d, 0 flush attempts fired        | reported
 F5       | S1       | omc.sop_forecast_view/p_473    | source 1.69M rows, IMV partition empty    | reported
 F6       | S1       | omc.forecast_analysis_view/473 | archived DP empty (ignore_sources)        | reported
 F3       | S1       | omc.forecast_analysis_view     | orphan _38e6d6c1 blocks swap (overlap)    | skipped(needs drop_orphans)
 F9       | S3       | (global)                       | REFLEX-DBG NOTICEs enabled                 | reported
```

Then `SELECT reflex_doctor('omc', fix => TRUE, drop_orphans => TRUE);` performs the safe
repairs top-down (source before dependents, following the IMV dependency graph so a fix
never runs against a still-stale parent) and reports `fixed`/`failed` per item.

### Relationship to existing functions

`reflex_doctor()` orchestrates primitives that already exist — it is glue + a health
model, not new maintenance machinery:

- detection reuses [`reflex_audit`](api/reflex_audit.md) tiers +
  [`reflex_ivm_status`](api/reflex_ivm_status.md) + the new pending-queue/residue checks;
- repair reuses [`reflex_flush_partition_source`](api/reflex_flush_partition_source.md),
  [`reflex_sync_partitions`](api/reflex_sync_partitions.md),
  [`reflex_reconcile_partition`](api/reflex_reconcile_partition.md),
  [`reflex_rebuild_imv`](api/reflex_reconcile.md),
  [`reflex_rebuild_triggers`](api/reflex_rebuild_triggers.md), and
  `__reflex_refresh_partition_snapshot`.

Landing F1 (re-arm) and F4 (durable stale flag) first removes the two *silent* legs of
the incident; `reflex_doctor()` then makes the remaining classes a one-command diagnosis
and repair.

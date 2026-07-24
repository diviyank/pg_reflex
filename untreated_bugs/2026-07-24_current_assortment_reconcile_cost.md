# 2026-07-24 — `alp.current_assortment_activity_view` reconcile costs 2.5 h: statement-attributed, version-attributed, fixability

**Status: investigated, resolved as no-package-fix (PS-CA, 1.11.1). The correct lever
is push-based invalidation, not a pull-based skip.** A dedicated investigation proved
that no sound, cheap, testable package-level "skip-unchanged reconcile" signal exists
for this shape: a blanket reconcile is a trustless output-drift safety net, so any
input-derived signal can at best certify "no input changed" — never "stored output is
correct" — and the only input-independent certificate of output correctness is
recomputing it (the reconcile itself). No skip logic was shipped; demonstration tests
proving each candidate signal is blind to a real staleness live in
`src/tests/pg_test_psca_skip_signal.rs` (they go RED as a signpost if a future change
makes a signal trackable).

**The correct fix, empirically confirmed:** do NOT blanket-reconcile a matview-sourced
IMV. After `REFRESH MATERIALIZED VIEW <mv>`, call `refresh_imv_depending_on('<mv>')` —
its `WHERE $1 = ANY(depends_on)` matches this IMV (the matview referenced only inside
the scalar subquery IS captured in `depends_on`, verified), so it reconciles exactly
when the matview changed, avoiding both the redundant 2.5 h reconciles AND the
staleness. This is the sound, push-based version of what a "skip-unchanged" pass was
trying to approximate by guess. The remaining 2.5 h cost of an actually-needed
reconcile is a property of the view shape (non-partition-key filter from an
uncorrelated scalar subquery → no pruning) and is the user's view to fix; no safe
pg_reflex-side rewrite exists.

Investigates the field data point appended to **B7**: a controlled full reconcile
of all 190 IMVs (one `reflex_reconcile` per transaction) ran 190/190 clean, but
`alp.current_assortment_activity_view` **alone took 9 211 s (~2.5 h)** on
**pg_reflex 1.10.11** (db_prod restore), holding the whole `alp` pass to ~4 h.

## TL;DR

| | version / instance | number | provenance |
|---|---|---|---|
| Field measurement | **1.10.11**, db_prod restore | **9 211 s** | B7 report |
| Real reconcile, db_clone | **1.9.0**, db_clone | **1.7 s** (non-reproducing) | `SELECT extversion` = 1.9.0; matview empty → 0 rows |
| Dominant statement | any version | **`INSERT INTO <target> <base_query>`** — scan-bound | `reconcile.rs:203` (1.11.0), `:196` (1.10.11) |
| 1.11.0 vs 1.10.11 | **1.11.0** candidate | **NEUTRAL** (by code inspection + plan shape) | PS-5 touches only the flush/merge path, not reconcile |

- The view is **NOT a BOOL_OR aggregate** (memory was wrong). It is a **passthrough**
  (`is_passthrough: true` in the registry, empty `end_query`):
  `SELECT product_id, location_id, is_active FROM alp.assortment_activity_relation
  WHERE assortment_id = (SELECT assortment_id FROM alp.sop_current_view)`.
- The cost is **Θ(full source scan)**, not Θ(rows returned). It rebuilds a
  small target (one assortment's rows) by **full-scanning the entire partitioned
  source**.
- **1.11.0 does not change it.** This is a pre-existing S1 the 1.11.0 release
  neither introduced nor claims to fix. It is **not a release blocker**.
  `integration/1.11.0` was not touched.

## Mechanism (statement-attributed)

`reflex_reconcile` on this IMV takes the **passthrough branch**
(`src/reconcile.rs:139-215` in 1.11.0 / `:132-214` in 1.10.11 — the branches are
byte-identical): drop indexes → `TRUNCATE target` → **`INSERT INTO target <base_query>`**
→ recreate indexes → `ANALYZE`. `base_query` is `record.base_query.clone()`
(`reconcile.rs:44`) — the user's SELECT **verbatim**, never rewritten at reconcile
time.

`EXPLAIN` of that `base_query` on db_clone shows the pathology directly — **no
partition pruning, every non-empty partition Seq-Scanned with a Filter**:

```
Append (rows=208674)
  InitPlan 1 -> Seq Scan on sop_current_view          -- the scalar subquery
  -> Seq Scan on assortment_activity_relation_p_95   Filter: (assortment_id = (InitPlan 1).col1)
  -> Seq Scan on assortment_activity_relation_p_96   Filter: (assortment_id = (InitPlan 1).col1)
  ...  (every partition, full Seq Scan)
```

Two compounding causes, **both properties of the view shape + source, not of any
pg_reflex version**:

1. **No partition pruning.** `assortment_id` is *not* the partition key of
   `alp.assortment_activity_relation` (it is LIST-partitioned on a snapshot id —
   partitions `p_70, p_95…p_102, default`). A filter on a non-partition-key column
   cannot prune, so *every* partition is scanned.
2. **Seq scan instead of index scan.** The filter comes from an **uncorrelated
   scalar subquery** `(SELECT assortment_id FROM alp.sop_current_view)`, evaluated
   as an `InitPlan`. Its value is **unknown at plan time**, so the planner uses
   average selectivity `1/n_distinct(assortment_id)`. On db_clone
   `assortment_id` has only **7 distinct values** across the whole table, so the
   planner estimates ~1/7 of *every* partition matches — well above the seq-scan
   crossover — and picks a **full Seq Scan per partition** rather than the
   per-partition `assortment_id` index. Runtime, only the current assortment
   matches, but the scan has already read everything.

Net: to rebuild a target holding one assortment, reconcile reads the **entire
multi-partition history** of every assortment, every time.

### Scale reproduction (statement attribution)

db_clone's real data does not reproduce (the `alp.sop_current_view` matview is
empty → `base_query` yields 0 rows → 1.7 s reconcile). Reproduced the plan shape
and attributed the statements on a throwaway 8 M-row synthetic (db_clone, **1.9.0**,
warm cache; source LIST-partitioned on a non-`assortment_id` key, `assortment_id`
with 4 distinct values, `storage: UNLOGGED` target matching the real IMV):

| reconcile statement | time (8 M rows, warm) | share |
|---|---|---|
| **`INSERT INTO target <base_query>`** | **1 068 ms** | dominant (~65 %) |
| `CREATE INDEX` (recreate) | 412 ms | ~25 % |
| `ANALYZE` | 56 ms | ~3 % |
| `DROP INDEX` / `TRUNCATE` | < 10 ms | — |

The `INSERT` is **scan-bound** — it full-scans all 8 partitions (`Seq Scan … Filter`,
`Rows Removed by Filter` on the non-matching ones) regardless of how few rows the
current assortment matches. With 200 distinct `assortment_id`s instead of 4, the
same query flips to per-partition Bitmap Index Scans and runs in **35 ms** — direct
confirmation that low cardinality is the trigger.

The absolute **2.5 h at db_prod is this same `INSERT`, scaled to prod data volume
and cold I/O** (prod source is far larger than db_clone's 208 k rows, and cold). It
was **not reproducible to wall-clock on db_clone** — the honest number there is the
plan shape + the statement attribution above, not 9 211 s.

## Version attribution — 1.11.0 is NEUTRAL

- **Working tree** `tools/pg_reflex` = **1.10.11**; candidate worktree
  `tools/pg_reflex-integration` = **1.11.0**; db_clone installed = **1.9.0**
  (`SELECT extversion FROM pg_extension`).
- The reconcile **passthrough branch is identical** between 1.10.11 and 1.11.0
  (`diff` of the two `reconcile.rs` passthrough branches is empty; the only delta
  anywhere in the file is an ANALYZE-ordering line in the *partitioned* branch,
  which this unpartitioned IMV never enters).
- **PS-5 (sargability gating) touches only the incremental path** — markers appear
  solely in `src/trigger/merge.rs`, `src/trigger/ops.rs`, `src/trigger/dispatch.rs`,
  `src/lib.rs`. **None in `reconcile.rs`.** PS-5 gates the MERGE / target-sync joins
  that run on trigger flush; it does not see the full-rebuild `INSERT … base_query`.
- `generate_base_query` (`src/query_decomposer.rs:249`) emits the passthrough SELECT
  verbatim — no sargability rewrite, no `enable_seqscan` toggle, no WHERE injection —
  on both versions. Confirmed the db_clone-stored `base_query` equals the source SQL
  file byte-for-byte.
- **No pg_reflex version can add partition pruning** on a non-partition-key filter
  column, nor change the planner's InitPlan selectivity estimate. The plan shape is
  version-independent.

**Verdict: 1.11.0 is neutral.** Same reconcile, same plan, same cost. (Not
wall-clocked at prod scale on a 1.11.0 instance — asserted from code identity +
plan-shape invariance, as the task permits when prod-scale data is unavailable.)

## Fixability — inherent to a full rebuild of this shape; no safe worthwhile fix

`reflex_reconcile` runs the user's `base_query` verbatim and is Θ(source scan) by
construction for any full rebuild. Options considered and rejected:

- **Force index scan** (`SET LOCAL enable_seqscan = off` around the `INSERT`) —
  global, query-blind, and *wrong* when the current assortment matches a large
  fraction (index scan would be slower). At ~1/7 selectivity here the planner's seq
  scan is arguably the correct choice anyway. Rejected: correctness/perf gamble,
  over-complexifies a hot path (violates the project's simplicity priority).
- **Inline the scalar subquery as a literal** before the `INSERT` so the planner
  sees the actual value's MCV selectivity — would only help if the current
  assortment is *rare*; at 1/7 it likely still seq-scans. Adds parser/rewrite
  complexity to reconcile for an unreliable win. Rejected.
- **Skip-if-source-unchanged guard inside reconcile** — cheap correct change
  detection is hard here (the matview subquery can change the selected assortment
  with no base-table write), and it duplicates the "skip already-fresh" idea already
  filed under B6/B7. Rejected as a reconcile-internal change.

**The real lever is not the per-IMV reconcile — it is not reconciling this shape
routinely.** Incremental maintenance (the point of pg_reflex) handles assortment
changes via triggers; reconcile is the occasional drift/safety-net path and is
Θ(source) for *any* full-rebuild IMV. The 190-IMV blanket pass paid a full rebuild
for every IMV unconditionally. **PS-7's chunked/resumable
`reflex_scheduled_reconcile` does NOT solve this** (the task's framing is correct): a
single 2.5 h IMV reconcile still runs in one uninterruptible statement and blocks
its chunk. Only making the pass *skip IMVs that don't need rebuilding* helps.

## Recommended follow-up (pre-spec)

Not a reconcile-internal fix. File against the orchestration path, folding into
B6/B7 rather than duplicating:

1. **Skip-fresh in the blanket pass.** `reflex_scheduled_reconcile` / any
   registry-wide recovery should treat an IMV whose incremental state is healthy
   (`known_stale = f`, `flush_count` advancing, `last_update_date` recent relative
   to its sources) as a **skip**, not an unconditional full rebuild. This is the one
   change that removes the 2.5 h from the common path — the IMV rarely *needs* a
   full rebuild.
2. **Cost-visibility.** Surface per-IMV reconcile wall-time in `reflex_ivm_status`
   so an operator sees *before* launching a blanket pass that
   `current_assortment_activity_view` is a 2.5 h item, and can reconcile it out of
   band.
3. **Document the anti-pattern.** A passthrough IMV whose filter column is neither
   the source partition key nor high-cardinality has a Θ(full-source-scan) reconcile.
   Worth a note in the IMV-authoring guidance: such views reconcile expensively even
   though their incremental maintenance is cheap.

No change to `integration/1.11.0`.

## Repro pointers

```sql
-- db_clone, 1.9.0: real objects do NOT reproduce (matview empty)
SELECT extversion FROM pg_extension WHERE extname='pg_reflex';      -- 1.9.0
SELECT count(*) FROM alp.sop_current_view;                          -- 0  -> base_query yields 0 rows
\timing on
SELECT reflex_reconcile('alp.current_assortment_activity_view');    -- 1.7 s (no rows)

-- plan shape (no pruning, seq scan per partition):
EXPLAIN SELECT product_id, location_id, is_active
        FROM alp.assortment_activity_relation
        WHERE assortment_id = (SELECT assortment_id FROM alp.sop_current_view);

-- assortment_id cardinality (why the planner seq-scans):
SELECT count(DISTINCT assortment_id) FROM alp.assortment_activity_relation;   -- 7
```

Scale reproduction / statement attribution used a throwaway `reflex_repro` schema
(8 M-row LIST-partitioned source, non-`assortment_id` key, low-cardinality
`assortment_id`, 1-row matview, UNLOGGED target) — dropped after measurement; the
db_clone extension (1.9.0) and the real view were left untouched.

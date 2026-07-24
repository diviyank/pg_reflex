# 2026-07-24 — no primitive recreates a dropped intermediate table, yet `internal-tables-exist` prescribes one at Error severity

**Status: untreated.** Found under PS-9 (B9) while implementing the
`partition-mirror` narrowing, and confirmed by probe on `main` @ `eca3807`
(1.11.0). Not a B9 regression — pre-existing.

When an aggregate IMV's `__reflex_intermediate_<view>` is gone (dropped by hand,
lost to a `DROP … CASCADE`, or removed with a schema), `reflex_audit` /
`reflex_doctor` report it correctly, but **the remedy they print cannot repair
it**, and no other exposed primitive can either. Same family as B5 and B9: a
prescribed fix that structurally cannot resolve its own finding.

## Reproduction (probe output, pg17, real IMV over a real partitioned source)

```sql
CREATE TABLE s (id BIGINT, region TEXT NOT NULL, amount NUMERIC) PARTITION BY LIST (region);
CREATE TABLE s_us PARTITION OF s FOR VALUES IN ('us');
CREATE TABLE s_eu PARTITION OF s FOR VALUES IN ('eu');
INSERT INTO s VALUES (1,'us',100),(2,'eu',200);
SELECT create_reflex_ivm('v', 'SELECT region, SUM(amount) AS total FROM s GROUP BY region',
                         NULL, NULL, NULL, NULL, ARRAY['region']);
DROP TABLE __reflex_intermediate_v CASCADE;
```

Observed:

```
reconcile                              => ERROR: partition reconcile failed
intermediate relations after reconcile => 0
rebuild_imv                            => ERROR: partition reconcile failed
```

and the audit report afterwards is byte-identical to the one before the repair
attempt:

```
[ERROR] v  internal-tables-exist
  Missing internal table(s) for IMV v:
    "__reflex_intermediate_v"
  Suggested fix:
    SELECT reflex_rebuild_imv('v');
```

The underlying `warning!` from the swap path names the real cause:
`missing intermediate bound for child '__reflex_intermediate_v_s_eu'`.

## Why every primitive fails

- **`reflex_rebuild_imv` is a literal alias for `reflex_reconcile`**
  (`src/lib.rs:823`: `fn reflex_rebuild_imv(view_name) { reconcile::reflex_reconcile(view_name) }`).
  The two prescriptions an operator would try are the *same call under two names*.
- **Partitioned IMVs** take the per-partition swap path in `reconcile_one`
  (`src/reconcile.rs:60-94`), which walks the anchor's children and calls
  `execute_partition_swap_for_child`. That builds a swap table and **ATTACHes** it
  to the intermediate parent — it requires the parent to already exist, and returns
  `"ERROR: partition reconcile failed"` when it does not.
- **Unpartitioned aggregate IMVs** would fail in the same spirit one branch down:
  the rebuild issues `TRUNCATE {intermediate}` / `INSERT INTO {intermediate} …`
  (`src/reconcile.rs:320-328`) against a name that does not resolve → 42P01.
- `reflex_sync_partitions` deliberately does not help: it gates every
  intermediate-child DDL on a `to_regclass` existence probe
  (`src/partition.rs:1055`) precisely because `CREATE TABLE … PARTITION OF
  <absent>` raises 42P01. It reports `+0 intermediate` and succeeds.

Intermediate DDL is emitted **only** at create time, via
`build_intermediate_table_ddl` (`src/create_ivm/mod.rs:1076`,
`src/schema_builder.rs:110`). No reconcile, rebuild, sync, flush or doctor path
re-issues it. The only working recovery today is `drop_reflex_ivm('<view>')`
followed by re-running the original `create_reflex_ivm` — which requires the
original SQL, and for a generated sub-IMV there is no spec file holding it (the
B3 `create_args` gap).

## What was ruled out

- **Not the PS-9 `partition-mirror` change.** Reproduced with the audit check's
  intermediate half fully silent; the finding above comes from
  `internal-tables-exist` (`src/audit/checks_a_catastrophic.rs:284-309`), untouched
  on this branch. PS-9 deliberately does **not** add a competing
  `partition-mirror` finding for absence, because doing so would ship a *second*
  unclearable Error prescribing the same failing call.
- **Not a `search_path` artefact.** The fixture is in `public` with the default
  test path, and the probe counts the relation via `pg_class` by bare relname
  (0 rows), not through a name-resolution round trip.
- **Not partition-specific.** The partitioned path is merely where it fails
  loudest (`ERROR: partition reconcile failed`); the unpartitioned aggregate path
  hits the same missing relation on `TRUNCATE`.
- **Not "reconcile silently succeeded".** It returned an explicit `ERROR: …`
  string and left `count(*) = 0` intermediate relations.

## Severity

S2. Not silent data loss — the audit does report the condition, and the remedy
fails loudly rather than claiming success. But it is unclearable by the printed
fix, so an operator following the tool's own instruction retries indefinitely,
and `reflex_doctor(fix => true)` will keep re-running a call that cannot converge.

## Fix direction

Two candidate directions; (a) is preferable.

- **(a) Make the intermediate re-creatable.** Factor the create-time DDL emission
  (`build_intermediate_table_ddl` + `affected_groups` + the partition-child loop)
  into a heal step that `reflex_reconcile` runs *before* the swap walk when the
  parent is absent. All the inputs are already in the registry row
  (`aggregations`, `partition_columns`, `partition_strategy`, `storage_mode`),
  which is why B3's missing `create_args` does not block this. Guard it so it only
  fires for `!end_query.is_empty()` rows, or it will try to build an intermediate
  for a passthrough IMV — the mirror image of the B9 phantom.
- **(b) Tell the truth instead.** Change `internal-tables-exist`'s
  `suggested_fix` for a missing intermediate to the sequence that actually works
  (`SELECT drop_reflex_ivm('<view>'); -- then re-run the original create_reflex_ivm`)
  and stop `reflex_doctor(fix => true)` from auto-running the failing call. Cheap,
  honest, but leaves the operator without an in-extension recovery.

Either way, add a regression test asserting that after `DROP TABLE
__reflex_intermediate_<view> CASCADE` the *printed* remedy clears the
`internal-tables-exist` finding — the same remedy-convergence property PS-9 added
for `partition-mirror`.

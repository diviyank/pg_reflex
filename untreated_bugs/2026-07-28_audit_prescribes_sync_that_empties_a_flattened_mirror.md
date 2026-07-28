# 2026-07-28 — `reflex_audit` tells the operator to run the one command that destroys a flattened mirror

**Status: untreated. Measured and pinned by a test** (PostgreSQL 16.11 under pgrx, on
`fix/swap-flattens-subpartitioned-child`).

Found while fixing
`2026-07-28_swap_flattens_subpartitioned_child_then_sync_empties_imv.md`; filed separately per
the `untreated_bugs/` hygiene rule — different defect, different fix location, and it survives
that fix.

Severity: **high, and unusually so for an advisory defect.** No wrong data is produced by the
audit itself. The failure is that the population most likely to run `reflex_audit` — operators
who suspect something is wrong with a partitioned IMV — is exactly the population whose IMV the
prescribed remedy empties.

## The mechanism

A mirror that has been flattened (see the sibling report: the swap's
`CREATE TABLE ... (LIKE old INCLUDING ALL)` cannot carry partitioning, so a depth-≥2 mirror
child is replaced by a plain table) still holds **correct data**. It is armed, not broken.

`reflex_audit` notices — the mirror's real leaves read as missing and the flattened parents as
extra — and emits `partition-tree-drift` findings. The remedy it prints is

```sql
SELECT reflex_sync_partitions('<imv>', TRUE);
```

(`src/audit/checks_b_drift.rs:407-426`).

That is precisely the destructive step. The sync's shape-drift heal drops the flattened children
and recreates them **empty, with no refill**, taking the IMV to zero rows. Measured on the
sibling report's fixture: 400 rows → 0; on a larger one, 2,800,000 → 0. NOTICE-level output only,
no error, `known_stale` never set, source intact.

`drop_orphans => FALSE` does **not** protect against it (measured).

## Why this is not merely "an unhelpful hint"

CLAUDE.md's rule is *"don't print a remedy that can't clear its own finding"* — an audit finding
whose suggested fix structurally cannot resolve it sends operators into a retry loop. This is a
strictly worse instance: the remedy does not fail to clear the finding, it **destroys the data
while clearing it**. The finding does go away. So does the IMV's contents.

## Reproduction

Pinned in-suite by `pg_subpart_reconcile_repairs_an_already_flattened_mirror`
(`src/tests/pg_test_subpartition_dataloss.rs`), which builds a genuinely flattened mirror by
replaying the old swap's exact statements, asserts the data is still correct via
`assert_imv_correct`, then asserts both halves of this hazard:

```rust
assert!(report.contains("partition-tree-drift"));   // the audit does flag it
assert!(report.contains("reflex_sync_partitions")); // ...and prescribes the destructive fix
```

The second assertion is deliberately a canary: if the audit's remedy text changes, the test
fails and whoever changed it is pointed at the operator warning that depends on it.

## Scope

After the fix on `fix/swap-flattens-subpartitioned-child`, a flattened mirror can no longer be
**created** — `reflex_reconcile` resolves mirror leaves, and `execute_partition_swap_for_child`
refuses a `relkind='p'` child outright. So this hazard applies only to mirrors **already
flattened in the field** before that fix is deployed.

That population is not hypothetical, and it is self-selecting: a flattened IMV shows no symptom
until someone runs a sync or an audit.

Find already-flattened mirrors with:

```sql
SELECT r.name, r.partition_depth
FROM public.__reflex_ivm_reference r
WHERE r.enabled
  AND COALESCE(r.partition_depth, 0) >= 2
  AND NOT EXISTS (SELECT 1 FROM pg_inherits i
                  JOIN pg_class c ON c.oid = i.inhrelid
                  WHERE i.inhparent = to_regclass(r.name) AND c.relkind = 'p');
```

A NULL `partition_depth` means "mirror the full source depth"; such rows are excluded by the
`COALESCE(...,0)` and must be inspected by hand.

## The correct remedy

`SELECT reflex_reconcile('<imv>');` — and nothing else. Verified by the same test: it restores
the depth-2 shape, restores the leaves, refills the data, and passes the bidirectional
`EXCEPT ALL` oracle. Dropping and recreating the IMV is not necessary.

On an **unfixed** build the repair is a stopgap rather than a cure — the repaired mirror is
immediately re-exposed, because the next `reflex_reconcile` sees non-empty children and flattens
again. Deploy the fix, then repair.

## Fix direction

Cheapest first; not attempted here.

1. **Make the audit's remedy safe for this shape.** `checks_b_drift.rs` should distinguish
   "mirror children are the wrong shape" from ordinary tree drift, and prescribe
   `reflex_reconcile` for the former. This is a one-branch change in the finding's
   `suggested_fix` and closes the hazard for existing findings.
2. **Make the sync refuse rather than empty.** The shape-drift heal should not drop and recreate
   a child that currently holds rows without refilling it — "refuse loudly, never no-op silently"
   applied to a destructive heal. This is the deeper fix and protects every caller of the sync,
   not only the ones the audit sent. It needs a survey of legitimate drift-heal cases first, since
   some of them genuinely intend to discard a child's contents.
3. Consider whether `known_stale` should be set when a heal empties a previously-populated child.
   Currently it is not, which is why the emptying is silent to every downstream check.

Option 2 subsumes option 1 but is riskier; option 1 is safe to ship immediately and independently.

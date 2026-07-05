# `reflex_doctor`

(1.10.8+) One-stop operator entrypoint that diagnoses every inconsistency class and applies only non-breaking repairs. Dry-run by default.

## Signature

```sql
reflex_doctor(
    target       TEXT    DEFAULT NULL,   -- one IMV or source root; NULL = whole DB
    fix          BOOLEAN DEFAULT FALSE,  -- FALSE = report only (dry run)
    drop_orphans BOOLEAN DEFAULT FALSE,  -- authorize the one destructive-ish repair (orphan drop)
    max_attempts INT     DEFAULT 3
) RETURNS TABLE(
    check_id TEXT, severity TEXT, object TEXT, finding TEXT, action TEXT, outcome TEXT
)
```

## Behaviour

Detects wedged pending roots, `known_stale` IMVs, archive residue, snapshot divergence, and orphan-overlap. When `fix => FALSE` (default), it is a dry run: prints the diagnosis and the exact remediation SQL without mutating anything. When `fix => TRUE`, applies **non-breaking** repairs top-down:

- Most repairs run automatically if `fix => TRUE`.
- The one destructive repair (dropping a confirmed orphan, F3) is gated behind `drop_orphans => TRUE`.
- The one non-additive repair (chain rebuild, F4b) is **never auto-performed** — it is reported with the `reflex_rebuild_chain(...)` call to run manually.

Each repair runs in its own subtransaction, so one failure records `failed:<err>` for that row without aborting the report. Outcome values are: `fixed`, `reported`, `skipped(needs drop_orphans)`, or `failed:<err>`.

`target` narrows the scope to one IMV name or source root; `NULL` audits the whole database.

Since 1.10.9 the underlying audit is crash-isolated per check: a check that raises a Postgres error becomes a `check-errored` finding rather than aborting the whole run. Whole-database scope (`target => NULL`) additionally surfaces orphan aux tables (**F9** — orphan intermediate/staging/scratch, with their `DROP … CASCADE`) and duplicate trigger functions (**F11**), both report-only. Archive-residue (F5/F6) repairs use `reflex_reconcile_partition` per partition, collapsing to a single `reflex_reconcile(<imv>)` when more than three partitions of one IMV are affected.

## Example: dry-run (report only)

```sql
SELECT check_id, severity, object, finding, action, outcome
FROM reflex_doctor()
ORDER BY severity DESC, object;
```

Result shows problems and the exact SQL to fix them, without mutating anything.

## Example: apply repairs

```sql
SELECT check_id, object, finding, outcome
FROM reflex_doctor('omc', fix => TRUE, drop_orphans => TRUE)
WHERE outcome != 'reported'
ORDER BY check_id;
```

Returns only fixes and failures; skips diagnostics.

## See also

- [`reflex_audit`](reflex_audit.md) — detailed consistency audit (report-only).
- [`reflex_ivm_status`](reflex_ivm_status.md) — check `known_stale` column to find broken IMVs.
- [`reflex_reconcile_partition`](reflex_reconcile_partition.md) — fix archive residue (archive_residue check).
- [`reflex_rebuild_chain`](reflex_rebuild_chain.md) — manual atomic rebuild of a decomposed IMV chain.

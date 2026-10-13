# `reflex_audit`

(1.5.0+) Runs a consistency audit over every enabled IMV, or a single named IMV, and returns a human-readable report. Use it to verify integrity after migrations, crashes, or manual DDL.

## Signatures

```sql
reflex_audit()               RETURNS TEXT   -- audit every enabled IMV
reflex_audit(view_name TEXT) RETURNS TEXT   -- audit one IMV
```

The single-IMV form raises if `view_name` is not registered or not enabled.

## Checks

Checks run in three tiers:

| Tier | Looks for |
|---|---|
| **Catastrophic** | Missing source or internal tables, trigger not attached, trigger mode mismatch, wrong staging shape. |
| **Drift** | `base_query` no longer runs, intermediate/target shape changed, partition-tree drift, partition mirror mismatch. |
| **Orphan** | Stray intermediate / scratch / staging tables, duplicate trigger functions. |

Additional checks:

- **`archive_residue`** (severity: Drift) — a partitioned IMV partition that is empty while its authoritative source (listed in `ignore_sources`) has rows for that partition key. Suggests `reflex_reconcile_partition` to fix.
- **`bare_name_ambiguity`** (severity: Drift) — an unqualified `depends_on` entry that resolves to a relation in more than one schema (detection-only; register schema-qualified names to fix).

Changes in 1.12.0:

- **`trigger-attached`** also reports disabled triggers (`Source <s> has disabled trigger(s): …`) and, for a partitioned source, each table of its partition tree (`Table <t> of the partition tree of source <s> is missing trigger(s): … and has disabled trigger(s): …`). The remedy of every `trigger-attached` finding is two statements: `SELECT reflex_rebuild_triggers('<source>');` then `SELECT reflex_reconcile('<imv>');`.
- **`trigger-mode-matches`** expects DEFERRED trigger functions on a source that any DEFERRED IMV reads, whatever the mode of the IMV audited: an IMMEDIATE IMV sharing a source with a DEFERRED one is no longer reported.
- **`internal-tables-exist`** requires `__reflex_scratch_<imv>` and, for a top-K MIN / MAX IMV, `__reflex_shrunk_<imv>`, and no longer demands `__reflex_affected_<imv>` for an ungrouped aggregate. `reflex_reconcile` recreates a missing scratch table.
- **`partition-mirror`**: the remedy for a dropped leaf is `SELECT reflex_sync_partitions('<imv>'); SELECT reflex_reconcile_partition('<imv>', '', '<source partition>');`.

## Examples

```sql
-- Audit everything
SELECT reflex_audit();

-- Audit one IMV
SELECT reflex_audit('sales_by_region');
```

When the audit flags drift, [`reflex_rebuild_imv`](reflex_reconcile.md) (or [`reflex_reconcile_partition`](reflex_reconcile_partition.md) for one partition) restores the IMV from source.

## See also

- [`reflex_doctor`](reflex_doctor.md) — auto-remediates detected inconsistencies (dry-run by default).
- [`reflex_ivm_status`](reflex_ivm_status.md) — quick health snapshot (includes `known_stale` flag).

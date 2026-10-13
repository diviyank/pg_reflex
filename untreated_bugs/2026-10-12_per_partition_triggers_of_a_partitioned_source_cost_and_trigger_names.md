# The per-partition triggers 1.12.0 installs on a partitioned source: cost not measured, and four of them are not named `__reflex_…` — perf / limitation (new in 1.12.0)

1.12.0 follows statements addressed to a partition by installing triggers on every partition
(`8d2c69c` and the TRUNCATE pair of the G8a work). Sources, in the ledger
`.superpowers/sdd/2026-10-07-1.12.0-create-contract-hardening/`: `fold-g8b1-report.md`
(concerns), `full-review-r3.md` (F6 and "What changed operationally", pg16, `0324f47`). Grouped
because both points are properties of the same new objects.

## What changed (R3 table)

| Topic | 1.11.5 | 1.12.0 |
|---|---|---|
| Triggers on the partitioned source | 4 (`__reflex_trigger_{ins,del,upd,trunc}_on_<src>`) | 6: + `__reflex_trigger_btrunc_on_<src>` (BEFORE TRUNCATE), `reflex_partitioned_source_<src>` (BEFORE INSERT/UPDATE/DELETE) |
| Triggers on each partition (leaf and branch) | none | 6: `__reflex_trigger_{btrunc,trunc}_on_<src>`, `reflex_partition_{ins,del,upd,guard}_on_<src>`; 3 when the column order differs |
| When they are installed | — | at create and `reflex_rebuild_triggers`; for a new / attached partition at the COMMIT of the DDL; removed at the COMMIT of a DETACH and at the drop of the last IMV |
| Cost per statement | — | one BEFORE trigger (a `pg_partition_ancestors` + `pg_trigger` probe on a partition, a `set_config`); **not benchmarked** |
| Cost of create / drop (**read from the code**) | locks the root | create: `ShareRowExclusiveLock` and 6 `CREATE TRIGGER` per partition; drop: `DROP TRIGGER` (`AccessExclusiveLock`) on every partition; COMMIT of any partition DDL: a catalog scan of the whole tree per registered spelling |

## 1. Cost

Nothing above is measured. For a source with thousands of partitions the create of the first IMV
takes a lock and runs six `CREATE TRIGGER` on each, the drop of the last IMV takes
`AccessExclusiveLock` on each, and every partition DDL scans the tree at COMMIT. The numbers of
`max_locks_per_transaction` such a create needs were not looked at either. No reproduction or
timing exists in the ledger; this is a list of what to benchmark.

## 2. Names

`reflex_partition_…` and `reflex_partitioned_source_…` lack the `__reflex_` prefix every other
pg_reflex object has. The reason is a test, not a design: g8a's
`v112_dp_drop_removes_the_truncate_triggers_of_every_partition` asserts that a partition carries
exactly 2 triggers named `__reflex_%`, and the author of the write triggers did not edit it
(`fold-g8b1-report.md`). R3 F6: nothing in the extension looks triggers up by that prefix
(`rg tgname src`: every lookup is by exact name or by function), so nothing breaks; **an operator
who filters `pg_trigger` on `__reflex_%` sees 2 of the 6 triggers of a partition and 5 of the 6
of the root**. Both reports ask for the name to be decided at integration; it was not changed.

## What is established and what is not

Established: the trigger counts and names (R3, pg16), and that no lookup depends on the prefix.
Not established: every cost figure.

## Fix direction (hypothesis to falsify)

Names: rename in `schema_builder.rs` (`partition_trigger_name`,
`partitioned_source_trigger_name`) and fix the count in that one test. The names are stored in
deployed 1.12.0 databases once released, so after the release this needs
`reflex_rebuild_triggers` to drop the old names; before the release it is a rename. Cost: a
benchmark of create / drop / one partition DDL on a source with 1 000 and 10 000 partitions, and
of a single-row INSERT with and without the BEFORE guard, before deciding anything.

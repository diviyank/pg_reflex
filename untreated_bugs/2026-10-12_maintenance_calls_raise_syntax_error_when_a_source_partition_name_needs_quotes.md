# `reflex_reconcile`, `reflex_rebuild_imv` and `refresh_reflex_imv` raise a syntax error when a partition of the source has a name that needs quotes — S2 (LOUD, the maintenance call is unusable)

Pre-existing (same on `d0a3e06`). Reported by the adversarial review of the 1.12.0 create
refusals (pg18); not re-run here, cause not investigated.

## Reproduction (as reported)

A partition-mirroring IMV over a partitioned source one of whose partitions is named
`"Sy"."P c"` (upper case, a space, in a mixed-case schema). Then:

```sql
SELECT reflex_reconcile('<imv>');     -- ERROR: syntax error at or near ")"
SELECT reflex_rebuild_imv('<imv>');   -- same
SELECT refresh_reflex_imv('<imv>');   -- same
```

## Expected vs actual

Expected: the call runs, or returns a tagged refusal. Actual: a raw syntax error.

## What is established and what is not

Established by the review: the three errors, on base and head. Not established: whether this
is the defect of
`2026-10-10_attached_partition_whose_name_does_not_read_back_unquoted_is_lost.md` (a partition
followed by its unquoted name) surfacing on another path; whether writes are maintained.

## Fix direction (hypothesis to falsify)

Quote partition names wherever the partition bookkeeping builds SQL from them, or refuse at
create a partitioned source that has such a partition, as is done for the source's own name.

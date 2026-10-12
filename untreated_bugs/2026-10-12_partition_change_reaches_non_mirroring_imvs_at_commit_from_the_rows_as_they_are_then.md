# A partition change (CREATE … PARTITION OF, ATTACH, DETACH) reaches the IMVs that do not mirror the partitions at COMMIT, from the partition's rows as they are then — perf / design (the cause behind several S1 reports and one full-reconcile cost)

Design-level finding of the 1.12.0 work, not one defect. Sources, in the ledger
`.superpowers/sdd/2026-10-07-1.12.0-create-contract-hardening/`: `fold-g8b1-report.md` §2
(pg18), `full-review-r3.md` (pg16, `0324f47` against `6946400`, "What changed operationally for
partitioned sources"), `full-review-r3-fix-report.md` (F1). The mechanism is pre-existing; the
cost described under "What 1.12.0 pays for it" is new in 1.12.0.

## The mechanism

A partition DDL on a source queues its root in `__reflex_partition_pending`. At COMMIT the flush
(`reflex_flush_partitions_impl`, `src/partition.rs`) applies, to every IMV of the source that
does not mirror its partitions (ungrouped, grouped by another column, joins, keyless), **every
row the added partition holds at that moment** as an INSERT delta
(`reflex_apply_partition_delta`), and the rows of a removed one as a DELETE delta. The statement
triggers, meanwhile, apply every write of the transaction as it happens. The two views of the
same transaction overlap whenever a row is both written through the tree and present in the
partition at COMMIT.

## What it has produced

Silent wrong results, each with its own report or fix:
- a partition created and filled through the parent in one transaction: the rows were applied
  twice (fixed in 1.12.0 by `ebb3aff`, see below);
- `2026-10-10_partition_change_and_write_to_another_join_source_in_one_transaction_double_counts.md`;
- `2026-10-10_reflex_reconcile_by_hand_in_the_transaction_that_attached_a_filled_partition_double_counts.md`;
- `2026-10-10_set_constraints_immediate_partitioned_source_several_deferred_imvs_double_applies.md`;
- `2026-10-10_drop_table_of_a_source_partition_that_holds_rows_leaves_the_imvs_stale.md` (at
  COMMIT the rows are gone, and nothing was queued);
- `2026-10-10_detach_partition_leaves_dependents_of_partition_mirroring_imvs_stale.md` (the
  mirror partition is dropped, which reaches no reader);
- a partition change and a partition TRUNCATE in one transaction (R3 F1, fixed by `f7928ec` the
  same way as the first item).

## What 1.12.0 pays for it

The fix of the first item does not make the two views agree; it detects the overlap and falls
back. From `fold-g8b1-report.md` §2: the delta of an unpartitioned IMV is replaced by
`reflex_reconcile` when a statement was addressed to a table of the source's tree in this
transaction (a transaction-local setting raised by a BEFORE trigger), or when the pending row
was not queued by this transaction or was already attempted. From the R3 table:

> Create partition (or ATTACH / DETACH) **and write through the parent or an existing
> partition**, one transaction: correct; costs a **full `reflex_reconcile` of every IMV of that
> source that does not mirror its partitions** (ungrouped, grouped by another column, joins,
> keyless), once per transaction at COMMIT, whatever the number of statements; any INSERT /
> UPDATE / DELETE on the tree in that transaction triggers it, also one touching no row or one
> issued before the DDL.

"On a large IMV that is a real cost for the pattern 'create the partition and insert through the
parent in one transaction'; not benchmarked" (g8b1). The way round it given by the review:
create the partition in an earlier transaction, or load the new partition directly (create
standalone, fill, ATTACH), which keeps the incremental path. A TRUNCATE of a partition is a full
rebuild of the source's IMVs applied as a row diff, once per statement.

## What is established and what is not

Established: the mechanism (read from the code by the author and the reviewer, and consistent
with every reproduction in the reports above); the fallback and its triggers (tests
`v112_pd_partition_added_and_written_in_one_transaction_*`,
`v112_pd_partition_flush_retried_after_a_failure_does_not_reapply_rows`). Not established: any
timing. No benchmark of the full reconcile at COMMIT, nor of the per-statement BEFORE trigger,
was run on a large source.

## Fix direction (untested hypothesis)

Apply the ATTACH / DETACH delta **at DDL time through the ordinary write path**: when the event
trigger sees the partition change, feed the partition's rows to the same delta code a statement
on the source runs (IMMEDIATE: apply; DEFERRED: stage), so that later statements of the
transaction see an IMV that already contains them, and the COMMIT flush has nothing left to
apply to non-mirroring IMVs. The overlap, the fallback to a full reconcile and the by-hand
reconcile double count would go with it.

Open questions to answer before any code:
- **Why COMMIT was chosen.** Not documented in the ledger. Candidates to check in the history:
  `ddl_command_end` fires inside the DDL statement, where the partition is already attached but
  reading a large partition holds the DDL's `ACCESS EXCLUSIVE` lock on the parent for the length
  of the read; a transaction that attaches then detaches the same table nets to nothing at
  COMMIT; an IMMEDIATE apply inside an event trigger runs under the DDL's snapshot.
- **DEFERRED staging.** A staged delta is a set of rows in `__reflex_delta_<source>`; staging a
  whole partition copies it. Whether to stage the rows, or a marker "partition X added" that the
  flush expands, and how that marker nets against later staged writes to the same rows.
- **`DROP TABLE` of a partition.** In `sql_drop` the rows are gone; the DELETE delta has to be
  taken in `ddl_command_start` (no object list for DROP there) or the drop has to be refused /
  turned into detach-then-drop. This is the open S1 report above and the hardest case.
- **Swaps.** DETACH + ATTACH of a replacement with the same bound in one transaction is the
  load-per-partition pattern and is correct today for the direct IMVs; applied at DDL time it
  becomes a full DELETE delta followed by a full INSERT delta of the slice, which is slower than
  today's slice rebuild. See `2026-10-12_partition_swap_full_reconciles_ignoring_dependent_chain.md`
  and `2026-07-28_full_reconcile_swaps_every_partition_and_cascades.md`.
- Partition-mirroring IMVs keep their own path (the mirror partition is attached / dropped);
  whether their dependents should be fed at DDL time too is the detach report above.

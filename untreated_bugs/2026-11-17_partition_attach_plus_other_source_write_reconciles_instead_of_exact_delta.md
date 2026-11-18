# A partition attach plus a write to another join source in one transaction reconciles instead of applying an exact delta — perf (correct, not incremental)

Residual of `2026-10-10_partition_change_and_write_to_another_join_source_in_one_transaction_double_counts.md`,
closed for correctness by task 5 (branch `fix/two-source-statement-recompute`). The partition flush now
takes the documented `force_reconcile` branch when another source of the IMV was written in the
same transaction. The result is right, but the whole IMV is rebuilt. That is a DDL-only, rare shape,
so the reconcile was kept for this release.

## Reproduction (any PG)

```sql
CREATE TABLE s(id INT NOT NULL, g TEXT NOT NULL, v INT) PARTITION BY LIST (g);
CREATE TABLE s_pa PARTITION OF s FOR VALUES IN ('a');
CREATE TABLE d(g TEXT PRIMARY KEY, label TEXT);
INSERT INTO s VALUES (1,'a',1); INSERT INTO d VALUES ('a','aa');
SELECT create_reflex_ivm('join_agg',
  'SELECT d.label, COUNT(*) AS c, SUM(s.v) AS sv FROM s JOIN d ON d.g = s.g GROUP BY d.label');
CREATE TABLE t_j (LIKE s INCLUDING ALL); INSERT INTO t_j VALUES (10,'j',2),(11,'j',3);
BEGIN;
ALTER TABLE s ATTACH PARTITION t_j FOR VALUES IN ('j');
INSERT INTO d VALUES ('j','jj');
COMMIT;  -- correct (jj,2,5), via a full reconcile of join_agg
```

Severity: performance only. The cost is a full rebuild of the IMV instead of `|P ⋈ ΔX|` work.

## Exact design for attach-then-write (not built)

While a partition P of source S has a pending (not yet flushed) attach, the delta of any other source X
of the IMV must join against `S` without P: substitute `(SELECT * FROM S WHERE tableoid <> P)`, or
the union of S's other leaves, for S in X's delta. The flush then applies `P ⋈ X_now` as S's delta.
Each joint pair (P row, ΔX row) is then counted exactly once, by the flush. That is exact because P is
a whole relation: either all of its rows are in the pending attach, or none are.

- Cost: one pending-queue lookup per delta of X, gated by S's existing pending guard setting.
  It does no work for IMVs without a pending attach.
- Several pending attaches on S: filter all of them (`tableoid NOT IN (...)`).
- Writes to P itself after the attach, in the same transaction: those already make the flush reconcile
  (`source_written_in_this_transaction`), and that must stay.

## Why the detach and DEFERRED variants cannot be exact

- **Detach-then-write X.** Once detached, P's rows are gone from S, so X's later delta never sees
  them. That part is right. But the flush must retract `P ⋈ X_at_detach`, and X's earlier-statement
  transition rows, which it would need to rebuild X as of the detach, are gone at flush time. Without
  stashing every delta of X for the rest of the transaction, there is no exact retraction.
- **DEFERRED IMVs.** All deltas, the partition flush included, are applied at COMMIT from the
  accumulated pending rows. The interleaving of the attach and the other sources' writes is lost by
  then, so the "who saw whom" information the exact correction needs no longer exists.

Both stay on the reconcile.

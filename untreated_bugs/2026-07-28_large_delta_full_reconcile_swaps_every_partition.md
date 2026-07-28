# 2026-07-28 — a large delta escalates to a full partitioned reconcile that swaps EVERY child, taking `AccessExclusive` on the IMV root

**Status: untreated. PRE-EXISTING — reproduces identically on `main` @ `2f8b786`.
Not caused by, and not fixed by, the IMV-root AccessExclusive work.**

## Symptom

A single large `INSERT` into a partitioned IMV's source freezes every reader of
the IMV — including readers pruning to a completely unrelated partition — for
the rest of the transaction. **No partition DDL is involved.**

## Reproduction (PG 17.7, pgrx instance, mirror depth 1)

```sql
CREATE TABLE s_src (k INT NOT NULL, v INT) PARTITION BY LIST (k);
CREATE TABLE s_src_1 PARTITION OF s_src FOR VALUES IN (1);
CREATE TABLE s_src_2 PARTITION OF s_src FOR VALUES IN (2);
INSERT INTO s_src SELECT 1, g FROM generate_series(1,300000) g;
SELECT create_reflex_ivm('s_imv','SELECT k, v FROM s_src','k,v',
                         NULL,NULL,NULL,ARRAY['k']);

BEGIN;
INSERT INTO s_src SELECT 2, g FROM generate_series(1,300000) g;   -- no DDL
-- observe pg_locks on s_imv from another session
```

Locks held by the writer on the IMV **root**, by delta size:

| rows inserted into an existing partition | lock modes on the IMV root |
|---|---|
| 1 000 | `RowExclusive` |
| 50 000 | `RowExclusive` |
| 300 000 | `RowExclusive`, `ShareRowExclusive`, `ShareUpdateExclusive`, **`AccessExclusive`** |

The escalation is announced in the server log:

```
INFO:  pg_reflex: reconciled IMV 's_imv' (partitioned, 2 children swapped)
```

## Mechanism

Above a delta-size threshold the maintenance path abandons the incremental
update and calls the partition-aware full `reflex_reconcile`, which rebuilds
**every** partition through `execute_partition_swap_for_child`. Each swap
`DETACH`es the old child from its immediate parent, and at mirror depth 1 that
parent **is the IMV root** — `AccessExclusive`, held to commit.

Note this swaps children that did not change at all: in the reproduction the
delta touches only `k = 2`, yet `s_imv_s_src_1` is swapped too.

The threshold is **relative to the base**, not absolute: 50 000 rows inserted
into a 300 000-row IMV does not escalate, while 50 000 into a 50 000-row IMV
does.

## Severity

**High availability impact, no correctness risk.** Data is correct throughout
(bidirectional `EXCEPT ALL` oracle: 0 mismatches in every run above). The
failure is that a routine bulk load blocks all readers of the IMV for the
remainder of the transaction, and the window scales with the load.

This is the *same end symptom* as
`2026-07-27_sync_partition_add_holds_accessexclusive_on_imv_root.md` and it
defeats that fix at volume: a create-partition-and-bulk-load transaction is now
lock-free on the partition-add path, but if the load is large enough relative to
the IMV it escalates here instead and the freeze returns. Measured on the fixed
branch, depth 1, base 50 000 + load 50 000: root `AccessExclusive` for 0.162 s,
unrelated-reader latency 116 ms. With base 300 000 + load 50 000 (no escalation)
the same shape shows no `AccessExclusive` at all and 13 ms reader latency.

## What was ruled out

* Not the partition-add path: reproduces with **no** DDL in the transaction.
* Not a regression: identical numbers on `2f8b786` and on the fix branch.
* Not the COMMIT-time partition flush: the lock appears on the `INSERT`
  statement itself, before `SET CONSTRAINTS ALL IMMEDIATE` runs.
* Not a DEFAULT-partition interaction: no default partition exists in the
  reproduction.

## Fix direction

1. **Scope the full reconcile to the partitions the delta actually touches.**
   Swapping unchanged children is pure waste and is what drags the root in.
2. **Reuse the fresh/empty in-place fill.** `execute_partition_swap_for_child`
   already fills a child in place when it is provably empty or provably created
   by the current transaction, which takes no lock on the parent. Extending that
   to "the reconcile is rebuilding this child wholesale anyway" would need the
   TRUNCATE-then-fill shape applied to non-fresh children — see the correctness
   note below.
3. **Or make the escalation itself cheaper to justify** — the threshold is a
   performance heuristic; at depth 1 it trades an incremental update for a
   whole-IMV reader freeze, which is rarely the better deal.

**Correctness note for whoever takes this.** TRUNCATE-then-fill-in-place is
semantically identical to the DETACH/ATTACH swap — the swap discards the old
child's contents wholesale too — but it takes `AccessExclusive` on the *child*,
which readers of THAT partition will feel. The swap's `AccessExclusive` lands on
the *parent* instead, which is strictly worse at depth 1 and strictly better for
readers of the swapped partition at depth ≥ 2. Any change here must be measured
at both depths, not just one.

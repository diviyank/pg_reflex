# 2026-07-28 — a sub-partition added to a newly-attached branch later in the SAME transaction is never mirrored, and the next INSERT aborts the user's transaction

**Status: untreated, mechanism CONFIRMED in code. PRE-EXISTING — verified RED on
`main` @ `2f8b786` and on the IMV-root AccessExclusive fix branch. Not a
regression from that work. Left unfixed deliberately: it is a separate defect
and belongs to a later batch, per `untreated_bugs/` hygiene.**

## Symptom

Attaching a new top-level source branch and then adding a sub-partition to that
branch **in the same transaction** leaves the sub-partition unmirrored. The next
INSERT that routes to it aborts the caller's transaction.

## Reproduction (PG 17.7, mirror depth 2)

```sql
CREATE TABLE zr4_src (k INT NOT NULL, d DATE NOT NULL, v INT) PARTITION BY LIST (k);
CREATE TABLE zr4_src_1 PARTITION OF zr4_src FOR VALUES IN (1) PARTITION BY RANGE (d);
CREATE TABLE zr4_src_1_m1 PARTITION OF zr4_src_1
    FOR VALUES FROM ('2025-01-01') TO ('2025-02-01');
SELECT create_reflex_ivm('zr4_imv','SELECT k, d, v FROM zr4_src',
                         'k,d,v', NULL, NULL, NULL, ARRAY['k','d']);

BEGIN;
CREATE TABLE zr4_src_5 (LIKE zr4_src INCLUDING ALL) PARTITION BY RANGE (d);
CREATE TABLE zr4_src_5_m1 PARTITION OF zr4_src_5
    FOR VALUES FROM ('2025-01-01') TO ('2025-02-01');
ALTER TABLE zr4_src ATTACH PARTITION zr4_src_5 FOR VALUES IN (5);   -- branch mirrored

CREATE TABLE zr4_src_5_m2 PARTITION OF zr4_src_5
    FOR VALUES FROM ('2025-02-01') TO ('2025-03-01');               -- NOT mirrored

INSERT INTO zr4_src VALUES (5, '2025-02-20', 8);
-- ERROR:  no partition of relation "zr4_imv_zr4_src_5" found for row
-- DETAIL:  Partition key of the failing row contains (d) = (2025-02-20).
```

## Severity

**Aborts the user's transaction.** No wrong data is produced — the failure is
loud — but a legitimate DDL sequence is rejected, and the whole enclosing
transaction (which may contain unrelated work) is lost. Reachable from the same
partition-rollover workflows that motivated the AccessExclusive report: build
next period's branch, attach it, then add a month to it.

## Mechanism — CONFIRMED in code

The `ddl_command_end` event trigger resolves the created partition's immediate
parent into `_parent`, then:

* `src/lib.rs:1148` resolves the partition ROOT into `_part_root` via
  `pg_partition_root(_parent::regclass)` — but uses it **only** for the
  `__reflex_partition_pending` enqueue;
* `src/lib.rs:1181` selects the IMVs to sync with
  `depends_on @> ARRAY[_parent]`, i.e. keyed off the **immediate parent**, not
  the resolved root.

For `zr4_src_5_m2` the immediate parent is `zr4_src_5`, which is not a
registered source — the IMV's `depends_on` names the root `zr4_src` — so no IMV
matches, no sync runs, and the mirror sub-partition is never created. The
branch-level ATTACH earlier in the same transaction matched only because its
immediate parent *was* the root.

The root is therefore already computed and in scope at the point the IMV lookup
runs; the lookup simply uses the wrong variable.

## Fix direction

Resolve the partition ROOT for the IMV lookup as well as for the pending
enqueue, so a sub-partition added at any depth syncs the IMVs that depend on the
root. Guard against the pg_reflex-owned relations already excluded from the
enqueue path.

**Test to pin it:** the reproduction above, asserting that the INSERT succeeds
and that `assert_imv_correct` holds afterwards — plus the same shape one level
deeper, so the fix is not special-cased to depth 2.

## What was ruled out

* Not a regression: identical failure on `2f8b786`.
* Not the detached-build/ATTACH change: the branch itself mirrors correctly; it
  is only the later sub-partition that is missed.

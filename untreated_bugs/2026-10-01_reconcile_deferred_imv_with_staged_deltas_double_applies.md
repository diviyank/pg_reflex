# `reflex_reconcile` of a DEFERRED IMV in a transaction that already staged deltas for it double-applies them at COMMIT — S1 (silent wrong aggregate)

Pre-existing (not introduced by 1.11.5). Found in the 1.11.5 final review.

## Mechanism

A DEFERRED IMV's source writes are staged in `__reflex_delta_<src>` with a
`__reflex_deferred_pending` row, and applied by the COMMIT-time flush. `reflex_reconcile`
(src/reconcile.rs:924 → `reconcile_one`) rebuilds the target from the base tables, which
already contain the transaction's writes, but it neither consumes the staged rows nor records
a rebuild watermark. Only the COMMIT-time rebuild pass records one
(`INSERT INTO __reflex_deferred_reconciled_batch`, src/trigger/deferred.rs:582), and only an
IMV with a watermark has its earlier deltas skipped by the flush (deferred.rs ~1180). So at
COMMIT the flush applies the staged delta on top of a target that already reflects it.

- Aggregate IMV: the change is counted twice, `known_stale` stays FALSE (silent).
- Passthrough IMV with a key: the re-insert hits `__reflex_uk_<imv>` (23505) and the IMV is
  marked `known_stale` (loud).

Also reachable through any caller that reconciles mid-transaction: `reflex_rebuild_imv`,
`reflex_scheduled_reconcile` / `reflex_doctor` run in a transaction that wrote sources, a user
function that writes a source and then repairs.

## Reproduction (pg17, 1.11.5)

```sql
CREATE TABLE sa (k INT, g INT, v INT);
INSERT INTO sa SELECT i, i % 3, i FROM generate_series(1, 30) i;
SELECT create_reflex_ivm('ra', 'SELECT g, COUNT(*) AS n, SUM(v) AS s FROM sa GROUP BY g',
  NULL, NULL, 'DEFERRED');
BEGIN;
INSERT INTO sa VALUES (100, 0, 1000);
SELECT reflex_reconcile('ra');   -- RECONCILED
COMMIT;
-- ra: g = 0 shows n = 12, s = 2165; fresh query: n = 11, s = 1165; known_stale = FALSE
```

## Fix direction

When `reflex_reconcile` rebuilds a DEFERRED IMV, record the same watermark the COMMIT-time
pass records (command id + next xid, via the code at deferred.rs:574-590) so the flush applies
only the deltas staged after the rebuild — or, simpler, list the IMV in
`pg_temp.__reflex_deferred_rebuild` and let the COMMIT-time pass rebuild it once from the
final state. Test: the repro above (oracle after COMMIT) plus a write after the reconcile in
the same transaction (must be applied).

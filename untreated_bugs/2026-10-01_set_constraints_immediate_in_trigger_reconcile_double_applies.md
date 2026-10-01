# Under `SET CONSTRAINTS ALL IMMEDIATE`, a DEFERRED IMV refreshed from inside a statement's trigger can double-apply that statement's delta — S2 (silent wrong aggregate, opt-in mode)

Residual of the 1.11.5 fix for in-transaction reconciles of DEFERRED IMVs.

## Mechanism

A source's trigger maintains its IMMEDIATE IMVs first and stages the DEFERRED IMVs' delta
last (sql/deferred_trigger_body.plpgsql.in). A reconcile of a DEFERRED IMV D reached from
that IMMEDIATE maintenance (the high-selectivity dispatch refreshing its IGNORING dependents)
reads the statement's write before D's delta for it is staged. 1.11.5 therefore does not
rebuild D there: it lists D for the COMMIT-time pass and queues a flush request
(`reconcile_waits_for_commit` / `list_for_commit_reconcile`, src/trigger/deferred.rs). At a
real COMMIT that pass runs after every statement, so the delta staged later is skipped.

Under `SET CONSTRAINTS ALL IMMEDIATE` the queued flush request fires at the end of the INSERT
that queues it, i.e. still inside the IMMEDIATE maintenance loop: the pass rebuilds D and
records its watermark before the outer trigger stages D's delta, which is then applied on top.
Only when D also reads the table written by that statement. Before 1.11.5 the same shape
double-applied in every mode.

## Reproduction (pg17)

```sql
CREATE TABLE us (id INT PRIMARY KEY, g INT, w INT);
INSERT INTO us SELECT i, i % 3, i FROM generate_series(1, 12) i;
SELECT create_reflex_ivm('u', 'SELECT g, SUM(w) AS sw, COUNT(*) AS n FROM us GROUP BY g');
UPDATE __reflex_ivm_reference SET wipe_threshold = 0 WHERE name = 'u';
SELECT create_reflex_ivm('d', 'SELECT us.g, COUNT(*) AS n, SUM(us.w) AS sw, SUM(u.n) AS un
  FROM us LEFT JOIN u ON u.g = us.g GROUP BY us.g', NULL, NULL, 'DEFERRED', '!u');
UPDATE __reflex_ivm_reference SET wipe_threshold = 1e9 WHERE name = 'd';
BEGIN;
SET CONSTRAINTS ALL IMMEDIATE;
UPDATE us SET w = w + 1;
COMMIT;
-- d: every group's sw counts the +1 twice
```

## Fix direction

Stage the DEFERRED delta before maintaining the IMMEDIATE IMVs in the trigger bodies (needs
trigger regeneration in the migration), or have the COMMIT-time pass postpone a listed IMV while
a statement that may still stage for it is in flight.

## Ruled out (2026-10-02): postponing the pass while `pg_trigger_depth() > 1`

The premise that the outer statement's own flush event runs the pass at depth 1 is
false. Measured on pg17 with the reproduction above plus a logging AFTER ROW trigger on
`__reflex_deferred_pending`: under `SET CONSTRAINTS ALL IMMEDIATE` both events fire at
depth 2 — the `'TRUNCATE'` request queued by `list_for_commit_reconcile` (before the
statement stages `d`'s delta) and the statement's own `'UPDATE'` pending row (after
staging), because both INSERTs into `__reflex_deferred_pending` run inside the source's
statement trigger (depth 1) and an immediate constraint trigger fires at the end of that
nested INSERT. Without SET CONSTRAINTS the same events fire at COMMIT at depth 1. So a
depth gate postpones the good event too; the re-enqueued request is consumed at once by a
nested flush (depth 3) and nothing is left to rebuild `d` before COMMIT: `d` would end
flagged stale (not wrong), and every in-trigger reconcile under IMMEDIATE would regress
the same way. Remaining options: stage the DEFERRED delta before the IMMEDIATE
maintenance in the trigger bodies (template change + regeneration in the migration), or
tell the two depth-2 events apart (request row vs staging row, or "staged since listing").

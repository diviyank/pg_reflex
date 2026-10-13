# `reflex_reconcile` of a partition-mirroring IMV whose source has a DEFAULT partition and no other raises `syntax error at or near ")"` — S2 (loud; the remedy the audit prints for such an IMV cannot run)

Found on pg16 while testing the audit remedies of the 1.12.0 integration branch
(`fix/full-review-r3`). Identical on `0324f47`, the branch base; 1.11.5 was not run. Not caused by
the fix round and not fixed there.

## Reproduction (pg16)

```sql
CREATE TABLE dd(id INT NOT NULL, g TEXT NOT NULL, v INT) PARTITION BY LIST (g);
CREATE TABLE dd_p1 PARTITION OF dd DEFAULT;
INSERT INTO dd VALUES (1,'a',1),(2,'a',2);
SELECT create_reflex_ivm('dd_v', 'SELECT g, SUM(v) AS sv FROM dd GROUP BY g');  -- mirrors partition column g
SELECT reflex_reconcile('dd_v');
-- ERROR:  syntax error at or near ")"
```

## Expected vs actual

Expected `RECONCILED`. Actual: the raw PostgreSQL error above (`scan.l`), in a plain call and inside
a `DO` block; the caller's transaction aborts. Create succeeds and the audit is clean after it;
writes and TRUNCATE were not exercised on this shape. Every finding whose remedy is
`reflex_reconcile('<imv>')` (among them the `trigger-attached` remedy) cannot be applied to such an
IMV.

## What was ruled out

- Not the name length: a 55-character source with a `FOR VALUES IN ('a')` partition reconciles.
- Not DEFAULT as such: the same source with a second partition `FOR VALUES IN ('z')` beside the
  DEFAULT one reconciles (`RECONCILED`).
- Not the trigger changes of the fix round: same error on `0324f47`.

Probe `zz_file_probe` (`.superpowers/sdd/2026-10-07-1.12.0-create-contract-hardening/full-review-r3-fix-evidence/b-r3fix-file-probe.rs`, outputs
`b-r3fix-fp-{base,head}.log`, lines `DEFAULT-only` and `DEFAULT and one LIST partition`).

## Mechanism

Not established. The statement that fails was not captured. Hypothesis: the slice predicate of a
DEFAULT mirror partition is built as the negation of its siblings' bounds, and with no sibling the
list is empty (`NOT IN ()` or `NOT ()`).

## Fix direction (hypothesis to falsify)

Capture the statement (`SET log_min_error_statement = error` on the worker, or run the generated
SQL of `reflex_reconcile_partition` by hand), then make the predicate of a DEFAULT node with no
sibling `TRUE`. Test: the fixture above, `reflex_reconcile`, `reflex_reconcile_partition` and a
TRUNCATE of the partition, IMMEDIATE and DEFERRED, with the bidirectional oracle; and the same at
the second level (a branch whose only leaf is DEFAULT).

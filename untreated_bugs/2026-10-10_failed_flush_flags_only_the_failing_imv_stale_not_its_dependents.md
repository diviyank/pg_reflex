# When a DEFERRED flush fails, only the failing IMV is flagged `known_stale`; the IMVs built on it are wrong and not flagged — S2 (wrong result without a flag)

Pre-existing: identical on `6946400` (1.11.5) and on the 1.12.0 integration branch at `0324f47`.
Found by review seat R3 of the 1.12.0 full re-review (pg16; `*/r3_commit.txt` steps 14 to 17 under
`.superpowers/sdd/2026-10-07-1.12.0-create-contract-hardening/full-review-r3-out/`).
The reduction below was run on pg16 on `0324f47` and on `fix/full-review-r3` (probe `zz_file_probe`, kept at `.superpowers/sdd/2026-10-07-1.12.0-create-contract-hardening/full-review-r3-fix-evidence/b-r3fix-file-probe.rs` with its outputs `b-r3fix-fp-{base,head}.log`): same outcome on both.

## Reproduction (pg16, every statement committed, all IMVs DEFERRED)

```sql
CREATE TABLE a(id INT PRIMARY KEY, k INT NOT NULL, v INT NOT NULL);
INSERT INTO a SELECT i, i % 4, i FROM generate_series(1, 12) i;
SELECT create_reflex_ivm('c1', 'SELECT k, SUM(v) AS sv, COUNT(*) AS c FROM a GROUP BY k', NULL, NULL, 'DEFERRED');
SELECT create_reflex_ivm('c2', 'SELECT COUNT(*) AS n, SUM(sv) AS t FROM c1', NULL, NULL, 'DEFERRED');
ALTER TABLE c1 ADD CONSTRAINT nope CHECK (sv < 1000);   -- makes the flush of c1 fail
INSERT INTO a VALUES (71, 1, 5000);                      -- COMMIT succeeds
```

## Expected vs actual

The COMMIT succeeds and `c1` is flagged `known_stale` (by design: a failing flush does not abort
the user's transaction); it misses the row: `(1,15,3)` for `(1,5015,4)`. `c2`, which reads `c1`, is
as wrong (`(4,78)` for `(4,5078)`) and carries no flag: `reflex_ivm_status`, the audit and a
monitor reading `known_stale` see one stale IMV where the whole chain under it is stale.

After `ALTER TABLE c1 DROP CONSTRAINT nope; INSERT INTO a VALUES (73, 3, 1);` in one transaction,
`c1` is still flagged and still misses row 71, and `c2` is still wrong and still not flagged (the
new row reached both). The review reports that the next successful flush repaired both in its
fixture; in this reduction it did not, and a `reflex_reconcile` is what an operator has to run. So
the flag on `c1` is the only signal that the chain needs it.

## What was ruled out

Not specific to the statement that fails: the write after the obstacle is removed is applied to
both IMVs.

## Fix direction (hypothesis to falsify)

Where the flush flags an IMV stale after a failure (`src/trigger/deferred.rs` and the partition
flush), flag every enabled IMV that reaches it through `depends_on` with a reason naming the
failing ancestor, and clear those flags when the ancestor's next flush or reconcile succeeds (the
cascade already visits them). Test: the chain above plus a dependent of a dependent, flags set
after the failing COMMIT and cleared after the repairing one.

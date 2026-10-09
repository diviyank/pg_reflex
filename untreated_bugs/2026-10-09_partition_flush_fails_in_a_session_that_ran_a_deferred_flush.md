# Adding a partition in a session that already ran a DEFERRED flush fails the partition flush: `"__reflex_new_<source>" is not a table` — S2 (flagged stale; wrong data until reconciled)

Pre-existing; reproduced on pg16 with the 1.12.0 fold branch `fix/fold-g8a-deferred-partition`
with its truncate-trigger sync disabled (same outcome), so it is not caused by that branch.

## Reproduction (pg16, ONE session, every statement committed)

```sql
CREATE TABLE s(id INT NOT NULL, g TEXT NOT NULL, v INT) PARTITION BY LIST (g);
CREATE TABLE s_pa PARTITION OF s FOR VALUES IN ('a', 'b');
CREATE TABLE s_pd PARTITION OF s DEFAULT;
INSERT INTO s VALUES (1,'a',1),(2,'a',2),(3,'b',5),(6,'p',7);
SELECT create_reflex_ivm('x', 'SELECT g, COUNT(*) AS c, SUM(v) AS sv FROM s GROUP BY g',
                         NULL, NULL, 'DEFERRED');
INSERT INTO s VALUES (203,'a',3);                 -- COMMIT runs a DEFERRED flush in this session
CREATE TABLE s_pf (LIKE s INCLUDING ALL);
INSERT INTO s_pf VALUES (211,'f',1),(212,'f',2);
ALTER TABLE s ATTACH PARTITION s_pf FOR VALUES IN ('f');   -- one transaction with the two above
```

## Expected vs actual

Expected: `x` holds group `f`. Actual: `x` has no group `f`; `__reflex_partition_pending` keeps
the root with `failures = 1`, `last_error = "__reflex_new_s" is not a table`; every DEFERRED IMV
of `s` is flagged `known_stale` with that reason. The same three statements in a session that ran
no DEFERRED flush are correct, and IMMEDIATE IMVs are correct either way.

Also seen in the same state (one session, a DEFERRED flush earlier):
- `CREATE TABLE s_pi PARTITION OF s ...; INSERT INTO s ...` in one transaction: data correct, all
  DEFERRED IMVs flagged stale with the same reason;
- a sub-partition created under an existing branch
  (`CREATE TABLE s_pc3 PARTITION OF s_pc FOR VALUES FROM (200) TO (300)`): the mirror leaf of an
  auto-partitioned keyed passthrough is not created, and the next `INSERT` into that range fails
  its DEFERRED flush (`no partition of relation "<imv>_s_pc" found for row`, SQLSTATE 23514, "The
  staged delta was discarded"): rows missing from the IMV, flagged stale;
- `DETACH PARTITION` of a non-empty partition: wrong afterwards in the same sequence (not
  isolated from the earlier failure).

## What is established and what is not

Established: the outcomes above, and that a new session per transaction makes the whole matrix of
`src/tests/pg_test_v112_deferred_partition.rs` correct (its DEFERRED "later partitions" test
reconnects before each step for this reason). Not established, but the likely cause: the DEFERRED
flush leaves session-level `TEMP VIEW`s named `__reflex_new_<source>` / `__reflex_old_<source>`
(`src/trigger/deferred.rs`, `CREATE OR REPLACE TEMP VIEW`), and `reflex_apply_partition_delta`
(bootstrap SQL in `src/lib.rs`) then tries to create and `TRUNCATE` a temp TABLE of the same name
for the partition's rows; `TRUNCATE` of a view raises `is not a table`.

## Fix direction (hypothesis to falsify)

Give the partition delta its own relation name, or drop the flush's temp views when the flush
ends. `reflex_apply_partition_delta` lives in the bootstrap DDL, so the change needs its migration
twin. Pin with the reproduction in one `dblink` session (IMMEDIATE and DEFERRED IMVs on the same
source), then remove the per-step reconnect of the DEFERRED later-partitions test.

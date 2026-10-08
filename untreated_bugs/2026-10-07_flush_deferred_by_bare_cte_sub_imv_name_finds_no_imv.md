# `reflex_flush_deferred('<imv>__cte_<alias>')` reports `NO DEFERRED IMVS`: the outer IMV registers its CTE sub-IMV source double-quoted — S3 (manual flush only; COMMIT path not examined)

Observed on the 1.12.0 branch (pg15) while writing the DEFERRED oracle tests of
`src/tests/pg_test_v112_cte_misc.rs`. Whether it predates the branch was not checked; the
registered dependency was already the quoted spelling before the CTE reference rewrite changed.

## Reproduction (pg15, inside one transaction)

```sql
CREATE TABLE t(id SERIAL PRIMARY KEY, season TEXT, v INT);
INSERT INTO t(season, v) VALUES ('e',1),('y',2),('e',3);
SELECT create_reflex_ivm('v', $$WITH e AS (SELECT season, SUM(v) AS s FROM t GROUP BY season)
  SELECT season, s FROM e WHERE season <> 'x'$$, NULL, NULL, 'DEFERRED');
INSERT INTO t(season, v) VALUES ('e',10),('z',30);
SELECT reflex_flush_deferred('t');          -- FLUSHED 1 DEFERRED OPERATIONS; v__cte_e is now e:14,y:2,z:30
SELECT reflex_flush_deferred('v__cte_e');   -- NO DEFERRED IMVS; v still holds e:4,y:2
SELECT reflex_flush_deferred('"v__cte_e"'); -- flushes; v is correct
```

The registry row of `v` reads `depends_on = {"\"v__cte_e\""}`: the source name carries its
double quotes.

## Expected vs actual

Expected: flushing a source by its plain name reaches every DEFERRED IMV that reads it. Actual:
only the double-quoted spelling of the sub-IMV name matches the registered dependency, so the
plain name reports that nothing depends on it and the outer IMV stays stale until something
flushes it under the quoted name.

## What is established and what is not

Established: the three return values and the IMV contents above, in one transaction (no COMMIT).
Not established: which name the COMMIT-time constraint trigger passes, so whether a committed
transaction is affected at all (the existing DEFERRED CTE tests flush only sources that the
outer query reads directly); whether `reflex_flush_deferred` should canonicalise its argument or
the dependency should be registered unquoted.

## Fix direction (hypothesis to falsify)

First check the COMMIT path with a real committed transaction. If it is correct, make
`reflex_flush_deferred` compare source names by resolved identity rather than by spelling, with
an oracle test flushing a chained DEFERRED CTE by the plain sub-IMV name.

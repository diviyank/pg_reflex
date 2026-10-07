# `"s".t AS t` (quoted schema with a redundant bare alias) fails loudly at the first DML — S3 (loud)

Pre-existing (the same before and after the 1.12.0 source-rewrite item for mixed quoting).
Observed during the 1.12.0 hardening work on the `release/1.12.0` branch (base commit 6946400;
Task 1b probe branch based on c71c65d), on pg15. Found by the Task 1b implementer ("found and not
fixed", item 3). Nothing wrong is stored; the failure is raised on the user's write.

## Reproduction (pg15)

Fixture used for all the Task 1b probes: schema `s`, table `t(id SERIAL PRIMARY KEY, season TEXT,
v INT)` seeded `('e',1),('y',2),('e',3)`. The recorded fragment is `FROM "s".t AS t WHERE t.v > 0`;
the surrounding select list below is illustrative, because the probe's exact select list is not
recorded in the sources.

```sql
CREATE SCHEMA s;
CREATE TABLE s.t(id SERIAL PRIMARY KEY, season TEXT, v INT);
INSERT INTO s.t(season, v) VALUES ('e',1),('y',2),('e',3);
SELECT create_reflex_ivm('v', 'SELECT t.season, SUM(t.v) AS s FROM "s".t AS t WHERE t.v > 0 GROUP BY t.season');
INSERT INTO s.t(season, v) VALUES ('e',10);
-- ERROR:  invalid reference to FROM-clause entry for table "__reflex_new_s_t"
```

The report says the same happens for `"s"."t" AS t` and `s."t" AS t`.

## Expected vs actual

Expected: the first DML maintains the IMV (an alias equal to the table's bare name is legal SQL).
Actual: the delta query is rewritten to read the transition table, the `AS t` alias is not
stripped, and the references `t.v` no longer resolve.

## What is known about the mechanism

`strip_redundant_bare_alias` (`src/sql_writer/identifier.rs`) only matches the spelling the
source is registered under, and takes a single pattern, so it is not a one-line addition to make
it handle the quoted spellings the rewrite now recognises. Not investigated further.

## Fix direction (hypothesis)

Have `strip_redundant_bare_alias` iterate the same list of spellings the rewrite uses
(`rewritten_source_spellings`), with the literal-skipping scan, so the alias is stripped for every
spelling the rewrite substitutes. Add oracle tests for the three quoted spellings with `AS t`,
and with an alias that differs from the table name as a control.

# A `public` table named by a PostgreSQL keyword, read as `zone` by one IMV and `"zone"` by another: the second is never maintained — S1 (silent)

Residual of the `t` / `"t"` bug (G1 joins review, P1). Pre-existing; the 1.12.0 identifier-quote
step closes it for every name that is not a keyword. Reproduced on the fix-round-2 head, pg17.

## Reproduction (pg17)

```sql
CREATE TABLE zone(id SERIAL PRIMARY KEY, season TEXT, v INT);
INSERT INTO zone(season, v) VALUES ('e',1),('y',2),('e',3);
SELECT create_reflex_ivm('a', 'SELECT season, SUM(v) AS s FROM zone GROUP BY season');
SELECT create_reflex_ivm('b', 'SELECT season, COUNT(*) AS c FROM "zone" GROUP BY season');
-- depends_on: a = {zone}   b = {"zone"}
INSERT INTO zone(season, v) VALUES ('e',10),('n',30);
-- a is correct. b still holds the rows of its creation: 3 rows differ from its query. No error.
```

`zone` is an unreserved keyword (`pg_get_keywords()`). Any keyword class behaves the same:
`data`, `name`, `value`, `type`, `key`, `role`, `year`, `location`, `version`. Not affected: a
table outside `public` (registered as `schema.table` from the catalog, whatever the caller
wrote: checked with `s.data` and `s."data"`), and every non-keyword name. Not checked: dropping
`a` (for a non-keyword name before 1.12.0 this removed the source triggers and left `b`
unmaintained as well), DEFERRED, passthroughs.

## Expected vs actual

Expected: one relation, one registered source, both IMVs maintained. Actual: two registered
spellings; the source triggers are installed once per table and their body selects the IMVs
with `'<spelling>' = ANY(depends_on)`, so the IMV under the other spelling is never reached.

## What was tried

- Fix round 1 adopted the spelling another IMV had already registered. Withdrawn: it gave an
  adopting LEFT JOIN passthrough an unsound key, left a later IMV unmaintained when the first
  one named both spellings, and made the rewrite hit a column named like the table.
- Fix round 2 removes, in the caller's query, the double quotes PostgreSQL would not write
  (`src/create_ivm/identifier_quotes.rs`). A keyword keeps its quotes on purpose: an unreserved
  keyword is not accepted bare in every position (a column label without `AS`), and
  pg_reflex's own text scanners read some bare keywords as syntax. Removing them only where
  the word names a relation needs a rewrite of the parsed query, not of its tokens.

## Fix direction (hypothesis)

Either unquote a keyword where the parsed query shows it names a relation or qualifies a
column (table factors and compound-identifier prefixes), or stop matching sources by text:
have the trigger body select IMVs by the relation the trigger fired on (`TG_RELID` against the
`regclass` of each `depends_on` entry). Add the oracle cells of `v112_jr_sp_*` and
`v112_jr_drop_*` for a keyword-named table.

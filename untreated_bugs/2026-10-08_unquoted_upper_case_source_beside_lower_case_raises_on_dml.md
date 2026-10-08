# A table written in unquoted upper case beside its lower-case spelling makes every write to it raise — S2 (loud, blocks writes)

Pre-existing (same on `903efbc` and on the G1 head `8ef9743`). Found by the G1 (joins)
adversarial review, probe cells `sjcase` and `bare_then_upper`, pg17. A single IMV written
`FROM T` works (`casesingle`).

## Reproduction (pg17)

```sql
CREATE TABLE t(id SERIAL PRIMARY KEY, season TEXT, v INT, w INT);
INSERT INTO t(season, v, w) VALUES ('e',1,10),('y',2,20),('e',3,30);

-- (a) one query, both spellings
SELECT create_reflex_ivm('v', 'SELECT a.season, COUNT(*) AS c, SUM(b.v) AS s
  FROM T a JOIN t b ON a.season = b.season GROUP BY a.season');
INSERT INTO t(season, v, w) VALUES ('e',10,1);
-- ERROR:  relation "__reflex_new_t" does not exist

-- (b) two IMVs, the second in upper case
SELECT create_reflex_ivm('v1', 'SELECT season, SUM(v) AS s FROM t GROUP BY season');
SELECT create_reflex_ivm('v2', 'SELECT season, SUM(w) AS s FROM T GROUP BY season');
INSERT INTO t(season, v, w) VALUES ('e',10,1);
-- ERROR:  relation "__reflex_new_T" does not exist
```

## Expected vs actual

Expected: `T` and `t` are the same relation for PostgreSQL, and both IMVs are maintained.
Actual: the registry holds two sources, `T` and `t` (`depends_on`), the write raises, and no
write to the table succeeds until one IMV is dropped.

## What is known about the mechanism

- A source is registered under the spelling of the caller's query when it lives in `public`
  (`canonicalize_analysis_sources`, `src/create_ivm/soundness.rs`). Since 1.12.0 the caller's
  query first loses the double quotes PostgreSQL itself would not write
  (`src/create_ivm/identifier_quotes.rs`, applied in the `create_reflex_ivm*` entry points), so
  `"t"` and `t` are one source. Case is not folded: an unquoted `T` is kept as written, because
  the transition-table rewrite is case-sensitive and must find the spelling the stored query
  holds.
- The error names a transition table built from one spelling (`__reflex_new_T`, or
  `__reflex_new_t` in the single-query form), which the firing trigger does not declare. Which
  trigger fires for which registered spelling was not traced.
- Possibly related: `2026-10-01_mixed_case_names_rejected_by_create_reflex_ivm.md`.

## Fix direction (hypothesis)

Fold an unquoted spelling the way PostgreSQL does before registering it, and make every
consumer of the registered name (`token_names_source`, `rewritten_source_spellings`,
`replace_source_with_transition`) match unquoted references case-insensitively. Add oracle tests
for `T`, `t`, `"t"` in one query and across IMVs, and for a table whose real name is mixed case.

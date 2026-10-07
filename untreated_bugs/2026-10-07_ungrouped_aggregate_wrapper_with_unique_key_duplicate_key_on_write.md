# Ungrouped aggregate inside a wrapper, created with a unique key, raises a duplicate key on every source write — S2 (LOUD, blocks writes)

Pre-existing: same outcome on the 1.11.5 base and on the 1.12.0 branch unless stated. Found by the 1.12.0 whole-branch review; fixture and steps are the probes it ran (pg17). Extends `2026-10-07_aggregate_spellings_become_keyless_passthrough_full_refresh.md`
(the keyless form is correct).

## Reproduction

```sql
SELECT create_reflex_ivm('v', 'SELECT COUNT(DISTINCT v) IS NULL AS e FROM s', 'e');
INSERT INTO s(g, v) VALUES ('a', 1);
-- ERROR: duplicate key value violates unique constraint "__reflex_uk_v"
```

## Expected vs actual

Expected: either the create is refused, or writes succeed. Actual: the IMV is created as a
passthrough with a key and every write to `s` fails. With GROUP BY the same expression is a raw
PostgreSQL error at create with nothing left behind.

## Fix direction (hypothesis to falsify)

An aggregate nested in an expression kind the analyzer does not descend into (IS NULL, BETWEEN,
IN) is not seen as an aggregate, so the query is planned as a row-level passthrough. Detect any
aggregate anywhere in the SELECT list from the AST and refuse the key (or the definition) when
the planner would treat the query as row-level.

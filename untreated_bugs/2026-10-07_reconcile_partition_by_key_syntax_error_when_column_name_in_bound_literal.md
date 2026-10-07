# `reflex_reconcile_partition(view, key)` raises a syntax error when the partition column's name occurs inside a bound literal — S2 (LOUD)

Pre-existing: same outcome on the 1.11.5 base and on the 1.12.0 branch unless stated. Found by the 1.12.0 whole-branch review; fixture and steps are the probes it ran (pg17).

## Reproduction

A LIST-partitioned source on column `k` with a partition `FOR VALUES IN ('a k b')`, a passthrough
IMV over it, then:

```sql
SELECT reflex_reconcile_partition('v', 'a k b');
-- ERROR: syntax error at or near "a"
```

Also (1.12.0 branch only): column `t`, partitions `'Été'`, `'Hiver'` and DEFAULT, key `'Été'`:
`syntax error at or near "Été"`. Reconciling by `source_partition` works.

## What is established and what is not

Established: the two errors. Read from the code, not proven: `substitute_identifier_ci`
(`src/sql_writer/identifier.rs`, called from `src/partition.rs`) tokenises on ASCII words and
does not skip string literals or treat a multi-byte neighbour as part of a word. 1.12.0 made the
source-name rewriter literal-aware; this rewriter was not changed. Hypothesis only: the same
function feeds `rebuild_diff::qualified_on`, where a mangled literal would stay valid SQL and
match nothing; no input reaching that was found.

## Fix direction (hypothesis to falsify)

Give `substitute_identifier_ci` the literal-skipping and multi-byte-neighbour rules the source
rewriter has, with a differential test proving unchanged output for input without quotes.

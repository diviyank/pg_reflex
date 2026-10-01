# Rebuild diff: a temp table per rebuild, and the keyless diff sorts the whole target twice — S3/S4 (perf, 2PC)

Found in the 1.11.5 release-candidate review (M2, M3). Correct results; cost and one
compatibility limit. Parked: no fix in 1.11.5.

## M2 — one temporary table created and dropped per rebuild (S3: 2PC incompatibility, S4: catalog churn)

`rebuild_target_rows` (src/rebuild_diff.rs:142) stages the rebuild query in
`CREATE TEMP TABLE __reflex_rb_<random> ON COMMIT DROP AS …` (src/rebuild_diff.rs:184) and
drops it at the end (src/rebuild_diff.rs:208). Every rebuild of an IMV with dependents —
`reflex_reconcile`, the wipe dispatch, trigger-side full refreshes
(`reflex_rebuild_target_rows`), every populated leaf of a partitioned rebuild — therefore:

- inserts and deletes rows in `pg_class`, `pg_attribute`, `pg_type` (and their indexes): a
  partitioned rebuild of N populated leaves churns the catalog N times, bloating it under
  frequent trigger-side full refreshes (FULL JOIN / ungrouped-aggregate fallbacks run one per
  statement on the source);
- marks the transaction as having touched temporary objects, so `PREPARE TRANSACTION` fails
  with `cannot PREPARE a transaction that has operated on temporary objects` (verified on
  pg16). Documented in CHANGELOG 1.11.5 Known limits and upgrading.md.

Reproduction (2PC): an IMV `up` with a dependent `dep`;
`BEGIN; SELECT reflex_reconcile('up'); PREPARE TRANSACTION 'x';` → the error above.

Fix directions:
- Reuse one per-session staging table per target shape (`CREATE TEMP TABLE IF NOT EXISTS …`
  + `TRUNCATE`) — removes the churn, not the 2PC limit.
- Stage in a CTE / subquery instead of a table: the keyed diff reads the staged rows three
  times (DELETE / UPDATE / INSERT) and the duplicate-key check once, so the rebuild query would
  run four times; acceptable only when the rebuild is cheap. A single `MERGE … WHEN NOT
  MATCHED BY SOURCE` (PG17+) would read it once but needs PG17 and a NULL-safe join (see the
  1.11.5 keyed diff: plain `=` plus a NULL-key array branch).
- A regular unlogged staging table owned by the extension (per-backend rows keyed by pid)
  would lift the 2PC limit at the cost of WAL-free but shared storage and cleanup.

## M3 — the keyless (whole-row) diff sorts the whole target twice (S4)

`apply_whole_row_diff` (src/rebuild_diff.rs:380) numbers duplicates with
`row_number() OVER (PARTITION BY ROW(x.*)::text)` over the old rows and over the staged rows,
and builds the numbered old-rows subquery twice (once for the DELETE, once for the INSERT,
src/rebuild_diff.rs:387-407). Each window needs a sort on the row's text form, so a keyless
rebuild sorts the full target twice and the staged rows twice, every value rendered to text —
O(n log n) with a large constant, and `work_mem` overflow spills to temp files on large
targets. Used whenever the target has no usable key (no unique index, a nullable array key
column, a second unique / exclusion index, or a column-set mismatch).

Fix directions:
- Number each side once into the staging step (e.g. stage `(k, rn)` for the old rows in a
  second temp table, or compute the multiset difference with
  `SELECT k, count(*) … GROUP BY k` on both sides — a hash aggregate instead of two sorts —
  then delete / insert `count difference` copies per k).
- Hash the row text (`md5(ROW(x.*)::text)`) as the grouping key to shrink sort / hash width,
  keeping the full text for the equality check.
- Measure first on a 1M-row keyless IMV with dependents: the keyed path is the common case
  (`__reflex_uk_*` exists whenever `unique_columns` is set or inferred).

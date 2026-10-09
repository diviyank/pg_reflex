# A source that is joined and also read by a HAVING subquery is listed twice in `depends_on` — S4 (no wrong result observed)

Pre-existing: same registry content on commit 8ef9743 and on the `fix/fold-g2-having` head, pg17.

## Reproduction

```sql
CREATE TABLE s(id SERIAL PRIMARY KEY, g TEXT, v INT);
CREATE TABLE o(id SERIAL PRIMARY KEY, g TEXT, k INT);
SELECT create_reflex_ivm('j',
  'SELECT s.g, COUNT(*) AS c FROM s JOIN o ON o.g = s.g GROUP BY s.g
   HAVING COUNT(*) > (SELECT MAX(k) FROM o)');
SELECT depends_on FROM public.__reflex_ivm_reference WHERE name LIKE '%j';
-- {s,o,o}
```

## Expected vs actual

Expected: each source once. Actual: `o` twice.

## What is established and what is not

Established: the duplicate entry. The IMV stays correct under INSERT, UPDATE and DELETE on both
tables, IMMEDIATE and DEFERRED (`v112_hv_sub_joined`, `v112_hv_sub_joined_def` in
`src/tests/pg_test_v112_having.rs`); a write to `o` recomputes this IMV in full, which is
idempotent. Not established: whether the duplicate makes a write to `o` process the IMV twice
(cost only), or misleads a consumer that counts the entries of `depends_on` (dependency graph,
drop, doctor).

## Fix direction (hypothesis to falsify)

Deduplicate the analysed source list before it is stored and before triggers are installed; check
the consumers of `depends_on` for an assumption that entries are unique.

# Shapes 1.12.0 recomputes in full on a write, or creates without a key, that have no report of their own — perf (new in 1.12.0, correctness unaffected or improved)

Each is a deliberate trade of a correctness fix; none was benchmarked. Sources, in the ledger
`.superpowers/sdd/2026-10-07-1.12.0-create-contract-hardening/`: `full-review-r1.md` §4 (pg17,
`0324f47` against `6946400`; `F` = the statement recomputes the IMV from its sources, measured by
the `xmin` of the target's rows), `fold-g8b2-report.md`, `fold-g67-review.md` N1,
`fold-g5-report.md`, `packaging-report.md` doubt 3. Grouped because the finding is the same for
all (an incremental path replaced by a full recompute, or a key no longer inferred, with no
measurement and no tracking record) and because the neighbouring shapes already have reports,
listed at the end.

| # | Shape | What now recomputes | Definition (from the probe source where one exists) | Source |
|---|---|---|---|---|
| 1 | a CTE referenced twice | every change of the CTE's sub-IMV recomputes the outer IMV | c05 `WITH r AS (SELECT region, SUM(amount) AS total FROM orders GROUP BY region) SELECT a.region, a.total, b.total AS other FROM r a JOIN r b ON a.region = b.region` | R1 §4 cause 3a (ruling "item G") |
| 2 | a passthrough with a declared key whose columns come from two tables | writes to either table | j49 `SELECT l.id AS line_id, o.id AS order_id, o.region, l.qty FROM order_lines l JOIN orders o ON o.id = l.order_id`, key `line_id, order_id`; r05 (key `id, manager`); o04, the sub-IMV of `SELECT DISTINCT ON (c.id) c.id, c.name, o.id AS last_order_id, o.created_at FROM customers c JOIN orders o ON o.customer_id = c.id ORDER BY c.id, o.created_at DESC, o.id` | R1 §4 cause 4. `6946400` was **wrong** on all three; with a key from one table nothing changed |
| 3 | a passthrough over a derived table, `FROM (SELECT id, … FROM t WHERE …) x` | loses its inferred key and is fully refreshed on each write unless the key is passed; per the packaging run it is **still recomputed when a key is passed** | not printed | `fold-g8b2-report.md` item 9 (`e78b34c`); `packaging-report.md` doubt 3 |
| 4 | a passthrough that reads a second table through a WHERE subquery | infers no key, so writes to the outer table recompute unless a key is passed | not printed | `packaging-report.md` doubt 3 |
| 5 | the primary key renamed in the select list (`c.id AS cid`, the remedy the alias-list refusal prints) | no key is inferred; pass the new name as the key | not printed | `packaging-report.md` doubt 3; documented in `docs/limitations/unsupported-shapes.md` |
| 6 | a mixed-case primary key column | never gives an inferred key | not printed | `fold-g5-report.md` ("Minor, pre-existing") |
| 7 | a column named like its table's alias (`FROM t v WHERE v > 0`), a whole-row function argument | recomputed in full | — | `fold-g8b2-report.md` item 5 (`e443d81`); the `COUNT(t)` half is `2026-10-08_value_named_like_source_table_recomputes_in_full.md` |
| 8 | a CTE named like a real table that the same body reads schema-qualified | a write to `public.o` is fully recomputed: `WITH o AS (SELECT season, SUM(v) AS s, COUNT(*) AS n FROM t GROUP BY season) SELECT o.season, o.s, r.k FROM o JOIN public.o r ON r.season = o.season` (`has_no_scoped_delta(base_query, 'public.o')` true on the head, false on the base; correct on both) | as shown | `fold-g67-review.md` N1 |
| 9 | a LEFT JOIN whose right operand is not a plain table and reads the source (derived table under a LEFT JOIN) | a write to that source | `dt_lj_*` cells, not printed | `fold-g1-report.md`: "such a write costs a full recompute. Not filed separately" |

## Expected vs actual

Expected of a 1.12.0 upgrade: the release notes name every shape whose write cost changed, and
each has a record to optimise against. Actual: the result is correct (for row 2 it was wrong on
1.11.5), but a view of one of these shapes over a large source now pays its full query on every
write, and for rows 3 to 6 the IMV is keyless unless the key is given.

## What is established and what is not

Established: rows 1 and 2 by the `xmin` comparison of R1 on both trees; row 8 by reading the
guard's answer on both trees; rows 3 to 5 by "one fixture each" in the packaging run (the
fixtures are not printed); rows 6, 7 and 9 by their authors' statements. Not established: any
timing; whether row 3's recompute under a declared key is intended (it contradicted the draft
release notes, which said the key avoids it).

## Already filed, not repeated here

`2026-10-07_subquery_table_write_recomputes_imv_in_full.md`,
`2026-10-07_right_join_imv_recomputed_in_full_on_every_write.md`,
`2026-10-10_update_of_unread_column_on_table_read_twice_recomputes_view.md`,
`2026-10-08_value_named_like_source_table_recomputes_in_full.md`,
`2026-10-07_aggregate_under_predicate_without_group_by_is_keyless_full_refresh.md`,
`2026-10-01_passthrough_dependent_without_key_mapping_full_refresh.md`.

## Fix direction (hypotheses to falsify)

Benchmark first: one large fixture per row, the cost of a single-row write before and after.
Then, by row: 1, count references per alias so that each reference of the CTE gets its own
scoped delta (the general fix of "a relation read twice"); 2, a key mapping per source for a
composite key; 3, keep the inferred key when the inner SELECT outputs the primary key under its
own name and calls no set-returning function (the finer rule the author left out); 5 and 6,
follow the key through a rename and through a quoted name; 8, do not count a CTE alias token as
a reference to a same-named table.

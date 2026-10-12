# An expression over aggregates whose type the type map has no name for is stored as TEXT — S2 (column type differs from the query; not executed on the branch)

Pre-existing by reading. Seen while fixing the aggregate classification on the 1.12.0 branch
(fix/fold-g5-classify), in a variant of the fix that was not merged.

## What was executed and what was reasoned

Executed (pg15.19, on the unmerged variant that maintains an aggregate under any expression
kind): `SELECT g, ARRAY[SUM(v), COUNT(*)] AS c FROM t GROUP BY g` was created with `c TEXT`; the
query returns `bigint[]` (`EXCEPT types text and bigint[] cannot be matched`). On the branch as
merged that query is rejected at create, so this reproduction is not reachable there.

Reasoned, not executed: the same path serves aggregate expressions that are accepted today.
`materialize_aggregate` (`src/create_ivm/mod.rs`) reads the type of a derived output column
through `augment_column_types_from_query`, whose `map_information_schema_type`
(`src/create_ivm/admin.rs`) turns every type it has no name for into `TEXT`. So
`SELECT g, MAX(ts) - MIN(ts) AS span FROM t GROUP BY g` (interval) or
`SELECT g, array_append(ARRAY[]::bigint[], SUM(v)) AS a FROM t GROUP BY g` would get a TEXT
column.

## Expected vs actual

Expected: the IMV column has the type the query returns. Actual (executed case): TEXT.

## Fix direction

For a derived mapping whose mapped type is the TEXT fallback, read the exact
`format_type(atttypid, atttypmod)` of the query's output column and use it as the mapping's cast
type (the unmerged variant did this and the array case then matched the query). Execute the
interval and `array_append` cases first.

## Executed cases met by later reviews (added 2026-10-12)

Seen by reviews of branches that were later merged; **not re-run on the release head**, where
`168d3d2` (types taken from PostgreSQL's description of the query) may have changed some of them.

- G2 re-review, P2 (pg17, same on `c3d4866`): `SELECT g, MAX(ts) - MIN(ts) AS x FROM s GROUP BY g`
  (and with `::interval`): end query `("__max_ts" - "__min_ts")::TEXT`; values equal as text,
  column type `text` instead of `interval`. `CAST(MAX(ts) - MIN(ts) AS interval day to second)`
  was a raw create error on the base and lands on the same outcome.
- G2 re-review, P3 (base and head): the typmod of a cast is dropped from the IMV column
  (`varchar(3)` becomes `text`, `numeric(10,2)` becomes `numeric`), for a top-level cast and for a
  group key; values equal. The non-regression review of the integrated tree (`0324f47`, R1 §5)
  later found `varchar(n)` / `char(n)` group keys with PostgreSQL's type, so the group-key half is
  probably closed.
- G5 review, pre-existing item 3 (identical on base and head): a constant beside GROUP BY
  (`INTERVAL '1 day' AS iv`, `ARRAY[1, 2] AS a`), and `'x' AS tag, 1.5 AS f` beside a window, are
  stored as text (the oracle fails with `EXCEPT types text and interval cannot be matched`). The
  full statements are not printed in the review.

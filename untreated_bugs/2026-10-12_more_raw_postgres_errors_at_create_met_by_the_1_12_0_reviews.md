# More definitions that fail at create with a raw PostgreSQL error instead of a tagged rejection (multi-column RANGE passthrough, a view or a temp table as source, a double quote in a group key, …) — S3 (loud at create)

Collected from the reviews of 1.12.0; each was reported as "raw, same on the base, not filed".
One report, as for `2026-10-10_raw_postgres_errors_at_create_for_five_definitions.md` and
`2026-10-07_five_untagged_raw_errors_at_create_found_by_g4_review.md`: the symptom and the
contract fix are the same (a `[reflex-unsupported]` rejection with a remedy, or support), the
causes are distinct, and each row can be closed on its own. Sources are in the ledger
`.superpowers/sdd/2026-10-07-1.12.0-create-contract-hardening/`.

**None of these was re-run on the release head.** Fixtures are not printed in the sources unless
shown; the "Definition" column is copied as written there.

| # | Definition | Error at create | Source (version; trees) |
|---|---|---|---|
| 1 | `SELECT id, v FROM rm`, `rm` the root of a table partitioned by RANGE on two columns (passthrough; with or without a WHERE) | `FROM must specify exactly one value per partitioning column` | `p3-ac-review.md` finding 11 (pg18; `d0a3e06` and `5d856e1`). The aggregate shape over the same root is maintained |
| 2 | an IMV whose source is an ordinary view (`… FROM some_vw`); a view over a partition | `"…_vw" is a view` / `"v_pa" is a view` | `fold-g5-review.md` pre-existing 7, `fold-g2-review.md` (pg16), `p3-ac-review.md` A table (pg18); base and head each time. The five-definitions report has only the UNION ALL IMV case (z04) |
| 3 | a temporary table as source, DEFERRED | "DEFERRED raw error" (text not printed); IMMEDIATE is correct | `p3-ac-review.md` A table (pg18; base and head) |
| 4 | a group key holding a double quote: `GROUP BY name COLLATE "C"`, `name::"char"`, `v::"int4"`, `j @> '{"k":"a"}'`, `$$…"X"…$$ \|\| name`, `"Mixed" - mixed`, `"x""y"` | `syntax error` or `zero-length delimited identifier` | `full-review-r2.md`, "Raw errors at create met on both trees" (pg18; `6946400` and `0324f47`) |
| 5 | `(t a JOIN u b ON …) JOIN u c ON …` as a keyless passthrough | `could not create unique index` | same |
| 6 | a table name holding a space | not printed | same; possibly covered since by the quoted-names report of the fix-wave review, not checked |
| 7 | `SELECT DISTINCT ON (g) … ORDER BY g, v, id` with `id` not selected; a window whose argument is not selected | `column "…" does not exist` | `fold-g3-review.md`, "Pre-existing, out of scope (same on base)" (pg17; `c3d4866` and `2b74d4f`) |
| 8 | `MIN` / `MAX` over an array column | raw error "in the top-K build" (text not printed) | same |
| 9 | `HAVING SUM(v) > ALL (SELECT …)` | "still a raw error at create on both" (text not printed) | `fold-g2-review.md` (pg16; `8ef9743` and `c3d4866`) |

## Expected vs actual

Expected: a tagged rejection naming the construct and a remedy, or support. Actual: PostgreSQL's
error for a statement pg_reflex generated, which names objects the user did not write.

## What is established and what is not

Established: each row on the trees named in its source. Not established: the outcome on the
release head (several create-path fixes landed after these reviews); for rows 3, 6, 8 and 9 the error text; whether any of them leaves relations or
registry rows behind; 1.11.5 for the rows whose base is a fold
base (2 in part, 7 to 9).

## What was ruled out

- Row 1 is not the partition-source refusal: the source is the root, and the aggregate over it
  works.
- Row 2 is not the UNION ALL IMV case already filed.
- Fixed since and left out: two output columns of one name (`column "max" specified more than
  once`, G3 review finding 6) are refused with a tagged message on the release head
  (`src/create_ivm/mod.rs`, test in `pg_test_v112_types.rs`).
- Already filed and left out: `WITH c(x, y) AS (…)` (c13 of the five-definitions report), a
  named window, `FROM ONLY t`, `-(1)` and integer literals beside GROUP BY, `- +v`.

## Fix direction (hypotheses to falsify, one per row)

1: the partitioned target is created by copying the source's partitioning, and the copy of a
two-column RANGE bound is written with one value; either copy the bound text as PostgreSQL
prints it or create the target unpartitioned. 2: detect `relkind = 'v'` among the sources and
refuse with the remedy "read the view's tables, or create an IMV with the view's query". 3:
refuse a temporary source in DEFERRED (the staging objects are permanent) with the remedy
IMMEDIATE. 4: quote-aware handling of the group key text (same family as
`2026-10-10_untagged_raw_errors_at_create_for_quoted_names_found_by_fix_wave_review.md`). 5: the
inferred key is not unique for that join; fall back to keyless. 7: add the unselected ORDER BY
or window argument as a hidden column, or refuse with "select it". 8: refuse arrays in top-K.
9: refuse a quantified
subquery comparison over an aggregate in HAVING as the other HAVING subquery forms are.

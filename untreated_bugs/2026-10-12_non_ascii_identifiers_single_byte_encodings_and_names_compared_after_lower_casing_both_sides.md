# Identifier folding residuals: a single-byte-encoding database is not handled, and several comparisons still lower-case both sides so `É`/`é` or `"T"`/`t` count as one name — limitation / hazard (no wrong result shown)

Residual declared by the author of the 1.12.0 fix `f79f9f8` ("fold an unquoted identifier as
PostgreSQL does, ASCII letters only"). Source: `full-review-r2-fix-report.md`, section C2, in the
ledger `.superpowers/sdd/2026-10-07-1.12.0-create-contract-hardening/`; PostgreSQL 18, UTF-8
database. Both points are about the same rule (how pg_reflex decides that two spellings are one
identifier), hence one report.

## What the source gives

No reproduction of a wrong result. The statements:

1. "Not handled: a single-byte-encoding database, where PostgreSQL also folds non-ASCII letters
   through the locale. Needs a line in the docs." In a UTF-8 database PostgreSQL folds ASCII
   letters only, which is what `f79f9f8` implements; in a LATIN1 (or other single-byte) database
   with a matching locale it also folds `É` to `é`. There pg_reflex would treat an unquoted `É`
   as a name distinct from `é` while PostgreSQL treats them as one. Not run.
2. "The remaining `to_lowercase()` calls lower-case **both** sides of a comparison (source and
   alias matching in `soundness.rs`, `trigger/ops.rs`, `trigger/outer_join.rs`,
   `expression_subquery.rs`, `sql_writer/identifier.rs`,
   `disable_update_skip_for_repeated_sources`). They can take `É` and `é` (or `"T"` and `t`) for
   one name, which over-matches; I did not change them. `normalized_column_name` (older) also
   uses `to_lowercase`; the C2 cells with `É` as a group key and select item pass with it as is."

Tests that exist: `v112_sr_non_ascii_upper_case_column` (8 queries over a table holding both
`"É"` and `"é"`), `v112_sr_non_ascii_upper_case_only_column_and_table` (3 queries).

## Expected vs actual

Expected: two identifiers are the same for pg_reflex exactly when they are for PostgreSQL in
that database. Actual: (1) unknown outside UTF-8; (2) in the listed comparisons, two distinct
quoted names that differ only by case are taken for one.

## What is established and what is not

Established: the code locations (by the author's audit) and that the eleven C2 queries pass.
Not established: any wrong result. An over-match in a soundness or skip guard normally turns
into a refusal or a full recompute, which is the safe direction; whether one of the listed sites
uses the match to **skip** work (the UPDATE skip for a source read once, an alias taken for the
source) with two tables `"T"` and `t` in one query was not tried. The filed report
`2026-10-12_remaining_matches_of_a_relation_by_name_without_schema.md` is the same family for
schemas.

## Fix direction (hypothesis to falsify)

Replace the both-sides `to_lowercase()` by the one folding function (`folded`: lower-case an
unquoted ASCII identifier, keep a quoted one) at each listed site, one site per commit, each
with a cell that holds `"T"` and `t` (and `"É"` and `"é"`) in one definition and the oracle.
For single-byte encodings: either document that pg_reflex supports UTF-8 databases only for
non-ASCII unquoted identifiers, or refuse at create an unquoted non-ASCII identifier when
`server_encoding` is not UTF8.

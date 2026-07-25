# 2026-07-25 — three minor follow-ups from the full 1.11.1 batch review

**Status: untreated, all low severity.** Found by adversarial correctness/performance review of
the full 1.11.1 batch. Grouped into one report since each is small and none warrants its own
fix cycle on its own; split out if one turns out to need real investigation.

## 1. RIGHT/FULL JOIN abandons the entire NOT-NULL set, not just the affected columns (missed optimization)

`src/create_ivm/soundness.rs` (`provably_not_null_key_columns`, around lines 1259-1262 and
1314-1317) clears `plan.not_null_columns` to empty the moment ANY RIGHT or FULL join appears
anywhere in the base query — even for a column with a catalog `NOT NULL` constraint on the
PRESERVED (non-nullable) side of that join, which is genuinely still provably NOT NULL. Harmless
before 2026-07-25 (nothing consumed the set on the affected code paths), but as of today's
nullable-explicit-key fixes (`0a55f78` and its siblings), this deterministically forces the
non-sargable `EXISTS`/`IS NOT DISTINCT FROM` predicate form for every RIGHT-JOIN passthrough IMV,
even ones with no actual NULL-key risk. Fix direction: narrow the abandonment to columns actually
sourced from the nullable side of the join, rather than the whole set.

## 2. A passthrough IMV's raw base SQL can evade FULL JOIN detection with unusual formatting

Aggregate IMVs regenerate `base_query` from a parsed AST (`sql_analyzer.rs`'s
`generate_base_query`), so `FULL JOIN`/`FULL OUTER` string detection is reliable there — verified
during the ljgroup fix review. PASSTHROUGH IMVs are different: their `base_query` is the RAW user
-supplied SQL (`create_ivm/mod.rs`, `ctx.sql: &'a str`), never reparsed/reformatted. A passthrough
IMV written with a line break inside the join keyword, e.g. `FROM a FULL\nJOIN b ...`, would
evade the exact-substring check in `trigger/mod.rs` (around line 136) that decides whether outer
-join handling applies at all — producing an IMV with NO outer-join maintenance logic and silent
wrong results on any secondary-side mutation. Fix direction: normalize whitespace before the
substring check, or better, detect the join type structurally (already-parsed AST info, if
available at that point) rather than by string matching raw SQL.

## 3. The 1.11.0→1.11.1 ignore_sources backfill misses an IMV whose sources are ALL ignored

`sql/pg_reflex--1.11.0--1.11.1.sql` (around line 189)'s corrective backfill CTE (`real_source`)
produces no row when every one of an IMV's sources is in its own `ignore_sources` list, so the
`bool_and` aggregate it feeds never runs and that IMV's row is silently skipped by the backfill —
it stays in whatever state it was in before the migration. Safe direction (the backfill only ever
flips a flag on, so a missed row just means "not yet corrected" rather than "wrongly corrected"),
but it is a real gap in the migration's stated coverage. Fix direction: use a `LEFT JOIN`/
`COALESCE` so an all-ignored IMV still gets a row (with the appropriate resulting flag value)
instead of being silently excluded from the aggregate.

# 2026-07-23 — MIN/MAX recompute scoping uses NULL-unsafe `IN`, drops NULL groups

**Status: untreated.** Surfaced by PS-5 Part B while gating the MIN/MAX recompute
joins; flagged independently in the PS-5 review. Pre-existing, not introduced by PS-5.

## Effect (silent wrong result)

`build_min_max_recompute_sql_inner` (`src/trigger/merge.rs`) scopes the recompute's
source aggregation to the affected groups by splicing, before the `GROUP BY`:

```sql
AND (<raw group cols>) IN (SELECT DISTINCT <norm cols> FROM <affected>)
```

`(NULL) IN (SELECT NULL)` evaluates to NULL, never TRUE. So when the affected group
key is NULL, that group's source rows are excluded from the scoped re-aggregation, the
recompute never re-derives its MIN/MAX, and the scalar left NULL by a retraction
(`Sub` sets `__min_x = NULL`) stays NULL forever. The NULL group's MIN/MAX is silently
wrong from then on.

## Reproduction

A MIN/MAX aggregate with a nullable group key, where the current MIN of the NULL group
is retracted (deleted), forcing the recompute path:

```sql
CREATE TABLE mmr (id SERIAL PRIMARY KEY, grp TEXT, val INT NOT NULL);
INSERT INTO mmr (grp,val) VALUES ('a',5),('a',9),(NULL,3),(NULL,8);
-- IMV: SELECT grp, MIN(val) AS lo, MAX(val) AS hi FROM mmr GROUP BY grp
DELETE FROM mmr WHERE grp IS NULL AND val = 3;   -- retract the NULL group's MIN
-- BUG: mmr_v NULL-group `lo` stays NULL instead of recomputing to 8.
```

Confirmed by `pg_test_correctness_min_max_recompute_gate_nullable_key` (PS-5 branch):
"EXCEPT ALL oracle failed for 'mmr_v': 2 mismatches" after the NULL-group retraction.

## Fix direction (own change, NOT PS-5)

Replace the `IN` scoping with a NULL-safe membership test, e.g. an
`EXISTS (... IS NOT DISTINCT FROM ...)` correlated against the affected table, spliced
in the same pre-GROUP-BY position. Note the tension with PS-5's sargability work: a
NULL-safe correlated EXISTS in the scoping filter is itself non-sargable, so if this
recompute's source scan turns out hot at scale it may want the same gated
fast/safe treatment PS-5 applied to the joins. The recompute only fires when a MIN/MAX
group underflowed (rare), so correctness should win over sargability for the first cut.

## Scope note

Same NULL-group family as PS-5. PS-5 gated the recompute's *join* conditions
(intermediate ⨝ __src and the EXISTS firing gate) to be sargable; it deliberately did
NOT touch this *scoping* filter, per review guidance to file it separately.

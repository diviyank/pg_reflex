# One statement writing two sources of a DEFERRED join IMV under `SET CONSTRAINTS ALL IMMEDIATE` counts the cross product twice — S1 (silent wrong aggregate)

Pre-existing (not introduced by 1.11.5). Found in the 1.11.5 final review.

## Mechanism

The deferred flush is a row-level constraint trigger on `__reflex_deferred_pending`
(`__reflex_deferred_flush_trigger`, src/schema_builder.rs:930, `DEFERRABLE INITIALLY
DEFERRED`). Each source's staging trigger inserts that source's pending row from inside its
own AFTER STATEMENT trigger (sql/deferred_trigger_body.plpgsql.in:31). Under
`SET CONSTRAINTS ALL IMMEDIATE`, an immediate constraint trigger fires at the end of the
query that queued it — here the nested `INSERT INTO __reflex_deferred_pending` — so source A's
flush runs before source B's staging trigger has even inserted its pending row.

When one statement writes both sources (a data-modifying CTE), both base tables already hold
their new rows when the first staging trigger runs. A's flush then sees one pending source:
`batch_has_multiple_sources` (src/trigger/deferred.rs:1134) and `imv_has_multiple_sources`
(:1232) are false, the cross-source guard never engages, and A's delta is applied as
`ΔA ⋈ B_new`. B's flush follows and applies `ΔB ⋈ A_new`. The cross product `ΔA ⋈ ΔB` is
counted twice — exactly the hazard the guard exists for, which the guard's comment says
"IMMEDIATE mode is immune" to because IMMEDIATE triggers see each statement separately; a CTE
statement is one statement for both sources.

- Passthrough IMV with a key: the second insert hits `__reflex_uk_<imv>` (23505), the flush's
  per-IMV savepoint catches it and the IMV is marked `known_stale` (loud).
- Aggregate IMV: the group is counted twice, `known_stale` stays FALSE (silent).
  Not every aggregate shape: one whose group key comes from the first source was correct in
  the repro below (its maintenance recomputes the affected groups); a group key from the second
  source, and an ungrouped `COUNT(*)`, double-count.

## Reproduction (pg17, 1.11.5)

```sql
CREATE TABLE ja (k INT, a INT); CREATE TABLE jb (k INT, b INT);
INSERT INTO ja SELECT g, g % 2 FROM generate_series(1, 10) g;
INSERT INTO jb SELECT g, g FROM generate_series(1, 10) g;
SELECT create_reflex_ivm('jh', 'SELECT jb.b % 3 AS g, COUNT(*) AS n, SUM(ja.a) AS s
  FROM ja JOIN jb ON jb.k = ja.k GROUP BY jb.b % 3', NULL, NULL, 'DEFERRED');
SELECT create_reflex_ivm('ji', 'SELECT COUNT(*) AS n FROM ja JOIN jb ON jb.k = ja.k',
  NULL, NULL, 'DEFERRED');
BEGIN;
SET CONSTRAINTS ALL IMMEDIATE;
WITH x AS (INSERT INTO ja VALUES (100, 1) RETURNING 1) INSERT INTO jb VALUES (100, 5);
COMMIT;
-- jh: g = 2 shows n = 5, s = 3; fresh query: n = 4, s = 2
-- ji: 12; fresh query: 11
-- known_stale = FALSE on both
```

The same CTE without `SET CONSTRAINTS ALL IMMEDIATE` is correct (both pending rows exist when
the COMMIT-time flush runs, so the guard engages).

## Fix direction

The guard must see the statement, not the pending table. Options: (1) in the staging trigger,
when the flush constraint is immediate, record pending rows but postpone the flush to the end
of the top-level statement (e.g. a statement-level AFTER trigger on the pending table that
fires once per top-level query); (2) in the flush, treat any source whose base table has
changes in this transaction newer than its staged delta as pending (hard to detect cheaply);
(3) cheapest safe option: when the flush runs with constraints IMMEDIATE and the IMV has two
or more observed sources written in the current statement (`pg_stat_get_xact_tuples_*`
deltas since the statement started), list it for the COMMIT-time rebuild instead of applying
the delta. Test with the repro above (aggregate oracle + passthrough not stale).

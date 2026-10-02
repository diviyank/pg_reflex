# One statement writing two sources of an IMMEDIATE join IMV counts ΔA ⋈ ΔB twice — S1 (silent wrong aggregate)

Pre-existing (not introduced by 1.11.5). Found by the r5 fix-round-1 review (probe `zrv10`:
20 mismatching groups on pg17 and on pg15). Sibling of
`2026-10-01_set_constraints_immediate_two_source_statement_double_counts.md`, which is the
DEFERRED flavour (needs `SET CONSTRAINTS ALL IMMEDIATE`); this one needs no special setting.

## Mechanism

An IMMEDIATE IMV is maintained by one AFTER STATEMENT trigger per source, each applying that
source's transition table joined against the *current* state of the other sources
(`ΔA ⋈ B`). The algebra `Δ(A ⋈ B) = ΔA ⋈ B_old + A_new ⋈ ΔB` (or the symmetric split) is only
right when, at the time A's trigger runs, B does not yet hold ΔB. Two ways one top-level
statement breaks that:

- a data-modifying CTE writing A and B (`WITH x AS (INSERT INTO a …) INSERT INTO b …`): both
  sub-statements run before any AFTER STATEMENT trigger fires, so A's trigger sees `B_new`
  and B's trigger sees `A_new`;
- a user AFTER trigger on A that writes B (or a BEFORE trigger chain doing the same): B's rows
  are in place when A's IMV trigger runs, and B's own IMV trigger then joins against `A_new`.

Either way `ΔA ⋈ ΔB` is applied by both triggers.

- Passthrough IMV with a key mapping: the second insert of the cross rows hits
  `__reflex_uk_<imv>` (23505) and the statement aborts (loud).
- Aggregate IMV: the affected groups are counted twice and nothing flags it (silent); a
  shape whose maintenance recomputes the affected groups from the source (MIN / MAX, some
  group keys from the first-fired source) can come out right.

## Reproduction sketch (pg17)

```sql
CREATE TABLE ja (k INT, a INT); CREATE TABLE jb (k INT, b INT);
INSERT INTO ja SELECT g, g % 2 FROM generate_series(1, 10) g;
INSERT INTO jb SELECT g, g FROM generate_series(1, 10) g;
SELECT create_reflex_ivm('ji', 'SELECT jb.b % 3 AS g, COUNT(*) AS n
  FROM ja JOIN jb ON jb.k = ja.k GROUP BY jb.b % 3');           -- IMMEDIATE (default)
WITH x AS (INSERT INTO ja VALUES (100, 1) RETURNING 1) INSERT INTO jb VALUES (100, 5);
-- ji shows one row too many for g = 2 against a fresh run of its query
```

## Fix direction

The trigger has to know the other source changed in the same statement. Options: (1) per
statement, record which sources of each multi-source IMV have fired (a transaction-local
table keyed by `statement_timestamp()`/command id) and, when a second source of the same IMV
fires in the same top-level statement, subtract `ΔA ⋈ ΔB` — it needs A's transition rows,
which are gone by then, so they would have to be staged; (2) cheaper and safe: when a
source's trigger detects that another source of the IMV was modified by the current command
(xmin of the other source's rows = current xid and cmin ≥ the statement's first command id),
fall back to a reconcile of the IMV for that statement; (3) document: writing two sources of
a join IMV in one statement is unsupported for IMMEDIATE IMVs — split the statement or use
DEFERRED (which is correct without `SET CONSTRAINTS ALL IMMEDIATE`).

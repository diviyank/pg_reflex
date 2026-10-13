# Function lookups by name over-reject: an unqualified call is judged by every function of that name in every schema and overload (volatility), or by any same-arity namesake that returns a set — limitation (loud, deliberate safe side; new in 1.12.0)

Residuals of two 1.12.0 create checks after their fix rounds. Sources, in the ledger
`.superpowers/sdd/2026-10-07-1.12.0-create-contract-hardening/`: `final-review.md` (I2, M2, M3;
pg17, `401f831` against `6946400`), `final-fix-report.md` and `final-fix-review.md` (F2, F4;
pg17, `04012ea`), `fold-g5-review.md` / `fold-g5-report.md` (F2; pg17). Grouped because the
cause is one: pg_reflex decides a property of a call from the function's **name**, not from the
function PostgreSQL would resolve, and errs toward refusing.
`2026-10-07_session_dependent_functions_are_matched_by_a_name_list.md` is the same idea for a
static list and does not cover these.

## 1. Volatility: an unqualified call is matched across schemas and overloads

Before the fix round (`final-review.md`, I2, k15; reproduced there):

```sql
CREATE SCHEMA zzfr_s1; CREATE SCHEMA zzfr_s2;
CREATE FUNCTION zzfr_s1.zzfr_f(int) RETURNS int LANGUAGE sql IMMUTABLE AS 'SELECT $1 + 1';
CREATE FUNCTION zzfr_s2.zzfr_f(int) RETURNS int LANGUAGE sql AS 'SELECT $1 + 1';   -- unlabelled
SELECT create_reflex_ivm('v', 'SELECT id, zzfr_s1.zzfr_f(v) AS x FROM s', 'id');
-- 401f831: ERROR: [reflex-unsupported] Volatile function(s) zzfr_f are not supported in IMV 'v'. …
-- 6946400: created
```

The fix round changed two things (`final-fix-report.md`): the message now prints the offending
`regprocedure`s and the `ALTER FUNCTION <regprocedure>` remedy, and **a schema-qualified call is
matched in that schema only**, so the statement above is accepted again. What remains, by
decision: "Unqualified calls keep the catalog-wide all-overload match", and the message says so
("An unqualified call is matched by name in every schema and for every overload").

So, not re-run in this form but following from the kept rule: `SELECT id, zzfr_f(v) AS x FROM s`
with `search_path = zzfr_s1` is refused because of `zzfr_s2.zzfr_f`, and an IMMUTABLE
`zzfr_ov(int)` called unqualified is refused while a VOLATILE `zzfr_ov(text)` exists (k16 before
the fix; unqualified overloads were not narrowed). The reviewer's point stands for unqualified
calls: "with schema-per-tenant layouts or an extension that ships a volatile function with a
common name, one unlabelled function anywhere in the database blocks every IMV calling any
function of that name". The remedy now converges: qualify the call, or label the named function.

Smaller items of the same check:
- with five volatile overloads the word IMMUTABLE ends at character 219 of the message (ordinary
  cases 164 to 186; the doctor cap is 400); one statement is printed per round, so N volatile
  overloads take N create attempts, each converging (`final-fix-review.md` F4, left untouched);
- through `reflex_rebuild_chain` the message is prefixed and the remedy ends near character 261
  with 10-character names (M2);
- the time-zone WARNING is noisy: `EXTRACT(year FROM date_col)` warns, and so do `to_char`,
  `to_date`, `date_part` on any type; one WARNING per sub-IMV on decomposition (M3, accepted by
  the spec, not changed).

## 2. Set-returning: a scalar call beside a set-returning namesake

`fold-g5-review.md` F2 found `upper(g)` taken for set-returning because some function named
`upper` returns a set. After the fix (`fold-g5-report.md`, F2) candidates are the functions of
that exact name and argument count, in the named schema or on the search path. One case is
"kept over-broad on purpose": with `f(int) SETOF` and `f(text)` scalar, a call is set-returning
when **any** candidate returns a set, so the IMV is keyless (correct, fully refreshed) without a
key and rejected with one. The reason is a reproduced defect of the narrower rule: created keyed
on the source key, IMMEDIATE raises a duplicate key on the write and "DEFERRED goes stale with no
error (oracle off by 4 after the flush)".

## 3. Ungrouped unknown aggregates

`final-fix-review.md` F2 (left untouched): `SELECT zagg(v) AS m, COUNT(DISTINCT v) AS n FROM s`
and `SELECT zagg(v) AS m, (MAX(v) BETWEEN 0 AND 5) AS e FROM s` are created as keyless
full-refresh passthroughs and are correct through ten steps, although the brief wanted an
unknown aggregate beside a recognised one rejected. Conversely a JOIN with a declared key that
no source owns, correct by full refresh before, is now rejected (`final-fix-report.md`, concern
2). Inconsistent, not wrong.

## Expected vs actual

Expected: the property of the function PostgreSQL resolves for the call (its `pg_proc` row for
these argument types and this `search_path`). Actual: the property of the name.

## What is established and what is not

Established: the pre-fix reproduction, the kept rule (stated in the message itself), the
set-returning case with its reason, the message positions. Not established: a run of the
unqualified cross-schema case on the release head; how often a common function name is volatile
in some schema of a real database.

## Fix direction (hypothesis to falsify)

Resolve the call as PostgreSQL does: describe the query once (the create already builds a
temporary view to read the column types) and read the function OIDs from the view's dependencies
(`pg_depend` on the view's rule), then test `provolatile` / `proretset` of exactly those OIDs.
Falsify with the cells k15, k16 and the G5 F2 pair, and with a polymorphic and a variadic
overload; when resolution fails for any reason, keep today's by-name answer.

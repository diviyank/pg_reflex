# 2026-07-25 — long wrapper names collapse the three union-mirror trigger functions into one, breaking the IMV at CREATE time

**Status: untreated.** Found adversarially while reviewing PS-17 (`reflex_rebuild_union_mirror` +
`trigger-attached` extension for `untreated_bugs/2026-07-24_union_mirror_triggers_unchecked.md`).
Pre-existing, unrelated to PS-17's diff — the function under suspicion,
`install_union_mirror_triggers`, is unchanged by that fix; only its caller is new. Filed
separately per the hygiene rule against folding adjacent bugs into unrelated work.

## Symptom

`create_reflex_ivm` on a CTE-over-UNION-ALL view consumed by an aggregate (the shape that
triggers `install_union_all_intermediate_wrapper`) reports success, but the materialised wrapper
is broken from the moment the first base-table write happens: an `INSERT` into an operand fails
with `ERROR: relation "__reflex_old" does not exist` (an UPDATE-only local variable, referenced
from the INSERT trigger body).

## Root cause

`src/create_ivm/decompose.rs:344` builds the three mirror-trigger **function** names from one
shared base with a raw (non-`safe_identifier`) suffix:

```rust
let fn_base = format!("__reflex_union_mirror_{safe_wrapper}_{operand_idx}");
let fn_ins = format!("{fn_base}_ins");
let fn_del = format!("{fn_base}_del");
let fn_upd = format!("{fn_base}_upd");
```

`safe_wrapper` is `sanitized_source_suffix(wrapper)` (`query_decomposer.rs:50`), which does no
length capping — `install_union_mirror_triggers`'s own comment (`decompose.rs:401-405`)
deliberately opts out of `safe_identifier`'s hash-suffixed truncation because trigger *names*
need to match PostgreSQL's own naive truncation for lookups elsewhere to line up. But that
comment's reasoning only covers `CREATE TRIGGER`; the three `CREATE OR REPLACE FUNCTION` names
share the same unbounded prefix, and PostgreSQL truncates a `>NAMEDATALEN-1`-byte identifier at a
char boundary — so once the truncated form eats past the `i`/`d`/`u` character that is the *only*
difference between `..._ins`, `..._del`, `..._upd`, all three DDLs `CREATE OR REPLACE` the exact
same `proname`, and the last one issued (`upd`) silently overwrites the other two.

**Verified threshold: wrapper name length ≥ 38 bytes** (for a 1-digit `operand_idx`; the prefix
`__reflex_union_mirror_` is 22 bytes, `+ wrapper + _0` reaches the 63-byte `NAMEDATALEN-1` limit
one byte before the `_ins`/`_del`/`_upd` discriminator survives truncation). Reproduced directly:
a 39-char wrapper name creates successfully, then the first base-table `INSERT` throws the
`__reflex_old` error because the INSERT trigger is executing the UPDATE function body.

This is realistic, not exotic — the wrapper name is `<view_name>__cte_<alias>` (7-8 extra bytes),
so any view name at or above ~30 characters is affected. `sop_incoming_stock_baseline_view`
(33 chars) is already over that bar.

## Relationship to PS-17

PS-17's new `trigger-attached` extension (same file family) *does* correctly detect the resulting
missing/wrong triggers once this collision has broken a wrapper — but its `suggested_fix`
(`reflex_rebuild_union_mirror`) re-runs the same buggy `install_union_mirror_triggers` and cannot
converge either, since the function-name collision reproduces every time. That check now truncates
its *expected trigger name* comparison to match PostgreSQL's real truncation (fixing a false
positive at the trigger-name layer, PS-17 finding F1) but does nothing for — and cannot detect —
the function-body collision underneath it. A future fix here should also make `trigger-attached`
(or a new check) verify the three function `prosrc` bodies are distinct for a materialised wrapper,
not just that the trigger names exist.

## Fix direction

`fn_base` (or the three derived function names) needs the same "match Postgres's own truncation,
but keep the DML-kind discriminator" treatment `install_union_mirror_triggers` already gives the
*trigger* names — except trigger names can share the plain naive truncation (they're never
compared to a hash-suffixed form elsewhere), while the **function** names specifically need the
`ins`/`del`/`upd` discriminator preserved even after truncation, e.g. truncate the wrapper-derived
core first and place the DML tag as a fixed-position, always-preserved suffix rather than
appending it after the unbounded part. Needs its own pre-spec: this touches create-time DDL
generation, so a length-boundary fixture (name ≥ 38 bytes) has to be added to whatever regression
test locks in the redesigned naming, and existing installations whose functions already collided
under the old scheme need a migration-time heal (re-run the corrected installer for every
materialised wrapper — same shape as the PS-6 passthrough-scratch heal).

## Severity

S1 — silent-at-create, hard-broken-at-first-write data corruption risk (INSERT trigger executing
UPDATE logic is not merely "missing", it runs the *wrong* maintenance code against live data).

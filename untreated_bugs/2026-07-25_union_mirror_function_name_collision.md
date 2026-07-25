# 2026-07-25 — no automatic detection that a deployed union-mirror wrapper already collided under the pre-1.11.1 naming scheme

**Status: narrowed.** The original create-time collision (below, kept for context) is fixed —
`install_union_mirror_triggers` (`src/create_ivm/decompose.rs`) now runs each mirror trigger
function's full raw name (DML tag + wrapper + operand index) through `safe_identifier`, which
hashes it into a truncated form whenever it exceeds 63 bytes, so the operand index and DML-kind
tag both survive regardless of wrapper length. `drop_reflex_ivm` (`src/drop_ivm.rs`) was updated
to match and additionally still probes the legacy (pre-1.11.1, unhashed) name form, so dropping a
wrapper created before this fix does not leak its functions. Regression tests:
`ps18_long_wrapper_mirror_functions_stay_distinct`, `ps18_long_wrapper_mirror_functions_run_correct_body`,
`ps18_drop_reflex_ivm_leaves_no_orphan_mirror_functions`, `ps18_drop_reflex_ivm_cleans_up_legacy_named_mirror_functions`
(`src/tests/pg_test_ps18.rs`).

## Residual: pre-1.11.1 wrappers already collided in the field have no automated signal

A wrapper created by pg_reflex 1.11.0 or earlier with a name ≥ 38 bytes was already
silently broken (see the original symptom below) before this fix ever ran. Upgrading the
module does **not** repair an already-collided wrapper — the fix only changes what a *new*
`create_reflex_ivm` call produces. Nothing detects the collision automatically:
`trigger-attached` (PS-17) only checks that the three trigger *names* exist, not that their
`tgfoid`s point at three distinct functions with the correct bodies, so a collided wrapper
audits clean.

**Available remedy, not yet wired to a finding**: `reflex_rebuild_union_mirror(wrapper)`
(shipped alongside `trigger-attached` in this same unreleased 1.11.1) re-runs
`install_union_mirror_triggers` with the corrected naming and re-binds each operand's
triggers to the new, distinct functions — this does repair a collided wrapper, but only if
an operator knows to run it. The old, now-unreferenced legacy-named function is left as a
harmless orphan, cleaned up automatically the next time the wrapper itself is dropped.

Fix direction for closing this residual: extend `trigger-attached` (or add a dedicated
check) to compare the three operand-relation triggers' `tgfoid`s for distinctness (cheap,
catalog-only) and report `reflex_rebuild_union_mirror` as the remedy when they've collapsed
onto one function. Low urgency — the shape requires a base view name ≳30 characters feeding
`install_union_all_intermediate_wrapper`, which is uncommon; `sop_incoming_stock_baseline_view`
(33 chars) is the only known example in this codebase's own fixtures/benchmarks, and it does
not hit this specific decomposition path.

## Severity

S3 — narrowed from S1. The original silent-wrong-result risk is closed for anything created
under the fixed module; what remains is an observability gap for wrappers that were already
broken under the old module, with a working manual remedy once the finding is spotted by hand.

---

## Original report (context, root cause now fixed)

Found adversarially while reviewing PS-17 (`reflex_rebuild_union_mirror` + `trigger-attached`
extension for `untreated_bugs/2026-07-24_union_mirror_triggers_unchecked.md`). Pre-existing,
unrelated to PS-17's diff — the function under suspicion, `install_union_mirror_triggers`, was
unchanged by that fix; only its caller was new.

### Symptom

`create_reflex_ivm` on a CTE-over-UNION-ALL view consumed by an aggregate (the shape that
triggers `install_union_all_intermediate_wrapper`) reported success, but the materialised wrapper
was broken from the moment the first base-table write happened: an `INSERT` into an operand
failed with `ERROR: relation "__reflex_old" does not exist` (an UPDATE-only local variable,
referenced from the INSERT trigger body) — or, past a second, longer threshold, silently mis-tagged
the new row with the wrong `__reflex_src_idx` instead of erroring at all (found during this fix's
own adversarial review: moving the DML tag before the wrapper component alone still let a
sufficiently long wrapper name truncate away the trailing operand-index digit, collapsing operand
0 and operand 1's same-kind function onto one `proname`).

### Root cause

`src/create_ivm/decompose.rs` built the three mirror-trigger **function** names from one shared,
unbounded, non-`safe_identifier` suffix, so PostgreSQL's NAMEDATALEN truncation could eat the
DML-kind discriminator (`ins`/`del`/`upd`) and, once that was fixed by re-ordering, could still eat
the operand-index discriminator at a longer threshold — either way collapsing distinct functions
onto one `proname`, with the last `CREATE OR REPLACE FUNCTION` issued silently overwriting the
others' bodies while already-bound triggers kept their captured `tgfoid`.

**Verified threshold: wrapper name length ≥ 38 bytes** (for a 1-digit `operand_idx`) for the
original ins/del/upd collision; the cross-operand collision (post-reorder) reproduces at the same
39-byte fixture used by the regression tests.

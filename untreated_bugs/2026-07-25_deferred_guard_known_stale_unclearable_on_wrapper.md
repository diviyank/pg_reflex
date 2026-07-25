# 2026-07-25 — DEFERRED cross-source guard may flag a materialised UNION-ALL wrapper `known_stale` with no way to clear it

**Status: untreated, reachability unconfirmed.** Found by adversarial review of the full 1.11.1
batch (`untreated_bugs/` hygiene pass), not from a field report. Severity assessed as S2/S3
(operational wedge, not silent wrong data) pending confirmation it is reachable at all.

## The mechanism

`trigger/deferred.rs`'s cross-source guard (fires when one transaction mutates ≥2 sources of an
IMV) now writes `known_stale = TRUE` (with a reason) on ANY `ERROR`-prefixed return string from
its repair call, per the 2026-07-25 fix in `reconcile_generated_child_for_cross_source_guard`
(`src/reconcile.rs`, merged as `55c2c44`). `reconcile_one`'s own wrapper refusal (added by an
earlier 2026-07-25 fix, `src/reconcile.rs` around line 378-391 — refuses to `TRUNCATE` a
decomposed-wrapper VIEW) **is itself** one such `ERROR`-prefixed return.

`known_stale` is cleared only inside `reconcile_one`'s own success tail (`src/reconcile.rs`,
around lines 497 and 762) — a code path a wrapper row can never reach, because the wrapper
refusal returns before that tail runs, on every subsequent call too.

## The open question

If a materialised UNION-ALL wrapper (has a stored `__reflex_src_idx` column) is ever reached by
the DEFERRED cross-source guard directly — i.e., if the wrapper ITSELF (not one of its operands)
can have ≥2 of its own sources mutated in one transaction under DEFERRED mode — it would be
permanently flagged `known_stale` with no primitive able to clear it: every future clear attempt
re-hits the same wrapper refusal, the same `ERROR` return, and re-sets the flag.

Whether this is reachable turns on whether a materialised wrapper's own TARGET table (as opposed
to its generated operand sub-IMVs) carries DEFERRED-mode staging triggers directly, and whether
`imv_has_multiple_sources`'s multi-source detection can be satisfied by a wrapper's `depends_on`
(which lists operand sub-IMV names, not raw source tables — see `decompose.rs:612-613`). This was
not settled by static reading.

## Fix direction, once confirmed

If reachable: the guard should special-case a wrapper row (checked the same way
`reconcile_one`'s own refusal or the audit's wrapper classification does — `is_decomposed_wrapper()`
/ `end_query='' AND aggregations::text='{}'`) BEFORE attempting the repair call, either skipping
the guard for a wrapper entirely (its operands are what actually need reconciling) or routing to
whatever the correct wrapper-level repair is, rather than letting the operand-repair machinery's
refusal propagate into a `known_stale` that nothing can clear.

## Next step

Confirm reachability with a live repro: create a materialised UNION-ALL wrapper, put it (not an
operand) directly behind DEFERRED-mode triggers on ≥2 of its nominal sources, mutate both in one
transaction, and check whether the guard fires against the wrapper itself. If it does not — e.g.
because the wrapper never gets its own staging triggers, only its operands do — this report can
be closed as a non-issue.

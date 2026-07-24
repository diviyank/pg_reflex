# 2026-07-24 — a stale decomposed wrapper whose parent is fresh is reconciled standalone by scheduled_reconcile → column-shift

**Status: untreated. Pre-existing (present on `main`/1.10.11), NOT a 1.11.0 regression.**
Surfaced by the final review of 1.11.0 (the covered-skip fix agent), flagged out of
scope for that fix.

## Mechanism
`reflex_scheduled_reconcile`'s candidate set is the age-gated registry scan. A
CTE/set-op-decomposed **wrapper** node (`is_generated_sub_imv = TRUE`,
`aggregations = '{}'`, e.g. `p__cte_u` from a `UNION ALL` body) is only removed
from the batch by the covered-skip filter when a candidate that reads it
(its parent) is ALSO in the batch. If the parent is **fresh** (recently
reconciled, so not a candidate) while the wrapper is **stale**, the wrapper is an
*uncovered* candidate → `reflex_reconcile(wrapper)` → `reconcile_one` takes the
passthrough branch (`reconcile.rs:139`) → `INSERT INTO <wrapper> <base_query>`
puts N payload columns into the wrapper's N+1 columns (`__reflex_src_idx` +
payload), **column-shifting** — silent wrong data in the wrapper, which then
propagates to the parent on its next maintenance.

`last_update_date` is written only by reconcile/creation, never by incremental
maintenance, so "wrapper stale while parent fresh" is reachable whenever something
reconciled only the parent (e.g. a targeted `reflex_reconcile(parent)`, or the
parent aged in and out on a different cadence).

## Why it is not a 1.11.0 blocker
- Present on `main` (1.10.11): the direct-candidate path to `reconcile_one` on a
  wrapper predates this release. 1.11.0's PS-1 Blocking-1 fix closed the
  *recursion* path (`generated_dependencies_shallowest_first` excludes decomposed
  nodes) and the covered-skip fix closed the *parent-is-candidate* path; this
  third path (parent fresh, wrapper stale, direct candidate) remains.
- Requires a specific state, not the common all-age-together case.

## Fix direction
Exclude decomposed nodes (`is_generated_sub_imv AND aggregations = '{}'`) from the
`reflex_scheduled_reconcile` **candidate** set entirely — they must never be
standalone-reconciled (they are maintained by their operands' mirror triggers).
That is a candidacy-semantics change (broader than the covered-skip fix), so it
wants its own cycle with a test that ages a wrapper stale while its parent is
fresh and asserts the wrapper is neither reconciled nor column-shifted. Consider
also making `reconcile_one` refuse a decomposed node outright as a backstop, so no
future caller can column-shift a wrapper.

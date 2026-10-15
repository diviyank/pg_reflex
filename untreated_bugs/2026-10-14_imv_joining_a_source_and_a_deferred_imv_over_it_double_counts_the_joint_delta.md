# An IMV joining source X with a DEFERRED IMV over X counts ΔX ⋈ ΔU twice

**Severity:** silent wrong result. Pre-existing (reproduced on b4e486b, 1.12.1 code + B1).
**Found:** 2026-10-14 by the B2 review (ledger `2026-10-14-1.12.2-bulk-leaf-rebuild/B2-review.txt`
N1, probes in `B2-review-probes.rs`).

Shape: IMV V joins source X with U, where U is a DEFERRED IMV that itself reads X. One transaction
changes X; at COMMIT, V receives ΔX (joined with U's state) and ΔU (joined with X's state), and the
joint term ΔX ⋈ ΔU is counted twice. 4 mismatches after a whole-plan UPDATE, 2 after a 1-row UPDATE.
1.12.2's B2 hides the large case (it replaces both deltas wholesale); the small case still fails.

Suspected cause: the cross-source guard is decided once, before the per-IMV loop
(src/trigger/deferred.rs ~1672-1683), so a source whose second input becomes pending during the
loop is not seen as multi-source. Needs a Step 0.

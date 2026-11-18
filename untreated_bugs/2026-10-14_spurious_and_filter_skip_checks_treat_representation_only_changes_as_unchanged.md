# Spurious-change and filter-skip checks treat representation-only changes as unchanged

**Severity:** silent wrong result for IMVs whose output depends on a value's representation
(e.g. an `amount::text` mirror, `MAX(x::text)`, a passthrough showing the stored scale). Pre-existing.
**Found:** 2026-10-14 by the D′ review (ledger `2026-10-14-1.12.2-bulk-leaf-rebuild/Dprime-review.txt`,
F1-pre; probes in `Dprime-review-probes.rs`).

The COMMIT source-wide spurious check (src/trigger/deferred.rs ~1574, whose comment claims
"byte-equal"), the COMMIT per-IMV filter-skip (~1910) and the IMMEDIATE-only filter-skip compare with
`=`/EXCEPT ALL. Changes such as numeric 5 → 5.00, float 0 → -0, citext case, jsonb numbers and
bpchar blanks compare equal and are skipped. Probes: a DEFERRED-only mirror and an IMMEDIATE-only
mirror each show 400 mismatches, with and without 1.12.2's D′.

Fix direction: the same exact-representation projection D′ adopts in 1.12.2 (raw only for an
allowlist of types whose `=` is byte-exact, `::text` otherwise).

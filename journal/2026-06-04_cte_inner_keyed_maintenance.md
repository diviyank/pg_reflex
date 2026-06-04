# CTE inner-IMV keyed maintenance — worth-it evaluation

**Date:** 2026-06-04
**Branch:** `feat/cte-inner-keyed-maintenance`
**Spec:** `docs/superpowers/specs/2026-06-04-cte-chained-imv-maintenance-optimizations-design.md`
**Plan:** `docs/superpowers/plans/2026-06-04-cte-chained-imv-maintenance-optimizations.md`

## What was wrong

A CTE-defined IMV is decomposed into a chain: inner `view__cte_<alias>` IMV → outer
`view`. The inner is created through the same passthrough path as a top-level IMV,
which calls `resolve_unique_columns`. That resolver's single-source PRIMARY-KEY
auto-detect query (`src/create_ivm/mod.rs:248`) read `array_agg(a.attname …)` — type
`name[]` — into a Rust `Vec<String>` (expects `text[]`). The `IncompatibleTypes`
error was silently swallowed by `.unwrap_or(None)`, so **PK auto-detect returned
empty for every passthrough IMV** (flat top-level *and* inner CTE). The inner CTE
therefore had no key and full-rebuilt on every flush; its oversized staging delta
then forced the outer to recompute the whole level too.

The three sibling catalog queries (`soundness.rs:355`, `soundness.rs:486`,
`partition.rs:102`) all cast `a.attname::TEXT`. `mod.rs:248` was the lone offender.

## The fix

One line: `array_agg(a.attname::text ORDER BY k.n)`. Aligns the query with its three
siblings; the result becomes `text[]` and `Vec<String>` deserialization succeeds.

## Measured (pgrx PG17, `bench_features_matrix.sql`, `MAXVOL=10000`, ~10k-row CTE edit incl. COMMIT cascade)

| Case | Source PK? | Flush time | Mismatches |
|---|---|---|---|
| **H cte/none** | `s_cte(id PRIMARY KEY)` | **132.6 ms** | 0 |
| **J cte/LIST** | `s_ctel` — **no PK** | 4872 ms | 0 |

- **H** drops from the previously profiled ~4.5–7 s (full inner rebuild + 2M-row outer
  `Except All`) to **~133 ms** — ≈35×. Inner staging delta is now O(K): a unit test
  asserts ≤6 delta rows for a 3-row edit (full rebuild would be ~1000).
- **J** is unchanged because `s_ctel` has no primary key, so no inner key is *provable*.
  This is the retained keyless full-rebuild fallback working as designed — and it
  doubles as a live "pre-fix-equivalent" reference: same CTE shape, no PK, still ~4.9 s,
  still correct. It confirms the fix is precisely what makes H fast.

The fix also repaired flat single-source passthrough keying, which the same bug had
silently broken package-wide.

## Correctness

- IMMEDIATE oracle: 0 mismatches across INSERT/UPDATE/DELETE and rows entering/leaving
  the CTE's WHERE filter.
- Keyless fallback retained: a CTE whose source has no unique constraint stays keyless
  and correct (no fabricated key — the hard correctness gate).
- Full `cargo pgrx test pg17` suite green (1200 tests). One intermittent failure
  observed — a `deadlock detected` in `pg_fuzz_subpartition_swap_sequence_matches_recompute`
  — is a pre-existing parallel-execution flake: it passes in isolation, appears on
  different/no tests across runs, and uses an *explicit* key so it never enters the
  changed auto-detect branch.
- clippy clean, fmt clean.

## Worth it?

**Yes — keep.** One-line change, zero added complexity, correctness-preserving, and
a ~35× win exactly where a unique key is provable. The remaining slow case (J) is
correct and is the documented keyless fallback; speeding it up would require either
requiring a PK on the partitioned source or the spec's unimplemented approach (b)
(gated trickle-down of the outer's declared key into a passthrough→passthrough inner)
— both out of scope here.

Component 2 (in-transaction explicit-flush cascade for chained IMVs) remains deferred
to its own brainstorm → spec → plan; its mechanism (drain via `ln`/`graph_child`, not
the source-keyed `depends_on`) needs design, and the COMMIT path is already correct.

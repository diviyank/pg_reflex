# 2026-07-24 — 2026-06-18 audit gap 2: the residual base-dependent per-flush cost is the unconditional `ANALYZE intermediate` (stays deferred, with evidence)

**Status: deferred (documented).** Diagnosed under PS-8. This closes the
attribution question the 2026-06-18 work left open; it is intentionally **not**
being fixed, and this entry records why.

## Attribution (confirmed)

After PS-5 removed the O(total_groups) intermediate-MERGE cost
(`src/trigger/merge.rs:244-277`), the only statement on the incremental dispatch
path whose cost grows with base size is the unconditional
`EXECUTE 'ANALYZE {intermediate}'` at `src/trigger/dispatch.rs:176` (mirrored at
`dispatch.rs:305`, `dispatch.rs:369`, `dispatch.rs:737`). Its own comment records
~150 ms on a 180k-row intermediate.

`flush_scales_with_base` (`src/lib.rs:1471`) flags any shape whose large-base
flush exceeds 50 ms and grows more than `base_ratio/3` (~8.3× at the 25× fixture
ratio). A fixture whose **intermediate** scales with base (high-cardinality group
key, or passthrough) makes ANALYZE cross that gate; a fixed-group fixture keeps
the intermediate flat — which is exactly the 2026-06-18 flake (identical work at
both scales, tripping on timing noise).

## Why it stays deferred

- **Load-bearing.** Without fresh stats after the MERGE the planner picks
  NestedLoop+SeqScan (the dispatch comment cites 12+ min on 100k groups). ANALYZE
  is a plan-quality safeguard, not incidental.
- **Bounded.** ANALYZE samples ~`300 * default_statistics_target` rows (~30k), so
  its cost flattens above that row count instead of growing with base forever. It
  is not the multi-second/minute pathology `assert_sublinear` exists to catch.
- **Right instrument already exists.** The shape PS-5 fixed is locked by the
  PLAN-based assertion `audit_ps5_nullable_group_key_target_sync_uses_index_scan`,
  which is immune to the ANALYZE term. The timing-based `assert_sublinear` should
  not be extended to cover ANALYZE — doing so would pressure a real safeguard to
  satisfy a wall-time ratio.

Removing or gating ANALYZE to make a timing probe pass would trade a plan-quality
safeguard for test convenience — the wrong trade under the CLAUDE.md priority
order (correctness/performance above test aesthetics).

## If it is ever prioritised

The tractable idea is a **delta-size-gated ANALYZE skip**: skip
`ANALYZE {intermediate}` when the affected/scratch row count is tiny relative to
the intermediate's `reltuples` (a handful of changed rows will not move the
histogram). That is a behaviour change and needs its own benchmark answering
"does the NEXT flush still get a good plan after a skipped ANALYZE?" — i.e. its
own pre-spec, not a drive-by edit here.

---

## Related minor gap (integration note, 2026-07-24): PS-3 backfill under-flags bare-name ignore_sources

The PS-3 `requires_explicit_refresh` migration backfill (`sql/pg_reflex--1.10.11--1.11.0.sql`,
PS-3 section) excludes ignored sources with an exact-string match
`NOT (s = ANY(ignored_sources))`, whereas create-time (`src/create_ivm/mod.rs`
`all_real_sources_are_matviews`) compares each ignore entry against BOTH the
qualified source and its bare form. So on upgrade, an IMV that ignores a real
table by *bare* name while `depends_on` stores it *qualified* is under-flagged
(stays invisible). Narrow config; new IMVs unaffected (create-time is correct).
Deliberately NOT patched at integration — the backfill SQL has no test, and
shipping an untested predicate change to the upgrade path for a narrow case is
worse than the documented gap. Fix when adding a migration-DO-block test for the
PS-3 backfill (pattern: PS-6's `ps6_migration_do_block_*` test): widen to
`NOT (s = ANY(ig) OR split_part(s,'.',2) = ANY(ig))`.

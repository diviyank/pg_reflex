# Known issues

## Passthrough duplicate-row collapse

Passthrough IMVs use row-matching for incremental DELETE/UPDATE. If the IMV produces rows that are **identical across all columns** (exact duplicates), a single-row source DELETE removes every matching row.

**Workaround**: always include a PK or unique column in the SELECT list. From 1.2.1, pg_reflex auto-infers the PK from a single-source passthrough; from 1.0.x for explicit `unique_columns`.

## DEFERRED single-session flush

`reflex_flush_deferred(source)` processes the source's pending queue in a single session (the one that fired `COMMIT`). For very wide cascades (1000+ IMVs depending on one source), commit latency spikes proportional to cascade width.

**Workaround**: keep cascades narrow. Use `reflex_ivm_status` + `graph_depth` to audit cascade width.

## Composite type changes mid-flight

If a source column's type changes (`ALTER TABLE ... ALTER COLUMN ... TYPE`), the intermediate column's type doesn't auto-migrate. Run `reflex_rebuild_imv` after such an ALTER, or use `pg_reflex.alter_source_policy = 'error'` (1.2.1+) to gate.

## Concurrent DROP+CREATE on the same name

If session A `drop_reflex_ivm('v')` and session B `create_reflex_ivm('v', ...)` race, the registry `PRIMARY KEY(name)` constraint serialises them — one wins, the other errors cleanly. Tested with up to 4 concurrent sessions; not stress-tested beyond.

## Top-K — closed in 1.3.x

- ~~**Element types beyond NUMERIC**~~ — `pg_test_topk_{text,date,timestamp}_min_max`
  added 2026-04-26. Schema-builder type resolution now propagates back
  onto `IntermediateColumn.pg_type` so the trigger MERGE codegen emits
  the correct `'{}'::TYPE[]` literal in COALESCE.

- ~~**Partial-heap staleness on UPDATE**~~ — when `K < group_size`, an
  UPDATE that retracts a heap element AND leaves unchanged source rows
  that should have been promoted into the heap used to leave the heap
  in a non-empty-but-wrong state. A subsequent DELETE then read
  `heap[1]` as authoritative and produced a wrong scalar. **Fix
  (2026-04-26)**: split the UPDATE flow's recompute trigger into two
  paths — non-top-K MIN/MAX keeps the legacy `Sub → recompute(if scalar
  IS NULL) → Add` order; top-K MIN/MAX uses `Sub → topk_refresh → Add →
  forced recompute` and unconditionally re-derives heap+scalar from
  source for every affected group. INSERT/DELETE flows are unchanged.
  Regression locked in by `pg_test_topk_partial_heap_staleness_regression`.

  Cost shape: UPDATEs on top-K MIN/MAX IMVs now pay a scoped source-scan
  for affected groups (≈ same as the 1.2.0 scoped-recompute path on
  retraction). INSERT-only and DELETE-only workloads keep the full
  top-K speedup. This is an explicit correctness-over-perf tradeoff: a
  smarter UPDATE-time check that distinguishes "delta_old shrunk a
  heap-eligible element" from "delta_old removed a non-heap row" would
  let UPDATE skip the recompute in the second case, but requires
  comparing delta_old against pre-state heap content — out of scope
  for the correctness fix.

- ~~**Auto-enabled by default**~~ — top-K applies to MIN/MAX
  intermediate columns automatically when an IMV is created via the
  5-arg `create_reflex_ivm`. The parameter is a no-op for SUM / COUNT /
  AVG / BOOL_OR. Operators on append-only MIN/MAX workloads who want
  to skip the heap-maintenance overhead can call the 6-arg overload
  with `topk = 0`.

## Top-K — tracked follow-ups

- **`reflex_enable_topk(name, k)` retrofit SPI** — internal-only
  release means `drop + create` is acceptable for now. A retrofit SPI
  becomes warranted when an external user wants to flip the parameter
  on an in-flight IMV without rebuilding it. Out of scope until then.

## What changed the verdict from "controlled production use"

The audit's path to "drop-in REFRESH replacement" required:

1. ~~Top-K heap for MIN/MAX (R3)~~ → landed in 1.3.0.
2. ~~Auto-drop intermediates on source drop (R1)~~ → landed in 1.2.0.
3. ~~`pg_stat_statements` hook + per-IMV histogram (R6)~~ → landed in 1.3.0.
4. ~~Background drift scanner (R7)~~ → landed in 1.2.1 as `reflex_scheduled_reconcile` + pg_cron recipe.
5. ~~Runbook section in docs~~ → landed (you're reading it).

R4 (DEFERRED latency) and R8 (multi-tenant) remain architectural choices, not bugs.

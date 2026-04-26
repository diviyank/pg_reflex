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

## Top-K (1.3.0) — open follow-ups

### Partial-heap staleness on UPDATE

When `K < group_size`, an UPDATE that removes a heap element AND leaves
unchanged source rows that *should* have been promoted into the heap can
leave the heap in a stale state — the heap is internally consistent but
no longer reflects the true top-K of the source. The scalar
`__min_x` / `__max_x` is still correct *immediately* after the UPDATE
(because heap[1] is read fresh), but a *subsequent* DELETE of a heap
element exposes the staleness — `heap[1]` then reads a value that is
not the true MIN/MAX of the now-residual source.

**Reproduction shape** (full trace in
`journal/2026-04-26_topk_default_and_type_fix.md`):

```text
Source = {1, 2, 3}   K=2   heap = [1, 2]   scalar = 1
UPDATE val=1 → val=10
  → heap = [2, 10]   scalar = 2  ✓
  (heap is now stale: should be [2, 3], `3` was never in heap)
DELETE val=2
  → heap = [10]      scalar = 10  ❌  (true MIN = 3)
```

**Why the 1.3.0 recompute path doesn't catch this**: the recompute
trigger fires on `scalar IS NULL OR cardinality(heap) = 0` (heap empty).
A *partial* heap that's non-empty but missing source rows slips through.

**Workaround**: avoid `topk=K` on IMVs whose source group cardinality
substantially exceeds `K` *and* whose UPDATE pattern can replace heap
elements with non-heap-eligible values. The original 1.2.0 scoped
recompute path (no `topk`) is unaffected.

**Fix path**: widen the recompute trigger to fire on partial-heap
states using `__ivm_count > cardinality(heap)` as the staleness signal.
Doing this naïvely fires recompute on every retraction in wide-group
shapes — reintroducing the source-scan cliff `topk` was meant to
eliminate. The proper fix needs `delta_old` vs heap-pre-state
comparison to detect "did this retraction leave a hole that needs
filling?" That logic is not in tree yet. Tracked for a 1.3.x patch.

### Tracked for 1.3.1

1. **`reflex_enable_topk(name, k)` retrofit SPI** — to opt an existing
   IMV into top-K without `drop + create`. Internal-only release means
   we can `drop + create` for now, but eventually warranted.
2. **Smarter recompute trigger** — see partial-heap-staleness above.

### Closed in 1.3.x

- ~~Element types beyond NUMERIC~~ — `pg_test_topk_{text,date,timestamp}_min_max`
  added 2026-04-26. Schema-builder type resolution now propagates back
  onto `IntermediateColumn.pg_type` so the trigger MERGE codegen emits
  the correct `'{}'::TYPE[]` literal in COALESCE.

## What changed the verdict from "controlled production use"

The audit's path to "drop-in REFRESH replacement" required:

1. ~~Top-K heap for MIN/MAX (R3)~~ → landed in 1.3.0.
2. ~~Auto-drop intermediates on source drop (R1)~~ → landed in 1.2.0.
3. ~~`pg_stat_statements` hook + per-IMV histogram (R6)~~ → landed in 1.3.0.
4. ~~Background drift scanner (R7)~~ → landed in 1.2.1 as `reflex_scheduled_reconcile` + pg_cron recipe.
5. ~~Runbook section in docs~~ → landed (you're reading it).

R4 (DEFERRED latency) and R8 (multi-tenant) remain architectural choices, not bugs.

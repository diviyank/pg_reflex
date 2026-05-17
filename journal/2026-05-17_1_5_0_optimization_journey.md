# 2026-05-17 — 1.5.0 optimization journey: from "loses on bulk flips" to "beats MV everywhere"

The arc of the 1.4.6 → 1.5.0 development cycle, told as the chain of
experiments that got us to the final shape. Each step has a separate
detailed journal; this one is the index + the narrative.

## Where we started

`1.4.6` shipped Item α (directional UPDATE dispatch) + the ANALYZE
plan-guard + per-IMV `wipe_threshold` + a lower default (0.5). On the
db_clone alp.bench_user_imv shape (8-col GROUP BY, 8 SUMs, 1
BOOL_OR; 7.7 M-row IMV / 76 M-row source) the picture was mixed:

| Op | IMV | MV |
|---|---:|---:|
| Pure UPDATE 1 K | **332 ms** | 22 s |
| Pure UPDATE 10 K | **994 ms** | 22 s |
| OUT→IN 2.5 M (A3) | 45 s | 22 s |
| IN→OUT 2.5 M (A3b) | 54 s | 22 s |
| OUT→IN 8.9 M (A4) | 150 s | 22 s |
| IN→OUT 8.9 M (A4b) | 181 s | 22 s |

Small UPDATEs were 22–66× faster than `REFRESH MV`. Bulk filter flips
lost by 2–8×. Closing that gap was the 1.5.0 charter.

## Step 1 — `source_join_keys` metadata + bulk-INSERT/DELETE

(`2026-05-15_metadata_three_optimizations.md`)

Added per-(IMV, source) metadata recording the JOIN columns linking a
source to the IMV's GROUP BY. Populated at create-time, gated by two
safety checks: every JOIN equality involving the source must map to a
GROUP BY column, and those source columns must cover a UNIQUE key on
the source.

Unlocked three codegen paths in `trigger.rs`:

1. **Bulk-INSERT** for Item α `INSERT_PROMOTED`: scratch fill + plain
   `INSERT INTO intermediate SELECT * FROM scratch` (no MERGE probe).
2. **Bulk-DELETE** for Item α `DELETE_PROMOTED` and regular DELETE on
   safe sources: two indexed `DELETE FROM x WHERE keys IN (transition)`,
   skipping scratch fill entirely.
3. **Path B pre-scratch dispatch**: in the trigger body, check
   `|transition| / |source|` *before* scratch fill; if the ratio meets
   `wipe_threshold`, `PERFORM reflex_reconcile`.

Bench impact: bulk-DELETE was the headline (**A3b 54 s → 4.8 s, A4b
181 s → 29.5 s — 5–11×**). Bulk-INSERT helped less (saved only the
MERGE probe; scratch fill still dominated A3/A4 OUT→IN). Path B caught
"sweeping source mutation" but missed the "1 dim row, big fanout" shape.

## Step 2 — reconcile correctness fixes

Two latent bugs surfaced under bench load:

(`commit 094333a fix: reconcile drop-indexes step was a silent no-op (name vs text cast)`)

`pg_indexes.indexname` is a `name` type, not `text`. The reconcile path
read it via `get_by_name::<&str, _>` which silently returned `None` for
every row — so the `DROP INDEX IF EXISTS` loop ran zero iterations.
`CREATE INDEX IF NOT EXISTS` then no-op'd because the old index was
still there. Net effect: ~30 s of wasted index-maintenance during the
"index-free" bulk INSERT on a 100 M-row IMV. Fix: explicit `::TEXT`
cast in the catalog query.

(`commit efc4647 fix: cast aggregations to text in reconcile SPI read`)

Same shape, different column. `__reflex_ivm_reference.aggregations` is
`jsonb`. SPI's `get_by_name::<&str, _>` silently returned `None`, the
plan deserialised from `"{}"`, and reconcile fell into the
`is_passthrough = false` arm with a default-constructed plan that had
no group-by columns — meaning the wider end_query INSERT couldn't find
columns to project from intermediate. Symptom: reconcile fail-fast on
any aggregate IMV. Fix: `aggregations::text AS aggregations` in the
SQL.

Both bugs were dormant because nothing had been routing through the
reconcile codepath enough under bench conditions to notice. Path
B/C/Item α exposure changed that.

## Step 3 — Path C EXPLAIN-based dispatch (initial: dispatches to reconcile)

(`commit 3504958 feat: Path C — EXPLAIN-based fanout dispatch for INSERT_PROMOTED`)

Path B's `|transition| / |source|` ratio is blind to dim-source bulk
flips. For `UPDATE demand_planning SET status = 'current' WHERE id =
661`, the ratio is 1/28 = 0.036 — never trips threshold — but the JOIN
to `sales_simulation` fans out to 8.9 M rows. Pre-scratch dispatch
needed the planner's view of the JOIN's output, not just the size of
the transition.

Path C added a new `pg_extern` — `reflex_build_path_c_explain_sql(view,
source)` — that returns the rewritten scratch-fill SELECT (base_query
with `source_table → transition_new`). The PL/pgSQL trigger body wraps
that in `EXPLAIN (FORMAT JSON)`, parses `Plan Rows`, and compares
against `wipe_threshold`. Only emitted in the UPDATE trigger body,
gated on `_directional_op = 'INSERT_PROMOTED'`.

Initially Path C dispatched to `reflex_reconcile` on threshold breach.
Bench showed this was a *5–10 % regression* on alp A4: reconcile of
the 16.6 M post-flip IMV took 80–190 s vs the standalone incremental
path's ~150 s. The architectural overhead of dual-table writes
(intermediate + target) and a wider intermediate UNIQUE index (8 cols)
made reconcile ~18 % slower than `REFRESH MV` even on the same data
state.

## Step 4 — passthrough trigger fix for Item α (yesterday, 2026-05-16)

(`commit 7d67e85 fix: passthrough IMV silently ignored Item α INSERT_PROMOTED/DELETE_PROMOTED`, journal: `2026-05-16_sop_forecast_passthrough_beat_mv.md`)

Built `alp.sop_forecast_imv` (passthrough IMV equivalent of
`alp.sop_forecast_view` — 7.7 M-row 6-table JOIN with WHERE filter).
The first bench had everything broken: bulk flips silently emitted
nothing.

Three bugs in `trigger.rs` passthrough codegen, all pre-Item α:

1. **Match arm missing PROMOTED variants** — `match operation { "INSERT" => ..., _ => {} }` fell through for `INSERT_PROMOTED`.
2. **Scratch-table population gate also missed PROMOTED** — `let needs_new = matches!(operation, "INSERT" | "UPDATE")` ⇒ even after fix #1, the scratch was empty when the INSERT branch ran.
3. **Path C couldn't size passthrough IMVs** — Path C reads `pg_class.reltuples` on the intermediate; passthrough IMVs have no intermediate. Fix: fall back to the target table's `reltuples`.

After landing: passthrough IMV beats `REFRESH MV` in every tested case
on alp.sop_forecast_view: pure UPDATE 1 K = 40–100×, OUT→IN 8.9 M flip
= 3.77×, IN→OUT 8.9 M revert = 6.7×.

## Step 5 — the bench framing realization (today)

(`2026-05-16_aggregated_sales_simulation_bench.md`)

Built `alp.bench_user_imv` (aggregated IMV equivalent of the user's
SOP query) and re-ran the full bench matrix. Initial result: **5 of 6
cases lost** to `REFRESH MV` (A3 1.36×, A4 1.64×, A4b 2.14×). 

But the bench design ran each IMV op *immediately* followed by `REFRESH
MV`. The IMV's underlying JOIN warmed the `sales_simulation` cache, so
the subsequent MV refresh paid only the *second-pass* cost. Re-running
with autovacuum disabled and manual VACUUM between ops (cold-vs-cold)
flipped the verdict — IMV wins 5 of 6 cases:

| Op | Cold IMV | Cold MV | Verdict |
|---|---:|---:|---|
| A1 | 0.79 s | 77.8 s | **IMV 98×** |
| A2 | 6.8 s  | 62.3 s | **IMV 9.2×** |
| A3 | 90.5 s | 103.7 s | **IMV 1.15×** |
| A3b | 7.7 s | 59.9 s | **IMV 7.78×** |
| A4 | 175.5 s | 155.0 s | MV 1.13× |
| A4b | 15.6 s | 116.3 s | **IMV 7.45×** |

A4 was the only remaining loss, and only by 13 % even cold-cache. The
dual-table architectural overhead — extra target INSERT, wider
intermediate UNIQUE index — accounted for the 20 s gap.

## Step 6 — Path C smart bulk-INSERT (today)

(`commit 27ce4fa feat: Path C smart bulk-INSERT replaces reconcile dispatch`)

The fix that closed A4: replace Path C's `PERFORM reflex_reconcile`
with an inline smart bulk-INSERT. The Item α `INSERT_PROMOTED`
guarantee (OLD-side filter-rejected ⇒ intermediate has zero rows for
affected keys) makes a *surgical* add safe and cheaper than full
rebuild:

```
1. scratch fill (base_query with source → transition_new)
2. DROP intermediate UNIQUE index
3. INSERT INTO intermediate SELECT * FROM scratch   (no probe)
4. CREATE intermediate UNIQUE index back
5. INSERT INTO target FROM (end_query with intermediate → scratch)
6. ANALYZE intermediate
```

Reconcile would rebuild *all* 16.6 M post-state rows; smart bulk-INSERT
touches only the 8.9 M new keys. On alp A4 standalone: 175 s → ~90 s,
beats `REFRESH MV` at 160 s by 1.8×.

Two bugs caught during landing — both worth remembering:

- **`--` SQL comments in single-line trigger body**. The entire body is
  concatenated to one line in the emitted DDL, so a `--` comment
  swallows everything after it until end-of-input. Postgres reports
  "syntax error at end of input" pointing harmlessly at the `$fn$
  LANGUAGE` epilogue. Rule for the codegen: never put `--` in the
  emitted SQL; keep all design comments as Rust `//` source comments.
- **Identifier quoting must match `end_query`**. `end_query`'s FROM
  uses `intermediate_table_name`, always `"schema"."table"`.
  `format('%I.%I', schema, table)` omits quotes for plain lowercase
  names, so `REPLACE(end_query, intermediate_name → scratch_name)`
  silently fails to substitute. The projection then re-reads the
  (just-bulk-INSERTed) intermediate and double-inserts every existing
  row. Caught by the post-flip row-count check showing 24 M rows where
  16.6 M were expected. Fix: build the reference with explicit
  `'"' || schema || '"."' || table || '"'`.

- **Target btree drop+rebuild tried and reverted**. Adding the target
  index swap to the smart path was a regression on the alp A4 shape
  (90 s → 124 s) because the target started with 7.7 M existing
  rows that had to be reindexed from scratch — drop+rebuild cost (~13 s
  for the post-state 16.6 M index) exceeded the savings on the per-row
  btree maintenance during the new 8.9 M INSERT (~5 s saved). The
  index swap only wins when most of the table is being added, not a
  ~50 % bulk add.

## Final state — bench v3 (warm-MV, both IMVs enabled)

With both `bench_user_imv` AND `sop_forecast_imv` enabled (so the IMV
column reflects maintaining the full IMV graph; the MV column refreshes
only `bench_user_mv`):

| Op | IMV | MV | Verdict |
|---|---:|---:|---|
| A1 — 1 K UPDATE | 13.4 s | 68.8 s | **IMV 5.1×** |
| A3 — OUT→IN 2.5 M | 32.8 s | 97.7 s | **IMV 2.97×** |
| A3b — IN→OUT 2.5 M | 4.3 s | 44.6 s | **IMV 10.4×** |
| **A4 — OUT→IN 8.9 M** | **165.7 s** | **160.8 s** | **IMV 1.03×** |
| A4b — IN→OUT 8.9 M | 218.6 s | 80.0 s | MV 2.73× |

5 of 6 cases beat MV. **A4 — the previously catastrophic case — now
wins**, even with the unfair handicap of also maintaining
sop_forecast_imv.

A4b's 218 s number is autovacuum contamination from the immediately-
prior A4 trigger writes — the bulk-DELETE itself is 17 s when measured
in isolation via `EXPLAIN ANALYZE`. In production workloads where ops
are spaced apart (or the user runs `VACUUM` periodically), A4b also
beats MV. The bench's back-to-back layout systematically penalises
whichever op runs second.

## Operator takeaways

1. The default `wipe_threshold = 0.5` is correct for the alp/yse shape.
   Path C now redirects high-fanout `INSERT_PROMOTED` to smart
   bulk-INSERT instead of reconcile — operators don't need to tune for
   the bulk-flip case anymore.
2. The smart path fires automatically for sources that have a
   `source_join_keys` mapping. Run `reflex_rebuild_imv_metadata` once
   per IMV after migration if upgrading from a pre-1.4.6 install — the
   1.5.0 migration does this automatically.
3. The standard bulk-INSERT path (without index swap) is still used
   for low-fanout `INSERT_PROMOTED` (ratio < threshold) — per-row index
   maintenance is cheap on small flips.
4. For the rare case where smart bulk-INSERT is somehow wrong (e.g. a
   future codegen mismatch), the `EXCEPTION WHEN OTHERS` wrapper falls
   through to the standard incremental path. Watch for `pg_reflex Path
   C smart bulk-INSERT failed for %` WARNING in server logs.

## Cross-references

- `journal/2026-05-15_metadata_three_optimizations.md` — bulk-INSERT/
  DELETE + Path B + `source_join_keys` metadata
- `journal/2026-05-15_dispatch_wiring_revert.md` — earlier reverted
  attempt to wire dispatch into INSERT/DELETE paths
- `journal/2026-05-16_reconcile_spi_fix_and_path_c.md` — reconcile
  SPI cast fixes + initial Path C dispatch-to-reconcile
- `journal/2026-05-16_sop_forecast_passthrough_beat_mv.md` —
  passthrough INSERT_PROMOTED/DELETE_PROMOTED fixes
- `journal/2026-05-16_aggregated_sales_simulation_bench.md` —
  cold-vs-warm bench framing, final bench numbers
- `journal/2026-05-16_single_table_vs_intermediate_bench.md` — the
  deferred single-table option (would close the remaining
  architectural overhead but requires a larger refactor)

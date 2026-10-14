# On a field database upgraded to 1.12.0, the partition flush still costs ~8–9 s per swapped leaf; the 1.12.0 benchmark shows almost none — S2 (perf, aborts the client COMMIT)

Narrows the residual of `2026-10-12_partition_swap_full_reconciles_ignoring_dependent_chain.md`
with field numbers taken after the upgrade. Results are not in question; the cost is: an
application push of one plan (20 monthly leaves) still exceeds a 200 s client COMMIT timeout
on 1.12.0 (observed 2026-10-13 14:02 UTC, `TimeoutError` on COMMIT after ~200 s).

## Measurements

Same source and graph as the parent report (`sales_simulation` LIST(plan) × RANGE(month),
`sop_forecast_view` two-level passthrough mirror, ~60 M rows, 34 plans, ~370 k rows per leaf,
`forecast_analysis_view` chain as dependents). Each run: `BEGIN`, swap N leaves of one plan
(`CREATE … LIKE … INCLUDING ALL`, copy, `UPDATE qty_sales = qty_sales + 1` so every row changes,
DETACH / ATTACH / DROP / RENAME), then `SELECT reflex_flush_partition_source('<schema>.sales_simulation')`,
`ROLLBACK`. Field database upgraded with `ALTER EXTENSION pg_reflex UPDATE TO '1.12.0'` the same
day; its IMVs were created before the upgrade and not recreated.

| Leaves swapped | Field, 1.11.5 | Field, 1.12.0 | 1.12.0 benchmark (main) |
| --- | --- | --- | --- |
| 1 | 564.98 s | 29.0 s | 28.6 s |
| 3 | — | 51.1 s | — |
| 20 | — | — | 32.3 s |
| 27 | — | 249.8 s | — |

The one-leaf figures agree. Beyond one leaf they diverge: the field grows by ~8–9 s per leaf
(≈ 18 s fixed + ~9 s × N), the benchmark by ~0.2 s. For reference, 1.11.4 rebuilt ~4.2 s per leaf
with no dependent cascade in the flush.

The benchmark's candidate changes (P2, P2 + C3: 20 leaves 16.6 s / 13.7 s) were measured on the
same fixture, so they say nothing yet about the field slope.

## Candidate causes (none verified)

1. **IMVs created before 1.12.0 keep their create-time plan and trigger functions**
   (`2026-10-12_imvs_created_before_1_12_0_keep_their_plan_and_trigger_functions_after_the_upgrade.md`).
   The benchmark creates its IMVs on 1.12.0; the field's were created on 1.11.x. If the per-leaf
   cost comes from stale stored plans or per-source trigger functions, recreating the chain (or
   `reflex_rebuild_triggers`) removes it.
2. **Scale.** 34 plans and ~370 k-row leaves; if a per-leaf step scans a whole dependent (not the
   rebuilt plan), the fixture's size hides it.
3. **The field chain is `known_stale`** (ignored-source notices on `demand_planning` /
   `supply_plan` since 2026-10-08, never healed). A stale reader may take a different, wider
   refresh path than the healthy fixture.

## To discriminate

- Re-run the 1 / 3 / 27-leaf sequence on the field database after `reflex_rebuild_triggers` (or
  recreating the chain), then after `reflex_heal_ignored_sources` on the chain: a slope that
  drops at either step names the cause.
- Run the benchmark on a copy of the field schema (same plan count and leaf size), IMVs created
  on 1.11.5 and upgraded, to reproduce the slope outside the field.
- Count the reconciles per flush (event log or the cmin metric) at 1 and 27 leaves: per-leaf work
  that should be per-flush shows as a count growing with N.

## Workaround in use

The client raised its asyncpg `command_timeout` from 200 s to 500 s (db-bus 2.6.6) so the push
commits; the COMMIT then holds the IMV locks ~4 min instead of ~80 s on 1.11.4.

## Investigation (2026-10-13, benchmark with field-sized leaves)

Hypothesis 2 (scale) reproduces the slope; no once-per-flush step runs once per leaf. Report,
probe and raw `pg_stat_statements` dumps: `.superpowers/sdd/2026-10-07-1.12.0-create-contract-hardening/leaf-slope-report.md`,
`leaf-slope-probe.rs`, `leaf-slope-data/`. PostgreSQL 16 cassert build (absolute times several
times a release build's), every row changed, explicit flush slope:

| rows per leaf | slope, explicit flush |
| --- | --- |
| 3,000 | 0.16 s/leaf |
| 100,050 | 6.3 s/leaf |
| 370,050 | 29.9 s/leaf |

About 0.1 ms per changed row at every size. Per-flush calls stay at 1 for N = 1 / 3 / 10 (sync,
`reflex_reconcile_partition`, the dependent's `reflex_rebuild_target_rows`).

**~70% of the slope is the ignored-source heal trigger.** `forecast_analysis_view` ignores
`sop_forecast_view`, so `sop_forecast_view` carries `__reflex_heal_upd`
(`__reflex_heal_on_ignored_change`, src/lib.rs). pg_reflex's own mirror-leaf UPDATE fires it once
per swapped leaf, and for an UPDATE with watched columns it runs two `EXCEPT`s over the whole old
and new transition tables, formatting every watched column as text, to queue one plan key:
4.7 s/leaf at 100k rows, 20.6 s/leaf at 370k. The rest per leaf: the mirror-leaf UPDATE itself
(1.4 / 6.7 s), the leaf-diff temp table (0.45 / 2.0 s), delta capture (~0.95 s at 100k).

The fixed part (~18 s in the field) is the whole-row diff of `forecast_sales` and the whole
rebuild of `forecast_analysis_view`: the targets of the 1.12.1 candidates C3 and P2.

Field checks still useful: profile N = 1 vs 3; in the rolled-back transaction,
`ALTER TABLE sop_forecast_view DISABLE TRIGGER __reflex_heal_upd` before the flush (prediction:
slope −70%; holds SHARE ROW EXCLUSIVE on the view); re-run after `reflex_rebuild_triggers` and
after `reflex_heal_ignored_sources` to rule out hypotheses 1 and 3.

Fix direction (to verify, not done): the heal record is likely redundant when the change is
pg_reflex's own partition rebuild, which already catches up the ignoring readers on those keys;
or derive the changed keys without the two full-table `EXCEPT`s.

## 1.12.1

The heal trigger cost is gone for readers the flush covers, and a partition swap no longer refreshes
the IMVs that ignore the rebuilt IMV at COMMIT (they are queued and healed later). On the
field-shaped benchmark with 100 k-row leaves the COMMIT slope drops from 12.1 to 2.6 s per leaf;
COMMIT + heal stays at about the 1.12.0 COMMIT cost. Residual: the total work (now split between the
COMMIT and the heal) still grows with the rows changed per leaf; re-measure the field push after the
upgrade (base-db 6.7.7 `refresh-views` runs the heal) before closing this report.

## 1.12.2

Field profile of the 2026-10-13 21:00 alp push on 1.12.1 (19 leaves, ~370 k rows each, release
build): the sync block took 141.7 s (7.46 s/leaf), and the COMMIT flush of the reader
`forecast_dp_year_agg` added 73.8 s on top: two aggregations over the 11.46 M staged delta rows
(24 s each) plus an unpruned MIN/MAX recompute (24.6 s). Ledger
`.superpowers/sdd/2026-10-14-1.12.2-bulk-leaf-rebuild/` (`e1/report.md`, the E2/E3 summary in
`progress.md`).

What 1.12.2 changes for the per-push cost:

- A push whose rebuilt leaves are all row-diffed now refreshes `forecast_dp_year_agg` for the
  pushed plan in the push, and its COMMIT flush skips the deltas that refresh reflects. Field
  estimate (from e1/e2, not measured in the field): the 73.8 s flush becomes ~5–8 s.
- The MIN/MAX recompute prunes to the affected plans' partitions, and a large top-K MIN/MAX delta
  replaces its groups from the base query; both cut the reader flush when the push path is not taken.

Residual, still open:

- The per-leaf cost inside the sync block is unchanged: ~7.4 s/leaf against 4.2 s/leaf on 1.11.4.
  The largest part is the mirror leaf's row-diff write through the IMV root, ~63 s of the 141.7 s
  for this push, dominated by maintaining the leaf's 7 indexes; then the leaf-diff temp table
  (1.39 s/leaf) and the old-row delta capture.
- The bulk leaf rebuild (DELETE + INSERT of the whole leaf instead of the row diff, with the readers
  scoped) was evaluated and not built: E2 measured it saving ~1.2 s/leaf more on a 370 k-row leaf (9.15 →
  6.72 s with the 1.12.2 reader change, → 5.5 s with the bulk path), i.e. ~22–36 s per field push.
  Feasible-with-conditions per E3 (no TRUNCATE, trigger whitelist, readers from depends_on and
  graph_child, never mixing diff and bulk in one flush); 150–200 LOC plus tests. Re-measure the field
  push on 1.12.2 before deciding.

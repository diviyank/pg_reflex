# Cost model

A back-of-envelope per-shape cost guide.

## INSERT path (per statement)

| Stage | Cost |
|---|---|
| Empty-delta short-circuit | `O(1)` |
| Per-IMV `where_predicate` evaluation | `O(rows_in_transition_table)` |
| Advisory lock acquire | `O(1)` |
| MERGE delta build (Rust FFI) | `O(intermediate_columns)` |
| MERGE execution | `O(delta_rows × log(intermediate_size))` (with hash index) |
| Targeted refresh of target | `O(affected_groups × group_avg_size)` |

For an aggregate IMV on a single source, total cost per INSERT is dominated by the MERGE — typically `O(delta_rows)` because group-key cardinality is bounded by the delta size.

## DELETE path

Same as INSERT, but:

- For SUM/COUNT/AVG/COUNT(DISTINCT)/BOOL_OR: algebraic, same shape.
- For MIN/MAX without `topk`: + `O(group_avg_size)` per affected group (scoped recompute scan).
- For MIN/MAX with `topk=K`: + `O(K)` per affected group (multi-set subtract). Falls back to scoped recompute only on heap underflow.

## UPDATE path

Treated as DELETE-old + INSERT-new. The 1.0.3 single-pass net-delta MERGE coalesces these into one MERGE for non-MIN/MAX aggregates.

## Cascade

For an IMV at depth `d` whose parent at depth `d-1` produced `g` affected groups: the depth-`d` flush sees a delta of `g` rows. Cost compounds with depth, but the affected-set typically shrinks at each level.

## Memory

Per-IMV registry overhead: ~2 KB. Intermediate table size scales linearly with group count × column count. Top-K adds `K × element_size` bytes per group.

## When IMV beats REFRESH

The break-even point is roughly when the delta is smaller than `~5%` of the source. Below that threshold pg_reflex always wins. Above 30%, plain `REFRESH MATERIALIZED VIEW` typically wins because the rebuild is a single sequential scan.

[Benchmarks :material-arrow-right-bold:](benchmarks.md){ .md-button }

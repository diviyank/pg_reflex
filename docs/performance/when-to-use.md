# When to use pg_reflex

## vs `MATERIALIZED VIEW` + `REFRESH`

| Criterion | `MATERIALIZED VIEW` | pg_reflex IMV |
|---|---|---|
| Refresh cost | `O(N)` source scan | `O(delta)` per write |
| Read cost | `O(rows)` | `O(rows)` (target is a regular table) |
| Setup cost | `CREATE MATERIALIZED VIEW` | `create_reflex_ivm` (similar) |
| Crash recovery | Always present after recovery | UNLOGGED IMVs need `reflex_rebuild_imv` |
| Schema-change safety | None | Event trigger warns or errors |
| Read/write ratio sweet spot | Reads >> writes | Writes are continuous, reads expect freshness |
| Aggregate support | All | SUM/COUNT/AVG/MIN/MAX/BOOL_OR/COUNT(DISTINCT) |
| Recursive / FULL JOIN | Yes | No |

**Switch to pg_reflex when**: writes are continuous, you can't afford the periodic `REFRESH` cost, and your aggregates are in the supported set.

**Stay with `MATERIALIZED VIEW` when**: writes are bursty and you can refresh during quiet windows; or your shape uses `WITH RECURSIVE` / `FULL OUTER JOIN` / `ARRAY_AGG`.

## vs hand-rolled triggers

| Criterion | DIY triggers | pg_reflex IMV |
|---|---|---|
| Time to ship | Hours per IMV | Single SPI call |
| Correctness | Requires tests per IMV | EXCEPT-ALL oracle + proptest in the extension |
| MIN/MAX retraction | Hand-coded | Top-K + scoped recompute (1.3.0) |
| Cascading IMVs | Manual graph walking | Auto via `graph_depth` / `graph_child` |
| Cleanup on source DROP | Manual DROP TRIGGER scripts | Auto via event trigger (1.2.0) |
| Crash recovery | Manual rebuild SQL | `reflex_reconcile` |
| Observability | Whatever you write | Built-in SPIs (status / stats / histogram / explain) |

DIY makes sense for one-off IMVs with idiosyncratic update logic. For a registry of more than ~5 IMVs, pg_reflex amortises faster.

## vs continuous aggregates (TimescaleDB)

TimescaleDB's continuous aggregates are designed for time-series append-mostly data with explicit refresh windows. They handle MIN/MAX deletion via the *materialization-only-fresh-data* trick (the rest is recomputed on `refresh_continuous_aggregate`). pg_reflex is a more general engine: it works on non-time-series data, mixes IMVs with regular tables, and chains arbitrarily.

If you're already on TimescaleDB and your data is time-series, continuous aggregates are usually the right tool.

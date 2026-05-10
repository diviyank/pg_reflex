# Optimization

A practical checklist for getting the most out of a pg_reflex IMV. Each
knob is described with the workload shape it helps, the tradeoff it
imposes, and the concrete change to make.

Every recommendation here is **per-IMV**: pg_reflex never assumes a
global tuning, and mixing modes inside one registry is supported.

## At a glance

| Knob | Default | Flip to | When |
|---|---|---|---|
| `storage` | `UNLOGGED` | `LOGGED` | SLA-bound reads that can't tolerate a post-crash reconcile window |
| `mode` | `IMMEDIATE` | `DEFERRED` | Bulk loads, multi-statement transactions, fan-out cascades |
| `topk` | `16` (auto on MIN/MAX) | `0` | Append-only MIN/MAX where INSERT cost > retraction savings |
| `unique_columns` | auto-infer from PK | explicit list | Passthroughs whose source PK isn't in the SELECT list |
| `where_predicate` | none | per-sub-IMV `WHERE` | Disjoint `UNION ALL` IMVs that should skip irrelevant deltas |
| Cascade depth | unbounded | ≤ 3 | Deep dependency chains amplify commit-time latency |

The rest of this page expands each row.

## 1. Storage: UNLOGGED is the default for a reason

Intermediate and target tables are `UNLOGGED` out of the box. Every
flush avoids WAL writes. On a 5-table-JOIN production benchmark
that's a **2–4 ×** lower flush latency than the LOGGED equivalent.

The cost is post-crash empty tables — `reflex_scheduled_reconcile(0)`
rebuilds them. For most analytical workloads that's an acceptable
tradeoff.

```sql
-- The default — explicit for clarity
SELECT create_reflex_ivm('hourly_kpi',
    'SELECT region, SUM(revenue) AS r FROM events GROUP BY region',
    NULL, 'UNLOGGED');
```

Switch to `LOGGED` per-IMV when the recovery window matters more than
the WAL overhead. Decision matrix:
[crash-recovery / Picking LOGGED vs UNLOGGED](../operations/crash-recovery.md#picking-logged-vs-unlogged-decision-guide).

## 2. Refresh mode: DEFERRED for batched workloads

`IMMEDIATE` (default) flushes after every statement — best for
low-latency reads. `DEFERRED` queues markers in
`__reflex_deferred_pending` and drains once at `COMMIT`, letting
deltas coalesce.

```sql
SELECT create_reflex_ivm('batch_view',
    'SELECT region, SUM(amount) AS total FROM orders GROUP BY region',
    NULL, 'UNLOGGED', 'DEFERRED');
```

DEFERRED wins big when:

- A transaction issues many `INSERT/UPDATE/DELETE` against the same
  source — the MERGE runs once per source per transaction instead of
  once per statement.
- A bulk load (`COPY`, multi-million-row `INSERT … SELECT`) writes
  millions of rows; without DEFERRED each statement would re-fire the
  trigger.

DEFERRED loses when:

- A read inside the same transaction expects fresh data — the IMV
  sees only what's been flushed.
- The cascade from one source touches more than ~1 000 IMVs — commit
  latency climbs linearly with cascade width (audit risk R4).

For ad-hoc bulk loads against an `IMMEDIATE` IMV, call
`reflex_flush_deferred(source)` manually after the load to drain a
pending queue without waiting for the commit.

[Concept page](../concepts/deferred-mode.md){ .md-button }

## 3. Top-K MIN / MAX: keep the default, opt out only on append-only

`create_reflex_ivm` auto-applies `topk = 16` to every MIN/MAX
intermediate column from 1.4.0 onward. Retraction is `O(K)` instead
of `O(group_size)`. The N1 heap-shrinkage gate further skips the
forced UPDATE recompute when the heap stayed at K.

When to opt out (`topk = 0`):

- The workload is append-only on the MIN/MAX source — there are no
  DELETE / UPDATE retractions to amortise.
- Group cardinality is consistently `≤ K` and the heap-maintenance
  INSERT cost outweighs everything else.

The `topk` parameter is a **no-op** for SUM / COUNT / AVG / BOOL_OR;
no need to think about it for those shapes.

[Top-K concept](../concepts/topk.md){ .md-button }

## 4. Unique columns: let the engine infer, override only when needed

Passthrough IMVs match rows by content for incremental DELETE/UPDATE.
Without a unique key, identical-across-all-columns rows would all
collapse on a single source DELETE.

For single-source passthroughs (1.2.1+), pg_reflex auto-infers the PK
from `pg_constraint`. No operator action is needed if the PK columns
appear in the SELECT list.

Override when the source PK is **not** in the SELECT list, or for
multi-source passthroughs where the target's identity comes from a
different column:

```sql
SELECT create_reflex_ivm('active_orders',
    'SELECT o.id, o.amount, p.name
       FROM orders o JOIN products p ON o.product_id = p.id',
    'id');                          -- explicit unique key on output
```

The engine creates a `UNIQUE INDEX __reflex_uk_<view>` on the target
to back targeted DELETE / UPDATE.

## 5. Indexes: what's auto-created vs what you should add

Auto-created at IMV creation time:

| Object | Index | Purpose |
|---|---|---|
| Intermediate table, single GROUP BY col | `USING hash` on the group column | ~30 % faster MERGE lookups vs B-tree |
| Intermediate table, multi-col GROUP BY | B-tree on the composite + per-column B-trees | MERGE lookup + per-column access |
| Target table, when `unique_columns` resolved | `UNIQUE INDEX __reflex_uk_<view>` | Targeted DELETE / UPDATE |
| Source table, MIN/MAX with GROUP BY | B-tree on GROUP BY columns | Scoped recompute scan locality |

What pg_reflex deliberately does **not** add:

- Indexes on the source that match arbitrary `WHERE` filters in the
  base query — those are workload-specific. Add them yourself when
  the recompute path appears in `EXPLAIN`.
- Covering indexes on the target for downstream read paths — same
  reason. Tune for your read pattern.

Use `reflex_explain_flush('<view>')` to see the planned MERGE / DELETE /
INSERT and spot a missing source-side index.

## 6. Disjoint UNION ALL: per-sub-IMV `where_predicate`

When the IMV decomposes into a `UNION ALL` of disjoint shapes (1.1.3+),
each sub-IMV gets its own `where_predicate` in the registry. The
deferred-flush trigger checks the predicate against the transition
table **before** taking the advisory lock; sub-IMVs whose predicate
matches no staged row skip the flush entirely.

```sql
-- Both halves are tracked, but DELETE on the "open" half doesn't fire
-- the closed-side flush, and vice-versa.
SELECT create_reflex_ivm('orders_split',
    'SELECT region, SUM(amount) AS t FROM orders WHERE status = ''open''  GROUP BY region
     UNION ALL
     SELECT region, SUM(amount) AS t FROM orders WHERE status = ''closed'' GROUP BY region');
```

The empty-affected `DO`-block gate (1.1.3) provides the same
short-circuit at MERGE time for any IMV — no operator action needed.

## 7. Cascades: keep depth ≤ 3, width ≤ 1 000

Each level of dependency multiplies commit-time work. The `graph_depth`
column in `__reflex_ivm_reference` tells you where each IMV sits in the
chain. Three levels is the comfortable budget; beyond that the audit
flags it as a yellow-light shape.

Width matters because `reflex_flush_deferred` drains the per-source
pending queue **in a single session**. A source feeding 1 000 IMVs
serialises 1 000 MERGE calls at COMMIT — that's a real spike on the
client.

Audit the topology with:

```sql
SELECT name, graph_depth, cardinality(graph_child) AS children
FROM reflex_ivm_status()
ORDER BY graph_depth DESC, children DESC;
```

If a single source fan-outs to a wide ring of identical-shape IMVs,
collapse them into one parent IMV (`UNION ALL` or `GROUP BY` with
extra dimensions) and read the slices via plain views.

## 8. Choose algebraic aggregates when the shape allows

Algebraic aggregates have a single MERGE per delta — no source scan,
no scoped recompute:

| Aggregate | Path | Cost shape on retraction |
|---|---|---|
| `SUM`, `COUNT`, `COUNT(DISTINCT)` | Algebraic | `O(delta)` |
| `AVG` | Algebraic (sum + count) | `O(delta)` |
| `BOOL_OR` | Algebraic (1.1.3 — true / non-null counters) | `O(delta)` |
| `MIN`, `MAX` (with auto top-K) | Heap + scoped recompute fallback | `O(K)` typical, `O(group_size)` on heap underflow |

When you have a choice — for instance when a dashboard could surface
either the median **or** the count-of-positive-values — prefer the
shape that lands on an algebraic aggregate.

`FILTER (WHERE …)` is rewritten to `CASE WHEN` (1.1.1+) so it inherits
whatever path the underlying aggregate uses. There is no penalty for
using `FILTER` over the equivalent `CASE` you'd write by hand.

## 9. Decomposition: CTE + sub-IMV

A complex query that joins multiple aggregates is often faster as a
CTE per aggregate plus a thin top-level join — each sub-aggregate
becomes its own incrementally-maintained IMV:

```sql
SELECT create_reflex_ivm('region_summary',
    'WITH revenue AS (
         SELECT region, SUM(amount) AS r FROM orders GROUP BY region
     ),
     headcount AS (
         SELECT region, COUNT(*) AS n FROM employees GROUP BY region
     )
     SELECT r.region, revenue.r, headcount.n
       FROM revenue
       JOIN headcount USING (region)');
```

`revenue` and `headcount` get their own intermediate tables and trigger
sets. A new `INSERT` on `orders` only fires `revenue`'s flush, not
`headcount`'s.

The decomposer does **not** recurse into `UNION` operators inside a
CTE body — lift them to the top level if you want each operand to
become its own sub-IMV. See
[unsupported shapes §3](../limitations/unsupported-shapes.md#3-operator-side-workarounds).

## 10. Source indexing for the recompute path

Every shape that falls back to a scoped source-scan recompute (MIN/MAX
heap underflow, FULL OUTER JOIN retraction) reads from the source
filtered by the affected-groups set. A B-tree on the source's GROUP BY
columns turns those scans from sequential to indexed.

For MIN/MAX with GROUP BY, pg_reflex auto-creates this index at IMV
creation time (`__reflex_idx_<view>_<source>`). For other shapes, add
one yourself if `EXPLAIN` shows a sequential scan in the recompute
body — `reflex_explain_flush('<view>')` is the easiest way to check.

## 11. PostgreSQL-side knobs that matter

These aren't pg_reflex settings, but they shape per-flush latency:

| Knob | Why |
|---|---|
| `shared_buffers` ≥ working set | Intermediate tables stay hot in cache, MERGE lookups stay sub-ms |
| `wal_buffers` (LOGGED IMVs) | Smooths WAL bursts on every flush |
| `autovacuum_vacuum_scale_factor` (target table) | Frequent UPDATEs on a passthrough produce dead tuples; tighter autovacuum keeps reads fast |
| `track_application_name = on` | Lets `pg_stat_statements` rows be filtered by `reflex_flush:<view>` |
| `effective_io_concurrency` | Helps the scoped recompute on MIN/MAX heap underflow when the source is on SSD/NVMe |

## 12. Observe before you optimise

Before tweaking anything, run:

```sql
-- Latency distribution per IMV
SELECT name, samples, p50_ms, p95_ms, p99_ms, max_ms
FROM (SELECT name FROM reflex_ivm_status()) AS s,
     LATERAL reflex_ivm_histogram(s.name)
ORDER BY p95_ms DESC NULLS LAST;

-- Last-known error per IMV
SELECT name, last_flush_ms, last_flush_rows, last_error
FROM reflex_ivm_status()
ORDER BY last_flush_ms DESC NULLS LAST;

-- What the next flush will do
SELECT * FROM reflex_explain_flush('<view>');
```

The histogram + `pg_stat_statements` correlation
(`application_name = 'reflex_flush:<view>'`) usually points straight
at the slow IMV. Optimise that one; ignore the rest until they show
up in p95.

[Monitoring page](../operations/monitoring.md){ .md-button }
[Cost model](cost-model.md){ .md-button }

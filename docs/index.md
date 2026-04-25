---
title: pg_reflex
hide:
  - navigation
---

<div class="reflex-hero" markdown>

# pg_reflex

**Incremental view maintenance for PostgreSQL.** Keep your aggregated views fresh in real time, without full refreshes — `O(delta)` trigger-based updates instead of `O(N)` `REFRESH MATERIALIZED VIEW`.

</div>

## What it is

pg_reflex is a PostgreSQL extension (built with [pgrx](https://github.com/pgcentralfoundation/pgrx)) that maintains materialized-view-style result tables incrementally. When source data changes — `INSERT`, `UPDATE`, `DELETE`, `TRUNCATE` — only the affected groups are recomputed, not the entire dataset.

It is **opt-in per IMV**, **drop-in for SUM / COUNT / AVG / MIN / MAX / BOOL_OR / DISTINCT** workloads, and **safe to deploy alongside plain `MATERIALIZED VIEW`** for the shapes that aren't supported.

<div class="reflex-bench-grid" markdown>

<div class="reflex-bench-card" markdown>
<div class="label">INSERT 1K rows<br>5-table JOIN</div>
<div class="value">235&nbsp;ms</div>
vs `REFRESH` 38.9s — **99.4% faster**
</div>

<div class="reflex-bench-card" markdown>
<div class="label">DELETE 50K rows</div>
<div class="value">297&nbsp;ms</div>
vs `REFRESH` 38.9s — **99.2% faster**
</div>

<div class="reflex-bench-card" markdown>
<div class="label">GROUP BY UPDATE 100 rows<br>5M-row source</div>
<div class="value">4.4&nbsp;ms</div>
vs `REFRESH` 55ms — **12× faster**
</div>

</div>

[Get started in 60 seconds :material-arrow-right-bold:](getting-started/first-imv.md){ .md-button .md-button--primary }
[Read the architecture :material-book-open-variant:](concepts/architecture.md){ .md-button }

## When to use

!!! tip "Green light"
    Analytical dashboards over append-mostly or narrowly-mutated sources. SUM / COUNT / AVG / COUNT(DISTINCT) / BOOL_OR. Cascade depth ≤ 3. Schema changes rare or operator-coordinated.

!!! warning "Yellow light"
    MIN/MAX over wide fact tables (>10M rows) where retraction is occasional (<10% of groups per flush). With `topk` enabled (1.3.0), the cliff is much shallower. Multi-session concurrent DDL on the same IMV graph: tested but not stress-tested beyond 4 concurrent flush sessions.

!!! danger "Red light"
    `WITH RECURSIVE`, `FULL OUTER JOIN` deltas, `ARRAY_AGG` / `JSON_AGG`. Mission-critical read paths where stale-on-schema-change is worse than downtime (use `pg_reflex.alter_source_policy = 'error'` from 1.2.1 to gate). Multi-tenant platforms where untrusted users can define IMV SQL.

[Full deployment profile :material-arrow-right-bold:](operations/deployment-profile.md){ .md-button }

## Highlights — version 1.3.0

- **Bounded top-K MIN/MAX heap.** Opt-in `topk=K` parameter on `create_reflex_ivm` keeps the K extremum values per group, turning `O(N)` retractions into `O(K)`.
- **Per-IMV flush histogram.** `reflex_ivm_histogram(view)` returns p50 / p95 / p99 / max from a 64-sample ring buffer.
- **`pg_stat_statements` correlation.** Each flush body sets `application_name = 'reflex_flush:<view>'`.
- **Scalar MIN/MAX (no `GROUP BY`)** is a tested supported shape.

[Full changelog :material-arrow-right-bold:](changelog.md){ .md-button }

## Three-line example

```sql
SELECT create_reflex_ivm('sales_by_region',
    'SELECT region, SUM(amount) AS total FROM sales GROUP BY region');

INSERT INTO sales (region, amount) VALUES ('US', 50);
SELECT * FROM sales_by_region;  -- already updated, no REFRESH needed
```

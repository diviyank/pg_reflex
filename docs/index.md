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

<div class="reflex-feature-grid" markdown>

<div class="reflex-feature-card" markdown>
### :material-flash: Incremental updates
Triggers maintain the result table on every `INSERT` / `UPDATE` / `DELETE` / `TRUNCATE`. No scheduled `REFRESH`, no full re-scan — only the affected groups are touched.
</div>

<div class="reflex-feature-card" markdown>
### :material-function-variant: Broad aggregate coverage
`SUM`, `COUNT`, `AVG`, `MIN`, `MAX`, `BOOL_OR`, `COUNT(DISTINCT)`, plus CTEs, `FILTER`, `DISTINCT ON`, and a curated set of window functions.
</div>

<div class="reflex-feature-card" markdown>
### :material-cog-outline: Operationally aware
Auto-drop event triggers, optional `DEFERRED` mode, per-IMV flush histograms, `pg_stat_statements` correlation, and a `pg_cron` reconcile recipe.
</div>

<div class="reflex-feature-card" markdown>
### :material-speedometer: Designed for speed
On the workloads it targets — append-mostly sources, narrow updates, cascade depth ≤ 3 — incremental flushes are typically much cheaper than a full `REFRESH`. Numbers vary by shape; see the [benchmarks](performance/benchmarks.md) page for the workloads we measured.
</div>

</div>

[Get started in 60 seconds :material-arrow-right-bold:](getting-started/first-imv.md){ .md-button .md-button--primary }
[Read the architecture :material-book-open-variant:](concepts/architecture.md){ .md-button }

## When to use

!!! success "Green light"
    Analytical dashboards over append-mostly or narrowly-mutated sources. SUM / COUNT / AVG / COUNT(DISTINCT) / BOOL_OR. Cascade depth ≤ 3. Schema changes rare or operator-coordinated.

!!! warning "Yellow light"
    UPDATE-heavy patterns on top-K MIN/MAX IMVs where the *group cardinality is at or below K* (heap holds the whole group) — every UPDATE shrinks the heap, so the scoped source-scan recompute fires regardless. Workloads where K ≪ group cardinality recover most of the pre-1.3.0 UPDATE perf via the 1.3.1 heap-shrinkage gate. If your shape is in the bad case, opt out via `topk = 0`. Multi-session concurrent DDL on the same IMV graph: tested with 4 concurrent flush sessions, not stress-tested beyond.

!!! danger "Red light"
    `WITH RECURSIVE`, `FULL OUTER JOIN` deltas, `ARRAY_AGG` / `JSON_AGG`. Mission-critical read paths where stale-on-schema-change is worse than downtime (use `pg_reflex.alter_source_policy = 'error'` from 1.2.1 to gate). Multi-tenant platforms where untrusted users can define IMV SQL.

[Full deployment profile :material-arrow-right-bold:](operations/deployment-profile.md){ .md-button }

## Highlights — version 1.3.0

- **Bounded top-K MIN/MAX heap, auto-enabled.** Reflex applies `topk=16` automatically to every MIN/MAX intermediate column — retractions stay bounded without operator opt-in. Append-only workloads can opt out via `topk = 0`.
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

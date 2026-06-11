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
    UPDATE-heavy patterns on top-K MIN/MAX IMVs where the *group cardinality is at or below K* (heap holds the whole group) — every UPDATE shrinks the heap, so the scoped source-scan recompute fires regardless. Workloads where K ≪ group cardinality recover most of the pre-auto-topk UPDATE perf via the 1.4.0 heap-shrinkage gate. If your shape is in the bad case, opt out via `topk = 0`. Multi-session concurrent DDL on the same IMV graph: tested with 4 concurrent flush sessions, not stress-tested beyond.

!!! danger "Red light"
    `WITH RECURSIVE`, `FULL OUTER JOIN` deltas, `ARRAY_AGG` / `JSON_AGG`. Mission-critical read paths where stale-on-schema-change is worse than downtime (use `pg_reflex.alter_source_policy = 'error'` from 1.2.1 to gate). Multi-tenant platforms where untrusted users can define IMV SQL.

[Full deployment profile :material-arrow-right-bold:](operations/deployment-profile.md){ .md-button }

## Recent highlights

- **Partition no-op skip now covers DETACH-then-DROP (1.10.4).** The 1.10.3 skip probed the partition child's rows, but the flush runs at COMMIT — so detaching *and* dropping a partition in one transaction left nothing to probe and force-reconciled the dependent IMV. The skip is now proven from the partition's `LIST` bound instead, so dropping a non-current LIST partition no longer rebuilds the downstream subtree. Also: `drop_reflex_ivm` no longer leaks the per-source DEFERRED staging delta table.
- **Incremental partition delta for unpartitioned IMVs (1.10.3).** Attaching or detaching a partition on a partitioned source no longer full-rebuilds dependent unpartitioned IMVs — the partition child flows through the normal incremental INSERT/DELETE path, so a net-zero change (e.g. attaching a non-current LIST assortment to a filtered IMV) skips the downstream cascade entirely.
- **In-place partitioned passthrough UPDATE (1.9.1).** The cold partition path now applies a pure-data UPDATE via `INSERT … ON CONFLICT DO UPDATE` plus a keyed delete-gone, instead of a full DELETE + recompute INSERT — ~3–4.5× faster flush on a 33.7M-row, 837-leaf passthrough IMV. See the [inner workings](concepts/inner-workings.md) walkthrough.
- **Partition-aware trigger dispatch + keyed passthrough secondaries (1.9.0).** Partitioned passthrough/aggregate IMVs route DML only to the affected child partitions; passthrough LEFT-JOIN secondaries and single-source PK passthrough/CTE IMVs now maintain incrementally instead of full-rebuilding.
- **`ignore_sources` honored on the DEFERRED path (1.7.6).** A source excluded via `ignore_sources` is now skipped on both the IMMEDIATE and DEFERRED trigger paths (including `reflex_flush_deferred`); earlier versions skipped only on IMMEDIATE.
- **Widened CTE/JOIN unique-key inference (1.7.5).** Chained-CTE cascades auto-resolve sound unique keys (equi-join equivalence, aggregate GROUP BY keys, CROSS-to-single-row), so they get incremental DELETE/UPDATE instead of full refresh.
- **Partitioned IMVs (1.5.0–1.7.4).** `partition_by`, per-partition reconcile/dispatch, atomic DETACH/ATTACH swap, and partition-anchor resolution across co-partitioned join keys.
- **Top-K MIN/MAX auto-enabled (`K=16`, 1.4.0).** MIN/MAX columns get a bounded top-K heap by default; retractions stay `O(K)`. Append-only workloads can opt out via `topk = 0`.

[Full changelog :material-arrow-right-bold:](changelog.md){ .md-button }

## Three-line example

```sql
SELECT create_reflex_ivm('sales_by_region',
    'SELECT region, SUM(amount) AS total FROM sales GROUP BY region');

INSERT INTO sales (region, amount) VALUES ('US', 50);
SELECT * FROM sales_by_region;  -- already updated, no REFRESH needed
```

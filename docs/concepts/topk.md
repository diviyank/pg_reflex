# Top-K MIN / MAX

The audit's flagship perf gap (R3): retracting from a `MIN` / `MAX` IMV historically meant a full source scan to find the next-smallest / next-largest value. The 1.2.0 *scoped recompute* path narrowed the scan to affected groups, but groups still had to be re-aggregated from the source.

Top-K (1.3.0) keeps the **K extremum values per group** in a sibling array column. Retraction = subtract from the array. Falls back to the scoped recompute only when the array underflows. As of **1.4.0** the heap is **auto-enabled with `K=16`** on every MIN/MAX intermediate; UPDATEs additionally pass through the **heap-shrinkage gate (N1)** so groups whose heap stayed at K skip the source scan entirely.

## Default (1.4.0+)

```sql
SELECT create_reflex_ivm(
    'stock_chart_weekly',
    'SELECT product_id, week, MIN(price) AS lo, MAX(price) AS hi
     FROM stock_history GROUP BY product_id, week'
);
-- top-K is on with K=16 by default
```

To opt out (append-only MIN/MAX workloads where the heap-maintenance INSERT cost outweighs the retraction benefit):

```sql
SELECT create_reflex_ivm(
    'stock_chart_weekly',
    'SELECT product_id, week, MIN(price) AS lo, MAX(price) AS hi
     FROM stock_history GROUP BY product_id, week',
    NULL, NULL, NULL,
    0            -- topk = 0 disables the heap
);
```

Existing 1.3.0 IMVs that did not opt into top-K retain their previous behaviour after upgrade — auto-on only applies to freshly created IMVs.

## What it stores

For each top-K-enabled MIN/MAX column, the intermediate gains a sibling array:

| Source aggregate | Scalar column | Top-K companion |
|---|---|---|
| `MIN(x)` | `__min_x <type>` | `__min_x_topk <type>[K]` (sorted ASC) |
| `MAX(x)` | `__max_x <type>` | `__max_x_topk <type>[K]` (sorted DESC) |

`<type>` is inherited from the source column via the same `resolve_column_type` path that already sized the scalar column.

## How it maintains itself

**On INSERT** (delta `MERGE` Add path):

```sql
__min_x_topk = (
    SELECT array_agg(v ORDER BY v ASC) FROM (
        SELECT v FROM unnest(t.__min_x_topk || COALESCE(d.__min_x_topk, '{}'::numeric[])) v
        ORDER BY v ASC LIMIT 16
    ) s
)
```

**On DELETE / UPDATE-old** (Subtract path):

```sql
__min_x_topk = public.__reflex_array_subtract_multiset(t.__min_x_topk, d.__min_x_topk)
__min_x      = (public.__reflex_array_subtract_multiset(t.__min_x_topk, d.__min_x_topk))[1]
```

`__reflex_array_subtract_multiset(arr, remove)` removes one occurrence of each `remove[i]` from `arr` — proper multi-set subtraction so duplicates are preserved correctly.

**On heap underflow** (`cardinality(__min_x_topk) = 0` after subtract): the existing `build_min_max_recompute_sql` UPDATE runs against the source for those groups, repopulating both `__min_x` and `__min_x_topk` from a fresh scan.

## Choosing K

| Workload | Recommended K |
|---|---|
| Insert-only, no retraction | 1–4 (top-K cost dominates) |
| Mixed INSERT/DELETE, narrow groups | 8–16 |
| Mixed INSERT/DELETE, wide groups (>10× K rows) | 16–32 |
| Frequent full-group retraction | top-K helps less; consider `REFRESH` instead |

`K = 16` is a reasonable default. Larger K increases per-group write cost and storage; smaller K means more frequent fallbacks to scoped recompute.

## Verifying

```sql
SELECT __min_x, __min_x_topk
FROM __reflex_intermediate_my_view
WHERE grp = 'a';
```

The first element of the array equals the scalar column when the heap is non-empty.

## Known limitations

- `reflex_enable_topk(name, k)` retrofit SPI is not shipped. To retrofit
  top-K onto an in-flight IMV, drop and recreate it.
- Group cardinality at or below `K` means every UPDATE shrinks the heap
  and trips the N1 gate, so the scoped recompute fires on every
  retraction. Use `topk = 0` for these shapes if you don't need bounded
  retraction.

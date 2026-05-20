# Aggregates

| Function | INSERT | DELETE | Notes |
|---|:---:|:---:|---|
| `SUM(x)` | ✅ Algebraic | ✅ Algebraic | Stores `__sum_x` + `__nonnull_count_x` (skipped when source is `NOT NULL`) |
| `COUNT(x)` | ✅ Algebraic | ✅ Algebraic | Stores `__count_x` |
| `COUNT(*)` | ✅ Algebraic | ✅ Algebraic | Stores `__count_star` |
| `COUNT(DISTINCT x)` | ✅ via reference counting | ✅ via reference counting | Compound key `(grp, x)` + `__ivm_count` |
| `AVG(x)` | ✅ Algebraic (decomposed) | ✅ Algebraic | `__sum_x / NULLIF(__count_x, 0)` |
| `MIN(x)` | ✅ via `LEAST` | ⚠️ Recomputes; with `topk` (1.3.0): O(K) | Works on numeric and non-numeric types (timestamptz, date, text); table-qualified args supported |
| `MAX(x)` | ✅ via `GREATEST` | ⚠️ Recomputes; with `topk`: O(K) | Works on numeric and non-numeric types (timestamptz, date, text); table-qualified args supported |
| `BOOL_OR(x)` | ✅ Algebraic (1.1.3+) | ✅ Algebraic | Two BIGINT companions: `__bool_or_x_true_count`, `__bool_or_x_nonnull_count` |
| `DISTINCT` | ✅ via reference counting | ✅ via reference counting | Columns become implicit group keys + `__ivm_count` |

## FILTER clause

`AGG(x) FILTER (WHERE cond)` is supported for SUM, COUNT, COUNT(*), AVG, MIN, MAX, BOOL_OR. Internally rewritten to `AGG(CASE WHEN cond THEN x END)`. Multiple filtered + unfiltered aggregates can coexist in the same query.

```sql
SELECT region,
       SUM(amount) AS total,
       SUM(amount) FILTER (WHERE type = 'refund') AS refunds,
       AVG(amount) FILTER (WHERE amount > 100) AS big_avg
FROM orders GROUP BY region;
```

## Type casts and MIN/MAX on non-numeric types

Aggregates with casts are supported. The cast is applied when materialising from intermediate to target — the intermediate stores NUMERIC for full precision.

```sql
SELECT region, SUM(amount)::BIGINT AS total FROM orders GROUP BY region;
-- target column is BIGINT; intermediate stores __sum_amount NUMERIC
```

`MIN()` and `MAX()` work on non-numeric types including `timestamptz`, `date`, `text`, and others. Both bare and table-qualified arguments are supported:

```sql
-- Bare argument
SELECT MAX(created_at) FROM events GROUP BY type;

-- Table-qualified argument  
SELECT MAX(e.created_at) FROM events e GROUP BY e.type;
```

Previously, table-qualified arguments on non-numeric MIN/MAX would fail with a type error; this is now fixed.

## Unsupported aggregates

`STRING_AGG`, `ARRAY_AGG`, `JSON_AGG`, `JSONB_AGG`, `XMLAGG`, `STDDEV`, `VARIANCE`, `PERCENTILE_*` — emit a `WARNING` at create time and are silently dropped from the IMV. Use `MATERIALIZED VIEW` for these.

`SUM(DISTINCT x)`, `AVG(DISTINCT x)`, `MIN/MAX/BOOL_OR(DISTINCT x)` are rejected (only `COUNT(DISTINCT x)` is supported).

[Top-K MIN/MAX deep dive :material-arrow-right-bold:](../concepts/topk.md){ .md-button }

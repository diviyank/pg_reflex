# Sufficient statistics

The trick that makes incremental view maintenance fast is **not** storing all the source rows. The intermediate table stores only enough state to compute the final result *and* to update that state on insert/delete.

## Algebraic vs holistic

| Aggregate | Algebraic? | Why |
|---|---|---|
| `SUM(x)` | ✅ Yes | `SUM(A ∪ B) = SUM(A) + SUM(B)`. Update by `±delta`. |
| `COUNT(x)` | ✅ Yes | `COUNT(A ∪ B) = COUNT(A) + COUNT(B)`. |
| `AVG(x)` | ✅ Yes (decomposed) | `AVG = SUM / COUNT`. Maintain SUM and COUNT separately. |
| `BOOL_OR(x)` | ✅ Yes (1.1.3+) | Maintain `true_count` and `nonnull_count`; result is `true_count > 0` when `nonnull_count > 0`. |
| `MIN(x)` | ⚠️ Algebraic on insert | `MIN(A ∪ B) = LEAST(MIN(A), MIN(B))` on insert; **but** retraction needs the second-smallest value. |
| `MAX(x)` | ⚠️ Algebraic on insert | Symmetric to MIN. |
| `COUNT(DISTINCT x)` | ⚠️ Algebraic via reference counting | Compound key `(grp, val)` + `__ivm_count` per pair. |
| `ARRAY_AGG(x)`, `JSON_AGG(x)` | ❌ Holistic | Order matters / reads as the whole array on every retrieval. Not supported. |

## How retraction works

For algebraic aggregates, retraction is symmetric to insert: subtract the delta. Done.

For `MIN` and `MAX`, retracting the current extremum requires finding the next-smallest (or next-largest) survivor. Three strategies in increasing sophistication:

1. **Full source scan** (1.0.x): set `__min_x = NULL`, then `UPDATE … FROM (orig_base_query)`. Correct but `O(N)`.
2. **Scoped recompute** (1.2.0): same UPDATE, but the `orig_base_query` is wrapped in a filter that restricts it to groups present in `__reflex_affected_<view>`. `O(affected_groups × group_size)`.
3. **Top-K heap** (1.3.0, opt-in via `topk=K`): each group keeps the K extremum values in a sorted array. Retraction = multi-set subtract from the array. Falls back to scoped recompute only when the heap underflows.

[See top-K design :material-arrow-right-bold:](topk.md){ .md-button }

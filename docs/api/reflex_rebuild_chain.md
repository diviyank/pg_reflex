# `reflex_rebuild_chain`

(1.10.8+) In-extension recovery for a corrupted *decomposed* (CTE / set-op) IMV chain: an atomic CASCADE drop and recreate from the stored creation arguments.

## Signature

```sql
reflex_rebuild_chain(view_name TEXT) RETURNS TEXT
```

## Behaviour

Performs an atomic CASCADE `DROP` of `view_name` and all its dependents, then recreates the entire chain by replaying the original `create_reflex_ivm` arguments captured in `__reflex_ivm_reference.create_args`. If recreate fails, the drop rolls back — you never get a half-dropped chain. Returns `'REBUILT <n> IMVs'` on success.

Use when a partitioned IMV with a complex query (CTEs, set operations) has structural corruption that per-partition reconcile cannot fix. The dry-run is `SELECT reflex_doctor(view_name);` — it will report F4b with the exact `reflex_rebuild_chain(...)` call to run.

## Example

```sql
SELECT reflex_rebuild_chain('forecast_analysis_view');
-- REBUILT 3 IMVs
```

## See also

- [`drop_reflex_ivm`](drop_reflex_ivm.md) — non-cascading drop (requires manual cleanup of children).
- [`reflex_reconcile`](reflex_reconcile.md) — full rebuild from source (non-decomposed IMVs).
- [`reflex_doctor`](reflex_doctor.md) — diagnoses when F4b rebuild is needed.

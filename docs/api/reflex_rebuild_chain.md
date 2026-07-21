# `reflex_rebuild_chain`

(1.10.8+) In-extension recovery for a corrupted *decomposed* (CTE / set-op) IMV chain: an atomic CASCADE drop and recreate from the stored creation arguments.

## Signature

```sql
reflex_rebuild_chain(view_name TEXT, cascade BOOLEAN DEFAULT FALSE) RETURNS TEXT
```

## Behaviour

Performs an atomic CASCADE `DROP` of `view_name` and all its dependents, then recreates the entire chain by replaying the original `create_reflex_ivm` arguments captured in `__reflex_ivm_reference.create_args`. If recreate fails, the drop rolls back — you never get a half-dropped chain. Returns `'REBUILT <n> IMVs'` on success.

Use when a partitioned IMV with a complex query (CTEs, set operations) has structural corruption that per-partition reconcile cannot fix. The dry-run is `SELECT reflex_doctor(view_name);` — it will report F4b with the exact `reflex_rebuild_chain(...)` call to run.

**`cascade`** (1.10.10+, default `FALSE`): `view_name`'s CASCADE drop can take down other IMVs that depend on it. Without `cascade => TRUE`, the function checks for such dependents first and **refuses** — no drop happens — reporting their names so you can rebuild them individually or opt in deliberately. With `cascade => TRUE`, it drops `view_name` and its dependents together and recreates all of them in dependency order (shallowest first), so each dependent exists again before anything that depends on it. It still refuses — even under `cascade => TRUE` — if any dependent has no stored `create_args` (created before 1.10.8): there would be nothing to replay its storage mode, refresh mode, or partitioning from, so recreating it would silently reset those settings. Rebuild such a dependent individually with `reflex_rebuild_imv` first.

## Example

```sql
SELECT reflex_rebuild_chain('forecast_analysis_view');
-- ERROR: IMV 'forecast_analysis_view' has 2 dependent IMV(s) that CASCADE would
-- destroy: forecast_summary_view, forecast_alerts_view. Re-run with cascade => TRUE
-- to drop and recreate them, or rebuild them individually.
-- (returned as text, not raised — the drop never ran)

SELECT reflex_rebuild_chain('forecast_analysis_view', cascade => TRUE);
-- REBUILT CHAIN (2 dependent(s) restored)
```

## See also

- [`drop_reflex_ivm`](drop_reflex_ivm.md) — non-cascading drop (requires manual cleanup of children).
- [`reflex_reconcile`](reflex_reconcile.md) — full rebuild from source (non-decomposed IMVs).
- [`reflex_doctor`](reflex_doctor.md) — diagnoses when F4b rebuild is needed.

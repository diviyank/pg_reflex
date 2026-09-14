# `reflex_ack_ignore_source`

(1.11.4+) Acknowledges that ignoring a source is unsound for an IMV, so the create-time soundness check, `reflex_rebuild_imv` and `reflex_audit` accept it.

## Signature

```sql
reflex_ack_ignore_source(imv TEXT, source TEXT) RETURNS TEXT
```

Returns `ACKNOWLEDGED`, or `ERROR: no IMV named '…'` / `ERROR: '…' is not in ignore_sources for IMV '…'`.

## Why it exists

`ignore_sources` tells pg_reflex not to maintain an IMV when a source changes. That is only safe when the source cannot change the IMV's contents. When the query filters on it (`WHERE`, `HAVING`, `JOIN … ON`, `JOIN … USING`), inner-joins it, or cannot be attributed (CTEs, set operations, wildcards), a change to the ignored source silently leaves the IMV wrong — and a later rebuild from the base query makes the divergence permanent for the slices it rewrites.

Since 1.11.4 `create_reflex_ivm` refuses such an ignore, and `reflex_audit` / `reflex_doctor` (F13) report existing ones.

## Acknowledging

At create time, prefix the source with `!`:

```sql
SELECT create_reflex_ivm('sop_forecast_view', $$ … $$, …,
                         ignore_sources => 'location,pricing,!demand_planning');
```

For an IMV that already exists:

```sql
SELECT reflex_ack_ignore_source('omc.sop_forecast_view', 'demand_planning');
```

The call records the acknowledgement in the registry's `ignore_ack` column **and** rewrites the entry to `!source` in the stored `create_args`, in one statement, so a replay by `reflex_rebuild_imv` keeps it. It is idempotent. On a legacy registry row whose `create_args` has no `ignore_sources` key, `create_args` is left untouched.

Acknowledging does not make the ignore safe — it records that you accept the divergence risk and have another way to refresh the IMV (for example a `reflex_reconcile_partition` call from the workflow that changes the ignored source).

# GUCs

pg_reflex reads a handful of runtime settings via `current_setting`. None require a restart — set them per-session with `SET`, per-transaction with `SET LOCAL`, or per-database/role with `ALTER DATABASE/ROLE ... SET`. All are optional; the compiled defaults apply when unset.

| Setting | Type | Default | Effect |
|---|---|---|---|
| [`reflex.wipe_threshold`](#reflexwipe_threshold) | float `0`–`1` | `0.5` | Dirty-row fraction at or above which a batch wipes-and-rebuilds instead of applying a delta. |
| [`reflex.wipe_floor_rows`](#reflexwipe_floor_rows) | integer | `1000` | Floor on the partition-size denominator of the dirty ratio. |
| [`reflex.assert_inplace_update`](#reflexassert_inplace_update) | boolean | `off` | Correctness assertion on the in-place UPDATE path; for CI/fuzz. |
| [`pg_reflex.alter_source_policy`](#pg_reflexalter_source_policy) | enum | `warn` | Reaction to `ALTER TABLE` on a tracked source. |

!!! note "Reserved"
    `reflex.partition_dispatch_cost_cap` (and the [`reflex_set_partition_dispatch_cost_cap`](reflex_set_partition_dispatch_cost_cap.md) setter) is reserved for the Tier 2 per-partition dispatch gate. It is **not yet consulted at runtime** — setting it currently has no effect.

## `reflex.wipe_threshold`

(1.4.6+) The dirty-row fraction at or above which a maintenance batch wipes-and-rebuilds the (partition of the) IMV rather than applying a row-by-row delta. Lower values flip to a full rebuild sooner; higher values prefer incremental deltas for longer.

Resolution order: per-IMV [`reflex_set_wipe_threshold`](reflex_set_wipe_threshold.md) override → this GUC → compiled default `0.5`.

```sql
SET reflex.wipe_threshold = 0.3;   -- prefer full rebuilds in this session
```

## `reflex.wipe_floor_rows`

(1.6.0+) A floor on the partition-size denominator of the dirty/size ratio, so a tiny or never-`ANALYZE`d partition (`reltuples = 0`) cannot trip a wipe on a single dirty row.

Resolution order: per-IMV [`reflex_set_wipe_floor_rows`](reflex_set_wipe_floor_rows.md) override → this GUC → compiled default `1000`.

```sql
SET reflex.wipe_floor_rows = 5000;
```

## `reflex.assert_inplace_update`

When `on`, the in-place UPDATE path re-derives the affected key set and `RAISE`s on any mismatch — a correctness assertion intended for CI and fuzz runs. Default `off`; leave it off in production, where the extra work is pure overhead.

```sql
SET reflex.assert_inplace_update = on;   -- CI / fuzz only
```

## `pg_reflex.alter_source_policy`

(1.2.1+) Controls how the `reflex_on_ddl_command_end` event trigger reacts when a tracked source is `ALTER TABLE`'d.

| Value | Behaviour |
|---|---|
| `'warn'` (default) | Emits `WARNING 'pg_reflex: source table % was altered; IMV % may be stale — run SELECT reflex_rebuild_imv(…)`'. The ALTER proceeds. |
| `'error'` | Raises an `EXCEPTION`, rolling back the ALTER. |

## Setting

Custom namespaced GUCs are session-settable without explicit registration on PG 9.2+:

```sql
-- Per-session (until reset or disconnect)
SET pg_reflex.alter_source_policy = 'error';

-- Per-transaction
BEGIN;
SET LOCAL pg_reflex.alter_source_policy = 'error';
ALTER TABLE orders ADD COLUMN x INT;
COMMIT;

-- Per-database
ALTER DATABASE mydb SET pg_reflex.alter_source_policy = 'error';

-- Reset
RESET pg_reflex.alter_source_policy;
```

## Why default to `warn`

Strict mode rolls back legitimate operator workflows — renames, type widenings, column adds. Defaulting to `warn` keeps the operational ergonomics; teams that want gating opt in via `ALTER DATABASE`.

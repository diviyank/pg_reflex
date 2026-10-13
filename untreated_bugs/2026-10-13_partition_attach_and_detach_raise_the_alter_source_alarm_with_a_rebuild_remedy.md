# ATTACH / DETACH PARTITION raise the alter-source alarm with a `reflex_rebuild_imv` remedy, and `alter_source_policy = 'error'` blocks them — S3 (noise, misleading remedy; blocks swaps under 'error')

Pre-existing (1.11.5 matched `depends_on @> ARRAY[split_part(_src, '.', 2)]` the same way). Seen in
the 1.12.0 upgrade test on PostgreSQL 15 (2026-10-13), on IMVs created by 1.11.5 and recreated on
1.12.0 alike.

## What happens

`__reflex_on_ddl_command_end` ends with the alter-source alarm: for every `ALTER TABLE` command it
warns once per IMV reading the altered table. `ALTER TABLE <root> ATTACH PARTITION …` and
`… DETACH PARTITION …` are `ALTER TABLE` commands on the root, so one leaf swap
(`DETACH` + `ATTACH`) prints, for each IMV of the source and for each reader of a partitioned IMV
that the auto-sync altered:

```
WARNING:  pg_reflex: source table public.sales was altered; IMV sales_pm may be stale — run SELECT reflex_rebuild_imv('sales_pm') to recover
WARNING:  pg_reflex: source table public.sales_pm was altered; IMV sales_plan_from_pm may be stale — run SELECT reflex_rebuild_imv('sales_plan_from_pm') to recover
```

The partition change is handled by the COMMIT-time partition flush; after the COMMIT every IMV of
the test matched its query (bidirectional `EXCEPT ALL` = 0). The warning is wrong, and its remedy
is a full rebuild, the most expensive operation, on IMVs that are already correct. An application
that swaps leaves daily prints it on every swap.

Under `pg_reflex.alter_source_policy = 'error'` the same branch raises, so every ATTACH / DETACH
of a partitioned source with IMVs is refused (the comment above the branch says it is meant for
"non-partition ALTERs").

## Reproduction

`.superpowers/sdd/2026-10-07-1.12.0-create-contract-hardening/logs/pkg3-upgrade/` (`p1.sql`,
`swap.sql`): LIST-partitioned `sales`, `sales_pm` mirroring it, `sales_plan_from_pm` over
`sales_pm`, `sales_total`; `BEGIN; ALTER TABLE sales DETACH PARTITION sales_p2; … ATTACH …; COMMIT`.

## Not established

- Whether `pg_event_trigger_ddl_commands()` lets the branch tell ATTACH / DETACH from a
  column-changing ALTER (the `command` column is `pg_ddl_command`, readable only from C); the
  partition branch above it already recognises partition DDL by other means.
- Whether any user relies on 'error' to block partition DDL.

## Fix direction

Skip the alarm for an `ALTER TABLE` whose only subcommands are ATTACH / DETACH PARTITION on a
root the partition branch has queued (and for the auto-sync's own ALTERs of a partitioned IMV),
keeping it for column, type and constraint changes. Test: a swap under 'warn' prints no alarm and
under 'error' commits, while `ALTER TABLE … ADD COLUMN` on the same source still warns / raises.

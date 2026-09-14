# `reflex_heal_ignored_sources`

(1.11.4+) Rebuilds the IMV partitions made wrong by a change to an ignored source.

## Signature

```sql
reflex_heal_ignored_sources(imv TEXT DEFAULT NULL) RETURNS TEXT
```

`imv = NULL` heals every IMV with queued partitions. Returns `HEALED <n> IMVs`, or `ERROR: …` naming each IMV whose heal failed.

## Why it exists

A source listed in `ignore_sources` gets no maintenance trigger. When the IMV's query filters on that source, a change to it leaves the IMV wrong: a demand plan whose `status` leaves the included set while its sales are rebuilt keeps an empty slice after the status returns.

Since 1.11.4, when an ignored source joins by equality onto the expression the IMV's first partition column projects, `create_reflex_ivm` installs three statement-level triggers on it (`__reflex_heal_ins`, `__reflex_heal_upd`, `__reflex_heal_del`). A write to that source only queues the affected partition keys in `public.__reflex_heal_pending`. An `UPDATE` queues a key only when a column the query reads changed. The rebuild happens later, in one of three places:

- `reflex_heal_ignored_sources()`, explicitly;
- [`reflex_scheduled_reconcile`](reflex_scheduled_reconcile.md), which heals before its age-gated reconciles;
- [`reflex_doctor(fix => TRUE)`](reflex_doctor.md), check **F14**.

While keys are queued, [`reflex_ivm_status`](reflex_ivm_status.md) reports the IMV, and every IMV reading it, as `known_stale`, with a `stale_reason` naming the ignored source, the keys and this function.

```sql
UPDATE demand_planning SET status = 'validated' WHERE id = 471;
-- sop_forecast_view is now known_stale for partition key 471

SELECT reflex_heal_ignored_sources('sop_forecast_view');
-- HEALED 1 IMVs
```

## Behaviour

- IMVs are healed shallowest first. Each one's queued keys are rebuilt by a single [`reflex_reconcile_partition`](reflex_reconcile_partition.md) call. A key with no matching IMV partition is a no-op and is drained.
- A heal removes exactly the queue rows it read. A key queued again while the heal runs keeps its newer timestamp and stays queued for the next sweep.
- A failed heal leaves its rows queued with `last_error` set and raises a `WARNING`.
- `drop_reflex_ivm` deletes the IMV's queued rows. The triggers stay on the source and do nothing for an IMV that no longer exists.

## Which ignored sources heal

Only an ignored source whose changed rows can be mapped to partitions:

- the IMV is partitioned, and the join condition contains `<source>.<col> = <expression projected as the first partition column>`;
- the join is `INNER`, or a `LEFT JOIN` of the ignored source;
- the condition has no `OR`, and the source appears under one alias.

Any other ignored source keeps the pre-1.11.4 contract: its changes never reach the IMV. `create_reflex_ivm` refuses such an entry unless acknowledged (see [`reflex_ack_ignore_source`](reflex_ack_ignore_source.md)). `TRUNCATE` of an ignored source queues nothing.

For an IMV created before 1.11.4, the upgrade runs [`reflex_rebuild_imv_metadata`](reflex_rebuild_imv_metadata.md) on every enabled partitioned IMV with ignored sources, which installs the triggers.

## pg_cron recipe

```sql
SELECT cron.schedule('reflex-heal', '*/5 * * * *',
    $$ SELECT reflex_heal_ignored_sources() $$);
```

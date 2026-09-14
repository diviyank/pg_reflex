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

- IMVs are healed shallowest first. Each one's queued keys are rebuilt by one partition reconcile, in its own subtransaction. A key with no matching IMV partition has no rows to rebuild and is drained.
- A heal removes exactly the queue rows it read. A key queued again while the heal runs keeps its newer timestamp and stays queued for the next sweep.
- A failed heal, including one that raises (a key the partition column cannot parse), rolls back only that IMV's heal, leaves its rows queued with `last_error` set, raises a `WARNING`, and lets the other IMVs heal. `reflex_scheduled_reconcile` and `reflex_doctor` carry on.
- Keys queued for a disabled IMV wait: nothing heals, reports or fails them until it is enabled again.
- `drop_reflex_ivm` deletes the IMV's queued rows. The triggers stay on the source and do nothing for an IMV that no longer exists.

## Writes to the ignored source

The triggers never make a write to the ignored source fail, and never make it expensive:

- The trigger function is `SECURITY DEFINER` with a pinned `search_path`, so a role that writes the source needs no grant on the pg_reflex tables.
- It opens no subtransaction.
- Keys are stored as `to_jsonb(<col>) #>> '{}'`, so a date key never depends on the writer's `DateStyle`.
- `TRUNCATE` cannot be scoped to keys: it marks the IMV `known_stale` with `SELECT reflex_reconcile('<imv>');`.
- If a mapped or watched column of the source was renamed or dropped, the write goes through and the IMV is marked `known_stale`: its changes can no longer be healed until the IMV is recreated against the current columns.

## Which ignored sources heal

Only an ignored source whose changed rows can be mapped to partitions:

- the IMV is partitioned, and the join condition contains `<source>.<col> = <expression projected as the first partition column>`;
- the join is `INNER`, or a `LEFT JOIN` of the ignored source;
- the condition has no `OR`, and the query reads the source exactly once (no second reference in a subquery, CTE or self-join);
- the source column and the partition column have the same type, two integer types, or two string types.

Any other ignored source keeps the pre-1.11.4 contract: its changes never reach the IMV. `create_reflex_ivm` refuses such an entry unless acknowledged (see [`reflex_ack_ignore_source`](reflex_ack_ignore_source.md)).

Only writes through the table the triggers are on are seen: on a partitioned ignored source, a write addressed directly to one of its partitions queues nothing, as for the regular maintenance triggers.

For an IMV created before 1.11.4, the upgrade runs [`reflex_rebuild_imv_metadata`](reflex_rebuild_imv_metadata.md) on every enabled partitioned IMV with ignored sources, which installs the triggers.

## pg_cron recipe

```sql
SELECT cron.schedule('reflex-heal', '*/5 * * * *',
    $$ SELECT reflex_heal_ignored_sources() $$);
```

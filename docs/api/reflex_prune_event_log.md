# `reflex_prune_event_log`

(1.11.4+) Deletes maintenance event-log rows older than an interval.

## Signature

```sql
reflex_prune_event_log(_older_than INTERVAL) RETURNS BIGINT
```

Returns the number of rows removed.

## Behaviour

`public.__reflex_event_log` records caught flush failures and slice-changing rebuilds (see [`reflex_ivm_status`](reflex_ivm_status.md#maintenance-event-log)). Its volume is bounded by design — nothing is written on an ordinary successful flush — but it is never pruned automatically.

```sql
SELECT reflex_prune_event_log(INTERVAL '90 days');
```

A `rebuild` row newer than the last `ANALYZE` of both the IMV target and the rebuilt slice makes `reflex_ivm_status` count that IMV exactly. A partition swap ANALYZEs its slice, so this only persists when that ANALYZE did not happen. Pruning such a row retires the signal, so prefer `ANALYZE <imv>` or `reflex_reconcile('<imv>')` over pruning to clear it.

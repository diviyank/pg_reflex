# 2026-07-27 — the partition flush's DO block commits its own destructive DDL when the reconcile it wraps reports failure

**Status: untreated.** This is the NARROWED residual of
`2026-07-27_reconcile_partition_commits_destructive_sync_on_failure.md`. The half that
lived inside `reflex_reconcile_partition` is fixed: the function now runs its orphan-swap
cleanup, its `drop_orphans` sync and its whole swap loop inside an explicit subtransaction
that is rolled back on any reported failure, on both the standalone and the `skip_sync`
batch path. Pinned by `pg_part_failed_reconcile_rolls_back_presync_orphan_drop`,
`…_presync_child_creation` and `pg_part_failed_skip_sync_reconcile_rolls_back_children_already_swapped`.

What is NOT fixed is the same property one level up, in the caller.

## The mechanism

`reflex_flush_partitions_impl` builds a per-root statement list and dispatches it as one
plpgsql `DO` block (`src/partition.rs:3108-3128`). The list is, in order:

```
PERFORM public.reflex_sync_partitions(<imv>, true);          -- :2936  destructive: drops orphan children
DROP TABLE IF EXISTS "<schema>"."<tgt_child>" CASCADE;       -- :2944
DROP TABLE IF EXISTS "<schema>"."<int_child>" CASCADE;       -- :2948
PERFORM public.reflex_reconcile_partition(<imv>, '', <nodes>, true);
PERFORM public.__reflex_refresh_partition_snapshot(<root>);
DELETE FROM public.__reflex_partition_pending WHERE source_root = <root>;
```

wrapped in `BEGIN … EXCEPTION WHEN OTHERS THEN <bump failures, set known_stale, WARN> END`.

A plpgsql `EXCEPTION` block is a subtransaction, but it only rolls back when an error is
**RAISED**. `reflex_reconcile_partition` reports failure by **RETURNING** `ERROR: …`, and
`PERFORM` discards that string. So on a failed reconcile the block completes **normally**
and RELEASEs: the sync's orphan drops and both `DROP TABLE … CASCADE` statements commit.

The original report's stated property therefore still holds here verbatim: *an arbitrary
DDL side effect of a failed operation is committed and reported as a failure.* It moved
from the Rust pre-sync into the plpgsql pre-sync of the automatic path.

## Why it is worse than it looks

The same swallowed return value also defeats the block's own failure accounting — the
`EXCEPTION` branch never fires, so `__reflex_refresh_partition_snapshot(root)` and the
`DELETE FROM __reflex_partition_pending` still run. `failures` stays 0, `known_stale` is
never set, no WARNING is logged, the pending row is gone, and the snapshot refresh
destroys the evidence that would have made the next flush retry. Silent staleness on top
of committed destructive DDL. That accounting half is tracked separately in
`2026-07-27_reconcile_partition_error_string_swallowed_by_perform.md`; the two share a root
cause and should probably be fixed together.

## What was ruled out

* Not the in-function atomicity bug — that is fixed and mutation-tested. Reverting the
  subtransaction turns those tests RED; this report's DDL is outside it either way.
* Not fixable by having the reconcile RAISE: `reflex_doctor`'s repair path, the audit
  report and the batch flush all inspect the returned text, and a raise would abort the
  caller's transaction.

## Reproduction sketch

Build a partitioned IMV over a real partitioned source, arrange one node of a root to fail
the reconcile (e.g. name a node with no target bound) while another node legitimately needs
an orphan drop, then run `reflex_flush_partitions(<root>)`. Observe: the orphan child is
gone, the pending row is deleted, `failures` is 0, and the flush reports success.

Severity: high when it fires (committed destructive DDL presented as a failure, on the
automatic path), low frequency (needs the flush's pre-sync to mutate AND the reconcile to
then report failure).

## Fix direction

Capture the reconcile's return text in the DO block instead of `PERFORM`ing it, and
`RAISE` on a leading `ERROR:` so the block's own `EXCEPTION` branch rolls the destructive
statements back and does its accounting. That single change closes both this report and
the flush half of the swallowed-`ERROR` report. Pin it with a test asserting the orphan
child survives, the pending row survives with `failures = 1`, and `known_stale` is set.

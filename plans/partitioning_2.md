# Partition management for pg_reflex

## Context

The starting question was "how do we almost never block readers during trigger updates?" The investigation surfaced that the blocking sites are the `TRUNCATE`+rebuild paths in `reflex_reconcile` and the self-join branch — and that fully fixing them in the general case (any IMV, including those with user views/MVs/FKs depending on them) requires OID-preserving rebuild machinery.

The decision: rather than solve this generally for every IMV, **partitioning becomes the opt-in mechanism** for users who care about reader availability. Partitioned IMVs get partition-scoped reconcile (only the dirty partition rebuilds; readers of other partitions are never blocked) and partition-wise maintenance. Non-partitioned IMVs keep today's `TRUNCATE`-based reconcile semantics — fast updates, accepted reader-blocking, no change.

The existing design in [`plans/partition_plan.md`](../../fentech/tools/pg_reflex/plans/partition_plan.md) (referenced as PP) already covers most of what's needed: opt-in `partition_by` argument, anchor-source-driven LIST/RANGE strategy, sync function, partition-scoped reconcile, cascading dependent IMVs. This plan is **PP plus two targeted extensions** and a small inventory correction.

## Scope

Implement PP as written, with these additions and clarifications:

1. **`reflex_sync_partitions` also drops IMV partitions whose source counterparts have been dropped.** PP today says "Never drop — log a NOTICE when a partition exists on the IMV but not the source" (PP §reflex_sync_partitions). The user's stated requirement is symmetric automatic management: source partition added → IMV partition created; source partition dropped → IMV partition dropped. We change "log NOTICE" to "issue `DROP TABLE … CASCADE` on the orphan IMV child" by default, with an opt-out flag for operators who want the legacy NOTICE behavior.

2. **Auto-follow when the main source is partitioned, even without an explicit `partition_by` argument.** PP §Phase 5 ("Passthrough auto-mirror") already covers passthrough IMVs. Extend the auto-mirror logic to **aggregate IMVs too** when:
   - exactly one source table is partitioned, AND
   - that source's partition column is in the `GROUP BY` set.

   In that case derive `partition_by` from the source's `pg_partitioned_table.partattrs` and proceed as if the user had passed it. If the source's partition column is not in `GROUP BY`, we cannot auto-partition (PP §Cons #1 — unique-index-must-include-partition-key); log a NOTICE and create the IMV unpartitioned.

3. **Reader-blocking semantics are clarified in `docs/concepts/delta-processing.md`.** The current lock table needs updating to reflect that partitioned IMVs limit reader blocking to the affected partition. Today's documentation talks only about full-table locks; the partitioned case is a new dimension.

4. **Track A (self-join `TRUNCATE` → `DELETE`) is dropped.** The user has explicitly chosen to keep `TRUNCATE` semantics for non-partitioned IMVs in service of fast-update performance. Self-join sources on non-partitioned IMVs therefore retain today's behavior. (Self-joins on partitioned IMVs would inherit partition-scoped reconcile if they qualify for it; nothing extra to do.)

## Files to modify

This list is a delta over PP's "Critical files" section. Every PP path stands; the additions are the drop-sync logic and the aggregate auto-mirror.

- **`src/partition.rs`** (new file, per PP): the sync function gets a `drop_orphans: bool = true` flag. When true, IMV partitions absent from the source are dropped via `DROP TABLE <child> CASCADE` on both intermediate and target. CASCADE here is local to the IMV's internal structure — it does not reach any user-owned object because the IMV partition children are pg_reflex-owned.
- **`src/create_ivm.rs:25`** (`create_reflex_ivm_impl`): in addition to PP's passthrough auto-mirror, when the user did not supply `partition_by` but the resolved anchor source is partitioned AND its partition column ∈ `AggregationPlan.group_by_columns`, derive `partition_by` from the source.
- **`src/lib.rs:152-217`**: `reflex_sync_partitions` SQL signature gains an optional `drop_orphans BOOL DEFAULT TRUE` argument.
- **`docs/concepts/delta-processing.md`**: extend the lock table with rows for partition-scoped operations on partitioned targets. The existing four `AccessExclusiveLock` rows acquire a partitioned-IMV qualifier: lock is on the affected partition child, not the parent.
- **`docs/concepts/internals.md`**: add a section pointing at the partition design and the sync/drop semantics.
- **`CHANGELOG.md`**: entry under the next major version covering PP + the two extensions in this plan.

## Existing utilities to reuse

- `pg_get_partition_constraintdef` — PP's mechanism for partition bound matching.
- `pg_inherits`, `pg_partitioned_table`, `pg_class.relpartbound` — PP's introspection surface.
- `reflex_rebuild_triggers` — unchanged; triggers attach to source roots regardless of partitioning.
- The existing TRUNCATE-based reconcile path in `reconcile.rs` — retained as-is for non-partitioned IMVs.

## Verification

### Unit tests (pgrx)

In addition to PP's per-phase tests:

- `src/tests/pg_test_partition.rs` (new): when a source partition is dropped via `ALTER TABLE source DETACH PARTITION … ; DROP TABLE …`, calling `reflex_sync_partitions(view)` drops the matching IMV partition (both intermediate and target). When `drop_orphans => FALSE` is passed, the IMV partition is preserved and a NOTICE is logged.
- Aggregate auto-mirror: create a partitioned source with partition column in GROUP BY, call `create_reflex_ivm` without `partition_by` — assert the IMV is partitioned identically. Create the same source with partition column NOT in GROUP BY, call `create_reflex_ivm` without `partition_by` — assert the IMV is unpartitioned and a NOTICE was emitted explaining why.
- Reader-blocking proof on partitioned IMVs (integration, two sessions): bulk write to partition X triggers reconcile of partition X only; concurrent `SELECT … WHERE partition_key = Y` (Y ≠ X) runs without blocking.

### Bench

`benchmarks/bench_sop_forecast.sql` — runs unchanged. The IMV here is not partitioned (current MV definition is flat), so the bench exercises the legacy `TRUNCATE`-based reconcile path. **Expected: zero change to wall-clock.** This validates that the non-partitioned path is untouched.

New bench `benchmarks/bench_partitioned_imv.sql`: same workload shape on a partitioned IMV (partition the IMV by `order_date` bucket or similar). Compare:
- Full reconcile wall-clock: partitioned vs unpartitioned. Expectation: partitioned is similar or slightly slower (CASCADE TRUNCATE + per-child INSERT).
- Partition-scoped reconcile wall-clock: should be O(partition_size/IMV_size) of full reconcile.
- Concurrent reader latency during partition-scoped reconcile: SELECT on a non-dirty partition should run uninterrupted; SELECT on the dirty partition still blocks (but only for the partition's reconcile duration, not the whole IMV's).

### CI gates

`cargo pgrx check && cargo pgrx test && cargo clippy && cargo fmt --check` — all green.

## Phasing

Tracks PP's six phases as written. The two extensions slot in naturally:

- **Phase 3 — Sync function + reconcile wiring** absorbs the `drop_orphans` flag and the symmetric add/drop behavior.
- **Phase 5 — Passthrough auto-mirror** extends to aggregate IMVs with partition-column ∈ GROUP BY.

No new phase needed; the extensions are local additions to existing phases.

## Out of scope

- Reconcile semantics for **non-partitioned IMVs**: deliberately unchanged. `TRUNCATE`-based, fast update, accepted reader-blocking. If a user wants non-blocking reconcile, the path forward is to partition the IMV.
- Self-join `TRUNCATE` → `DELETE`: dropped per user direction.
- OID-preserving rebuild mechanisms for non-partitioned IMVs (relfilenode-swap, RENAME swap, internal `_reflex_gen` sentinel): considered, not adopted. Partitioning subsumes the use case.
- HASH partitioning support: PP defers it; no change here.
- Repartitioning a live IMV without rebuild: PP defers it; users `drop_reflex_ivm` + recreate with new `partition_by`.
- Cross-column partition mapping in cascades: PP defers it; downstream IMVs partitioned on a different column fall back to full reconcile (per PP §Cons #9).

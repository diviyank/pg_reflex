# One COMMIT that meets a lock timeout or a deadlock can suspend a partition root: every queued flush counts a failure, whatever the SQLSTATE — S2 (root suspended, IMVs stop following the source until an operator re-arms it)

Found while fixing `2026-10-12_default_drain_catalog_query_locks_every_partition_in_the_database`
(branch `fix/v112-partition-catalog-locks`). That fix removes the cross-schema lock that produced
the deadlocks of the field report; this report is about what the flush does with *any* transient
lock error, which is unchanged.

## Mechanism

Two facts combine.

1. **A failure is counted per queued flush, not per transaction.** The DDL event trigger
   (`src/lib.rs`, `pg_reflex_event_trigger`) runs
   `INSERT INTO __reflex_partition_pending … ON CONFLICT DO UPDATE SET enqueued_at = …` once per
   partition DDL command, and `__reflex_partition_flush_trigger` is
   `AFTER INSERT OR UPDATE OF enqueued_at … DEFERRABLE INITIALLY DEFERRED FOR EACH ROW`. A
   transaction that runs N partition DDL commands on one root therefore queues N flushes of that
   root at COMMIT. When the first fails, the pending row stays, so each of the other N-1 runs the
   whole flush again and fails the same way.
2. **Every error counts.** The per-root block built in `reflex_flush_partitions_impl`
   (`src/partition.rs`, the `do_block` string) ends with
   `EXCEPTION WHEN OTHERS THEN UPDATE __reflex_partition_pending SET last_error = …, failures = failures + 1`.
   `40P01` (deadlock detected), `40001` (serialization failure) and `55P03` (lock not available /
   lock timeout) are counted like a logic error. At `PARTITION_FLUSH_FAILURE_CAP` (5) both flush
   entry points skip the root.

So "5 consecutive failures" is not five failed commits: one transaction that swaps three leaves
(six DDL commands) and loses one lock race reaches the cap inside that single COMMIT.

## Evidence

- Field log of the parent report: the flush of `omc.sales_simulation` is logged as failed at least
  4 times inside one COMMIT, and the pending row afterwards reads `attempts 31, failures 5,
  last_error 'deadlock detected'`.
- Reproduced on this branch with the original drain query restored (mutation run
  `f-mut1-original-query.log`, test
  `v112_pl_maintaining_one_schema_takes_no_lock_in_another_*`, PostgreSQL 18): one transaction
  `DETACH PARTITION …; ATTACH PARTITION …` (two partition DDL commands) under `lock_timeout = 4s`
  leaves `tb.sim failures=2 (canceling statement due to lock timeout)`; the next transaction that
  adds one partition leaves `failures=3`; two manual flushes later the root is at 5 and every
  later flush of it is skipped.

## Not established

- Whether `40001` can be raised inside the flush at all (it runs the reconcile at the caller's
  isolation level; not reproduced).
- Whether the failed flush of the first queued event can be made to stop the remaining events of
  the same COMMIT cheaply (no per-transaction marker exists today).

## Fix direction (hypothesis to falsify)

Either or both, in the `do_block` handler only:

- do not count a transient class: `failures = failures + CASE WHEN SQLSTATE IN ('40P01', '40001',
  '55P03') THEN 0 ELSE 1 END`, keeping `last_error`, `known_stale` and the WARNING as they are, so
  the root stays pending and is retried by the next flush;
- count at most one failure per transaction and root (for example compare `last_attempt_at`,
  which is stamped outside the block, with `transaction_timestamp()`).

Why it was not done with the lock fix: the first variant is three lines, but it changes the
suspension contract (a root that is blocked by a long-lived lock is then retried, and waits up to
`lock_timeout`, on every commit that touches it and on every `reflex_flush_partitions()` call,
with no cap), `reflex_doctor` and `reflex_ivm_status` classify roots on `failures`, and the block
is in the flush dispatch that the partition-swap cascade work is rewriting. It needs its own tests
(a real second session holding a lock, the count after one COMMIT with several DDL commands, the
doctor classification of a root that only ever failed transiently) and a decision on whether an
uncapped retry of `55P03` is wanted.

# An ERROR inside a push's rebuild region, under a user SAVEPOINT, aborts the backend

**Severity:** backend SIGABRT (a production abort would restart the whole cluster); seen on the
cassert pgrx build — whether a non-assert build also aborts is NOT yet established.
**Found:** 2026-10-14 by the adversarial review of the 1.12.2 push-scoped reader work
(ledger `2026-10-14-1.12.2-bulk-leaf-rebuild/A-review.txt`, F3; probes `A-review-probes.rs`, P3).

## Reproduction (base c0e138f)
Mutation N10: inject an ERROR into the existing `last_update_date` UPDATE of
`rebuild_partitions_and_queue_dependents` (src/partition.rs), then run a push inside a user
SAVEPOINT (probe P3) → SIGABRT. An injected ERROR in an existing cascade query on a push into an
empty leaf (N9) does not crash. A `statement_timeout` or cancel landing in that region is the
realistic trigger.

## Open
- Non-assert build: abort or clean error? (compare with the assert-only PG upstream e371cb7b89
  class seen in 1.12.0).
- Which subtransaction handling in the rebuild region (advisory lock + inner subtransaction)
  mis-unwinds when the outer transaction is itself in a user savepoint.
Related memory: the 2026-07 S1 bug-1 atomicity branch was rejected for a backend SIGABRT.

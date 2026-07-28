# 2026-07-28 — `reflex_reconcile`'s soft `"ERROR: …"` return is discarded by every `PERFORM` caller, committing a partially-rebuilt IMV with `known_stale` untouched

**Status: untreated. Pre-existing — not introduced by any current S1 branch.** Raised during
the adversarial review of `fix/swap-flattens-subpartitioned-child` and filed separately per the
`untreated_bugs/` hygiene rule. Adjacent to
`2026-07-27_reconcile_partition_commits_destructive_sync_on_failure.md`, which is the same
pattern on a different entry point.

Severity: **medium-high, latent.** No reproduction with live data loss has been constructed; the
mechanism is confirmed by tracing every consumer. It is a silent-failure hazard rather than a
demonstrated corruption, which is why it is filed rather than fixed inside an unrelated branch.

---

## Mechanism

`reconcile_one`'s partitioned branch reports per-child swap failures softly: it emits a
`warning!` and returns the **string** `"ERROR: partition reconcile failed"`
(`src/reconcile.rs`, partitioned branch). It does not `pgrx::error!`, by deliberate design —
raising would abort the caller's transaction.

Unlike `reflex_reconcile_partition_impl`, which wraps its work in an explicit `SubTransaction`
and rolls back on failure, `reconcile_one`'s partitioned branch has **no sub-transaction**. Its
swap loop is not atomic: children earlier in the loop have already been rebuilt when a later one
fails.

So the contract is "the caller must read the returned string". Consumers split cleanly:

**Read it and handle it (correct):**

* `src/trigger/deferred.rs:566` — `SELECT public.reflex_reconcile(...) AS r`, then flags
  `known_stale`. Pinned by `xsu_guard_reconcile_failure_flags_known_stale`.
* `reconcile_named_node` (`src/reconcile.rs:1247`) — `if result.starts_with("ERROR")`.
* `reflex_doctor` F4 / F4b.

**Discard it (`PERFORM`):**

* `src/trigger/dispatch.rs:146`, `:340`, `:568`
* `src/lib.rs:1322`, `:1333`, `:1371`, `:1381`, `:1393`
* `src/partition.rs:2331`, `:3523`

On any of those paths a failed reconcile leaves the transaction to **commit** with the IMV
partially swapped and `known_stale`, `stale_reason`, `stale_since` all untouched. Nothing in the
catalog or in `reflex_ivm_status()` records that the rebuild did not finish. The next reader sees
a mixture of rebuilt and stale partitions and no signal.

`PERFORM` is the correct plpgsql idiom for discarding a result set — the defect is that the
function communicates failure *through* the result, so `PERFORM` silently discards the error
channel.

## What was ruled out

* **"The `warning!` is the signal."** A `WARNING` is not durable and is not queryable. Monitoring
  keys off `known_stale` / `reflex_ivm_status()`, and neither is set on these paths.
* **"The transaction aborts anyway."** No — the return is a string, not a raise. `reconcile_one`
  chose the soft return specifically so the caller's transaction survives, and it does.
* **"It is atomic, so a failure changes nothing."** Not on this path.
  `reflex_reconcile_partition_impl` opens a `SubTransaction`; `reconcile_one`'s partitioned
  branch does not, so a mid-loop failure leaves earlier children already swapped.
* **"It is unreachable."** The trip-caps at `src/trigger/dispatch.rs:340` / `:568` call
  `reflex_reconcile` from ordinary DML whenever more than half an IMV's partitions are hot, and
  `src/lib.rs:1322`ff is the partition-delta fallback. These are live paths.

## Relationship to the sub-partition data-loss fix

The refusal guard added by `fix/swap-flattens-subpartitioned-child`
(`execute_partition_swap_for_child` returning `Err` on a `relkind='p'` child) creates a *new way
to reach* this soft return. With that branch's leaf-resolution fix also in place the guard is a
backstop that no operator path reaches — measured: reverting only the guard turns exactly one
test red, and reverting only the leaf resolution turns ten red, three of them pre-existing tests
that then fail with `ERROR: partition reconcile failed`.

So this defect is **not** a blocker for that branch, and it is **not** caused by it. It is the
pre-existing hazard the guard would ride on if leaf resolution ever regressed.

## Fix direction

Two options; the second is smaller and matches the codebase's existing recovery vocabulary.

1. **Make the partitioned branch atomic.** Wrap `reconcile_one`'s swap loop in a
   `SubTransaction` the way `reflex_reconcile_partition_impl` does, so a failure leaves the IMV
   exactly as it was. This is the stronger guarantee and removes the "partially swapped" half of
   the problem entirely, leaving only the silent part.
2. **Make the failure durable regardless of the caller.** Before returning
   `"ERROR: partition reconcile failed"`, set `known_stale = TRUE` with a `stale_reason` naming
   the child that failed. Then a `PERFORM` caller still discards the string, but the state is
   recorded and monitoring sees it. This is what `src/trigger/deferred.rs:566` already does
   manually, hoisted into the producer so it cannot be forgotten.

Doing both is defensible: (1) fixes the partial rebuild, (2) fixes the invisibility. Note that
under (1) alone the failure would still be invisible, and under (2) alone the IMV would still be
partially rebuilt but at least flagged.

## Acceptance test

A real partitioned IMV whose swap is made to fail mid-loop (a child made un-swappable, e.g. by
the sub-partitioned-child guard), driven through a `PERFORM` path rather than a
result-reading one. After the transaction commits:

* `known_stale` must be TRUE with a `stale_reason` naming the failure, **and/or**
* the IMV's contents must be byte-identical to the pre-reconcile state (no partial swap).

The test must be shown RED before the fix — today it commits clean with `known_stale = FALSE`.

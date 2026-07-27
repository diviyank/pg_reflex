# 2026-07-27 — every internal caller of `reflex_reconcile_partition` discards its `ERROR:` return

**Status: untreated.** Split out of
`2026-07-27_reconcile_partition_commits_destructive_sync_on_failure.md` (fixed:
a reported failure now rolls back its own DDL). That fix made "ERROR" honest —
nothing happened. It did **not** make anyone notice.

## The mechanism

`reflex_reconcile_partition` signals failure by RETURNING a string starting with
`ERROR:`, never by raising. Every internal caller throws that string away:

| Caller | Form |
|---|---|
| `src/partition.rs:1886` (same-partition dependent cascade) | `client.update("SELECT public.reflex_reconcile_partition(child, keys)")`, result unread |
| `src/partition.rs:2964` (batch flush, per root) | `PERFORM public.reflex_reconcile_partition(imv, '', nodes, true)` |
| `src/trigger/dispatch.rs:346`, `:573` | `PERFORM public.reflex_reconcile_partition(...)` |

`PERFORM` evaluates the call and discards the row, so a returned `ERROR:` string
is indistinguishable from success to the plpgsql block around it.

## Why it matters

The flush's per-root DO block relies on an exception to decide a root failed:

```
DO $$ BEGIN
  ... PERFORM public.reflex_reconcile_partition(imv, '', nodes, true); ...
  PERFORM public.__reflex_refresh_partition_snapshot(root);
  DELETE FROM public.__reflex_partition_pending WHERE source_root = root;
EXCEPTION WHEN OTHERS THEN  -- bumps failures, sets known_stale, WARNs
```

A reconcile that returns `ERROR:` raises nothing, so the `EXCEPTION` branch never
fires: the snapshot is refreshed and the pending row deleted **as if the leaf had
been reconciled**. The IMV slice stays stale, `failures` is not incremented,
`known_stale` is not set, no WARNING is logged, and the snapshot refresh destroys
the evidence that would have made the next flush retry it. Silent staleness with
no operator signal.

The trigger-dispatch call sites have the same shape: a failed partition reconcile
in the write path is invisible.

## What was ruled out

* Not the atomicity bug — that is fixed; the call leaves no partial DDL behind.
  The residue here is purely "the failure is never reported to anyone".
* Not fixable by making the function RAISE: `reflex_doctor`'s repair path, the
  audit report, and the batch flush all inspect the returned text, and a raise
  would abort the caller's transaction. The return-string contract is deliberate.

## Reproduction sketch

Build a partitioned IMV, drive `reflex_flush_partitions` for a root whose leaf
reconcile fails (e.g. a leaf whose target child has no bound), and observe:
the pending row is deleted, `failures` stays 0, no WARNING is emitted, and the
IMV slice is stale.

Severity: medium-high (silent staleness, self-clearing pending queue), and it is
the mechanism by which a genuine per-leaf failure disappears from the flush's own
failure accounting.

## Fix direction

Capture the returned text at each call site instead of `PERFORM`ing it, and on a
leading `ERROR:` either `RAISE` inside the DO block (so the existing EXCEPTION
branch does its accounting) or bump `failures` / `known_stale` directly. The
flush block is the one that matters most — it already owns the retry accounting
that this bypasses. Pin it with a test that fails ONE leaf of a multi-leaf root
and asserts the pending row survives with `failures = 1`.

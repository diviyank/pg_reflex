# 2026-07-28 — `build_scoped_cascade_reconcile`'s EXCEPTION fallback calls the one-argument `reflex_reconcile`, escalating `drop_orphans` to TRUE

**Status: untreated. PRE-EXISTING** — not introduced by
`fix/swap-ddl-destroys-dependents`. Found while fixing that branch's finding F1, which was
the *same* defect at a different call site; F1 was a regression and is fixed, this one
predates it and is filed separately per hygiene.

Severity: **medium — unauthorized data destruction**, but on a narrower path than F1 was.

## Where

`src/partition.rs:2331`, inside the plpgsql emitted by
`build_scoped_cascade_reconcile`:

```sql
PERFORM public.reflex_reconcile({child_lit});
```

`public.reflex_reconcile(text)` is the ONE-argument form, which is
`reflex_reconcile_with_orphans(child, true)` (`src/lib.rs:723-727`). The two-argument
overload exists specifically so a caller can decline to authorize partition destruction
(`src/lib.rs:705-719`, `src/doctor.rs:330-336`: *"instead of silently dropping orphan
partitions the way the one-argument form does"*).

The same escalation appears at `src/partition.rs:3571`
(`PERFORM public.reflex_reconcile({})` in the flush path) — worth checking in the same
pass, though it may have different authorization semantics since the flush has no operator
`drop_orphans` input to honour.

## Why it is a real hazard

A dependent's PRESERVED orphan partition holds the user's rows: the source partition went
away, the IMV partition was deliberately kept because nobody authorized deleting it (the
event trigger's own comment: *"orphan deletion is never automatic (IMV data is the user's
…)"*). Reaching `reflex_reconcile(child)` one-arg drops it.

## Reachability — the part that needs establishing

Unlike F1, this is **not** reachable from `reflex_reconcile(x, FALSE)`. It sits on
`reflex_reconcile_partition_impl`'s cascade, entered from:

* `reflex_reconcile_partition(...)` called directly by an operator,
* the COMMIT-time partition flush,
* the trigger-dispatch partition trip-cap.

None of those currently carry an operator `drop_orphans` choice to violate, which is why
this is medium rather than high. **Step 0 for this report is to establish whether any
caller that has declined orphan destruction can reach it.** If none can, the right outcome
may be a no-fix plus a comment recording why the one-argument form is safe *here* — which
is itself valuable, because the next reader will otherwise flag it exactly as the reviewer
did.

Note also that this line is only reached on the EXCEPTION fallback inside the scoped
cascade, i.e. when the key-scoped rebuild failed — a path with no direct test coverage.

## Fix direction

If reachability is established: thread the caller's `drop_orphans` into
`build_scoped_cascade_reconcile` and emit `reflex_reconcile({child}, {drop_orphans})`.
`reflex_reconcile_partition_impl` would need to accept and forward the flag, which changes
a `#[pg_extern]` signature — check `cargo pgrx schema` and the migration implications
before committing to it.

## Acceptance test

A partitioned IMV whose dependent holds a preserved orphan partition, driven through
`reflex_reconcile_partition` on a path that takes the EXCEPTION fallback, with the caller
having declined orphan destruction. The dependent's orphan must survive. Must be shown RED
first — if it cannot be made RED, that is the negative result and the report closes with a
comment instead of a change.

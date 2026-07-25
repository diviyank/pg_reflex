# 2026-07-25 — swap-path F3 orphan-heal scopes to the tree ROOT, not the leaf's immediate parent, on multi-level partitioned IMVs

**Status: untreated.** Discovered as an adjacent finding while fixing
`nightly_swap_target_overlap_restale` (this branch, `fix/ps16-ddl-sync-overlap-restale`);
not touched by that fix, filed separately per the bug-resolution methodology's
"adjacent bugs get their own report" rule.

## What's off

`execute_partition_swap_for_child`'s F3 heal (src/partition.rs, the block right
before `attach_new_tgt`) drops a confirmed orphan whose bounds exactly match the
incoming swap target, to avoid a `would overlap partition` abort. It computes the
candidate set via:

```rust
let tgt_children = list_partition_children(client, &tgt_parent);
```

`tgt_parent` here is `quote_identifier(view_name)` — the **tree root** — not
`tgt_immediate_parent` (`read_immediate_parent_qual(...)`), which the DETACH/ATTACH
DDL just above this same block correctly uses. For a single-level partitioned IMV
the root *is* the immediate parent, so this is currently correct in every shipped
scenario. For a **multi-level** partitioned IMV (e.g. `LIST(region) -> LIST(quarter)`,
swapping a leaf like `..._west_q1`), the leaf's true immediate parent is the `west`
branch table, not the root — so this heal lists the wrong level of children (root's
direct children, e.g. `east`/`west`) and compares their bounds against the leaf's
bound (e.g. `'Q1'`). The bounds never match at the wrong level, so the heal silently
finds nothing and does nothing.

## Why this is lower severity than the swap-overlap bug it was found alongside

This is an **under-reach**, not an over-reach: comparing the wrong level of
siblings just fails to find a match, so the code neither wrongly drops live data
(the sibling bug's original defect) nor wrongly preserves something it shouldn't —
it just fails to heal a same-bound collision at a nested level, leaving the swap to
abort with the original `would overlap partition` error in that specific case. Also,
this function is invoked from `reflex_reconcile_partition_impl` and the deferred
commit-time flush, both of which already run `reflex_sync_partitions_impl(..,
drop_orphans=true)` beforehand — that whole-tree, depth-agnostic drop (now
correctly parent-scoped by the sibling fix) already clears most confirmed orphans
before this swap-level heal would ever need to. So the practical exposure is
narrow: a nested-leaf swap whose orphan collision survives the earlier
drop_orphans=true pass (e.g. a leaf swap-only path that doesn't route through
`reflex_sync_partitions_impl` first).

## Fix direction

Change `let tgt_children = list_partition_children(client, &tgt_parent);` to use
`tgt_immediate_parent` (already computed a few lines above, for the DETACH/ATTACH
DDL) instead of `tgt_parent`. Mirrors the fix already applied to the sibling
`reflex_sync_partitions_impl` create-loop heal (`drop_bound_collision_orphan`),
which resolves the exact immediate parent per node via
`PartitionNodeDdl.int_parent_qual`/`tgt_parent_qual`.

## Regression test needed

A multi-level partitioned IMV (3+ levels or 2 levels with sibling branches sharing
a repeated leaf bound, matching the sibling fix's `east`/`west`/`'Q1'` fixture)
where a nested leaf's swap-target collides with a same-bound orphan under its OWN
immediate parent — assert the swap heals and does not abort with `would overlap
partition`. Not yet written; no fix has been attempted.

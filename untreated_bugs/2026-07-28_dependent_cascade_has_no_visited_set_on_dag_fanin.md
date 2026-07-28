# 2026-07-28 — the dependent cascade has no visited set, so a DAG fan-in node is rebuilt once per PATH

**Status: untreated.** Found by adversarial review of
`fix/swap-ddl-destroys-dependents` (finding F3) and filed rather than folded in, per
`untreated_bugs/` hygiene. **Correctness is unaffected** — a full reconcile is idempotent
and the last rebuild of a fan-in node happens after all its inputs are fresh. This is a
complexity defect.

Severity: **low-medium.** Redundant work only, but it compounds per layer and each
redundant dispatch also re-runs `reflex_sync_partitions` over the dependent's whole
partition tree.

## Where

`src/reconcile.rs`, `cascade_partitioned_rebuild_to_dependents`:

```rust
for dep in &dependents {
    let result = reflex_reconcile_with_orphans(dep, drop_orphans);
    ...
}
```

Each call re-enters `reflex_reconcile_with_orphans` → `cascade_partitioned_rebuild_to_dependents`
with no memo and no cycle guard. The same shape exists in the older fan-out in
`reflex_reconcile_partition_impl` (`src/partition.rs`, the `for child in &children` loop),
so a fix should probably cover both.

## Topology that exhibits it

`depends_on_imv` explicitly supports an IMV reading two IMVs that both read a third:

```
A ─→ B ─┐
│       ├─→ D
└─→ C ──┘
```

`reflex_reconcile('A')` dispatches B and C; B dispatches D; C dispatches D again. **D is
fully rebuilt twice.** With L such layers the cost is the number of distinct root-to-node
paths, which is exponential in L for a fully-connected fan-in, not the number of nodes.

The handoff on that branch claimed *"O(D) dispatches for D direct dependents, once per
reconcile"*. That is true per node for a **tree** and false for a DAG; the branch's own
doc comment has since been corrected to say so, but the behaviour is unchanged.

**Not yet reproduced with timings.** The topology above is the reproduction to build:
assert D's rebuild count (e.g. via `last_update_date` bumps, a counting trigger, or
`reflex_ivm_status()` flush counters) is 1, not 2.

## Cycle risk — PLAUSIBLE only

A genuine `graph_child` cycle would not terminate. The reviewer looked for a path that
creates one and found none — `create_reflex_ivm` rejects cycles — so this is not a claimed
defect. But a visited set closes it for free, which is an argument for preferring that fix
over merely documenting the complexity.

## Fix direction

A transaction-scoped visited set, so each IMV is rebuilt at most once per top-level
reconcile. Options:

* A `HashSet<String>` threaded through the recursion — clean, but the recursion crosses a
  SQL boundary (`reflex_reconcile_with_orphans` is re-entered through a `#[pg_extern]`),
  so it cannot simply be a Rust parameter without changing that signature.
* A transaction-scoped GUC or a temp relation holding the visited names, in the style of
  `pg_reflex.internal_reconcile_root` / `pg_reflex.internal_swap_root`. Fits the existing
  idiom; must be cleared on every exit path, and must not suppress a *later, separate*
  top-level reconcile in the same transaction.

Correctness bias: a visited set that wrongly marks a node visited would **skip** a needed
rebuild — silent staleness. So it must be keyed to a single top-level reconcile invocation,
and when in any doubt must fail toward rebuilding again.

## Acceptance test

1. The diamond above: `reflex_reconcile('A')` rebuilds D exactly once, and D satisfies the
   bidirectional `EXCEPT ALL` oracle afterwards.
2. Two *separate* `reflex_reconcile` calls in one transaction must each still do their
   work — the visited set must not leak between them.
3. Both shown RED / GREEN appropriately under mutation of the visited set.

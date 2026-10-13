# The stale report from a queued generated sub-IMV reaches its parent through `graph_child` only — S4 (hardening)

Found by the 1.12.1 review of `heal_stale_reasons_by_imv` (`src/heal.rs`, commit `0f8054a`). No real
fixture reproduces a wrong result; filed so the dependency is visible.

## What holds

After a partition swap honours `ignored_sources`, `reflex_ivm_status` reports every IMV the heal queue
leaves wrong by walking readers from each queued node. The walk follows `graph_child` only: the
`depends_on_imv` edge was removed as redundant, and the `depends_on &&` edge cannot reach a parent
from its generated sub-IMV, because the parent's `depends_on` stores the sub-IMV name quoted while the
walk compares unquoted names.

Every path that writes `depends_on_imv` also calls `add_graph_child_links`
(`create_ivm/mod.rs`, `decompose.rs`), `drop_reflex_ivm` removes only the dropped child's link, and
`graph_repair` rebuilds both arrays.

## Residual

A registry whose `graph_child` was edited by hand, or never backfilled, would leave the parent IMV of
a queued sub-IMV wrong with no `known_stale` report until the heal runs.

## Fix direction

Also match the quoted name (`'"' || origin || '"'`) in the `depends_on` comparison of the walk, or keep
the `depends_on_imv` edge beside `graph_child`; both only widen the stale set (safe direction).

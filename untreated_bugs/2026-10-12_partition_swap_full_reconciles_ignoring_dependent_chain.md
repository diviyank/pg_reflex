# Residual of "a one-leaf partition swap fully reconciles the ignoring-dependent chain" — S3 (cost only)

The field report (1.11.5, PostgreSQL 17): replacing one leaf of `sales_simulation`
(LIST(plan) x RANGE(month)) made the COMMIT-time partition flush rebuild every plan of every
node of the `forecast_analysis_view` chain, several nodes more than once (565 s for one leaf,
push COMMIT cancelled by the client at 200 s).

## What was established and fixed

Fixed on branch `fix/v112-swap-cascade-scope` (`src/cascade.rs`, tests
`src/tests/pg_test_v112_swap_cascade.rs`).

- **Cause of the widening: identifier length, not staleness.** A dependent's mirror child is
  named `<dependent>_<source node>`; past 63 bytes the name is cut and hashed. The cascade
  recovered the source partition from the dependent's child name by stripping the
  `<dependent>_` prefix, which gives a truncated name for a hashed child, so the scoped
  rebuild failed on "missing intermediate/target bound"; the error was swallowed and the
  dependent was rebuilt whole. `<schema>.forecast_analysis_view__cte_forecast_bounds_` plus a
  `sales_simulation_p_<plan>_<year>_<month>` leaf crosses 63 bytes for a long schema name and
  not for a short one, which is the "smaller tenant takes the scoped branch" contrast of the
  original report. The stale flag, an orphan target partition and the leaf depth were each
  ruled out by reproduction.
- **A second widening:** a rebuild of partitions that were not diffed in place (a new plan)
  reached a same-column dependent through a source that is not its partition anchor
  (`date_limits -> forecast_sales`, `history_sales -> forecast_analysis_view`); its partition
  names could not be mapped and the dependent was rebuilt whole.
- **Once per path:** confirmed. A whole rebuild refreshed its own dependents, so a node
  reachable by several paths was rebuilt once per path, and the explicit flush additionally
  descended into the generated sub-IMVs of each (the x3 / x2 of the field log). At COMMIT the
  descent is skipped, which is why the explicit call of the field test cost more than the
  push.
- **Per leaf:** refuted. The swapped leaves of one root are handed to one call.
- **Unchanged leaf:** it cascades like a changed one, by design (see below).

Now a partition rebuild queues its dependents, merges the scopes reaching one dependent and
refreshes each once in dependency order. A dependent is refreshed for part of its rows only
when its query proves that a row's partition key is the rebuilt view's AND the part can be
named without guessing; otherwise it is refreshed whole once, with a NOTICE naming it and the
reason.

## Residual

1. **Several partition-mirroring IMVs over one source root.** The queue lives for one
   `reflex_reconcile_partition` call. The flush issues one call per IMV mirroring the root, so
   a node below two such IMVs is still refreshed once per IMV.
2. **Shapes whose scope is not proven are refreshed whole.** By construction (correctness
   first): the rebuilt view read twice (a sub-query counts), read inside a CTE or derived
   table, a set operation, a window, `DISTINCT ON`, a partition column taken from another
   relation that is not equated to the rebuilt view's by an inner join, RIGHT / NATURAL /
   USING joins, comma-joined tables, a FULL JOIN that is not the first join or is followed by
   anything but LEFT JOINs. Each costs one whole refresh per flush and says so in a NOTICE.
3. **Parts that cannot be named are refreshed whole.** A dependent that mirrors the rebuilt
   view's own partition tree is refreshed in the mirrored partitions. Any other dependent
   (one mirroring another root, an unpartitioned aggregate) is refreshed by key, and the keys
   are known only when the top-level bound lists them (`FOR VALUES IN (...)` of plain values)
   or the rebuilt partition was not created or altered in the transaction (it is then read
   before and after). So for such a dependent a replaced top-level RANGE or DEFAULT
   partition, and any partition holding a NULL key, means a whole refresh.
4. **Second partition level.** A dependent whose second-level column is not proven to be the
   rebuilt view's is refreshed in the whole first-level partition, not the leaf.
5. **An aggregate grouped by a qualified column (`GROUP BY v.plan`) is refreshed whole**: the
   registry stores its group column with the qualifier and the group-scoped refresh does not
   recognise it. Writing `GROUP BY plan` keeps the group scope.
6. **An unchanged leaf still refreshes the dependents that ignore the rebuilt view.** Ordinary
   writes do not reach such a dependent, so an empty diff of the swapped leaf does not prove
   it current; the swap is what catches it up.
7. **A reconcile queued for COMMIT from inside a trigger** still becomes a whole one, and a
   node reached both by the cascade and by that COMMIT-time reconcile is refreshed twice:
   `2026-10-02_in_trigger_scoped_reconcile_escalates_to_full_commit_rebuild.md`.
8. **Not verified in the field.** The cause was reproduced with the field names and shapes on
   a synthetic chain, not on the tenant. After the upgrade the push should show partition
   refreshes of the three direct dependents only; any `is refreshed whole, not by partition`
   NOTICE on that chain names what is left.

## Related, filed separately

- `2026-10-10_detach_partition_leaves_dependents_of_partition_mirroring_imvs_stale.md`
  (unchanged by this work).
- `2026-10-12_deferred_flush_into_partitioned_join_of_an_ignored_source_and_an_aggregate_fails_column_does_not_exist.md`
- `2026-10-12_create_raises_raw_syntax_error_for_a_partitioned_passthrough_joining_a_cte_aggregate_of_its_own_source.md`
- `2026-10-12_default_drain_catalog_query_locks_every_partition_in_the_database.md`
- `2026-10-12_swap_of_a_partition_of_a_source_that_is_not_the_mirrored_one_leaves_the_imv_stale.md`

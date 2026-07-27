# 2026-07-27 — partitioned passthrough IMVs are permanently on the non-sargable membership predicate

**Status: untreated.** Field-reported from `db_prod`: a `COMMIT` on an `omc.sales_simulation`
push ran **96 minutes** (pid 2375195, `application_name = reflex_flush:omc.sop_forecast_view`,
100% CPU, zero wait events, zero blockers) to maintain a **132 MB / 930k-row** IMV. It did
eventually commit; the next queued push immediately started another long `COMMIT`.

Two independent defects stack. **A** is the proximate cause and is fixable in codegen. **B**
makes the documented operator remedy a no-op, so there is currently **no operational
workaround** — the only lever is code.

## A — the partition-dispatch branch is the one branch PS-5 never gated

`passthrough_op_stmts` (`src/trigger/ops.rs`) has two arms per operation. The unpartitioned
arm uses `passthrough_keyed_delete_match` — the PS-5 runtime-gated fast/safe pair. The
**partition-dispatch** arm uses `passthrough_keyed_delete_predicate`, which returns the raw
ungated `null_safe_in`. Both the `DELETE` and `UPDATE` arms do this.

The carve-out is deliberate and documented at `src/trigger/ops.rs:1112-1115`:

> PS-5 deliberately does NOT gate this branch: the predicate is spliced into a DO block that
> also swaps the hot leaves, so emitting the block once per variant would run that swap TWICE
> (the swap is not gated and cannot be). Keeping the single always-NULL-safe form trades this
> branch's plan quality for correctness, which is the right direction.

The cost of that trade is stated by `null_safe_in`'s own doc (`src/trigger/ops.rs:993-997`):

> the NULL-safe branch matches the WHOLE target against `pt_old` with `IS NOT DISTINCT FROM`,
> which no operator family covers, so the planner's only option is a nested loop over the
> entire IMV.

**The objection does not hold.** The aggregate sibling solves exactly this problem 130 lines
earlier in the same file, `src/trigger/dispatch.rs:254-257`:

```rust
// PS-5 — the cold-partition MERGE may be a gated pair; split and bind USING
// on each part (the gated variants share the cold-filter `$1`/`$2`).
let merge_stmts: Vec<String> = split_reflex_statements(merge_sql_with_filter);
let merge_execs = execute_each(&merge_stmts, Some(using));
```

It does not emit the DO block per variant — it splits the gated pair into separate `EXECUTE`s
*inside one block*. The swap runs once, untouched. In
`build_passthrough_partition_dispatch_sql` the cold `DELETE`/`INSERT` are already separate
`EXECUTE format(...)` calls (`del_part` / `ins_part`), so gating them cannot duplicate the
swap. Only the cold **DELETE** carries a membership predicate; the cold `INSERT` is scoped by
`pt_new` via `scoped_delta_query` and needs no gate.

## B — passthrough IMVs never get NOT-NULL inference at all, and the remedy is a no-op

Measured on `db_prod`, all 190 registered IMVs:

| `not_null_columns` length | passthrough | aggregate |
|---|---|---|
| 0 | **95 (all of them)** | 85 |
| 6 | 0 | 5 |
| 9 | 0 | 5 |

Not one passthrough IMV in the entire cluster has a single recorded NOT-NULL column — including
columns that are catalog `NOT NULL`. `omc.sop_forecast_view`'s key is
`(dem_plan_id, product_id, location_id, order_date)`; `pg_attribute` says `dem_plan_id` and
`order_date` are `attnotnull = t`, yet the registry records `[]`.

Root cause — two early returns on `is_passthrough`:

- `initial_aggregate_materialization` (`src/create_ivm/mod.rs:1754`) is the only create-time
  caller of `infer_not_null_columns`, and opens with `if ctx.plan.is_passthrough { return; }`.
- `reflex_probe_not_null_columns_impl` (`src/create_ivm/soundness.rs:1533`) — the operator-facing
  re-probe — opens with `if plan.is_passthrough { return Ok(Vec::new()); }`.

Consequences:

1. `all_not_null` in `passthrough_keyed_delete_predicate` can **never** be true for a
   passthrough IMV, so its sargable `build_membership_predicate` fast path is unreachable
   code in production.
2. Every key column gets `IS NOT DISTINCT FROM` — **0 of 4 sargable** for
   `omc.sop_forecast_view`, not 2 of 4. The worst available form.
3. `reflex_probe_not_null_columns` is documented as the fix for precisely this symptom
   ("perf bugs (false `IS NOT DISTINCT FROM` when NULLs don't exist → composite-index defeat,
   the 405 s yse regression)") but silently returns success having done nothing. This is the
   "don't print a remedy that can't clear its own finding" anti-pattern from `CLAUDE.md`.
4. Adding `NOT NULL` constraints to the source columns does not help either, because the
   registry is never re-derived for a passthrough.

## Blast radius (measured on db_prod)

```
is_passthrough | partitioned | gate_fails |  n | status
      t        |      t      |     t      | 40 | EXPOSED (ungated branch)
      t        |      f      |     t      | 55 | gated  ← identical nullable keys, fine
      f        |      t      |     f      | 10 | gated
      f        |      f      |     f      | 85 | gated
```

**40 enabled IMVs are on the ungated branch.** The 55 unpartitioned passthrough IMVs have the
same empty `not_null_columns` and are fine — the PS-5 gate rescues them at runtime. Only
partitioning selects the ungated branch.

`last_flush_ms` tracks the split:

| | n | max_ms | avg_ms | >10 s |
|---|---|---|---|---|
| exposed | 40 | 813,246 | 50,605 | 8 |
| not exposed | 84 | 391,775 | 11,723 | 5 |

Worst observed: `alp.forecast_analysis_view` 813 s, `alp.forecast_analysis_view__cte_forecast_sales`
764 s. `omc.sop_forecast_view`'s `flush_ms_history` shows the regime change directly — older
entries 8–2100 ms, recent tail `{628, 77, 29235, 44, 34623, 341, 271129}`.

## Why the gate will actually fix it here

The gate only helps if the affected set has no NULL key at runtime. Probed on `db_prod`:

| schema | NULL `product_id` | NULL `location_id` | rows |
|---|---|---|---|
| omc | 0 | 0 | 1,917,609 |
| alp | 0 | 0 | 126,099,113 |
| nvg | 0 | 0 | 138,757 |
| petrone | 0 | 0 | 1,024,113 |

Zero NULLs in 129M rows. The fast sargable variant will be taken on every flush, while
remaining correct by construction if a NULL ever appears.

## Reproduction

Any partitioned passthrough IMV whose key is not fully catalog-`NOT NULL` — which, given **B**,
is every partitioned passthrough IMV. Push a large `UPDATE` covering most of one partition key's
rows and time the `COMMIT`. `omc.sop_forecast_view`: 96 min for 930k rows / 132 MB.

## Fix direction

**A (do first — solves the incident).** In `passthrough_op_stmts`, use the gated
`passthrough_keyed_delete_match` in the partition-dispatch arms, and let
`build_passthrough_partition_dispatch_sql` take the cold DELETE as a list of variants, emitting
one `EXECUTE` per variant inside the single DO block, exactly as `dispatch.rs:254-257` does.
Delete the stale "cannot be gated" comment. Regression test must assert the emitted DO block
contains **one** swap call and **two** cold-DELETE `EXECUTE`s, and must go RED if the arm is
reverted to the ungated predicate.

**B (separate change).** Run the structural NOT-NULL inference for passthrough IMVs, at create
time and in `reflex_probe_not_null_columns`. It is `infer_not_null_columns` — the *structural*
inference that "never trusts transient null-freeness" — not the old data probe, so it is sound
for passthrough. Until then `reflex_probe_not_null_columns` should at minimum report that it
did nothing rather than reporting success. B does not subsume A: `product_id`/`location_id`
carry no constraint, so `all_not_null` stays false and the gate is still required.

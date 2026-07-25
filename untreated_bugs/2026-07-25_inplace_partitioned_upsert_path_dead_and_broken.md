# 2026-07-25 — the in-place partitioned passthrough UPDATE body has never executed, and raises spuriously if enabled

**Severity: low as shipped (dead code, no user-visible wrong result), high as a trap.**
Two prior investigations have now reasoned about this body as if it were live. It is not.

## What is dead

`resolve_inplace_non_key_cols` (`src/trigger/ops.rs`) resolves the target's non-key columns
via SPI and returns `Vec::new()` when it finds none; the caller then falls back to the
standard cold DELETE+INSERT body and never constructs an `InplaceSpec`. The SPI query is

```sql
SELECT attname FROM pg_attribute WHERE attrelid = $1::regclass AND attnum > 0 AND NOT attisdropped ORDER BY attnum
```

read back with `row.get_by_name::<String, _>("attname").unwrap_or(None)`. `pg_attribute.attname`
is of type `name`, not `text`; the `String` conversion yields `None` for every row and
`unwrap_or(None)` swallows it. The function therefore returns `[]` for **every** IMV in every
real backend — the same `name`-type silent-swallow gotcha already recorded twice in this
codebase's journals.

Consequence: `InplaceSpec`, `build_inplace_cold_list_block` and `build_inplace_cold_range_block`
(`src/trigger/dispatch.rs`) are unreachable. Every partitioned keyed passthrough UPDATE runs the
standard body. `pt_inplace_upsert_filter_and_keychange_oracle` (`src/tests/pg_test_passthrough.rs`)
passes without exercising a single line of the code it names — verified by dumping
`reflex_build_delta_sql`'s output for that exact fixture: the emitted cold block is the standard
`DELETE … / INSERT …` pair, with no `__reflex_pt_proj`.

## What is broken underneath

Changing the projection to `attname::text` makes the path live. It then fails immediately:

* `pt_inplace_upsert_filter_and_keychange_oracle` (all-NOT-NULL composite key `id,region`) →
  `ERROR: pg_reflex in-place assertion failed for "up_v": affected key set diverged`.
* Minimised: a single-source LIST-partitioned IMV keyed `id,region`, cold-forced, survives a
  pure-data update and a filter-exit update, then raises on
  `UPDATE up3_src SET region = 'A' WHERE id = 3` — a legitimate partition-key change.

Mechanism: the guard compares `__reflex_pt_proj` (the NEW image, keyed by the **new** key) against
target rows scoped by membership in `pt_old` (the **old** key). Any key change makes the two
legitimately differ, so the `UNION ALL`'s second branch is non-empty and the assertion raises.
With `reflex.assert_inplace_update = off` the maintenance itself produced correct results on the
non-key-change steps (bidirectional `EXCEPT ALL` oracle: 0 mismatches), so the defect is in the
guard's scoping, not necessarily in the delete-gone.

## What was fixed, and what this leaves

The NULL-blind `(key) IN (SELECT … FROM pt_old)` membership in both in-place blocks — the third
instance of the 2026-07-25 nullable-explicit-key defect, after `passthrough_keyed_delete_predicate`
and `build_null_safe_membership_predicate`'s LEFT/RIGHT-JOIN caller — has been gated on
`plan.not_null_columns` (`inplace_pt_old_membership` in `src/trigger/dispatch.rs`), so a revival
does not inherit it. Output is byte-identical for a fully NOT NULL key. Pinned by
`pg_part_inplace_membership_is_null_safe_when_key_nullable`
(`src/tests/pg_test_partition_dispatch.rs`) at the codegen seam, because no end-to-end test can
reach the body.

Deliberately **not** done here: flipping `attname` to `attname::text`. On its own it converts a
working standard path into one that raises on key changes — strictly worse. Enabling this
optimization is its own piece of work: fix the assert's scoping (compare like-for-like key
images), decide whether the RANGE block's

```
DELETE FROM {qv} __t WHERE … USING _hot_child_names;
```

is even valid — it is emitted as a bare statement inside the `DO` block, where plpgsql accepts
`USING` only on `EXECUTE`, and `$1`/`$2` have no binding — and then benchmark it against the
standard body it would replace. If that work is not wanted, the honest alternative is deleting
`InplaceSpec` and both block builders outright; they are ~120 lines of dead code that have twice
misled analysis.

## Reproduction of the dead-ness

`SELECT public.reflex_build_delta_sql('<imv>','<src>','UPDATE', base_query, end_query,
aggregations::text, base_query) FROM public.__reflex_ivm_reference WHERE name='<imv>'`
on any LIST-partitioned keyed passthrough IMV: the cold block contains no `__reflex_pt_proj`.

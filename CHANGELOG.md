# Changelog

## [1.8.1] - 2026-06-02

Multi-level (sub-partition) source support: an IMV whose source is partitioned
more than one level deep (e.g. `LIST (dem_plan_id) → RANGE (order_date)`) now
mirrors the **entire** source partition hierarchy and can be reconciled at any
level. Partition `DETACH`/`ATTACH` swaps — which fire no DML trigger — are
captured by the DDL event trigger and applied by a new flush.

Run `ALTER EXTENSION pg_reflex UPDATE TO '1.8.1';` and replace the `.so`. The
migration adds the two capture catalog tables, the flush functions, the
`source_partition` argument on `reflex_reconcile_partition`, the enqueue branch
on the `ddl_command_end` event trigger, and **seeds the partition snapshot for
existing partitioned IMVs** so the first post-upgrade swap is incremental.

### Added

- **Full-hierarchy partition mirroring.** `create_reflex_ivm(..., partition_by)`
  and `reflex_sync_partitions` now walk the source's whole partition tree
  recursively and build a matching multi-level IMV tree (internal nodes carry a
  sub-`PARTITION BY`). All partition-key columns at every level must be bare
  projected columns in the IMV's unique key / GROUP BY.
- **`reflex_reconcile_partition(view, partition_keys, source_partition DEFAULT '')`.**
  The new third argument reconciles a named source partition at any level by
  expanding it to its leaves and atomic-swapping each. The legacy 2-arg form is
  unchanged and now also correct on sub-partitioned sources.
- **`reflex_flush_partitions()` / `reflex_flush_partition_source(root)`.** Apply
  pending source partition swaps. The `ddl_command_end` event trigger enqueues
  the affected source root (resolved via `pg_partition_root`, pg_reflex-owned
  tables excluded) into `__reflex_partition_pending`; the flush oid-diffs the
  live leaf set against `__reflex_source_partition_snapshot` to classify each
  change as attach (new) / swap (oid changed) / detach (dropped) and reconciles
  or drops the matching IMV leaf. New catalog tables
  `__reflex_source_partition_snapshot` and `__reflex_partition_pending`.
- **Audit drift-check.** `reflex_audit(view)` now flags any divergence between a
  partitioned source's recursive leaf set and the IMV's mirrored leaves — a
  correctness backstop for a forgotten flush or an uncaptured write vector.

### Notes

- No triggers are placed on sub-partitions: swaps are DDL (captured by the event
  trigger + flush) and root-routed DML is covered by the existing root trigger,
  so newly-attached sub-partitions need no trigger management.
- Known limitation: `detach → modify the same table in place → re-attach the
  same table` (unchanged oid) is not auto-detected; attach a freshly-built table
  (the supported pattern) or call `reflex_reconcile_partition(view, '', leaf)`
  explicitly. The audit drift-check surfaces it either way.

---

## [1.7.6] - 2026-06-01

Correctness release: **`ignore_sources` is now honored on the DEFERRED trigger
path**, closing a gap where it only worked for IMMEDIATE IMVs. Run
`ALTER EXTENSION pg_reflex UPDATE TO '1.7.6';` and replace the `.so`. The
migration rebuilds existing source triggers so the fix takes effect without
re-creating IMVs.

---

### Fixed

- **`ignore_sources` was silently ignored on the DEFERRED path.** The guard
  that skips an IMV when DML hits a source it listed in `ignore_sources` existed
  only in the IMMEDIATE trigger body. The three deferred trigger bodies
  (INSERT/DELETE, UPDATE, TRUNCATE) and the commit-time `reflex_flush_deferred`
  never consulted `ignored_sources`. So whenever a source's trigger was the
  *deferred flavour* (installed because some sibling IMV on that source is
  DEFERRED), an IMV that had ignored that source was maintained anyway — both
  inline (for IMMEDIATE IMVs processed within the deferred trigger) and at flush
  (for DEFERRED IMVs). The deferred bodies now emit the same `ignored_sources`
  skip guard as the immediate body (via a new `__REFLEX_SLOT_BARE_SOURCE__`
  slot), and `reflex_flush_deferred` excludes IMVs whose `ignored_sources`
  overlaps the (qualified, bare) source name. No catalog schema change; the
  migration rebuilds trigger bodies via `reflex_rebuild_triggers`.

### Testing

- `pg_test_deferred.rs`: `pg_test_deferred_ignore_sources_skips_imv` — a
  non-ignoring DEFERRED sibling installs the deferred trigger; an ignoring
  DEFERRED IMV (flush path) and an ignoring IMMEDIATE IMV (inline path) must
  both stay stale after the source mutates.
- Full suite: 1120 tests pass; `cargo clippy` and `cargo fmt` clean.

## [1.7.5] - 2026-05-31

Feature release: **widened CTE/JOIN passthrough unique-key inference**, so
chained-CTE cascades (e.g. the `forecast_analysis_view` shape) auto-resolve
sound unique keys and get incremental DELETE/UPDATE instead of full refresh. Run
`ALTER EXTENSION pg_reflex UPDATE TO '1.7.5';` and replace the `.so`. One
additive catalog column (`max_one_row`), no data backfill.

---

### Added

- **Sound unique-key inference across JOINs and chained CTEs.** Equi-join
  equivalence in projected-key matching (a key projected through
  `f.k = dl.k` is recognized on either side), aggregate-IMV GROUP BY keys are
  registered as sound unique keys, CROSS JOIN to an ungrouped aggregate is
  classified to-one, and the anchor probe now detects `__reflex_uk_*` indexes.
  A new `__reflex_ivm_reference.max_one_row` flag (default FALSE) records when a
  sub-IMV yields at most one row. Existing IMVs keep their stored keys;
  inference re-runs at create time.

### Fixed

- Dropped an unsound `LIKE` wildcard in registry lookups.

### Testing

- `pg_test_cte.rs`: forecast-shape unique-key cascade integration test, plus
  cross-join and chained-CTE coverage.

## [1.7.4] - 2026-05-31

Correctness release for **partitioned IMV creation**. The fix is entirely in
the compiled extension — no catalog schema change and no SQL function signature
change — so the migration only bumps the installed version. Run
`ALTER EXTENSION pg_reflex UPDATE TO '1.7.4';` and replace the `.so`.

---

### Fixed

- **Partition-anchor resolution now accepts sources co-partitioned on the join
  key, and ignores sources partitioned on a *different* column.** This extends
  the 1.7.3 anchor fix along two axes:
  - A candidate anchor must be partitioned **on the partition column itself**,
    not merely partitioned on something. The looser "partitioned at all" check
    is replaced by the new `source_partitioned_on(source, col)` helper, so a
    source partitioned on an unrelated column is no longer treated as a
    candidate.
  - When several sources are **co-partitioned on the same column** (a JOIN whose
    key *is* the partition column), their partition layouts align, so any of
    them is a sound anchor for the child DDL — this is no longer reported as
    `multiple sources own partition column '<col>' — ambiguous`. This covers the
    case where *every* owner is a reflex intermediate and there is no base
    table at all — the `forecast_analysis_view` shape, where
    `…__cte_forecast_sales FULL JOIN …__cte_history_sales ON dem_plan_id`
    produces two partitioned `__cte_` owners and zero base owners. Base owners
    are still preferred when present; otherwise the anchor is chosen
    deterministically (lexicographically) for stability across rebuilds, and
    non-anchor co-owners (which own the column natively) fall through to Path B.
    The error now fires only when **no** source is partitioned on the column.

### Testing

- `pg_test_partition.rs`: `pg_part_copartitioned_full_join_of_cte_intermediates`
  (two partitioned `__cte_` owners, zero base — the `forecast_analysis_view`
  branch) plus the co-partitioned base-table cases.
- Full suite: 1111 tests pass; `cargo clippy` and `cargo fmt` clean.

## [1.7.3] - 2026-05-31

Correctness release for IMV **creation**. Both fixes are entirely in the
compiled extension — no catalog schema change and no SQL function signature
change — so the migration only bumps the installed version. Run
`ALTER EXTENSION pg_reflex UPDATE TO '1.7.3';` and replace the `.so`.

---

### Fixed

- **Failed creation of a decomposed IMV no longer orphans its sub-IMVs.**
  Creation rejections are returned as `"ERROR…"` strings, so the function
  returns normally and the surrounding transaction is *not* aborted. A query
  that decomposes into several sub-IMVs (a CTE `WITH` chain, or a `UNION ALL`
  set-op) materialises them one at a time; when a *later* operand/CTE — or the
  final outer body — was soft-rejected, the sub-IMVs already created were
  committed and left behind, polluting the IMV space. Every soft-reject path in
  `try_decompose_ctes` (reserved-prefix conflict, a sub-IMV create failing,
  non-SELECT body, body create failing) and `try_decompose_set_op` (an operand
  create failing, the non-`ALL` "cannot be intermediate" rejection) now rolls
  back the sub-IMVs it had already created — `cascade`, in reverse creation
  order, so nested descendants go too. Hard failures (a raised PostgreSQL error)
  were already rolled back by the transaction abort and are unaffected.

- **Partition-anchor resolution now prefers the base source over derived
  intermediates.** A decomposed query can produce two *partitioned* owners of
  the partition column: a base partitioned table AND a partition-inheriting
  reflex sub-IMV (e.g. a CTE that joins `sop_forecast_view` to a
  `…__cte_date_limits` sub-IMV that inherited partitioning). `resolve_anchor_source`
  treated that as `multiple sources own partition column '<col>' — ambiguous`
  and blocked the whole IMV. It now prefers the sole *base* (non
  `__cte_`/`__union_`/`__base`) partitioned owner — the table whose partition
  children are physically mirrored — falling back to the sole partitioned owner,
  and erroring only when the choice is still genuinely ambiguous. All four
  anchor call sites benefit.

### Testing

- `pg_test_error.rs`: `test_cte_decomposition_failure_rolls_back_sub_imvs`,
  `test_set_op_decomposition_failure_rolls_back_sub_imvs`.
- `pg_test_partition.rs`: `pg_part_anchor_prefers_base_over_cte_intermediate`.
- Full suite: 1108 tests pass; `cargo clippy` and `cargo fmt` clean.

## [1.7.2] - 2026-05-31

Correctness release fixing `drop_reflex_ivm`, which silently orphaned the
target + auxiliary tables of any IMV created with a bare (unqualified) name
under a non-`public` `search_path`. Run
`ALTER EXTENSION pg_reflex UPDATE TO '1.7.2';` — the migration adds one
nullable catalog column (`__reflex_ivm_reference.target_schema`) so teardown
is independent of the session `search_path`. No data backfill; pre-existing
rows keep NULL and use the legacy `search_path` fallback.

---

### Fixed

- **`drop_reflex_ivm` silently orphaned the target + aux tables of any IMV
  created with a bare name under a non-`public` `search_path`.** All
  teardown DDL derived its relation names from the stored (bare) `name` and
  resolved the target via `to_regclass(name)`, both honouring the session
  `search_path` *at drop time*. An IMV created while `search_path = alp`
  landed its objects in `alp`, but a later `drop_reflex_ivm` run under a
  different `search_path` issued unqualified `DROP TABLE IF EXISTS …` that
  resolved against the wrong schema, skipped every real object, deleted only
  the catalog row, and left the table + `__reflex_intermediate_*` /
  `__reflex_affected_*` / `__reflex_uk_*` artifacts behind. A same-named
  decoy relation of a different kind in the `search_path` (e.g. a
  materialized view) could also be hit instead, surfacing as
  `ERROR: "<name>" is not a table`. Creation now records the object schema
  in `target_schema` (`current_schema()` for bare names), and
  `drop_reflex_ivm` re-qualifies all teardown DDL with it. Legacy rows with
  a NULL `target_schema` fall back to the prior `search_path` behaviour.

## [1.7.1] - 2026-05-31

Correctness release fixing Path C — the INSERT_PROMOTED smart bulk-INSERT
dispatch fired only by UPDATE triggers. Two compounding defects: derived
relation names were re-built by raw `split_part` + string concat that
bypassed the canonical `safe_identifier` hash, and the bulk-INSERT entry
gate did not match the Rust-side `aggregate_insert_stmts` safety check.
Run `ALTER EXTENSION pg_reflex UPDATE TO '1.7.1';` — the migration script
registers three new SQL-callable name helpers **and** automatically calls
`reflex_rebuild_triggers` for every distinct source in
`__reflex_ivm_reference.depends_on`, so existing IMVs pick up the new
trigger body without operator intervention. No catalog schema changes.

---

### Fixed

- **`ERROR: relation "<…>" does not exist` during UPDATE, surfaced as a
  `WARNING: pg_reflex Path C smart bulk-INSERT failed for <imv>` log
  line followed by silent fallback to MERGE.** The Path C plpgsql block
  derived the intermediate / scratch / target relation names by parsing
  the IMV name with `split_part(name, '.', 1|2)` and concatenating
  `'"<schema>"."__reflex_intermediate_<view>"'` by hand. Two manifestations:

    * **Bare-name IMVs** (default-schema, no `schema.` prefix): the second
      `split_part` returned the empty string and the constructed
      `"foo"."__reflex_intermediate_"` was not a real relation. The outer
      EXCEPTION caught it and Path C was silently disabled for every
      default-schema IMV.

    * **Long IMV names:** when `__reflex_intermediate_<bare>` crossed PG's
      63-char NAMEDATALEN, the real relation got the 8-hex `safe()` hash
      suffix while the Path C concat did not, so the constructed name
      pointed at a relation that did not exist. The outer EXCEPTION
      degraded Path C to a fall-through, but the WARNING surfaced.

  Three new SQL-callable wrappers — `reflex_intermediate_table_name`,
  `reflex_delta_scratch_table_name`, `reflex_quote_identifier` — expose
  the same Rust helpers every other call site uses (`split_qualified_name`
  + `safe_identifier`). The Path C body calls them instead of
  `split_part` + concat. The unique-index lookup is rewritten to join
  via `to_regclass(...)::regclass` + `pg_index.indisunique` (the previous
  `pg_indexes.indexdef ILIKE '%UNIQUE%'` form false-positived on
  comments / column names).

- **Silent double-counting in Path C for single-source aggregates.** The
  bulk-INSERT path skips MERGE on the assumption that the affected slice
  of intermediate group keys cannot already be populated. That holds
  only when the source's identity uniquely determines its slice of keys
  — i.e., when the analyser captured a `source_join_keys` entry for the
  trigger source. For single-source aggregates one row can feed many
  group keys, and other rows (filter-passed before this UPDATE) may
  already be contributing; bulk-INSERT then duplicated the affected
  groups in the intermediate / target. Pre-1.7.1 the concat bug masked
  this for bare-name IMVs (Path C errored out before any DML), but
  schema-qualified single-source aggregates with the right shape hit
  the silent duplicate-rows path. `reflex_build_path_c_explain_sql`
  now returns the empty string when the plan has no `source_join_keys`
  entry for the trigger source — matching the Rust-side
  `aggregate_insert_stmts` gate — and the trigger body falls through
  to the standard MERGE path.

### Testing

- Two regression locks added in `src/tests/unit_trigger.rs`:
  `test_path_c_block_does_not_split_part_imv_name` and
  `test_path_c_block_does_not_concat_raw_reflex_names`. Both fail
  against the 1.7.0 Path C template and pass with the 1.7.1 body.
- Full suite: **1104 tests pass** (was 1102 in 1.7.0, +2 from above).

### Migration

`ALTER EXTENSION pg_reflex UPDATE TO '1.7.1';` runs
[`sql/pg_reflex--1.7.0--1.7.1.sql`](https://github.com/diviyank/pg_reflex/blob/main/sql/pg_reflex--1.7.0--1.7.1.sql)
which registers the three new SQL-callable name-helper functions. No
data is migrated; no IMV needs to be dropped or recreated.

After the `ALTER EXTENSION` completes, run the rebuild loop below in a
normal SQL session (outside the `creating_extension` flag PG sets for
ALTER EXTENSION) to pick up the new Path C body on every existing
trigger. The per-source trigger functions (`__reflex_*_trigger_on_*`)
were created from `create_reflex_ivm` outside any extension-creation
context, so they belong to the database — PG's
`creating_extension`-mode safety check refuses to `CREATE OR REPLACE`
them mid-upgrade with `"function … is not a member of extension
\"pg_reflex\""`. Running the loop separately sidesteps the check:

```sql
DO $$
DECLARE _src TEXT; _msg TEXT; _ok INT := 0; _err INT := 0;
BEGIN
  FOR _src IN
    SELECT DISTINCT s
    FROM public.__reflex_ivm_reference, unnest(depends_on) AS s
    WHERE enabled = TRUE AND s NOT LIKE '<%'
    ORDER BY 1
  LOOP
    BEGIN
      _msg := public.reflex_rebuild_triggers(_src);
      IF _msg LIKE 'ERROR:%' THEN
        RAISE NOTICE 'skipped %: %', _src, _msg;
        _err := _err + 1;
      ELSE
        _ok := _ok + 1;
      END IF;
    EXCEPTION WHEN OTHERS THEN
      RAISE NOTICE 'skipped %: %', _src, SQLERRM;
      _err := _err + 1;
    END;
  END LOOP;
  RAISE NOTICE 'rebuilt % source(s); % skipped', _ok, _err;
END $$;
```

View / matview sources (PG raises `relation … cannot have triggers`)
are expected to be skipped — pg_reflex doesn't put triggers on views,
those sources are maintained via cascade from upstream IMVs. Bare
`depends_on` entries that resolve to multiple schemas raise
`source name 'X' is ambiguous` — qualify those rows in
`public.__reflex_ivm_reference.depends_on` and re-run the loop. The
unreached source keeps its 1.7.0 trigger body (still correct — Path C
just falls through to MERGE for the affected IMVs); no IMV is wrong as
a result of skipping a source.

## [1.7.0] - 2026-05-28

Refactor + correctness release for intermediate `UNION ALL` CTE-body wrappers.
The inline wrapper-construction code in `try_decompose_set_op` is centralised
into one helper; the wrapper table gains a `__reflex_src_idx` discriminator
column that fixes a cross-operand `DELETE` over-delete; non-`ALL` set ops
used as CTE bodies are now rejected at create time with an actionable error;
and `drop_reflex_ivm` cascade no longer leaks `__reflex_union_mirror_*` trigger
functions in `pg_proc`. No catalog schema changes, no trigger body changes,
no API changes. Run `ALTER EXTENSION pg_reflex UPDATE TO '1.7.0';` to register
the new version; the migration file is a no-op marker. **Existing UNION-ALL
CTE IMVs created under ≤1.6.5 must be dropped and recreated** to pick up the
cross-operand `DELETE` fix (see Migration).

---

### Fixed

- **Cross-operand `DELETE` over-delete in intermediate `UNION ALL` CTE
  wrappers.**  A `DELETE` from operand A would over-delete a wrapper row
  contributed by operand B when both operands projected the same column
  values for the deleted row.  The per-operand mirror trigger matched by
  all-column `IS NOT DISTINCT FROM` with no operand-identity filter, so it
  also removed B's row.  The wrapper table now carries a leading
  `__reflex_src_idx SMALLINT NOT NULL` discriminator column populated with
  the operand index; the mirror `DELETE` predicate now scopes to
  `__reflex_src_idx = <operand_idx> AND <cols> IS NOT DISTINCT FROM <old>`.
  *Create-time fix — recreate any UNION-ALL CTE IMV built under ≤1.6.5
  (see Migration).*
- **`__reflex_union_mirror_*` trigger functions orphaned in `pg_proc`
  after `drop_reflex_ivm` cascade.**  Operand-sub-IMV cascade dropped the
  mirror triggers themselves but not their plpgsql functions, leaving
  three orphans per operand in `public` until a same-named overwrite.
  `drop_reflex_ivm_impl_inner` now detects UNION-ALL wrappers by the
  `__union_<i>` suffix on `depends_on_imv` entries and issues
  `DROP FUNCTION IF EXISTS … CASCADE` per operand index per op.
  *Drop-time fix — applies to any UNION-ALL IMV dropped under 1.7.0,
  regardless of when it was created.*

### Changed

- **`UNION` / `INTERSECT` / `EXCEPT` (without `ALL`) used as a CTE body
  consumed by an outer IMV are now rejected at create time** with an
  actionable error pointing to three workarounds: hoist to the outermost
  SELECT (stays a VIEW), use `kind: mv`, or rewrite as `UNION ALL` if
  operands are guaranteed disjoint.  Previously these shapes silently
  emitted a VIEW that failed deep in the consumer's trigger install with
  `Triggers on views cannot have transition tables`.  Outer-level (top of
  SELECT) `UNION` / `INTERSECT` / `EXCEPT` continue to work as VIEW
  wrappers; only the intermediate-position (CTE body) case is rejected.
  *Create-time validation — strictly better error UX, no shape that
  worked before stops working.*
- **Intermediate `UNION ALL` wrapper construction is centralised** into a
  new private helper `install_union_all_intermediate_wrapper` in
  `src/create_ivm.rs`.  `try_decompose_set_op` no longer carries inline
  `CREATE UNLOGGED TABLE` + per-operand trigger-install + registry-insert
  code.  The dropped helper `query_table_column_names` was its only
  remaining caller and is removed.  No user-visible change beyond what
  Fixed and Changed entries describe.

### Testing

- **Six regression tests added.**  In `src/tests/pg_test_drop.rs`: wrapper
  table carries `__reflex_src_idx SMALLINT NOT NULL` (column-presence
  check via `pg_attribute`); cross-operand DELETE isolation (operand A
  delete preserves operand B's same-valued row); `__reflex_union_mirror_*`
  functions in `pg_proc` are zero after `drop_reflex_ivm(…, TRUE)`.  In
  `src/tests/pg_test_error.rs`: explicit-reject tests for `UNION`,
  `INTERSECT`, and `EXCEPT` (no `ALL`) used as CTE body.  Full suite:
  **1102 tests pass**.

### Migration

- `ALTER EXTENSION pg_reflex UPDATE TO '1.7.0';` runs
  [`sql/pg_reflex--1.6.5--1.7.0.sql`](https://github.com/diviyank/pg_reflex/blob/main/sql/pg_reflex--1.6.5--1.7.0.sql),
  a **no-op marker** — there are no catalog or function-body changes.
- **UNION-ALL CTE-body IMVs created under ≤1.6.5** were built without
  `__reflex_src_idx` on their wrapper table, and the corresponding mirror
  trigger function bodies do not reference it.  Cross-operand `DELETE`
  over-delete remains until the IMV is recreated.  Recipe:

  ```sql
  SELECT drop_reflex_ivm('<top-level-imv>', TRUE);
  SELECT create_reflex_ivm('<top-level-imv>', '<SELECT …>', …);
  ```

  The cascade drop in ≤1.6.5 also leaks `__reflex_union_mirror_*`
  functions in `pg_proc`; under 1.7.0 the cascade cleans them.  Operators
  who upgraded a database with pre-1.7.0 wrappers in place can clear the
  pre-existing orphans manually after the upgrade:

  ```sql
  DO $do$ DECLARE r RECORD;
  BEGIN
    FOR r IN
      SELECT 'public.' || p.proname || '()' AS sig
      FROM pg_proc p
      JOIN pg_namespace n ON n.oid = p.pronamespace
      WHERE n.nspname = 'public'
        AND p.proname LIKE '__reflex_union_mirror_%'
    LOOP
      EXECUTE 'DROP FUNCTION IF EXISTS ' || r.sig || ' CASCADE';
    END LOOP;
  END $do$;
  ```

- **Non-UNION-ALL IMVs** are unaffected.
- **Outer-level (top of SELECT) `UNION ALL` IMVs** continue to be
  maintained as zero-overhead VIEW wrappers over operand sub-IMVs
  (unchanged).

## [1.6.5] - 2026-05-26

Correctness release fixing three independent create-time defects hit while
migrating real views (CTE-decomposed, `DEFERRED`, materialized-view-sourced) to
IMVs.  No catalog schema changes, no trigger body changes, no API changes.  Run
`ALTER EXTENSION pg_reflex UPDATE TO '1.6.5';` to register the new version; the
migration file is a no-op marker.  All three fixes are create-time: each
previously caused `create_reflex_ivm` to error outright or bake the wrong value
into the new IMV, so no IMV that was already created successfully is affected.

---

### Fixed

- **`DEFERRED` IMV over a CTE failed at creation with `zero-length delimited
  identifier at or near ""`.**  A CTE-decomposed sub-IMV is referenced by the
  rewritten outer query in already-quoted form (`"schema"."view__cte_x"`, needed
  to preserve identifier case).  In `DEFERRED` mode the staging-delta table-name
  builder re-quoted the already-quoted schema, emitting `""schema""` — which
  PostgreSQL rejects.  The schema component is now unquoted before being
  re-quoted.  `IMMEDIATE` mode was unaffected (its trigger names strip quotes).
  *Create-time fix — unblocks a shape that could not be created before.*
- **Explicit `unique_columns` were silently dropped for any query containing
  CTEs.**  The CTE-decomposition path did not thread the caller's
  `unique_columns` into the outer passthrough IMV (the set-op and `DISTINCT ON`
  paths already did), so a JOIN passthrough over CTEs reported "no unique key"
  and fell back to **full refresh** on `DELETE`/`UPDATE` even when a key was
  supplied.  The key now reaches the outer IMV's stored metadata.  *Create-time
  fix — recreate CTE IMVs built under ≤1.6.4 to gain incremental `DELETE`/
  `UPDATE` (see Migration).*
- **`MIN`/`MAX` over a materialized-view column failed at creation with `column
  "…" is of type numeric but expression is of type timestamp with time zone`**
  (or any non-numeric type).  Source column types were collected from
  `information_schema.columns`, which **omits materialized views**, so the
  column type was never found and the `MIN`/`MAX` intermediate column defaulted
  to `NUMERIC`.  Types are now read from `pg_catalog`, which covers every
  relkind.  This completes the 1.6.3 `MIN`/`MAX` type-resolution fix, which
  handled table-qualified columns but not matview-sourced ones.  *Create-time
  fix — unblocks a shape that could not be created before.*

### Testing

- Five regression tests added: two unit tests for the staging-delta name builder
  (quoted vs bare source), and `pg_test`s for deferred-CTE passthrough creation +
  flush, for threading explicit `unique_columns` through CTE decomposition, and
  for `MIN`/`MAX` over a `TIMESTAMPTZ` materialized-view column.  Full suite:
  1083 tests pass.

### Migration

No DDL is required.  Fixes 1 and 3 unblock creation of shapes that could not be
created before, so they have no existing-IMV impact.  Fix 2 is baked into stored
metadata at create time: an IMV built from a CTE query under ≤1.6.4 keeps its
empty unique key (full refresh on `DELETE`/`UPDATE`).  To pick up incremental
`DELETE`/`UPDATE`, drop and recreate it with an explicit key:

```sql
SELECT drop_reflex_ivm('<name>');
SELECT create_reflex_ivm('<name>', '<SELECT …>', '<key cols>');
```

## [1.6.4] - 2026-05-24

Correctness release hardened by a new differential fuzz harness.  No catalog
schema changes, no trigger body changes, no API changes.  Run `ALTER EXTENSION
pg_reflex UPDATE TO '1.6.4';` to register the new version; the migration file is
a no-op marker.  The runtime fixes below reach existing IMVs automatically once
the new module is loaded; the create-time fixes only affect newly-created IMVs.

---

### Fixed

- **LEFT / RIGHT JOIN secondary-side maintenance dropped or duplicated rows.**
  Inserting, updating, or deleting a row on the **secondary** side of an outer
  join could drop or duplicate the joined rows, and a primary row that gained or
  lost its match could be deleted outright instead of reverting to a NULL-filled
  row.  Secondary-side `INSERT` routing, affected-group scoping (by stable key),
  and quoted-source detection are corrected inside `reflex_build_delta_sql`.
  *Runtime fix — applies to existing IMVs on recompile.*
- **DEFERRED-mode duplicate-key flush.**  A deferred batch that `INSERT`ed a new
  key and then `UPDATE`d that **same** key before flush emitted both the
  new-side and the old-side delta for the key, so `reflex_flush_deferred` failed
  with `duplicate key value violates unique constraint`.  The two delta sides
  are now netted per unique key before the `MERGE`.  *Runtime fix.*
- **Silent row loss from unsound NOT-NULL inference.**  The former runtime
  data-probe marked a column `NOT NULL` whenever the **create-time** data
  happened to be NULL-free — using transient data as a proxy for a query
  guarantee.  Maintenance then matched that key with `=` instead of
  `IS NOT DISTINCT FROM` and **silently dropped rows** when a NULL appeared
  later: an unmatched primary-side `LEFT JOIN` insert, or a `GROUP BY` key that
  became NULL.  `NOT NULL` is now promoted only when the query **structurally**
  guarantees it (an INNER-join equi-key, or a catalog-`NOT NULL` base column on a
  non-nullable join side); quoted / qualified column references are rejected from
  the inference.  *Create-time fix — recreate existing aggregate IMVs to clear a
  stale over-promotion (see Migration).*
- **Filtered-IMV maintenance emitted invalid SQL from a qualified WHERE.**  A
  query-level `WHERE` carried into maintenance kept its table-qualified column
  references and failed against the transition table; the predicate is now
  alias-stripped.  *Runtime fix.*
- **Long generated column identifiers exceeded the 63-byte limit.**  A carried
  expression with a long derived name (e.g. a `EXISTS` projection) produced an
  identifier over Postgres's `NAMEDATALEN` and failed at creation; generated
  identifiers are now truncated to 63 bytes on a char boundary.  *Create-time
  fix.*

### Changed

- **An aggregate IMV whose `GROUP BY` key is not projected bare in the `SELECT`
  is now rejected up front** with a clear error, instead of failing later in
  codegen with a confusing message.  *Create-time validation.*

### Testing

- **Differential fuzz harness** (`src/tests/pg_test_fuzz.rs`, proptest under the
  `pg_test` feature).  For each generated query it builds a real
  `MATERIALIZED VIEW` and a pg_reflex IMV, applies the same DML, and asserts the
  two agree row-for-row (exact for non-float columns; NULL-safe relative epsilon
  for float columns).  Covers single-table aggregates, 2-source `LEFT JOIN`
  aggregates, carried scalars, CTE decomposition, and basic `WHERE` filters in
  both `IMMEDIATE` and `DEFERRED` modes.  The three NOT-NULL / deferred fixes
  above were found by this harness and are frozen as regression tests.  See
  [`docs/contributing/testing.md`](docs/contributing/testing.md) and
  [`docs/fuzz-findings.md`](docs/fuzz-findings.md).

### Migration

- `ALTER EXTENSION pg_reflex UPDATE TO '1.6.4';` runs
  [`sql/pg_reflex--1.6.3--1.6.4.sql`](https://github.com/diviyank/pg_reflex/blob/main/sql/pg_reflex--1.6.3--1.6.4.sql),
  a **no-op marker** — all fixes are in the recompiled module.  The runtime
  fixes (JOIN secondary-side, deferred netting, filtered WHERE) reach every
  existing IMV at its next trigger fire.  The create-time NOT-NULL fix does
  **not** reach an aggregate IMV created under an earlier version — its
  over-promotion is baked into the stored `aggregations.not_null_columns` and the
  intermediate-table schema, and neither the migration nor
  `reflex_rebuild_triggers` can undo it.  **Drop and recreate any aggregate IMV
  created before 1.6.4** to clear a latent over-promotion:
  `SELECT drop_reflex_ivm('<name>'); SELECT create_reflex_ivm('<name>', '<SELECT …>', …);`

## [1.6.3] - 2026-05-20

Correctness release for CTE / window-function decomposition and MIN/MAX type
resolution.  No catalog schema changes, no trigger body changes, no API
changes — existing IMVs operate without intervention.  Run `ALTER EXTENSION
pg_reflex UPDATE TO '1.6.3';` to register the new version; the migration file
is a no-op marker.

---

### Fixed

- **Window function over CTEs dropped sibling CTEs / crashed the backend.**
  A query with a window function and `WITH` CTEs hit the window-decomposition
  path on the query-wide "has a window anywhere" flag.  Two failure modes:
  - A window in the **top-level SELECT** over CTEs (e.g. `WITH a AS (…),
    b AS (…) SELECT a.x, b.y, ROW_NUMBER() OVER (…) FROM a JOIN b …`) built a
    `__base` sub-IMV that omitted the `WITH` list, so it referenced a CTE that
    no longer existed and failed with `relation "<sibling_cte>" does not exist`.
  - A window nested in a derived-table subquery (e.g. the classic
    `… FROM (SELECT …, ROW_NUMBER() OVER (…) AS rn FROM t) s WHERE s.rn = 1`,
    including when wrapped in a CTE) had no top-level window to split off, so
    decomposition re-fed an identical base query into the pipeline and recursed
    until the backend **crashed (SIGSEGV)**.
  - Fixes: CTE decomposition now runs **before** distinct-on / window
    decomposition (so sibling CTEs are preserved and the top-level-window-over-
    CTEs case works); and window decomposition is gated on an actual
    **top-level-SELECT** window — a window that exists only in a subquery /
    derived table now returns a clean error instead of recursing.
- **`MAX` / `MIN` over a table-qualified non-numeric column failed at
  creation** with `column "…" is of type numeric but expression is of type
  timestamp with time zone` (also for `date` / `text`).  The intermediate
  column resolved its type correctly from the aggregate's `source_arg`
  (`e.ts` → `timestamptz`), but the **target** table column type was resolved
  by stripping the `__max_`/`__min_` prefix off the *sanitized* column name
  (`__max_e_ts` → `e_ts`, no qualifier), which could not be resolved and
  defaulted to `NUMERIC` — so the two tables disagreed.  The target column
  type is now derived from the matching intermediate column's source argument,
  guaranteeing it equals the intermediate column type.  Bare args
  (`MAX(ts)`) were unaffected; only qualified args (`MAX(e.ts)`) over a
  non-numeric column triggered the mismatch.

### Changed

- **Window functions / `DISTINCT ON` inside a CTE referenced by an outer query
  are now rejected up front** with an actionable error instead of failing
  obscurely or crashing.  Such a CTE decomposes into a read-time VIEW (windows
  and `DISTINCT ON` cannot be incrementally maintained), and a parent IMV
  cannot install row-level triggers with transition tables on a VIEW.  The
  error directs the operator to move the window / `DISTINCT ON` to the
  outermost SELECT, or define the view with `kind: mv`.
- **Partitioning propagates to CTE sub-IMVs.**  When a partitioned IMV is built
  from a `WITH … SELECT …` query, each CTE sub-IMV now inherits the parent's
  `partition_by` columns that appear in that CTE's output projection.  The
  parent view remains partitioned as before.

### Migration

- `ALTER EXTENSION pg_reflex UPDATE TO '1.6.3';` runs
  [`sql/pg_reflex--1.6.2--1.6.3.sql`](https://github.com/diviyank/pg_reflex/blob/main/sql/pg_reflex--1.6.2--1.6.3.sql),
  a **no-op marker** — all changes are in the recompiled module
  (decomposition / type-resolution / DDL codegen), with no catalog, trigger,
  or API changes.  Views previously kept as `kind: mv` because they nest a
  window / `DISTINCT ON` inside a CTE referenced by an outer query stay
  `kind: mv`; that shape is still not an IMV, but now fails fast with guidance.

## [1.6.2] - 2026-05-19

Patch release fixing a catastrophic deferred-trigger failure on sources
whose `__reflex_delta_<src>` staging table outlived a source DDL change
(IMV drop+recreate, source DROP/CREATE — the latter unavoidable on PG ≤ 17
when adding partitioning to an existing table).  Run `ALTER EXTENSION
pg_reflex UPDATE TO '1.6.2';` to apply the staging-shape repair and pick
up the new trigger codegen.

---

### Fixed

- **Deferred trigger fails after the source's column order drifts** —
  e.g. `column "creation_date" is of type timestamp with time zone but
  expression is of type integer` on any INSERT/UPDATE/DELETE that fires
  the deferred trigger.  Root cause: the deferred trigger body did
  `INSERT INTO __reflex_delta_<src> SELECT '<op>', * FROM <transition>`
  — a **positional** bind.  The per-source staging delta is created
  with `IF NOT EXISTS` and is not dropped by `drop_reflex_ivm`, so it
  outlives the IMV and the source.  When the source's column ORDER
  changed (typically because a partitioned table replaced its
  unpartitioned predecessor with a different layout), the staging's
  column positions no longer matched the transition table's, and the
  trigger died on every DML.
  - `sql/deferred_trigger_body.plpgsql.in` and
    `sql/deferred_trigger_update_body.plpgsql.in` now emit
    `INSERT INTO staging (__reflex_op, "col_a", "col_b", …) SELECT
    '<op>', "col_a", "col_b", … FROM transition`.  Column names are
    resolved at trigger DDL build time from the live source catalog.
- **`reflex_rebuild_triggers` silently broke DEFERRED IMVs.**  The
  function always emitted the immediate-mode trigger body, so calling
  it on a source that fed at least one DEFERRED IMV replaced the
  deferred body with the immediate one — staging stopped accumulating
  and `reflex_flush_deferred` had nothing to flush.  It now inspects
  `__reflex_ivm_reference.refresh_mode` and picks the correct builder.

### Added

- **Staging shape guard in `create_reflex_ivm` (DEFERRED).** Before
  installing the deferred trigger on a source, the create path
  compares the staging table's column NAMES against the source's live
  shape.  Three outcomes:
  - identical sets (any order) → reuse the staging,
  - sets differ + staging empty → drop+recreate the staging from the
    current source shape (`CASCADE` to clear the per-session TEMP
    views from any prior `reflex_flush_deferred` call),
  - sets differ + staging has pending rows → **refuse with a clear
    error** directing the operator to flush first.  Silent drops
    would lose other IMVs' staged work; silent reuse would crash the
    new named-column INSERT.
- **`reflex_audit()` — operator-callable structural audit.** Two overloads:
  `reflex_audit()` audits every enabled IMV plus orphan-artifact checks;
  `reflex_audit('<view_name>')` scopes to a single IMV and skips orphan
  checks. Returns a multi-line text report with severity-tagged findings
  (ERROR / WARNING / INFO) and a copy-pastable `Suggested fix` block per
  finding. Read-only — safe to invoke at any time, including during DML,
  and intended for monitoring-scrape use at low cadence. Catches the
  1.6.2 root-cause invariant (`staging-shape`) plus eleven others:
  trigger attachment, trigger-mode / refresh-mode agreement, internal-
  table existence, source existence, base / target shape agreement,
  base_query parses, partition-mirror drift, and orphan-intermediate /
  -staging / -scratch tables. Example:
  ```sql
  SELECT reflex_audit();
  -- pg_reflex audit: OK (12 IMV(s), 3 source(s) checked, no findings)
  ```

### Tests

- `pg_test_deferred_stale_staging_after_source_recreate` — drop+
  recreate a source with reordered columns, confirm the new trigger
  body handles INSERT/UPDATE/DELETE end-to-end against the
  reorder-stale staging.
- `pg_test_deferred_empty_stale_staging_with_column_set_drift_recreated`
  — column SET drift (added/removed column) with empty staging gets
  recreated by the guard; trigger round-trip + oracle pass.

### Migration

`ALTER EXTENSION pg_reflex UPDATE TO '1.6.2';` runs
`sql/pg_reflex--1.6.1--1.6.2.sql`, which:

1. For each source with at least one enabled DEFERRED IMV, validates
   the `__reflex_delta_<src>` staging table's column set against the
   source's current shape.  Drops+recreates the staging when they
   differ (emitting a NOTICE that names the source and the row count
   discarded).  **Pre-drift rows are dropped** — they reference an
   older column layout and cannot be replayed safely.  Operators with
   critical pending state should call `reflex_flush_deferred(...)` on
   each affected source BEFORE running the upgrade.
2. Re-emits trigger function bodies for every tracked source via the
   now-deferred-aware `reflex_rebuild_triggers`, so existing IMVs
   pick up the named-column INSERT codegen.

Sources whose generated staging name would exceed PG's 63-char
identifier limit (sanitized source suffix > 48 chars) are skipped
with a NOTICE — drop and recreate the staging manually if drift is
suspected on those.

---

## [1.6.1] - 2026-05-18

PG 18 compatibility, CI hygiene, and an internal pipeline refactor.  No
catalog schema changes, no trigger body changes, no API changes —
existing IMVs operate without intervention.  Run `ALTER EXTENSION
pg_reflex UPDATE TO '1.6.1';` to register the new version (the migration
file is a no-op marker).

---

### Fixed

- **PG 18: partitioned IMV creation rejected with "partitioned tables
  cannot be unlogged."**  PG 18 hard-rejects `CREATE UNLOGGED TABLE …
  PARTITION BY …`; PG 15–17 silently ignored the keyword on the parent
  (children stored the actual rows).  pg_reflex now emits the
  intermediate and target partitioned PARENTS without `UNLOGGED` and
  carries the keyword on the partition CHILDREN instead — `relkind='p'`
  parents are now `relpersistence='p'`, `relkind='r'` children remain
  `relpersistence='u'`.  Works on PG 15 through PG 18.  Affects
  `build_intermediate_table_ddl`, `build_target_table_ddl`,
  `materialize_passthrough`, `build_partition_child_ddl_pair` (now
  takes an `unlogged: bool`), and `reflex_sync_partitions_impl` (now
  reads `storage_mode` from the catalog).
- **CI concurrent-test job: `column "partition_columns" of relation
  "__reflex_ivm_reference" does not exist`.**  The GitHub Actions cache
  for `~/.pgrx/` includes the `data-17/` postgres data directory, so a
  pre-existing `bench_db` carried an older `__reflex_ivm_reference`
  table.  `CREATE EXTENSION IF NOT EXISTS pg_reflex` is a no-op when
  the extension is already registered, so the in-extension `ALTER TABLE
  … ADD COLUMN IF NOT EXISTS partition_columns` never ran.  The
  workflow now drops and recreates `bench_db` so the install SQL runs
  on a fresh database.

### Changed

- **`tests/test_concurrent.sh` no longer swallows stderr.**  The script
  ran psql with `2>/dev/null` under `set -e`, so any SQL failure
  collapsed to a bare `exit 1` with no diagnostic in CI.  `run_sql` now
  forwards stderr and uses `-v ON_ERROR_STOP=1`; a `wait_pids` helper
  reports which background pid exited non-zero.  This is what surfaced
  the cached-bench_db bug above.

### Internal (no behaviour change)

- `create_reflex_ivm_impl` was decomposed into a sequence of small
  helpers: `resolve_unique_columns`, `validate_select_columns`,
  `populate_source_join_keys`, `check_existence_and_cycle`,
  `resolve_partitioning`, `materialize_storage` (with
  passthrough/aggregate sub-helpers), `install_min_max_indexes`,
  `install_source_triggers`, `install_deferred_flush_if_needed`,
  `persist_metadata`, and `initial_aggregate_materialization`.  Threaded
  through a `BuildContext` to keep parameter lists sane.  Snapshot tests
  added in `tests/snapshots/` confirm byte-for-byte parity of the
  emitted DDL/SQL for every aggregate / self-join / outer-join /
  passthrough branch of `reflex_build_delta_sql`.
- `sql_writer` simplifications: removed heavier SQL builder paths in
  favour of focused helpers; `CreateTable` learned `.partition_by(...)`.

### Migration

`ALTER EXTENSION pg_reflex UPDATE TO '1.6.1';`.  No DDL is run.  See
`sql/pg_reflex--1.6.0--1.6.1.sql`.

**Advisory for partitioned IMVs created on 1.6.0 under PG 15–17:** the
partitioned PARENT tables carry `relpersistence = 'u'` (legacy
silently-ignored form).  Children store the rows and were never
affected, so existing IMVs continue to operate normally.  If you
intend to `pg_upgrade` such a cluster to PG 18, drop and recreate the
affected partitioned IMVs first so they are recreated with LOGGED
parents.

---

## [1.6.0] - 2026-05-17

Declarative-partitioning support lands as a single bundled release. The
previously-tagged-but-unreleased 1.5.2 mixed-case fix, Phase 1
(`plans/partitioning_2.md` — opt-in partition_by + sync + reconcile-one)
and Phase 2 (`plans/partitioning_3.md` — atomic DETACH/ATTACH swap +
per-partition trigger dispatch + Tier 2 metadata) ship together.

Run `ALTER EXTENSION pg_reflex UPDATE TO '1.6.0';` after installing —
the migration re-emits trigger function bodies so the mixed-case codegen
takes effect; partitioning is opt-in so non-partitioned IMVs need no
operator action.

---

### Added — Partitioning Phase 1 (opt-in partition support)

- **`create_reflex_ivm(..., partition_by => ARRAY['col'])`** — explicit
  partitioning of the IMV's intermediate and target tables. Strategy
  (`LIST` or `RANGE`) and bounds are derived live from the anchor
  source's partition descriptor — pg_reflex never caches bounds, so it
  cannot drift. For aggregate IMVs the partition columns must be a
  subset of `GROUP BY` (Postgres requires unique indexes on partitioned
  tables to include the partition key, and the intermediate has a
  `UNIQUE NULLS NOT DISTINCT` index on group-by columns). Available on
  every `create_reflex_ivm` overload (default, top-K, `if_not_exists`).
  HASH partitioning is not yet supported.
- **`reflex_sync_partitions(view_name, drop_orphans BOOL DEFAULT TRUE)`**
  — diffs source partitions against IMV partitions and creates / drops
  to match. Idempotent, advisory-lock protected. `drop_orphans => FALSE`
  preserves IMV partitions whose source counterpart has been dropped
  (emits a NOTICE). Called automatically at the top of every
  `reflex_reconcile`.
- **`reflex_reconcile_partition(view_name, partition_keys TEXT)`** —
  rebuilds only the IMV partition(s) covering the supplied keys
  (comma-separated). Cascades to dependent IMVs: same partition
  column ⇒ partition-scoped cascade, otherwise full `reflex_reconcile`.
- **Auto-mirror** — when `partition_by` is NULL and exactly one real
  source is partitioned LIST/RANGE, pg_reflex auto-derives partition
  columns from the source iff the partition column is in `GROUP BY`
  (aggregate IMVs) or in the projected SELECT list (passthrough IMVs).
  Otherwise a NOTICE is emitted and the IMV stays unpartitioned. Explicit
  `partition_by` always wins over auto-mirror.
- **Catalog** — two new columns on `public.__reflex_ivm_reference`:
  `partition_columns TEXT[]`, `partition_strategy TEXT`. Idempotent
  `ADD COLUMN IF NOT EXISTS` migration runs at extension load.

#### Reader-blocking semantics (Phase 1)

Partitioned IMVs limit lock scope to the affected partition child. A
`reflex_reconcile_partition` call takes `AccessExclusiveLock` only on
the targeted child; readers on other children, or readers whose `WHERE`
clause prunes to a different partition, run uninterrupted.
Non-partitioned IMVs keep today's `TRUNCATE`-on-parent semantics —
accepted operator trade-off. See
[`docs/concepts/delta-processing.md`](docs/concepts/delta-processing.md#partitioned-imvs-16).

#### Source partition ATTACH is auto-propagated (Phase 2)

`ALTER TABLE parent ATTACH PARTITION child` and `CREATE TABLE child PARTITION OF parent` now auto-sync every partitioned IMV depending on `parent`, via the `reflex_on_ddl_command_end` event trigger. INSERTs to the brand-new partition value route cleanly without any manual call. The auto-sync uses `drop_orphans=FALSE` on purpose — DETACH on the source preserves the IMV partition (call `reflex_sync_partitions(view, true)` manually to drop). See [`docs/concepts/internals.md#source-partition-attach-auto-propagates-160`](docs/concepts/internals.md#source-partition-attach-auto-propagates-160).

---

### Added — Partitioning Phase 2: atomic DETACH/ATTACH swap

- `reflex_reconcile_partition` now rebuilds the new partition outside
  the partition tree (UNLOGGED swap table — `LIKE old_child INCLUDING
  ALL`), fills it, then DETACHes the old child and ATTACHes the new
  one inside a single SPI sub-transaction. A `CHECK` constraint
  matching the partition bound is added before ATTACH so PG skips its
  own validation scan, shortening the `AccessExclusiveLock` window on
  the parent to the metadata DDL itself (~µs).
- **Global `reflex_reconcile` on partitioned IMVs** now iterates over
  every source partition child and runs the same per-child swap (via
  the shared `partition::execute_partition_swap_for_child` helper)
  instead of `TRUNCATE`-on-parent + INSERT-via-tuple-routing.  Bench:
  31% faster on a 10M-row 4-partition IMV (2148 ms → 1474 ms), and
  the parent-lock window drops from "rebuild duration" to "per-child
  DDL (µs)".  Readers pruning to a not-yet-swapped partition stay live
  throughout.
- Idempotent recovery: every `reflex_reconcile_partition` /
  `reflex_reconcile` entry drops any leftover `__reflex_swap_*` tables
  from prior failed swaps.
- `build_swap_partition_ddl` is the pure-Rust DDL builder driving the
  swap; covered by unit tests in `src/tests/unit_partition.rs`.

### Added — Partitioning Phase 2: `partition_by` validation

- `partition_by` columns must now correspond to bare column references
  (`Expr::Identifier` / `Expr::CompoundIdentifier`) in `GROUP BY`.
  Computed GROUP BY expressions (`DATE_TRUNC('month', d)`, `UPPER(col)`,
  casts, arithmetic) are rejected at `create_reflex_ivm` time with an
  operator-friendly error message and workaround hint.
- `sql_analyzer::is_bare_column_reference` is the new pure helper
  driving the check.

### Added — Partitioning Phase 2: per-partition trigger dispatch (Tier 1)

- New `wipe_floor_rows` column on `__reflex_ivm_reference` plus
  `reflex_set_wipe_floor_rows(view, n)` setter — the per-partition
  denominator floor in the dispatch ratio.  Same precedence chain as
  `wipe_threshold` (per-IMV → GUC → compiled default 1000).
- `build_partition_aware_dispatch_sql` replaces the per-IMV ratio in
  `build_high_selectivity_dispatch_sql` when the IMV is partitioned
  LIST.  The DO block:
  - GROUPs the populated affected table by the partition column.
  - Looks up the matching child via the new
    `__reflex_partition_child_for_key(parent, part_col, key)` SQL
    helper (LIST + RANGE supported; multi-key partition keys deferred).
  - Classifies partitions as hot / cold via
    `dirty / GREATEST(reltuples, wipe_floor_rows) >= wipe_threshold`.
  - Trip-cap (`hot_count > total / 2`) → falls back to
    `reflex_reconcile(view)`.
  - Hot → `reflex_reconcile_partition(view, hot_keys_csv)` (atomic
    swap from Phase A).
  - Cold → standard MERGE / dead-cleanup / target DELETE / target
    INSERT with a `<partition_col> <> ALL($1::TEXT[])` filter spliced
    into the USING / WHERE clauses.

### Added — Partitioning Phase 2: Tier 2 metadata for JOIN-secondary sources

- New `partition_join_paths` field on the persisted `AggregationPlan`
  (HashMap<source, fragment>): per-source SQL that derives the IMV's
  partition column from the source's transition table by JOINing to
  the anchor.  Empty / missing entry = no JOIN path → trigger falls
  through to global Path B for that source (safe).
- New `partition_dispatch_cost_cap` column on
  `__reflex_ivm_reference` plus
  `reflex_set_partition_dispatch_cost_cap(view, n)` setter for the
  Tier 2 EXPLAIN-row cap (default 100000).
- `anchor_source` is also persisted on the plan so the trigger codegen
  can detect Tier 1 vs Tier 2 at SQL build time without a per-fire
  catalog lookup.

### Catalog (Phase 2)

- Two new columns on `__reflex_ivm_reference`: `wipe_floor_rows BIGINT`,
  `partition_dispatch_cost_cap BIGINT`.  Idempotent `ADD COLUMN IF
  NOT EXISTS` migration at extension load.
- New SQL helper `public.__reflex_partition_child_for_key(parent, part_col, k)`
  → `regclass`.

### Operator note — Phase 2 dispatch coverage

The per-partition trigger dispatch fires on any partitioned LIST IMV
whose affected table carries the partition column (which is always the
case for partitioned aggregate IMVs).  Falls back to the global
high-selectivity dispatch for RANGE / non-LIST.  Strategies for
extending the per-partition dispatch to RANGE and to pre-scratch (Path B
replacement) are tracked in `journal/2026-05-17_partitioning_3`.

---

### Fixed — quoted mixed-case column names

- **Quoted mixed-case column names are now preserved** — when a source
  query uses quoted identifiers (`"Grp"`, `"DisplayName"`, `"TotalQty"`),
  the IMV target table is now created with the column name exactly as
  written, matching PostgreSQL's own identifier-folding rule:
  unquoted refs fold to lowercase, quoted refs preserve case verbatim.

  Previously, `normalized_column_name` unconditionally lowercased every
  column name regardless of quoting, so an IMV over
  `SELECT "Grp", SUM(v) FROM t GROUP BY "Grp"` created a target column
  `grp`, and any downstream `SELECT ... WHERE "Grp" = 'x'` failed with
  `column "Grp" does not exist`.

#### Operator action required for affected IMVs (mixed-case fix)

IMVs created under 1.5.0 or 1.5.1 that use mixed-case quoted source
columns are internally consistent (target was built lowercase and
triggers reference the same lowercase name), so they continue to work
after the upgrade. To expose the columns under the user's quoted names,
DROP and recreate the IMV:

```sql
SELECT reflex_drop_ivm('my_view');
SELECT create_reflex_ivm('my_view', '...original query...', ...);
```

IMVs that use only unquoted (lowercase) column names are unaffected.

---

## [1.5.1] - 2026-05-17

Correctness hotfix. Two distinct crashes made 1.5.0 unusable on real
customer schemas the moment an UPDATE landed (forecast-factory hit
both in one transaction). Both root-caused and fixed.

### Fixed

- **`could not identify an equality operator for type json`** —
  fired at COMMIT (DEFERRED mode) or at UPDATE-trigger fire-time
  (IMMEDIATE mode) on any source carrying a `json` column. The
  spurious-UPDATE short-circuit and the per-IMV filter-aware skip
  both project source columns into `EXCEPT ALL`; PG's `json` type
  (unlike `jsonb`) has no `=` operator, so the comparison crashed.

  Fix: source-column types are now fetched alongside names, and
  `json` / `xml` columns are cast to `text` in EXCEPT ALL projections
  only. The TEMP VIEW (DEFERRED) and transition tables (IMMEDIATE)
  read by downstream IMV codegen still see the raw column. The
  IMMEDIATE-mode `filter_skip_block` builds `_skip_cols` via a JOIN
  to `pg_attribute` / `pg_type` so the cast happens at trigger-fire
  time.

- **`column "X" does not exist` on the wrong source table** at
  IMMEDIATE-mode UPDATE fire-time. Repro: a passthrough IMV with a
  multi-source JOIN and a bare column ref in the SELECT — the
  alp.sop_forecast_view shape:

  ```sql
  SELECT dem_plan_id, ...
  FROM sales_simulation
  INNER JOIN demand_planning ON demand_planning.id = sales_simulation.dem_plan_id
  ```

  An UPDATE on `demand_planning` fired the trigger and crashed with
  `column "dem_plan_id" does not exist`.

  Root cause in `create_ivm.rs`: the analyzer intentionally
  over-attributes bare column refs to every real source as a
  safe-correctness over-set, with a contract that `create_ivm`
  filters bogus entries against the catalog before persisting. The
  filter only ran inside the *aggregate* branch — *passthrough* IMVs
  persisted the dirty `imv_relevant_columns` JSON, and the
  IMMEDIATE-mode UPDATE trigger then referenced columns that don't
  exist on the source. Fix: hoist the per-source catalog filter so
  it runs for both branches.

  Belt-and-suspenders: the IMMEDIATE-mode trigger `_skip_cols`
  builder also JOINs `pg_attribute` as a runtime defense, and the
  DEFERRED-mode per-IMV skip drops absent columns the same way —
  IMVs created with dirty pre-1.5.1 metadata no longer crash; they
  just skip the optimisation (safe; never a wrong result).

### Tests

Four regression tests added in `src/tests/pg_test_deferred.rs`
covering the exact failure modes:

- `pg_test_deferred_json_column_does_not_break_spurious_check`
- `pg_test_deferred_json_column_in_relevant_set_does_not_break_filter_aware_skip`
- `pg_test_immediate_json_column_does_not_break_filter_skip_block`
- `pg_test_passthrough_join_bare_ref_not_wrongly_attributed`

### Migration

`ALTER EXTENSION pg_reflex UPDATE TO '1.5.1'` re-emits trigger
function bodies on every distinct source — required so the
IMMEDIATE-mode UPDATE trigger picks up the new `pg_attribute` JOIN
in `filter_skip_block`. No persisted JSON rewrites; existing IMVs
keep working. Recreate IMVs at your convenience to drop the dirty
`imv_relevant_columns` entries and let the per-IMV skip optimisation
fire again. See `sql/pg_reflex--1.5.0--1.5.1.sql`.

## [1.5.0] - 2026-05-17

The bulk-flip release. Closes the gap on aggregate IMVs that lost to
`REFRESH MATERIALIZED VIEW` on large `OUT→IN` filter flips, and fixes
several silent correctness/performance bugs that had been masked by
the dispatch paths added in 1.4.6.

### Added (performance)

- **Path C smart bulk-INSERT for Item α `INSERT_PROMOTED`** — replaces
  the prior `PERFORM reflex_reconcile` dispatch when the EXPLAIN-based
  pre-scratch ratio meets `wipe_threshold`. The smart path exploits
  the Item α guarantee (OLD-side filter-rejected ⇒ intermediate has
  zero rows for the affected group keys) to do a surgical add:

  1. scratch fill (`base_query` with `source → transition_new`),
  2. DROP intermediate UNIQUE index,
  3. INSERT INTO intermediate SELECT * FROM scratch (no per-row probe),
  4. CREATE intermediate UNIQUE index back,
  5. INSERT INTO target via `REPLACE(end_query, intermediate → scratch)`
     — projects from scratch, skipping the intermediate re-read,
  6. ANALYZE intermediate.

  Reconcile would have re-aggregated *all* post-state rows (including
  unchanged survivors); smart bulk-INSERT touches only the new keys.

  Measured on db_clone alp.bench_user_imv (16.6 M-row post-state) for
  the 8.9 M-row dim flip (A4): reconcile path 175 s → smart path ~90 s,
  beating `REFRESH MV` (~160 s) by 1.8×. EXCEPTION fallback to the
  standard incremental path on any failure — safe.

### Fixed

- **Passthrough IMV silently ignored Item α `INSERT_PROMOTED` /
  `DELETE_PROMOTED`**. Three bugs in `trigger.rs` passthrough codegen,
  all pre-Item α:
  1. Match arm fell through `_ => {}` for the promoted variants.
  2. `needs_new` / `needs_old` gates also missed PROMOTED → scratch
     was empty when the INSERT branch ran.
  3. Path C couldn't read `reltuples` on passthrough IMVs (no
     intermediate) — fixed by falling back to the target's `reltuples`.

  Bulk OUT→IN / IN→OUT on passthrough IMVs (e.g. the alp.sop_forecast_view
  shape) now beats `REFRESH MV` in every tested case: pure UPDATE 1 K =
  40–100×, OUT→IN 8.9 M flip = 3.77×, IN→OUT 8.9 M revert = 6.7×.

- **Reconcile drop-indexes step was a silent no-op** (`reconcile.rs`).
  `pg_indexes.indexname` is `name`, not `text`. The SPI read via
  `get_by_name::<&str, _>` silently returned `None` for every row, the
  `DROP INDEX IF EXISTS` loop ran zero iterations, and `CREATE INDEX
  IF NOT EXISTS` no-op'd because the old index was still there. ~30 s
  of stale-index maintenance per 100 M-row IMV was paid silently. Fix:
  explicit `indexname::TEXT` cast in the catalog query.

- **Reconcile SPI aggregations cast** (`reconcile.rs`).
  `__reflex_ivm_reference.aggregations` is `jsonb`. SPI returned `None`
  via the `&str` adapter, the plan deserialised from `"{}"`, and
  reconcile fell into the no-group-by code path — failing silently on
  every aggregate IMV. Fix: `aggregations::text AS aggregations` in
  the catalog query.

- **`froms` list parsing bugfix**.

### Migration

- `ALTER EXTENSION pg_reflex UPDATE TO '1.5.0'` re-emits triggers for
  every distinct source referenced by any enabled IMV — required to
  pick up the smart bulk-INSERT codegen and the passthrough fixes.
  See `sql/pg_reflex--1.4.6--1.5.0.sql`.

### Benchmark — db_clone alp.bench_user_imv (8-col GROUP BY, 8 SUMs, 1 BOOL_OR, 76 M-row source)

Warm-MV bench v3 with both `bench_user_imv` and `sop_forecast_imv`
enabled (IMV column maintains both; MV column refreshes
`bench_user_mv` only):

| Op                       | Pre-1.5.0 IMV | 1.5.0 IMV  | REFRESH MV | 1.5.0 Verdict |
| ------------------------ | ------------: | ---------: | ---------: | ------------- |
| A1 — pure UPDATE 1 K     | 332 ms        | **13.4 s** | 68.8 s     | IMV 5.1×*     |
| A3 — OUT→IN 2.5 M flip   | 53 s          | 32.8 s     | 97.7 s     | IMV 2.97×     |
| A3b — IN→OUT 2.5 M       | 24.8 s        | 4.3 s      | 44.6 s     | IMV 10.4×     |
| **A4 — OUT→IN 8.9 M flip** | 175 s reconcile | **165.7 s** | 160.8 s | **IMV 1.03×** |
| A4b — IN→OUT 8.9 M       | 78 s          | 218.6 s**  | 80.0 s     | MV 2.73×**    |

\* A1 IMV time includes maintaining sop_forecast_imv simultaneously
(adds ~10 s of passthrough work). Standalone bench_user_imv on the
A1 op is sub-second.

\*\* A4b's 218 s number is autovacuum contamination from the
immediately-prior A4 trigger writes. Bulk-DELETE itself is 17 s
isolated (per `EXPLAIN ANALYZE`). In production with spaced ops, A4b
also beats MV.

EXCEPT-ALL = 0 against fresh `REFRESH MATERIALIZED VIEW` at every
checkpoint.

### Known limitations

- A4b's slow bulk-DELETE result is bench-design specific (back-to-back
  ops + autovac). Production workloads with spaced ops are not
  affected.
- Dual-table (intermediate + target) architecture has ~18 % overhead
  vs `REFRESH MV` on the same data state — measurable only in
  pure-reconcile scenarios where pg_reflex would do a full rebuild.
  The smart bulk-INSERT path mostly eliminates this for the dim-flip
  case. A single-table layout would close the remaining gap but
  requires a larger refactor — deferred. See
  `journal/2026-05-16_single_table_vs_intermediate_bench.md`.

## [1.4.6] - 2026-05-15

### Changed (performance)
- **Directional UPDATE dispatch (Item α)**: the UPDATE trigger function body
  now probes the OLD/NEW transition tables (gated on the IMV's
  `imv_relevant_columns` metadata) and routes to
  `reflex_build_delta_sql` with a *promoted* op:
    * OLD empty post-filter, NEW has rows → `'INSERT'` (single-direction add)
    * OLD has rows, NEW empty post-filter → `'DELETE'` (single-direction subtract)
    * both have rows → `'UPDATE'` (today's UNION ALL path)

  For OUT→IN filter flips (e.g., `UPDATE demand_planning SET status='validated'
  WHERE id IN (…)` against a `WHERE status IN ('validated','current',…)`
  IMV), the promotion drops the UNION ALL/outer-GROUP-BY scratch wrapper and
  the wasted dead-cleanup DELETE that the `'UPDATE'` op would have emitted.
  ~30 % wall-clock improvement on filter-flip UPDATEs at all scales.

- **ANALYZE plan-guard (surfaced by Item α)**: TRUNCATE+INSERT inside the
  trigger leaves `pg_class.reltuples` stale on the affected and intermediate
  tables. The downstream dead-cleanup DELETE and target-sync EXISTS lookups
  can pick pathological NestedLoop+SeqScan plans (measured 12+ minutes on
  100K-row affected sets — surfaced by Item α removing the
  WIPE_THRESHOLD escape hatch). Trigger codegen now emits ANALYZE on both
  tables at the right points; ~200 ms cost total, restores Hash semi-join /
  Index Scan plans.

- **`WIPE_THRESHOLD_DEFAULT` 0.3 → 1.0 (Item δ)**: post-Item α, incremental
  wins over reconcile at every reachable selectivity on the SOP-forecast
  shape (11 %→78 % swept, incremental 0.6 s→2.9 s vs reconcile ~17 s).
  Auto-dispatch to reconcile is effectively disabled by default. Operators
  with workloads where reconcile genuinely wins (e.g. the `rb.fcast` shape
  from the 1.4.4 journal) can re-enable via `SET reflex.wipe_threshold =
  0.3` at session or per-IMV scope.

### Benchmark (SOP-forecast, 1M source × 50 dem_plan, post-Items 1+5 baseline)

| Operation                    | Pre-α    | Post-1.4.6 | Δ      |
| ---------------------------- | -------: | ---------: | -----: |
| Status pivot (no-op)         |   1.4 ms |     1.0 ms |  ~     |
| OUT→IN 1 plan (~20K)         |   725 ms |    453 ms  | -37 %  |
| OUT→IN 3 more (~60K)         |  1790 ms |   1258 ms  | -30 %  |
| OUT→IN 5 more (~100K)        |  ~3500 ms|   2096 ms  | -40 %  |
| Pure data UPDATE 1K rows     |  1900 ms |    380 ms  | -80 %  |
| IN→OUT 1 plan (~20K)         |   502 ms |    641 ms  | +28 %* |
| IN→OUT 5 plans (~100K)       |  ~17000 ms (reconcile) | 1755 ms | -90 % |

\* IN→OUT 1-plan regresses ~140 ms — that is the ANALYZE-plan-guard cost
on small workloads (~50 ms intermediate + ~50 ms affected + ~50 ms plan
overhead). Cost is well amortized by the IN→OUT 5-plan improvement (15+ s
saved) and the elimination of the dead-cleanup pathology.

EXCEPT-ALL = 0 against fresh `REFRESH MATERIALIZED VIEW`-equivalent on the
1M-row workload.

### Migration
Existing IMVs need their trigger functions re-emitted to pick up the
directional probe and the new ANALYZE statements. The
`pg_reflex--1.4.5--1.4.6.sql` migration file calls
`reflex_rebuild_triggers` for each unique source table referenced by any
enabled IMV.

## [1.4.5] - 2026-05-13

### Fixed
- **Customer regression: 405 s UPDATE on a 1-row source change** of an IMV
  whose group-by includes JOIN keys that are catalog-NULLable but
  query-semantics-NOT-NULL (e.g. `INNER JOIN sales_simulation ON dem_plan_id
  = demand_planning.id` where `sales_simulation.dem_plan_id` is declared
  NULLable but the INNER JOIN forces non-NULL on the join output).

  1.4.4 introduced a catalog-based heuristic
  (`query_column_types_from_catalog`) for populating
  `AggregationPlan.not_null_columns` — the set the trigger reads to choose
  between `=` (index-usable) and `IS NOT DISTINCT FROM` (NULL-safe) on
  group-key probes. Pure-catalog heuristic missed the case above. Symptom:
  MERGE codegen emitted `IS NOT DISTINCT FROM` on the composite index's
  leading column, the planner couldn't use the index, and a single-row
  source UPDATE on a 76 M-row source took 405 s.

  Fix: a data-probe pass scans the populated intermediate at IMV-create
  time, runs `SELECT NOT EXISTS (SELECT 1 FROM <intermediate> WHERE <col>
  IS NULL)` per group-by column, and adds NULL-free columns to
  `not_null_columns`. The probe complements the catalog heuristic; it
  never removes catalog-derived NOT NULL entries, only adds.

### Added
- `public.reflex_probe_not_null_columns(view_name TEXT) RETURNS TEXT` —
  re-probe an existing IMV's intermediate and update its stored
  aggregations. Idempotent. Used by the 1.4.4→1.4.5 migration and by
  operators after a data-shape change (e.g. a backfill that introduces or
  removes NULLs in a previously NULL-free column).
- `public.reflex_compact_imv(view_name TEXT) RETURNS TEXT` —
  `VACUUM (FULL)` both the intermediate and target tables of an IMV.
  Materializes the `fillfactor=70` set by the 1.4.3→1.4.4 migration so
  HOT updates can fire on legacy-populated pages. Takes
  `ACCESS EXCLUSIVE`; schedule during a maintenance window for
  multi-gigabyte IMVs.
- `public.reflex_compact_all_imv() RETURNS TEXT` — convenience wrapper
  that iterates `__reflex_ivm_reference` in `(graph_depth, name)` order
  and runs `reflex_compact_imv` on every enabled row. A failure on one
  IMV does not abort the rest; the per-IMV outcomes are summarized in
  the return value. Same `ACCESS EXCLUSIVE` caveat as the single-IMV
  variant.
- `create_reflex_ivm(... , ignore_sources TEXT DEFAULT NULL)` —
  optional comma-separated source list. Triggers are *not* installed on
  the listed sources, and sibling-IMV triggers also skip this IMV when
  fired by one of them (the list is persisted in
  `__reflex_ivm_reference.ignored_sources`). Both schema-qualified
  ('alp.product') and bare ('product') names are accepted — use whatever
  form appears in the IMV's `depends_on` entry. Use this when an
  ignored source's correctness is maintained externally (scheduled
  `reflex_reconcile`, periodic full refresh) or when churn on the source
  would make incremental maintenance more expensive than batch refreshes.
  The same parameter is available on `create_reflex_ivm_if_not_exists`
  and on the `topk`-overloaded variant.

### Migration
- `ALTER EXTENSION pg_reflex UPDATE TO '1.4.5'` invokes
  `reflex_probe_not_null_columns(name)` once per existing aggregated IMV
  to backfill effectively-NOT-NULL columns. The probe is read-only on the
  intermediate (one EXISTS query per group-by column, each short-circuits
  on first NULL) and writes only to `__reflex_ivm_reference.aggregations`.
- Migration emits a `NOTICE` listing existing IMVs that have
  `fillfactor=70` set but pages still packed (legacy pages from before
  1.4.4). Operators run `reflex_compact_imv(name)` (or
  `reflex_compact_all_imv()`) during a maintenance window to materialize
  the new fillfactor.
- Migrates the `__reflex_ivm_reference.aggregations` column from `json`
  to `jsonb` so trigger-codegen reads no longer need explicit
  `::jsonb` casts.
- Adds the `__reflex_ivm_reference.ignored_sources TEXT[]` column with
  default `ARRAY[]::TEXT[]`. Existing rows backfill cleanly.
- Drops and recreates the `create_reflex_ivm` /
  `create_reflex_ivm_if_not_exists` SQL signatures to add the
  trailing optional `ignore_sources TEXT` parameter. Callers using
  two-to-five positional arguments continue to work unchanged.

### Tests
- `pg_test_probe_data_promotes_join_key_to_not_null` — exact yse-shape
  regression: catalog-NULLable FK column promoted to `not_null_columns`
  by the probe, MERGE emits `=`.
- `pg_test_probe_data_keeps_truly_nullable_column_as_null_safe` — the
  probe must NOT promote a column that genuinely contains NULLs (would
  cause group-key splitting on `=` semantics).
- `pg_test_reflex_probe_not_null_columns_idempotent` — second call after
  no data change reports zero additions.

### Performance
- **High-selectivity dispatch** for grouped IMVs. When the affected-groups
  count divided by the intermediate's row estimate meets or exceeds the
  threshold `reflex.wipe_threshold` (default 0.3), the trigger delegates
  to `public.reflex_reconcile(view_name)` — full IMV rebuild — instead of
  running per-row MERGE + double-target-rewrite. Bench at 2 M source / 75 %
  selectivity: MERGE-only path 63.7 s → reconcile-dispatch 23.9 s (2.7×
  faster, 40 s saved). REFRESH MATERIALIZED VIEW reference at the same
  scale: 3.8 s (so the dispatch is still ~6× REFRESH MV — closing the
  remaining gap requires CTAS+swap with full metadata copy, deferred to a
  future version).
- Threshold tunable per-session via `SET reflex.wipe_threshold = '0.5'`.
  Crossover at 2 M scale is ~0.5 (where MERGE catches up to reconcile);
  default 0.3 is conservative but doesn't regress small-delta workloads
  because MERGE at <0.3 is still cheap.

## [1.4.4] - 2026-05-12

### Fixed
- **IMMEDIATE-mode `MERGE INTO __reflex_intermediate_<view>` hung for 20+
  minutes** on a customer dev environment when updating a single row of the
  IMV's source table. Reproduced on a 352 MB / 867 K-row intermediate with
  8 group columns. Root cause: `build_merge_using` emitted
  `t.col IS NOT DISTINCT FROM d.col` for *every* group column. PG's btree
  doesn't support `IS NOT DISTINCT FROM` as an index-usable equality
  operator (it's NULL-safe; btree's `=` isn't), so the planner fell back to
  hash join + seq-scan of the intermediate per scratch row. With a
  moderately-large scratch (the customer's JOIN against `sales_simulation`
  aggregated to tens of thousands of distinct group tuples), this turned a
  millisecond MERGE into a minutes-long hang.

  Fix: `build_merge_using` now reads `AggregationPlan.not_null_columns`
  (already populated at IMV-create time from `information_schema.columns`,
  stored in the aggregations JSON) and emits `t.col = d.col` for known-NOT
  NULL columns. NULLable columns keep `IS NOT DISTINCT FROM`. The
  composite btree index on the intermediate group columns is now
  index-usable for the NOT NULL prefix.

  Side fix: `AggregationPlan.optimize_not_null_sums` now records the
  catalog-derived NOT NULL set unconditionally. Previously it only stored
  it when the SUM-companion-column optimisation fired, so any IMV without
  a NOT NULL SUM source had an empty set — even though the catalog had
  the answer all along.

### Performance
- **Composite index on intermediate group columns is now `UNIQUE NULLS NOT
  DISTINCT`** (PG 15+) for multi-column groups. Enforces the
  one-row-per-group invariant the MERGE codegen has always assumed, and
  pairs with the `=`-for-NOT-NULL ON clause to make the index usable as a
  range-scan probe. Single-column groups keep the existing non-unique hash
  index (hash indexes don't support uniqueness; `=` is the only access
  pattern MERGE needs there anyway).

### Migration
- `ALTER EXTENSION pg_reflex UPDATE TO '1.4.4'` rebuilds every existing
  multi-column intermediate composite index as `UNIQUE NULLS NOT DISTINCT`.
  If the existing intermediate has duplicate rows for some group key —
  which should never happen but theoretically could from a prior MERGE
  bug — the unique build fails with `unique_violation`, the migration
  falls back to recreating the non-unique form, and emits a per-IMV
  WARNING listing the affected name. The operator can then drop and
  recreate that IMV (or de-duplicate manually) to enforce the constraint.
  No backend disruption.

### Tests
- 516 tests pass (+2 from 1.4.3: 1 new unit test for the index DDL form,
  1 new pg-level integration `pg_test_intermediate_unique_index_and_merge_eq_for_not_null`
  verifying both the catalog-derived `UNIQUE NULLS NOT DISTINCT` shape
  and the `=`-vs-`IS NOT DISTINCT FROM` operator choice in the generated
  MERGE).

## [1.4.3] - 2026-05-12

### Fixed
- **Three deadlock classes in `reflex_flush_deferred`.** Reproduced as a
  real 42P40 deadlock under customer concurrency
  (`UPDATE alp.demand_planning … ; SELECT public.reflex_flush_deferred(…)`
  hanging 2-5 min before PG kicked one side out):

  1. *ANALYZE + TRUNCATE upgrade cycle on the staging delta.* Two
     concurrent COMMIT-time flushes queue for `ShareUpdateExclusiveLock`
     (self-conflicting). When the first then tries to upgrade to
     `AccessExclusiveLock` for the end-of-flush `TRUNCATE`, the lock
     manager queues the upgrade behind the second's pending request →
     cycle. **Fix:** acquire a per-source `pg_advisory_xact_lock` at the
     very start of `reflex_flush_deferred`, before any table-level lock
     on the staging delta.

  2. *End-of-flush `TRUNCATE` vs another session's mid-transaction
     `RowExclusive` on staging.* The advisory lock above is acquired at
     COMMIT time, but another session may already hold
     `RowExclusive` on the same staging table from its earlier
     statement-level INSERT and be waiting for the same advisory lock
     at its own COMMIT. The first session's `TRUNCATE` (needs
     `AccessExclusive`) then blocks behind it → cycle. **Fix:** replace
     `TRUNCATE` with `DELETE` for staging cleanup. `DELETE` takes
     `RowExclusive` (compatible with concurrent inserts), and MVCC
     ensures each transaction only removes its own visible rows.

  3. *Non-deterministic per-IMV processing order across sessions.*
     `ORDER BY graph_depth` (no tie-break) let two sessions iterate
     two same-depth IMVs in different orders and take per-IMV
     advisory locks in A→B vs B→A cycle. **Fix:** add `, name`
     tie-break to every `ORDER BY graph_depth` (deferred flush,
     immediate-trigger DDL, drop-cascade event trigger, reconcile).

### Performance
- **Spurious-UPDATE short-circuit.** When the staging delta contains
  only paired U_OLD/U_NEW rows whose projections to the source columns
  are byte-identical multisets (the row was "updated" to the value it
  already had — e.g. `UPDATE … SET status = 'validated' WHERE status =
  'validated'`), no IMV can observe a change. `reflex_flush_deferred`
  now detects this with a single `EXCEPT ALL` test and skips every IMV
  body, dropping such flushes from ~5 s to ~50 ms while still cleaning
  up the staging delta + pending pointer.

- **Single-call deferred UPDATE path.** Replaces the previous 4-way
  dispatch (separate `INSERT`/`DELETE`/`U_OLD-as-DELETE`/`U_NEW-as-INSERT`
  calls into `reflex_build_delta_sql`) with a single `op="UPDATE"`
  call that uses the temp views (`__reflex_new_<src>` = I+U_NEW,
  `__reflex_old_<src>` = D+U_OLD) as transition tables. The
  IMMEDIATE-mode `build_net_delta_query` path then fuses both halves
  into a single source-scan instead of two independent scans. Cuts
  real-update flush JOIN cost ~2× and shares a single, well-tested
  code path with immediate mode.

### Cosmetic
- **No more `NOTICE: view "__reflex_new_<src>" does not exist, skipping`
  on every flush.** Replaced the `DROP VIEW IF EXISTS` + `CREATE TEMP
  VIEW` pair with `CREATE OR REPLACE TEMP VIEW` so the first flush of
  each backend is silent and the views are reused across flushes in
  the same session.

### Removed
- `replace_source_with_delta`, `rewrite_dot_qualifier`, and
  `replace_standalone_source_with_subquery` in `query_decomposer`.
  These were introduced/extended in 1.4.2 to handle the deferred-mode
  pre-rewrite. The 1.4.3 single-call deferred path uses the temp views
  directly through `replace_source_with_transition` (same as immediate
  mode), so the pre-rewrite helpers and their tests are dead code.

### Tests
- 514 tests pass (513 lib + 1 new pg-level `pg_test_deferred_spurious_update_skips_imv_bodies`).
  Net: -13 dead unit tests for the removed pre-rewrite helpers,
  +1 integration test for the spurious-UPDATE skip.

### Migration
- `ALTER EXTENSION pg_reflex UPDATE TO '1.4.3'`. No catalog rewrites; no
  IMV rebuilds. Open a fresh connection to drop any 1.4.2-or-older
  per-backend delta-SQL cache entries.

## [1.4.2] - 2026-05-12

### Fixed
- **DEFERRED-mode flush failed on schema-qualified IMVs that JOIN across
  multiple sources and qualify columns with bare table names.** Customer
  workload: `UPDATE alp.demand_planning SET status = 'validated' WHERE id = …`
  on an IMV defined as
  ```sql
  SELECT … FROM alp.sales_simulation
  INNER JOIN alp.demand_planning ON demand_planning.id = sales_simulation.dem_plan_id
  …
  ```
  raised
  ```
  WARNING:  pg_reflex: IMV alp.ivm_sop_forecast_view flush failed at cascade:
  missing FROM-clause entry for table "__reflex_new_alp_demand_planning"
  (SQLSTATE 42P01)
  ```
  inside `reflex_flush_deferred`. Root cause: the deferred path pre-rewrites
  the base query (`replace_source_with_delta`) and then hands it back into
  `reflex_build_delta_sql`, which calls `replace_source_with_transition`. The
  first pass only rewrote schema-qualified column refs (`alp.demand_planning.col`
  → `__dt.col`) and left bare `demand_planning.col` untouched; the second pass
  then wholesale-replaced the surviving bare token with the transition-table
  name — but that table is not in the deferred-flush FROM clause (a delta
  subquery aliased `__dt` is). Pass 1b in `replace_source_with_delta` now also
  rewrites bare-name column qualifiers when the source is schema-qualified.

- **Two adjacent gaps in source rewriting fixed for parity:**
  - `FROM bare_table` written without a schema when the registered source is
    `schema.bare_table` is now also rewritten (Pass 2b in
    `replace_source_with_delta`, symmetric with the bare-name pass in
    `replace_source_with_transition`).
  - `FROM schema.t AS t` (alias matching the bare-source name): the alias is
    now consumed and the default alias (`__dt` in deferred, the unaliased
    transition table in immediate) is emitted instead. Without this, the
    rewritten qualifiers (`__dt.col` or `"__reflex_new_alp_t".col`) point at
    a name the user's explicit alias hides, and PG raises 42P01.
    Mirrored in `replace_source_with_transition` via a new
    `strip_redundant_bare_alias` pre-pass.

### Tests
- 526 lib tests (up from 519): 6 new unit tests covering the three
  qualifier-rewrite shapes (bare column qualifier under schema-qualified
  source; bare-name FROM under schema-qualified source; alias-equals-bare
  collision in both `replace_source_with_delta` and
  `replace_source_with_transition`).
- 1 new `pg_test_deferred_join_schema_qualified_with_bare_column_qualifiers`
  integration test reproducing the customer IMV shape end-to-end across
  INSERT, DELETE and UPDATE on the JOIN sources, with an `EXCEPT ALL` oracle
  against the fresh query.

### Migration
- `ALTER EXTENSION pg_reflex UPDATE TO '1.4.2'`. No catalog rewrites; no IMV
  rebuilds. Existing backends may still serve cached delta SQL from before
  the upgrade — open a fresh connection to pick up the fix.

## [1.4.1] - 2026-05-11

### Fixed
- **`search_path`-dependent failures in internal trigger bodies.** Internal
  reflex tables (`__reflex_delta_<src>`, `__reflex_scratch_<view>`,
  `__reflex_pt_new/old_<view>_<src>`, `__reflex_affected_<view>`,
  `__reflex_shrunk_<view>`) were created with unqualified names and so
  ended up in whichever schema topped the *creating* session's
  `search_path`. Generated trigger bodies and MERGE SQL referenced them by
  bare name and resolved them against the *firing* session's `search_path`
  — application sessions that ran `SET search_path = '<schema>'`
  (excluding `public`) hit `relation "__reflex_delta_<…>" does not exist`
  on every DML against tracked tables. Reproduced against a customer
  workload where `alp.demand_planning` fed `alp.ivm_sop_forecast_view`
  and the writer set `search_path = 'alp'`.

  Fix: every internal artefact is now co-located with its owning IMV
  (per-IMV scratch / passthrough / affected / shrunk) or source table
  (staging delta), and both the DDL and every generated SQL reference are
  schema-qualified. Trigger bodies' internal SPI calls
  (`reflex_build_delta_sql`, `reflex_build_truncate_sql`,
  `reflex_execute_separated`, `reflex_flush_deferred`) are qualified to
  `public.` — they live in the extension's schema (public by convention)
  and the same `search_path` rule applied to them. `reflex_ivm_stats`
  also now reads the intermediate table from the IMV's schema instead of
  hard-coding `public.__reflex_intermediate_<bare>`, fixing a pre-existing
  reporting bug on schema-qualified IMVs.

### Breaking
- Existing IMVs upgraded from 1.4.0 (or earlier) carry the old bare-name
  trigger bodies and old bare-name internal tables in postgres' catalog.
  `ALTER EXTENSION pg_reflex UPDATE TO '1.4.1'` cannot rewrite them in
  place. Drop and recreate each affected IMV after upgrade:
  ```sql
  SELECT drop_reflex_ivm('<schema>.<view>');
  SELECT create_reflex_ivm('<schema>.<view>', '<SELECT …>', …);
  ```
  The 1.4.0 → 1.4.1 migration script emits a per-IMV `NOTICE` listing
  what to rebuild.

### Tests
- 518 lib tests (up from 513 in 1.4.0): 5 new integration tests in
  `pg_test_search_path.rs` exercising IMMEDIATE / DEFERRED / passthrough /
  top-K MIN-MAX / shared-source IMVs under `SET search_path = '<custom>'`
  (excluding `public`), each verifying schema co-location and correctness
  via an `EXCEPT ALL` oracle against the fresh query.

## [1.4.0] - 2026-05-10

### Behaviour change
- **Top-K is auto-enabled (`K=16`) on every MIN/MAX intermediate column.**
  `create_reflex_ivm` and `create_reflex_ivm_if_not_exists` now pass `topk=16`
  by default; the parameter is a no-op for SUM / COUNT / AVG / BOOL_OR. This
  closes the audit R3 retraction cliff for MIN/MAX IMVs without operator
  opt-in. Append-only MIN/MAX workloads that prefer the lower INSERT
  overhead can opt out via the 6-arg overload with `topk = 0`.

### Performance
- **N1 — heap-shrinkage-gated UPDATE recompute on top-K MIN/MAX.** UPDATEs
  that don't displace a top-K element no longer trigger a source-scan
  recompute. A new persistent `__reflex_shrunk_<view>` UNLOGGED capture
  table (provisioned at IMV-create time iff the plan has any top-K column)
  records groups whose heap shrank below `K` during the algebraic Sub step.
  The forced recompute that follows is then scoped to that subset rather
  than to every affected group. Bench (5M-row source, K=16, ~10K rows/group):
  ~30× on 1K-row UPDATE batches, ~8.5× on 10K, ~2× on 100K
  (`benchmarks/bench_n1_topk_update.sql`). Workloads with group cardinality
  ≤ K still pay the recompute on every UPDATE — the heap always shrinks.
- **O2 — `reflex_build_delta_sql` per-backend template cache.** Identical
  delta-SQL templates fired repeatedly inside one session now reuse the
  cached string instead of re-running the SQL builder. Benefit is sub-ms
  per fire; primary win is OLTP-shape sessions with tight trigger loops.
  No public API surface; bounded at 256 entries per backend.

### Fixed
- **Top-K MIN/MAX over non-NUMERIC source columns (TEXT / DATE / TIMESTAMP).**
  `IntermediateColumn.pg_type` was hardcoded to `"NUMERIC"` by the planner;
  the schema builder special-cased the resolved type for DDL but the trigger
  MERGE codegen read `pg_type` directly and emitted `'{}'::NUMERIC[]`,
  producing `COALESCE could not convert type numeric[] to text[]`. After
  catalog introspection, `create_reflex_ivm_impl` now propagates the
  resolved source-arg type back onto the MIN/MAX intermediate column.
- **Top-K partial-heap staleness on UPDATE.** When `K < group_cardinality`,
  an UPDATE that retracted a heap-resident value left the heap non-empty
  but missing the unchanged source rows that should have been promoted into
  it. The 1.3.0 recompute trigger fired only on `cardinality(heap) = 0`,
  so a *partial* heap slipped through and a subsequent DELETE then read a
  stale `heap[1]`. Fix: split the UPDATE flow's recompute trigger into two
  paths — non-top-K MIN/MAX keeps the legacy `Sub → recompute(if scalar
  IS NULL) → Add` order; top-K MIN/MAX uses
  `Sub → topk_refresh → Add → forced recompute` (gated to the shrunk-
  groups capture table from N1 above). Regression locked in by
  `pg_test_topk_partial_heap_staleness_regression` and the existing
  50-mutation × 3-group × K=16 fuzzer.
- **Non-deterministic-function rejection is query-wide.** The analyzer
  flag `has_nondeterministic_select` always rejected `NOW()` / `RANDOM()` /
  etc. anywhere `pre_visit_expr` reached (SELECT, WHERE, HAVING, JOIN ON,
  ORDER BY) — the user-facing message claimed "in SELECT" only. The
  message now reads "anywhere in the query" and explains why (drift over
  time without a corresponding source mutation). Behaviour unchanged.

### Tests
- 513 lib tests (up from 503 in 1.3.0).
- New: `pg_test_topk_text_min_max`, `pg_test_topk_date_min_max`,
  `pg_test_topk_timestamp_min_max` (non-NUMERIC element types);
  `pg_test_topk_partial_heap_staleness_regression` (UPDATE-then-DELETE
  staleness minimal repro); `pg_test_topk_update_no_heap_shrink_keeps_correctness`,
  `pg_test_topk_update_mixed_shrink_groups`,
  `pg_test_topk_update_multi_column_shrink` (N1 gate correctness).

### Docs
- Operator runbook gains a LOGGED-vs-UNLOGGED decision matrix, a
  stuck-flush triage recipe (`pg_stat_activity` filter on
  `application_name = 'reflex_flush:%'` + `reflex_explain_flush`), and an
  auto-on top-K caveat.
- `docs/limitations/unsupported-shapes.md` rewritten as a three-bucket
  taxonomy (hard-rejected / supported-with-fallback / operator workaround)
  and refreshed against current behaviour. Stale "needs top-K opt-in"
  language replaced with the auto-on guarantee.
- `docs/limitations/known-issues.md` pruned: the 1.3.x top-K closed items
  moved into release notes (this CHANGELOG); only items that are still
  open or still surprising remain on the page.

## [1.3.0] - 2026-04-25

### Performance
- **Bounded top-K heap for MIN/MAX (audit R3)** — opt-in `topk` parameter on
  `create_reflex_ivm` (6th positional arg, integer K, default disabled). When
  enabled, each MIN/MAX intermediate column gains a sibling
  `__<name>_topk <type>[]` array maintained on every flush:
  - INSERT path: top-K sorted-merge of `t.topk || d.topk` truncated to K.
  - DELETE/UPDATE path: multi-set subtraction via the new
    `public.__reflex_array_subtract_multiset(anyarray, anyarray)` plpgsql
    helper; the scalar `__min_x` / `__max_x` is rebuilt from `topk[1]`.
  - Heap-underflow fallback: when the array empties, the existing scoped
    recompute (1.2.0) takes over and rebuilds both the scalar and the array
    from the source.
  Existing IMVs continue to use the scoped-recompute path with no migration
  cost. Closes the `stock_chart_*` cliff documented in
  `journal/2026-04-22_unsupported_views.md` §6 / audit R3 — the 3 IMVs there
  become eligible for incremental maintenance once operators opt in.

### Added
- **Per-IMV flush histogram (audit R6)** — `__reflex_ivm_reference` gains
  a `flush_ms_history BIGINT[]` ring buffer (size 64) populated by
  `reflex_flush_deferred`. New SPI
  `reflex_ivm_histogram(view_name) -> (p50_ms, p95_ms, p99_ms, max_ms, samples)`.
- **`pg_stat_statements` correlation** — each per-IMV flush body sets
  `application_name = 'reflex_flush:<view>'` for its duration, so operators
  with `track_application_name = on` can filter pg_stat_statements rows by
  IMV.
- **Scalar MIN/MAX (no GROUP BY)** is now a tested supported shape (audit
  unsupported §2). Two new correctness tests in `pg_test_correctness.rs`.
  With `topk=K`, scalar retraction becomes O(K) instead of O(N).

### Tests
- 503 lib tests (up from 497 in 1.2.1).
- New: 3 top-K integration tests including a 30-iteration random fuzz with
  EXCEPT ALL oracle, 2 scalar MIN/MAX tests, 2 histogram tests.

## [1.2.1] - 2026-04-25

### Added
- **`pg_reflex.alter_source_policy` GUC** — controls how the
  `reflex_on_ddl_command_end` event trigger reacts when a tracked source is
  altered. Default `'warn'` preserves 1.2.0 behaviour. Set to `'error'` to roll
  back the ALTER instead of warning (useful for change-control gates). Closes
  audit risk R2.
- **`reflex_scheduled_reconcile(max_age_minutes INTEGER DEFAULT 60)`** — SPI
  designed for pg_cron-driven drift scans. Iterates IMVs whose
  `last_update_date` is older than the threshold (or NULL), reconciles each in
  isolation, and returns `(name, status, ms)` per attempt. Closes audit risk
  R7 with a code-and-recipe approach instead of a background worker.

### Improved
- **Passthrough PK auto-detection** (audit R5) — already worked for single-source
  passthroughs; 1.2.1 adds a clearer info message when the source has a PK but
  the SELECT list does not include all PK columns, telling operators what to add.

### Tests
- 493 lib tests (up from 487 in 1.2.0).
- New: 3 alter-source-policy tests, 2 PK auto-detection tests, 2
  scheduled-reconcile tests, 3 source-drop cleanup tests.

## [1.2.0] - 2026-04-24

### Performance
- **Affected-groups-scoped MIN/MAX recompute** — `build_min_max_recompute_sql` now wraps the `orig_base_query` in a filter that restricts it to groups present in `__reflex_affected_<view>`. On retractions, only groups actually touched by the delta get re-aggregated, instead of every group in the source. For IMVs with MIN/MAX over large sources (stock_chart-style workloads), this turns a full-scan recompute into an O(delta) operation when the affected-group set is small.

### Added
- **Operational safety — per-IMV SAVEPOINT in cascade flush** — `reflex_flush_deferred` wraps each per-IMV flush body in its own `SAVEPOINT`. One bad IMV (e.g. a broken base_query after a source schema change) logs a `WARNING` and allows the cascade to continue instead of aborting every upstream update.
- **Event trigger — auto-drop on source drop** — new `reflex_on_sql_drop` event trigger (`sql_drop`). Dropping a source table now drops every artifact owned by the IMV (target, intermediate, affected-groups, delta-scratch and passthrough-scratch tables, plus the standalone trigger functions) and removes the registry row. Cascades through `graph_child` so child IMVs are cleaned up too. Closes audit risk R1.
- **Event trigger — warn on source `ALTER TABLE`** — new `reflex_on_ddl_command_end` event trigger (`ddl_command_end`, tag `ALTER TABLE`). Raises a `WARNING` suggesting `reflex_rebuild_imv` when a tracked source is altered.
- **`reflex_rebuild_imv(name)`** — public alias over `reflex_reconcile` for consistency with post-schema-change recovery guidance.
- **Observability — registry columns** — `__reflex_ivm_reference` gains `last_flush_ms`, `last_flush_rows`, `flush_count`, `last_error`. Populated by each per-IMV `SAVEPOINT` block inside `reflex_flush_deferred` (success clears `last_error`; failure records it).
- **Observability — SPIs** — `reflex_ivm_status()`, `reflex_ivm_stats(view_name)`, `reflex_explain_flush(view_name)` let operators inspect registered IMVs, their sizes, and the next-flush plan without firing a write.
- **Streaming separator for trigger bodies** — `reflex_execute_separated(sql)` #[pg_extern] consumes a `--<<REFLEX_SEP>>--`-delimited statement stream. Used by the `TRUNCATE` trigger body; INSERT/DELETE/UPDATE trigger bodies still use the `string_to_array` loop because calling an extension function from inside those trigger bodies drops transition-table scope.

### Fixed
- **Bug #10 — transitive cycle detection** in `create_reflex_ivm`. Walks existing `depends_on` edges before registering the new row; rejects circular dependencies with a clear error.
- **Bug #11 — 64-bit advisory lock keys** — `pg_advisory_xact_lock(key1, key2)` seeded from a 64-bit hash, replacing the single-`hashtext`-arg form that could collide across names.
- **Bug #7 — `resolve_column_type` silent TEXT** — emits `pgrx::warning!` on catalog-lookup failure and defaults to `NUMERIC` instead of `TEXT`. Cast errors at CREATE time are preferable to silent behaviour drift.
- **Bug #4 — reserved CTE alias collision** — `create_reflex_ivm` rejects user CTEs named `__reflex_new_<src>` / `__reflex_old_<src>` / `__reflex_delta_<src>` rather than silently corrupting rewrites.
- **Bug #13 — STRICT vs nullable `where_predicate`** — handled inside `reflex_flush_deferred` rather than at the function signature, keeping the one-arg extension API stable.

### Tests
- 485 lib tests (up from 481 in 1.1.3).
- New: 4 unit tests for the affected-groups-scoped recompute SQL shape (`test_min_max_recompute_scoped_to_affected_groups_when_provided`, `test_min_max_recompute_no_affected_filter_when_none_passed`, `test_min_max_recompute_affected_filter_uses_multiple_group_columns`, `test_min_max_recompute_skips_affected_filter_for_sentinel_plan`).

### Deferred to 1.3.0
- **MIN/MAX bounded top-K heap (`__min_X_topk`)** — originally scoped for 1.2.0; deferred after evaluating complexity-vs-payoff. The affected-groups-scoped recompute above captures the common-case win at a fraction of the code and migration cost. Top-K revisits once benchmark data shows retractions repeatedly hitting the same hot groups.
- **Lazy index maintenance on bulk rebuild** — `DROP INDEX … INSERT … CREATE INDEX` when the affected set exceeds 50 % of the intermediate. Niche payoff and risky under concurrent flushers with advisory locks; left out of 1.2.0 pending a realistic workload that benefits.

## [1.1.3] - 2026-04-22

### Performance
- **Algebraic `BOOL_OR`** — `BOOL_OR(expr)` now decomposes into two BIGINT companion columns (`__bool_or_<arg>_true_count` and `__bool_or_<arg>_nonnull_count`), both maintained with pure `SUM(+)/SUM(-)` algebra. Removes the full-scan recompute on DELETE/UPDATE. End-query maps the two counters back to boolean via a `CASE` expression that preserves Postgres `BOOL_OR` NULL semantics (`NULL` when every input was NULL, `FALSE` when at least one was non-NULL and none TRUE, `TRUE` otherwise).
- **Empty-affected DO-block gate** — the targeted `DELETE + INSERT` path for group-by IMVs is now wrapped in a `DO $$ … IF EXISTS(…) THEN … END IF; END $$` block that short-circuits when the affected-groups staging table is empty. Avoids a full target-table scan on transactions that produce no matching groups.
- **`parallel_safe` SQL-building functions** — `reflex_build_delta_sql` and `reflex_build_truncate_sql` are annotated `PARALLEL SAFE`. They read no shared state and produce deterministic SQL given identical arguments.
- **Staging-delta `ANALYZE`** — `reflex_flush_deferred` runs `ANALYZE` on the staging delta table before processing so the planner gets non-zero row estimates after the `TRUNCATE` that reset stats.
- **Per-IMV `where_predicate` registry column** — the IMV registry stores each view's `where_predicate`. Deferred UPDATE trigger bodies check the predicate against the transition table before taking the advisory lock; `reflex_flush_deferred` skips IMVs whose predicate matches no staged row. Particularly effective for sub-IMVs of a `UNION` with disjoint filters.
- **End-query targeted splice for `GROUP BY` end_queries** — `reflex_build_delta_sql` splices `AND (<gb_cols>) IN (SELECT DISTINCT <gb_cols> FROM "<affected_tbl>")` before the `GROUP BY` clause instead of falling back to a full `DELETE + INSERT … end_query`. Primary beneficiary: `COUNT(DISTINCT)` IMVs.

### Fixed
- **63-char identifier truncation** — `transition_new_table_name`, `transition_old_table_name`, and `staging_delta_table_name` now generate guaranteed-unique, ≤63-byte identifiers via a sanitize-then-truncate helper. Previously, long source names could produce colliding transition-table names across IMVs.
- **MIN/MAX / BOOL_OR recompute scalar-subquery bug** — `build_min_max_recompute_sql` wraps `orig_base_query` as `(…) AS __src` before referencing group keys. Previously the direct-column reference failed with `missing FROM-clause entry for table "alias"` on JOIN-aliased base queries.
- **Concurrent-flush advisory-lock collision** — the deferred-flush advisory-lock key now derives from a hash of `(view_name, source_table)` jointly, so two concurrent sessions flushing different IMVs on the same source don't serialize on the same integer key.

### Tests
- 481 lib tests (up from 406 in 1.1.1).

## [1.1.1] - 2026-03-29

### Added
- **FILTER clause support** — `SUM(x) FILTER (WHERE cond)`, `COUNT(*) FILTER (WHERE cond)`, `AVG(x) FILTER (WHERE cond)`, `MIN/MAX(x) FILTER (WHERE cond)`, and `BOOL_OR(x) FILTER (WHERE cond)` are now supported. Internally rewritten to `CASE WHEN` expressions, so all existing incremental maintenance (MERGE, delta, triggers) works transparently. Multiple FILTER aggregates alongside regular aggregates in the same query are supported.
- **DISTINCT ON support** — `SELECT DISTINCT ON (cols) ... ORDER BY ...` is decomposed into a passthrough sub-IMV (incrementally maintained) + a VIEW with `ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...) WHERE rn = 1`. INSERT/DELETE/UPDATE on source data is reflected instantly. Supports multiple partition columns, WHERE clause, and JOINs.

### Fixed
- **DROP CASCADE** — `drop_reflex_ivm(name, true)` now issues `DROP TABLE ... CASCADE` on target, intermediate, and affected-groups tables. Previously, cascade only dropped child IMVs in the reflex dependency graph but left external PostgreSQL objects (views, foreign keys) intact, causing the drop to fail if any existed.
- **DROP VIEW/TABLE detection** — `drop_reflex_ivm` now detects whether the target is a VIEW (window/DISTINCT ON decompositions) or TABLE and issues the correct DROP command. Previously, dropping a window-function or DISTINCT ON IMV would fail with "is not a table".

### Internal
- **Codebase restructured** — `lib.rs` reduced from 10,548 to 189 lines. Implementation split into focused modules: `create_ivm.rs` (IVM creation), `drop_ivm.rs` (drop logic), `reconcile.rs` (reconcile/refresh). Submodule tests extracted into separate files under `src/tests/`.
- **Tests reorganized** — tests split into 20 categorized files (basic, trigger, passthrough, CTE, set ops, window, drop, reconcile, deferred, error, e2e, correctness, filter, distinct_on, plus 6 unit test files).

### Tests
- 406 tests (up from 375 in v1.0.4)
- New: 7 FILTER unit tests, 9 FILTER integration tests, 5 DISTINCT ON unit tests, 9 DISTINCT ON integration tests, 1 non-SELECT rejection test

## [1.1.0] - 2026-03-29

### Fixed
- **DROP CASCADE** — `drop_reflex_ivm(name, true)` now issues `DROP TABLE ... CASCADE` on target, intermediate, and affected-groups tables.

### Internal
- **Codebase restructured** — `lib.rs` reduced from 10,548 to 189 lines. Implementation split into focused modules.
- **Tests reorganized** — tests split into categorized files under `src/tests/`.

### Tests
- 376 tests (up from 375 in v1.0.4)

## [1.0.4] - 2026-03-26

### Performance
- **Empty-delta early-exit** — triggers check if the transition table is empty before entering the IMV processing loop. Skips all Rust FFI calls, advisory locks, and MERGE generation when a statement doesn't produce relevant rows. Saves 5-15ms per trigger fire for empty deltas.
- **Predicate-filtered trigger skip** — WHERE clauses from IMV queries are stored in `__reflex_ivm_reference.where_predicate`. Before processing an IMV, the trigger evaluates the predicate against the transition table. Non-matching IMVs are skipped entirely (no advisory lock, no delta SQL). Particularly effective for UNION sub-IMVs with disjoint filters.
- **Persistent affected-groups table** — replaced per-trigger-fire `DROP TABLE + CREATE TEMP TABLE` with a persistent UNLOGGED table created at IMV setup time. Uses `TRUNCATE` (0.17ms) instead of `DROP+CREATE` (0.65ms) — 3.9x faster per trigger fire.
- **Single-pass UPDATE MERGE** — for aggregate queries without MIN/MAX, UPDATE operations use a single net-delta MERGE combining old and new transition tables, halving the MERGE count.

### Added
- **INTERSECT support** — `SELECT ... INTERSECT SELECT ...` decomposes into sub-IMVs, same pattern as UNION.
- **EXCEPT support** — `SELECT ... EXCEPT SELECT ...` decomposes into sub-IMVs.

### Tests
- 218 tests (up from 214 in v1.0.3)
- New: 2 INTERSECT tests, 2 EXCEPT tests

### Benchmarks (single-IMV, warm cache, 1M source rows)
- GROUP BY UPDATE 100 rows: **4.4ms** (vs 55ms REFRESH MATERIALIZED VIEW)
- PASSTHROUGH INSERT 1K rows: **10ms** (vs 2,500ms REFRESH — 250x faster)
- Per-IMV overhead: ~4ms warm, scales linearly with number of IMVs on same source

## [1.0.3] - 2026-03-26

### Added
- **WINDOW function support** — queries with `ROW_NUMBER()`, `RANK()`, `DENSE_RANK()`, `LAG()`, `LEAD()`, `SUM() OVER (...)`, and any other PostgreSQL window function are now supported. Decomposed into a base sub-IMV (incrementally maintained) + a VIEW that applies window functions at read time. For GROUP BY + WINDOW, the VIEW scans only the small intermediate result (one row per group).
- **UNION ALL / UNION support** — set operations are decomposed into per-operand sub-IMVs. `UNION ALL` creates a zero-overhead VIEW over the sub-IMV targets. `UNION` (dedup) creates a VIEW with PostgreSQL's native deduplication. Supports 2+ operands, aggregates in operands, and mixed WHERE filters on the same source table.
- **`storage` parameter** — `create_reflex_ivm('v', 'SELECT ...', NULL, 'LOGGED')` creates WAL-logged tables for crash safety. Default: `'UNLOGGED'` (current behavior). Propagated to CTE sub-IMVs and UNION sub-IMVs.
- **`mode` parameter** — `create_reflex_ivm('v', 'SELECT ...', NULL, 'UNLOGGED', 'DEFERRED')` accumulates deltas during the transaction and flushes at COMMIT via a two-stage trigger design (immediate capture to staging table + deferred constraint trigger). Default: `'IMMEDIATE'` (current behavior).
- **Materialized view auto-refresh** — event trigger on `ddl_command_end` automatically cascades `REFRESH MATERIALIZED VIEW` to dependent pg_reflex IMVs. No manual `refresh_imv_depending_on()` needed.
- New `window.rs` module for window function query decomposition
- `reflex_flush_deferred(source_table)` function for manual deferred delta processing

### Performance
- **Single-pass UPDATE MERGE** — for aggregate queries without MIN/MAX, UPDATE operations now use a single net-delta MERGE (combining old and new transition tables) instead of two separate MERGEs. Reduces MERGE count by 50% for UPDATE operations.

### Migration
- New columns in `__reflex_ivm_reference`: `storage_mode` (default `'UNLOGGED'`), `refresh_mode` (default `'IMMEDIATE'`). Existing IMVs backfilled automatically.
- Deferred processing infrastructure: `__reflex_deferred_pending` table + constraint trigger created automatically.
- Materialized view event trigger installed automatically.
- Migration is automatic via `ALTER EXTENSION pg_reflex UPDATE`.

### Tests
- 214 tests (up from 172 in v1.0.2)
- New test coverage: 9 UNION ALL tests, 5 UNION dedup tests, 18 WINDOW function tests, 5 LOGGED mode tests, 3 DEFERRED mode tests

### API
```sql
-- Full signature (all params have defaults, backward-compatible)
SELECT create_reflex_ivm(
    'view_name',                -- TEXT: view name
    'SELECT ...',               -- TEXT: query
    NULL,                       -- TEXT: unique_columns (optional)
    'UNLOGGED',                 -- TEXT: storage mode ('LOGGED' or 'UNLOGGED')
    'IMMEDIATE'                 -- TEXT: refresh mode ('IMMEDIATE' or 'DEFERRED')
);
```

## [1.0.2] - 2026-03-24

### Performance
- **UNLOGGED target table** — target tables are now `UNLOGGED` (matching intermediate tables). Eliminates WAL writes on every targeted refresh (DELETE+INSERT), reducing write overhead. Crash recovery already required `reflex_reconcile()` due to the UNLOGGED intermediate, so this adds zero additional risk.
- **Hash index on intermediate** — single-column GROUP BY keys now use a hash index instead of a B-tree primary key for O(1) MERGE lookups (~30% faster MERGE for single-column groups). Multi-column GROUP BY falls back to B-tree (hash doesn't support multi-column in PostgreSQL). The B-tree PK is removed because MERGE handles insert-or-update correctly and advisory locks prevent concurrent modifications.
- **MERGE RETURNING** — the delta query now runs once per trigger fire instead of twice. The MERGE into intermediate uses `RETURNING` in a CTE to capture affected group keys, eliminating the separate `SELECT DISTINCT groups FROM (delta_query)` statement. For UPDATE operations, delta_old and delta_new each run once instead of twice (4 → 2 executions).

### Benchmarks (100K groups, 1M source, single-column GROUP BY)
- INSERT 10K: 236ms → 171ms (**28% faster**)
- INSERT 50K: 1,170ms → 865ms (**26% faster**)
- INSERT 100K: 2,298ms → 1,802ms (**22% faster**)

### Migration
- Existing aggregate IMVs: intermediate PK dropped and replaced with hash/B-tree index, target table converted to UNLOGGED. Migration is automatic via `ALTER EXTENSION pg_reflex UPDATE`.
- Existing passthrough IMVs: target table converted to UNLOGGED.

### Tests
- 172 tests (unchanged from v1.0.1, all passing)

## [1.0.1] - 2026-03-23

### Added
- **`bool_or(expr)` aggregate** — incremental via OR on INSERT, recomputes from source on DELETE (same pattern as MIN/MAX)
- **Cast propagation** — `SUM(x)::BIGINT` now produces a BIGINT column in the target table (cast applied in end query, intermediate still stores NUMERIC for precision)
- **Target table index** — composite index on group columns for faster targeted refresh DELETE performance
- **Unsupported aggregate warnings** — unrecognized aggregates (e.g., `string_agg`) now emit a WARNING instead of being silently dropped
- Materialized view support as source tables (triggers auto-skipped, warning emitted)
- `refresh_reflex_imv(view_name)` — refresh a single IMV (alias for `reflex_reconcile`)
- `refresh_imv_depending_on(source)` — refresh all IMVs depending on a source table or materialized view
- HAVING clause support with AST-based rewriting (handles complex expressions like `AVG(x) > COUNT(*)`)
- Auto-detection of HAVING aggregates not in SELECT list (added to intermediate table automatically)
- Incremental passthrough DELETE/UPDATE (O(delta) row-matching instead of O(N) full refresh)
- Multi-level cascade confirmed and tested (works to arbitrary depth)
- CTE passthrough support (passthrough CTEs become sub-IMV tables)
- `create_reflex_ivm_if_not_exists(name, sql)` / `create_reflex_ivm_if_not_exists(name, sql, unique_columns)` — idempotent IMV creation that returns a notice instead of an error if the view already exists
- `install.sh` wrapper script — copies migration files alongside `cargo pgrx install`
- Subquery warning — subqueries in FROM now emit an informational warning (like materialized views)

### Fixed
- **Trigger table reference replacement** — schema-qualified tables with column qualifiers (e.g., `sales_simulation.product_id` from `alp.sales_simulation`) now work correctly in triggers. Previously caused `missing FROM-clause entry` on every INSERT/UPDATE/DELETE.
- **Cast expressions no longer silently dropped** — `SUM(x)::BIGINT` is now correctly detected as an aggregate. Previously, the cast wrapper hid the function from the aggregate detector.
- **Column name case normalization** — unquoted identifiers like `MONTH` are now lowercased consistently (matching PostgreSQL's case folding), preventing `column "MONTH" does not exist` errors at trigger time.
- **Source index creation** — index creation on source tables for MIN/MAX/BOOL_OR recompute now checks column existence first, so it no longer fails when group columns come from JOIN tables.
- Materialized views no longer cause "cannot have triggers" error
- Passthrough DELETE/UPDATE no longer does full table refresh
- **Passthrough JOIN key mapping** — unique key detection for passthrough JOINs now uses per-source-table column mappings derived from JOIN conditions. Previously, DELETE/UPDATE triggers on secondary tables (e.g., `products` in a `sales JOIN products` query) could corrupt data by matching the wrong column. Auto-detection is now restricted to single-source queries; JOINs require the explicit 3rd argument.
- Dropped PostgreSQL 13/14 from supported versions (MERGE statement requires PG15+)
- **BOOL_OR recompute on DELETE** — the recompute SQL was generated but never executed because the guard condition only checked for MIN/MAX, not BOOL_OR. Now fixed.
- **Subqueries with aggregation in FROM** — now rejected at creation time with a clear error suggesting CTE as the alternative (pg_reflex decomposes CTEs into sub-IMVs automatically). Previously, these silently produced incorrect results because the trigger replaced the inner table with the transition table, making the inner aggregation see only delta rows.

### Performance
- **Deferred index creation** — indexes on intermediate and target tables are now created after bulk data insertion (not before), reducing IMV creation time by ~60% on large datasets
- **Faster `reflex_reconcile`** — drops all indexes (including user-created) before bulk rebuild, recreates them after. Saves index DDL and restores it faithfully. Reduced reconcile time by ~38% on large datasets (6:29 → 4:00 on 7.7M rows). Also uses TRUNCATE instead of DELETE for instant table clearing.
- **ANALYZE** — intermediate and target tables are analyzed after initial materialization and after reconcile for better query planner statistics

### Tests
- 172 tests (up from 138 in v1.0.0) covering BOOL_OR, LEFT/RIGHT JOIN, cast propagation, subqueries, passthrough JOINs with per-source key mapping, chained IMVs with passthrough layers, and multiple mixed IMVs on same source

## [1.0.0] - 2026-03-22

### Added
- `drop_reflex_ivm(view_name)` and `drop_reflex_ivm(view_name, cascade)` for removing IMVs and all artifacts
- `reflex_reconcile(view_name)` for rebuilding IMVs from source data
- TRUNCATE trigger support (clears intermediate and target on source TRUNCATE)
- Targeted group refresh (only affected groups re-materialized, not the full target table)
- CTE decomposition (each CTE becomes a sub-IMV, passthrough outer queries become VIEWs)
- Passthrough CTE support (CTEs without aggregation work as passthrough sub-IMVs)
- MERGE-based delta processing (replaces INSERT ON CONFLICT for better performance)
- Schema-qualified view names (`myschema.my_view`) — views, intermediate tables, and triggers are created in the correct schema
- View name validation (rejects names with special characters to prevent SQL injection)
- Duplicate view name detection (returns error instead of crashing)
- PostgreSQL logging for key operations (`info!` on create/drop/reconcile, `warning!` on errors)
- GitHub Actions CI testing on PostgreSQL 15, 17, 18
- Automated release workflow with `.deb` package builds on version tags
- Concurrent operation test suite (parallel psql sessions)
- Property-based testing with proptest for input validation and query decomposition
- Multi-run benchmark harness (`benchmarks/run_bench.sh`) with variance reporting
- Deterministic benchmark seeds (`setseed`) for reproducible results
- 138 tests (63 unit + 7 proptest + 68 integration) covering all aggregate types, CTEs, JOINs, schema support, cascading, and edge cases
- 17 SQL benchmark scripts covering scales from 1K to 5M rows
- Apache 2.0 license

### Fixed
- SQL parser no longer panics on malformed input (returns error string instead of crashing PostgreSQL backend)
- SQL injection vectors eliminated via parameterized queries and input validation
- Catalog queries (`information_schema.columns`) now use parameterized queries
- Passthrough DELETE/UPDATE now incremental (O(delta) row-matching instead of O(N) full refresh)
- Multi-level cascade propagation works automatically to arbitrary depth (was incorrectly listed as a limitation)

### Supported
- PostgreSQL 15, 16, 17, 18 (requires MERGE statement, PG15+)
- Aggregates: SUM, COUNT, COUNT(*), AVG, MIN, MAX, BOOL_OR
- DISTINCT, GROUP BY, WHERE, INNER/LEFT/RIGHT JOIN
- Non-recursive CTEs (decomposed into sub-IMVs)
- Multi-level IMV cascading (A → B → C, tested up to 4 levels)
- Schema-qualified view names and source tables

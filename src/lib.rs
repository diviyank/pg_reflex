use pgrx::prelude::*;

// Stub archive for `cargo test --lib` on Linux.
//
// The test binary links natively and pgrx_pg_sys drags in unresolved refs
// to postgres server symbols (errstart, palloc0, CurrentMemoryContext, ...)
// that only exist when the .so is loaded into postgres. build.rs builds a
// static archive of weak stubs to satisfy these references. The `#[link]`
// directive below is scoped to `cfg(test)` so the archive is ONLY pulled in
// by the test binary — the cdylib postgres actually dlopens stays free of
// stub variables that would otherwise shadow PG's real globals and segfault
// every SPI call (observed on PG 17.7 as SIGSEGV / SIGABRT under cassert).
#[cfg(all(test, target_os = "linux"))]
#[link(name = "pg_reflex_pg_stubs", kind = "static")]
unsafe extern "C" {}

// macOS: same stub archive, but force-loaded. Under -Wl,-undefined,
// dynamic_lookup the linker would otherwise defer the (undefined) data
// globals to runtime flat-namespace lookup and never pull the weak
// definitions from the archive — so the standalone test binary aborts at
// load with `symbol not found in flat namespace '_CacheMemoryContext'`.
// `+whole-archive` includes every stub object unconditionally; cfg(test)
// keeps it out of the cdylib postgres dlopens.
#[cfg(all(test, target_os = "macos"))]
#[link(
    name = "pg_reflex_pg_stubs",
    kind = "static",
    modifiers = "+whole-archive"
)]
unsafe extern "C" {}

mod aggregation;
mod audit;
mod create_ivm;
mod doctor;
mod drop_ivm;
mod graph_repair;
mod introspect;
mod partition;
mod query_decomposer;
mod reconcile;
mod schema_builder;
mod sql_analyzer;
mod sql_writer;
mod trigger;
mod window;

::pgrx::pg_module_magic!(name, version);

/// Machine-recognizable tag embedded in every *deliberate* rejection message
/// returned by `create_reflex_ivm_impl`. The differential fuzz harness uses it
/// to distinguish an intended limitation (function returns this string) from a
/// codegen defect (Postgres raises an exception on generated SQL). Inserted
/// right after the `ERROR: ` prefix so existing `.starts_with("ERROR")` and
/// message-substring assertions keep passing.
pub(crate) const REFLEX_UNSUPPORTED_TAG: &str = "[reflex-unsupported]";

/// Format a deliberate-rejection message: `ERROR: [reflex-unsupported] <msg>`.
/// Returns a leaked `&'static str` to match the existing rejection return type.
pub(crate) fn reflex_reject(msg: &str) -> &'static str {
    Box::leak(format!("ERROR: {} {}", REFLEX_UNSUPPORTED_TAG, msg).into_boxed_str())
}

// This SQL will be executed exactly once when 'CREATE EXTENSION' is run.
// Collate "C" for faster lookups
extension_sql!(
    r#"
    -- Top-K (1.3.0): multi-set subtraction over arrays. Removes one occurrence
    -- of each value in `remove` from `arr`, preserving multiplicity.
    -- Used by trigger.rs MERGE codegen when retracting from top-K MIN/MAX heaps.
    --
    -- Implementation note: PL/pgSQL forbids declaring local variables of
    -- pseudo-type `anyarray` / `anyelement`, so we mutate the resolved-type
    -- input parameter `arr` directly (allowed: parameters have concrete
    -- runtime types) and index into `remove` by position.
    CREATE OR REPLACE FUNCTION public.__reflex_array_subtract_multiset(
        arr anyarray, remove anyarray
    ) RETURNS anyarray
    LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE AS $REFLEX$
    DECLARE
        i INT;
        pos INT;
    BEGIN
        IF arr IS NULL THEN RETURN NULL; END IF;
        IF remove IS NULL THEN RETURN arr; END IF;
        FOR i IN 1..COALESCE(cardinality(remove), 0) LOOP
            pos := array_position(arr, remove[i]);
            IF pos IS NOT NULL THEN
                arr := arr[1:pos-1] || arr[pos+1:];
            END IF;
        END LOOP;
        RETURN arr;
    END;
    $REFLEX$;

    CREATE TABLE IF NOT EXISTS public.__reflex_ivm_reference (
        name TEXT PRIMARY KEY COLLATE "C",
        graph_depth INT NOT NULL,
        depends_on TEXT[],
        depends_on_imv TEXT[],
        unlogged_tables TEXT[],
        graph_child TEXT[],
        sql_query TEXT,
        base_query TEXT,
        end_query TEXT,
        parsed_sql_query JSON,
        aggregations JSONB,
        index_columns TEXT[],
        unique_columns TEXT[],
        enabled BOOLEAN DEFAULT TRUE,
        last_update_date TIMESTAMP,
        storage_mode TEXT DEFAULT 'UNLOGGED',
        refresh_mode TEXT DEFAULT 'IMMEDIATE',
        where_predicate TEXT,
        last_flush_ms BIGINT,
        last_flush_rows BIGINT,
        flush_count BIGINT DEFAULT 0,
        last_error TEXT,
        flush_ms_history BIGINT[] DEFAULT ARRAY[]::BIGINT[],
        ignored_sources TEXT[] DEFAULT ARRAY[]::TEXT[],
        -- 1.4.6 — per-IMV override of reflex.wipe_threshold (NULL = inherit
        -- from session GUC, falls back to compiled default WIPE_THRESHOLD_DEFAULT).
        -- Set via public.reflex_set_wipe_threshold(name, value). Operators
        -- tune this when one IMV's shape diverges from the global default —
        -- e.g. small-row-count IMVs where reconcile takes < 1 s and a low
        -- threshold (0.10) accelerates moderate bulk operations.
        wipe_threshold NUMERIC,
        -- 1.7.1 — schema the IMV's objects (target + aux tables) were created
        -- in, captured from current_schema() at create time for bare names.
        -- drop_reflex_ivm reuses it to qualify teardown DDL so cleanup no longer
        -- depends on the session search_path at drop time. NULL for legacy rows
        -- (drop falls back to search_path resolution, preserving old behavior).
        target_schema TEXT,
        -- 1.7.5 — TRUE for an ungrouped aggregate sub-IMV (aggregate with empty
        -- GROUP BY → at most one row). Read by JOIN unique-key inference so a
        -- CROSS JOIN to such a relation (e.g. a single-row history_bounds CTE)
        -- is classified to-one. NULL/FALSE for everything else.
        max_one_row BOOLEAN DEFAULT FALSE,
        -- 1.8.2 — IMV partition mirror depth: how many source partition
        -- levels this IMV mirrors. NULL = mirror the FULL source depth
        -- (legacy/default behavior). Set by resolve_partitioning when the
        -- IMV is shallower than its source (explicit partition_by or
        -- auto-prune). See plans/2026-06-03-imv-partition-depth.md.
        partition_depth INT
    );

    -- Index on name for fast lookups
    CREATE INDEX IF NOT EXISTS idx__reflex_ivm_name ON public.__reflex_ivm_reference(name);

    -- Partitioning (plans/partitioning_2.md). Opt-in: NULL/empty means the
    -- IMV is unpartitioned (legacy behaviour, byte-for-byte). When set, the
    -- intermediate + target tables are created as declarative partitioned
    -- tables on these columns, with one child per source partition.
    -- partition_strategy is 'LIST' or 'RANGE'. Bounds are NEVER cached —
    -- they're looked up live from the anchor source via pg_inherits +
    -- relpartbound so we cannot drift.
    ALTER TABLE public.__reflex_ivm_reference
        ADD COLUMN IF NOT EXISTS partition_columns TEXT[];
    ALTER TABLE public.__reflex_ivm_reference
        ADD COLUMN IF NOT EXISTS partition_strategy TEXT;

    -- 1.6.0 (plans/partitioning_3.md §3): per-IMV floor for the per-partition
    -- denominator in the trigger-time dispatch ratio.  `dirty / GREATEST(
    -- reltuples, wipe_floor_rows) >= wipe_threshold` is the per-partition
    -- decision.  Without a floor a partition with reltuples=0 (brand-new or
    -- never-ANALYZE'd) trips the dispatch at any non-zero |dirty|.  NULL =
    -- inherit GUC `reflex.wipe_floor_rows` → compiled default (1000).
    ALTER TABLE public.__reflex_ivm_reference
        ADD COLUMN IF NOT EXISTS wipe_floor_rows BIGINT;

    -- 1.6.0 (plans/partitioning_3.md §4): Tier 2 partition-derivation cost
    -- cap.  When the trigger fires on a non-anchor (JOIN-secondary) source
    -- and would JOIN to the anchor to derive partition keys, EXPLAIN the
    -- JOIN first; if the planner estimates more than this many rows, skip
    -- the per-partition dispatch and fall through to global Path B.  NULL =
    -- inherit GUC `reflex.partition_dispatch_cost_cap` → compiled default
    -- (100000).
    ALTER TABLE public.__reflex_ivm_reference
        ADD COLUMN IF NOT EXISTS partition_dispatch_cost_cap BIGINT;

    ALTER TABLE public.__reflex_ivm_reference
        ADD COLUMN IF NOT EXISTS max_one_row BOOLEAN DEFAULT FALSE;

    ALTER TABLE public.__reflex_ivm_reference
        ADD COLUMN IF NOT EXISTS partition_depth INT;

    ALTER TABLE public.__reflex_ivm_reference
        ADD COLUMN IF NOT EXISTS known_stale BOOLEAN NOT NULL DEFAULT FALSE;
    ALTER TABLE public.__reflex_ivm_reference
        ADD COLUMN IF NOT EXISTS stale_reason TEXT;
    ALTER TABLE public.__reflex_ivm_reference
        ADD COLUMN IF NOT EXISTS stale_since TIMESTAMPTZ;

    -- 1.11.0 (PS-3): TRUE when every real source of this node is a materialized
    -- view. Such a node cannot self-maintain (PG fires no trigger on a matview),
    -- so it is a snapshot frozen at create time and needs an explicit
    -- refresh_imv_depending_on('<mv>') after each REFRESH MATERIALIZED VIEW.
    -- PERMANENT and structural — distinct from known_stale (which means "a flush
    -- failed"), and NEVER cleared by reconcile or the doctor's verify_stale_cleared
    -- authority. Surfaced by reflex_ivm_status and as reflex_doctor finding F12.
    ALTER TABLE public.__reflex_ivm_reference
        ADD COLUMN IF NOT EXISTS requires_explicit_refresh BOOLEAN NOT NULL DEFAULT FALSE;

    -- 1.11.1 (PS-14): repeat-call visibility for targeted recovery. Bumped ONLY
    -- by the targeted-recovery entry points (reflex_rebuild_imv / reflex_reconcile
    -- invoked directly on one IMV, outside a trigger) — never by the internal
    -- recursive/cascade reconcile_one descent, or the count would be meaningless.
    -- A non-converging retry loop (field: 1020 reflex_rebuild_imv calls in 3.5 h
    -- on a partitioned ignore_sources IMV) becomes observable in-database via
    -- reflex_ivm_status instead of only in pg_stat_statements.
    ALTER TABLE public.__reflex_ivm_reference
        ADD COLUMN IF NOT EXISTS rebuild_count BIGINT NOT NULL DEFAULT 0;
    ALTER TABLE public.__reflex_ivm_reference
        ADD COLUMN IF NOT EXISTS last_rebuild_at TIMESTAMPTZ;

    -- 1.10.8: JSON object capturing creation-time arguments (unique_columns,
    -- storage_mode, refresh_mode, topk_k, ignore_sources, partition_by,
    -- explicit_unpartitioned) for faithful IMV chain reconstruction via
    -- reflex_rebuild_chain. NULL for legacy rows; new rows populated at
    -- create time to enable transparent rebuild from stored specs.
    ALTER TABLE public.__reflex_ivm_reference
        ADD COLUMN IF NOT EXISTS create_args TEXT;

    -- 1.11.0 (PS-1): TRUE when pg_reflex itself created this node while
    -- decomposing a single user create_reflex_ivm call — a CTE sub-IMV
    -- (`__cte_<alias>`), a set-op operand (`__union_<i>`), or a DISTINCT-ON /
    -- window base (`__base`). reflex_reconcile recurses into these and only
    -- these: a user-declared IMV dependency is someone else's object and
    -- reconciling it is not this call's business. Recorded explicitly rather
    -- than inferred from the name prefix, which a user IMV literally named
    -- `foo__cte_bar` would defeat.
    ALTER TABLE public.__reflex_ivm_reference
        ADD COLUMN IF NOT EXISTS is_generated_sub_imv BOOLEAN NOT NULL DEFAULT FALSE;

    -- Multi-level partition capture (plans/sub_partitioning.md). Snapshot of
    -- each tracked source root's recursive LEAF set, keyed by (root, child).
    -- reflex_flush_partitions oid-diffs the live leaf set against this to
    -- classify attach (new) / swap (oid changed) / detach (gone).
    CREATE TABLE IF NOT EXISTS public.__reflex_source_partition_snapshot (
        source_root TEXT NOT NULL,
        child_name  TEXT NOT NULL,
        child_oid   BIGINT NOT NULL,
        bound       TEXT,
        ancestors   TEXT[],
        PRIMARY KEY (source_root, child_name)
    );

    ALTER TABLE public.__reflex_source_partition_snapshot
        ADD COLUMN IF NOT EXISTS ancestors TEXT[];

    -- Roots enqueued by the DDL event trigger when a source partition is
    -- attached/detached; drained by reflex_flush_partitions.
    CREATE TABLE IF NOT EXISTS public.__reflex_partition_pending (
        source_root TEXT NOT NULL,
        enqueued_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (source_root)
    );

    ALTER TABLE public.__reflex_partition_pending
        ADD COLUMN IF NOT EXISTS attempts INT NOT NULL DEFAULT 0;
    ALTER TABLE public.__reflex_partition_pending
        ADD COLUMN IF NOT EXISTS last_error TEXT;
    ALTER TABLE public.__reflex_partition_pending
        ADD COLUMN IF NOT EXISTS failures INT NOT NULL DEFAULT 0;
    -- 1.11.0: stamped by the drain, so a pending row's age reflects the last
    -- flush attempt rather than the last enqueue (which every ATTACH resets).
    -- NULL means no drain has ever fired for this row — the F1 re-arm hole.
    ALTER TABLE public.__reflex_partition_pending
        ADD COLUMN IF NOT EXISTS last_attempt_at TIMESTAMPTZ;

    -- 1.6.0: SQL helper used by the per-partition dispatch DO block emitted
    -- by build_partition_aware_dispatch_sql.  Given a partitioned parent +
    -- partition column name + a single text-form key, returns the regclass
    -- of the child whose constraint covers the key (NULL if none).
    --
    -- Implementation: walk the parent's children via pg_inherits, evaluate
    -- each child's `pg_get_partition_constraintdef` boolean expression
    -- with the partition column substituted by the literal key.  Same
    -- shape used inline by reflex_reconcile_partition (Rust path) — kept
    -- as a SQL helper so the trigger-time dispatch can reuse it without
    -- per-fire SPI round-trips through reflex_build_delta_sql.
    --
    -- LIST: works directly (constraint is `col = ANY(ARRAY['A','B'])`).
    -- RANGE: works for single-column RANGE (constraint is a < /<= test).
    -- Multi-column partition keys: NOT supported (single-key v1 limit).
    CREATE OR REPLACE FUNCTION public.__reflex_partition_child_for_key(
        parent regclass, part_col TEXT, k TEXT
    ) RETURNS regclass
    LANGUAGE plpgsql STABLE AS $REFLEX$
    DECLARE
        _r RECORD;
        _expr TEXT;
        _match BOOLEAN;
        _ident_re TEXT;
    BEGIN
        IF parent IS NULL OR part_col IS NULL OR k IS NULL THEN
            RETURN NULL;
        END IF;
        _ident_re := '\m(?:' || regexp_replace(part_col, '([\\.+*?^$()\[\]{}|])', '\\\1', 'g')
                     || ')\M';
        FOR _r IN
            SELECT c.oid::regclass AS rc,
                   pg_get_partition_constraintdef(c.oid) AS def
            FROM pg_inherits i
            JOIN pg_class c ON c.oid = i.inhrelid
            WHERE i.inhparent = parent
        LOOP
            IF _r.def IS NULL OR _r.def = '' THEN CONTINUE; END IF;
            _expr := regexp_replace(_r.def, _ident_re, quote_literal(k), 'gi');
            BEGIN
                EXECUTE 'SELECT (' || _expr || ')::boolean' INTO _match;
            EXCEPTION WHEN OTHERS THEN
                _match := FALSE;
            END;
            IF _match THEN
                RETURN _r.rc;
            END IF;
        END LOOP;
        RETURN NULL;
    END;
    $REFLEX$;

    -- Helper for reflex_doctor repairs: execute SQL in a subtransaction and return outcome.
    -- CONTRACT: `_sql` MUST be a statement that returns a value (every caller
    -- passes `SELECT reflex_*(...)`). `EXECUTE ... INTO` cannot run a statement that
    -- returns no data, so a bare DDL/DML repair yields
    -- 'failed:INTO used with a command that cannot return data'. That degrades
    -- safely, but a future executable repair (e.g. F9's DROP ... CASCADE) must be
    -- wrapped so it returns something, or this helper must be extended first.
    -- Returns 'fixed' only when the statement neither raised nor RETURNED an
    -- 'ERROR: …' string. reflex_reconcile, reflex_sync_partitions and
    -- drop_reflex_ivm all signal some failures by returning that text rather than
    -- raising, so discarding the result reported those repairs as successful —
    -- the outcome an operator can least afford to be lied to about.
    -- The EXCEPTION block acts as a savepoint: failing repairs rollback only themselves,
    -- not the outer reflex_doctor transaction, ensuring isolation.
    CREATE OR REPLACE FUNCTION public.__reflex_doctor_try_repair(_sql TEXT)
    RETURNS TEXT LANGUAGE plpgsql AS $fn$
    DECLARE
        _res TEXT;
    BEGIN
        EXECUTE _sql INTO _res;
        IF _res IS NOT NULL AND upper(_res) LIKE 'ERROR%' THEN
            RETURN 'failed:' || left(_res, 400);
        END IF;
        RETURN 'fixed';
    EXCEPTION WHEN OTHERS THEN
        RETURN 'failed:' || left(SQLERRM, 400);
    END;
    $fn$;
    "#,
    name = "pg_reflex_init",
);

/// Validates that a view name contains only safe characters.
/// Allows: ASCII letters, digits, underscore, period (for schema qualification).
/// Rejects everything else (quotes, semicolons, whitespace, etc.).
fn validate_view_name(name: &str) -> Result<(), &'static str> {
    if name.is_empty() {
        return Err("ERROR: Invalid view name: name is empty");
    }
    if name.starts_with('.') || name.chars().next().is_some_and(|c| c.is_ascii_digit()) {
        return Err("ERROR: Invalid view name: must start with a letter or underscore");
    }
    if name.contains("..") || name.ends_with('.') {
        return Err("ERROR: Invalid view name: invalid period placement");
    }
    for ch in name.chars() {
        if !(ch.is_ascii_alphanumeric() || ch == '_' || ch == '.') {
            return Err(
                "ERROR: Invalid view name: only alphanumeric, underscore, and period allowed",
            );
        }
    }
    Ok(())
}

/// Default top-K heap size auto-applied to every MIN/MAX intermediate column
/// when the operator does not pass an explicit `topk` argument. Reflex
/// detects MIN/MAX presence in the IMV's plan; the parameter is a no-op for
/// SUM / COUNT / AVG / BOOL_OR aggregations. K=16 matches the value used in
/// the 1.3.0 landing benchmark and trades ~2.5× INSERT overhead for ~5-6×
/// faster retractions on MIN/MAX IMVs. Operators on append-only MIN/MAX
/// workloads can opt out by passing `topk = 0` to the 6-arg overload.
const DEFAULT_TOPK_K: usize = 16;

/// Parse a comma-separated source list into a Vec<String>. Empty input → empty vec.
/// Both schema-qualified ("alp.product") and bare ("product") names are accepted.
fn parse_ignore_sources_list(s: &str) -> Vec<String> {
    s.split(',')
        .map(|p| p.trim().to_string())
        .filter(|p| !p.is_empty())
        .collect()
}

/// Create an IMV. `ignore_sources` is a comma-separated TEXT list of source
/// tables to exclude from trigger installation; DML on those sources will
/// NOT refresh this IMV (use `reflex_reconcile` or periodic refresh
/// instead). Both schema-qualified ('alp.product') and bare ('product')
/// names are accepted — use the form that appears in the IMV's SQL.
/// The list is persisted in `__reflex_ivm_reference.ignored_sources` so
/// triggers installed by sibling IMVs also skip this IMV when fired by
/// an ignored source.
#[pg_extern]
fn create_reflex_ivm(
    view_name: &str,
    sql: &str,
    unique_columns: default!(Option<&str>, "NULL"),
    storage: default!(Option<&str>, "'UNLOGGED'"),
    mode: default!(Option<&str>, "'IMMEDIATE'"),
    ignore_sources: default!(Option<&str>, "NULL"),
) -> &'static str {
    let ignore_vec = parse_ignore_sources_list(ignore_sources.unwrap_or(""));
    create_ivm::create_reflex_ivm_impl(
        view_name,
        sql,
        unique_columns.unwrap_or(""),
        false,
        storage.unwrap_or("UNLOGGED"),
        mode.unwrap_or("IMMEDIATE"),
        Some(DEFAULT_TOPK_K),
        &ignore_vec,
        &[],
        false,
    )
}

/// Partitioned overload of `create_reflex_ivm`. `partition_by` is the
/// list of OUTPUT column names to partition on (must be ⊆ GROUP BY for
/// aggregate IMVs); strategy + bounds are derived from the anchor source's
/// partition descriptor.
#[pg_extern(name = "create_reflex_ivm")]
fn create_reflex_ivm_partitioned(
    view_name: &str,
    sql: &str,
    unique_columns: Option<&str>,
    storage: Option<&str>,
    mode: Option<&str>,
    ignore_sources: Option<&str>,
    partition_by: Vec<Option<String>>,
) -> &'static str {
    let ignore_vec = parse_ignore_sources_list(ignore_sources.unwrap_or(""));
    let part_cols = partition::parse_partition_by_input(Some(partition_by));
    create_ivm::create_reflex_ivm_impl(
        view_name,
        sql,
        unique_columns.unwrap_or(""),
        false,
        storage.unwrap_or("UNLOGGED"),
        mode.unwrap_or("IMMEDIATE"),
        Some(DEFAULT_TOPK_K),
        &ignore_vec,
        &part_cols,
        // empty partition_by (explicit) => force unpartitioned on a partitioned source
        part_cols.is_empty(),
    )
}

#[pg_extern(name = "create_reflex_ivm")]
fn create_reflex_ivm_with_topk(
    view_name: &str,
    sql: &str,
    unique_columns: Option<&str>,
    storage: Option<&str>,
    mode: Option<&str>,
    topk: i32,
    ignore_sources: default!(Option<&str>, "NULL"),
) -> &'static str {
    let ignore_vec = parse_ignore_sources_list(ignore_sources.unwrap_or(""));
    create_ivm::create_reflex_ivm_impl(
        view_name,
        sql,
        unique_columns.unwrap_or(""),
        false,
        storage.unwrap_or("UNLOGGED"),
        mode.unwrap_or("IMMEDIATE"),
        if topk > 0 { Some(topk as usize) } else { None },
        &ignore_vec,
        &[],
        false,
    )
}

#[pg_extern(name = "create_reflex_ivm")]
#[allow(clippy::too_many_arguments)]
fn create_reflex_ivm_with_topk_partitioned(
    view_name: &str,
    sql: &str,
    unique_columns: Option<&str>,
    storage: Option<&str>,
    mode: Option<&str>,
    topk: i32,
    ignore_sources: Option<&str>,
    partition_by: Vec<Option<String>>,
) -> &'static str {
    let ignore_vec = parse_ignore_sources_list(ignore_sources.unwrap_or(""));
    let part_cols = partition::parse_partition_by_input(Some(partition_by));
    create_ivm::create_reflex_ivm_impl(
        view_name,
        sql,
        unique_columns.unwrap_or(""),
        false,
        storage.unwrap_or("UNLOGGED"),
        mode.unwrap_or("IMMEDIATE"),
        if topk > 0 { Some(topk as usize) } else { None },
        &ignore_vec,
        &part_cols,
        // empty partition_by (explicit) => force unpartitioned on a partitioned source
        part_cols.is_empty(),
    )
}

#[pg_extern]
fn create_reflex_ivm_if_not_exists(
    view_name: &str,
    sql: &str,
    unique_columns: default!(Option<&str>, "NULL"),
    storage: default!(Option<&str>, "'UNLOGGED'"),
    mode: default!(Option<&str>, "'IMMEDIATE'"),
    ignore_sources: default!(Option<&str>, "NULL"),
) -> &'static str {
    let ignore_vec = parse_ignore_sources_list(ignore_sources.unwrap_or(""));
    create_ivm::create_reflex_ivm_impl(
        view_name,
        sql,
        unique_columns.unwrap_or(""),
        true,
        storage.unwrap_or("UNLOGGED"),
        mode.unwrap_or("IMMEDIATE"),
        Some(DEFAULT_TOPK_K),
        &ignore_vec,
        &[],
        false,
    )
}

#[pg_extern(name = "create_reflex_ivm_if_not_exists")]
fn create_reflex_ivm_if_not_exists_partitioned(
    view_name: &str,
    sql: &str,
    unique_columns: Option<&str>,
    storage: Option<&str>,
    mode: Option<&str>,
    ignore_sources: Option<&str>,
    partition_by: Vec<Option<String>>,
) -> &'static str {
    let ignore_vec = parse_ignore_sources_list(ignore_sources.unwrap_or(""));
    let part_cols = partition::parse_partition_by_input(Some(partition_by));
    create_ivm::create_reflex_ivm_impl(
        view_name,
        sql,
        unique_columns.unwrap_or(""),
        true,
        storage.unwrap_or("UNLOGGED"),
        mode.unwrap_or("IMMEDIATE"),
        Some(DEFAULT_TOPK_K),
        &ignore_vec,
        &part_cols,
        // empty partition_by (explicit) => force unpartitioned on a partitioned source
        part_cols.is_empty(),
    )
}

/// Sync IMV partitions with the source's partition set.  When
/// `drop_orphans` is true (default), IMV partitions whose source
/// counterpart has been dropped are removed via `DROP TABLE ... CASCADE`
/// (touches only pg_reflex-owned objects below the IMV partition).  When
/// false, orphans are preserved and a NOTICE is emitted.
///
/// Idempotent; safe to call repeatedly.  No-op when the IMV is
/// unpartitioned.
#[pg_extern]
fn reflex_sync_partitions(view_name: &str, drop_orphans: default!(bool, "TRUE")) -> String {
    partition::reflex_sync_partitions_impl(view_name, drop_orphans)
}

/// Reconcile only the partition(s) of an IMV that cover the given
/// `partition_keys` (comma-separated text list).  The partition's
/// intermediate + target child are TRUNCATEd and rebuilt via the base /
/// end query restricted by the child's partition constraint.  Cascades
/// to dependent IMVs partitioned on the same column with the same keys,
/// or to full reconcile otherwise.
///
/// Alternatively pass `source_partition` (a single source partition, or a
/// comma-separated list) to reconcile every IMV node mapping to those source
/// partitions in ONE call; the dependent cascade then fires once over the
/// union of affected keys rather than once per node.
#[pg_extern]
fn reflex_reconcile_partition(
    view_name: &str,
    partition_keys: &str,
    source_partition: default!(&str, "''"),
    skip_sync: default!(bool, "FALSE"),
) -> String {
    partition::reflex_reconcile_partition_impl(
        view_name,
        partition_keys,
        source_partition,
        skip_sync,
    )
}

/// Resolve pending source-partition changes: oid-diff each dirty source root
/// against the snapshot, then swap-fill / create / drop the matching IMV
/// partitions (cascading to dependents). Call after a batch of DETACH/ATTACH
/// swaps. Drains __reflex_partition_pending.
#[pg_extern]
fn reflex_flush_partitions() -> String {
    partition::reflex_flush_partitions_impl(None)
}

/// Flush a single source root (skips the pending-queue scan).
#[pg_extern]
fn reflex_flush_partition_source(source_root: &str) -> String {
    partition::reflex_flush_partitions_impl(Some(source_root))
}

/// Re-arm pending partition roots that the failure cap has given up on, so the
/// next flush attempts them again. Pass NULL for every root.
///
/// A root that has failed `PARTITION_FLUSH_FAILURE_CAP` flushes in a row is
/// skipped by both `reflex_flush_partitions()` and
/// `reflex_flush_partition_source(root)`, so no flush can move it and no flush
/// can clear the counter (it is cleared only by the DELETE a *successful* drain
/// performs). Fix the underlying cause first — re-arming a root whose cause is
/// still present simply spends another attempt. Returns the number of roots
/// re-armed.
#[pg_extern]
fn reflex_reset_partition_failures(source_root: default!(Option<&str>, "NULL")) -> i64 {
    partition::reflex_reset_partition_failures_impl(source_root)
}

/// Internal: replace the source-partition snapshot for `source_root` with the
/// live leaf set. SQL-callable so the per-root flush subtransaction can refresh
/// the snapshot atomically with its reconciles. Not part of the public API.
#[pg_extern(name = "__reflex_refresh_partition_snapshot")]
fn reflex_refresh_partition_snapshot(source_root: &str) -> &'static str {
    pgrx::Spi::connect_mut(|client| {
        partition::refresh_source_snapshot(client, source_root);
    });
    "OK"
}

/// Test-only: run the F2 drain → (caller-supplied build DDL) → refill cycle
/// against one partitioned root. `build_ddl` is one or more `;`-separated
/// statements that create partitions under the (now-emptied) tree.
#[cfg(any(test, feature = "pg_test"))]
#[pg_extern]
fn __reflex_test_drain_build_refill(root: &str, build_ddl: &str) -> String {
    let outcome: Result<(), String> = Spi::connect_mut(|client| {
        let entries = partition::drain_tree_defaults(client, &[root.to_string()])?;
        for stmt in build_ddl
            .split(';')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
        {
            client
                .update(stmt, None, &[])
                .map_err(|e| format!("build: {}", e))?;
        }
        partition::refill_tree_defaults(client, entries)
    });
    match outcome {
        Ok(()) => "OK".to_string(),
        Err(e) => format!("ERROR: {}", e),
    }
}

/// Detect and heal snapshot divergence: refresh the partition snapshot if it
/// disagrees with the live tree. Returns "OK (no divergence)" or
/// "HEALED (N divergent leaves)".
#[pg_extern]
fn reflex_refresh_partition_snapshot_if_diverged(source_root: &str) -> String {
    pgrx::Spi::connect_mut(|client| {
        let snap = partition::read_snapshot_pairs(client, source_root);
        let live = partition::current_source_leaf_oids(client, source_root);
        let diverged = partition::detect_snapshot_live_divergence(&snap, &live);
        if diverged.is_empty() {
            "OK (no divergence)".to_string()
        } else {
            partition::refresh_source_snapshot(client, source_root);
            format!("HEALED ({} divergent leaves)", diverged.len())
        }
    })
}

/// Drop a reflex IMV and all its artifacts (triggers, tables, reference row).
/// Refuses to drop if the IMV has children unless cascade is true.
#[pg_extern]
fn drop_reflex_ivm(view_name: &str) -> &'static str {
    if let Err(msg) = validate_view_name(view_name) {
        return msg;
    }
    drop_ivm::drop_reflex_ivm_impl(view_name, false)
}

#[pg_extern(name = "drop_reflex_ivm")]
fn drop_reflex_ivm_cascade(view_name: &str, cascade: bool) -> &'static str {
    if let Err(msg) = validate_view_name(view_name) {
        return msg;
    }
    drop_ivm::drop_reflex_ivm_impl(view_name, cascade)
}

/// Reconcile an IMV by rebuilding intermediate + target from scratch, with
/// explicit control over whether the pre-rebuild partition sync may drop orphan
/// IMV partitions.
///
/// The one-argument form hardcodes `drop_orphans => TRUE`, which is 1.10.11
/// behaviour and remains the default. This overload exists for callers that gate
/// destruction on their own authorization: `reflex_doctor` refuses an F3 orphan
/// drop when the operator did not pass `drop_orphans`, then reached the same
/// destruction anyway through its F4 `reflex_reconcile` repair. Passing FALSE here
/// keeps that promise.
#[pg_extern(name = "reflex_reconcile")]
fn reflex_reconcile_scoped(view_name: &str, drop_orphans: bool) -> &'static str {
    reconcile::stamp_targeted_recovery(view_name);
    reconcile::reflex_reconcile_with_orphans(view_name, drop_orphans)
}

/// Reconcile an IMV by rebuilding intermediate + target from scratch.
/// Use this as a safety net (manually or via pg_cron) to fix drift.
#[pg_extern]
fn reflex_reconcile(view_name: &str) -> &'static str {
    reconcile::stamp_targeted_recovery(view_name);
    reconcile::reflex_reconcile(view_name)
}

/// 1.4.5: VACUUM FULL both the intermediate and target tables of an IMV.
/// Materializes the fillfactor=70 set by the 1.4.3→1.4.4 migration so HOT
/// updates can fire on legacy-populated pages.
///
/// Takes ACCESS EXCLUSIVE on each table; schedule during a maintenance
/// window for multi-gigabyte IMVs.
#[pg_extern]
fn reflex_compact_imv(view_name: &str) -> String {
    create_ivm::reflex_compact_imv_impl(view_name)
}

/// 1.4.5: VACUUM FULL every enabled IMV's intermediate and target tables.
/// Convenience wrapper around `reflex_compact_imv` that iterates over all
/// rows of `__reflex_ivm_reference` in graph_depth order. Each call takes
/// ACCESS EXCLUSIVE on the IMV's tables; schedule during a maintenance
/// window. Returns a per-IMV summary; failures on one IMV do not abort
/// the others (the error is recorded in the result).
#[pg_extern]
fn reflex_compact_all_imv() -> String {
    create_ivm::reflex_compact_all_imv_impl()
}

/// 1.4.5: Re-probe NOT NULL columns from the intermediate's actual data and
/// update `__reflex_ivm_reference.aggregations.not_null_columns`.
///
/// Run this after a data shape change (e.g., a batch load that introduces
/// NULLs into a previously NULL-free column, or the inverse). The 1.4.4→1.4.5
/// migration invokes this once per existing IMV to backfill effectively-NOT
/// NULL columns the catalog heuristic missed.
///
/// Trigger codegen reads `not_null_columns` to choose between `=` (sargable,
/// index-usable) and `IS NOT DISTINCT FROM` (NULL-safe) on group-key probes.
/// Mismatched membership causes either correctness bugs (false `=` when
/// NULLs exist → group-key splitting drift) or perf bugs (false `IS NOT
/// DISTINCT FROM` when NULLs don't exist → composite-index defeat, the
/// 405 s yse regression).
#[pg_extern]
fn reflex_probe_not_null_columns(view_name: &str) -> String {
    create_ivm::reflex_probe_not_null_columns_impl(view_name)
}

/// 1.4.5: Re-analyze an existing IMV's stored `base_query` and merge the
/// newly computed `imv_relevant_columns` / `imv_relevant_where` maps into
/// its `aggregations` JSON. Idempotent.
///
/// Used by the 1.4.4→1.4.5 migration to backfill the static analysis the
/// filter-aware spurious-skip relies on; also useful after a future
/// analyzer extension shifts what falls into either map.
#[pg_extern]
fn reflex_rebuild_imv_metadata(view_name: &str) -> String {
    create_ivm::reflex_rebuild_imv_metadata_impl(view_name)
}

/// 1.4.5: Re-emit the consolidated trigger function bodies for a source
/// table, picking up the latest codegen. CREATE OR REPLACE overwrites
/// existing function bodies without changing trigger identity.
///
/// Used by the 1.4.4→1.4.5 migration to install the filter-aware
/// spurious-skip block on triggers attached to pre-1.4.5 IMVs.
#[pg_extern]
fn reflex_rebuild_triggers(source_table: &str) -> String {
    create_ivm::reflex_rebuild_triggers_impl(source_table)
}

/// Repair primitive for a materialised UNION-ALL wrapper's per-operand
/// `__reflex_union_mirror_{ins,del,upd}_<wrapper>_<i>` triggers
/// (`install_union_mirror_triggers`, create-time only). Re-running it heals a
/// dropped trigger, a dropped trigger function, or both, for every operand
/// recorded in the wrapper's `depends_on`. Refuses (clean `ERROR: ...`
/// string, not a raise) a VIEW wrapper — which has no operand triggers by
/// design — or a non-wrapper IMV. See
/// `untreated_bugs/2026-07-24_union_mirror_triggers_unchecked.md`.
#[pg_extern]
fn reflex_rebuild_union_mirror(wrapper: &str) -> String {
    create_ivm::reflex_rebuild_union_mirror_impl(wrapper)
}

/// Rebuild a decomposed (CTE/set-op) IMV chain from the stored registry spec.
/// CASCADE-drops the top IMV and all sub-IMVs, then recreates them faithfully
/// using the stored creation parameters. Atomicity guaranteed: drop + recreate
/// occur in a single SPI transaction, so on failure neither persists.
///
/// Used for recovery when a decomposed chain accumulates structural damage that
/// bottom-up reconciliation cannot resolve. Returns a status string or ERROR.
///
/// Blast radius: if other IMVs depend on `view_name`, CASCADE would drop those
/// too. Without `cascade => TRUE` this refuses and names the dependents instead
/// of silently destroying them. With `cascade => TRUE`, dependents are dropped
/// and recreated alongside the named IMV.
#[pg_extern]
fn reflex_rebuild_chain(view_name: &str, cascade: default!(bool, "FALSE")) -> String {
    create_ivm::reflex_rebuild_chain_impl(view_name, cascade)
}

/// Refresh a single IMV by rebuilding from source. Alias for reflex_reconcile.
/// Use after REFRESH MATERIALIZED VIEW on a source that feeds this IMV.
#[pg_extern]
fn refresh_reflex_imv(view_name: &str) -> &'static str {
    reconcile::reflex_reconcile(view_name)
}

/// Refresh ALL IMVs that depend on a given source table or materialized view.
/// Processes IMVs in graph_depth order (L1 before L2).
#[pg_extern]
fn refresh_imv_depending_on(source: &str) -> &'static str {
    reconcile::refresh_imv_depending_on(source)
}

/// Rebuild an IMV from scratch to fix drift. Alias for reflex_reconcile.
/// This function is anchor-scoped: for unpartitioned IMVs, it rebuilds the entire
/// target; for partitioned IMVs, it re-derives every child partition of the primary
/// (anchor) source unconditionally, even if structurally unchanged.
///
/// IMPORTANT: Partitions fed only by sources listed in `ignore_sources` (authoritative
/// sources that are not incrementally maintained) will NOT be filled by this function.
/// When the anchor source has no rows for a partition key, but an authoritative
/// ignore_sources table does (archive residue scenario), the partition stays empty.
/// For such cases, use `reflex_reconcile_partition(view, partition_keys)` to
/// force-fill the partition from all its authoritative sources, or perform a
/// full chain drop+recreate. See docs/untreated.md § F6 for details.
#[pg_extern]
fn reflex_rebuild_imv(view_name: &str) -> &'static str {
    reconcile::stamp_targeted_recovery(view_name);
    reconcile::reflex_reconcile(view_name)
}

/// 1.4.6 — set or clear the per-IMV wipe_threshold override. The dispatch
/// DO block emitted by the trigger reads this value before the GUC
/// reflex.wipe_threshold and the compiled default. Pass `value = NULL` to
/// clear the override (fall back to GUC / compiled default).
///
/// Returns a status string with the new effective threshold for this IMV.
#[pg_extern]
fn reflex_set_wipe_threshold(view_name: &str, value: Option<pgrx::AnyNumeric>) -> String {
    if let Err(msg) = validate_view_name(view_name) {
        return msg.to_string();
    }
    let result: Result<u64, String> = Spi::connect_mut(|client| {
        crate::sql_writer::registry::set_wipe_threshold(client, view_name, value.clone())
            .map_err(|e| format!("update failed: {}", e))
    });
    match result {
        Ok(0) => format!(
            "ERROR: IMV '{}' not found in __reflex_ivm_reference",
            view_name
        ),
        Ok(_) => match value {
            Some(v) => format!("OK — '{}' wipe_threshold set to {}", view_name, v),
            None => format!(
                "OK — '{}' wipe_threshold cleared (uses GUC/default)",
                view_name
            ),
        },
        Err(e) => format!("ERROR: {}", e),
    }
}

/// 1.6.0 — set or clear the per-IMV `wipe_floor_rows` override.  The
/// per-partition dispatch DO block reads this floor on the denominator of
/// the dirty/partition-size ratio so a tiny / never-ANALYZE'd partition
/// (`reltuples = 0`) cannot trip the dispatch with a single dirty row.
/// Pass `value = NULL` to clear (fall back to GUC / compiled default 1000).
#[pg_extern]
fn reflex_set_wipe_floor_rows(view_name: &str, value: Option<i64>) -> String {
    if let Err(msg) = validate_view_name(view_name) {
        return msg.to_string();
    }
    let result: Result<u64, String> = Spi::connect_mut(|client| {
        crate::sql_writer::registry::set_wipe_floor_rows(client, view_name, value)
            .map_err(|e| format!("update failed: {}", e))
    });
    match result {
        Ok(0) => format!(
            "ERROR: IMV '{}' not found in __reflex_ivm_reference",
            view_name
        ),
        Ok(_) => match value {
            Some(v) => format!("OK — '{}' wipe_floor_rows set to {}", view_name, v),
            None => format!(
                "OK — '{}' wipe_floor_rows cleared (uses GUC/default)",
                view_name
            ),
        },
        Err(e) => format!("ERROR: {}", e),
    }
}

/// 1.6.0 (plans/partitioning_3.md §4) — set or clear the per-IMV
/// `partition_dispatch_cost_cap`.  When a Tier 2 (JOIN-secondary)
/// source-trigger fires on a partitioned IMV, the dispatch JOINs to the
/// anchor source to derive partition keys; if the planner's estimated row
/// count of that JOIN exceeds this cap, the per-partition dispatch is
/// skipped and the trigger falls back to global Path B.  NULL = inherit
/// GUC `reflex.partition_dispatch_cost_cap` → compiled default (100000).
#[pg_extern]
fn reflex_set_partition_dispatch_cost_cap(view_name: &str, value: Option<i64>) -> String {
    if let Err(msg) = validate_view_name(view_name) {
        return msg.to_string();
    }
    let result: Result<u64, String> = Spi::connect_mut(|client| {
        crate::sql_writer::registry::set_partition_dispatch_cost_cap(client, view_name, value)
            .map_err(|e| format!("update failed: {}", e))
    });
    match result {
        Ok(0) => format!(
            "ERROR: IMV '{}' not found in __reflex_ivm_reference",
            view_name
        ),
        Ok(_) => match value {
            Some(v) => format!(
                "OK — '{}' partition_dispatch_cost_cap set to {}",
                view_name, v
            ),
            None => format!(
                "OK — '{}' partition_dispatch_cost_cap cleared (uses GUC/default)",
                view_name
            ),
        },
        Err(e) => format!("ERROR: {}", e),
    }
}

#[pg_extern(name = "reflex_audit")]
fn reflex_audit_all() -> String {
    audit::reflex_audit_impl(audit::AuditScope::All)
}

#[pg_extern(name = "reflex_audit")]
fn reflex_audit_one(view_name: &str) -> String {
    audit::reflex_audit_impl(audit::AuditScope::One(view_name.to_string()))
}

/// Comprehensive diagnosis and repair orchestrator. Detects and optionally fixes
/// inconsistencies across the IMV registry, pending queue, and audit findings.
///
/// Returns a TABLE with columns:
///   check_id TEXT     — identifier like F1, F2, F4, etc.
///   severity TEXT     — ERROR, WARNING, or INFO
///   object TEXT       — the affected IMV or source root
///   finding TEXT      — human-readable description
///   action TEXT       — the exact SQL to remediate (as a string)
///   outcome TEXT      — result of attempted fix ('fixed', 'reported', 'skipped(...)', 'failed:...')
#[pg_extern]
#[allow(clippy::type_complexity)]
fn reflex_doctor(
    target: default!(Option<&str>, "NULL"),
    fix: default!(bool, "FALSE"),
    drop_orphans: default!(bool, "FALSE"),
    max_attempts: default!(i32, "3"),
) -> TableIterator<
    'static,
    (
        name!(check_id, String),
        name!(severity, String),
        name!(object, String),
        name!(finding, String),
        name!(action, String),
        name!(outcome, String),
    ),
> {
    let rows = doctor::reflex_doctor_impl(target, fix, drop_orphans, max_attempts);
    TableIterator::new(rows)
}

extension_sql!(
    r#"
    CREATE OR REPLACE FUNCTION public.__reflex_on_sql_drop()
    RETURNS event_trigger LANGUAGE plpgsql AS $$
    DECLARE
        _obj RECORD;
        _imv RECORD;
    BEGIN
        FOR _obj IN
            SELECT object_identity
            FROM pg_event_trigger_dropped_objects()
            WHERE object_type = 'table'
        LOOP
            FOR _imv IN
                SELECT name
                FROM public.__reflex_ivm_reference
                WHERE depends_on @> ARRAY[_obj.object_identity]
                   OR depends_on @> ARRAY[split_part(_obj.object_identity, '.', 2)]
                ORDER BY graph_depth DESC, name DESC
            LOOP
                BEGIN
                    PERFORM public.drop_reflex_ivm(_imv.name, TRUE);
                    RAISE NOTICE 'pg_reflex: dropped IMV % (source % was dropped)', _imv.name, _obj.object_identity;
                EXCEPTION WHEN OTHERS THEN
                    RAISE WARNING 'pg_reflex: failed to drop IMV % after source % drop: %',
                        _imv.name, _obj.object_identity, SQLERRM;
                    DELETE FROM public.__reflex_ivm_reference WHERE name = _imv.name;
                END;
            END LOOP;

            -- An IMV whose own *target* table was dropped (e.g. DROP SCHEMA …
            -- CASCADE, or a stray DROP TABLE) must also be torn down, otherwise
            -- the registry row orphans, pointing at a relation that no longer
            -- exists. The source branch above never catches this when the
            -- source is a view or lives outside the dropped scope. Match on the
            -- EXACT target identity (target_schema + bare name) — never a prefix
            -- — so a partition swap dropping child / __reflex_swap_* tables can
            -- never be mistaken for the registered target.
            FOR _imv IN
                SELECT name
                FROM public.__reflex_ivm_reference
                WHERE COALESCE(target_schema, 'public') || '.'
                      || (regexp_match(name, '([^.]+)$'))[1] = _obj.object_identity
                ORDER BY graph_depth DESC, name DESC
            LOOP
                BEGIN
                    PERFORM public.drop_reflex_ivm(_imv.name, TRUE);
                    RAISE NOTICE 'pg_reflex: dropped IMV % (target % was dropped)', _imv.name, _obj.object_identity;
                EXCEPTION WHEN OTHERS THEN
                    RAISE WARNING 'pg_reflex: failed to drop IMV % after target % drop: %',
                        _imv.name, _obj.object_identity, SQLERRM;
                    DELETE FROM public.__reflex_ivm_reference WHERE name = _imv.name;
                END;
            END LOOP;
        END LOOP;
    END;
    $$;

    CREATE EVENT TRIGGER reflex_on_sql_drop
        ON sql_drop
        EXECUTE FUNCTION public.__reflex_on_sql_drop();

    CREATE OR REPLACE FUNCTION public.__reflex_on_ddl_command_end()
    RETURNS event_trigger LANGUAGE plpgsql AS $$
    DECLARE
        _cmd RECORD;
        _imv RECORD;
        _src TEXT;
        _parent TEXT;
        _part_root TEXT;
        _policy TEXT;
        _affected TEXT[] := ARRAY[]::TEXT[];
        _synced_keys TEXT[] := ARRAY[]::TEXT[];
        _sync_key TEXT;
        _reconcile_root TEXT;
    BEGIN
        _policy := lower(COALESCE(NULLIF(current_setting('pg_reflex.alter_source_policy', true), ''), 'warn'));
        IF _policy NOT IN ('warn', 'error') THEN
            RAISE WARNING 'pg_reflex: invalid pg_reflex.alter_source_policy=%, falling back to ''warn''', _policy;
            _policy := 'warn';
        END IF;

        -- Root whose chain reflex_reconcile is currently rebuilding, set by that
        -- function around its DISABLE/ENABLE TRIGGER of each generated sub-IMV.
        -- Those internal ALTERs are on tracked sources (a generated child sits in
        -- its parent's depends_on), so the warn/error branch below would fire a
        -- spurious "run reflex_rebuild_imv" for a rebuild already in flight and,
        -- under 'error' policy, abort the reconcile outright. Suppressed for the
        -- nodes of the active chain only — a DIFFERENT root that reads the same
        -- node still warns, because that consumer really did miss the refresh.
        _reconcile_root := NULLIF(current_setting('pg_reflex.internal_reconcile_root', true), '');

        -- 1.6.0: auto-sync IMV partitions when a source's partition tree changes.
        --
        -- Two trigger surfaces matter:
        --   (a) ALTER TABLE parent ATTACH/DETACH PARTITION child
        --       → pg_event_trigger_ddl_commands() returns object_identity = parent,
        --         command_tag = 'ALTER TABLE'.
        --   (b) CREATE TABLE child PARTITION OF parent FOR VALUES ...
        --       → command_tag = 'CREATE TABLE'; object_identity = child;
        --         the parent must be looked up via pg_inherits.
        --
        -- For every command we resolve a candidate parent table name, then for
        -- each partitioned IMV depending on that parent we call
        -- reflex_sync_partitions(view, drop_orphans=>FALSE) — orphan deletion is
        -- never automatic (IMV data is the user's, and a DETACH on the source
        -- side is not a delete signal). reflex_sync_partitions is idempotent
        -- and advisory-lock protected, so duplicate fires inside one
        -- transaction collapse harmlessly.
        --
        -- The previous (1.5.x) warn/error contract for non-partition ALTERs
        -- (column add/drop on a tracked source) is preserved below.

        FOR _cmd IN
            SELECT object_identity, object_type, command_tag
            FROM pg_event_trigger_ddl_commands()
            WHERE command_tag IN ('ALTER TABLE', 'CREATE TABLE')
        LOOP
            -- Resolve the parent table for partition-tree changes. NULL for
            -- non-partition events (regular ALTER TABLE on a leaf table).
            _parent := NULL;
            IF _cmd.command_tag = 'ALTER TABLE' THEN
                -- ATTACH / DETACH PARTITION: object_identity is the parent.
                -- Other ALTER variants (ADD COLUMN, …) also land here with
                -- object_identity = the altered table — we sync anyway iff
                -- that table is a partitioned source of a partitioned IMV.
                _parent := _cmd.object_identity;
            ELSIF _cmd.command_tag = 'CREATE TABLE' THEN
                -- CREATE TABLE … PARTITION OF parent: look up parent via
                -- pg_inherits regardless of `object_type` (PG reports
                -- 'table' or 'table partition' depending on version).
                -- Empty result = the new table isn't a partition; _parent
                -- stays NULL and the branch below skips.
                BEGIN
                    SELECT n.nspname || '.' || c.relname INTO _parent
                    FROM pg_inherits i
                    JOIN pg_class c   ON c.oid = i.inhparent
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE i.inhrelid = _cmd.object_identity::regclass;
                EXCEPTION WHEN OTHERS THEN
                    _parent := NULL;
                END;
            END IF;

            IF _parent IS NOT NULL THEN
                -- Capture for flush: resolve the partition ROOT (a multi-level
                -- attach reports an intermediate level as _parent, but IMVs depend
                -- on the top-level source) and enqueue it, unless it is
                -- pg_reflex-owned (our own atomic swap ATTACH/DETACHes IMV
                -- partitions; reacting to those would race the code-driven cascade).
                BEGIN
                    SELECT n.nspname || '.' || c.relname
                      INTO _part_root
                      FROM pg_class c
                      JOIN pg_namespace n ON n.oid = c.relnamespace
                     WHERE c.oid = pg_partition_root(_parent::regclass);
                EXCEPTION WHEN OTHERS THEN
                    _part_root := NULL;
                END;

                IF _part_root IS NOT NULL
                   AND _part_root NOT LIKE '%\_\_reflex\_%'
                   AND NOT EXISTS (
                       SELECT 1 FROM public.__reflex_ivm_reference r
                       WHERE r.name = _part_root OR r.name = split_part(_part_root, '.', 2)
                   )
                   AND EXISTS (
                       -- Enqueue for ANY enabled IMV depending on this root:
                       -- partitioned IMVs reconcile per-partition; unpartitioned
                       -- IMVs get a full reconcile (flush handles both). Without
                       -- this, an unpartitioned IMV on a swap source goes stale.
                       SELECT 1 FROM public.__reflex_ivm_reference r
                       WHERE r.enabled
                         AND (r.depends_on @> ARRAY[_part_root]
                              OR r.depends_on @> ARRAY[split_part(_part_root, '.', 2)])
                   )
                THEN
                    INSERT INTO public.__reflex_partition_pending (source_root)
                    VALUES (_part_root)
                    ON CONFLICT (source_root)
                    DO UPDATE SET enqueued_at = statement_timestamp(),
                                  attempts    = public.__reflex_partition_pending.attempts + 1;
                END IF;

                FOR _imv IN
                    SELECT name FROM public.__reflex_ivm_reference
                    WHERE partition_columns IS NOT NULL
                      AND array_length(partition_columns, 1) > 0
                      AND (depends_on @> ARRAY[_parent]
                           OR depends_on @> ARRAY[split_part(_parent, '.', 2)])
                LOOP
                    _sync_key := _parent || '|' || _imv.name;
                    IF _sync_key = ANY(_synced_keys) THEN
                        CONTINUE;
                    END IF;
                    _synced_keys := _synced_keys || _sync_key;
                    BEGIN
                        PERFORM public.reflex_sync_partitions(_imv.name, FALSE);
                        RAISE NOTICE 'pg_reflex: auto-synced partitions for IMV % (source %)',
                            _imv.name, _parent;
                    EXCEPTION WHEN OTHERS THEN
                        UPDATE public.__reflex_ivm_reference
                           SET known_stale = TRUE, stale_reason = left(SQLERRM, 2000), stale_since = now()
                         WHERE name = _imv.name;
                        RAISE WARNING 'pg_reflex: auto-sync of IMV % failed after source % partition change: % — run SELECT reflex_sync_partitions(''%'') manually',
                            _imv.name, _parent, SQLERRM, _imv.name;
                    END;
                END LOOP;
            END IF;
        END LOOP;

        -- Warn/error policy for non-partition ALTERs on tracked sources.
        -- This branch is unchanged from 1.5.x except that auto-sync above may
        -- have already healed pure partition-tree changes; the warning still
        -- fires (column shape may have changed) so the operator knows to
        -- inspect.
        FOR _cmd IN
            SELECT object_identity, command_tag
            FROM pg_event_trigger_ddl_commands()
            WHERE command_tag = 'ALTER TABLE'
        LOOP
            _src := _cmd.object_identity;
            FOR _imv IN
                SELECT name FROM public.__reflex_ivm_reference
                WHERE depends_on @> ARRAY[_src]
                   OR depends_on @> ARRAY[split_part(_src, '.', 2)]
            LOOP
                -- Skip pg_reflex's own DISABLE/ENABLE TRIGGER on a generated
                -- sub-IMV of the chain being reconciled: the consumer named here
                -- is that same chain's root or an intermediate generated node of
                -- it, and it is about to be rebuilt. A consumer on a DIFFERENT
                -- root does not match this prefix, so its legitimate stale signal
                -- still fires.
                IF _reconcile_root IS NOT NULL
                   AND ( _imv.name = _reconcile_root
                         OR split_part(_imv.name, '.', 2)
                            = split_part(_reconcile_root, '.', 2)
                         OR split_part(_imv.name, '.', 2)
                            LIKE split_part(_reconcile_root, '.', 2) || '\_\_%' )
                THEN
                    CONTINUE;
                END IF;
                _affected := _affected || (_src || ' -> ' || _imv.name);
                IF _policy = 'warn' THEN
                    RAISE WARNING 'pg_reflex: source table % was altered; IMV % may be stale — run SELECT reflex_rebuild_imv(''%'') to recover',
                        _src, _imv.name, _imv.name;
                END IF;
            END LOOP;
        END LOOP;

        IF _policy = 'error' AND array_length(_affected, 1) > 0 THEN
            RAISE EXCEPTION 'pg_reflex: ALTER blocked by pg_reflex.alter_source_policy=''error'' on tracked source(s); affected: %',
                array_to_string(_affected, ', ')
                USING HINT = 'Set pg_reflex.alter_source_policy = ''warn'' (default) or drop_reflex_ivm() first.';
        END IF;
    END;
    $$;

    CREATE EVENT TRIGGER reflex_on_ddl_command_end
        ON ddl_command_end
        WHEN TAG IN ('ALTER TABLE', 'CREATE TABLE')
        EXECUTE FUNCTION public.__reflex_on_ddl_command_end();

    -- 1.10.0: auto-drain the partition pending queue at COMMIT. Mirrors the
    -- deferred-DML flush mechanism (schema_builder.rs): a DEFERRABLE INITIALLY
    -- DEFERRED constraint trigger on __reflex_partition_pending fires once per
    -- enqueued root at COMMIT, running a SCOPED flush of just that root. Scoped
    -- (not the all-roots reflex_flush_partitions) so each root drains
    -- independently; combined with the per-root subtransaction inside
    -- reflex_flush_partition_source, one broken root cannot wedge the queue.
    -- Recursion is bounded: the flush's own ATTACH/DETACH on __reflex_-owned
    -- tables is ignored by the ddl_command_end enqueue guard (NOT LIKE
    -- '%__reflex_%'), so no new pending rows are produced.
    -- Incremental partition delta (plans/2026-06-11): apply an attached/detached
    -- partition child to an UNPARTITIONED IMV as the bulk INSERT/DELETE it
    -- semantically is, instead of a full TRUNCATE+rebuild reconcile. Mirrors the
    -- INSERT/DELETE trigger body pipeline (pred-check skip → Path B ratio
    -- dispatch → reflex_build_delta_sql → execute), parameterized at runtime.
    -- `_trans` is the conventional transition-table name (computed caller-side via
    -- transition_{new,old}_table_name(_source)) that reflex_build_delta_sql reads
    -- from. Every uncertain branch falls back to reflex_reconcile (always correct).
    CREATE OR REPLACE FUNCTION public.reflex_apply_partition_delta(
        _imv TEXT, _source TEXT, _op TEXT, _child TEXT, _trans TEXT
    ) RETURNS TEXT LANGUAGE plpgsql AS $fn$
    DECLARE
        _rec RECORD;
        _sql TEXT;
        _no_pass BOOLEAN;
        _src_total BIGINT;
        _trans_count BIGINT;
        _thr NUMERIC;
    BEGIN
        SELECT base_query, end_query, aggregations::text AS aggregations,
               where_predicate, wipe_threshold
          INTO _rec
          FROM public.__reflex_ivm_reference
         WHERE name = _imv AND enabled = TRUE;
        IF NOT FOUND THEN RETURN 'SKIPPED (imv not found)'; END IF;

        PERFORM pg_advisory_xact_lock(hashtext(_imv), hashtext(reverse(_imv)));

        -- No-op short-circuit FIRST, probing the child directly so a filtered-out
        -- partition is skipped in O(1) (the planner evaluates the WHERE against the
        -- partition's constant key) without materializing the transition at all.
        -- where_predicate is the bare-column form, which evaluates against the
        -- child the same as against the flat transition table.
        IF _rec.where_predicate IS NOT NULL AND _rec.where_predicate <> '' THEN
            EXECUTE format('SELECT NOT EXISTS(SELECT 1 FROM %s WHERE %s LIMIT 1)',
                           _child, _rec.where_predicate) INTO _no_pass;
            IF _no_pass THEN
                RETURN 'SKIPPED (no rows pass filter)';
            END IF;
        END IF;

        -- Materialize the partition child as the conventional transition table
        -- reflex_build_delta_sql reads from.
        EXECUTE format('DROP TABLE IF EXISTS pg_temp.%I', _trans);
        EXECUTE format('CREATE TEMP TABLE %I ON COMMIT DROP AS SELECT * FROM %s', _trans, _child);

        -- Path B: a bulk change large relative to the source is cheaper to
        -- reconcile than to delta (same decision a real bulk INSERT makes).
        BEGIN
            SELECT reltuples::BIGINT INTO _src_total FROM pg_class WHERE oid = _source::regclass;
            IF _src_total IS NOT NULL AND _src_total >= 1000 THEN
                EXECUTE format('SELECT count(*) FROM %I', _trans) INTO _trans_count;
                _thr := COALESCE(_rec.wipe_threshold,
                                 current_setting('reflex.wipe_threshold', true)::NUMERIC, 0.5);
                IF _trans_count::NUMERIC / _src_total >= _thr THEN
                    EXECUTE format('DROP TABLE IF EXISTS pg_temp.%I', _trans);
                    PERFORM public.reflex_reconcile(_imv);
                    RETURN 'RECONCILED (path B)';
                END IF;
            END IF;
        EXCEPTION WHEN OTHERS THEN NULL; END;

        -- Incremental delta — the exact pipeline the INSERT/DELETE triggers run.
        _sql := public.reflex_build_delta_sql(_imv, _source, _op,
                    _rec.base_query, _rec.end_query, _rec.aggregations, _rec.base_query);
        IF _sql IS NULL OR _sql = '' THEN
            EXECUTE format('DROP TABLE IF EXISTS pg_temp.%I', _trans);
            PERFORM public.reflex_reconcile(_imv);
            RETURN 'RECONCILED (no incremental delta)';
        END IF;
        PERFORM public.reflex_execute_separated(_sql);
        EXECUTE format('DROP TABLE IF EXISTS pg_temp.%I', _trans);
        RETURN 'DELTA';
    END;
    $fn$;

    -- Detach-then-drop no-op proof. When a partition is DETACHed and DROPped in
    -- the same transaction, the deferred flush at COMMIT finds the child gone and
    -- cannot probe its rows (reflex_apply_partition_delta's path). For a DROP we
    -- can still prove the partition was irrelevant to an UNPARTITIONED IMV's
    -- filter — and therefore that its removal is a guaranteed no-op — from its
    -- captured LIST bound: if no value the partition could hold passes
    -- where_predicate, the IMV never held one of its rows. The synthetic probe
    -- relation exposes ONLY the partition key column, so any predicate touching a
    -- non-key column raises "column does not exist", which is trapped and
    -- conservatively reconciled. Postgres parses the bound value list itself
    -- (`unnest(ARRAY[…])`), so there is no fragile text splitting. Every
    -- inconclusive branch falls back to reflex_reconcile (always correct).
    CREATE OR REPLACE FUNCTION public.reflex_partition_drop_maybe_skip(
        _imv TEXT, _keycol TEXT, _bound_inner TEXT
    ) RETURNS TEXT LANGUAGE plpgsql AS $fn$
    DECLARE
        _wp TEXT;
        _hit BOOLEAN;
    BEGIN
        SELECT where_predicate INTO _wp
          FROM public.__reflex_ivm_reference
         WHERE name = _imv AND enabled = TRUE;
        IF NOT FOUND THEN RETURN 'SKIPPED (imv not found)'; END IF;

        PERFORM pg_advisory_xact_lock(hashtext(_imv), hashtext(reverse(_imv)));

        -- No filter → every dropped partition's rows were in the IMV; we cannot
        -- prove a no-op, so reconcile.
        IF _wp IS NULL OR _wp = '' THEN
            PERFORM public.reflex_reconcile(_imv);
            RETURN 'RECONCILED (no predicate)';
        END IF;

        BEGIN
            EXECUTE format(
                'SELECT bool_or(%s) FROM (SELECT unnest(ARRAY[%s]) AS %I) AS s',
                _wp, _bound_inner, _keycol
            ) INTO _hit;
        EXCEPTION WHEN OTHERS THEN
            PERFORM public.reflex_reconcile(_imv);
            RETURN 'RECONCILED (probe inconclusive)';
        END;

        -- bool_or IS NOT TRUE  ⇔  no partition value passes the filter  ⇔  the
        -- partition never contributed a row to the IMV  ⇔  its removal is a no-op.
        IF _hit IS NOT TRUE THEN
            RETURN 'SKIPPED (bound excluded by filter)';
        END IF;

        -- A value passes the filter, so the partition may have held IMV rows; they
        -- are gone with the dropped child, so a DELETE delta is impossible.
        PERFORM public.reflex_reconcile(_imv);
        RETURN 'RECONCILED (bound relevant)';
    END;
    $fn$;

    CREATE OR REPLACE FUNCTION public.__reflex_partition_flush_fn()
    RETURNS TRIGGER LANGUAGE plpgsql AS $fn$
    BEGIN
        PERFORM public.reflex_flush_partition_source(NEW.source_root);
        RETURN NULL;
    END;
    $fn$;

    DROP TRIGGER IF EXISTS __reflex_partition_flush_trigger
        ON public.__reflex_partition_pending;

    -- UPDATE OF enqueued_at, not bare UPDATE: the flush's own EXCEPTION handler
    -- writes last_error/failures to this table, and a bare UPDATE trigger would
    -- re-arm itself into an unbounded retry loop at COMMIT. Only the DDL event
    -- trigger's re-enqueue touches enqueued_at.
    CREATE CONSTRAINT TRIGGER __reflex_partition_flush_trigger
        AFTER INSERT OR UPDATE OF enqueued_at ON public.__reflex_partition_pending
        DEFERRABLE INITIALLY DEFERRED
        FOR EACH ROW EXECUTE FUNCTION public.__reflex_partition_flush_fn();
    "#,
    name = "pg_reflex_event_trigger",
    requires = ["pg_reflex_init"],
);

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_extern]
    fn hello_pg_reflex() -> &'static str {
        "Hello, pg_reflex"
    }

    /// Verify IMV matches a fresh computation using EXCEPT ALL oracle.
    fn assert_imv_correct(imv: &str, fresh_sql: &str) {
        let check = format!(
            "SELECT COUNT(*) FROM (\
                (SELECT * FROM {} EXCEPT ALL SELECT * FROM ({}) AS __fresh1) \
                UNION ALL \
                (SELECT * FROM ({}) AS __fresh2 EXCEPT ALL SELECT * FROM {}) \
             ) __oracle",
            imv, fresh_sql, fresh_sql, imv
        );
        let mismatches = Spi::get_one::<i64>(&check)
            .expect("oracle query failed")
            .expect("oracle returned NULL");
        assert_eq!(
            mismatches, 0,
            "EXCEPT ALL oracle failed for '{}': {} mismatches between IMV and fresh query",
            imv, mismatches
        );
    }

    /// Read the last recorded flush wall-time (ms) for an IMV. Panics loudly if
    /// no flush was recorded (NULL) — in immediate mode that means the DML did
    /// not maintain the IMV, which is itself a defect worth surfacing.
    fn last_flush_ms_of(imv: &str) -> i64 {
        Spi::get_one::<i64>(&format!(
            "SELECT last_flush_ms FROM reflex_ivm_status() WHERE name = '{}'",
            imv
        ))
        .expect("status query failed")
        .expect("last_flush_ms was NULL — no flush recorded for this IMV")
    }

    /// Apply an identical single-row delta `samples` times (one flush each) and
    /// return the MIN recorded flush wall-time. `make_insert(k)` must produce a
    /// distinct-PK INSERT for sample `k` so each cycle enqueues fresh work.
    ///
    /// At this scale the flush wall-time is dominated by base-INDEPENDENT fixed
    /// overhead (registry reads, scratch/affected build, advisory lock, DO-block
    /// dispatch, target-sync setup) whose run-to-run jitter is large — observed
    /// ~2x on a quiet box and ~10x on a loaded CI runner (a fixed-group aggregate
    /// flush that costs 25ms locally stretched to 242ms on CI, tripping a
    /// single-sample ratio test on pure noise). MIN collapses those outliers to
    /// the genuine floor cost, which is what the O(base)-vs-O(delta) comparison
    /// in `assert_sublinear` is actually meant to measure.
    const PLAN_PROBE_SAMPLES: i32 = 5;

    fn min_flush_ms_sampled(
        source: &str,
        imv: &str,
        make_insert: impl Fn(i32) -> String,
        samples: i32,
    ) -> i64 {
        let mut best = i64::MAX;
        for k in 0..samples {
            Spi::run(&make_insert(k)).expect("delta insert");
            Spi::run(&format!("SELECT reflex_flush_deferred('{}')", source)).expect("flush");
            best = best.min(last_flush_ms_of(imv));
        }
        best
    }

    /// Plan-quality discriminator. Given flush wall-times for an identical O(1)
    /// delta against a small base and a `base_ratio`x-larger base, decide whether
    /// the shape's maintenance cost SCALES with base size (an O(base) plan grows
    /// ~`base_ratio`x; an O(delta) plan stays flat). Returns `false` when the
    /// large-base flush is cheap (< 50ms) — at that point the shape is fast
    /// enough at scale that O(base) vs O(delta) is not an operational concern
    /// (the bugs this guards against are multi-second/​minute flushes). Only when
    /// the large-base flush is operationally heavy do we require it to stay near
    /// the small-base cost rather than tracking base growth.
    ///
    /// The floor is 50ms, not 30: on a loaded CI runner the base-INDEPENDENT
    /// fixed overhead of the heaviest probe shape (UNION ALL — two operands, each
    /// a decomposed sub-IMV) had a `min`-sampled floor cost of ~35ms, tripping the
    /// old 30ms guard on pure noise even after min-of-N sampling. 50ms clears that
    /// observed fixed-overhead ceiling while staying 1-2 orders of magnitude below
    /// the multi-second regressions this discriminator exists to catch.
    fn flush_scales_with_base(small_ms: i64, big_ms: i64, base_ratio: i64) -> bool {
        if big_ms < 50 {
            return false;
        }
        big_ms as f64 > std::cmp::max(small_ms, 1) as f64 * (base_ratio as f64 / 3.0)
    }

    /// Plan-quality probe: a fixed O(1) delta must not cost O(base). Panics with
    /// the recorded numbers when the shape scales with base size.
    fn assert_sublinear(label: &str, small_ms: i64, big_ms: i64, base_ratio: i64) {
        assert!(
            !flush_scales_with_base(small_ms, big_ms, base_ratio),
            "PLAN-QUALITY GAP [{}]: base grew {}x, flush grew {}ms -> {}ms \
             => maintenance scales with base (O(base), not O(delta))",
            label,
            base_ratio,
            small_ms,
            big_ms
        );
    }

    #[pg_extern]
    fn crate_test_list_partition_tree(root: &str) -> i64 {
        Spi::connect(|client| crate::partition::list_partition_tree(client, root).len() as i64)
    }

    /// Drive the per-child DETACH/ATTACH swap primitive directly, on an
    /// arbitrary source-child name, and surface its `Result` as a string. The
    /// operator entry points resolve leaves before calling it, so this is the
    /// only way to hand it the input it must refuse: a source BRANCH whose
    /// derived mirror child is a partitioned relation.
    #[pg_extern]
    fn crate_test_partition_swap_for_child(view: &str, src_child: &str) -> String {
        let record = Spi::connect(|client| crate::sql_writer::registry::read_imv(client, view))
            .expect("IMV not found in registry");
        let schema = crate::query_decomposer::split_qualified_name(view)
            .0
            .unwrap_or("public")
            .to_string();
        let unlogged = record.storage_mode.eq_ignore_ascii_case("UNLOGGED");
        Spi::connect_mut(|client| {
            match crate::partition::execute_partition_swap_for_child(
                client,
                view,
                &schema,
                src_child,
                &record.base_query,
                &record.end_query,
                unlogged,
            ) {
                Ok(()) => "OK".to_string(),
                Err(e) => format!("ERROR: {}", e),
            }
        })
    }

    /// Call `reconcile_one` directly, bypassing `reflex_reconcile`'s bottom-up
    /// descent into generated children. PS-12 test 2 needs this to exercise the
    /// backstop on a materialised wrapper in isolation: the operator entry point
    /// would first reconcile the wrapper's operand sub-IMVs, whose INSERT mirror
    /// triggers append to the wrapper — the pre-existing doubling hazard — which
    /// would obscure whether the backstop itself is what leaves the wrapper
    /// unchanged.
    #[pg_extern]
    fn crate_test_reconcile_one(view: &str) -> String {
        crate::reconcile::reconcile_one(view, true).to_string()
    }

    include!("tests/pg_test_basic.rs");
    include!("tests/pg_test_trigger.rs");
    include!("tests/pg_test_passthrough.rs");
    include!("tests/pg_test_cte.rs");
    include!("tests/pg_test_set_ops.rs");
    include!("tests/pg_test_window.rs");
    include!("tests/pg_test_drop.rs");
    include!("tests/pg_test_reconcile.rs");
    include!("tests/pg_test_deferred.rs");
    include!("tests/pg_test_error.rs");
    include!("tests/pg_test_e2e.rs");
    include!("tests/pg_test_correctness.rs");
    include!("tests/pg_test_filter.rs");
    include!("tests/pg_test_distinct_on.rs");
    include!("tests/pg_test_1_2_0.rs");
    include!("tests/pg_test_no_sigabrt.rs");
    include!("tests/pg_test_search_path.rs");
    include!("tests/pg_test_directional_dispatch.rs");
    include!("tests/pg_test_coverage.rs");
    include!("tests/pg_test_partition.rs");
    include!("tests/pg_test_subpartition.rs");
    include!("tests/pg_test_subpartition_dataloss.rs");
    include!("tests/pg_test_reconcile_dependent_dataloss.rs");
    include!("tests/pg_test_partition_attach_locks.rs");
    include!("tests/pg_test_partition_dispatch.rs");
    include!("tests/pg_test_audit.rs");
    include!("tests/pg_test_fuzz.rs");
    include!("tests/pg_test_audit_gaps.rs");
    include!("tests/pg_test_cross_source.rs");
    include!("tests/pg_test_union_operand_cross_source.rs");
    include!("tests/pg_test_union_operand_direct_reconcile.rs");
    include!("tests/pg_test_field_replay.rs");
    include!("tests/pg_test_union_subquery_delta.rs");
    include!("tests/pg_test_registry.rs");
    include!("tests/pg_test_doctor.rs");
    include!("tests/pg_test_rebuild_chain.rs");
    include!("tests/pg_test_decomposed_chain.rs");
    include!("tests/pg_test_ps3.rs");
    include!("tests/pg_test_ps9.rs");
    include!("tests/pg_test_ps10.rs");
    include!("tests/pg_test_ps12.rs");
    include!("tests/pg_test_psca_skip_signal.rs");
    include!("tests/pg_test_ps14.rs");
    include!("tests/pg_test_ps17.rs");
    include!("tests/pg_test_ps18.rs");
    include!("tests/pg_test_leftjoin_secondary_groupkey.rs");
    include!("tests/pg_test_ps16.rs");
    include!("tests/pg_test_outerjoin_notnull_groupkey.rs");
    include!("tests/pg_test_qualified_groupby_qualifier.rs");
}

/// This module is required by `cargo pgrx test` invocations.
/// It must be visible at the root of your extension crate.
#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {
        // perform one-off initialization when the pg_test framework starts
    }

    #[must_use]
    pub fn postgresql_conf_options() -> Vec<&'static str> {
        // The differential fuzz gate (fuzz_differential_exact) builds many IMVs +
        // MVs in a single test transaction; each holds locks released only at
        // transaction end. Raise the per-transaction lock budget so larger ad-hoc
        // runs (PG_REFLEX_FUZZ_CASES up to a few hundred) don't hit "out of shared
        // memory". Very large runs still need batching.
        vec!["max_locks_per_transaction = 4096"]
    }
}

#[cfg(test)]
#[path = "tests/unit_proptest.rs"]
mod proptest_tests;

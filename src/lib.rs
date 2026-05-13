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

mod aggregation;
mod create_ivm;
mod drop_ivm;
mod introspect;
mod query_decomposer;
mod reconcile;
mod schema_builder;
mod sql_analyzer;
mod trigger;
mod window;

::pgrx::pg_module_magic!(name, version);

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
        ignored_sources TEXT[] DEFAULT ARRAY[]::TEXT[]
    );

    -- Index on name for fast lookups
    CREATE INDEX IF NOT EXISTS idx__reflex_ivm_name ON public.__reflex_ivm_reference(name);
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
    )
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

/// Reconcile an IMV by rebuilding intermediate + target from scratch.
/// Use this as a safety net (manually or via pg_cron) to fix drift.
#[pg_extern]
fn reflex_reconcile(view_name: &str) -> &'static str {
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
#[pg_extern]
fn reflex_rebuild_imv(view_name: &str) -> &'static str {
    reconcile::reflex_reconcile(view_name)
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
        _policy TEXT;
        _affected TEXT[] := ARRAY[]::TEXT[];
    BEGIN
        _policy := lower(COALESCE(NULLIF(current_setting('pg_reflex.alter_source_policy', true), ''), 'warn'));
        IF _policy NOT IN ('warn', 'error') THEN
            RAISE WARNING 'pg_reflex: invalid pg_reflex.alter_source_policy=%, falling back to ''warn''', _policy;
            _policy := 'warn';
        END IF;

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
        WHEN TAG IN ('ALTER TABLE')
        EXECUTE FUNCTION public.__reflex_on_ddl_command_end();
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
        // return any postgresql.conf settings that are required for your tests
        vec![]
    }
}

#[cfg(test)]
#[path = "tests/unit_proptest.rs"]
mod proptest_tests;

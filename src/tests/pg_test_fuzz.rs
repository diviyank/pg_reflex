// Differential correctness fuzz harness. See
// docs/superpowers/specs/2026-05-22-imv-differential-correctness-design.md
// and docs/superpowers/plans/2026-05-22-imv-differential-correctness.md.

// SPI-free model now lives in the pgrx-free `fuzz_model` crate; re-export under
// the historical local paths so the SPI oracle + #[pg_test]s read unchanged.
pub use fuzz_model::model;
pub use fuzz_model::generate;

#[cfg(any(test, feature = "pg_test"))]
pub mod oracle {
    use fuzz_model::model::*;
    use fuzz_model::axes::{PlannedCase, SourceObjectKind};
    use fuzz_model::oracle_pure::{
        cols_of, diff_subquery, rename_case, CASE_SEQ,
    };
    pub use fuzz_model::oracle_pure::{repro_sql, Outcome, float_diff_from_where};
    use fuzz_model::render;
    use pgrx::prelude::*;
    use std::sync::atomic::Ordering;

    pub fn evaluate(case: &FuzzCase) -> Outcome {
        let seq = CASE_SEQ.fetch_add(1, Ordering::Relaxed);
        let suffix = format!("_fz{seq}");
        let case = rename_case(case, &suffix);

        // DETERMINISTIC SETUP: Create base tables and MV. These never raise from codegen,
        // so they're safe to run in the outer transaction.
        for t in &case.tables {
            Spi::run(&render::create_table_sql(t))
                .expect("setup ddl: create base table failed");
        }

        let mv = format!("mv{suffix}");
        let imv = format!("imv{suffix}");

        Spi::run(&render::create_mv_sql(&mv, &case.select_body))
            .expect("setup ddl: create mv failed");

        // Build the DML statements for the DO block.
        let mut dml_lines = Vec::new();
        for txn in &case.dml {
            for stmt in &txn.statements {
                let cols = cols_of(&case, match stmt {
                    DmlStmt::Insert { table, .. }
                    | DmlStmt::Delete { table, .. }
                    | DmlStmt::Update { table, .. }
                    | DmlStmt::Truncate { table } => table,
                });
                let sql = render::dml_sql(stmt, &|_t: &str| cols.clone());
                dml_lines.push(format!("    {sql};"));
            }
        }
        let dml_block = dml_lines.join("\n");

        // Build flush lines if deferred.
        let mut flush_lines = Vec::new();
        if case.deferred {
            for t in &case.tables {
                flush_lines.push(format!("    PERFORM reflex_flush_deferred('{}');", t.name));
            }
        }
        let flush_block = flush_lines.join("\n");

        // Build the diff subquery (without the outer SELECT).
        // Use float-tolerant comparison if the case has any float output columns.
        let has_float = case.output_columns.iter().any(|c| c.ty.is_float());
        let diff_from = if has_float {
            fuzz_model::oracle_pure::float_diff_from_where(&mv, &imv, &case.unique_columns, &case.output_columns)
        } else {
            diff_subquery(&mv, &imv)
        };

        // Construct the DO block as a string. Use $reflexbody$ for body dollar-quoting
        // to avoid collisions with the DO block's outer $$ quotes.
        let keys = case.unique_columns.join(",");
        let mode = if case.deferred { "DEFERRED" } else { "IMMEDIATE" };
        let body = case.select_body.rendered_sql.clone();

        // Build a PL/pgSQL function that encodes the result as JSON for transport across SPI.
        // This avoids pgrx type deserialization issues with RETURNS TABLE.
        let func_name = format!("oracle_func_{}", seq);
        let create_func_sql = format!(
            r#"CREATE OR REPLACE FUNCTION public.{} () RETURNS text AS $func$
DECLARE
  v_msg text;
  v_diff bigint;
  v_status text := 'MATCH';
  v_detail text := '';
BEGIN
  v_msg := create_reflex_ivm('{}', $reflexbody${}$reflexbody$, '{}', NULL, '{}', NULL);
  IF position('{}' in v_msg) > 0 THEN
    v_status := 'SKIP';
    v_detail := v_msg;
  ELSIF v_msg NOT LIKE 'CREATE REFLEX%%' THEN
    v_status := 'BUG';
    v_detail := 'unexpected create return: ' || v_msg;
  ELSE
{}
{}
    REFRESH MATERIALIZED VIEW {};

    SELECT count(*)::bigint INTO v_diff FROM {};

    IF v_diff > 0 THEN
      v_status := 'BUG';
      v_detail := v_diff || ' mismatched rows';
    END IF;
  END IF;

  RETURN v_status || '|||' || v_detail;
EXCEPTION WHEN OTHERS THEN
  RETURN 'BUG' || '|||' || ('codegen exception: ' || SQLERRM);
END;
$func$ LANGUAGE plpgsql;
"#,
            func_name,
            imv,               // create_reflex_ivm arg
            body,              // $reflexbody content
            keys,              // third arg to create_reflex_ivm
            mode,              // fifth arg to create_reflex_ivm
            crate::REFLEX_UNSUPPORTED_TAG,  // position check
            dml_block,         // DML statements
            flush_block,       // flush statements
            mv,                // REFRESH target
            diff_from,         // SELECT FROM (exact or float-tolerant)
        );

        // Create the function.
        if let Err(e) = Spi::run(&create_func_sql) {
            return Outcome::Bug(format!("create function error: {e:?}"));
        }

        // Call the function and parse the result.
        let call_func_sql = format!("SELECT {}();", func_name);
        let outcome = match Spi::get_one::<&str>(&call_func_sql) {
            Ok(Some(result_str)) => {
                let parts: Vec<&str> = result_str.splitn(2, "|||").collect();
                let status = if !parts.is_empty() { parts[0] } else { "UNKNOWN" };
                let detail = if parts.len() > 1 { parts[1].to_string() } else { String::new() };
                match status {
                    "MATCH" => Outcome::Match,
                    "SKIP" => Outcome::Skip(detail),
                    "BUG" => Outcome::Bug(detail),
                    _ => Outcome::Bug(format!("unknown status: {}", status)),
                }
            }
            e => Outcome::Bug(format!("function call error: {:?}", e)),
        };

        // Note: No explicit cleanup of tables/MVs/IMVs here. The test runs in a transaction
        // that rolls back at the end, so all objects are cleaned up automatically.
        // Attempting to drop with CASCADE triggers pg_reflex's drop handlers which can cause
        // side effects. We rely on the outer transaction rollback for cleanup.

        outcome
    }

    pub fn evaluate_planned(pc: &PlannedCase) -> Outcome {
        evaluate_planned_inner(pc)
    }

    fn evaluate_planned_inner(pc: &PlannedCase) -> Outcome {
        // The gate generates Table sources only (see fuzz_model::axes::all_source), so the
        // only source objects here are base tables. A non-Table source would mean the gate
        // scope was widened without wiring up the source-object creation path — fail loudly
        // rather than silently skipping, so coverage gaps can never hide behind a green gate.
        assert!(
            pc.source_objects
                .iter()
                .all(|o| matches!(o.kind, fuzz_model::axes::SourceObjectKind::Table)),
            "evaluate_planned reached a non-Table source, which is out of the gate's scope; \
             wire up source-object creation before widening all_source()"
        );

        let seq = CASE_SEQ.fetch_add(1, Ordering::Relaxed);
        let suffix = format!("_pfz{seq}");

        // Rename case with a different suffix to distinguish from evaluate()
        let case = rename_case(&pc.case, &suffix);

        // Build a mapping of old table names to new names for renaming source objects.
        let mut table_renames: Vec<(String, String)> = Vec::new();
        for old_t in &pc.case.tables {
            let new_name = format!("{}{}", old_t.name, &suffix);
            table_renames.push((old_t.name.clone(), new_name));
        }

        // Step 1: Create base tables and seed them.
        for t in &case.tables {
            if let Err(e) = Spi::run(&render::create_table_sql(t)) {
                return Outcome::Bug(format!("create base table failed: {e:?}"));
            }
        }

        // Step 2: Create source objects (View/MatView/SubImv) in order.
        // Source objects from pc.source_objects are based on the original (non-renamed) case.
        // However, the base tables have been renamed. So we need to rename table references
        // in the source object definitions.
        for src_obj in &pc.source_objects {
            match src_obj.kind {
                SourceObjectKind::Table => {
                    // Base tables already created above; skip
                }
                SourceObjectKind::View => {
                    // Execute the VIEW DDL directly, renaming table references.
                    // IMPORTANT: We must be careful not to rename table names in the view name itself!
                    // The view name is src_obj.name and should NOT be modified.
                    if let Some(define_sql) = &src_obj.define_sql {
                        // Parse the view definition to avoid renaming the view name
                        // Format: "CREATE VIEW <name> AS SELECT ... FROM <table>"
                        // We'll replace table names only in the FROM clause and beyond
                        let mut renamed_sql = define_sql.clone();

                        // Find "AS SELECT" and only replace table names after this point
                        if let Some(as_select_pos) = renamed_sql.find(" AS SELECT") {
                            let (create_view_part, select_part) = renamed_sql.split_at(as_select_pos);
                            let mut renamed_select_part = select_part.to_string();

                            // Now replace table names only in the SELECT part
                            for (old_name, new_name) in &table_renames {
                                renamed_select_part = renamed_select_part.replace(old_name, new_name);
                            }

                            renamed_sql = format!("{}{}", create_view_part, renamed_select_part);
                        }

                        let create_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            Spi::run(&renamed_sql)
                        }));
                        match create_result {
                            Ok(Ok(())) => {
                                // View creation reported success, but verify it actually exists
                                let check_sql = format!(
                                    "SELECT EXISTS(SELECT 1 FROM information_schema.views WHERE table_name = '{}' AND table_schema = 'public')",
                                    src_obj.name.replace("'", "''")
                                );
                                match Spi::get_one::<bool>(&check_sql) {
                                    Ok(Some(true)) => {},
                                    _ => {
                                        return Outcome::Bug(format!(
                                            "create view {} reported success but view doesn't exist. SQL was: {}",
                                            src_obj.name, renamed_sql
                                        ));
                                    }
                                }
                            },
                            Ok(Err(e)) => {
                                return Outcome::Bug(format!("create view {} failed: {e:?}", src_obj.name));
                            }
                            Err(_) => {
                                return Outcome::Bug(format!("create view {} panicked: SQL: {}", src_obj.name, renamed_sql));
                            }
                        }
                    }
                }
                SourceObjectKind::MatView => {
                    // Execute the MATERIALIZED VIEW DDL directly, renaming table references.
                    // IMPORTANT: We must be careful not to rename table names in the matview name itself!
                    if let Some(define_sql) = &src_obj.define_sql {
                        // Parse the matview definition to avoid renaming the matview name
                        // Format: "CREATE MATERIALIZED VIEW <name> AS SELECT ... FROM <table>"
                        // We'll replace table names only in the FROM clause and beyond
                        let mut renamed_sql = define_sql.clone();

                        // Find "AS SELECT" and only replace table names after this point
                        if let Some(as_select_pos) = renamed_sql.find(" AS SELECT") {
                            let (create_matview_part, select_part) = renamed_sql.split_at(as_select_pos);
                            let mut renamed_select_part = select_part.to_string();

                            // Now replace table names only in the SELECT part
                            for (old_name, new_name) in &table_renames {
                                renamed_select_part = renamed_select_part.replace(old_name, new_name);
                            }

                            renamed_sql = format!("{}{}", create_matview_part, renamed_select_part);
                        }

                        let create_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            Spi::run(&renamed_sql)
                        }));
                        match create_result {
                            Ok(Ok(())) => {},
                            Ok(Err(e)) => {
                                return Outcome::Bug(format!("create matview {} failed: {e:?}", src_obj.name));
                            }
                            Err(_) => {
                                return Outcome::Bug(format!("create matview {} panicked: SQL: {}", src_obj.name, renamed_sql));
                            }
                        }
                    }
                }
                SourceObjectKind::SubImv => {
                    // Extract the SELECT body from the placeholder define_sql.
                    // The placeholder format is: "CREATE REFLEX IMV <name> AS SELECT ..."
                    if let Some(define_sql) = &src_obj.define_sql {
                        // Find " AS " and extract the SELECT part
                        if let Some(as_pos) = define_sql.find(" AS ") {
                            let select_body = &define_sql[as_pos + 4..];
                            // Rename table references in the select body
                            let mut renamed_select_body = select_body.to_string();
                            for (old_name, new_name) in &table_renames {
                                renamed_select_body = renamed_select_body.replace(old_name, new_name);
                            }
                            // Get unique columns from the base case
                            let keys = case.unique_columns.join(",");
                            let create_result = Spi::get_one::<&str>(&format!(
                                "SELECT create_reflex_ivm('{}', $body${}$body$, '{}', NULL, 'IMMEDIATE', NULL)",
                                src_obj.name, renamed_select_body, keys
                            ));
                            match create_result {
                                Ok(Some(msg)) => {
                                    if msg.contains(crate::REFLEX_UNSUPPORTED_TAG) {
                                        return Outcome::Skip(format!("SubImv {} creation returned UNSUPPORTED: {}", src_obj.name, msg));
                                    } else if !msg.starts_with("CREATE REFLEX") {
                                        return Outcome::Bug(format!("unexpected SubImv create return: {}", msg));
                                    }
                                }
                                e => return Outcome::Bug(format!("SubImv {} create_reflex_ivm call error: {:?}", src_obj.name, e)),
                            }
                        } else {
                            return Outcome::Bug(format!("SubImv {} define_sql missing ' AS ': {}", src_obj.name, define_sql));
                        }
                    }
                }
            }
        }

        // Verify that all non-Table source objects were created successfully
        for src_obj in &pc.source_objects {
            if !matches!(src_obj.kind, SourceObjectKind::Table) {
                // Check if this object exists in information_schema
                let check_sql = format!(
                    "SELECT EXISTS(SELECT 1 FROM information_schema.views WHERE table_name = '{}' AND table_schema = 'public')",
                    src_obj.name.replace("'", "''")
                );
                match Spi::get_one::<bool>(&check_sql) {
                    Ok(Some(true)) => {
                        // Object exists, good
                    }
                    _ => {
                        // Object doesn't exist - this is a problem!
                        return Outcome::Bug(format!(
                            "source object {} ({:?}) was not created; expected it to exist for oracle MV",
                            src_obj.name, src_obj.kind
                        ));
                    }
                }
            }
        }

        let mv = format!("mv{suffix}");
        let imv = format!("imv{suffix}");

        // Step 3: Build the DML statements for the DO block.
        let mut dml_lines = Vec::new();
        for txn in &case.dml {
            for stmt in &txn.statements {
                let cols = cols_of(&case, match stmt {
                    DmlStmt::Insert { table, .. }
                    | DmlStmt::Delete { table, .. }
                    | DmlStmt::Update { table, .. }
                    | DmlStmt::Truncate { table } => table,
                });
                let sql = render::dml_sql(stmt, &|_t: &str| cols.clone());
                dml_lines.push(format!("    {sql};"));
            }
        }
        let dml_block = dml_lines.join("\n");

        // Step 4: Build flush lines if deferred.
        let mut flush_lines = Vec::new();
        if case.deferred {
            for t in &case.tables {
                flush_lines.push(format!("    PERFORM reflex_flush_deferred('{}');", t.name));
            }
        }
        let flush_block = flush_lines.join("\n");

        // Step 5: Build maintenance block based on source_is_refresh_driven.
        let maint_block = if pc.source_is_refresh_driven {
            // For refresh-driven sources (MatView), refresh them explicitly
            let mut maint_lines = Vec::new();
            for src_obj in &pc.source_objects {
                if matches!(src_obj.kind, SourceObjectKind::MatView) {
                    maint_lines.push(format!("    REFRESH MATERIALIZED VIEW {};", src_obj.name));
                    maint_lines.push(format!("    PERFORM refresh_imv_depending_on('{}');", src_obj.name));
                }
            }
            maint_lines.join("\n")
        } else {
            // For table sources with immediate triggers, no explicit maintenance needed.
            // For deferred sources, flush_block already handles it.
            String::new()
        };

        // Step 6: Prepare the oracle MV body.
        // FIX: Don't use rename_case()'s renamed body directly because it renames
        // source object names (View/MatView) which are NOT actually renamed in SQL.
        // Only base tables get the _pfz suffix. Source objects keep original names.
        let mut body = case.select_body.rendered_sql.clone();

        // Undo source object renames that rename_case() applied, BUT ONLY for non-Table sources.
        // Table sources DO get the _pfz suffix applied (they are base tables that were created with it).
        // View/MatView/SubImv sources do NOT get the suffix (they are created with original names).
        for src_obj in &pc.source_objects {
            if matches!(src_obj.kind, SourceObjectKind::Table) {
                // Table sources: the body correctly has the _pfz suffix, no undo needed.
                continue;
            }
            // For View/MatView/SubImv: undo the rename that rename_case() applied.
            let wrongly_renamed = format!("{}{}", src_obj.name, &suffix);
            // Only replace if the wrongly_renamed version exists in the body
            if body.contains(&wrongly_renamed) {
                body = body.replace(&wrongly_renamed, &src_obj.name);
            }
        }

        // Step 7: Now create oracle MV with the corrected body (after undo-rename).
        let oracle_body = SelectBody {
            rendered_sql: body.clone(),
        };
        let create_mv_sql_str = render::create_mv_sql(&mv, &oracle_body);

        // Try to create the oracle MV. Catch panics to provide better diagnostics.
        // Note: pgrx's Spi::run can panic on SQL errors instead of returning Err.
        let mv_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            Spi::run(&create_mv_sql_str)
        }));

        match mv_result {
            Ok(Ok(())) => {
                // Oracle MV created successfully
            }
            Ok(Err(e)) => {
                return Outcome::Bug(format!("create oracle mv error: {e:?}"));
            }
            Err(_panic_obj) => {
                // Panic during oracle MV creation. The oracle body SQL may be invalid.
                // Try to execute the SELECT part alone to see if the query is valid
                let select_part = create_mv_sql_str.trim_start_matches("CREATE MATERIALIZED VIEW ")
                    .split(" AS ")
                    .nth(1)
                    .unwrap_or("");

                let test_select = format!("SELECT 1 WHERE EXISTS ({})", select_part);
                let test_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    Spi::get_one::<bool>(&test_select)
                }));

                let diag = match test_result {
                    Ok(Ok(_)) => "SELECT query works".to_string(),
                    Ok(Err(e)) => format!("SELECT query failed: {e:?}"),
                    Err(_) => "SELECT query panicked".to_string(),
                };

                return Outcome::Bug(format!("create oracle mv panicked: {} | SQL: {}", diag, create_mv_sql_str));
            }
        }

        // Build the diff subquery.
        let has_float = case.output_columns.iter().any(|c| c.ty.is_float());
        let diff_from = if has_float {
            fuzz_model::oracle_pure::float_diff_from_where(&mv, &imv, &case.unique_columns, &case.output_columns)
        } else {
            diff_subquery(&mv, &imv)
        };

        // Construct the DO block as a string.
        let keys = case.unique_columns.join(",");

        // Honor the Lifecycle axis for the drop step. CascadeDrop exercises the
        // cascade path; CreateMutateDrop/Partitioned exercise a plain drop, which
        // must still clean a decomposed view's internal sub-IMVs on its own.
        let cascade_arg = match pc.axes.lifecycle {
            fuzz_model::axes::Lifecycle::CascadeDrop => ", true",
            fuzz_model::axes::Lifecycle::CreateMutateDrop | fuzz_model::axes::Lifecycle::Partitioned => {
                ""
            }
        };

        // Build a PL/pgSQL function that encodes the result as JSON for transport across SPI.
        let func_name = format!("oracle_planned_func_{}", seq);
        let create_func_sql = format!(
            r#"CREATE OR REPLACE FUNCTION public.{} () RETURNS text AS $func$
DECLARE
  v_msg text;
  v_diff bigint;
  v_drop_result text;
  v_orphans bigint;
  v_status text := 'MATCH';
  v_detail text := '';
BEGIN
  v_msg := create_reflex_ivm('{}', $reflexbody${}$reflexbody$, '{}', NULL, 'IMMEDIATE', NULL);
  IF position('{}' in v_msg) > 0 THEN
    v_status := 'SKIP';
    v_detail := v_msg;
  ELSIF v_msg NOT LIKE 'CREATE REFLEX%%' THEN
    v_status := 'BUG';
    v_detail := 'unexpected create return: ' || v_msg;
  ELSE
{}
{}
{}
    REFRESH MATERIALIZED VIEW {};

    SELECT count(*)::bigint INTO v_diff FROM {};

    IF v_diff > 0 THEN
      v_status := 'BUG';
      v_detail := v_diff || ' mismatched rows';
    ELSE
      -- Drop the IMV and check for orphans
      v_drop_result := drop_reflex_ivm('{}'{});
      IF position('DROP' in v_drop_result) = 0 THEN
        v_status := 'BUG';
        v_detail := 'drop did not report DROP: ' || v_drop_result;
      ELSE
        SELECT count(*)::bigint INTO v_orphans
        FROM pg_class
        WHERE relname LIKE '%{1}%'
              AND relkind IN ('r', 'i');
        IF v_orphans > 0 THEN
          v_status := 'BUG';
          v_detail := v_orphans || ' orphan objects after drop: ' ||
                      (SELECT string_agg(relname, ', ') FROM pg_class
                       WHERE relname LIKE '%{1}%' AND relkind IN ('r', 'i'));
        END IF;
      END IF;
    END IF;
  END IF;

  RETURN v_status || '|||' || v_detail;
EXCEPTION WHEN OTHERS THEN
  RETURN 'BUG' || '|||' || ('codegen exception: ' || SQLERRM);
END;
$func$ LANGUAGE plpgsql;
"#,
            func_name,
            imv,               // create_reflex_ivm arg
            body,              // $reflexbody content
            keys,              // third arg to create_reflex_ivm
            crate::REFLEX_UNSUPPORTED_TAG,  // position check
            dml_block,         // DML statements
            flush_block,       // flush statements (may be empty)
            maint_block,       // maintenance block
            mv,                // REFRESH target
            diff_from,         // SELECT FROM (exact or float-tolerant)
            imv,               // drop_reflex_ivm arg
            cascade_arg,       // cascade flag
        );

        // Create the function.
        if let Err(e) = Spi::run(&create_func_sql) {
            return Outcome::Bug(format!("create function error: {e:?}"));
        }

        // Call the function and parse the result.
        // Note: Spi::get_one can panic if the query execution raises an exception.
        // We're relying on the outer catch_unwind to catch these panics.
        let call_func_sql = format!("SELECT {}();", func_name);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            Spi::get_one::<&str>(&call_func_sql)
        }));

        match result {
            Ok(Ok(Some(result_str))) => {
                let parts: Vec<&str> = result_str.splitn(2, "|||").collect();
                let status = if !parts.is_empty() { parts[0] } else { "UNKNOWN" };
                let detail = if parts.len() > 1 { parts[1].to_string() } else { String::new() };
                match status {
                    "MATCH" => Outcome::Match,
                    "SKIP" => Outcome::Skip(detail),
                    "BUG" => Outcome::Bug(detail),
                    _ => Outcome::Bug(format!("unknown status: {}", status)),
                }
            }
            Ok(Ok(None)) => Outcome::Bug("function returned NULL".to_string()),
            Ok(Err(e)) => Outcome::Bug(format!("function call error: {:?}", e)),
            Err(panic_e) => {
                let msg = if let Some(s) = panic_e.downcast_ref::<String>() {
                    format!("string: {}", s)
                } else if let Some(s) = panic_e.downcast_ref::<&str>() {
                    format!("&str: {}", s)
                } else {
                    "unknown panic type".to_string()
                };
                Outcome::Bug(format!("function call panicked: {msg}"))
            }
        }
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_test]
fn oracle_recovery_survives_internal_exception() {
    // Prove that the DO/EXCEPTION pattern leaves the outer transaction clean.
    // We deliberately raise an exception in a DO block, then call evaluate() on
    // a normal case to prove the outer transaction is still usable.
    Spi::run("DO $$ BEGIN RAISE EXCEPTION 'boom'; EXCEPTION WHEN OTHERS THEN NULL; END $$")
        .unwrap();
    // outer txn must still be usable:
    use proptest::strategy::{Strategy, ValueTree};
    use proptest::test_runner::TestRunner;
    let mut runner = TestRunner::default();
    let c = generate::fuzz_case().new_tree(&mut runner).unwrap().current();
    match oracle::evaluate(&c) {
        oracle::Outcome::Match | oracle::Outcome::Skip(_) => {}
        oracle::Outcome::Bug(msg) => {
            panic!("expected Match/Skip after internal exception, got bug: {msg}")
        }
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_test]
fn oracle_matches_on_a_simple_generated_case() {
    use proptest::strategy::{Strategy, ValueTree};
    use proptest::test_runner::TestRunner;

    let mut runner = TestRunner::default();
    let case = generate::fuzz_case()
        .new_tree(&mut runner)
        .unwrap()
        .current();
    match oracle::evaluate(&case) {
        oracle::Outcome::Match | oracle::Outcome::Skip(_) => {}
        oracle::Outcome::Bug(msg) => {
            panic!(
                "expected match/skip on simple case, got bug: {msg}\n{}",
                oracle::repro_sql(&case)
            )
        }
    }
}

/// Test evaluate_planned with a simple table-passthrough case.
#[cfg(any(test, feature = "pg_test"))]
#[pg_test]
fn evaluate_planned_table_passthrough_matches_and_drops_clean() {
    use fuzz_model::axes::{plan_case, Axes, FilterMode, Lifecycle, MutationSpread, QueryShape, RefreshMode, SourceKind, UniqueCols};
    use fuzz_model::model::ColType;
    let a = Axes {
        source: SourceKind::Table,
        shape: QueryShape::Passthrough,
        refresh: RefreshMode::Immediate,
        agg: None,
        measure_ty: ColType::Numeric,
        unique: UniqueCols::Absent,
        lifecycle: Lifecycle::CreateMutateDrop,
        filter: FilterMode::None,
        spread: MutationSpread::SingleSource,
    };
    let pc = plan_case(&a, 9001).unwrap();
    match oracle::evaluate_planned(&pc) {
        oracle::Outcome::Match => {}
        other => panic!("expected Match, got {other:?}"),
    }
}

fn fuzz_case_count() -> u32 {
    std::env::var("PG_REFLEX_FUZZ_CASES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(64)
}

/// FIXED finding #1 — see docs/fuzz-findings.md. Root cause: the former data-probe
/// marked the LEFT-joined column NOT NULL from a create-time intermediate that
/// happened to be null-free, so MERGE maintenance dropped a later unmatched
/// primary-insert row. Fixed by `infer_not_null_columns`, which promotes a column
/// only when the query structurally guarantees non-NULL.
///
/// A two-table LEFT JOIN aggregate, after inserting a new PRIMARY-side (t0) row
/// that matches NO secondary row, must KEEP that row with the secondary columns NULL.
    #[cfg(any(test, feature = "pg_test"))]
    #[pg_test]
    fn finding_1_leftjoin_unmatched_primary_insert_drops_row() {
        Spi::run("CREATE TABLE f1_t0 (id int primary key, m numeric, d text)").unwrap();
        Spi::run("CREATE TABLE f1_t1 (id int primary key, fk int, w numeric)").unwrap();

        // Seed ONLY matched rows: every t0.id has a matching agg.g (= t1.fk), so
        // the LEFT-joined column `sw` is NULL-free in the create-time intermediate.
        // This is the precondition that makes the data-probe optimization wrongly
        // mark `sw` NOT NULL — the trigger for finding #1. (A seed where some rows
        // are already unmatched at create time would leave `sw` already-NULL and
        // never exercise the bug.)
        Spi::run("INSERT INTO f1_t0 VALUES (0,0.0,'g0'),(1,1.1,'g1'),(2,2.2,'g2')").unwrap();
        Spi::run("INSERT INTO f1_t1 VALUES (10,0,5.0),(11,1,6.0),(12,2,7.0)").unwrap();

        // Create the MV
        let body = "WITH agg AS (SELECT fk AS g, SUM(w) AS sw FROM f1_t1 GROUP BY fk) \
                    SELECT f1_t0.id, SUM(f1_t0.m) AS s, a.sw FROM f1_t0 LEFT JOIN agg a ON a.g = f1_t0.id GROUP BY f1_t0.id, a.sw";
        Spi::run(&format!("CREATE MATERIALIZED VIEW f1_mv AS {body}")).unwrap();

        // Create the IMV
        let r = crate::create_reflex_ivm("f1_imv", body, Some("id"), None, None, None);
        assert!(r.starts_with("CREATE REFLEX"), "IMV creation failed: {r}");

        // Insert a new primary-side row (id=8) that matches NO secondary row →
        // its `sw` must be NULL in the IMV, but a NOT-NULL-marked `sw` would make
        // MERGE maintenance drop the row entirely.
        Spi::run("INSERT INTO f1_t0 (id, m, d) VALUES (8, 3.2, 'g0')").unwrap();

        // Refresh the MV
        Spi::run("REFRESH MATERIALIZED VIEW f1_mv").unwrap();

        // Compare the two views
        let diff = Spi::get_one::<i64>(
            "SELECT count(*)::bigint FROM ( \
               (SELECT * FROM f1_mv EXCEPT SELECT * FROM f1_imv) UNION ALL \
               (SELECT * FROM f1_imv EXCEPT SELECT * FROM f1_mv)) d",
        )
        .unwrap()
        .unwrap();

        // Get the row sets for debugging
        let mv_rows = Spi::get_one::<String>(
            "SELECT string_agg(CAST((id, s, sw) AS text), '; ' ORDER BY id) FROM f1_mv",
        )
        .unwrap()
        .unwrap_or_else(|| "empty".into());

        let imv_rows = Spi::get_one::<String>(
            "SELECT string_agg(CAST((id, s, sw) AS text), '; ' ORDER BY id) FROM f1_imv",
        )
        .unwrap()
        .unwrap_or_else(|| "empty".into());

        assert_eq!(diff, 0, "finding #1: IMV diverged from MV by {diff} rows\nMV: {mv_rows}\nIMV: {imv_rows}");
    }

    /// FIXED finding #2 — see docs/fuzz-findings.md. Active regression test.
    ///
    /// A single-table passthrough view with DEFERRED incremental maintenance used to
    /// fail during reflex_flush_deferred() with "duplicate key value violates unique
    /// constraint" when one batch INSERTed a new key and then UPDATEd that same key:
    /// the flush emitted both the new-side and old-side delta for the key. Fixed by
    /// netting the two delta sides per unique key before the MERGE (commit ae1faa0).
    #[cfg(any(test, feature = "pg_test"))]
    #[pg_test]
    fn finding_2_deferred_mode_duplicate_key_violation() {
        // Minimal verified repro: in DEFERRED mode, INSERTing a new key and then
        // UPDATEing that SAME key within one deferred batch (before flush) makes the
        // flush MERGE violate the target unique constraint __reflex_uk_*. (A batch of
        // only-updates-of-existing-keys + only-inserts-of-new-keys flushes fine; the
        // trigger is insert+update of the SAME key in one batch.) flush takes the
        // SOURCE table name (f2_t0), not the IMV name.
        Spi::run("CREATE TABLE f2_t0 (id int primary key, m numeric)").unwrap();
        Spi::run("INSERT INTO f2_t0 VALUES (1, 1.0)").unwrap();
        let body = "SELECT id, m FROM f2_t0";
        Spi::run(&format!("CREATE MATERIALIZED VIEW f2_mv AS {body}")).unwrap();
        let r = crate::create_reflex_ivm("f2_imv", body, Some("id"), None, Some("DEFERRED"), None);
        assert!(r.starts_with("CREATE REFLEX"), "IMV creation failed: {r}");

        // One deferred batch: insert id=2, then update id=2.
        Spi::run("INSERT INTO f2_t0 VALUES (2, 5.0)").unwrap();
        Spi::run("UPDATE f2_t0 SET m = m + 1 WHERE id = 2").unwrap();

        // flush must NOT raise duplicate-key; IMV must then match a refreshed MV.
        Spi::run("SELECT reflex_flush_deferred('f2_t0')").unwrap();
        Spi::run("REFRESH MATERIALIZED VIEW f2_mv").unwrap();

        let diff = Spi::get_one::<i64>(
            "SELECT count(*)::bigint FROM ( \
               (SELECT * FROM f2_mv EXCEPT SELECT * FROM f2_imv) UNION ALL \
               (SELECT * FROM f2_imv EXCEPT SELECT * FROM f2_mv)) d",
        )
        .unwrap()
        .unwrap();

        assert_eq!(diff, 0, "finding #2: IMV diverged from MV by {diff} rows after DEFERRED flush");
    }

/// FIXED finding #3 — see docs/fuzz-findings.md. The former data-probe marked a
/// group-by column NOT NULL whenever the create-time data happened to be
/// null-free, even on a plain nullable column with no INNER-join / filter
/// guarantee. A later legitimately-NULL group was then dropped by `=`-matching
/// MERGE maintenance. Fixed by inferring NOT NULL only from query structure.
///
/// `GROUP BY d` on a nullable `d` that is null-free at create time, then inserting
/// NULL-`d` rows, must keep the NULL group in the IMV.
#[cfg(any(test, feature = "pg_test"))]
#[pg_test]
fn finding_3_nullable_groupby_key_drops_null_group() {
    Spi::run("CREATE TABLE g3_t0 (id int primary key, m numeric, d text)").unwrap();
    // Seed with NO nulls in `d`: the precondition that made the old data-probe
    // wrongly mark `d` NOT NULL.
    Spi::run("INSERT INTO g3_t0 VALUES (1,1.0,'a'),(2,2.0,'b'),(3,3.0,'a')").unwrap();
    let body = "SELECT d, SUM(m) AS s FROM g3_t0 GROUP BY d";
    Spi::run(&format!("CREATE MATERIALIZED VIEW g3_mv AS {body}")).unwrap();
    let r = crate::create_reflex_ivm("g3_imv", body, Some("d"), None, None, None);
    assert!(r.starts_with("CREATE REFLEX"), "IMV creation failed: {r}");
    // Introduce a legitimate NULL group.
    Spi::run("INSERT INTO g3_t0 VALUES (4,4.0,NULL),(5,5.0,NULL)").unwrap();
    Spi::run("REFRESH MATERIALIZED VIEW g3_mv").unwrap();
    let diff = Spi::get_one::<i64>(
        "SELECT count(*)::bigint FROM ( \
           (SELECT * FROM g3_mv EXCEPT SELECT * FROM g3_imv) UNION ALL \
           (SELECT * FROM g3_imv EXCEPT SELECT * FROM g3_mv)) d",
    )
    .unwrap()
    .unwrap();
    let mv = Spi::get_one::<String>("SELECT string_agg(CAST((d,s) AS text), '; ' ORDER BY d NULLS LAST) FROM g3_mv").unwrap().unwrap_or_default();
    let imv = Spi::get_one::<String>("SELECT string_agg(CAST((d,s) AS text), '; ' ORDER BY d NULLS LAST) FROM g3_imv").unwrap().unwrap_or_default();
    assert_eq!(diff, 0, "finding #3: nullable-groupby diverged by {diff}\nMV:  {mv}\nIMV: {imv}");
}

/// FIXED finding #4 (fuzz-harness false positive) — see docs/fuzz-findings.md.
/// A filtered float-aggregate with a NULL group is maintained correctly by
/// pg_reflex (exact NULL-safe diff is 0), but the harness's float comparator used
/// a `FULL JOIN ... ON a.k = b.k` that is not NULL-safe, so the NULL group showed
/// as two phantom unmatched rows. Guards both the IMV correctness and the now
/// NULL-safe `float_diff_from_where`.
#[cfg(any(test, feature = "pg_test"))]
#[pg_test]
fn finding_4_filtered_float_aggregate_null_group_diff_safe() {
    Spi::run("CREATE TABLE q4_t0 (id int primary key, m numeric, d text, f float8, x text)").unwrap();
    let body = "SELECT d, SUM(m) AS s, COUNT(*) AS c, AVG(m) AS avg_m, SUM(f) AS sf FROM q4_t0 WHERE id % 2 = 0 GROUP BY d";
    Spi::run(&format!("CREATE MATERIALIZED VIEW q4_mv AS {body}")).unwrap();
    // Create IMV on the EMPTY table (mirrors the fuzzer's incremental path).
    let r = crate::create_reflex_ivm("q4_imv", body, Some("d"), None, None, None);
    assert!(r.starts_with("CREATE REFLEX"), "IMV creation failed: {r}");
    // Apply DML incrementally, including a NULL-group row (id=200, d=NULL).
    Spi::run("INSERT INTO q4_t0 (id,m,d,f,x) VALUES (0,0.0,'g0',0.0,'g0'),(1,1.1,'g1',1.1,'g1'),(2,2.2,'g2',2.2,'g2'),(3,3.0,'g3',3.0,'g3'),(4,4.1,'g0',4.1,'g0'),(5,0.2,'g1',0.2,'g1'),(6,1.0,'g2',1.0,'g2'),(7,2.1,'g3',2.1,'g3')").unwrap();
    Spi::run("INSERT INTO q4_t0 (id,m,d,f,x) VALUES (100,0.1,'g0',0.1,'g0')").unwrap();
    Spi::run("INSERT INTO q4_t0 (id,m,d,f,x) VALUES (200,0.2,NULL,NULL,NULL)").unwrap();
    Spi::run("UPDATE q4_t0 SET m = m + 1 WHERE id % 2 = 0").unwrap();
    Spi::run("DELETE FROM q4_t0 WHERE id = 0").unwrap();
    Spi::run("REFRESH MATERIALIZED VIEW q4_mv").unwrap();

    // pg_reflex is correct: the exact NULL-safe diff is 0.
    let exact_diff = Spi::get_one::<i64>(
        "SELECT count(*)::bigint FROM ((SELECT * FROM q4_mv EXCEPT SELECT * FROM q4_imv) \
         UNION ALL (SELECT * FROM q4_imv EXCEPT SELECT * FROM q4_mv)) d").unwrap().unwrap();
    assert_eq!(exact_diff, 0, "pg_reflex diverged on filtered float aggregate with NULL group");

    // The harness float comparator must also see 0 (it must be NULL-safe).
    let cols = [
        model::Column { name: "d".into(), ty: model::ColType::Text, nullable: true },
        model::Column { name: "s".into(), ty: model::ColType::Numeric, nullable: true },
        model::Column { name: "c".into(), ty: model::ColType::BigInt, nullable: false },
        model::Column { name: "avg_m".into(), ty: model::ColType::Float8, nullable: true },
        model::Column { name: "sf".into(), ty: model::ColType::Float8, nullable: true },
    ];
    let float_diff = Spi::get_one::<i64>(&format!(
        "SELECT count(*)::bigint FROM {}",
        oracle::float_diff_from_where("q4_mv", "q4_imv", &["d".into()], &cols)
    )).unwrap().unwrap();
    assert_eq!(float_diff, 0, "harness float_diff_from_where is not NULL-group-safe");
}

/// Perf-preservation guard for the structural NOT-NULL inference. A
/// catalog-NULLable column made non-NULL by an INNER-join equi-condition (the
/// yse.ivm_sop_forecast_view 405 s shape) MUST still be promoted to NOT NULL so
/// MERGE maintenance keeps `=` matching (index-friendly). If this regresses, the
/// inference lost the INNER-join equi-key case.
#[cfg(any(test, feature = "pg_test"))]
#[pg_test]
fn inner_join_equikey_promoted_not_null() {
    Spi::run("CREATE TABLE ij_dp (id int primary key)").unwrap();
    // dpid is catalog-NULLABLE, but the INNER join below makes it non-NULL.
    Spi::run("CREATE TABLE ij_ss (id int primary key, dpid int, m numeric)").unwrap();
    Spi::run("INSERT INTO ij_dp VALUES (10),(20),(30)").unwrap();
    Spi::run("INSERT INTO ij_ss VALUES (1,10,1.0),(2,20,2.0),(3,10,3.0)").unwrap();
    let body = "SELECT ss.dpid, SUM(ss.m) AS s FROM ij_ss ss \
                INNER JOIN ij_dp dp ON dp.id = ss.dpid GROUP BY ss.dpid";
    let r = crate::create_reflex_ivm("ij_imv", body, Some("dpid"), None, None, None);
    assert!(r.starts_with("CREATE REFLEX"), "IMV creation failed: {r}");

    // The INNER-join equi-key dpid must be inferred NOT NULL.
    let promoted = Spi::get_one::<bool>(
        "SELECT COALESCE((aggregations::jsonb->'not_null_columns') @> '[\"dpid\"]'::jsonb, false) \
         FROM public.__reflex_ivm_reference WHERE name = 'ij_imv'",
    )
    .unwrap()
    .unwrap_or(false);
    assert!(
        promoted,
        "INNER-join equi-key 'dpid' was not inferred NOT NULL — 405s index optimization lost"
    );
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_test]
fn fuzz_differential_exact() {
    use proptest::test_runner::{Config, TestCaseError, TestRunner};

    use std::cell::RefCell;

    let cfg = Config {
        cases: fuzz_case_count(),
        failure_persistence: None,
        ..Config::default()
    };
    let mut runner = TestRunner::new(cfg);
    let first_bug = RefCell::new(None);
    let result = runner.run(&generate::fuzz_case(), |case| match oracle::evaluate(&case) {
        oracle::Outcome::Match | oracle::Outcome::Skip(_) => Ok(()),
        oracle::Outcome::Bug(msg) => {
            if first_bug.borrow().is_none() {
                *first_bug.borrow_mut() = Some((msg.clone(), oracle::repro_sql(&case)));
            }
            Err(TestCaseError::fail(format!(
                "{msg}\n--- minimal repro ---\n{}",
                oracle::repro_sql(&case)
            )))
        }
    });
    if let Err(e) = result {
        if let Some((msg, repro)) = first_bug.into_inner() {
            panic!("differential fuzz found a bug:\n{msg}\n--- minimal repro ---\n{repro}");
        } else {
            panic!("differential fuzz failed with no bug captured. proptest error: {e:?}");
        }
    }
}

/// Random axes-driven differential proptest. Samples valid axis assignments and
/// runs each through the planned oracle. Complements the deterministic all-pairs
/// gate by reaching higher-order interactions; case count is env-driven
/// (`PG_REFLEX_FUZZ_CASES`).
#[cfg(any(test, feature = "pg_test"))]
#[pg_test]
fn fuzz_planned_random() {
    use proptest::strategy::{Strategy, ValueTree};
    use proptest::test_runner::TestRunner;
    let mut runner = TestRunner::default();
    let n = fuzz_case_count();
    for i in 0..n {
        let a = fuzz_model::generate::axes_strategy()
            .new_tree(&mut runner)
            .unwrap()
            .current();
        if let Some(pc) = fuzz_model::axes::plan_case(&a, 300_000 + i as u64) {
            if let oracle::Outcome::Bug(d) = oracle::evaluate_planned(&pc) {
                panic!(
                    "random axes {a:?} => BUG: {d}\n--- minimal repro ---\n{}",
                    fuzz_model::oracle_pure::repro_sql(&pc.case)
                );
            }
        }
    }
}

/// Bug 1: COALESCE over a joined GROUP BY key. Either it builds and matches
/// the MV, or pg_reflex deliberately rejects it (tagged). It must NOT raise
/// a Postgres exception (the old failure: column "sx" does not exist).
#[cfg(any(test, feature = "pg_test"))]
#[pg_test]
fn regression_bug1_coalesce_over_joined_group_key() {
    use pgrx::prelude::*;
    Spi::run("CREATE TABLE b1_t(g int primary key, v int)").unwrap();
    Spi::run("CREATE TABLE b1_a(g int, x int)").unwrap();
    Spi::run("INSERT INTO b1_t VALUES (1,10),(2,20)").unwrap();
    Spi::run("INSERT INTO b1_a VALUES (1,5),(1,7)").unwrap();
    let body = "WITH agg AS (SELECT g, SUM(x) AS sx FROM b1_a GROUP BY g) \
                SELECT t.g, SUM(t.v) AS s, COALESCE(a.sx, 0) AS sx0 \
                FROM b1_t t LEFT JOIN agg a ON a.g = t.g GROUP BY t.g, a.sx";
    let r = crate::create_reflex_ivm("b1_imv", body, Some("g"), None, None, None);
    assert!(
        r.starts_with("CREATE REFLEX") || r.contains(crate::REFLEX_UNSUPPORTED_TAG),
        "Bug 1 regressed: create must succeed or be cleanly rejected, got: {r}"
    );
}

/// Bug 2: carried EXISTS with a boolean conjunct. Old failure: column ...
/// "is of type numeric but expression is of type boolean".
#[cfg(any(test, feature = "pg_test"))]
#[pg_test]
fn regression_bug2_exists_with_boolean_conjunct() {
    use pgrx::prelude::*;
    Spi::run("CREATE TABLE b2_t(g int primary key, v int)").unwrap();
    Spi::run("CREATE TABLE b2_pt(product_id int, is_active bool)").unwrap();
    Spi::run("INSERT INTO b2_t VALUES (1,10),(2,20)").unwrap();
    Spi::run("INSERT INTO b2_pt VALUES (1,true)").unwrap();
    let body = "SELECT t.g, SUM(t.v) AS s, \
                EXISTS(SELECT 1 FROM b2_pt c WHERE c.product_id = t.g AND c.is_active) AS flag \
                FROM b2_t t GROUP BY t.g";
    let r = crate::create_reflex_ivm("b2_imv", body, Some("g"), None, None, None);
    assert!(
        r.starts_with("CREATE REFLEX") || r.contains(crate::REFLEX_UNSUPPORTED_TAG),
        "Bug 2 regressed: create must succeed or be cleanly rejected, got: {r}"
    );
}

/// Bug 3 (commit 4d1d382): COUNT over a LEFT JOIN — secondary-side
/// incremental maintenance. Build, mutate the secondary side, and assert
/// the IMV matches a refreshed MV.
#[cfg(any(test, feature = "pg_test"))]
#[pg_test]
fn regression_bug3_count_over_left_join_secondary_side() {
    use pgrx::prelude::*;
    Spi::run("CREATE TABLE b3_t(g int primary key, v int)").unwrap();
    Spi::run("CREATE TABLE b3_s(g int, w int)").unwrap();
    Spi::run("INSERT INTO b3_t VALUES (1,10),(2,20),(3,30)").unwrap();
    Spi::run("INSERT INTO b3_s VALUES (1,1),(1,1),(2,1)").unwrap();
    let body = "SELECT t.g, COUNT(s.w) AS c FROM b3_t t \
                LEFT JOIN b3_s s ON s.g = t.g GROUP BY t.g";
    Spi::run(&format!("CREATE MATERIALIZED VIEW b3_mv AS {body}")).unwrap();
    let r = crate::create_reflex_ivm("b3_imv", body, Some("g"), None, None, None);
    assert!(r.starts_with("CREATE REFLEX"), "create failed: {r}");
    // Mutate the secondary (LEFT) side.
    Spi::run("INSERT INTO b3_s VALUES (3,1),(1,1)").unwrap();
    Spi::run("DELETE FROM b3_s WHERE g = 2").unwrap();
    Spi::run("REFRESH MATERIALIZED VIEW b3_mv").unwrap();
    let diff = Spi::get_one::<i64>(
        "SELECT count(*)::bigint FROM ( \
           (SELECT * FROM b3_mv EXCEPT SELECT * FROM b3_imv) UNION ALL \
           (SELECT * FROM b3_imv EXCEPT SELECT * FROM b3_mv)) d",
    )
    .unwrap()
    .unwrap();
    assert_eq!(diff, 0, "Bug 3 regressed: IMV diverged from MV by {diff} rows");
}

// ---------------------------------------------------------------------------
// Creation-bug class regression rows (this session's 2-way interaction bugs).
//
// Each pins one feature interaction from the b41f4fb/1.6.5 bug family to a
// deterministic, axes-driven case run through the full differential oracle
// (create → mutate → refresh → diff → drop → orphan-check). The gate is scoped
// to Table sources (see fuzz_model::axes::all_source), so each row drives the
// machinery the bug lived in (CTE/set-op decomposition, deferred maintenance,
// non-numeric aggregate output typing, partition propagation, cascade vs plain
// drop) over Table sources rather than via View/MatView/CteSubImv *sources*,
// which are out of scope. A revert of the corresponding fix turns the row red.
// ---------------------------------------------------------------------------

/// Asserts the planned case is valid and the oracle does not flag a divergence.
#[cfg(any(test, feature = "pg_test"))]
fn assert_planned_matches(a: &fuzz_model::axes::Axes, seq: u64, label: &str) {
    let pc = fuzz_model::axes::plan_case(a, seq)
        .unwrap_or_else(|| panic!("{label}: axes must be valid: {a:?}"));
    match oracle::evaluate_planned(&pc) {
        oracle::Outcome::Match | oracle::Outcome::Skip(_) => {}
        oracle::Outcome::Bug(d) => panic!("{label} regressed: {d}"),
    }
}

/// Bug 1 analog — a CTE-decomposed IMV under DEFERRED maintenance. The internal
/// sub-IMV is created with a quoted `<view>__cte_…` source; the quoting/trigger
/// path (canonical_source) must round-trip so deferred maintenance matches.
#[cfg(any(test, feature = "pg_test"))]
#[pg_test]
fn regression_decomp_cte_deferred_creates() {
    use fuzz_model::axes::*;
    use fuzz_model::model::ColType;
    let a = Axes {
        source: SourceKind::Table,
        shape: QueryShape::CteDecomposed,
        refresh: RefreshMode::Deferred,
        agg: Some(AggFn::Sum),
        measure_ty: ColType::Numeric,
        unique: UniqueCols::Absent,
        lifecycle: Lifecycle::CreateMutateDrop,
        filter: FilterMode::None,
        spread: MutationSpread::SingleSource,
    };
    assert_planned_matches(&a, 200_001, "decomposed CTE × deferred");
}

/// Bug 2 analog — an explicit unique key threaded through a JOIN-aggregate IMV.
/// Locks that the provided key reaches the create path (the threading fix),
/// keeping the IMV populated and matching the MV.
#[cfg(any(test, feature = "pg_test"))]
#[pg_test]
fn regression_join_provided_unique_key_threads() {
    use fuzz_model::axes::*;
    use fuzz_model::model::ColType;
    let a = Axes {
        source: SourceKind::Table,
        shape: QueryShape::JoinInner,
        refresh: RefreshMode::Immediate,
        agg: Some(AggFn::Sum),
        measure_ty: ColType::Numeric,
        unique: UniqueCols::Provided,
        lifecycle: Lifecycle::CreateMutateDrop,
        filter: FilterMode::None,
        spread: MutationSpread::SingleSource,
    };
    assert_planned_matches(&a, 200_002, "join × provided unique key");
}

/// Bug 3 analog — MIN over a non-numeric (timestamptz) measure. Locks the
/// aggregate output-type inference (agg_result_ty): the result column must be
/// typed timestamptz, not hardcoded numeric.
#[cfg(any(test, feature = "pg_test"))]
#[pg_test]
fn regression_minmax_nonnumeric_output_type() {
    use fuzz_model::axes::*;
    use fuzz_model::model::ColType;
    let a = Axes {
        source: SourceKind::Table,
        shape: QueryShape::SingleAggregate,
        refresh: RefreshMode::Immediate,
        agg: Some(AggFn::Min),
        measure_ty: ColType::Timestamptz,
        unique: UniqueCols::Absent,
        lifecycle: Lifecycle::CreateMutateDrop,
        filter: FilterMode::None,
        spread: MutationSpread::SingleSource,
    };
    assert_planned_matches(&a, 200_003, "min × timestamptz output type");
}

/// Bug 4 analog — CASCADE drop of a CTE-decomposed IMV must remove its internal
/// sub-IMVs with no orphans (complements the non-cascade lock in
/// drop_decomposed_imv_noncascade_leaves_no_subimv_orphans).
#[cfg(any(test, feature = "pg_test"))]
#[pg_test]
fn regression_decomp_cte_cascade_drop() {
    use fuzz_model::axes::*;
    use fuzz_model::model::ColType;
    let a = Axes {
        source: SourceKind::Table,
        shape: QueryShape::CteDecomposed,
        refresh: RefreshMode::Immediate,
        agg: Some(AggFn::Sum),
        measure_ty: ColType::Numeric,
        unique: UniqueCols::Absent,
        lifecycle: Lifecycle::CascadeDrop,
        filter: FilterMode::None,
        spread: MutationSpread::SingleSource,
    };
    assert_planned_matches(&a, 200_004, "decomposed CTE × cascade drop");
}

/// Bug 5 analog — a partitioned CTE-decomposed IMV. Locks partition-subset
/// propagation into the decomposition sub-IMVs.
#[cfg(any(test, feature = "pg_test"))]
#[pg_test]
fn regression_decomp_cte_partitioned() {
    use fuzz_model::axes::*;
    use fuzz_model::model::ColType;
    let a = Axes {
        source: SourceKind::Table,
        shape: QueryShape::CteDecomposed,
        refresh: RefreshMode::Immediate,
        agg: Some(AggFn::Sum),
        measure_ty: ColType::Numeric,
        unique: UniqueCols::Absent,
        lifecycle: Lifecycle::Partitioned,
        filter: FilterMode::None,
        spread: MutationSpread::SingleSource,
    };
    assert_planned_matches(&a, 200_005, "decomposed CTE × partitioned");
}

/// Deterministic pairwise matrix CI gate.
///
/// Runs the full pairwise set of axes combinations through `evaluate_planned`.
/// This is a permanent gate that ensures every axis combination can be created,
/// mutated, maintained, and dropped without bugs. Unlike the proptest-driven
/// fuzz_differential_exact, this gate is deterministic, fast, and runs in CI.
///
/// Each pairwise case is identified by its axes combination. A Bug outcome
/// collects a full list and fails the gate, allowing the controller to triage
/// and fix real pg_reflex bugs vs harness/codegen artifacts.
///
/// Each case runs in its own savepoint to isolate failures and prevent cascading
/// errors from one case affecting the next.
#[cfg(any(test, feature = "pg_test"))]
#[pg_test]
fn fuzz_pairwise_matrix_gate() {
    use fuzz_model::axes::{pairwise, valid_space};
    use pgrx::prelude::*;
    let mut failures: Vec<String> = Vec::new();
    let pairwise_cases = pairwise(&valid_space());
    let total = pairwise_cases.len();

    for (i, a) in pairwise_cases.into_iter().enumerate() {
        let pc = match fuzz_model::axes::plan_case(&a, 100_000 + i as u64) {
            Some(pc) => pc,
            None => continue, // no template yet; tracked, never silent
        };

        // Each case runs in its own savepoint to isolate failures
        let sp_name = format!("sp_pairwise_{}", i);
        let _ = Spi::run(&format!("SAVEPOINT {}", sp_name));

        // Wrap evaluate_planned to catch panics from invalid SQL that's generated
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            oracle::evaluate_planned(&pc)
        }));

        match outcome {
            Ok(oracle::Outcome::Match) => {
                let _ = Spi::run(&format!("RELEASE {}", sp_name));
            }
            Ok(oracle::Outcome::Skip(_)) => {
                let _ = Spi::run(&format!("RELEASE {}", sp_name));
            }
            Ok(oracle::Outcome::Bug(detail)) => {
                // Rollback this savepoint to clean up partial state
                let _ = Spi::run(&format!("ROLLBACK TO {}", sp_name));
                failures.push(format!("case {}/{} axes {a:?} => BUG: {detail}", i, total, a = a));
            }
            Err(e) => {
                // Panic occurred during evaluation (e.g., SQL exception at SPI level)
                let _ = Spi::run(&format!("ROLLBACK TO {}", sp_name));
                let msg = if let Some(s) = e.downcast_ref::<String>() {
                    s.clone()
                } else if let Some(s) = e.downcast_ref::<&str>() {
                    s.to_string()
                } else {
                    "unknown error".to_string()
                };
                failures.push(format!("case {}/{} axes {a:?} => PANIC: {msg}", i, total, a = a));
            }
        }
    }
    assert!(failures.is_empty(), "pairwise gate found {} failures:\n{}",
            failures.len(), failures.join("\n"));
}

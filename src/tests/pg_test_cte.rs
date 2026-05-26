
#[pg_test]
fn test_cte_simple_aggregate() {
    Spi::run("CREATE TABLE cte_src1 (id SERIAL, region TEXT, amount NUMERIC)")
        .expect("create table");
    Spi::run("INSERT INTO cte_src1 (region, amount) VALUES ('US', 100), ('US', 200), ('EU', 300)")
        .expect("seed");

    let result = crate::create_reflex_ivm(
        "cte_simple",
        "WITH regional AS (SELECT region, SUM(amount) AS total FROM cte_src1 GROUP BY region) SELECT region, total FROM regional",
        None,
        None,
        None,
        None,
    );
    assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");

    // Sub-IMV should exist with correct data
    let us = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT total FROM cte_simple__cte_regional WHERE region = 'US'",
    ).expect("q").expect("v");
    assert_eq!(us.to_string(), "300");

    // The main view should be a VIEW reading from the sub-IMV
    let us_view = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT total FROM cte_simple WHERE region = 'US'",
    ).expect("q").expect("v");
    assert_eq!(us_view.to_string(), "300");
}

#[pg_test]
fn test_cte_trigger_propagation() {
    Spi::run("CREATE TABLE cte_src2 (id SERIAL, region TEXT, amount NUMERIC)")
        .expect("create table");
    Spi::run("INSERT INTO cte_src2 (region, amount) VALUES ('A', 10), ('B', 20)")
        .expect("seed");

    crate::create_reflex_ivm(
        "cte_prop",
        "WITH totals AS (SELECT region, SUM(amount) AS total FROM cte_src2 GROUP BY region) SELECT region, total FROM totals",
        None,
        None,
        None,
        None,
    );

    // INSERT into source → sub-IMV updates → VIEW reflects changes
    Spi::run("INSERT INTO cte_src2 (region, amount) VALUES ('A', 40)")
        .expect("insert");

    let a = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT total FROM cte_prop WHERE region = 'A'",
    ).expect("q").expect("v");
    assert_eq!(a.to_string(), "50"); // 10 + 40

    // DELETE → propagates
    Spi::run("DELETE FROM cte_src2 WHERE amount = 10").expect("delete");
    let a2 = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT total FROM cte_prop WHERE region = 'A'",
    ).expect("q").expect("v");
    assert_eq!(a2.to_string(), "40");
}

#[pg_test]
fn test_cte_with_where_filter() {
    Spi::run("CREATE TABLE cte_src3 (id SERIAL, region TEXT, amount NUMERIC)")
        .expect("create table");
    Spi::run("INSERT INTO cte_src3 (region, amount) VALUES ('X', 50), ('Y', 200)")
        .expect("seed");

    crate::create_reflex_ivm(
        "cte_filtered",
        "WITH totals AS (SELECT region, SUM(amount) AS total FROM cte_src3 GROUP BY region) SELECT region, total FROM totals WHERE total > 100",
        None,
        None,
        None,
        None,
    );

    // Only Y (200) should appear, not X (50)
    let count = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM cte_filtered",
    ).expect("q").expect("v");
    assert_eq!(count, 1);

    // INSERT that pushes X over threshold
    Spi::run("INSERT INTO cte_src3 (region, amount) VALUES ('X', 100)")
        .expect("insert");
    let count2 = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM cte_filtered",
    ).expect("q").expect("v");
    assert_eq!(count2, 2); // Both X (150) and Y (200) now > 100
}

#[pg_test]
fn test_cte_multiple_chained() {
    Spi::run("CREATE TABLE cte_src4 (id SERIAL, region TEXT, city TEXT, amount NUMERIC)")
        .expect("create table");
    Spi::run(
        "INSERT INTO cte_src4 (region, city, amount) VALUES \
         ('US', 'NYC', 100), ('US', 'LA', 200), ('EU', 'London', 300)",
    )
    .expect("seed");

    let result = crate::create_reflex_ivm(
        "cte_chain",
        "WITH by_city AS (\
            SELECT region, city, SUM(amount) AS city_total FROM cte_src4 GROUP BY region, city\
         ), by_region AS (\
            SELECT region, SUM(city_total) AS total FROM by_city GROUP BY region\
         ) SELECT region, total FROM by_region",
        None,
        None,
        None,
        None,
    );
    assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");

    // Verify both sub-IMVs exist
    let city_count = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM cte_chain__cte_by_city",
    ).expect("q").expect("v");
    assert_eq!(city_count, 3);

    // Verify final VIEW
    let us = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT total FROM cte_chain WHERE region = 'US'",
    ).expect("q").expect("v");
    assert_eq!(us.to_string(), "300"); // 100 + 200
}

#[pg_test]
fn test_cte_main_body_with_aggregation() {
    Spi::run("CREATE TABLE cte_src5 (id SERIAL, region TEXT, amount NUMERIC)")
        .expect("create table");
    Spi::run("INSERT INTO cte_src5 (region, amount) VALUES ('A', 10), ('B', 20), ('C', 30)")
        .expect("seed");

    // Main body has COUNT(*) → should create an IMV, not a VIEW
    let result = crate::create_reflex_ivm(
        "cte_agg_main",
        "WITH totals AS (SELECT region, SUM(amount) AS total FROM cte_src5 GROUP BY region) SELECT COUNT(*) AS num_regions FROM totals",
        None,
        None,
        None,
        None,
    );
    assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");

    let cnt = Spi::get_one::<i64>(
        "SELECT num_regions FROM cte_agg_main",
    ).expect("q").expect("v");
    assert_eq!(cnt, 3);
}

#[pg_test]
fn test_cte_passthrough_sub_imv() {
    Spi::run(
        "CREATE TABLE cte_pt_src (id SERIAL, region TEXT NOT NULL, val INT NOT NULL, active BOOLEAN NOT NULL)",
    )
    .expect("create table");
    Spi::run(
        "INSERT INTO cte_pt_src (region, val, active) VALUES \
         ('A', 10, true), ('A', 20, false), ('B', 30, true)",
    )
    .expect("seed");

    // CTE is passthrough (no aggregation) — should become a passthrough sub-IMV
    let result = crate::create_reflex_ivm(
        "cte_pt_view",
        "WITH active_orders AS (
            SELECT id, region, val FROM cte_pt_src WHERE active = true
        )
        SELECT region, SUM(val) AS total FROM active_orders GROUP BY region",
        None,
        None,
        None,
        None,
    );
    assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");

    // Verify initial state
    let a = Spi::get_one::<i64>(
        "SELECT total FROM cte_pt_view WHERE region = 'A'",
    )
    .expect("q")
    .expect("v");
    assert_eq!(a, 10i64, "Only active A rows: 10");

    // Insert active row → should propagate through CTE sub-IMV
    Spi::run("INSERT INTO cte_pt_src (region, val, active) VALUES ('A', 5, true)")
        .expect("insert");

    let a2 = Spi::get_one::<i64>(
        "SELECT total FROM cte_pt_view WHERE region = 'A'",
    )
    .expect("q")
    .expect("v");
    assert_eq!(a2, 15i64, "After insert active A: 10 + 5 = 15");

    // Insert inactive row → should NOT affect view
    Spi::run("INSERT INTO cte_pt_src (region, val, active) VALUES ('A', 100, false)")
        .expect("insert inactive");

    let a3 = Spi::get_one::<i64>(
        "SELECT total FROM cte_pt_view WHERE region = 'A'",
    )
    .expect("q")
    .expect("v");
    assert_eq!(a3, 15i64, "Inactive row should not affect view");
}

// Regression: a DEFERRED passthrough IMV that joins a CTE-decomposed sub-IMV
// used to fail at creation with `zero-length delimited identifier at or near ""`.
// The sub-IMV source is stored already-quoted (`"schema"."view__cte_x"`); the
// deferred staging-name builder re-quoted the schema into `""schema""`. Immediate
// mode was unaffected (its trigger names strip quotes), so this exercises the
// deferred path specifically and verifies maintenance still runs.
#[pg_test]
fn test_cte_deferred_passthrough_sub_imv_staging() {
    Spi::run("CREATE TABLE cte_def_main (pid INT NOT NULL, val INT NOT NULL)").expect("t1");
    Spi::run("CREATE TABLE cte_def_agg (pid INT NOT NULL, qty INT NOT NULL)").expect("t2");
    Spi::run("INSERT INTO cte_def_main VALUES (1, 10), (2, 20)").expect("seed1");
    Spi::run("INSERT INTO cte_def_agg VALUES (1, 5), (1, 7), (2, 3)").expect("seed2");

    let result = crate::create_reflex_ivm(
        "cte_def_view",
        "WITH agg AS (SELECT pid, SUM(qty)::BIGINT AS sq FROM cte_def_agg GROUP BY pid)
         SELECT m.pid, m.val, a.sq FROM cte_def_main m LEFT JOIN agg a ON a.pid = m.pid",
        None,
        Some("UNLOGGED"),
        Some("DEFERRED"),
        None,
    );
    assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");

    let sq = Spi::get_one::<i64>("SELECT sq FROM cte_def_view WHERE pid = 1")
        .expect("q")
        .expect("v");
    assert_eq!(sq, 12i64, "pid=1: 5+7=12");

    // Mutate the base table, then flush manually (the commit-time constraint
    // trigger never fires inside the rolled-back pg_test transaction). The
    // staging delta named off the quoted sub-IMV source must resolve here.
    Spi::run("INSERT INTO cte_def_main VALUES (1, 99)").expect("insert");
    Spi::run("SELECT reflex_flush_deferred('cte_def_main')").expect("flush");
    let cnt = Spi::get_one::<i64>("SELECT COUNT(*) FROM cte_def_view WHERE pid = 1")
        .expect("q")
        .expect("v");
    assert_eq!(cnt, 2i64, "two pid=1 rows after deferred insert flush");
}

// Regression: the CTE-decomposition path did not thread the caller's explicit
// `unique_columns` into the outer passthrough IMV (unlike the set-op and
// distinct-on paths). The outer body was re-created with an empty key, so a
// JOIN passthrough silently fell back to full-refresh on DELETE/UPDATE even
// when the user supplied a key.
#[pg_test]
fn test_cte_passthrough_threads_explicit_unique_columns() {
    Spi::run("CREATE TABLE cte_uk_main (pid INT NOT NULL, val INT NOT NULL)").expect("t1");
    Spi::run("CREATE TABLE cte_uk_agg (pid INT NOT NULL, qty INT NOT NULL)").expect("t2");
    Spi::run("INSERT INTO cte_uk_main VALUES (1, 10), (2, 20)").expect("seed1");
    Spi::run("INSERT INTO cte_uk_agg VALUES (1, 5), (2, 3)").expect("seed2");

    let result = crate::create_reflex_ivm(
        "cte_uk_view",
        "WITH agg AS (SELECT pid, SUM(qty)::BIGINT AS sq FROM cte_uk_agg GROUP BY pid)
         SELECT m.pid, m.val, a.sq FROM cte_uk_main m LEFT JOIN agg a ON a.pid = m.pid",
        Some("pid"),
        None,
        None,
        None,
    );
    assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");

    let uk = Spi::get_one::<Vec<String>>(
        "SELECT unique_columns FROM public.__reflex_ivm_reference WHERE name LIKE '%cte_uk_view'",
    )
    .expect("q")
    .expect("v");
    assert_eq!(
        uk,
        vec!["pid".to_string()],
        "explicit unique key must reach the outer passthrough through CTE decomposition"
    );
}

#[pg_test]
fn test_cte_sibling_with_window() {
    // This test reproduces the core bug: CTE decomposition must run BEFORE window decomposition.
    // Bug: when a query has CTEs AND a window function in the main body, the old order
    // would run window decomposition first. Window decomposition drops the WITH clause to build
    // the base query, causing "relation <sibling_cte> does not exist".
    // Fix: CTE decomposition runs first, turning each CTE into its own sub-IMV (recursively).
    // Then the main body (which still has window functions) is processed without the WITH clause.
    // Window decomposition can then safely drop the WITH clause since there are no more CTEs.

    Spi::run(
        "CREATE TABLE tcte_src1 (id INT, grp TEXT)",
    )
    .expect("create src1");
    Spi::run(
        "INSERT INTO tcte_src1 VALUES (1, 'A'), (2, 'A'), (3, 'B')",
    )
    .expect("seed src1");

    Spi::run(
        "CREATE TABLE tcte_src2 (id INT, grp TEXT, val INT)",
    )
    .expect("create src2");
    Spi::run(
        "INSERT INTO tcte_src2 VALUES (10, 'A', 100), (11, 'A', 200), (12, 'B', 150)",
    )
    .expect("seed src2");

    // Two sibling CTEs (no window functions):
    // - agg_cte: aggregate, counts rows per group
    // - sum_cte: aggregate, sums values per group
    // Main query: aggregate from one CTE with window function RANK() in top-level SELECT
    // With old order: window decomposition fires on "has_window_function=true",
    //   drops WITH, tries to build base from just SELECT...FROM (without agg_cte, sum_cte),
    //   ERROR: relation "agg_cte" does not exist
    // With fix: CTE decomposition fires first, agg_cte → sub-IMV, sum_cte → sub-IMV,
    //   main body is rewritten to reference sub-IMVs (no longer has WITH clause),
    //   then window decomposition safely processes the window function without dropping CTEs
    let result = crate::create_reflex_ivm(
        "cte_sibling_window",
        "WITH agg_cte AS (
            SELECT grp, COUNT(*) AS cnt FROM tcte_src1 GROUP BY grp
        ), sum_cte AS (
            SELECT grp, SUM(val) AS total_val FROM tcte_src2 GROUP BY grp
        )
        SELECT grp, cnt, total_val, RANK() OVER (ORDER BY total_val DESC) AS rnk
        FROM (
            SELECT a.grp, a.cnt, s.total_val
            FROM agg_cte a
            JOIN sum_cte s ON s.grp = a.grp
        ) t",
        None,
        None,
        None,
        None,
    );
    assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW", "CTE decomposition should succeed without 'does not exist' error");

    // Verify row count
    let cnt = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM cte_sibling_window",
    )
    .expect("q")
    .expect("v");
    assert_eq!(cnt, 2i64, "Should have 2 rows (one per group A, B)");

    // Verify correctness
    let cnt_a = Spi::get_one::<i64>(
        "SELECT cnt FROM cte_sibling_window WHERE grp = 'A'",
    )
    .expect("q")
    .expect("v");
    assert_eq!(cnt_a, 2i64, "A has 2 rows in tcte_src1");

    let val_sum_a = Spi::get_one::<i64>(
        "SELECT total_val FROM cte_sibling_window WHERE grp = 'A'",
    )
    .expect("q")
    .expect("v");
    assert_eq!(val_sum_a, 300i64, "A values in tcte_src2: 100 + 200 = 300");
}

#[pg_test]
fn test_cte_sibling_with_window_main_query() {
    // This test adds a variant of test_cte_sibling_with_window covering the scenario:
    // - Two CTEs, one with aggregation, one with passthrough (no window in CTE itself)
    // - Main query applies a window function and references BOTH CTEs
    // Bug: CTE decomposition must run BEFORE window decomposition.
    // Old behavior: window decomposition would detect has_window_function=true on the query,
    //   then attempt to drop the WITH clause to build a base query. This caused:
    //   "relation <sibling_cte> does not exist" because the other CTE hadn't been decomposed yet.
    // Fix: CTE decomposition runs first (in src/create_ivm.rs at line ~1908), turning each CTE
    //   into its own sub-IMV recursively. The main body is rewritten to reference sub-IMVs
    //   (no longer containing WITH clauses). Then window decomposition safely processes any
    //   window functions without dropping CTEs.

    Spi::run(
        "CREATE TABLE tcte_winpass_src1 (id SERIAL, grp TEXT)",
    )
    .expect("create src1");
    Spi::run(
        "INSERT INTO tcte_winpass_src1 VALUES (1, 'A'), (2, 'A'), (3, 'B'), (4, 'B')",
    )
    .expect("seed src1");

    Spi::run(
        "CREATE TABLE tcte_winpass_src2 (id INT, grp TEXT, val INT)",
    )
    .expect("create src2");
    Spi::run(
        "INSERT INTO tcte_winpass_src2 VALUES (10, 'A', 100), (11, 'A', 200), (12, 'B', 150), (13, 'B', 250)",
    )
    .expect("seed src2");

    // agg_cte: aggregation (non-window)
    // vals_cte: passthrough, no aggregation
    // Main query: window function applied to joined result
    let result = crate::create_reflex_ivm(
        "cte_winpass_view",
        "WITH agg_cte AS (
            SELECT grp, COUNT(*) AS cnt FROM tcte_winpass_src1 GROUP BY grp
        ), vals_cte AS (
            SELECT grp, val FROM tcte_winpass_src2
        )
        SELECT grp, cnt, val, RANK() OVER (ORDER BY val DESC) AS rnk
        FROM (
            SELECT a.grp, a.cnt, v.val
            FROM agg_cte a
            LEFT JOIN vals_cte v ON v.grp = a.grp
        ) t",
        None,
        None,
        None,
        None,
    );
    assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW", "Two sibling CTEs with window in main query should decompose without 'does not exist' error");

    // Verify row count
    let cnt = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM cte_winpass_view",
    )
    .expect("q")
    .expect("v");
    assert_eq!(cnt, 4i64, "Should have 4 rows");

    // Verify correctness: aggregation is preserved
    let cnt_a = Spi::get_one::<i64>(
        "SELECT cnt FROM cte_winpass_view WHERE grp = 'A' AND val IS NOT NULL LIMIT 1",
    )
    .expect("q")
    .expect("v");
    assert_eq!(cnt_a, 2i64, "Group A has cnt=2");

    // Verify window ranking works
    let rnk_200 = Spi::get_one::<i64>(
        "SELECT rnk FROM cte_winpass_view WHERE val = 200",
    )
    .expect("q")
    .expect("v");
    assert_eq!(rnk_200, 2i64, "Val 200 should have rank 2 (after 250)");

    let rnk_250 = Spi::get_one::<i64>(
        "SELECT rnk FROM cte_winpass_view WHERE val = 250",
    )
    .expect("q")
    .expect("v");
    assert_eq!(rnk_250, 1i64, "Val 250 should have rank 1 (highest)");
}

#[pg_test]
fn test_cte_window_in_cte_referenced_by_parent() {
    // Test Task 5: Window function inside a CTE that is referenced by an outer query
    // should be rejected with a clear error message before any sub-IMV is created.
    // This prevents orphan tables/views and avoids the cryptic PostgreSQL error:
    // "ERROR: "<view>__cte_<alias>" is a view — Triggers on views cannot have transition tables."

    Spi::run(
        "CREATE TABLE tcte_win_events (id SERIAL, grp TEXT, ts TIMESTAMP, val INT)",
    )
    .expect("create events table");
    Spi::run(
        "INSERT INTO tcte_win_events (grp, ts, val) VALUES \
         ('A', '2024-01-01', 10), ('A', '2024-01-02', 20), ('B', '2024-01-01', 30)",
    )
    .expect("seed events");

    Spi::run(
        "CREATE TABLE tcte_win_items (id SERIAL, grp TEXT, val INT)",
    )
    .expect("create items table");
    Spi::run(
        "INSERT INTO tcte_win_items (grp, val) VALUES ('A', 100), ('B', 200)",
    )
    .expect("seed items");

    let result = crate::create_reflex_ivm(
        "tcte_win_rejected",
        "WITH ranked AS (
            SELECT grp, val, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY val DESC) AS rn
            FROM tcte_win_items
        )
        SELECT e.grp, e.val, r.rn
        FROM tcte_win_events e
        LEFT JOIN ranked r ON r.grp = e.grp",
        None,
        None,
        None,
        None,
    );

    assert!(
        result.starts_with("ERROR"),
        "Window function in CTE referenced by outer query should be rejected, got: {}",
        result
    );
    assert!(
        result.contains("window") || result.contains("kind: mv"),
        "Error should mention window function or kind: mv suggestion, got: {}",
        result
    );
}

#[pg_test]
fn test_cte_distinct_on_in_cte_referenced_by_parent() {
    // Test Task 5: DISTINCT ON inside a CTE that is referenced by an outer query
    // should be rejected with a clear error message before any sub-IMV is created.
    // Note: DISTINCT ON without ORDER BY is simpler and avoids the ORDER BY rejection
    // that happens during base query decomposition.

    Spi::run(
        "CREATE TABLE tcte_don_events (id SERIAL, grp TEXT, val INT)",
    )
    .expect("create events table");
    Spi::run(
        "INSERT INTO tcte_don_events (grp, val) VALUES ('A', 20), ('A', 10), ('B', 30)",
    )
    .expect("seed events");

    Spi::run(
        "CREATE TABLE tcte_don_items (id SERIAL, grp TEXT, val INT)",
    )
    .expect("create items table");
    Spi::run(
        "INSERT INTO tcte_don_items (grp, val) VALUES ('A', 100), ('A', 50), ('B', 200)",
    )
    .expect("seed items");

    let result = crate::create_reflex_ivm(
        "tcte_don_rejected",
        "WITH latest AS (
            SELECT DISTINCT ON (grp) grp, val FROM tcte_don_items
        )
        SELECT e.grp, e.val, l.val AS latest_val
        FROM tcte_don_events e
        LEFT JOIN latest l ON l.grp = e.grp",
        None,
        None,
        None,
        None,
    );

    assert!(
        result.starts_with("ERROR"),
        "DISTINCT ON in CTE referenced by outer query should be rejected, got: {}",
        result
    );
    assert!(
        result.contains("DISTINCT ON") || result.contains("kind: mv"),
        "Error should mention DISTINCT ON or kind: mv suggestion, got: {}",
        result
    );
}

#[pg_test]
fn test_cte_without_window_or_distinct_on_still_works() {
    // Regression guard: ensure the pre-scan does not over-reject normal CTEs.
    // A plain CTE with no window/DISTINCT-ON should still work correctly.

    Spi::run(
        "CREATE TABLE tcte_normal_src (id SERIAL, grp TEXT NOT NULL, val INT NOT NULL)",
    )
    .expect("create table");
    Spi::run(
        "INSERT INTO tcte_normal_src (grp, val) VALUES ('A', 10), ('A', 20), ('B', 30)",
    )
    .expect("seed");

    let result = crate::create_reflex_ivm(
        "tcte_normal_ok",
        "WITH agg AS (
            SELECT grp, SUM(val) AS total FROM tcte_normal_src GROUP BY grp
        )
        SELECT grp, total FROM agg",
        Some("grp"),
        None,
        None,
        None,
    );

    assert_eq!(
        result,
        "CREATE REFLEX INCREMENTAL VIEW",
        "Normal CTE without window/DISTINCT-ON should still succeed: {}",
        result
    );

    // Verify row count
    let count = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM tcte_normal_ok",
    )
    .expect("q")
    .expect("v");
    assert_eq!(count, 2, "Should have 2 groups: A and B");

    // Verify data is correct
    let total_a = Spi::get_one::<i64>(
        "SELECT total FROM tcte_normal_ok WHERE grp = 'A'",
    )
    .expect("q")
    .expect("v");
    assert_eq!(total_a, 30, "A: 10 + 20 = 30");
}


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

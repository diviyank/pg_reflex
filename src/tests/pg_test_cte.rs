
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

// Regression (zero-length delimited identifier at COMMIT): a *schema-qualified*
// CTE view whose inner CTE is a PASSTHROUGH feeding an OUTER AGGREGATE decomposes
// into a passthrough sub-IMV (`s.v__cte_base`) plus an aggregate parent (`s.v`).
// On a base mutation, maintaining the sub-IMV cascades to the parent; the parent
// is flushed with its source UNQUOTED (`s.v__cte_base`, as enqueued by the
// sub-IMV's deferred triggers) while its stored `base_query` references that
// source QUOTED (`"s"."v__cte_base"`). `replace_source_with_transition` could not
// match across the `"."` and the bare-token pass injected the transition name
// inside the existing quotes -> `"s".""__reflex_old_..."" ` -> 42601 `zero-length
// delimited identifier`, aborting the user's transaction at commit. Unlike the
// prior staging-name fix, this is the aggregate parent's delta codegen.
#[pg_test]
fn test_cte_deferred_aggregate_on_passthrough_schema_qualified_cascade() {
    Spi::run("CREATE SCHEMA ckr").expect("schema");
    Spi::run("CREATE TABLE ckr.inv (id INT NOT NULL, d DATE NOT NULL)").expect("t");
    Spi::run("INSERT INTO ckr.inv VALUES (1, '2026-01-05'), (2, '2026-02-10'), (3, '2027-03-01')")
        .expect("seed");

    let result = crate::create_reflex_ivm(
        "ckr.kind",
        "WITH base AS (SELECT EXTRACT('year' FROM d)::INT AS y, d FROM ckr.inv) \
         SELECT 'Stock' AS kind, y, MIN(d) AS mn, MAX(d) AS mx FROM base GROUP BY y",
        None,
        Some("UNLOGGED"),
        Some("DEFERRED"),
        None,
    );
    assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");

    let mn0 = Spi::get_one::<String>("SELECT mn::text FROM ckr.kind WHERE y = 2026")
        .expect("q")
        .expect("v");
    assert_eq!(mn0, "2026-01-05", "baseline 2026 minimum");

    // Reproduce the commit-time cascade the constraint trigger performs: flush
    // the base source, then the sub-IMV source it enqueues. The second flush is
    // where the parent's aggregate delta codegen previously emitted the `""`.
    // Read the enqueued source name from the catalog rather than hardcoding the
    // `__cte_base` suffix.
    let cte_src = Spi::get_one::<String>(
        "SELECT depends_on[1] FROM public.__reflex_ivm_reference WHERE name = 'ckr.kind'",
    )
    .expect("q")
    .expect("v");

    Spi::run("DELETE FROM ckr.inv WHERE id = 1").expect("del");
    Spi::run("SELECT reflex_flush_deferred('ckr.inv')").expect("flush base");
    Spi::run(&format!("SELECT reflex_flush_deferred('{}')", cte_src)).expect("flush parent cascade");

    // Deleting the row holding the 2026 minimum must advance it to 2026-02-10 —
    // proving the parent aggregate was actually maintained (not silently skipped
    // by a swallowed error).
    let mn1 = Spi::get_one::<String>("SELECT mn::text FROM ckr.kind WHERE y = 2026")
        .expect("q")
        .expect("v");
    assert_eq!(mn1, "2026-02-10", "2026 minimum after deleting the min row via cascade");
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

// Part A — structural inference: a JOIN passthrough whose anchor's PRIMARY KEY
// is projected and whose every other source is reached by an equi-join covering
// a UNIQUE key (to-one) has a sound unique key = the anchor PK. It must be
// inferred so DELETE/UPDATE is targeted instead of a full refresh.
#[pg_test]
fn test_join_passthrough_infers_key_from_to_one_join() {
    Spi::run("CREATE TABLE jk_o (id INT PRIMARY KEY, cid INT NOT NULL, amt INT NOT NULL)")
        .expect("o");
    Spi::run("CREATE TABLE jk_c (id INT PRIMARY KEY, label TEXT NOT NULL)").expect("c");
    Spi::run("INSERT INTO jk_c VALUES (1,'x'),(2,'y')").expect("seedc");
    Spi::run("INSERT INTO jk_o VALUES (10,1,100),(11,1,200),(12,2,300)").expect("seedo");

    let result = crate::create_reflex_ivm(
        "jk_view",
        "SELECT o.id, o.amt, c.label FROM jk_o o JOIN jk_c c ON c.id = o.cid",
        None,
        None,
        None,
        None,
    );
    assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");

    let uk = Spi::get_one::<Vec<String>>(
        "SELECT unique_columns FROM public.__reflex_ivm_reference WHERE name = 'jk_view'",
    )
    .expect("q")
    .expect("inferred key");
    assert_eq!(uk, vec!["id".to_string()], "anchor PK inferred as unique key");

    assert_eq!(
        Spi::get_one::<i64>("SELECT COUNT(*) FROM jk_view")
            .unwrap()
            .unwrap(),
        3
    );
    Spi::run("DELETE FROM jk_o WHERE id = 11").expect("del");
    assert_eq!(
        Spi::get_one::<i64>("SELECT COUNT(*) FROM jk_view WHERE id = 11")
            .unwrap()
            .unwrap(),
        0,
        "targeted DELETE removed the row"
    );
    assert_eq!(
        Spi::get_one::<i64>("SELECT COUNT(*) FROM jk_view")
            .unwrap()
            .unwrap(),
        2
    );
}

// Part A — negative: when the joined side is NOT unique on the join column the
// join is to-many, the anchor PK is no longer unique in the result, so NO key
// may be inferred (and the unsound unique index must not be built).
#[pg_test]
fn test_join_passthrough_no_key_when_to_many() {
    Spi::run("CREATE TABLE jm_o (id INT PRIMARY KEY, gid INT NOT NULL, amt INT NOT NULL)")
        .expect("o");
    Spi::run("CREATE TABLE jm_c (gid INT NOT NULL, label TEXT NOT NULL)").expect("c");
    Spi::run("INSERT INTO jm_c VALUES (1,'x'),(1,'y')").expect("seedc");
    Spi::run("INSERT INTO jm_o VALUES (10,1,100)").expect("seedo");

    let result = crate::create_reflex_ivm(
        "jm_view",
        "SELECT o.id, o.amt, c.label FROM jm_o o JOIN jm_c c ON c.gid = o.gid",
        None,
        None,
        None,
        None,
    );
    assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");

    let uk: Option<Vec<String>> = Spi::get_one::<Vec<String>>(
        "SELECT unique_columns FROM public.__reflex_ivm_reference WHERE name = 'jm_view'",
    )
    .expect("q");
    assert!(
        uk.is_none_or(|v| v.is_empty()),
        "to-many join must not infer a key"
    );
    // The result genuinely has two rows for the single order; a wrongly-built
    // unique index would have failed creation above.
    assert_eq!(
        Spi::get_one::<i64>("SELECT COUNT(*) FROM jm_view")
            .unwrap()
            .unwrap(),
        2
    );
}

// Part A — negative: a date-RANGE join (the shape in forecast_analysis_view) is
// to-many and has no equality covering a unique key, so no key is inferred.
#[pg_test]
fn test_join_passthrough_no_key_for_range_join() {
    Spi::run("CREATE TABLE rj_o (id INT PRIMARY KEY, d INT NOT NULL)").expect("o");
    Spi::run("CREATE TABLE rj_w (lo INT NOT NULL, hi INT NOT NULL, tag TEXT NOT NULL)")
        .expect("w");
    Spi::run("INSERT INTO rj_w VALUES (0, 100, 'a'), (0, 100, 'b')").expect("seedw");
    Spi::run("INSERT INTO rj_o VALUES (1, 50)").expect("seedo");

    let result = crate::create_reflex_ivm(
        "rj_view",
        "SELECT o.id, o.d, w.tag FROM rj_o o JOIN rj_w w ON o.d >= w.lo AND o.d <= w.hi",
        None,
        None,
        None,
        None,
    );
    assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");
    let uk: Option<Vec<String>> = Spi::get_one::<Vec<String>>(
        "SELECT unique_columns FROM public.__reflex_ivm_reference WHERE name = 'rj_view'",
    )
    .expect("q");
    assert!(
        uk.is_none_or(|v| v.is_empty()),
        "range join must not infer a key"
    );
}

// Part B — explicit per-CTE unique key via the `alias : cols` extension to the
// `unique_columns` argument. The anchor sources have no PK, so structural
// inference yields nothing; only the explicit spec gives the CTE sub-IMV a key.
#[pg_test]
fn test_cte_explicit_per_cte_unique_key() {
    Spi::run("CREATE TABLE pc_a (k INT NOT NULL, v INT NOT NULL)").expect("a");
    Spi::run("CREATE TABLE pc_b (k INT NOT NULL, w INT NOT NULL)").expect("b");
    Spi::run("INSERT INTO pc_a VALUES (1,10),(2,20)").expect("seeda");
    Spi::run("INSERT INTO pc_b VALUES (1,100),(2,200)").expect("seedb");

    let result = crate::create_reflex_ivm(
        "pc_view",
        "WITH j AS (SELECT a.k, a.v, b.w FROM pc_a a JOIN pc_b b ON b.k = a.k) \
         SELECT k, v, w FROM j",
        Some("k ; j : k"),
        None,
        None,
        None,
    );
    assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");

    let cte_uk = Spi::get_one::<Vec<String>>(
        "SELECT unique_columns FROM public.__reflex_ivm_reference WHERE name = 'pc_view__cte_j'",
    )
    .expect("q")
    .expect("explicit CTE key");
    assert_eq!(cte_uk, vec!["k".to_string()], "explicit per-CTE key applied");

    let outer_uk = Spi::get_one::<Vec<String>>(
        "SELECT unique_columns FROM public.__reflex_ivm_reference WHERE name = 'pc_view'",
    )
    .expect("q")
    .expect("outer key");
    assert_eq!(outer_uk, vec!["k".to_string()], "outer key still threaded");

    // Targeted DELETE through the keyed CTE sub-IMV.
    Spi::run("DELETE FROM pc_a WHERE k = 1").expect("del");
    assert_eq!(
        Spi::get_one::<i64>("SELECT COUNT(*) FROM pc_view WHERE k = 1")
            .unwrap()
            .unwrap(),
        0
    );
    assert_eq!(
        Spi::get_one::<i64>("SELECT COUNT(*) FROM pc_view")
            .unwrap()
            .unwrap(),
        1
    );
}

// Component 1: an inner CTE sub-IMV whose single source has a PRIMARY KEY that
// the CTE projects must auto-detect that PK as its unique key (keyed maintenance),
// without any explicit per-CTE unique_columns spec.
#[pg_test]
fn test_cte_inner_auto_detects_source_pk() {
    Spi::run("CREATE TABLE cik_s (id INT PRIMARY KEY, grp INT NOT NULL, qty INT, flag BOOL)")
        .expect("create");
    Spi::run("INSERT INTO cik_s SELECT i, i % 10, i, (i % 2 = 0) FROM generate_series(1, 100) i")
        .expect("seed");

    let result = crate::create_reflex_ivm(
        "cik_view",
        "WITH f AS (SELECT id, grp, qty FROM cik_s WHERE flag) SELECT id, grp, qty FROM f",
        Some("id"),
        None,
        None,
        None,
    );
    assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");

    let inner_uk = Spi::get_one::<Vec<String>>(
        "SELECT unique_columns FROM public.__reflex_ivm_reference WHERE name = 'cik_view__cte_f'",
    )
    .expect("q")
    .expect("inner key");
    assert_eq!(
        inner_uk,
        vec!["id".to_string()],
        "inner CTE sub-IMV must auto-detect the source PK as its unique key"
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

// Component 1: IMMEDIATE-mode CTE IMV stays correct under INSERT/UPDATE/DELETE,
// including rows entering and leaving the CTE's WHERE filter.
#[pg_test]
fn test_cte_inner_keyed_correctness_immediate() {
    Spi::run("CREATE TABLE ckc_s (id INT PRIMARY KEY, grp INT NOT NULL, qty INT, flag BOOL)")
        .expect("create");
    Spi::run("INSERT INTO ckc_s SELECT i, i % 10, i, (i % 2 = 0) FROM generate_series(1, 100) i")
        .expect("seed");

    crate::create_reflex_ivm(
        "ckc_view",
        "WITH f AS (SELECT id, grp, qty FROM ckc_s WHERE flag) SELECT id, grp, qty FROM f",
        Some("id"),
        None,
        None,
        None,
    );

    let fresh =
        "WITH f AS (SELECT id, grp, qty FROM ckc_s WHERE flag) SELECT id, grp, qty FROM f";
    assert_imv_correct("ckc_view", fresh);

    Spi::run("UPDATE ckc_s SET qty = qty + 1 WHERE id = 2").expect("update");
    assert_imv_correct("ckc_view", fresh);

    Spi::run("DELETE FROM ckc_s WHERE id = 4").expect("delete");
    assert_imv_correct("ckc_view", fresh);

    Spi::run("INSERT INTO ckc_s VALUES (201, 1, 99, true)").expect("insert");
    assert_imv_correct("ckc_view", fresh);

    // Row entering the filter (flag false -> true).
    Spi::run("UPDATE ckc_s SET flag = true WHERE id = 3").expect("enter filter");
    assert_imv_correct("ckc_view", fresh);

    // Row leaving the filter (flag true -> false).
    Spi::run("UPDATE ckc_s SET flag = false WHERE id = 6").expect("leave filter");
    assert_imv_correct("ckc_view", fresh);
}

// Component 1: in DEFERRED mode, a small edit to the source must produce an
// O(K) staging delta on the inner CTE sub-IMV (keyed delete + delta insert),
// not a full-table rebuild, and the inner level must stay correct after flush.
#[pg_test]
fn test_cte_inner_delta_is_bounded_deferred() {
    Spi::run("CREATE TABLE cdb_s (id INT PRIMARY KEY, grp INT NOT NULL, qty INT, flag BOOL)")
        .expect("create");
    Spi::run("INSERT INTO cdb_s SELECT i, i % 10, i, (i % 2 = 0) FROM generate_series(1, 1000) i")
        .expect("seed");

    crate::create_reflex_ivm(
        "cdb_view",
        "WITH f AS (SELECT id, grp, qty FROM cdb_s WHERE flag) SELECT id, grp, qty FROM f",
        Some("id"),
        None,
        Some("DEFERRED"),
        None,
    );

    // 500 rows pass the filter; edit exactly 3 of them.
    Spi::run("UPDATE cdb_s SET qty = qty + 1 WHERE id IN (2, 4, 6)").expect("update");
    Spi::run("SELECT reflex_flush_deferred('cdb_s')").expect("flush");

    // Inner staging delta must be O(K): 3 updated rows -> at most 6 delta rows
    // (delete + insert per row). A full rebuild would be ~1000 (2 * 500).
    let inner_delta = Spi::get_one::<i64>("SELECT count(*) FROM __reflex_delta_cdb_view__cte_f")
        .expect("q")
        .expect("v");
    assert!(
        inner_delta <= 6,
        "inner delta must be O(K) for a 3-row edit, got {} (full rebuild would be ~1000)",
        inner_delta
    );

    // The inner level is directly maintained by the source flush; assert it is correct.
    // (The chained OUTER's mid-transaction freshness is Component 2's concern.)
    assert_imv_correct(
        "cdb_view__cte_f",
        "SELECT id, grp, qty FROM cdb_s WHERE flag",
    );
}

// Component 1 correctness gate: an inner CTE over a source with NO unique
// constraint must stay keyless (no fabricated key) and remain correct via the
// retained full-rebuild fallback.
#[pg_test]
fn test_cte_inner_keyless_fallback_retained() {
    Spi::run("CREATE TABLE ckf_s (k INT NOT NULL, v INT NOT NULL)").expect("create");
    Spi::run("INSERT INTO ckf_s SELECT i % 5, i FROM generate_series(1, 50) i").expect("seed");

    crate::create_reflex_ivm(
        "ckf_view",
        "WITH f AS (SELECT k, v FROM ckf_s WHERE v > 0) SELECT k, v FROM f",
        None,
        None,
        None,
        None,
    );

    // No PK/unique on ckf_s, no explicit per-CTE key => inner must be keyless.
    let inner_uk: Option<Vec<String>> = Spi::get_one::<Vec<String>>(
        "SELECT unique_columns FROM public.__reflex_ivm_reference WHERE name = 'ckf_view__cte_f'",
    )
    .expect("q");
    let is_keyless = inner_uk.as_ref().is_none_or(|v| v.is_empty());
    assert!(
        is_keyless,
        "inner CTE with no provable key must stay keyless, got {:?}",
        inner_uk
    );

    // Still correct under DML via full-rebuild fallback.
    let fresh = "WITH f AS (SELECT k, v FROM ckf_s WHERE v > 0) SELECT k, v FROM f";
    assert_imv_correct("ckf_view", fresh);
    Spi::run("INSERT INTO ckf_s VALUES (1, 100)").expect("insert");
    assert_imv_correct("ckf_view", fresh);
    Spi::run("DELETE FROM ckf_s WHERE v = 100").expect("delete");
    assert_imv_correct("ckf_view", fresh);
}

// 1.7.5 — an ungrouped aggregate IMV (no GROUP BY → exactly one row) must be
// flagged max_one_row so JOIN inference can treat a CROSS JOIN to it as to-one.
#[pg_test]
fn test_ungrouped_aggregate_sets_max_one_row() {
    Spi::run("CREATE TABLE mor_src (id INT PRIMARY KEY, amt INT NOT NULL)").expect("t");
    Spi::run("INSERT INTO mor_src VALUES (1,10),(2,20)").expect("seed");

    let result = crate::create_reflex_ivm(
        "mor_view",
        "SELECT SUM(amt)::BIGINT AS total FROM mor_src",
        None,
        None,
        None,
        None,
    );
    assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");

    let flag = Spi::get_one::<bool>(
        "SELECT max_one_row FROM public.__reflex_ivm_reference WHERE name = 'mor_view'",
    )
    .expect("q")
    .expect("flag");
    assert!(flag, "ungrouped aggregate must set max_one_row = TRUE");

    // A grouped aggregate must NOT be flagged.
    let result2 = crate::create_reflex_ivm(
        "mor_grouped",
        "SELECT id, SUM(amt)::BIGINT AS total FROM mor_src GROUP BY id",
        None,
        None,
        None,
        None,
    );
    assert_eq!(result2, "CREATE REFLEX INCREMENTAL VIEW");
    let flag2 = Spi::get_one::<bool>(
        "SELECT COALESCE(max_one_row, FALSE) FROM public.__reflex_ivm_reference WHERE name = 'mor_grouped'",
    )
    .expect("q")
    .expect("flag2");
    assert!(!flag2, "grouped aggregate must NOT set max_one_row");
}

// 1.7.5 — anchor probe must accept a sound UNIQUE index (not only a PRIMARY
// KEY). A CTE sub-IMV keyed via the explicit per-CTE spec gets a
// `__reflex_uk_*` NULLS NOT DISTINCT unique index; a downstream JOIN must be
// able to anchor on it. Here `j` (no PK on its sources) is keyed via the spec,
// then the outer body joins it to a to-one lookup.
#[pg_test]
fn test_anchor_accepts_reflex_unique_index() {
    Spi::run("CREATE TABLE au_a (k INT NOT NULL, v INT NOT NULL)").expect("a");
    Spi::run("CREATE TABLE au_lk (k INT PRIMARY KEY, label TEXT NOT NULL)").expect("lk");
    Spi::run("INSERT INTO au_a VALUES (1,10),(2,20)").expect("seeda");
    Spi::run("INSERT INTO au_lk VALUES (1,'x'),(2,'y')").expect("seedlk");

    // `j` keyed on k via the explicit per-CTE spec → builds __reflex_uk_*.
    // Outer body: j JOIN au_lk ON k (to-one), anchor must be `j` via its uk.
    // NO explicit outer key given; must infer from anchor.
    let result = crate::create_reflex_ivm(
        "au_view",
        "WITH j AS (SELECT k, v FROM au_a) \
         SELECT j.k, j.v, lk.label FROM j JOIN au_lk lk ON lk.k = j.k",
        Some("; j : k"),
        None,
        None,
        None,
    );
    assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");

    let uk = Spi::get_one::<Vec<String>>(
        "SELECT unique_columns FROM public.__reflex_ivm_reference WHERE name = 'au_view'",
    )
    .expect("q")
    .expect("outer key inferred from reflex unique index anchor");
    assert_eq!(uk, vec!["k".to_string()]);
}

// 1.7.5 — CROSS JOIN to a single-row (ungrouped aggregate) relation is to-one;
// the anchor's key survives. Shape mirrors date_limits' history_bounds arm.
#[pg_test]
fn test_infer_cross_join_to_single_row() {
    Spi::run("CREATE TABLE cj_dp (id INT PRIMARY KEY)").expect("dp");
    Spi::run("CREATE TABLE cj_amt (v INT NOT NULL)").expect("amt");
    Spi::run("INSERT INTO cj_dp VALUES (1),(2),(3)").expect("seeddp");
    Spi::run("INSERT INTO cj_amt VALUES (10),(20)").expect("seedamt");

    let result = crate::create_reflex_ivm(
        "cj_view",
        "WITH hb AS (SELECT MAX(v) AS mx FROM cj_amt) \
         SELECT d.id, hb.mx FROM cj_dp d CROSS JOIN hb",
        None,
        None,
        None,
        None,
    );
    assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");
    let uk = Spi::get_one::<Vec<String>>(
        "SELECT unique_columns FROM public.__reflex_ivm_reference WHERE name = 'cj_view'",
    )
    .expect("q")
    .expect("anchor key survives CROSS-to-single-row");
    assert_eq!(uk, vec!["id".to_string()]);
}

// 1.7.5 — mixed equi + range condition is to-one when the equi alone covers a
// unique key of the joined relation; the range is just an extra filter. Mirrors
// forecast_sales (JOIN ON dpid = dpid AND order_date BETWEEN …).
#[pg_test]
fn test_infer_equi_plus_range_is_to_one() {
    Spi::run("CREATE TABLE er_f (id INT PRIMARY KEY, gid INT NOT NULL, d INT NOT NULL)")
        .expect("f");
    Spi::run("CREATE TABLE er_w (gid INT PRIMARY KEY, lo INT NOT NULL, hi INT NOT NULL)")
        .expect("w");
    Spi::run("INSERT INTO er_w VALUES (1,0,100),(2,0,100)").expect("seedw");
    Spi::run("INSERT INTO er_f VALUES (10,1,50),(11,2,60)").expect("seedf");

    let result = crate::create_reflex_ivm(
        "er_view",
        "SELECT f.id, f.d, w.lo FROM er_f f \
         JOIN er_w w ON w.gid = f.gid AND f.d >= w.lo AND f.d <= w.hi",
        None,
        None,
        None,
        None,
    );
    assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");
    let uk = Spi::get_one::<Vec<String>>(
        "SELECT unique_columns FROM public.__reflex_ivm_reference WHERE name = 'er_view'",
    )
    .expect("q")
    .expect("equi covers key → to-one → anchor PK inferred");
    assert_eq!(uk, vec!["id".to_string()]);
}

// 1.7.5 — pure-range to-many INNER join: result key = anchor key ∪ joined
// relation's projected key. Mirrors history_sales (hsv range-join date_limits;
// key = hsv key ∪ dem_plan_id). Both sides keyed + projected, so it is provable.
#[pg_test]
fn test_infer_to_many_inner_key_union() {
    Spi::run("CREATE TABLE tm_h (hid INT PRIMARY KEY, d INT NOT NULL)").expect("h");
    Spi::run("CREATE TABLE tm_w (dp INT PRIMARY KEY, lo INT NOT NULL, hi INT NOT NULL)")
        .expect("w");
    Spi::run("INSERT INTO tm_w VALUES (1,0,50),(2,60,100)").expect("seedw");
    Spi::run("INSERT INTO tm_h VALUES (10,25),(11,70)").expect("seedh");

    let result = crate::create_reflex_ivm(
        "tm_view",
        "SELECT h.hid, w.dp, h.d FROM tm_h h \
         JOIN tm_w w ON h.d >= w.lo AND h.d <= w.hi",
        None,
        None,
        None,
        None,
    );
    assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");
    let mut uk = Spi::get_one::<Vec<String>>(
        "SELECT unique_columns FROM public.__reflex_ivm_reference WHERE name = 'tm_view'",
    )
    .expect("q")
    .expect("to-many key union inferred");
    uk.sort();
    assert_eq!(uk, vec!["dp".to_string(), "hid".to_string()]);

    assert_eq!(
        Spi::get_one::<i64>("SELECT COUNT(*) FROM tm_view").unwrap().unwrap(),
        2
    );
}

// 1.7.5 — to-many INNER join where the joined relation has NO projectable sound
// key → still unprovable → no key (keeps full-refresh fallback).
#[pg_test]
fn test_infer_to_many_without_joined_key_stays_none() {
    Spi::run("CREATE TABLE tk_h (hid INT PRIMARY KEY, d INT NOT NULL)").expect("h");
    Spi::run("CREATE TABLE tk_w (lo INT NOT NULL, hi INT NOT NULL, tag TEXT NOT NULL)")
        .expect("w");
    Spi::run("INSERT INTO tk_w VALUES (0,100,'a'),(0,100,'b')").expect("seedw");
    Spi::run("INSERT INTO tk_h VALUES (10,50)").expect("seedh");

    let result = crate::create_reflex_ivm(
        "tk_view",
        "SELECT h.hid, h.d, w.tag FROM tk_h h JOIN tk_w w ON h.d >= w.lo AND h.d <= w.hi",
        None,
        None,
        None,
        None,
    );
    assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");
    let uk: Option<Vec<String>> = Spi::get_one::<Vec<String>>(
        "SELECT unique_columns FROM public.__reflex_ivm_reference WHERE name = 'tk_view'",
    )
    .expect("q");
    assert!(
        uk.is_none_or(|v| v.is_empty()),
        "to-many with unkeyed joined relation must not infer a key"
    );
}

// 1.7.5 — LEFT to-many must NOT compose a key (NULL-padding); stays None.
#[pg_test]
fn test_infer_left_to_many_stays_none() {
    Spi::run("CREATE TABLE lm_h (hid INT PRIMARY KEY, d INT NOT NULL)").expect("h");
    Spi::run("CREATE TABLE lm_w (dp INT PRIMARY KEY, lo INT NOT NULL, hi INT NOT NULL)")
        .expect("w");
    Spi::run("INSERT INTO lm_w VALUES (1,0,100),(2,0,100)").expect("seedw");
    Spi::run("INSERT INTO lm_h VALUES (10,50)").expect("seedh");

    let result = crate::create_reflex_ivm(
        "lm_view",
        "SELECT h.hid, w.dp, h.d FROM lm_h h \
         LEFT JOIN lm_w w ON h.d >= w.lo AND h.d <= w.hi",
        None,
        None,
        None,
        None,
    );
    assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");
    let uk: Option<Vec<String>> = Spi::get_one::<Vec<String>>(
        "SELECT unique_columns FROM public.__reflex_ivm_reference WHERE name = 'lm_view'",
    )
    .expect("q");
    assert!(
        uk.is_none_or(|v| v.is_empty()),
        "LEFT to-many must not compose a key"
    );
}

// 1.7.5 — end-to-end: a forecast_analysis_view-shaped chain. date_limits is
// seeded (union-anchored, discriminant dropped); forecast_sales and
// history_sales must then auto-resolve their keys via the widened inference.
#[pg_test]
fn test_forecast_shape_cte_cascade() {
    Spi::run("CREATE TABLE fc_dp (dem_plan_id INT PRIMARY KEY, status TEXT NOT NULL)")
        .expect("dp");
    Spi::run(
        "CREATE TABLE fc_fcst (dem_plan_id INT NOT NULL, product_id INT NOT NULL, \
         location_id INT NOT NULL, order_date INT NOT NULL, qty INT NOT NULL, \
         PRIMARY KEY (dem_plan_id, product_id, location_id, order_date))",
    )
    .expect("fcst");
    Spi::run(
        "CREATE TABLE fc_hist (product_id INT NOT NULL, location_id INT NOT NULL, \
         order_date INT NOT NULL, qty INT NOT NULL, \
         PRIMARY KEY (product_id, location_id, order_date))",
    )
    .expect("hist");
    Spi::run("INSERT INTO fc_dp VALUES (1,'archive'),(2,'archive')").expect("seeddp");
    Spi::run("INSERT INTO fc_fcst VALUES (1,100,10,5,7),(2,100,10,6,9)").expect("seedf");
    Spi::run("INSERT INTO fc_hist VALUES (100,10,5,3),(100,10,6,4)").expect("seedh");

    let result = crate::create_reflex_ivm(
        "fc_view",
        "WITH date_limits AS ( \
           SELECT dem_plan_id, MIN(order_date) AS min_date, MAX(order_date) AS max_date \
           FROM fc_fcst GROUP BY dem_plan_id \
         ), forecast_sales AS ( \
           SELECT dl.dem_plan_id, f.product_id, f.location_id, f.order_date, f.qty \
           FROM fc_fcst f JOIN date_limits dl \
             ON f.dem_plan_id = dl.dem_plan_id \
            AND f.order_date >= dl.min_date AND f.order_date <= dl.max_date \
         ), history_sales AS ( \
           SELECT dl.dem_plan_id, h.product_id, h.location_id, h.order_date, h.qty \
           FROM fc_hist h JOIN date_limits dl \
             ON h.order_date >= dl.min_date AND h.order_date <= dl.max_date \
         ) \
         SELECT fs.dem_plan_id, fs.product_id, fs.location_id, fs.order_date, \
                fs.qty AS forecast, hs.qty AS hist \
         FROM forecast_sales fs \
         JOIN history_sales hs \
           ON fs.dem_plan_id = hs.dem_plan_id AND fs.product_id = hs.product_id \
          AND fs.location_id = hs.location_id AND fs.order_date = hs.order_date",
        Some("dem_plan_id,product_id,location_id,order_date ; date_limits : dem_plan_id"),
        None,
        None,
        None,
    );
    assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");

    let mut fs_sorted = Spi::get_one::<Vec<String>>(
        "SELECT unique_columns FROM public.__reflex_ivm_reference \
         WHERE name = 'fc_view__cte_forecast_sales'",
    )
    .expect("q")
    .expect("forecast_sales key auto-resolved");
    fs_sorted.sort();
    assert_eq!(
        fs_sorted,
        vec![
            "dem_plan_id".to_string(),
            "location_id".to_string(),
            "order_date".to_string(),
            "product_id".to_string()
        ],
        "forecast_sales: equi+range to-one → fc_fcst PK"
    );

    let mut hs_sorted = Spi::get_one::<Vec<String>>(
        "SELECT unique_columns FROM public.__reflex_ivm_reference \
         WHERE name = 'fc_view__cte_history_sales'",
    )
    .expect("q")
    .expect("history_sales key auto-resolved");
    hs_sorted.sort();
    assert_eq!(
        hs_sorted,
        vec![
            "dem_plan_id".to_string(),
            "location_id".to_string(),
            "order_date".to_string(),
            "product_id".to_string()
        ],
        "history_sales: to-many key union (hist PK ∪ dem_plan_id)"
    );

    let before = Spi::get_one::<i64>("SELECT COUNT(*) FROM fc_view").unwrap().unwrap();
    assert!(before >= 1);
    Spi::run("DELETE FROM fc_fcst WHERE dem_plan_id = 2").expect("del");
    let after = Spi::get_one::<i64>("SELECT COUNT(*) FROM fc_view WHERE dem_plan_id = 2")
        .unwrap()
        .unwrap();
    assert_eq!(after, 0, "rows for the deleted dem_plan are gone");
}

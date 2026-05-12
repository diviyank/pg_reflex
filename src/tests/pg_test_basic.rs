
#[pg_test]
fn test_hello_pg_reflex() {
    assert_eq!("Hello, pg_reflex", hello_pg_reflex());
}

#[pg_test]
fn test_create_simple_sum_imv() {
    Spi::run("CREATE TABLE test_orders (id SERIAL, city TEXT, amount NUMERIC)")
        .expect("create table");
    Spi::run(
        "INSERT INTO test_orders (city, amount) VALUES
         ('Paris', 100), ('Paris', 200), ('London', 300)",
    )
    .expect("insert data");

    let result = crate::create_reflex_ivm(
        "test_city_totals",
        "SELECT city, SUM(amount) AS total FROM test_orders GROUP BY city",
        None,
        None,
        None,
    );
    assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");

    // Verify intermediate table exists and has correct data
    let count = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM __reflex_intermediate_test_city_totals",
    )
    .expect("query")
    .expect("count");
    assert_eq!(count, 2); // Paris, London

    // Verify target table has correct data
    let paris_total = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT total FROM test_city_totals WHERE city = 'Paris'",
    )
    .expect("query")
    .expect("value");
    assert_eq!(paris_total.to_string(), "300");

    let london_total = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT total FROM test_city_totals WHERE city = 'London'",
    )
    .expect("query")
    .expect("value");
    assert_eq!(london_total.to_string(), "300");
}

#[pg_test]
fn test_create_avg_imv() {
    Spi::run("CREATE TABLE test_emp (id SERIAL, dept TEXT, salary NUMERIC)")
        .expect("create table");
    Spi::run(
        "INSERT INTO test_emp (dept, salary) VALUES
         ('eng', 100), ('eng', 200), ('sales', 150)",
    )
    .expect("insert data");

    let result = crate::create_reflex_ivm(
        "test_dept_avg",
        "SELECT dept, AVG(salary) AS avg_sal FROM test_emp GROUP BY dept",
        None,
        None,
        None,
    );
    assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");

    // Verify intermediate table has SUM and COUNT columns
    let eng_sum = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT \"__sum_salary\" FROM __reflex_intermediate_test_dept_avg WHERE dept = 'eng'",
    )
    .expect("query")
    .expect("value");
    assert_eq!(eng_sum.to_string(), "300");

    let eng_count = Spi::get_one::<i64>(
        "SELECT \"__count_salary\" FROM __reflex_intermediate_test_dept_avg WHERE dept = 'eng'",
    )
    .expect("query")
    .expect("value");
    assert_eq!(eng_count, 2);

    // Verify target table has correct AVG (150 = 300/2)
    let eng_avg = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT ROUND(avg_sal::numeric, 2) FROM test_dept_avg WHERE dept = 'eng'",
    )
    .expect("query")
    .expect("value");
    assert_eq!(eng_avg.to_string(), "150.00");
}

#[pg_test]
fn test_create_distinct_imv() {
    Spi::run("CREATE TABLE test_visits (id SERIAL, country TEXT)").expect("create table");
    Spi::run(
        "INSERT INTO test_visits (country) VALUES ('US'), ('US'), ('FR'), ('FR'), ('FR')",
    )
    .expect("insert data");

    let result = crate::create_reflex_ivm(
        "test_distinct_countries",
        "SELECT DISTINCT country FROM test_visits",
        None,
        None,
        None,
    );
    assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");

    // Verify target table has only distinct countries
    let count = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM test_distinct_countries",
    )
    .expect("query")
    .expect("count");
    assert_eq!(count, 2); // US, FR
}

#[pg_test]
fn test_create_count_star_imv() {
    Spi::run("CREATE TABLE test_items (id SERIAL, category TEXT)").expect("create table");
    Spi::run(
        "INSERT INTO test_items (category) VALUES ('A'), ('A'), ('A'), ('B'), ('B')",
    )
    .expect("insert data");

    let result = crate::create_reflex_ivm(
        "test_cat_counts",
        "SELECT category, COUNT(*) AS cnt FROM test_items GROUP BY category",
        None,
        None,
        None,
    );
    assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");

    let a_count = Spi::get_one::<i64>(
        "SELECT cnt FROM test_cat_counts WHERE category = 'A'",
    )
    .expect("query")
    .expect("value");
    assert_eq!(a_count, 3);
}

#[pg_test]
fn test_create_min_max_imv() {
    Spi::run("CREATE TABLE test_scores (id SERIAL, subject TEXT, score NUMERIC)")
        .expect("create table");
    Spi::run(
        "INSERT INTO test_scores (subject, score) VALUES
         ('math', 85), ('math', 92), ('math', 78),
         ('science', 88), ('science', 95)",
    )
    .expect("insert data");

    let result = crate::create_reflex_ivm(
        "test_score_range",
        "SELECT subject, MIN(score) AS lo, MAX(score) AS hi FROM test_scores GROUP BY subject",
        None,
        None,
        None,
    );
    assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");

    let math_lo = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT lo FROM test_score_range WHERE subject = 'math'",
    )
    .expect("query")
    .expect("value");
    assert_eq!(math_lo.to_string(), "78");

    let math_hi = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT hi FROM test_score_range WHERE subject = 'math'",
    )
    .expect("query")
    .expect("value");
    assert_eq!(math_hi.to_string(), "92");
}

#[pg_test]
fn test_create_multi_aggregate_imv() {
    Spi::run("CREATE TABLE test_sales (id SERIAL, region TEXT, revenue NUMERIC)")
        .expect("create table");
    Spi::run(
        "INSERT INTO test_sales (region, revenue) VALUES
         ('US', 1000), ('US', 2000), ('EU', 1500)",
    )
    .expect("insert data");

    let result = crate::create_reflex_ivm(
        "test_region_stats",
        "SELECT region, SUM(revenue) AS total, COUNT(*) AS cnt, AVG(revenue) AS avg_rev FROM test_sales GROUP BY region",
        None,
        None,
        None,
    );
    assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");

    let us_total = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT total FROM test_region_stats WHERE region = 'US'",
    )
    .expect("query")
    .expect("value");
    assert_eq!(us_total.to_string(), "3000");

    let us_cnt = Spi::get_one::<i64>(
        "SELECT cnt FROM test_region_stats WHERE region = 'US'",
    )
    .expect("query")
    .expect("value");
    assert_eq!(us_cnt, 2);
}

#[pg_test]
fn test_chained_imv_depth() {
    Spi::run("CREATE TABLE test_base (id SERIAL, val TEXT, amount NUMERIC)")
        .expect("create table");
    Spi::run(
        "INSERT INTO test_base (val, amount) VALUES ('a', 10), ('a', 20), ('b', 30)",
    )
    .expect("insert data");

    // First IMV at depth 1
    crate::create_reflex_ivm(
        "test_imv_1",
        "SELECT val, SUM(amount) AS total FROM test_base GROUP BY val",
        None,
        None,
        None,
    );

    // Second IMV depends on test_imv_1, should be at depth 2
    crate::create_reflex_ivm(
        "test_imv_2",
        "SELECT val, SUM(total) AS grand_total FROM test_imv_1 GROUP BY val",
        None,
        None,
        None,
    );

    let depth1 = Spi::get_one::<i32>(
        "SELECT graph_depth FROM public.__reflex_ivm_reference WHERE name = 'test_imv_1'",
    )
    .expect("query")
    .expect("value");
    assert_eq!(depth1, 1);

    let depth2 = Spi::get_one::<i32>(
        "SELECT graph_depth FROM public.__reflex_ivm_reference WHERE name = 'test_imv_2'",
    )
    .expect("query")
    .expect("value");
    assert_eq!(depth2, 2);

    // Verify graph_child of imv_1 includes imv_2
    let children = Spi::get_one::<Vec<String>>(
        "SELECT graph_child FROM public.__reflex_ivm_reference WHERE name = 'test_imv_1'",
    )
    .expect("query")
    .expect("value");
    assert!(children.contains(&"test_imv_2".to_string()));
}

#[pg_test]
fn test_reference_table_populated() {
    Spi::run("CREATE TABLE test_ref_src (id SERIAL, city TEXT, amount NUMERIC)")
        .expect("create table");
    Spi::run("INSERT INTO test_ref_src (city, amount) VALUES ('X', 1)").expect("insert");

    crate::create_reflex_ivm(
        "test_ref_view",
        "SELECT city, SUM(amount) AS total FROM test_ref_src GROUP BY city",
        None,
        None,
        None,
    );

    // Verify all key fields are populated
    let row = Spi::get_one::<bool>(
        "SELECT
            name IS NOT NULL
            AND graph_depth IS NOT NULL
            AND depends_on IS NOT NULL
            AND sql_query IS NOT NULL
            AND base_query IS NOT NULL
            AND end_query IS NOT NULL
            AND aggregations IS NOT NULL
            AND index_columns IS NOT NULL
            AND enabled = TRUE
         FROM public.__reflex_ivm_reference WHERE name = 'test_ref_view'",
    )
    .expect("query")
    .expect("value");
    assert!(row);
}

#[pg_test]
fn test_create_logged_imv() {
    Spi::run("CREATE TABLE log_orders (id SERIAL, city TEXT, amount NUMERIC)")
        .expect("create table");
    Spi::run("INSERT INTO log_orders (city, amount) VALUES ('Paris', 100), ('London', 200)")
        .expect("insert data");

    let result = crate::create_reflex_ivm(
        "log_city_totals",
        "SELECT city, SUM(amount) AS total FROM log_orders GROUP BY city",
        None,
        Some("LOGGED"),
        None,
    );
    assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");

    // Verify both tables are LOGGED (relpersistence = 'p')
    let target_persist = Spi::get_one::<String>(
        "SELECT relpersistence::text FROM pg_class WHERE relname = 'log_city_totals'",
    ).expect("query").expect("value");
    assert_eq!(target_persist, "p", "Target table should be permanent (logged)");

    let intermediate_persist = Spi::get_one::<String>(
        "SELECT relpersistence::text FROM pg_class WHERE relname = '__reflex_intermediate_log_city_totals'",
    ).expect("query").expect("value");
    assert_eq!(intermediate_persist, "p", "Intermediate table should be permanent (logged)");

    // Verify data is correct
    let paris_total = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT total FROM log_city_totals WHERE city = 'Paris'",
    ).expect("query").expect("value");
    assert_eq!(paris_total.to_string(), "100");

    // Verify storage_mode in reference table
    let mode = Spi::get_one::<String>(
        "SELECT storage_mode FROM public.__reflex_ivm_reference WHERE name = 'log_city_totals'",
    ).expect("query").expect("value");
    assert_eq!(mode, "LOGGED");
}

#[pg_test]
fn test_create_logged_passthrough() {
    Spi::run("CREATE TABLE log_pt_src (id SERIAL PRIMARY KEY, val TEXT NOT NULL)")
        .expect("create table");
    Spi::run("INSERT INTO log_pt_src (val) VALUES ('a'), ('b')").expect("insert");

    crate::create_reflex_ivm(
        "log_pt_view",
        "SELECT id, val FROM log_pt_src",
        None,
        Some("LOGGED"),
        None,
    );

    // Verify target table is LOGGED
    let persist = Spi::get_one::<String>(
        "SELECT relpersistence::text FROM pg_class WHERE relname = 'log_pt_view'",
    ).expect("query").expect("value");
    assert_eq!(persist, "p", "Passthrough target should be permanent (logged)");
}

/// 1.4.4 — the composite intermediate index is UNIQUE NULLS NOT DISTINCT for
/// multi-column groups, and `reflex_build_delta_sql` emits `=` for NOT NULL
/// group columns in MERGE ON clauses (so the planner can use that index
/// instead of seq-scanning the intermediate). Customer-reported regression:
/// 20-min hang on a single-row UPDATE because the MERGE used hash-join +
/// seq-scan of an 867 K-row intermediate.
#[pg_test]
fn pg_test_intermediate_unique_index_and_merge_eq_for_not_null() {
    Spi::run("CREATE SCHEMA mrg").expect("schema");
    Spi::run(
        "CREATE TABLE mrg.merge_src (\
            id     BIGINT NOT NULL PRIMARY KEY, \
            status TEXT   NOT NULL, \
            qty    INT)",
    )
    .expect("create src");
    Spi::run(
        "CREATE TABLE mrg.merge_join (\
            id          BIGINT NOT NULL PRIMARY KEY, \
            src_id      BIGINT NOT NULL, \
            grp_a       INT    NOT NULL, \
            grp_b       INT    NOT NULL, \
            nullable_g  INT)",
    )
    .expect("create join");
    Spi::run("INSERT INTO mrg.merge_src VALUES (1, 'validated', 10), (2, 'draft', 20)")
        .expect("seed src");
    Spi::run("INSERT INTO mrg.merge_join VALUES (1, 1, 100, 200, 9999), (2, 1, 100, 200, NULL)")
        .expect("seed join");

    // IMV with 4 group cols: 3 NOT NULL (grp_a, grp_b, status) + 1 NULLable
    // (nullable_g). After 1.4.4 the MERGE ON clause must emit `=` for the
    // 3 NOT NULL cols and `IS NOT DISTINCT FROM` for the NULLable one.
    crate::create_reflex_ivm(
        "mrg.merge_view",
        "SELECT mrg.merge_src.status, mrg.merge_join.grp_a, mrg.merge_join.grp_b, \
                mrg.merge_join.nullable_g, \
                SUM(mrg.merge_src.qty) AS total \
         FROM mrg.merge_src \
         INNER JOIN mrg.merge_join ON mrg.merge_join.src_id = mrg.merge_src.id \
         GROUP BY mrg.merge_src.status, mrg.merge_join.grp_a, mrg.merge_join.grp_b, \
                  mrg.merge_join.nullable_g",
        None,
        None,
        Some("IMMEDIATE"),
    );

    // (A) The composite intermediate index must be UNIQUE.
    let is_unique: bool = Spi::get_one(
        "SELECT ix.indisunique \
         FROM pg_index ix \
         JOIN pg_class cl ON cl.oid = ix.indexrelid \
         WHERE cl.relname = 'idx__reflex_int_merge_view'",
    )
    .expect("read indisunique")
    .expect("index exists");
    assert!(
        is_unique,
        "1.4.4: composite intermediate index must be UNIQUE"
    );

    // (B) The composite must also be NULLS NOT DISTINCT — so a NULL nullable_g
    // counts as one group, not many. PG 15+ exposes this via indnullsnotdistinct.
    let nulls_not_distinct: bool = Spi::get_one(
        "SELECT ix.indnullsnotdistinct \
         FROM pg_index ix \
         JOIN pg_class cl ON cl.oid = ix.indexrelid \
         WHERE cl.relname = 'idx__reflex_int_merge_view'",
    )
    .expect("read indnullsnotdistinct")
    .expect("index exists");
    assert!(
        nulls_not_distinct,
        "1.4.4: composite intermediate index must be NULLS NOT DISTINCT so NULL group keys collapse"
    );

    // (C) reflex_build_delta_sql for INSERT op on this IMV must emit `=`
    // for status / grp_a / grp_b (NOT NULL) and `IS NOT DISTINCT FROM` for
    // nullable_g.
    let sql: String = Spi::get_one(
        "SELECT public.reflex_build_delta_sql( \
             'mrg.merge_view', 'mrg.merge_src', 'INSERT', base_query, end_query, \
             aggregations::TEXT, base_query) \
         FROM public.__reflex_ivm_reference WHERE name = 'mrg.merge_view'"
    )
    .expect("build sql")
    .expect("non-empty");

    assert!(
        sql.contains("t.\"status\" = d.\"status\""),
        "NOT NULL `status` must use `=`: {}",
        sql
    );
    assert!(
        sql.contains("t.\"grp_a\" = d.\"grp_a\""),
        "NOT NULL `grp_a` must use `=`: {}",
        sql
    );
    assert!(
        sql.contains("t.\"grp_b\" = d.\"grp_b\""),
        "NOT NULL `grp_b` must use `=`: {}",
        sql
    );
    assert!(
        sql.contains("t.\"nullable_g\" IS NOT DISTINCT FROM d.\"nullable_g\""),
        "NULLable `nullable_g` must keep `IS NOT DISTINCT FROM`: {}",
        sql
    );

    // (D) Correctness end-to-end after an UPDATE.
    Spi::run("UPDATE mrg.merge_src SET qty = qty + 5 WHERE id = 1").expect("update");
    let fresh = "SELECT mrg.merge_src.status, mrg.merge_join.grp_a, mrg.merge_join.grp_b, \
                        mrg.merge_join.nullable_g, SUM(mrg.merge_src.qty) AS total \
                 FROM mrg.merge_src \
                 INNER JOIN mrg.merge_join ON mrg.merge_join.src_id = mrg.merge_src.id \
                 GROUP BY mrg.merge_src.status, mrg.merge_join.grp_a, mrg.merge_join.grp_b, \
                          mrg.merge_join.nullable_g";
    let mismatches: i64 = Spi::get_one(&format!(
        "SELECT COUNT(*) FROM ( \
            (SELECT * FROM mrg.merge_view EXCEPT ALL SELECT * FROM ({fresh}) f1) \
            UNION ALL \
            (SELECT * FROM ({fresh}) f2 EXCEPT ALL SELECT * FROM mrg.merge_view) \
         ) o"
    ))
    .expect("oracle")
    .expect("v");
    assert_eq!(
        mismatches, 0,
        "IMV must match fresh aggregate after UPDATE with the new MERGE ON clause"
    );
}

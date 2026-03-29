
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

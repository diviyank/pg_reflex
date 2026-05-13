
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
        None,
    );

    // Second IMV depends on test_imv_1, should be at depth 2
    crate::create_reflex_ivm(
        "test_imv_2",
        "SELECT val, SUM(total) AS grand_total FROM test_imv_1 GROUP BY val",
        None,
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
        None,
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

/// 1.4.5 — data-probe pass for `not_null_columns`. Reproduces the customer
/// regression: a JOIN key (`fk_id`) is declared NULLable on the source table
/// but the IMV's INNER JOIN excludes NULLs, so the column is effectively
/// non-NULL on the intermediate. The probe scans the intermediate at create
/// time, sees zero NULLs in `fk_id`, and adds it to `not_null_columns`. The
/// trigger codegen then emits `=` for `fk_id` (sargable, index-usable) instead
/// of `IS NOT DISTINCT FROM` (catalog-NULLable, defeats the composite index).
#[pg_test]
fn pg_test_probe_data_promotes_join_key_to_not_null() {
    Spi::run("CREATE SCHEMA prb").expect("schema");
    // Source `parent` has NOT NULL id. `child` has NULLable fk_id (matches the
    // yse.sales_simulation shape: FK column catalog-declared NULLable even
    // though the JOIN below forces it non-NULL on the join output).
    Spi::run("CREATE TABLE prb.parent (id BIGINT NOT NULL PRIMARY KEY, label TEXT NOT NULL)")
        .expect("create parent");
    Spi::run(
        "CREATE TABLE prb.child ( \
            id     BIGINT NOT NULL PRIMARY KEY, \
            fk_id  BIGINT,  /* NULLable! */ \
            grp    INT    NOT NULL, \
            qty    INT    NOT NULL)",
    )
    .expect("create child");
    Spi::run("INSERT INTO prb.parent VALUES (1, 'p1'), (2, 'p2')").expect("seed parent");
    Spi::run(
        "INSERT INTO prb.child VALUES \
            (10, 1, 100, 5), (11, 1, 100, 7), \
            (12, 2, 200, 3), (13, 2, 200, 9)",
    )
    .expect("seed child");

    // IMV: GROUP BY fk_id, grp. fk_id is catalog-NULLable BUT the INNER JOIN
    // on fk_id = parent.id makes it non-NULL on every output row.
    crate::create_reflex_ivm(
        "prb.kv",
        "SELECT c.fk_id, c.grp, SUM(c.qty) AS total \
         FROM prb.child c \
         INNER JOIN prb.parent p ON p.id = c.fk_id \
         GROUP BY c.fk_id, c.grp",
        None,
        None,
        Some("IMMEDIATE"),
        None,
    );

    // (A) The probe should have added `fk_id` to `not_null_columns`. `grp`
    // was already catalog-NOT-NULL so it would be present either way.
    let nn_cols: String = Spi::get_one(
        "SELECT (aggregations::jsonb->'not_null_columns')::text \
         FROM public.__reflex_ivm_reference WHERE name = 'prb.kv'",
    )
    .expect("read aggregations")
    .expect("row");
    assert!(
        nn_cols.contains("\"fk_id\""),
        "1.4.5: probe must promote effectively-NOT-NULL `fk_id` to not_null_columns; \
         got: {}",
        nn_cols
    );

    // (B) The MERGE ON clause must use `=` for both columns (index-usable).
    let sql: String = Spi::get_one(
        "SELECT public.reflex_build_delta_sql( \
             'prb.kv', 'prb.child', 'INSERT', base_query, end_query, \
             aggregations::TEXT, base_query) \
         FROM public.__reflex_ivm_reference WHERE name = 'prb.kv'",
    )
    .expect("build sql")
    .expect("non-empty");
    assert!(
        sql.contains("t.\"fk_id\" = d.\"fk_id\""),
        "1.4.5: MERGE ON `fk_id` must use `=` after data-probe; got: {}",
        sql
    );
    assert!(
        !sql.contains("t.\"fk_id\" IS NOT DISTINCT FROM"),
        "1.4.5: MERGE ON `fk_id` must NOT use IS NOT DISTINCT FROM after probe; got: {}",
        sql
    );

    // (C) Correctness: an UPDATE through the trigger still produces the right
    // aggregate even though we're now using `=` semantics on a catalog-NULLable
    // column. (Safety follows from the probe: there are no NULLs to mis-handle.)
    Spi::run("UPDATE prb.child SET qty = qty + 100 WHERE id = 10").expect("update");
    let fresh = "SELECT c.fk_id, c.grp, SUM(c.qty) AS total \
                 FROM prb.child c \
                 INNER JOIN prb.parent p ON p.id = c.fk_id \
                 GROUP BY c.fk_id, c.grp";
    let mismatches: i64 = Spi::get_one(&format!(
        "SELECT COUNT(*) FROM ( \
            (SELECT * FROM prb.kv EXCEPT ALL SELECT * FROM ({fresh}) f1) \
            UNION ALL \
            (SELECT * FROM ({fresh}) f2 EXCEPT ALL SELECT * FROM prb.kv) \
         ) o"
    ))
    .expect("oracle")
    .expect("v");
    assert_eq!(mismatches, 0, "IMV must match fresh aggregate after UPDATE");
}

/// 1.4.5 — the probe must NOT promote a column that actually contains NULLs.
/// If it did, the trigger's `=` semantics would silently split NULL groups
/// into multiple buckets → drift.
#[pg_test]
fn pg_test_probe_data_keeps_truly_nullable_column_as_null_safe() {
    Spi::run("CREATE SCHEMA prb2").expect("schema");
    Spi::run(
        "CREATE TABLE prb2.src ( \
            id    BIGINT NOT NULL PRIMARY KEY, \
            opt   INT,         /* truly nullable */ \
            qty   INT NOT NULL)",
    )
    .expect("create src");
    // Seed with a row whose `opt` is genuinely NULL.
    Spi::run("INSERT INTO prb2.src VALUES (1, 10, 5), (2, NULL, 7), (3, 10, 3)")
        .expect("seed src");

    crate::create_reflex_ivm(
        "prb2.kv",
        "SELECT opt, SUM(qty) AS total FROM prb2.src GROUP BY opt",
        None,
        None,
        Some("IMMEDIATE"),
        None,
    );

    let nn_cols: String = Spi::get_one(
        "SELECT (aggregations::jsonb->'not_null_columns')::text \
         FROM public.__reflex_ivm_reference WHERE name = 'prb2.kv'",
    )
    .expect("read aggregations")
    .expect("row");
    assert!(
        !nn_cols.contains("\"opt\""),
        "1.4.5: probe MUST NOT promote truly-NULLable `opt` to not_null_columns; \
         got: {}",
        nn_cols
    );

    // Correctness on UPDATE that touches both the NULL and non-NULL groups.
    Spi::run("UPDATE prb2.src SET qty = qty + 100 WHERE id IN (1, 2)").expect("update");
    let fresh = "SELECT opt, SUM(qty) AS total FROM prb2.src GROUP BY opt";
    let mismatches: i64 = Spi::get_one(&format!(
        "SELECT COUNT(*) FROM ( \
            (SELECT * FROM prb2.kv EXCEPT ALL SELECT * FROM ({fresh}) f1) \
            UNION ALL \
            (SELECT * FROM ({fresh}) f2 EXCEPT ALL SELECT * FROM prb2.kv) \
         ) o"
    ))
    .expect("oracle")
    .expect("v");
    assert_eq!(
        mismatches, 0,
        "NULL-safe MERGE path must merge NULL group correctly after UPDATE"
    );
}

/// 1.4.5 — high-selectivity dispatch: TRUNCATE+rebuild branch.
///
/// Forces the dispatch to take the wipe path by setting
/// reflex.wipe_threshold = 0 (any non-empty affected set qualifies). Asserts
/// that the IMV's final state matches a fresh aggregate (correctness
/// preserved across the path swap).
#[pg_test]
fn pg_test_wipe_dispatch_high_selectivity_correctness() {
    Spi::run("CREATE SCHEMA wdh").expect("schema");
    Spi::run("CREATE TABLE wdh.parent (id BIGINT PRIMARY KEY, label TEXT NOT NULL)")
        .expect("c parent");
    Spi::run("CREATE TABLE wdh.child (id BIGINT PRIMARY KEY, fk_id BIGINT NOT NULL, grp INT NOT NULL, qty INT NOT NULL)")
        .expect("c child");
    Spi::run("INSERT INTO wdh.parent VALUES (1,'a'),(2,'b'),(3,'c')").expect("seed parent");
    Spi::run(
        "INSERT INTO wdh.child VALUES \
            (1,1,10,5),(2,1,10,3),(3,1,20,7), \
            (4,2,10,2),(5,2,20,8), \
            (6,3,30,9)",
    )
    .expect("seed child");

    crate::create_reflex_ivm(
        "wdh.kv",
        "SELECT c.fk_id, c.grp, SUM(c.qty) AS total \
         FROM wdh.child c INNER JOIN wdh.parent p ON p.id = c.fk_id \
         GROUP BY c.fk_id, c.grp",
        None,
        None,
        Some("IMMEDIATE"),
        None,
    );

    // Force the wipe path: threshold=0 means any non-empty affected qualifies.
    Spi::run("SET reflex.wipe_threshold = '0'").expect("set threshold");

    // INSERT — adds a new row → affected has 1 group → wipe path fires.
    Spi::run("INSERT INTO wdh.child VALUES (7, 1, 10, 100)").expect("insert");
    let fresh = "SELECT c.fk_id, c.grp, SUM(c.qty) AS total \
                 FROM wdh.child c INNER JOIN wdh.parent p ON p.id = c.fk_id \
                 GROUP BY c.fk_id, c.grp";
    let mismatch: i64 = Spi::get_one(&format!(
        "SELECT COUNT(*) FROM ( \
            (SELECT * FROM wdh.kv EXCEPT ALL SELECT * FROM ({fresh}) f) \
            UNION ALL \
            (SELECT * FROM ({fresh}) f2 EXCEPT ALL SELECT * FROM wdh.kv) \
         ) o"
    ))
    .expect("oracle")
    .expect("v");
    assert_eq!(mismatch, 0, "wipe path INSERT must match fresh aggregate");

    // UPDATE — touches some rows → wipe path
    Spi::run("UPDATE wdh.child SET qty = qty + 50 WHERE fk_id = 1").expect("update");
    let mismatch: i64 = Spi::get_one(&format!(
        "SELECT COUNT(*) FROM ( \
            (SELECT * FROM wdh.kv EXCEPT ALL SELECT * FROM ({fresh}) f) \
            UNION ALL \
            (SELECT * FROM ({fresh}) f2 EXCEPT ALL SELECT * FROM wdh.kv) \
         ) o"
    ))
    .expect("oracle")
    .expect("v");
    assert_eq!(mismatch, 0, "wipe path UPDATE must match fresh aggregate");

    // DELETE — removes rows → wipe path
    Spi::run("DELETE FROM wdh.child WHERE id IN (1, 6)").expect("delete");
    let mismatch: i64 = Spi::get_one(&format!(
        "SELECT COUNT(*) FROM ( \
            (SELECT * FROM wdh.kv EXCEPT ALL SELECT * FROM ({fresh}) f) \
            UNION ALL \
            (SELECT * FROM ({fresh}) f2 EXCEPT ALL SELECT * FROM wdh.kv) \
         ) o"
    ))
    .expect("oracle")
    .expect("v");
    assert_eq!(mismatch, 0, "wipe path DELETE must match fresh aggregate");
}

/// 1.4.5 — dispatch must take the MERGE path when threshold is high
/// (so a tiny delta doesn't trigger a full refresh).
#[pg_test]
fn pg_test_wipe_dispatch_low_selectivity_uses_merge() {
    Spi::run("CREATE SCHEMA wdl").expect("schema");
    Spi::run("CREATE TABLE wdl.parent (id BIGINT PRIMARY KEY, label TEXT NOT NULL)")
        .expect("c parent");
    Spi::run("CREATE TABLE wdl.child (id BIGINT PRIMARY KEY, fk_id BIGINT NOT NULL, grp INT NOT NULL, qty INT NOT NULL)")
        .expect("c child");
    Spi::run("INSERT INTO wdl.parent VALUES (1,'a')").expect("seed");
    // 100 groups, 1 row each → intermediate has 100 rows.
    Spi::run(
        "INSERT INTO wdl.child (id, fk_id, grp, qty) \
         SELECT g, 1, g, 10 FROM generate_series(1, 100) g",
    )
    .expect("seed child");

    crate::create_reflex_ivm(
        "wdl.kv",
        "SELECT c.fk_id, c.grp, SUM(c.qty) AS total \
         FROM wdl.child c INNER JOIN wdl.parent p ON p.id = c.fk_id \
         GROUP BY c.fk_id, c.grp",
        None,
        None,
        Some("IMMEDIATE"),
        None,
    );

    // Threshold 0.5: single-row affected (1/100 = 1 %) → MERGE path.
    Spi::run("SET reflex.wipe_threshold = '0.5'").expect("set threshold");
    Spi::run("UPDATE wdl.child SET qty = qty + 100 WHERE id = 5").expect("update");

    let fresh = "SELECT c.fk_id, c.grp, SUM(c.qty) AS total \
                 FROM wdl.child c INNER JOIN wdl.parent p ON p.id = c.fk_id \
                 GROUP BY c.fk_id, c.grp";
    let mismatch: i64 = Spi::get_one(&format!(
        "SELECT COUNT(*) FROM ( \
            (SELECT * FROM wdl.kv EXCEPT ALL SELECT * FROM ({fresh}) f) \
            UNION ALL \
            (SELECT * FROM ({fresh}) f2 EXCEPT ALL SELECT * FROM wdl.kv) \
         ) o"
    ))
    .expect("oracle")
    .expect("v");
    assert_eq!(
        mismatch, 0,
        "MERGE path single-row UPDATE must match fresh aggregate"
    );
}

/// 1.4.5 — `create_reflex_ivm(... ignore_sources)` suppresses trigger
/// installation on listed sources. DML on those sources must NOT update the
/// IMV; reconcile picks up the drift.
#[pg_test]
fn pg_test_ignore_sources_suppresses_trigger() {
    Spi::run("CREATE SCHEMA isr").expect("schema");
    Spi::run("CREATE TABLE isr.parent (id BIGINT PRIMARY KEY, label TEXT NOT NULL)")
        .expect("c parent");
    Spi::run("CREATE TABLE isr.child (id BIGINT PRIMARY KEY, fk_id BIGINT NOT NULL, qty INT NOT NULL)")
        .expect("c child");
    Spi::run("INSERT INTO isr.parent VALUES (1, 'p1'), (2, 'p2')").expect("seed parent");
    Spi::run("INSERT INTO isr.child VALUES (1,1,5),(2,1,3),(3,2,7)").expect("seed child");

    // Create IMV with parent ignored — UPDATEs on parent should NOT fire
    // a trigger that refreshes the IMV.
    let r = Spi::get_one::<&str>(
        "SELECT public.create_reflex_ivm( \
            'isr.kv', \
            'SELECT c.fk_id, SUM(c.qty) AS total FROM isr.child c \
             INNER JOIN isr.parent p ON p.id = c.fk_id GROUP BY c.fk_id', \
            NULL, 'UNLOGGED', 'IMMEDIATE', 'isr.parent' \
         )",
    )
    .expect("create")
    .expect("v");
    assert_eq!(r, "CREATE REFLEX INCREMENTAL VIEW");

    // No trigger should exist on isr.parent for this IMV's flow.
    let trig_count: i64 = Spi::get_one(
        "SELECT count(*) FROM pg_trigger t \
         JOIN pg_class c ON c.oid = t.tgrelid \
         JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE n.nspname = 'isr' AND c.relname = 'parent' \
           AND t.tgname LIKE '\\_\\_reflex\\_trigger%'",
    )
    .expect("trig count")
    .expect("v");
    assert_eq!(
        trig_count, 0,
        "no reflex trigger should be installed on isr.parent — got {}",
        trig_count
    );

    // Trigger SHOULD exist on isr.child (not ignored).
    let trig_count_child: i64 = Spi::get_one(
        "SELECT count(*) FROM pg_trigger t \
         JOIN pg_class c ON c.oid = t.tgrelid \
         JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE n.nspname = 'isr' AND c.relname = 'child' \
           AND t.tgname LIKE '\\_\\_reflex\\_trigger%'",
    )
    .expect("trig count child")
    .expect("v");
    assert!(
        trig_count_child > 0,
        "reflex triggers must be installed on isr.child (not ignored)"
    );

    // Insert a NEW parent and child for it. Since parent is ignored, the
    // IMV should NOT see the new fk_id row. Verify drift.
    Spi::run("INSERT INTO isr.parent VALUES (3, 'p3')").expect("new parent");
    Spi::run("INSERT INTO isr.child VALUES (4, 3, 99)").expect("new child");
    // Child INSERT WILL fire a trigger — but the IMV's JOIN requires
    // parent.id = c.fk_id so the new (fk_id=3) row needs parent=3 to be
    // visible. It IS visible (we inserted it). So the IMV DOES update.
    let total_for_3: Option<i64> = Spi::get_one(
        "SELECT total FROM isr.kv WHERE fk_id = 3",
    )
    .expect("query");
    assert!(
        total_for_3.is_some(),
        "child INSERT should still update IMV (child not ignored)"
    );

    // Now UPDATE parent.label. With parent ignored, the trigger doesn't
    // fire. The IMV doesn't reference label so the IMV's state is unchanged
    // anyway — but the key test is that the trigger doesn't run (proxy: no
    // error from a malformed dispatch).
    Spi::run("UPDATE isr.parent SET label = 'p3-renamed' WHERE id = 3")
        .expect("update parent");

    // reconcile should rebuild correctly regardless.
    let _ = Spi::get_one::<&str>("SELECT public.reflex_reconcile('isr.kv')")
        .expect("reconcile");
    let total_for_3: Option<i64> = Spi::get_one(
        "SELECT total FROM isr.kv WHERE fk_id = 3",
    )
    .expect("query");
    assert!(total_for_3.is_some(), "post-reconcile IMV must include fk_id=3");
}

/// 1.4.5 — `reflex_compact_all_imv()` on an empty catalog returns the
/// no-op marker. VACUUM FULL itself cannot run inside the pgrx test
/// transaction wrapper, so we only test the iteration entry / empty
/// fast-path here. End-to-end VACUUM behavior is exercised by the
/// 1.4.4→1.4.5 migration and operator-facing benchmarks.
#[pg_test]
fn pg_test_reflex_compact_all_imv_empty_catalog() {
    let summary: String = Spi::get_one("SELECT public.reflex_compact_all_imv()")
        .expect("compact all")
        .expect("v");
    assert!(
        summary.contains("no enabled IMVs"),
        "expected empty-catalog marker, got: {}",
        summary
    );
}

/// 1.4.5 — `reflex_probe_not_null_columns(view_name)` re-probes an existing
/// IMV. Used by the 1.4.4→1.4.5 migration. Idempotent: a second call after
/// no data change must report zero additions.
#[pg_test]
fn pg_test_reflex_probe_not_null_columns_idempotent() {
    Spi::run("CREATE SCHEMA prb3").expect("schema");
    Spi::run("CREATE TABLE prb3.parent (id BIGINT NOT NULL PRIMARY KEY)").expect("c parent");
    Spi::run("CREATE TABLE prb3.child (id BIGINT NOT NULL PRIMARY KEY, fk_id BIGINT, qty INT NOT NULL)")
        .expect("c child");
    Spi::run("INSERT INTO prb3.parent VALUES (1)").expect("seed parent");
    Spi::run("INSERT INTO prb3.child VALUES (1, 1, 10)").expect("seed child");

    crate::create_reflex_ivm(
        "prb3.kv",
        "SELECT c.fk_id, SUM(c.qty) AS total FROM prb3.child c \
         INNER JOIN prb3.parent p ON p.id = c.fk_id GROUP BY c.fk_id",
        None,
        None,
        Some("IMMEDIATE"),
        None,
    );

    // First call (from creation) should have already promoted fk_id.
    // Calling the SQL-exposed entrypoint again returns "no additional".
    let result: String = Spi::get_one(
        "SELECT public.reflex_probe_not_null_columns('prb3.kv')",
    )
    .expect("call probe")
    .expect("row");
    assert!(
        result.contains("no additional"),
        "second call must be idempotent (no new columns); got: {}",
        result
    );
}

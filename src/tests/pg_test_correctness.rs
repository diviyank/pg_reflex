// ========================================================================
// Group 1 — Targeted refresh tests
// ========================================================================

/// Test that INSERT creates new groups and updates existing groups correctly.
#[pg_test]
fn pg_test_targeted_refresh_insert_correctness() {
    // Setup: 100 rows across 10 groups (group_id 0..9, 10 rows each)
    Spi::run("CREATE TABLE tr_src (id SERIAL, group_id INT NOT NULL, amount NUMERIC NOT NULL)").expect("create");
    Spi::run(
        "INSERT INTO tr_src (group_id, amount) \
         SELECT i % 10, (i * 7 % 100)::numeric FROM generate_series(1, 100) i"
    ).expect("seed");

    Spi::run(
        "SELECT create_reflex_ivm('tr_insert_test', \
         'SELECT group_id, SUM(amount) AS total, COUNT(*) AS cnt FROM tr_src GROUP BY group_id')"
    ).expect("create imv");

    // Verify 10 groups
    let cnt = Spi::get_one::<i64>("SELECT COUNT(*) FROM tr_insert_test").expect("q").expect("v");
    assert_eq!(cnt, 10);

    // INSERT 20 rows: 15 into existing groups (0..4), 5 into NEW groups (10..14)
    Spi::run(
        "INSERT INTO tr_src (group_id, amount) \
         SELECT CASE WHEN i <= 15 THEN (i - 1) % 5 ELSE i - 16 + 10 END, 100.0 \
         FROM generate_series(1, 20) i"
    ).expect("insert");

    // Now should have 15 groups (10 original + 5 new)
    let cnt2 = Spi::get_one::<i64>("SELECT COUNT(*) FROM tr_insert_test").expect("q").expect("v");
    assert_eq!(cnt2, 15);

    // Verify correctness against direct query
    let mismatches = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM ( \
            SELECT r.group_id::text FROM tr_insert_test r \
            FULL OUTER JOIN (SELECT group_id, SUM(amount) AS total, COUNT(*) AS cnt FROM tr_src GROUP BY group_id) d \
                ON r.group_id::text = d.group_id::text \
            WHERE r.total IS DISTINCT FROM d.total OR r.cnt IS DISTINCT FROM d.cnt \
        ) x"
    ).expect("q").expect("v");
    assert_eq!(mismatches, 0, "IMV should match direct query after INSERT");
}

/// Test that DELETE removes groups when all their rows are deleted.
#[pg_test]
fn pg_test_targeted_refresh_delete_group_elimination() {
    Spi::run("CREATE TABLE tr_del_src (id SERIAL, region TEXT NOT NULL, amount NUMERIC NOT NULL)").expect("create");
    Spi::run("INSERT INTO tr_del_src (region, amount) VALUES ('A', 10), ('A', 20), ('A', 30)").expect("ins A");
    Spi::run("INSERT INTO tr_del_src (region, amount) VALUES ('B', 40), ('B', 50)").expect("ins B");
    Spi::run("INSERT INTO tr_del_src (region, amount) VALUES ('C', 60)").expect("ins C");

    Spi::run(
        "SELECT create_reflex_ivm('tr_del_test', \
         'SELECT region, SUM(amount) AS total, COUNT(*) AS cnt FROM tr_del_src GROUP BY region')"
    ).expect("create imv");

    // 3 groups: A(60, 3), B(90, 2), C(60, 1)
    let cnt = Spi::get_one::<i64>("SELECT COUNT(*) FROM tr_del_test").expect("q").expect("v");
    assert_eq!(cnt, 3);

    // Delete ALL rows from group B
    Spi::run("DELETE FROM tr_del_src WHERE region = 'B'").expect("delete B");

    // Group B should be gone
    let cnt2 = Spi::get_one::<i64>("SELECT COUNT(*) FROM tr_del_test").expect("q").expect("v");
    assert_eq!(cnt2, 2);

    let has_b = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM tr_del_test WHERE region = 'B'"
    ).expect("q").expect("v");
    assert_eq!(has_b, 0, "Group B should be eliminated");

    // A and C should be unchanged
    let a_total = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT total FROM tr_del_test WHERE region = 'A'"
    ).expect("q").expect("v");
    assert_eq!(a_total.to_string(), "60");
}

/// Test that UPDATE correctly handles rows changing groups.
#[pg_test]
fn pg_test_targeted_refresh_update_group_change() {
    Spi::run("CREATE TABLE tr_upd_src (id SERIAL, region TEXT NOT NULL, amount NUMERIC NOT NULL)").expect("create");
    Spi::run("INSERT INTO tr_upd_src (region, amount) VALUES \
              ('East', 100), ('East', 200), ('West', 300), ('West', 400)").expect("seed");

    Spi::run(
        "SELECT create_reflex_ivm('tr_upd_test', \
         'SELECT region, SUM(amount) AS total, COUNT(*) AS cnt FROM tr_upd_src GROUP BY region')"
    ).expect("create imv");

    // East=300(2), West=700(2)
    let east = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT total FROM tr_upd_test WHERE region = 'East'"
    ).expect("q").expect("v");
    assert_eq!(east.to_string(), "300");

    // Move one East row to a NEW group "North"
    Spi::run("UPDATE tr_upd_src SET region = 'North' WHERE id = 1").expect("update");

    // East should lose 100 (now 200, cnt=1), North should appear (100, cnt=1), West unchanged
    let east2 = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT total FROM tr_upd_test WHERE region = 'East'"
    ).expect("q").expect("v");
    assert_eq!(east2.to_string(), "200");

    let north = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT total FROM tr_upd_test WHERE region = 'North'"
    ).expect("q").expect("v");
    assert_eq!(north.to_string(), "100");

    let west = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT total FROM tr_upd_test WHERE region = 'West'"
    ).expect("q").expect("v");
    assert_eq!(west.to_string(), "700");

    let cnt = Spi::get_one::<i64>("SELECT COUNT(*) FROM tr_upd_test").expect("q").expect("v");
    assert_eq!(cnt, 3);
}

/// Test targeted refresh with multi-column GROUP BY.
#[pg_test]
fn pg_test_targeted_refresh_multi_column_group() {
    Spi::run("CREATE TABLE tr_mc_src (id SERIAL, region TEXT NOT NULL, category TEXT NOT NULL, amount NUMERIC NOT NULL)").expect("create");
    Spi::run("INSERT INTO tr_mc_src (region, category, amount) VALUES \
              ('US', 'A', 10), ('US', 'B', 20), ('EU', 'A', 30), ('EU', 'B', 40)").expect("seed");

    Spi::run(
        "SELECT create_reflex_ivm('tr_mc_test', \
         'SELECT region, category, SUM(amount) AS total FROM tr_mc_src GROUP BY region, category')"
    ).expect("create imv");

    // 4 groups: US-A(10), US-B(20), EU-A(30), EU-B(40)
    let cnt = Spi::get_one::<i64>("SELECT COUNT(*) FROM tr_mc_test").expect("q").expect("v");
    assert_eq!(cnt, 4);

    // INSERT into existing group US-A and new group US-C
    Spi::run("INSERT INTO tr_mc_src (region, category, amount) VALUES ('US', 'A', 5), ('US', 'C', 50)").expect("insert");

    let cnt2 = Spi::get_one::<i64>("SELECT COUNT(*) FROM tr_mc_test").expect("q").expect("v");
    assert_eq!(cnt2, 5, "Should have 5 groups after insert (4 + US-C)");

    let us_a = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT total FROM tr_mc_test WHERE region = 'US' AND category = 'A'"
    ).expect("q").expect("v");
    assert_eq!(us_a.to_string(), "15"); // 10 + 5

    // DELETE all EU rows
    Spi::run("DELETE FROM tr_mc_src WHERE region = 'EU'").expect("delete");

    let cnt3 = Spi::get_one::<i64>("SELECT COUNT(*) FROM tr_mc_test").expect("q").expect("v");
    assert_eq!(cnt3, 3, "Should have 3 groups after deleting EU");
}

/// Test that INTEGER GROUP BY columns are preserved (not cast to TEXT).
#[pg_test]
fn pg_test_integer_group_by_type_preservation() {
    Spi::run("CREATE TABLE tr_type_src (id SERIAL, bucket_id INTEGER NOT NULL, val NUMERIC NOT NULL)").expect("create");
    Spi::run("INSERT INTO tr_type_src (bucket_id, val) SELECT i % 5, i::numeric FROM generate_series(1, 50) i").expect("seed");

    Spi::run(
        "SELECT create_reflex_ivm('tr_type_test', \
         'SELECT bucket_id, SUM(val) AS total, COUNT(*) AS cnt FROM tr_type_src GROUP BY bucket_id')"
    ).expect("create imv");

    // Check the column type in the target table — should preserve INTEGER
    let col_type = Spi::get_one::<String>(
        "SELECT data_type::text FROM information_schema.columns \
         WHERE table_name = 'tr_type_test' AND column_name = 'bucket_id'"
    ).expect("q").expect("v");
    assert_eq!(col_type, "integer", "bucket_id should be INTEGER, not TEXT");

    // Regardless of type, correctness should hold
    Spi::run("INSERT INTO tr_type_src (bucket_id, val) VALUES (0, 999), (5, 111)").expect("insert");

    let cnt = Spi::get_one::<i64>("SELECT COUNT(*) FROM tr_type_test").expect("q").expect("v");
    assert_eq!(cnt, 6, "Should have 6 groups (0-4 original + 5 new)");

    // Full correctness check using text cast to handle both cases
    let mismatches = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM ( \
            SELECT r.bucket_id FROM tr_type_test r \
            FULL OUTER JOIN (SELECT bucket_id, SUM(val) AS total FROM tr_type_src GROUP BY bucket_id) d \
                ON r.bucket_id::text = d.bucket_id::text \
            WHERE r.total IS DISTINCT FROM d.total \
        ) x"
    ).expect("q").expect("v");
    assert_eq!(mismatches, 0, "IMV should match direct query");
}

/// Test correctness with higher cardinality (10K rows, 1K groups).
#[pg_test]
fn pg_test_high_cardinality_correctness() {
    Spi::run("CREATE TABLE tr_hc_src (id SERIAL, grp INT NOT NULL, val NUMERIC NOT NULL)").expect("create");
    Spi::run(
        "INSERT INTO tr_hc_src (grp, val) \
         SELECT i % 1000, ROUND((random() * 100)::numeric, 2) FROM generate_series(1, 10000) i"
    ).expect("seed");

    Spi::run(
        "SELECT create_reflex_ivm('tr_hc_test', \
         'SELECT grp, SUM(val) AS total, COUNT(*) AS cnt FROM tr_hc_src GROUP BY grp')"
    ).expect("create imv");

    let cnt = Spi::get_one::<i64>("SELECT COUNT(*) FROM tr_hc_test").expect("q").expect("v");
    assert_eq!(cnt, 1000);

    // INSERT 500 rows (some new groups 1000..1049, some existing)
    Spi::run(
        "INSERT INTO tr_hc_src (grp, val) \
         SELECT CASE WHEN i <= 450 THEN i % 500 ELSE 999 + i - 449 END, 10.0 \
         FROM generate_series(1, 500) i"
    ).expect("insert");

    // DELETE 200 rows from known ids
    Spi::run("DELETE FROM tr_hc_src WHERE id <= 200").expect("delete");

    // UPDATE 100 rows (change amounts)
    Spi::run("UPDATE tr_hc_src SET val = val + 1 WHERE id > 200 AND id <= 300").expect("update");

    // Full correctness verification
    let mismatches = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM ( \
            SELECT r.grp FROM tr_hc_test r \
            FULL OUTER JOIN (SELECT grp, SUM(val) AS total, COUNT(*) AS cnt FROM tr_hc_src GROUP BY grp) d \
                ON r.grp::text = d.grp::text \
            WHERE r.total IS DISTINCT FROM d.total OR r.cnt IS DISTINCT FROM d.cnt \
        ) x"
    ).expect("q").expect("v");
    assert_eq!(mismatches, 0, "IMV should match direct query after INSERT+DELETE+UPDATE");

    // Verify group count makes sense
    let final_cnt = Spi::get_one::<i64>("SELECT COUNT(*) FROM tr_hc_test").expect("q").expect("v");
    let expected_cnt = Spi::get_one::<i64>(
        "SELECT COUNT(DISTINCT grp) FROM tr_hc_src"
    ).expect("q").expect("v");
    assert_eq!(final_cnt, expected_cnt, "Group count should match source distinct count");
}

// ========================================================================
// Group 2 — Edge case correctness tests
// ========================================================================

#[pg_test]
fn test_empty_source_table() {
    Spi::run("CREATE TABLE empty_src (id SERIAL, grp TEXT, val NUMERIC)").expect("create table");
    let result = crate::create_reflex_ivm(
        "empty_view",
        "SELECT grp, SUM(val) AS total FROM empty_src GROUP BY grp",
        None,
        None,
        None,
    );
    assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");
    let count =
        Spi::get_one::<i64>("SELECT COUNT(*) FROM empty_view").expect("q").expect("v");
    assert_eq!(count, 0, "Empty source should produce empty view");
    // Now insert and verify trigger works
    Spi::run("INSERT INTO empty_src (grp, val) VALUES ('x', 42)").expect("insert");
    let total = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT total FROM empty_view WHERE grp = 'x'",
    )
    .expect("q")
    .expect("v");
    assert_eq!(total.to_string(), "42");
}

#[pg_test]
fn test_update_group_by_column() {
    Spi::run(
        "CREATE TABLE grpmove_src (id SERIAL, grp TEXT, val NUMERIC)",
    )
    .expect("create table");
    Spi::run(
        "INSERT INTO grpmove_src (grp, val) VALUES ('A', 10), ('A', 20), ('B', 30)",
    )
    .expect("seed");
    crate::create_reflex_ivm(
        "grpmove_view",
        "SELECT grp, SUM(val) AS total FROM grpmove_src GROUP BY grp",
        None,
        None,
        None,
    );
    // Move a row from group A to group B
    Spi::run("UPDATE grpmove_src SET grp = 'B' WHERE val = 10").expect("update");
    let a = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT total FROM grpmove_view WHERE grp = 'A'",
    )
    .expect("q")
    .expect("v");
    assert_eq!(a.to_string(), "20", "Group A should have lost 10");
    let b = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT total FROM grpmove_view WHERE grp = 'B'",
    )
    .expect("q")
    .expect("v");
    assert_eq!(b.to_string(), "40", "Group B should have gained 10");
}

#[pg_test]
fn test_min_max_delete_recompute() {
    Spi::run("CREATE TABLE mmr_src (id SERIAL, grp TEXT, val NUMERIC)").expect("create table");
    Spi::run("INSERT INTO mmr_src (grp, val) VALUES ('X', 10), ('X', 20), ('X', 30)")
        .expect("seed");
    crate::create_reflex_ivm(
        "mmr_view",
        "SELECT grp, MIN(val) AS lo, MAX(val) AS hi FROM mmr_src GROUP BY grp",
        None,
        None,
        None,
    );
    let lo =
        Spi::get_one::<pgrx::AnyNumeric>("SELECT lo FROM mmr_view WHERE grp = 'X'")
            .expect("q")
            .expect("v");
    assert_eq!(lo.to_string(), "10", "Initial MIN should be 10");
    // Delete the MIN row — should trigger recompute
    Spi::run("DELETE FROM mmr_src WHERE val = 10").expect("delete min");
    let lo2 =
        Spi::get_one::<pgrx::AnyNumeric>("SELECT lo FROM mmr_view WHERE grp = 'X'")
            .expect("q")
            .expect("v");
    assert_eq!(lo2.to_string(), "20", "After deleting 10, MIN should be 20");
}

#[pg_test]
fn test_delete_all_rows_from_source() {
    Spi::run("CREATE TABLE delall_src (id SERIAL, grp TEXT, val NUMERIC)").expect("create table");
    Spi::run("INSERT INTO delall_src (grp, val) VALUES ('A', 10), ('B', 20)").expect("seed");
    crate::create_reflex_ivm(
        "delall_view",
        "SELECT grp, SUM(val) AS total FROM delall_src GROUP BY grp",
        None,
        None,
        None,
    );
    Spi::run("DELETE FROM delall_src").expect("delete all");
    let count =
        Spi::get_one::<i64>("SELECT COUNT(*) FROM delall_view").expect("q").expect("v");
    assert_eq!(count, 0, "View should be empty after deleting all source rows");
}

#[pg_test]
fn test_null_in_aggregate_expression() {
    Spi::run(
        "CREATE TABLE null_agg_src (id SERIAL, grp TEXT NOT NULL, val NUMERIC)",
    )
    .expect("create table");
    Spi::run(
        "INSERT INTO null_agg_src (grp, val) VALUES ('A', 10), ('A', NULL), ('A', 30)",
    )
    .expect("seed");
    crate::create_reflex_ivm(
        "null_agg_view",
        "SELECT grp, SUM(val) AS total, COUNT(val) AS cnt FROM null_agg_src GROUP BY grp",
        None,
        None,
        None,
    );
    // SUM should ignore NULL: 10 + 30 = 40
    let total = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT total FROM null_agg_view WHERE grp = 'A'",
    )
    .expect("q")
    .expect("v");
    assert_eq!(total.to_string(), "40", "SUM should ignore NULLs");
    // COUNT(val) should skip NULL: 2
    let cnt = Spi::get_one::<i64>(
        "SELECT cnt FROM null_agg_view WHERE grp = 'A'",
    )
    .expect("q")
    .expect("v");
    assert_eq!(cnt, 2, "COUNT(col) should skip NULLs");
}

#[pg_test]
fn test_count_col_vs_count_star() {
    Spi::run(
        "CREATE TABLE ccvs_src (id SERIAL, grp TEXT NOT NULL, val NUMERIC)",
    )
    .expect("create table");
    Spi::run(
        "INSERT INTO ccvs_src (grp, val) VALUES ('X', 1), ('X', NULL), ('X', 3), ('X', NULL)",
    )
    .expect("seed");
    crate::create_reflex_ivm(
        "ccvs_view",
        "SELECT grp, COUNT(*) AS cnt_star, COUNT(val) AS cnt_val FROM ccvs_src GROUP BY grp",
        None,
        None,
        None,
    );
    let cnt_star = Spi::get_one::<i64>(
        "SELECT cnt_star FROM ccvs_view WHERE grp = 'X'",
    )
    .expect("q")
    .expect("v");
    assert_eq!(cnt_star, 4, "COUNT(*) should count all rows including NULLs");
    let cnt_val = Spi::get_one::<i64>(
        "SELECT cnt_val FROM ccvs_view WHERE grp = 'X'",
    )
    .expect("q")
    .expect("v");
    assert_eq!(cnt_val, 2, "COUNT(col) should skip NULLs");
}

#[pg_test]
fn test_distinct_with_group_by() {
    Spi::run(
        "CREATE TABLE dg_src (id SERIAL, grp TEXT NOT NULL, val TEXT NOT NULL)",
    )
    .expect("create table");
    Spi::run(
        "INSERT INTO dg_src (grp, val) VALUES \
         ('A', 'x'), ('A', 'x'), ('A', 'y'), ('B', 'x'), ('B', 'x')",
    )
    .expect("seed");
    let result = crate::create_reflex_ivm(
        "dg_view",
        "SELECT DISTINCT grp, val FROM dg_src",
        None,
        None,
        None,
    );
    assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");
    let count =
        Spi::get_one::<i64>("SELECT COUNT(*) FROM dg_view").expect("q").expect("v");
    // DISTINCT (A,x), (A,y), (B,x) = 3 unique pairs
    assert_eq!(count, 3, "DISTINCT should eliminate duplicate (grp, val) pairs");
}

#[pg_test]
fn test_insert_zero_rows() {
    Spi::run(
        "CREATE TABLE zr_src (id SERIAL, grp TEXT NOT NULL, val NUMERIC NOT NULL)",
    )
    .expect("create table");
    Spi::run("INSERT INTO zr_src (grp, val) VALUES ('A', 10)").expect("seed");
    crate::create_reflex_ivm(
        "zr_view",
        "SELECT grp, SUM(val) AS total FROM zr_src GROUP BY grp",
        None,
        None,
        None,
    );
    // Insert zero rows (WHERE false) — trigger fires but no delta
    Spi::run("INSERT INTO zr_src (grp, val) SELECT 'B', 99 WHERE false").expect("empty insert");
    let count =
        Spi::get_one::<i64>("SELECT COUNT(*) FROM zr_view").expect("q").expect("v");
    assert_eq!(count, 1, "Zero-row insert should not change view");
}

#[pg_test]
fn test_update_value_only() {
    Spi::run(
        "CREATE TABLE uvo_src (id SERIAL, grp TEXT NOT NULL, val NUMERIC NOT NULL)",
    )
    .expect("create table");
    Spi::run(
        "INSERT INTO uvo_src (grp, val) VALUES ('A', 10), ('A', 20)",
    )
    .expect("seed");
    crate::create_reflex_ivm(
        "uvo_view",
        "SELECT grp, SUM(val) AS total FROM uvo_src GROUP BY grp",
        None,
        None,
        None,
    );
    // Update value, not group column
    Spi::run("UPDATE uvo_src SET val = 50 WHERE val = 10").expect("update");
    let total = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT total FROM uvo_view WHERE grp = 'A'",
    )
    .expect("q")
    .expect("v");
    assert_eq!(total.to_string(), "70", "SUM should be 50 + 20 = 70");
}

#[pg_test]
fn test_multiple_deletes_same_group() {
    Spi::run(
        "CREATE TABLE md_src (id SERIAL, grp TEXT NOT NULL, val NUMERIC NOT NULL)",
    )
    .expect("create table");
    Spi::run(
        "INSERT INTO md_src (grp, val) VALUES ('A', 10), ('A', 20), ('A', 30), ('A', 40)",
    )
    .expect("seed");
    crate::create_reflex_ivm(
        "md_view",
        "SELECT grp, SUM(val) AS total, COUNT(*) AS cnt FROM md_src GROUP BY grp",
        None,
        None,
        None,
    );
    // Delete two rows separately
    Spi::run("DELETE FROM md_src WHERE val = 10").expect("delete 1");
    Spi::run("DELETE FROM md_src WHERE val = 30").expect("delete 2");
    let total = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT total FROM md_view WHERE grp = 'A'",
    )
    .expect("q")
    .expect("v");
    assert_eq!(total.to_string(), "60", "SUM should be 20 + 40 = 60");
    let cnt = Spi::get_one::<i64>(
        "SELECT cnt FROM md_view WHERE grp = 'A'",
    )
    .expect("q")
    .expect("v");
    assert_eq!(cnt, 2, "COUNT should be 2 after deleting 2 of 4 rows");
}

#[pg_test]
fn test_large_batch_correctness() {
    Spi::run(
        "CREATE TABLE lb_src (id SERIAL, grp TEXT NOT NULL, val NUMERIC NOT NULL)",
    )
    .expect("create table");
    // 10K rows across 100 groups
    Spi::run(
        "INSERT INTO lb_src (grp, val) \
         SELECT 'g' || (i % 100), i FROM generate_series(1, 10000) i",
    )
    .expect("seed 10K rows");
    crate::create_reflex_ivm(
        "lb_view",
        "SELECT grp, SUM(val) AS total, COUNT(*) AS cnt FROM lb_src GROUP BY grp",
        None,
        None,
        None,
    );
    // Compare IMV against direct query
    let mismatches = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM ( \
            SELECT grp, total, cnt FROM lb_view \
            EXCEPT \
            SELECT grp, SUM(val) AS total, COUNT(*) AS cnt FROM lb_src GROUP BY grp \
        ) x",
    )
    .expect("q")
    .expect("v");
    assert_eq!(mismatches, 0, "IMV should match direct query for 10K rows");
    // Insert another batch and re-verify
    Spi::run(
        "INSERT INTO lb_src (grp, val) \
         SELECT 'g' || (i % 100), i FROM generate_series(10001, 15000) i",
    )
    .expect("insert 5K more");
    let mismatches2 = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM ( \
            SELECT grp, total, cnt FROM lb_view \
            EXCEPT \
            SELECT grp, SUM(val) AS total, COUNT(*) AS cnt FROM lb_src GROUP BY grp \
        ) x",
    )
    .expect("q")
    .expect("v");
    assert_eq!(mismatches2, 0, "IMV should match after additional batch insert");
}

#[pg_test]
fn test_avg_with_all_same_values() {
    Spi::run(
        "CREATE TABLE avg_same_src (id SERIAL, grp TEXT NOT NULL, val NUMERIC NOT NULL)",
    )
    .expect("create table");
    Spi::run(
        "INSERT INTO avg_same_src (grp, val) VALUES ('X', 42), ('X', 42), ('X', 42)",
    )
    .expect("seed");
    crate::create_reflex_ivm(
        "avg_same_view",
        "SELECT grp, AVG(val) AS avg_val FROM avg_same_src GROUP BY grp",
        None,
        None,
        None,
    );
    let avg = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT avg_val FROM avg_same_view WHERE grp = 'X'",
    )
    .expect("q")
    .expect("v");
    // AVG of identical values should be that value (no precision loss)
    let avg_f: f64 = avg.to_string().parse().expect("parse avg");
    assert!(
        (avg_f - 42.0).abs() < 0.0001,
        "AVG of identical values should be exact, got {}",
        avg_f
    );
}

// ========================================================================
// Group 3 — Correctness named tests (oracle-based)
// ========================================================================

/// A1: COUNT(*) vs COUNT(col) with NULLs
#[pg_test]
fn test_correctness_count_with_nulls() {
    Spi::run("CREATE TABLE ca1 (id SERIAL, grp TEXT, val INT)").expect("create");
    Spi::run("INSERT INTO ca1 (grp, val) VALUES ('a', 1), ('a', NULL), ('b', NULL), ('b', 3), ('b', NULL)").expect("seed");

    crate::create_reflex_ivm("ca1_view",
        "SELECT grp, COUNT(*) AS cnt_star, COUNT(val) AS cnt_val FROM ca1 GROUP BY grp",
        None, None, None);

    let fresh = "SELECT grp, COUNT(*) AS cnt_star, COUNT(val) AS cnt_val FROM ca1 GROUP BY grp";
    assert_imv_correct("ca1_view", fresh);

    // Insert more NULLs
    Spi::run("INSERT INTO ca1 (grp, val) VALUES ('a', NULL), ('c', NULL)").expect("insert");
    assert_imv_correct("ca1_view", fresh);

    // Delete non-NULL
    Spi::run("DELETE FROM ca1 WHERE val = 1").expect("delete");
    assert_imv_correct("ca1_view", fresh);
}

/// A2: Group disappears after deleting all rows
#[pg_test]
fn test_correctness_group_disappears() {
    Spi::run("CREATE TABLE ca2 (id SERIAL PRIMARY KEY, grp TEXT, val INT)").expect("create");
    Spi::run("INSERT INTO ca2 (grp, val) VALUES ('a', 10), ('a', 20), ('b', 30)").expect("seed");

    crate::create_reflex_ivm("ca2_view",
        "SELECT grp, SUM(val) AS total FROM ca2 GROUP BY grp",
        None, None, None);

    let fresh = "SELECT grp, SUM(val) AS total FROM ca2 GROUP BY grp";
    assert_imv_correct("ca2_view", fresh);

    // Delete all 'a' rows -> group should vanish
    Spi::run("DELETE FROM ca2 WHERE grp = 'a'").expect("delete");
    assert_imv_correct("ca2_view", fresh);

    // Only 'b' should remain
    assert_eq!(
        Spi::get_one::<i64>("SELECT COUNT(*) FROM ca2_view").expect("q").expect("v"),
        1
    );
}

/// A3: Full-table aggregate on empty table (SUM=NULL, COUNT=0, AVG=NULL)
#[pg_test]
fn test_correctness_empty_table_aggregates() {
    Spi::run("CREATE TABLE ca3 (id SERIAL PRIMARY KEY, val INT)").expect("create");
    Spi::run("INSERT INTO ca3 (val) VALUES (10), (20), (30)").expect("seed");

    crate::create_reflex_ivm("ca3_view",
        "SELECT SUM(val) AS s, COUNT(val) AS c, COUNT(*) AS cs FROM ca3",
        None, None, None);

    let fresh = "SELECT SUM(val) AS s, COUNT(val) AS c, COUNT(*) AS cs FROM ca3";
    assert_imv_correct("ca3_view", fresh);

    // Delete all rows
    Spi::run("DELETE FROM ca3").expect("delete all");
    // Full-table aggregate without GROUP BY on empty table:
    // SUM=NULL, COUNT(val)=0, COUNT(*)=0
    assert_imv_correct("ca3_view", fresh);
}

/// A5: MIN/MAX after deleting the extremum
#[pg_test]
fn test_correctness_min_max_extremum_deleted() {
    Spi::run("CREATE TABLE ca5 (id SERIAL PRIMARY KEY, grp TEXT, val INT NOT NULL)").expect("create");
    Spi::run("INSERT INTO ca5 (grp, val) VALUES ('a', 10), ('a', 20), ('a', 30), ('b', 5), ('b', 15)").expect("seed");

    crate::create_reflex_ivm("ca5_view",
        "SELECT grp, MIN(val) AS lo, MAX(val) AS hi FROM ca5 GROUP BY grp",
        None, None, None);

    let fresh = "SELECT grp, MIN(val) AS lo, MAX(val) AS hi FROM ca5 GROUP BY grp";
    assert_imv_correct("ca5_view", fresh);

    // Delete the MIN of group 'a' (val=10) and MAX of group 'b' (val=15)
    Spi::run("DELETE FROM ca5 WHERE (grp = 'a' AND val = 10) OR (grp = 'b' AND val = 15)").expect("delete extrema");
    assert_imv_correct("ca5_view", fresh);

    // Now a: MIN=20, MAX=30; b: MIN=5, MAX=5
    let a_lo = Spi::get_one::<i32>("SELECT lo FROM ca5_view WHERE grp = 'a'").expect("q").expect("v");
    assert_eq!(a_lo, 20);
}

/// A7: Multiple aggregates on same column
#[pg_test]
fn test_correctness_multi_agg_same_col() {
    Spi::run("CREATE TABLE ca7 (id SERIAL, grp TEXT, a INT NOT NULL)").expect("create");
    Spi::run("INSERT INTO ca7 (grp, a) VALUES ('x', 10), ('x', 20), ('x', 30), ('y', 5)").expect("seed");

    crate::create_reflex_ivm("ca7_view",
        "SELECT grp, COUNT(a) AS c, MIN(a) AS lo, MAX(a) AS hi, SUM(a) AS s FROM ca7 GROUP BY grp",
        None, None, None);

    let fresh = "SELECT grp, COUNT(a) AS c, MIN(a) AS lo, MAX(a) AS hi, SUM(a) AS s FROM ca7 GROUP BY grp";
    assert_imv_correct("ca7_view", fresh);

    Spi::run("INSERT INTO ca7 (grp, a) VALUES ('x', 1), ('y', 100)").expect("insert");
    assert_imv_correct("ca7_view", fresh);

    Spi::run("DELETE FROM ca7 WHERE a = 1").expect("delete");
    assert_imv_correct("ca7_view", fresh);

    Spi::run("UPDATE ca7 SET a = 99 WHERE a = 30").expect("update");
    assert_imv_correct("ca7_view", fresh);
}

/// A9: HAVING with threshold crossing
#[pg_test]
fn test_correctness_having_threshold() {
    Spi::run("CREATE TABLE ca9 (id SERIAL, grp TEXT, val INT)").expect("create");
    Spi::run("INSERT INTO ca9 (grp, val) VALUES ('a', 10), ('a', 20), ('b', 5)").expect("seed");

    crate::create_reflex_ivm("ca9_view",
        "SELECT grp, SUM(val) AS total FROM ca9 GROUP BY grp HAVING SUM(val) > 15",
        None, None, None);

    let fresh = "SELECT grp, SUM(val) AS total FROM ca9 GROUP BY grp HAVING SUM(val) > 15";
    assert_imv_correct("ca9_view", fresh);

    // b has SUM=5, below threshold. Insert to push it over.
    Spi::run("INSERT INTO ca9 (grp, val) VALUES ('b', 20)").expect("insert");
    assert_imv_correct("ca9_view", fresh);

    // Delete from 'a' to push it below threshold
    Spi::run("DELETE FROM ca9 WHERE grp = 'a' AND val = 20").expect("delete");
    assert_imv_correct("ca9_view", fresh);
}

/// B1: Self-join — auto-detected, uses full refresh
#[pg_test]
fn test_correctness_self_join() {
    Spi::run("CREATE TABLE cb1 (id SERIAL PRIMARY KEY, i INT NOT NULL, v INT NOT NULL)").expect("create");
    Spi::run("INSERT INTO cb1 (i, v) VALUES (1, 10), (2, 20), (1, 30)").expect("seed");

    crate::create_reflex_ivm("cb1_view",
        "SELECT t1.i, SUM(t1.v + t2.v) AS total FROM cb1 t1 JOIN cb1 t2 ON t1.i = t2.i GROUP BY t1.i",
        None, None, None);

    let fresh = "SELECT t1.i, SUM(t1.v + t2.v) AS total FROM cb1 t1 JOIN cb1 t2 ON t1.i = t2.i GROUP BY t1.i";
    assert_imv_correct("cb1_view", fresh);

    // INSERT triggers full refresh for self-join (auto-detected)
    Spi::run("INSERT INTO cb1 (i, v) VALUES (1, 5)").expect("insert");
    assert_imv_correct("cb1_view", fresh);

    // DELETE also triggers full refresh
    Spi::run("DELETE FROM cb1 WHERE v = 5").expect("delete");
    assert_imv_correct("cb1_view", fresh);

    // UPDATE too
    Spi::run("UPDATE cb1 SET v = 99 WHERE i = 2").expect("update");
    assert_imv_correct("cb1_view", fresh);
}

/// B6: JOIN producing duplicates (1:many)
#[pg_test]
fn test_correctness_join_duplicates() {
    Spi::run("CREATE TABLE cb6_a (id SERIAL PRIMARY KEY, grp TEXT)").expect("create");
    Spi::run("CREATE TABLE cb6_b (id SERIAL PRIMARY KEY, grp TEXT, val INT)").expect("create");
    Spi::run("INSERT INTO cb6_a (grp) VALUES ('x'), ('y')").expect("seed");
    Spi::run("INSERT INTO cb6_b (grp, val) VALUES ('x', 1), ('x', 2), ('x', 3), ('y', 10)").expect("seed");

    crate::create_reflex_ivm("cb6_view",
        "SELECT a.grp, SUM(b.val) AS total FROM cb6_a a JOIN cb6_b b ON a.grp = b.grp GROUP BY a.grp",
        None, None, None);

    let fresh = "SELECT a.grp, SUM(b.val) AS total FROM cb6_a a JOIN cb6_b b ON a.grp = b.grp GROUP BY a.grp";
    assert_imv_correct("cb6_view", fresh);

    Spi::run("INSERT INTO cb6_b (grp, val) VALUES ('x', 100)").expect("insert b");
    assert_imv_correct("cb6_view", fresh);

    Spi::run("INSERT INTO cb6_a (grp) VALUES ('x')").expect("insert a duplicate grp");
    assert_imv_correct("cb6_view", fresh);
}

/// C3/C4: Insert NULL, update non-NULL to NULL
#[pg_test]
fn test_correctness_null_mutations() {
    Spi::run("CREATE TABLE cc (id SERIAL PRIMARY KEY, grp TEXT NOT NULL, val INT)").expect("create");
    Spi::run("INSERT INTO cc (grp, val) VALUES ('a', 10), ('a', 20), ('b', 30)").expect("seed");

    crate::create_reflex_ivm("cc_view",
        "SELECT grp, SUM(val) AS total, COUNT(val) AS cv, COUNT(*) AS cs FROM cc GROUP BY grp",
        None, None, None);

    let fresh = "SELECT grp, SUM(val) AS total, COUNT(val) AS cv, COUNT(*) AS cs FROM cc GROUP BY grp";
    assert_imv_correct("cc_view", fresh);

    // Insert NULL val
    Spi::run("INSERT INTO cc (grp, val) VALUES ('a', NULL)").expect("insert null");
    assert_imv_correct("cc_view", fresh);

    // Update non-NULL to NULL
    Spi::run("UPDATE cc SET val = NULL WHERE val = 10").expect("update to null");
    assert_imv_correct("cc_view", fresh);

    // Update NULL to non-NULL
    Spi::run("UPDATE cc SET val = 99 WHERE id = (SELECT id FROM cc WHERE val IS NULL LIMIT 1)").expect("update from null");
    assert_imv_correct("cc_view", fresh);
}

/// D1: DISTINCT ref counting — insert duplicate, delete one copy
#[pg_test]
fn test_correctness_distinct_refcount() {
    Spi::run("CREATE TABLE cd1 (id SERIAL PRIMARY KEY, val TEXT)").expect("create");
    Spi::run("INSERT INTO cd1 (val) VALUES ('a'), ('a'), ('a'), ('b'), ('b')").expect("seed");

    crate::create_reflex_ivm("cd1_view", "SELECT DISTINCT val FROM cd1", None, None, None);
    let fresh = "SELECT DISTINCT val FROM cd1";
    assert_imv_correct("cd1_view", fresh);

    // Delete one 'a' — should still appear
    Spi::run("DELETE FROM cd1 WHERE id = 1").expect("delete");
    assert_imv_correct("cd1_view", fresh);

    // Delete remaining 'a's
    Spi::run("DELETE FROM cd1 WHERE val = 'a'").expect("delete all a");
    assert_imv_correct("cd1_view", fresh);

    // Insert new value
    Spi::run("INSERT INTO cd1 (val) VALUES ('c'), ('c')").expect("insert");
    assert_imv_correct("cd1_view", fresh);
}

/// F1: TRUNCATE
#[pg_test]
fn test_correctness_truncate() {
    Spi::run("CREATE TABLE cf1 (id SERIAL, grp TEXT, val INT)").expect("create");
    Spi::run("INSERT INTO cf1 (grp, val) VALUES ('a', 10), ('b', 20)").expect("seed");

    crate::create_reflex_ivm("cf1_view",
        "SELECT grp, SUM(val) AS total FROM cf1 GROUP BY grp",
        None, None, None);

    let fresh = "SELECT grp, SUM(val) AS total FROM cf1 GROUP BY grp";
    assert_imv_correct("cf1_view", fresh);

    Spi::run("TRUNCATE cf1").expect("truncate");
    assert_imv_correct("cf1_view", fresh);

    // Re-insert
    Spi::run("INSERT INTO cf1 (grp, val) VALUES ('c', 100)").expect("reinsert");
    assert_imv_correct("cf1_view", fresh);
}

/// F3: UPDATE that changes GROUP BY key (moves row between groups)
#[pg_test]
fn test_correctness_update_group_key() {
    Spi::run("CREATE TABLE cf3 (id SERIAL PRIMARY KEY, grp TEXT NOT NULL, val INT NOT NULL)").expect("create");
    Spi::run("INSERT INTO cf3 (grp, val) VALUES ('a', 10), ('a', 20), ('b', 30)").expect("seed");

    crate::create_reflex_ivm("cf3_view",
        "SELECT grp, SUM(val) AS total, COUNT(*) AS cnt FROM cf3 GROUP BY grp",
        None, None, None);

    let fresh = "SELECT grp, SUM(val) AS total, COUNT(*) AS cnt FROM cf3 GROUP BY grp";
    assert_imv_correct("cf3_view", fresh);

    // Move a row from 'a' to 'b'
    Spi::run("UPDATE cf3 SET grp = 'b' WHERE val = 10").expect("move row");
    assert_imv_correct("cf3_view", fresh);

    // a: SUM=20, COUNT=1; b: SUM=40, COUNT=2
    let a = Spi::get_one::<i64>("SELECT total FROM cf3_view WHERE grp = 'a'")
        .expect("q").expect("v");
    assert_eq!(a, 20i64);
}

/// F6: Large batch insert (10K rows) — verify correctness at scale
#[pg_test]
fn test_correctness_batch_insert_10k() {
    Spi::run("CREATE TABLE cf6 (id SERIAL, grp TEXT, val INT)").expect("create");
    Spi::run("INSERT INTO cf6 (grp, val) VALUES ('seed', 1)").expect("seed");

    crate::create_reflex_ivm("cf6_view",
        "SELECT grp, SUM(val) AS total, COUNT(*) AS cnt FROM cf6 GROUP BY grp",
        None, None, None);

    // Insert 10K rows across 100 groups
    Spi::run("INSERT INTO cf6 (grp, val) SELECT 'g' || (i % 100), i FROM generate_series(1, 10000) i").expect("batch");
    let fresh = "SELECT grp, SUM(val) AS total, COUNT(*) AS cnt FROM cf6 GROUP BY grp";
    assert_imv_correct("cf6_view", fresh);
}

/// CTE with multiple levels — verify cascading is correct
#[pg_test]
fn test_correctness_cte_cascade() {
    Spi::run("CREATE TABLE cte_src (id SERIAL, region TEXT, city TEXT, amount INT)").expect("create");
    Spi::run("INSERT INTO cte_src (region, city, amount) VALUES \
        ('EU', 'Paris', 100), ('EU', 'Berlin', 200), ('US', 'NYC', 300), ('US', 'LA', 50)").expect("seed");

    crate::create_reflex_ivm("cte_view",
        "WITH by_city AS (SELECT region, city, SUM(amount) AS city_total FROM cte_src GROUP BY region, city) \
         SELECT region, SUM(city_total) AS total, COUNT(*) AS num_cities FROM by_city GROUP BY region",
        None, None, None);

    let fresh = "WITH by_city AS (SELECT region, city, SUM(amount) AS city_total FROM cte_src GROUP BY region, city) \
                 SELECT region, SUM(city_total) AS total, COUNT(*) AS num_cities FROM by_city GROUP BY region";
    assert_imv_correct("cte_view", fresh);

    Spi::run("INSERT INTO cte_src (region, city, amount) VALUES ('EU', 'Madrid', 150)").expect("insert");
    assert_imv_correct("cte_view", fresh);

    Spi::run("DELETE FROM cte_src WHERE city = 'LA'").expect("delete");
    assert_imv_correct("cte_view", fresh);

    Spi::run("UPDATE cte_src SET amount = 999 WHERE city = 'Paris'").expect("update");
    assert_imv_correct("cte_view", fresh);
}

/// UNION ALL correctness after mixed INSERT/DELETE
#[pg_test]
fn test_correctness_union_all() {
    Spi::run("CREATE TABLE cu_a (id SERIAL PRIMARY KEY, val TEXT)").expect("create");
    Spi::run("CREATE TABLE cu_b (id SERIAL PRIMARY KEY, val TEXT)").expect("create");
    Spi::run("INSERT INTO cu_a (val) VALUES ('x'), ('y')").expect("seed");
    Spi::run("INSERT INTO cu_b (val) VALUES ('y'), ('z')").expect("seed");

    crate::create_reflex_ivm("cu_view",
        "SELECT val FROM cu_a UNION ALL SELECT val FROM cu_b",
        None, None, None);

    let fresh = "SELECT val FROM cu_a UNION ALL SELECT val FROM cu_b";
    assert_imv_correct("cu_view", fresh);

    Spi::run("INSERT INTO cu_a (val) VALUES ('z')").expect("insert");
    assert_imv_correct("cu_view", fresh);

    Spi::run("DELETE FROM cu_b WHERE val = 'y'").expect("delete");
    assert_imv_correct("cu_view", fresh);
}

/// WINDOW GROUP BY + RANK correctness through multiple mutations
#[pg_test]
fn test_correctness_window_groupby_rank() {
    Spi::run("CREATE TABLE cw (id SERIAL, city TEXT, amount INT)").expect("create");
    Spi::run("INSERT INTO cw (city, amount) VALUES \
        ('a', 100), ('a', 200), ('b', 50), ('c', 300), ('c', 100)").expect("seed");

    crate::create_reflex_ivm("cw_view",
        "SELECT city, SUM(amount) AS total, RANK() OVER (ORDER BY SUM(amount) DESC) AS rnk FROM cw GROUP BY city",
        None, None, None);

    let fresh = "SELECT city, SUM(amount) AS total, RANK() OVER (ORDER BY SUM(amount) DESC) AS rnk FROM cw GROUP BY city";
    assert_imv_correct("cw_view", fresh);

    // INSERT changes ranking
    Spi::run("INSERT INTO cw (city, amount) VALUES ('b', 500)").expect("insert");
    assert_imv_correct("cw_view", fresh);

    // DELETE changes ranking
    Spi::run("DELETE FROM cw WHERE city = 'c' AND amount = 300").expect("delete");
    assert_imv_correct("cw_view", fresh);

    // UPDATE changes ranking
    Spi::run("UPDATE cw SET amount = 1 WHERE city = 'a'").expect("update");
    assert_imv_correct("cw_view", fresh);
}

/// AVG with values that don't divide evenly
#[pg_test]
fn test_correctness_avg_precision() {
    Spi::run("CREATE TABLE cavg (id SERIAL, grp TEXT, val NUMERIC)").expect("create");
    Spi::run("INSERT INTO cavg (grp, val) VALUES ('a', 1), ('a', 2), ('a', 3)").expect("seed");

    crate::create_reflex_ivm("cavg_view",
        "SELECT grp, AVG(val) AS avg_val FROM cavg GROUP BY grp",
        None, None, None);

    let fresh = "SELECT grp, AVG(val) AS avg_val FROM cavg GROUP BY grp";
    assert_imv_correct("cavg_view", fresh);

    // Add a value that makes AVG non-integer
    Spi::run("INSERT INTO cavg (grp, val) VALUES ('a', 7)").expect("insert");
    assert_imv_correct("cavg_view", fresh);
    // AVG = (1+2+3+7)/4 = 3.25

    // Delete and recheck
    Spi::run("DELETE FROM cavg WHERE val = 2").expect("delete");
    assert_imv_correct("cavg_view", fresh);
    // AVG = (1+3+7)/3 = 3.666...
}

/// Passthrough JOIN — INSERT/UPDATE/DELETE correctness
#[pg_test]
fn test_correctness_passthrough_join() {
    Spi::run("CREATE TABLE cp_src (id SERIAL PRIMARY KEY, did INT NOT NULL, val TEXT)").expect("create");
    Spi::run("CREATE TABLE cp_dim (id SERIAL PRIMARY KEY, label TEXT)").expect("create");
    Spi::run("INSERT INTO cp_dim (label) VALUES ('A'), ('B'), ('C')").expect("seed dim");
    Spi::run("INSERT INTO cp_src (did, val) VALUES (1, 'x'), (2, 'y'), (1, 'z')").expect("seed src");

    crate::create_reflex_ivm("cp_view",
        "SELECT s.id, s.val, d.label FROM cp_src s JOIN cp_dim d ON s.did = d.id",
        Some("id"), None, None);

    let fresh = "SELECT s.id, s.val, d.label FROM cp_src s JOIN cp_dim d ON s.did = d.id";
    assert_imv_correct("cp_view", fresh);

    Spi::run("INSERT INTO cp_src (did, val) VALUES (3, 'new')").expect("insert");
    assert_imv_correct("cp_view", fresh);

    Spi::run("UPDATE cp_src SET val = 'updated' WHERE id = 1").expect("update");
    assert_imv_correct("cp_view", fresh);

    Spi::run("DELETE FROM cp_src WHERE id = 2").expect("delete");
    assert_imv_correct("cp_view", fresh);
}

/// UPDATE that doesn't change any value (SET val = val) — should be a no-op
#[pg_test]
fn test_correctness_noop_update() {
    Spi::run("CREATE TABLE nop (id SERIAL PRIMARY KEY, grp TEXT NOT NULL, val INT NOT NULL)").expect("create");
    Spi::run("INSERT INTO nop (grp, val) VALUES ('a', 10), ('b', 20)").expect("seed");

    crate::create_reflex_ivm("nop_view",
        "SELECT grp, SUM(val) AS total FROM nop GROUP BY grp",
        None, None, None);

    let fresh = "SELECT grp, SUM(val) AS total FROM nop GROUP BY grp";
    assert_imv_correct("nop_view", fresh);

    // No-op update — same values
    Spi::run("UPDATE nop SET val = val").expect("noop update");
    assert_imv_correct("nop_view", fresh);

    // No-op update with WHERE FALSE — 0 rows affected
    Spi::run("UPDATE nop SET val = 999 WHERE FALSE").expect("where false");
    assert_imv_correct("nop_view", fresh);
}

/// DELETE WHERE FALSE — 0 rows affected
#[pg_test]
fn test_correctness_delete_where_false() {
    Spi::run("CREATE TABLE dwf (id SERIAL, grp TEXT, val INT)").expect("create");
    Spi::run("INSERT INTO dwf (grp, val) VALUES ('a', 10), ('b', 20)").expect("seed");

    crate::create_reflex_ivm("dwf_view",
        "SELECT grp, SUM(val) AS total FROM dwf GROUP BY grp",
        None, None, None);

    let fresh = "SELECT grp, SUM(val) AS total FROM dwf GROUP BY grp";
    Spi::run("DELETE FROM dwf WHERE FALSE").expect("delete where false");
    assert_imv_correct("dwf_view", fresh);
}

/// INSERT exact duplicate rows — aggregate must count both
#[pg_test]
fn test_correctness_exact_duplicates() {
    Spi::run("CREATE TABLE dup (id SERIAL, grp TEXT, val INT)").expect("create");
    Spi::run("INSERT INTO dup (grp, val) VALUES ('a', 10), ('a', 10), ('a', 10)").expect("seed");

    crate::create_reflex_ivm("dup_view",
        "SELECT grp, SUM(val) AS total, COUNT(*) AS cnt FROM dup GROUP BY grp",
        None, None, None);

    let fresh = "SELECT grp, SUM(val) AS total, COUNT(*) AS cnt FROM dup GROUP BY grp";
    assert_imv_correct("dup_view", fresh);
    // total=30, cnt=3

    // Insert more exact duplicates
    Spi::run("INSERT INTO dup (grp, val) VALUES ('a', 10), ('a', 10)").expect("more dups");
    assert_imv_correct("dup_view", fresh);
    // total=50, cnt=5
}

// ========================================================================
// Group 4 — More correctness tests
// ========================================================================

/// UPDATE that changes JOIN key — row disappears from JOIN result
#[pg_test]
fn test_correctness_update_join_key() {
    Spi::run("CREATE TABLE ujk_src (id SERIAL PRIMARY KEY, did INT NOT NULL, val TEXT)").expect("create");
    Spi::run("CREATE TABLE ujk_dim (id INT PRIMARY KEY, label TEXT)").expect("create");
    Spi::run("INSERT INTO ujk_dim VALUES (1, 'A'), (2, 'B')").expect("seed dim");
    Spi::run("INSERT INTO ujk_src (did, val) VALUES (1, 'x'), (2, 'y')").expect("seed src");

    crate::create_reflex_ivm("ujk_view",
        "SELECT s.id, s.val, d.label FROM ujk_src s JOIN ujk_dim d ON s.did = d.id",
        Some("id"), None, None);

    let fresh = "SELECT s.id, s.val, d.label FROM ujk_src s JOIN ujk_dim d ON s.did = d.id";
    assert_imv_correct("ujk_view", fresh);

    // Update join key to a non-existent dim ID — row should disappear from JOIN result
    Spi::run("UPDATE ujk_src SET did = 999 WHERE id = 1").expect("orphan");
    assert_imv_correct("ujk_view", fresh);

    // Only row with did=2 should remain
    assert_eq!(
        Spi::get_one::<i64>("SELECT COUNT(*) FROM ujk_view").expect("q").expect("v"),
        1
    );
}

/// DELETE from dimension table in a JOIN — orphaned source rows vanish from view
#[pg_test]
fn test_correctness_delete_dimension() {
    Spi::run("CREATE TABLE dd_src (id SERIAL PRIMARY KEY, did INT NOT NULL, val INT)").expect("create");
    Spi::run("CREATE TABLE dd_dim (id INT PRIMARY KEY, label TEXT)").expect("create");
    Spi::run("INSERT INTO dd_dim VALUES (1, 'A'), (2, 'B')").expect("seed dim");
    Spi::run("INSERT INTO dd_src (did, val) VALUES (1, 10), (1, 20), (2, 30)").expect("seed src");

    crate::create_reflex_ivm("dd_view",
        "SELECT d.label, SUM(s.val) AS total FROM dd_src s JOIN dd_dim d ON s.did = d.id GROUP BY d.label",
        None, None, None);

    let fresh = "SELECT d.label, SUM(s.val) AS total FROM dd_src s JOIN dd_dim d ON s.did = d.id GROUP BY d.label";
    assert_imv_correct("dd_view", fresh);

    // Delete dimension row — orphans source rows
    Spi::run("DELETE FROM dd_dim WHERE id = 1").expect("delete dim");
    assert_imv_correct("dd_view", fresh);
}

/// DISTINCT with UPDATE — value changes, old and new must both be tracked
#[pg_test]
fn test_correctness_distinct_update() {
    Spi::run("CREATE TABLE du (id SERIAL PRIMARY KEY, val TEXT)").expect("create");
    Spi::run("INSERT INTO du (val) VALUES ('a'), ('a'), ('b')").expect("seed");

    crate::create_reflex_ivm("du_view", "SELECT DISTINCT val FROM du", None, None, None);
    let fresh = "SELECT DISTINCT val FROM du";
    assert_imv_correct("du_view", fresh);

    // Update one 'a' to 'c' — 'a' should still exist (refcount=1), 'c' appears
    Spi::run("UPDATE du SET val = 'c' WHERE id = 1").expect("update");
    assert_imv_correct("du_view", fresh);

    // Update last 'a' to 'c' — 'a' should vanish, 'c' refcount=2
    Spi::run("UPDATE du SET val = 'c' WHERE val = 'a'").expect("update last");
    assert_imv_correct("du_view", fresh);
}

/// BOOL_OR with DELETE — should recompute from source
#[pg_test]
fn test_correctness_bool_or_delete() {
    Spi::run("CREATE TABLE bo (id SERIAL PRIMARY KEY, grp TEXT NOT NULL, flag BOOLEAN NOT NULL)").expect("create");
    Spi::run("INSERT INTO bo (grp, flag) VALUES ('a', true), ('a', false), ('b', false), ('b', false)").expect("seed");

    crate::create_reflex_ivm("bo_view",
        "SELECT grp, bool_or(flag) AS any_true FROM bo GROUP BY grp",
        None, None, None);

    let fresh = "SELECT grp, bool_or(flag) AS any_true FROM bo GROUP BY grp";
    assert_imv_correct("bo_view", fresh);

    // Delete the only TRUE row in 'a' — bool_or should become FALSE
    Spi::run("DELETE FROM bo WHERE grp = 'a' AND flag = true").expect("delete true");
    assert_imv_correct("bo_view", fresh);

    // Insert TRUE into 'b'
    Spi::run("INSERT INTO bo (grp, flag) VALUES ('b', true)").expect("insert true");
    assert_imv_correct("bo_view", fresh);
}

/// Very large single group (10K rows in 1 group) — stress intermediate MERGE
#[pg_test]
fn test_correctness_large_single_group() {
    Spi::run("CREATE TABLE lsg (id SERIAL, grp TEXT, val INT)").expect("create");
    Spi::run("INSERT INTO lsg (grp, val) SELECT 'only', i FROM generate_series(1, 10000) i").expect("seed");

    crate::create_reflex_ivm("lsg_view",
        "SELECT grp, SUM(val) AS total, COUNT(*) AS cnt, MIN(val) AS lo, MAX(val) AS hi FROM lsg GROUP BY grp",
        None, None, None);

    let fresh = "SELECT grp, SUM(val) AS total, COUNT(*) AS cnt, MIN(val) AS lo, MAX(val) AS hi FROM lsg GROUP BY grp";
    assert_imv_correct("lsg_view", fresh);

    // Delete the MIN row (val=1)
    Spi::run("DELETE FROM lsg WHERE val = 1").expect("delete min");
    assert_imv_correct("lsg_view", fresh);

    // Delete the MAX row (val=10000)
    Spi::run("DELETE FROM lsg WHERE val = 10000").expect("delete max");
    assert_imv_correct("lsg_view", fresh);

    // Bulk update
    Spi::run("UPDATE lsg SET val = val + 1 WHERE val <= 100").expect("bulk update");
    assert_imv_correct("lsg_view", fresh);
}

/// Rapid successive mutations — INSERT, UPDATE, DELETE in sequence
#[pg_test]
fn test_correctness_rapid_mutations() {
    Spi::run("CREATE TABLE rm (id SERIAL PRIMARY KEY, grp TEXT NOT NULL, val INT NOT NULL)").expect("create");
    Spi::run("INSERT INTO rm (grp, val) VALUES ('a', 1), ('b', 2), ('c', 3)").expect("seed");

    crate::create_reflex_ivm("rm_view",
        "SELECT grp, SUM(val) AS total FROM rm GROUP BY grp",
        None, None, None);

    let fresh = "SELECT grp, SUM(val) AS total FROM rm GROUP BY grp";
    assert_imv_correct("rm_view", fresh);

    // 10 rapid mutations
    Spi::run("INSERT INTO rm (grp, val) VALUES ('a', 10)").expect("1");
    Spi::run("UPDATE rm SET val = val * 2 WHERE grp = 'b'").expect("2");
    Spi::run("DELETE FROM rm WHERE grp = 'c'").expect("3");
    Spi::run("INSERT INTO rm (grp, val) VALUES ('c', 100), ('d', 50)").expect("4");
    Spi::run("UPDATE rm SET grp = 'd' WHERE grp = 'a' AND val = 1").expect("5");
    Spi::run("DELETE FROM rm WHERE val = 10").expect("6");
    Spi::run("INSERT INTO rm (grp, val) VALUES ('a', 7), ('a', 8), ('a', 9)").expect("7");
    Spi::run("UPDATE rm SET val = 0 WHERE grp = 'd'").expect("8");
    Spi::run("DELETE FROM rm WHERE val = 0 AND grp = 'd'").expect("9");
    Spi::run("INSERT INTO rm (grp, val) VALUES ('e', 999)").expect("10");

    assert_imv_correct("rm_view", fresh);
}

/// EXCEPT preserves operand order — A EXCEPT B != B EXCEPT A
#[pg_test]
fn test_correctness_except_order() {
    Spi::run("CREATE TABLE eo_a (id SERIAL, val TEXT)").expect("create");
    Spi::run("CREATE TABLE eo_b (id SERIAL, val TEXT)").expect("create");
    Spi::run("INSERT INTO eo_a (val) VALUES ('x'), ('y'), ('z')").expect("seed a");
    Spi::run("INSERT INTO eo_b (val) VALUES ('y'), ('z'), ('w')").expect("seed b");

    // A EXCEPT B should give 'x' (in A but not B)
    crate::create_reflex_ivm("eo_ab",
        "SELECT val FROM eo_a EXCEPT SELECT val FROM eo_b",
        None, None, None);
    let fresh_ab = "SELECT val FROM eo_a EXCEPT SELECT val FROM eo_b";
    assert_imv_correct("eo_ab", fresh_ab);
    assert_eq!(
        Spi::get_one::<i64>("SELECT COUNT(*) FROM eo_ab WHERE val = 'x'").expect("q").expect("v"),
        1, "x should be in A EXCEPT B"
    );
    assert_eq!(
        Spi::get_one::<i64>("SELECT COUNT(*) FROM eo_ab WHERE val = 'w'").expect("q").expect("v"),
        0, "w should NOT be in A EXCEPT B"
    );

    // Mutate and re-check
    Spi::run("INSERT INTO eo_b (val) VALUES ('x')").expect("insert x into b");
    assert_imv_correct("eo_ab", fresh_ab);
    // Now A EXCEPT B should be empty (all of A is in B)
    assert_eq!(
        Spi::get_one::<i64>("SELECT COUNT(*) FROM eo_ab").expect("q").expect("v"),
        0, "A EXCEPT B should be empty after adding x to B"
    );
}

/// INTERSECT after DELETE makes intersection empty
#[pg_test]
fn test_correctness_intersect_empties() {
    Spi::run("CREATE TABLE ie_a (id SERIAL PRIMARY KEY, val TEXT)").expect("create");
    Spi::run("CREATE TABLE ie_b (id SERIAL PRIMARY KEY, val TEXT)").expect("create");
    Spi::run("INSERT INTO ie_a (val) VALUES ('x'), ('y')").expect("seed a");
    Spi::run("INSERT INTO ie_b (val) VALUES ('x'), ('y')").expect("seed b");

    crate::create_reflex_ivm("ie_view",
        "SELECT val FROM ie_a INTERSECT SELECT val FROM ie_b",
        None, None, None);

    let fresh = "SELECT val FROM ie_a INTERSECT SELECT val FROM ie_b";
    assert_imv_correct("ie_view", fresh);
    assert_eq!(
        Spi::get_one::<i64>("SELECT COUNT(*) FROM ie_view").expect("q").expect("v"),
        2
    );

    // Delete all from A — intersection becomes empty
    Spi::run("DELETE FROM ie_a").expect("delete all a");
    assert_imv_correct("ie_view", fresh);
    assert_eq!(
        Spi::get_one::<i64>("SELECT COUNT(*) FROM ie_view").expect("q").expect("v"),
        0
    );

    // Re-insert into A — intersection restores
    Spi::run("INSERT INTO ie_a (val) VALUES ('y')").expect("reinsert");
    assert_imv_correct("ie_view", fresh);
    assert_eq!(
        Spi::get_one::<i64>("SELECT COUNT(*) FROM ie_view").expect("q").expect("v"),
        1
    );
}

/// WINDOW with PARTITION BY — delete empties a partition
#[pg_test]
fn test_correctness_window_partition_empty() {
    Spi::run("CREATE TABLE wpe (id SERIAL PRIMARY KEY, dept TEXT, name TEXT, score INT)").expect("create");
    Spi::run("INSERT INTO wpe (dept, name, score) VALUES \
        ('eng', 'Alice', 90), ('eng', 'Bob', 80), \
        ('sales', 'Carol', 70)").expect("seed");

    crate::create_reflex_ivm("wpe_view",
        "SELECT dept, name, score, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY score DESC) AS rnk FROM wpe",
        None, None, None);

    let fresh = "SELECT dept, name, score, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY score DESC) AS rnk FROM wpe";
    assert_imv_correct("wpe_view", fresh);

    // Delete all from 'sales' partition
    Spi::run("DELETE FROM wpe WHERE dept = 'sales'").expect("empty partition");
    assert_imv_correct("wpe_view", fresh);

    // Only eng partition remains
    assert_eq!(
        Spi::get_one::<i64>("SELECT COUNT(*) FROM wpe_view").expect("q").expect("v"),
        2
    );
}

/// AVG: delete all rows from a group — AVG should not divide by zero
#[pg_test]
fn test_correctness_avg_group_vanishes() {
    Spi::run("CREATE TABLE avg_van (id SERIAL PRIMARY KEY, grp TEXT NOT NULL, val NUMERIC)").expect("create");
    Spi::run("INSERT INTO avg_van (grp, val) VALUES ('a', 10), ('a', 20), ('b', 30)").expect("seed");

    crate::create_reflex_ivm("avg_van_view",
        "SELECT grp, AVG(val) AS avg_val FROM avg_van GROUP BY grp",
        None, None, None);

    let fresh = "SELECT grp, AVG(val) AS avg_val FROM avg_van GROUP BY grp";
    assert_imv_correct("avg_van_view", fresh);

    // Delete all 'a' rows — group should vanish, no division by zero
    Spi::run("DELETE FROM avg_van WHERE grp = 'a'").expect("delete all a");
    assert_imv_correct("avg_van_view", fresh);
    assert_eq!(
        Spi::get_one::<i64>("SELECT COUNT(*) FROM avg_van_view WHERE grp = 'a'").expect("q").expect("v"),
        0
    );
}

/// Multiple operations on same rows within one statement (CTE DML)
#[pg_test]
fn test_correctness_cte_dml_multi_table() {
    Spi::run("CREATE TABLE cm_a (id SERIAL PRIMARY KEY, val INT)").expect("create a");
    Spi::run("CREATE TABLE cm_b (id SERIAL PRIMARY KEY, val INT)").expect("create b");
    Spi::run("INSERT INTO cm_a (val) VALUES (1), (2), (3)").expect("seed a");
    Spi::run("INSERT INTO cm_b (val) VALUES (10), (20)").expect("seed b");

    crate::create_reflex_ivm("cm_view",
        "SELECT SUM(val) AS total FROM cm_a",
        None, None, None);

    let fresh = "SELECT SUM(val) AS total FROM cm_a";
    assert_imv_correct("cm_view", fresh);

    // CTE that inserts using data from another table
    Spi::run("INSERT INTO cm_a (val) SELECT val FROM cm_b").expect("cte insert");
    assert_imv_correct("cm_view", fresh);
    // total should be 1+2+3+10+20 = 36
}

/// Passthrough without unique key — DELETE falls back to full refresh
#[pg_test]
fn test_correctness_passthrough_no_key() {
    Spi::run("CREATE TABLE pnk (id SERIAL, city TEXT, val INT)").expect("create");
    Spi::run("INSERT INTO pnk (city, val) VALUES ('a', 1), ('b', 2), ('c', 3)").expect("seed");

    // No unique key provided, no PK auto-detection (id not in SELECT)
    crate::create_reflex_ivm("pnk_view",
        "SELECT city, val FROM pnk",
        None, None, None);

    let fresh = "SELECT city, val FROM pnk";
    assert_imv_correct("pnk_view", fresh);

    Spi::run("INSERT INTO pnk (city, val) VALUES ('d', 4)").expect("insert");
    assert_imv_correct("pnk_view", fresh);

    // DELETE triggers full refresh (no key for targeted delete)
    Spi::run("DELETE FROM pnk WHERE city = 'b'").expect("delete");
    assert_imv_correct("pnk_view", fresh);

    Spi::run("UPDATE pnk SET val = 99 WHERE city = 'a'").expect("update");
    assert_imv_correct("pnk_view", fresh);
}

/// UNION with aggregates in operands — correctness through mutations
#[pg_test]
fn test_correctness_union_agg_mutations() {
    Spi::run("CREATE TABLE uam_a (id SERIAL, grp TEXT, val INT)").expect("create a");
    Spi::run("CREATE TABLE uam_b (id SERIAL, grp TEXT, val INT)").expect("create b");
    Spi::run("INSERT INTO uam_a (grp, val) VALUES ('x', 10), ('x', 20), ('y', 30)").expect("seed a");
    Spi::run("INSERT INTO uam_b (grp, val) VALUES ('x', 100), ('z', 50)").expect("seed b");

    crate::create_reflex_ivm("uam_view",
        "SELECT grp, SUM(val) AS total FROM uam_a GROUP BY grp \
         UNION ALL \
         SELECT grp, SUM(val) AS total FROM uam_b GROUP BY grp",
        None, None, None);

    let fresh = "SELECT grp, SUM(val) AS total FROM uam_a GROUP BY grp \
                 UNION ALL \
                 SELECT grp, SUM(val) AS total FROM uam_b GROUP BY grp";
    assert_imv_correct("uam_view", fresh);

    Spi::run("INSERT INTO uam_a (grp, val) VALUES ('z', 5)").expect("insert a");
    assert_imv_correct("uam_view", fresh);

    Spi::run("DELETE FROM uam_b WHERE grp = 'x'").expect("delete b");
    assert_imv_correct("uam_view", fresh);

    Spi::run("UPDATE uam_a SET val = 999 WHERE grp = 'y'").expect("update a");
    assert_imv_correct("uam_view", fresh);
}

/// Stress: interleaved INSERT/DELETE/UPDATE on 50 groups
#[pg_test]
fn test_correctness_stress_interleaved() {
    Spi::run("CREATE TABLE stress (id SERIAL PRIMARY KEY, grp INT NOT NULL, val INT NOT NULL)").expect("create");
    Spi::run("INSERT INTO stress (grp, val) SELECT i % 50, i FROM generate_series(1, 5000) i").expect("seed");

    crate::create_reflex_ivm("stress_view",
        "SELECT grp, SUM(val) AS total, COUNT(*) AS cnt FROM stress GROUP BY grp",
        None, None, None);

    let fresh = "SELECT grp, SUM(val) AS total, COUNT(*) AS cnt FROM stress GROUP BY grp";
    assert_imv_correct("stress_view", fresh);

    // Batch INSERT
    Spi::run("INSERT INTO stress (grp, val) SELECT i % 50, i * 10 FROM generate_series(1, 1000) i").expect("batch insert");
    assert_imv_correct("stress_view", fresh);

    // Batch UPDATE — change group keys for some rows
    Spi::run("UPDATE stress SET grp = grp + 25 WHERE id <= 500").expect("batch update grp");
    assert_imv_correct("stress_view", fresh);

    // Batch DELETE
    Spi::run("DELETE FROM stress WHERE id > 5000").expect("batch delete");
    assert_imv_correct("stress_view", fresh);

    // Large UPDATE on values
    Spi::run("UPDATE stress SET val = val + 1").expect("update all");
    assert_imv_correct("stress_view", fresh);
}

/// LEFT JOIN: right side NULLs appear/disappear — auto full-refresh on right-side DELETE
#[pg_test]
fn test_correctness_left_join_nulls() {
    Spi::run("CREATE TABLE lj_l (id SERIAL PRIMARY KEY, grp TEXT NOT NULL)").expect("create");
    Spi::run("CREATE TABLE lj_r (id SERIAL PRIMARY KEY, grp TEXT, val INT)").expect("create");
    Spi::run("INSERT INTO lj_l (grp) VALUES ('a'), ('b'), ('c')").expect("seed l");
    Spi::run("INSERT INTO lj_r (grp, val) VALUES ('a', 10), ('b', 20)").expect("seed r");

    crate::create_reflex_ivm("lj_view",
        "SELECT l.grp, SUM(r.val) AS total FROM lj_l l LEFT JOIN lj_r r ON l.grp = r.grp GROUP BY l.grp",
        None, None, None);

    let fresh = "SELECT l.grp, SUM(r.val) AS total FROM lj_l l LEFT JOIN lj_r r ON l.grp = r.grp GROUP BY l.grp";
    assert_imv_correct("lj_view", fresh);

    // Insert into right -> 'c' goes from NULL to having a value
    Spi::run("INSERT INTO lj_r (grp, val) VALUES ('c', 50)").expect("fill null");
    assert_imv_correct("lj_view", fresh);

    // Delete from right -> auto full-refresh detects LEFT JOIN secondary table
    Spi::run("DELETE FROM lj_r WHERE grp = 'c'").expect("back to null");
    assert_imv_correct("lj_view", fresh);

    // Delete all from right -> all LEFT JOIN results become NULL
    Spi::run("DELETE FROM lj_r").expect("delete all right");
    assert_imv_correct("lj_view", fresh);
}

/// Cast propagation: SUM(x)::BIGINT correctness
#[pg_test]
fn test_correctness_cast_propagation() {
    Spi::run("CREATE TABLE ccast (id SERIAL, grp TEXT, val INT)").expect("create");
    Spi::run("INSERT INTO ccast (grp, val) VALUES ('a', 100), ('a', 200), ('b', 50)").expect("seed");

    crate::create_reflex_ivm("ccast_view",
        "SELECT grp, SUM(val)::BIGINT AS total FROM ccast GROUP BY grp",
        None, None, None);

    let fresh = "SELECT grp, SUM(val)::BIGINT AS total FROM ccast GROUP BY grp";
    assert_imv_correct("ccast_view", fresh);

    Spi::run("INSERT INTO ccast (grp, val) VALUES ('a', 50)").expect("insert");
    assert_imv_correct("ccast_view", fresh);

    Spi::run("DELETE FROM ccast WHERE val = 200").expect("delete");
    assert_imv_correct("ccast_view", fresh);

    Spi::run("UPDATE ccast SET val = 999 WHERE grp = 'b'").expect("update");
    assert_imv_correct("ccast_view", fresh);
}

/// Multiple IMVs on same source — all correct after mutations
#[pg_test]
fn test_correctness_multi_imv_same_source() {
    Spi::run("CREATE TABLE msrc (id SERIAL PRIMARY KEY, grp TEXT NOT NULL, val INT NOT NULL)").expect("create");
    Spi::run("INSERT INTO msrc (grp, val) VALUES ('a', 10), ('a', 20), ('b', 30), ('b', 40)").expect("seed");

    crate::create_reflex_ivm("m1_view",
        "SELECT grp, SUM(val) AS total FROM msrc GROUP BY grp",
        None, None, None);
    crate::create_reflex_ivm("m2_view",
        "SELECT grp, COUNT(*) AS cnt FROM msrc GROUP BY grp",
        None, None, None);
    crate::create_reflex_ivm("m3_view",
        "SELECT grp, AVG(val) AS avg_val FROM msrc GROUP BY grp",
        None, None, None);

    let f1 = "SELECT grp, SUM(val) AS total FROM msrc GROUP BY grp";
    let f2 = "SELECT grp, COUNT(*) AS cnt FROM msrc GROUP BY grp";
    let f3 = "SELECT grp, AVG(val) AS avg_val FROM msrc GROUP BY grp";

    assert_imv_correct("m1_view", f1);
    assert_imv_correct("m2_view", f2);
    assert_imv_correct("m3_view", f3);

    // INSERT — all 3 must update correctly
    Spi::run("INSERT INTO msrc (grp, val) VALUES ('a', 100), ('c', 5)").expect("insert");
    assert_imv_correct("m1_view", f1);
    assert_imv_correct("m2_view", f2);
    assert_imv_correct("m3_view", f3);

    // UPDATE — group key change
    Spi::run("UPDATE msrc SET grp = 'c' WHERE val = 40").expect("update");
    assert_imv_correct("m1_view", f1);
    assert_imv_correct("m2_view", f2);
    assert_imv_correct("m3_view", f3);

    // DELETE
    Spi::run("DELETE FROM msrc WHERE grp = 'a' AND val = 10").expect("delete");
    assert_imv_correct("m1_view", f1);
    assert_imv_correct("m2_view", f2);
    assert_imv_correct("m3_view", f3);
}

/// Wide intermediate: 6 aggregates on same table
#[pg_test]
fn test_correctness_wide_intermediate() {
    Spi::run("CREATE TABLE wide (id SERIAL, grp TEXT NOT NULL, a INT NOT NULL, b INT NOT NULL)").expect("create");
    Spi::run("INSERT INTO wide (grp, a, b) VALUES ('x', 10, 100), ('x', 20, 200), ('y', 30, 300)").expect("seed");

    crate::create_reflex_ivm("wide_view",
        "SELECT grp, SUM(a) AS sa, SUM(b) AS sb, COUNT(*) AS cnt, \
                MIN(a) AS mina, MAX(b) AS maxb, AVG(a) AS avga \
         FROM wide GROUP BY grp",
        None, None, None);

    let fresh = "SELECT grp, SUM(a) AS sa, SUM(b) AS sb, COUNT(*) AS cnt, \
                 MIN(a) AS mina, MAX(b) AS maxb, AVG(a) AS avga \
                 FROM wide GROUP BY grp";
    assert_imv_correct("wide_view", fresh);

    Spi::run("INSERT INTO wide (grp, a, b) VALUES ('x', 1, 999), ('y', 50, 1)").expect("insert");
    assert_imv_correct("wide_view", fresh);

    Spi::run("DELETE FROM wide WHERE a = 1").expect("delete min");
    assert_imv_correct("wide_view", fresh);

    Spi::run("UPDATE wide SET a = a + 1, b = b - 1").expect("update both");
    assert_imv_correct("wide_view", fresh);
}

/// Delete ALL rows then re-insert — full lifecycle
#[pg_test]
fn test_correctness_delete_all_reinsert() {
    Spi::run("CREATE TABLE dar (id SERIAL PRIMARY KEY, grp TEXT NOT NULL, val INT NOT NULL)").expect("create");
    Spi::run("INSERT INTO dar (grp, val) VALUES ('a', 10), ('b', 20)").expect("seed");

    crate::create_reflex_ivm("dar_view",
        "SELECT grp, SUM(val) AS total, COUNT(*) AS cnt FROM dar GROUP BY grp",
        None, None, None);

    let fresh = "SELECT grp, SUM(val) AS total, COUNT(*) AS cnt FROM dar GROUP BY grp";
    assert_imv_correct("dar_view", fresh);

    // Delete everything
    Spi::run("DELETE FROM dar").expect("delete all");
    assert_imv_correct("dar_view", fresh);
    assert_eq!(Spi::get_one::<i64>("SELECT COUNT(*) FROM dar_view").expect("q").expect("v"), 0);

    // Re-insert completely different data
    Spi::run("INSERT INTO dar (grp, val) VALUES ('x', 100), ('x', 200), ('y', 50)").expect("reinsert");
    assert_imv_correct("dar_view", fresh);
}

/// HAVING: group bounces above and below threshold multiple times
#[pg_test]
fn test_correctness_having_bounce() {
    Spi::run("CREATE TABLE hb (id SERIAL PRIMARY KEY, grp TEXT NOT NULL, val INT NOT NULL)").expect("create");
    Spi::run("INSERT INTO hb (grp, val) VALUES ('a', 8), ('a', 3), ('b', 20)").expect("seed");

    crate::create_reflex_ivm("hb_view",
        "SELECT grp, SUM(val) AS total FROM hb GROUP BY grp HAVING SUM(val) >= 10",
        None, None, None);

    let fresh = "SELECT grp, SUM(val) AS total FROM hb GROUP BY grp HAVING SUM(val) >= 10";
    // Initial: a=11 (>=10, visible), b=20 (visible)
    assert_imv_correct("hb_view", fresh);

    // Push 'a' below threshold
    Spi::run("DELETE FROM hb WHERE grp = 'a' AND val = 3").expect("drop below");
    assert_imv_correct("hb_view", fresh);
    // a=8 (<10, hidden)

    // Push 'a' back above
    Spi::run("INSERT INTO hb (grp, val) VALUES ('a', 5)").expect("back above");
    assert_imv_correct("hb_view", fresh);
    // a=13 (>=10, visible again)

    // Push below again
    Spi::run("DELETE FROM hb WHERE grp = 'a' AND val = 8").expect("below again");
    assert_imv_correct("hb_view", fresh);
    // a=5 (<10, hidden)
}

// ========================================================================
// Group 5 — More correctness tests
// ========================================================================

/// UNION with 3+ operands — mutations on each
#[pg_test]
fn test_correctness_union_three_operands() {
    Spi::run("CREATE TABLE u3a (id SERIAL, val TEXT)").expect("create a");
    Spi::run("CREATE TABLE u3b (id SERIAL, val TEXT)").expect("create b");
    Spi::run("CREATE TABLE u3c (id SERIAL, val TEXT)").expect("create c");
    Spi::run("INSERT INTO u3a (val) VALUES ('x'), ('y')").expect("seed a");
    Spi::run("INSERT INTO u3b (val) VALUES ('y'), ('z')").expect("seed b");
    Spi::run("INSERT INTO u3c (val) VALUES ('z'), ('w')").expect("seed c");

    crate::create_reflex_ivm("u3_view",
        "SELECT val FROM u3a UNION ALL SELECT val FROM u3b UNION ALL SELECT val FROM u3c",
        None, None, None);

    let fresh = "SELECT val FROM u3a UNION ALL SELECT val FROM u3b UNION ALL SELECT val FROM u3c";
    assert_imv_correct("u3_view", fresh);

    Spi::run("INSERT INTO u3a (val) VALUES ('new_a')").expect("insert a");
    assert_imv_correct("u3_view", fresh);

    Spi::run("DELETE FROM u3b WHERE val = 'y'").expect("delete b");
    assert_imv_correct("u3_view", fresh);

    Spi::run("INSERT INTO u3c (val) VALUES ('new_c1'), ('new_c2')").expect("insert c");
    assert_imv_correct("u3_view", fresh);
}

/// WINDOW: multiple partitions with INSERT/DELETE across partitions
#[pg_test]
fn test_correctness_window_multi_partition_mutations() {
    Spi::run("CREATE TABLE wmp (id SERIAL, dept TEXT, name TEXT, score INT)").expect("create");
    Spi::run("INSERT INTO wmp (dept, name, score) VALUES \
        ('eng', 'Alice', 90), ('eng', 'Bob', 80), ('eng', 'Charlie', 70), \
        ('sales', 'Dave', 95), ('sales', 'Eve', 85), \
        ('ops', 'Frank', 60)").expect("seed");

    crate::create_reflex_ivm("wmp_view",
        "SELECT dept, name, score, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY score DESC) AS rnk FROM wmp",
        None, None, None);

    let fresh = "SELECT dept, name, score, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY score DESC) AS rnk FROM wmp";
    assert_imv_correct("wmp_view", fresh);

    // Insert into eng — only eng partition changes
    Spi::run("INSERT INTO wmp (dept, name, score) VALUES ('eng', 'Zara', 100)").expect("insert eng");
    assert_imv_correct("wmp_view", fresh);

    // Delete from sales
    Spi::run("DELETE FROM wmp WHERE name = 'Dave'").expect("delete sales");
    assert_imv_correct("wmp_view", fresh);

    // Insert new department
    Spi::run("INSERT INTO wmp (dept, name, score) VALUES ('hr', 'Grace', 75), ('hr', 'Hank', 80)").expect("new dept");
    assert_imv_correct("wmp_view", fresh);

    // Delete entire department
    Spi::run("DELETE FROM wmp WHERE dept = 'ops'").expect("delete dept");
    assert_imv_correct("wmp_view", fresh);
}

/// GROUP BY + WINDOW: aggregate changes trigger re-ranking
#[pg_test]
fn test_correctness_groupby_window_rerank() {
    Spi::run("CREATE TABLE gwr (id SERIAL, city TEXT, amount INT)").expect("create");
    Spi::run("INSERT INTO gwr (city, amount) VALUES \
        ('a', 100), ('a', 100), ('b', 150), ('c', 50), ('c', 50), ('c', 50)").expect("seed");

    crate::create_reflex_ivm("gwr_view",
        "SELECT city, SUM(amount) AS total, \
                DENSE_RANK() OVER (ORDER BY SUM(amount) DESC) AS rnk \
         FROM gwr GROUP BY city",
        None, None, None);

    let fresh = "SELECT city, SUM(amount) AS total, \
                 DENSE_RANK() OVER (ORDER BY SUM(amount) DESC) AS rnk \
                 FROM gwr GROUP BY city";
    assert_imv_correct("gwr_view", fresh);
    // a=200(1), b=150(2), c=150(2) — tied

    // Push 'c' to top
    Spi::run("INSERT INTO gwr (city, amount) VALUES ('c', 200)").expect("insert");
    assert_imv_correct("gwr_view", fresh);
    // c=350(1), a=200(2), b=150(3)

    // Remove 'b' entirely
    Spi::run("DELETE FROM gwr WHERE city = 'b'").expect("delete b");
    assert_imv_correct("gwr_view", fresh);
}

/// Empty INSERT (INSERT ... SELECT ... WHERE FALSE) — 0 rows
#[pg_test]
fn test_correctness_empty_insert() {
    Spi::run("CREATE TABLE ei (id SERIAL, grp TEXT, val INT)").expect("create");
    Spi::run("INSERT INTO ei (grp, val) VALUES ('a', 10)").expect("seed");

    crate::create_reflex_ivm("ei_view",
        "SELECT grp, SUM(val) AS total FROM ei GROUP BY grp",
        None, None, None);

    let fresh = "SELECT grp, SUM(val) AS total FROM ei GROUP BY grp";
    assert_imv_correct("ei_view", fresh);

    // Empty insert — 0 rows
    Spi::run("INSERT INTO ei (grp, val) SELECT 'x', 1 WHERE FALSE").expect("empty insert");
    assert_imv_correct("ei_view", fresh);
}

/// Passthrough: UPDATE on both source and dimension table
#[pg_test]
fn test_correctness_passthrough_update_both_tables() {
    Spi::run("CREATE TABLE pub_src (id SERIAL PRIMARY KEY, did INT NOT NULL, val TEXT)").expect("create src");
    Spi::run("CREATE TABLE pub_dim (id INT PRIMARY KEY, label TEXT)").expect("create dim");
    Spi::run("INSERT INTO pub_dim VALUES (1, 'A'), (2, 'B')").expect("seed dim");
    Spi::run("INSERT INTO pub_src (did, val) VALUES (1, 'x'), (2, 'y'), (1, 'z')").expect("seed src");

    crate::create_reflex_ivm("pub_view",
        "SELECT s.id, s.val, d.label FROM pub_src s JOIN pub_dim d ON s.did = d.id",
        Some("id"), None, None);

    let fresh = "SELECT s.id, s.val, d.label FROM pub_src s JOIN pub_dim d ON s.did = d.id";
    assert_imv_correct("pub_view", fresh);

    // Update source
    Spi::run("UPDATE pub_src SET val = 'updated' WHERE id = 1").expect("update src");
    assert_imv_correct("pub_view", fresh);

    // Update dimension label — all joined rows should reflect new label
    Spi::run("UPDATE pub_dim SET label = 'AAA' WHERE id = 1").expect("update dim");
    assert_imv_correct("pub_view", fresh);
}

/// DISTINCT + GROUP BY combined
#[pg_test]
fn test_correctness_distinct_with_group_by() {
    Spi::run("CREATE TABLE dg (id SERIAL, grp TEXT, val INT)").expect("create");
    Spi::run("INSERT INTO dg (grp, val) VALUES ('a', 10), ('a', 10), ('a', 20), ('b', 30), ('b', 30)").expect("seed");

    crate::create_reflex_ivm("dg_view",
        "SELECT DISTINCT grp, SUM(val) AS total FROM dg GROUP BY grp",
        None, None, None);

    let fresh = "SELECT DISTINCT grp, SUM(val) AS total FROM dg GROUP BY grp";
    assert_imv_correct("dg_view", fresh);

    Spi::run("INSERT INTO dg (grp, val) VALUES ('a', 10)").expect("insert");
    assert_imv_correct("dg_view", fresh);

    Spi::run("DELETE FROM dg WHERE grp = 'b' AND val = 30 AND id = (SELECT MIN(id) FROM dg WHERE grp = 'b')").expect("delete one");
    assert_imv_correct("dg_view", fresh);
}

/// Full-table aggregate (no GROUP BY) with multiple aggs and mutations
#[pg_test]
fn test_correctness_full_table_agg_lifecycle() {
    Spi::run("CREATE TABLE fta (id SERIAL PRIMARY KEY, val INT)").expect("create");

    crate::create_reflex_ivm("fta_view",
        "SELECT SUM(val) AS s, COUNT(*) AS c, COUNT(val) AS cv FROM fta",
        None, None, None);

    let fresh = "SELECT SUM(val) AS s, COUNT(*) AS c, COUNT(val) AS cv FROM fta";

    // Empty table
    assert_imv_correct("fta_view", fresh);

    // First insert
    Spi::run("INSERT INTO fta (val) VALUES (10)").expect("first insert");
    assert_imv_correct("fta_view", fresh);

    // More inserts including NULL
    Spi::run("INSERT INTO fta (val) VALUES (20), (NULL), (30)").expect("more inserts");
    assert_imv_correct("fta_view", fresh);

    // Delete non-NULL
    Spi::run("DELETE FROM fta WHERE val = 10").expect("delete");
    assert_imv_correct("fta_view", fresh);

    // Delete all
    Spi::run("DELETE FROM fta").expect("delete all");
    assert_imv_correct("fta_view", fresh);

    // Re-insert
    Spi::run("INSERT INTO fta (val) VALUES (99)").expect("reinsert");
    assert_imv_correct("fta_view", fresh);
}

/// CTE with passthrough body reading from aggregate CTE
#[pg_test]
fn test_correctness_cte_passthrough_body() {
    Spi::run("CREATE TABLE cpb (id SERIAL, region TEXT, amount INT)").expect("create");
    Spi::run("INSERT INTO cpb (region, amount) VALUES ('US', 100), ('US', 200), ('EU', 50)").expect("seed");

    crate::create_reflex_ivm("cpb_view",
        "WITH totals AS (SELECT region, SUM(amount) AS total FROM cpb GROUP BY region) \
         SELECT region, total FROM totals WHERE total > 100",
        None, None, None);

    let fresh = "WITH totals AS (SELECT region, SUM(amount) AS total FROM cpb GROUP BY region) \
                 SELECT region, total FROM totals WHERE total > 100";
    assert_imv_correct("cpb_view", fresh);

    // Push EU above threshold
    Spi::run("INSERT INTO cpb (region, amount) VALUES ('EU', 200)").expect("push above");
    assert_imv_correct("cpb_view", fresh);

    // Push US below
    Spi::run("DELETE FROM cpb WHERE region = 'US' AND amount = 200").expect("push below");
    assert_imv_correct("cpb_view", fresh);
}

/// Negative values in aggregates
#[pg_test]
fn test_correctness_negative_values() {
    Spi::run("CREATE TABLE neg (id SERIAL, grp TEXT, val INT)").expect("create");
    Spi::run("INSERT INTO neg (grp, val) VALUES ('a', -10), ('a', 20), ('a', -5), ('b', -100)").expect("seed");

    crate::create_reflex_ivm("neg_view",
        "SELECT grp, SUM(val) AS total, MIN(val) AS lo, MAX(val) AS hi FROM neg GROUP BY grp",
        None, None, None);

    let fresh = "SELECT grp, SUM(val) AS total, MIN(val) AS lo, MAX(val) AS hi FROM neg GROUP BY grp";
    assert_imv_correct("neg_view", fresh);

    Spi::run("INSERT INTO neg (grp, val) VALUES ('a', -50)").expect("insert negative");
    assert_imv_correct("neg_view", fresh);

    // Delete the MIN (most negative)
    Spi::run("DELETE FROM neg WHERE val = -50").expect("delete min");
    assert_imv_correct("neg_view", fresh);

    // Update to zero
    Spi::run("UPDATE neg SET val = 0 WHERE val = -10").expect("to zero");
    assert_imv_correct("neg_view", fresh);
}

/// Decimal/numeric precision across INSERT/DELETE cycles
#[pg_test]
fn test_correctness_decimal_precision() {
    Spi::run("CREATE TABLE dp (id SERIAL, grp TEXT, val NUMERIC(12,4))").expect("create");
    Spi::run("INSERT INTO dp (grp, val) VALUES ('a', 0.0001), ('a', 0.0002), ('a', 0.0003)").expect("seed");

    crate::create_reflex_ivm("dp_view",
        "SELECT grp, SUM(val) AS total, AVG(val) AS avg_val FROM dp GROUP BY grp",
        None, None, None);

    let fresh = "SELECT grp, SUM(val) AS total, AVG(val) AS avg_val FROM dp GROUP BY grp";
    assert_imv_correct("dp_view", fresh);

    // Many small inserts
    for i in 1..=20 {
        Spi::run(&format!("INSERT INTO dp (grp, val) VALUES ('a', 0.{:04})", i)).expect("small insert");
    }
    assert_imv_correct("dp_view", fresh);

    // Delete half
    Spi::run("DELETE FROM dp WHERE id <= 10").expect("delete half");
    assert_imv_correct("dp_view", fresh);
}

/// INTERSECT with aggregates in operands
#[pg_test]
fn test_correctness_intersect_with_agg() {
    Spi::run("CREATE TABLE ia_a (id SERIAL, grp TEXT, val INT)").expect("create a");
    Spi::run("CREATE TABLE ia_b (id SERIAL, grp TEXT, val INT)").expect("create b");
    Spi::run("INSERT INTO ia_a (grp, val) VALUES ('x', 10), ('x', 20), ('y', 30)").expect("seed a");
    Spi::run("INSERT INTO ia_b (grp, val) VALUES ('x', 30), ('z', 50)").expect("seed b");

    crate::create_reflex_ivm("ia_view",
        "SELECT grp, SUM(val) AS total FROM ia_a GROUP BY grp \
         INTERSECT \
         SELECT grp, SUM(val) AS total FROM ia_b GROUP BY grp",
        None, None, None);

    let fresh = "SELECT grp, SUM(val) AS total FROM ia_a GROUP BY grp \
                 INTERSECT \
                 SELECT grp, SUM(val) AS total FROM ia_b GROUP BY grp";
    assert_imv_correct("ia_view", fresh);

    // Make 'x' totals match: a.x=30, b.x=30
    // Currently a.x=30, b.x=30 — already matching -> should appear in INTERSECT
    assert_eq!(
        Spi::get_one::<i64>("SELECT COUNT(*) FROM ia_view WHERE grp = 'x'").expect("q").expect("v"),
        1
    );

    // Change a.x total so it no longer matches b.x
    Spi::run("INSERT INTO ia_a (grp, val) VALUES ('x', 1)").expect("break match");
    assert_imv_correct("ia_view", fresh);
}

/// Stress: 100 sequential mutations covering INSERT/UPDATE/DELETE
#[pg_test]
fn test_correctness_stress_100_mutations() {
    Spi::run("CREATE TABLE s100 (id SERIAL PRIMARY KEY, grp INT NOT NULL, val INT NOT NULL)").expect("create");
    Spi::run("INSERT INTO s100 (grp, val) SELECT i % 20, i FROM generate_series(1, 1000) i").expect("seed");

    crate::create_reflex_ivm("s100_view",
        "SELECT grp, SUM(val) AS total, COUNT(*) AS cnt FROM s100 GROUP BY grp",
        None, None, None);

    let fresh = "SELECT grp, SUM(val) AS total, COUNT(*) AS cnt FROM s100 GROUP BY grp";

    for i in 1..=30 {
        match i % 3 {
            0 => Spi::run(&format!(
                "INSERT INTO s100 (grp, val) SELECT ({} + j) % 20, {} * j FROM generate_series(1, 50) j", i, i
            )).expect("insert"),
            1 => Spi::run(&format!(
                "UPDATE s100 SET val = val + 1 WHERE grp = {}", i % 20
            )).expect("update"),
            _ => Spi::run(&format!(
                "DELETE FROM s100 WHERE grp = {} AND id <= (SELECT MIN(id) + 5 FROM s100 WHERE grp = {})", i % 20, i % 20
            )).expect("delete"),
        };
    }

    // Final correctness check after 30 mutations
    assert_imv_correct("s100_view", fresh);
}

// ========================================================================
// Group 6 — Fuzz tests
// ========================================================================

/// Fuzz: random GROUP BY + SUM/COUNT with random INSERT/UPDATE/DELETE
#[pg_test]
fn test_fuzz_groupby_sum_count() {
    Spi::run("SELECT setseed(0.42)").expect("seed");

    for round in 0..10 {
        let tbl = format!("fuzz_sc_{}", round);
        let view = format!("fuzz_sc_v_{}", round);

        // Random table with 3-50 groups, 100-500 rows
        Spi::run(&format!(
            "CREATE TABLE {} (id SERIAL PRIMARY KEY, grp INT NOT NULL, val INT NOT NULL)", tbl
        )).expect("create");
        Spi::run(&format!(
            "INSERT INTO {} (grp, val) SELECT (random() * 30)::int, (random() * 1000 - 500)::int \
             FROM generate_series(1, 100 + (random() * 400)::int)", tbl
        )).expect("seed");

        let query = format!(
            "SELECT grp, SUM(val) AS total, COUNT(*) AS cnt FROM {} GROUP BY grp", tbl
        );
        crate::create_reflex_ivm(&view, &query, None, None, None);
        assert_imv_correct(&view, &query);

        // 5-15 random mutations
        let num_mutations = 5 + (round % 11);
        for m in 0..num_mutations {
            match m % 3 {
                0 => {
                    // Random INSERT (1-50 rows)
                    Spi::run(&format!(
                        "INSERT INTO {} (grp, val) SELECT (random() * 30)::int, (random() * 1000 - 500)::int \
                         FROM generate_series(1, 1 + (random() * 49)::int)", tbl
                    )).expect("insert");
                }
                1 => {
                    // Random UPDATE (change values)
                    Spi::run(&format!(
                        "UPDATE {} SET val = (random() * 2000 - 1000)::int \
                         WHERE id <= (SELECT MIN(id) + (random() * 20)::int FROM {})", tbl, tbl
                    )).expect("update");
                }
                _ => {
                    // Random DELETE (1-20 rows)
                    Spi::run(&format!(
                        "DELETE FROM {} WHERE id IN (\
                            SELECT id FROM {} ORDER BY random() LIMIT (1 + (random() * 19)::int)\
                        )", tbl, tbl
                    )).expect("delete");
                }
            }
            assert_imv_correct(&view, &query);
        }

        // Cleanup
        Spi::run(&format!("SELECT drop_reflex_ivm('{}', true)", view)).expect("drop");
        Spi::run(&format!("DROP TABLE IF EXISTS {} CASCADE", tbl)).expect("drop table");
    }
}

/// Fuzz: random GROUP BY + AVG with NULLs
#[pg_test]
fn test_fuzz_groupby_avg_with_nulls() {
    Spi::run("SELECT setseed(0.7)").expect("seed");

    for round in 0..8 {
        let tbl = format!("fuzz_avg_{}", round);
        let view = format!("fuzz_avg_v_{}", round);

        Spi::run(&format!(
            "CREATE TABLE {} (id SERIAL PRIMARY KEY, grp TEXT NOT NULL, val NUMERIC)", tbl
        )).expect("create");
        // Insert with ~20% NULLs
        Spi::run(&format!(
            "INSERT INTO {} (grp, val) SELECT \
                'g' || (random() * 10)::int, \
                CASE WHEN random() < 0.2 THEN NULL ELSE (random() * 1000)::numeric(10,2) END \
             FROM generate_series(1, 200)", tbl
        )).expect("seed");

        let query = format!(
            "SELECT grp, AVG(val) AS avg_val, COUNT(val) AS cv, COUNT(*) AS cs FROM {} GROUP BY grp", tbl
        );
        crate::create_reflex_ivm(&view, &query, None, None, None);
        assert_imv_correct(&view, &query);

        for m in 0..10 {
            match m % 4 {
                0 => Spi::run(&format!(
                    "INSERT INTO {} (grp, val) VALUES ('g' || (random()*10)::int, \
                     CASE WHEN random() < 0.3 THEN NULL ELSE (random()*500)::numeric(10,2) END)", tbl
                )).expect("insert"),
                1 => Spi::run(&format!(
                    "UPDATE {} SET val = NULL WHERE id = (SELECT id FROM {} ORDER BY random() LIMIT 1)", tbl, tbl
                )).expect("update to null"),
                2 => Spi::run(&format!(
                    "UPDATE {} SET val = (random()*999)::numeric(10,2) WHERE val IS NULL AND id = \
                     (SELECT id FROM {} WHERE val IS NULL ORDER BY random() LIMIT 1)", tbl, tbl
                )).expect("update from null"),
                _ => Spi::run(&format!(
                    "DELETE FROM {} WHERE id = (SELECT id FROM {} ORDER BY random() LIMIT 1)", tbl, tbl
                )).expect("delete"),
            };
            assert_imv_correct(&view, &query);
        }

        Spi::run(&format!("SELECT drop_reflex_ivm('{}', true)", view)).expect("drop");
        Spi::run(&format!("DROP TABLE IF EXISTS {} CASCADE", tbl)).expect("drop table");
    }
}

/// Fuzz: random MIN/MAX with random extremum deletions
#[pg_test]
fn test_fuzz_min_max_extremum() {
    Spi::run("SELECT setseed(0.13)").expect("seed");

    for round in 0..8 {
        let tbl = format!("fuzz_mm_{}", round);
        let view = format!("fuzz_mm_v_{}", round);

        Spi::run(&format!(
            "CREATE TABLE {} (id SERIAL PRIMARY KEY, grp INT NOT NULL, val INT NOT NULL)", tbl
        )).expect("create");
        Spi::run(&format!(
            "INSERT INTO {} (grp, val) SELECT (random()*5)::int, (random()*1000)::int \
             FROM generate_series(1, 150)", tbl
        )).expect("seed");

        let query = format!(
            "SELECT grp, MIN(val) AS lo, MAX(val) AS hi, COUNT(*) AS cnt FROM {} GROUP BY grp", tbl
        );
        crate::create_reflex_ivm(&view, &query, None, None, None);
        assert_imv_correct(&view, &query);

        for _ in 0..12 {
            // Randomly delete the current MIN or MAX of a random group
            let action = Spi::get_one::<i32>(
                "SELECT (random() * 3)::int"
            ).expect("q").expect("v");

            match action {
                0 => {
                    // Delete the MIN row of a random group
                    Spi::run(&format!(
                        "DELETE FROM {} WHERE id = (\
                            SELECT id FROM {} WHERE val = (\
                                SELECT MIN(val) FROM {} WHERE grp = (\
                                    SELECT grp FROM {} ORDER BY random() LIMIT 1\
                                )\
                            ) LIMIT 1\
                        )", tbl, tbl, tbl, tbl
                    )).expect("delete min");
                }
                1 => {
                    // Delete the MAX row
                    Spi::run(&format!(
                        "DELETE FROM {} WHERE id = (\
                            SELECT id FROM {} WHERE val = (\
                                SELECT MAX(val) FROM {} WHERE grp = (\
                                    SELECT grp FROM {} ORDER BY random() LIMIT 1\
                                )\
                            ) LIMIT 1\
                        )", tbl, tbl, tbl, tbl
                    )).expect("delete max");
                }
                2 => {
                    // Insert new potential extremum
                    Spi::run(&format!(
                        "INSERT INTO {} (grp, val) VALUES ((random()*5)::int, (random()*2000 - 500)::int)", tbl
                    )).expect("insert extremum");
                }
                _ => {
                    // Random update
                    Spi::run(&format!(
                        "UPDATE {} SET val = (random()*1500)::int WHERE id = (\
                            SELECT id FROM {} ORDER BY random() LIMIT 1\
                        )", tbl, tbl
                    )).expect("update");
                }
            }
            assert_imv_correct(&view, &query);
        }

        Spi::run(&format!("SELECT drop_reflex_ivm('{}', true)", view)).expect("drop");
        Spi::run(&format!("DROP TABLE IF EXISTS {} CASCADE", tbl)).expect("drop table");
    }
}

/// Fuzz: random DISTINCT with random INSERT/DELETE
#[pg_test]
fn test_fuzz_distinct() {
    Spi::run("SELECT setseed(0.99)").expect("seed");

    for round in 0..8 {
        let tbl = format!("fuzz_dist_{}", round);
        let view = format!("fuzz_dist_v_{}", round);

        Spi::run(&format!(
            "CREATE TABLE {} (id SERIAL PRIMARY KEY, val TEXT NOT NULL)", tbl
        )).expect("create");
        Spi::run(&format!(
            "INSERT INTO {} (val) SELECT 'v' || (random()*20)::int FROM generate_series(1, 200)", tbl
        )).expect("seed");

        let query = format!("SELECT DISTINCT val FROM {}", tbl);
        crate::create_reflex_ivm(&view, &query, None, None, None);
        assert_imv_correct(&view, &query);

        for _ in 0..15 {
            match Spi::get_one::<i32>("SELECT (random()*2)::int").expect("q").expect("v") {
                0 => Spi::run(&format!(
                    "INSERT INTO {} (val) SELECT 'v' || (random()*25)::int FROM generate_series(1, 1 + (random()*10)::int)", tbl
                )).expect("insert"),
                1 => Spi::run(&format!(
                    "DELETE FROM {} WHERE id IN (SELECT id FROM {} ORDER BY random() LIMIT (1 + (random()*5)::int))", tbl, tbl
                )).expect("delete"),
                _ => Spi::run(&format!(
                    "UPDATE {} SET val = 'v' || (random()*25)::int WHERE id = (SELECT id FROM {} ORDER BY random() LIMIT 1)", tbl, tbl
                )).expect("update"),
            };
            assert_imv_correct(&view, &query);
        }

        Spi::run(&format!("SELECT drop_reflex_ivm('{}', true)", view)).expect("drop");
        Spi::run(&format!("DROP TABLE IF EXISTS {} CASCADE", tbl)).expect("drop table");
    }
}

/// Fuzz: random GROUP BY with NULL group keys
#[pg_test]
fn test_fuzz_null_group_keys() {
    Spi::run("SELECT setseed(0.31)").expect("seed");

    for round in 0..8 {
        let tbl = format!("fuzz_nk_{}", round);
        let view = format!("fuzz_nk_v_{}", round);

        Spi::run(&format!(
            "CREATE TABLE {} (id SERIAL PRIMARY KEY, grp TEXT, val INT NOT NULL)", tbl
        )).expect("create");
        // ~25% NULL group keys
        Spi::run(&format!(
            "INSERT INTO {} (grp, val) SELECT \
                CASE WHEN random() < 0.25 THEN NULL ELSE 'g' || (random()*8)::int END, \
                (random()*500)::int \
             FROM generate_series(1, 200)", tbl
        )).expect("seed");

        let query = format!(
            "SELECT grp, SUM(val) AS total, COUNT(*) AS cnt FROM {} GROUP BY grp", tbl
        );
        crate::create_reflex_ivm(&view, &query, None, None, None);
        assert_imv_correct(&view, &query);

        for _ in 0..12 {
            match Spi::get_one::<i32>("SELECT (random()*3)::int").expect("q").expect("v") {
                0 => Spi::run(&format!(
                    "INSERT INTO {} (grp, val) VALUES (\
                        CASE WHEN random() < 0.3 THEN NULL ELSE 'g' || (random()*8)::int END, \
                        (random()*500)::int)", tbl
                )).expect("insert"),
                1 => Spi::run(&format!(
                    "UPDATE {} SET grp = CASE WHEN random() < 0.3 THEN NULL ELSE 'g' || (random()*8)::int END \
                     WHERE id = (SELECT id FROM {} ORDER BY random() LIMIT 1)", tbl, tbl
                )).expect("update grp"),
                2 => Spi::run(&format!(
                    "DELETE FROM {} WHERE id = (SELECT id FROM {} ORDER BY random() LIMIT 1)", tbl, tbl
                )).expect("delete"),
                _ => Spi::run(&format!(
                    "UPDATE {} SET val = (random()*999)::int WHERE grp IS NULL AND id = \
                     (SELECT MIN(id) FROM {} WHERE grp IS NULL)", tbl, tbl
                )).expect("update null grp val"),
            };
            assert_imv_correct(&view, &query);
        }

        Spi::run(&format!("SELECT drop_reflex_ivm('{}', true)", view)).expect("drop");
        Spi::run(&format!("DROP TABLE IF EXISTS {} CASCADE", tbl)).expect("drop table");
    }
}

/// Fuzz: random JOIN aggregate with mutations on both tables
#[pg_test]
fn test_fuzz_join_aggregate() {
    Spi::run("SELECT setseed(0.55)").expect("seed");

    for round in 0..5 {
        let src = format!("fuzz_js_{}", round);
        let dim = format!("fuzz_jd_{}", round);
        let view = format!("fuzz_j_v_{}", round);

        Spi::run(&format!(
            "CREATE TABLE {} (id INT PRIMARY KEY, label TEXT NOT NULL)", dim
        )).expect("create dim");
        Spi::run(&format!(
            "INSERT INTO {} SELECT i, 'label_' || i FROM generate_series(1, 10) i", dim
        )).expect("seed dim");

        Spi::run(&format!(
            "CREATE TABLE {} (id SERIAL PRIMARY KEY, did INT NOT NULL, val INT NOT NULL)", src
        )).expect("create src");
        Spi::run(&format!(
            "INSERT INTO {} (did, val) SELECT (random()*9+1)::int, (random()*100)::int \
             FROM generate_series(1, 200)", src
        )).expect("seed src");

        let query = format!(
            "SELECT d.label, SUM(s.val) AS total, COUNT(*) AS cnt \
             FROM {} s JOIN {} d ON s.did = d.id GROUP BY d.label", src, dim
        );
        crate::create_reflex_ivm(&view, &query, None, None, None);
        assert_imv_correct(&view, &query);

        for _ in 0..10 {
            match Spi::get_one::<i32>("SELECT (random()*2)::int").expect("q").expect("v") {
                0 => Spi::run(&format!(
                    "INSERT INTO {} (did, val) VALUES ((random()*9+1)::int, (random()*100)::int)", src
                )).expect("insert src"),
                1 => Spi::run(&format!(
                    "DELETE FROM {} WHERE id = (SELECT id FROM {} ORDER BY random() LIMIT 1)", src, src
                )).expect("delete src"),
                _ => Spi::run(&format!(
                    "UPDATE {} SET val = (random()*200)::int WHERE id = (SELECT id FROM {} ORDER BY random() LIMIT 1)", src, src
                )).expect("update src"),
            };
            assert_imv_correct(&view, &query);
        }

        Spi::run(&format!("SELECT drop_reflex_ivm('{}', true)", view)).expect("drop");
        Spi::run(&format!("DROP TABLE IF EXISTS {} CASCADE", src)).expect("drop src");
        Spi::run(&format!("DROP TABLE IF EXISTS {} CASCADE", dim)).expect("drop dim");
    }
}

/// Fuzz: random passthrough with random mutations
#[pg_test]
fn test_fuzz_passthrough() {
    Spi::run("SELECT setseed(0.77)").expect("seed");

    for round in 0..5 {
        let tbl = format!("fuzz_pt_{}", round);
        let view = format!("fuzz_pt_v_{}", round);

        Spi::run(&format!(
            "CREATE TABLE {} (id SERIAL PRIMARY KEY, city TEXT, amount NUMERIC)", tbl
        )).expect("create");
        Spi::run(&format!(
            "INSERT INTO {} (city, amount) SELECT 'c' || (random()*20)::int, (random()*1000)::numeric(10,2) \
             FROM generate_series(1, 100)", tbl
        )).expect("seed");

        let query = format!("SELECT id, city, amount FROM {}", tbl);
        crate::create_reflex_ivm(&view, &query, Some("id"), None, None);
        assert_imv_correct(&view, &query);

        for _ in 0..10 {
            match Spi::get_one::<i32>("SELECT (random()*2)::int").expect("q").expect("v") {
                0 => Spi::run(&format!(
                    "INSERT INTO {} (city, amount) VALUES ('c' || (random()*20)::int, (random()*1000)::numeric(10,2))", tbl
                )).expect("insert"),
                1 => Spi::run(&format!(
                    "DELETE FROM {} WHERE id = (SELECT id FROM {} ORDER BY random() LIMIT 1)", tbl, tbl
                )).expect("delete"),
                _ => Spi::run(&format!(
                    "UPDATE {} SET amount = (random()*999)::numeric(10,2) WHERE id = (\
                        SELECT id FROM {} ORDER BY random() LIMIT 1)", tbl, tbl
                )).expect("update"),
            };
            assert_imv_correct(&view, &query);
        }

        Spi::run(&format!("SELECT drop_reflex_ivm('{}', true)", view)).expect("drop");
        Spi::run(&format!("DROP TABLE IF EXISTS {} CASCADE", tbl)).expect("drop table");
    }
}

// ========================================================================
// Group 7 — Type/cast/keyword correctness
// ========================================================================

/// TIMESTAMP GROUP BY
#[pg_test]
fn test_correctness_timestamp_groupby() {
    Spi::run("CREATE TABLE ts_src (id SERIAL, ts TIMESTAMP NOT NULL, val INT NOT NULL)").expect("create");
    Spi::run("INSERT INTO ts_src (ts, val) VALUES \
        ('2024-01-01 10:00:00', 10), ('2024-01-01 10:00:00', 20), \
        ('2024-06-15 12:00:00', 30)").expect("seed");

    crate::create_reflex_ivm("ts_view",
        "SELECT ts, SUM(val) AS total FROM ts_src GROUP BY ts",
        None, None, None);
    let fresh = "SELECT ts, SUM(val) AS total FROM ts_src GROUP BY ts";
    assert_imv_correct("ts_view", fresh);

    Spi::run("INSERT INTO ts_src (ts, val) VALUES ('2024-01-01 10:00:00', 5)").expect("insert");
    assert_imv_correct("ts_view", fresh);

    Spi::run("DELETE FROM ts_src WHERE val = 10").expect("delete");
    assert_imv_correct("ts_view", fresh);
}

/// DATE GROUP BY
#[pg_test]
fn test_correctness_date_groupby() {
    Spi::run("CREATE TABLE dt_src (id SERIAL, d DATE NOT NULL, val INT NOT NULL)").expect("create");
    Spi::run("INSERT INTO dt_src (d, val) VALUES ('2024-01-01', 100), ('2024-01-01', 200), ('2024-12-31', 50)").expect("seed");

    crate::create_reflex_ivm("dt_view",
        "SELECT d, SUM(val) AS total, COUNT(*) AS cnt FROM dt_src GROUP BY d",
        None, None, None);
    let fresh = "SELECT d, SUM(val) AS total, COUNT(*) AS cnt FROM dt_src GROUP BY d";
    assert_imv_correct("dt_view", fresh);

    Spi::run("INSERT INTO dt_src (d, val) VALUES ('2024-12-31', 150)").expect("insert");
    assert_imv_correct("dt_view", fresh);

    Spi::run("UPDATE dt_src SET val = 999 WHERE d = '2024-01-01' AND val = 100").expect("update");
    assert_imv_correct("dt_view", fresh);
}

/// FLOAT8 SUM — use integer-representable floats to avoid precision issues in EXCEPT ALL
#[pg_test]
fn test_correctness_float_sum() {
    Spi::run("CREATE TABLE fl_src (id SERIAL, grp TEXT, val FLOAT8)").expect("create");
    Spi::run("INSERT INTO fl_src (grp, val) VALUES ('a', 1.0), ('a', 2.0), ('b', 3.0)").expect("seed");

    crate::create_reflex_ivm("fl_view",
        "SELECT grp, SUM(val) AS total FROM fl_src GROUP BY grp",
        None, None, None);
    let fresh = "SELECT grp, SUM(val) AS total FROM fl_src GROUP BY grp";
    assert_imv_correct("fl_view", fresh);

    Spi::run("INSERT INTO fl_src (grp, val) VALUES ('a', 4.0)").expect("insert");
    assert_imv_correct("fl_view", fresh);

    Spi::run("DELETE FROM fl_src WHERE grp = 'b'").expect("delete");
    assert_imv_correct("fl_view", fresh);

    Spi::run("UPDATE fl_src SET val = 10.0 WHERE val = 1.0").expect("update");
    assert_imv_correct("fl_view", fresh);
}

/// BIGINT SUM — large values
#[pg_test]
fn test_correctness_bigint_sum() {
    Spi::run("CREATE TABLE bi_src (id SERIAL, grp TEXT, val BIGINT)").expect("create");
    Spi::run("INSERT INTO bi_src (grp, val) VALUES ('a', 1000000000), ('a', 2000000000), ('b', 9000000000000)").expect("seed");

    crate::create_reflex_ivm("bi_view",
        "SELECT grp, SUM(val) AS total, COUNT(*) AS cnt FROM bi_src GROUP BY grp",
        None, None, None);
    let fresh = "SELECT grp, SUM(val) AS total, COUNT(*) AS cnt FROM bi_src GROUP BY grp";
    assert_imv_correct("bi_view", fresh);

    Spi::run("INSERT INTO bi_src (grp, val) VALUES ('a', 5000000000000)").expect("insert");
    assert_imv_correct("bi_view", fresh);

    Spi::run("DELETE FROM bi_src WHERE val = 2000000000").expect("delete");
    assert_imv_correct("bi_view", fresh);
}

/// TEXT MIN/MAX — lexicographic ordering
#[pg_test]
fn test_correctness_text_min_max() {
    Spi::run("CREATE TABLE tmm (id SERIAL PRIMARY KEY, grp INT, val TEXT NOT NULL)").expect("create");
    Spi::run("INSERT INTO tmm (grp, val) VALUES (1, 'banana'), (1, 'apple'), (1, 'cherry'), (2, 'zebra')").expect("seed");

    crate::create_reflex_ivm("tmm_view",
        "SELECT grp, MIN(val) AS lo, MAX(val) AS hi FROM tmm GROUP BY grp",
        None, None, None);
    let fresh = "SELECT grp, MIN(val) AS lo, MAX(val) AS hi FROM tmm GROUP BY grp";
    assert_imv_correct("tmm_view", fresh);

    // Delete the MIN
    Spi::run("DELETE FROM tmm WHERE val = 'apple'").expect("delete min");
    assert_imv_correct("tmm_view", fresh);

    // Insert new MIN
    Spi::run("INSERT INTO tmm (grp, val) VALUES (1, 'aardvark')").expect("insert new min");
    assert_imv_correct("tmm_view", fresh);
}

/// Mixed-type composite GROUP BY key (INT, TEXT, DATE)
#[pg_test]
fn test_correctness_mixed_type_groupby() {
    Spi::run("CREATE TABLE mix (id SERIAL, region INT, city TEXT, d DATE, val INT)").expect("create");
    Spi::run("INSERT INTO mix (region, city, d, val) VALUES \
        (1, 'NYC', '2024-01-01', 10), (1, 'NYC', '2024-01-01', 20), \
        (1, 'LA', '2024-01-01', 30), (2, 'NYC', '2024-06-01', 40)").expect("seed");

    crate::create_reflex_ivm("mix_view",
        "SELECT region, city, d, SUM(val) AS total FROM mix GROUP BY region, city, d",
        None, None, None);
    let fresh = "SELECT region, city, d, SUM(val) AS total FROM mix GROUP BY region, city, d";
    assert_imv_correct("mix_view", fresh);

    Spi::run("INSERT INTO mix (region, city, d, val) VALUES (1, 'NYC', '2024-01-01', 5)").expect("insert");
    assert_imv_correct("mix_view", fresh);

    Spi::run("UPDATE mix SET region = 2 WHERE val = 10").expect("update key");
    assert_imv_correct("mix_view", fresh);
}

/// SUM::BIGINT through mutations
#[pg_test]
fn test_correctness_cast_sum_bigint_mutations() {
    Spi::run("CREATE TABLE csb (id SERIAL, grp TEXT, val INT)").expect("create");
    Spi::run("INSERT INTO csb (grp, val) VALUES ('a', 10), ('a', 20), ('b', 30)").expect("seed");

    crate::create_reflex_ivm("csb_view",
        "SELECT grp, SUM(val)::BIGINT AS total FROM csb GROUP BY grp",
        None, None, None);
    let fresh = "SELECT grp, SUM(val)::BIGINT AS total FROM csb GROUP BY grp";
    assert_imv_correct("csb_view", fresh);

    Spi::run("INSERT INTO csb (grp, val) VALUES ('a', 100)").expect("insert");
    assert_imv_correct("csb_view", fresh);

    Spi::run("DELETE FROM csb WHERE val = 20").expect("delete");
    assert_imv_correct("csb_view", fresh);

    Spi::run("UPDATE csb SET val = 999 WHERE grp = 'b'").expect("update");
    assert_imv_correct("csb_view", fresh);
}

/// COUNT(*)::INT through mutations
#[pg_test]
fn test_correctness_cast_count_int() {
    Spi::run("CREATE TABLE cci (id SERIAL, grp TEXT)").expect("create");
    Spi::run("INSERT INTO cci (grp) VALUES ('a'), ('a'), ('b')").expect("seed");

    crate::create_reflex_ivm("cci_view",
        "SELECT grp, COUNT(*)::INT AS cnt FROM cci GROUP BY grp",
        None, None, None);
    let fresh = "SELECT grp, COUNT(*)::INT AS cnt FROM cci GROUP BY grp";
    assert_imv_correct("cci_view", fresh);

    Spi::run("INSERT INTO cci (grp) VALUES ('a'), ('c')").expect("insert");
    assert_imv_correct("cci_view", fresh);

    Spi::run("DELETE FROM cci WHERE grp = 'b'").expect("delete");
    assert_imv_correct("cci_view", fresh);
}

/// Columns with underscore-heavy names (common in analytics)
#[pg_test]
fn test_correctness_underscore_column_names() {
    Spi::run("CREATE TABLE uc (id SERIAL, user_region TEXT, order_amount INT, item_count INT)").expect("create");
    Spi::run("INSERT INTO uc (user_region, order_amount, item_count) VALUES ('us_east', 10, 2), ('us_east', 20, 3), ('eu_west', 30, 1)").expect("seed");

    crate::create_reflex_ivm("uc_view",
        "SELECT user_region, SUM(order_amount) AS total_amount, SUM(item_count) AS total_items FROM uc GROUP BY user_region",
        None, None, None);
    let fresh = "SELECT user_region, SUM(order_amount) AS total_amount, SUM(item_count) AS total_items FROM uc GROUP BY user_region";
    assert_imv_correct("uc_view", fresh);

    Spi::run("INSERT INTO uc (user_region, order_amount, item_count) VALUES ('us_east', 50, 5)").expect("insert");
    assert_imv_correct("uc_view", fresh);

    Spi::run("DELETE FROM uc WHERE order_amount = 10").expect("delete");
    assert_imv_correct("uc_view", fresh);
}

/// SQL keyword column names — now properly handled via quote stripping in normalized_column_name
#[pg_test]
fn test_correctness_keyword_column_names() {
    Spi::run("CREATE TABLE kw_src (id SERIAL, \"select\" TEXT, \"from\" INT)").expect("create");
    Spi::run("INSERT INTO kw_src (\"select\", \"from\") VALUES ('a', 10), ('a', 20), ('b', 30)").expect("seed");

    crate::create_reflex_ivm("kw_view",
        "SELECT \"select\", SUM(\"from\") AS total FROM kw_src GROUP BY \"select\"",
        None, None, None);
    let fresh = "SELECT \"select\", SUM(\"from\") AS total FROM kw_src GROUP BY \"select\"";
    assert_imv_correct("kw_view", fresh);

    Spi::run("INSERT INTO kw_src (\"select\", \"from\") VALUES ('a', 50)").expect("insert");
    assert_imv_correct("kw_view", fresh);

    Spi::run("DELETE FROM kw_src WHERE \"from\" = 10").expect("delete");
    assert_imv_correct("kw_view", fresh);
}

/// NULL GROUP BY keys — IS NOT DISTINCT FROM handles NULL = NULL correctly
#[pg_test]
fn test_correctness_null_group_key() {
    Spi::run("CREATE TABLE ngk (id SERIAL PRIMARY KEY, grp TEXT, val INT NOT NULL)").expect("create");
    Spi::run("INSERT INTO ngk (grp, val) VALUES ('a', 10), ('a', 20), (NULL, 30), (NULL, 40), ('b', 50)").expect("seed");

    crate::create_reflex_ivm("ngk_view",
        "SELECT grp, SUM(val) AS total, COUNT(*) AS cnt FROM ngk GROUP BY grp",
        None, None, None);

    let fresh = "SELECT grp, SUM(val) AS total, COUNT(*) AS cnt FROM ngk GROUP BY grp";
    assert_imv_correct("ngk_view", fresh);
    // NULL group: total=70, cnt=2; 'a': total=30, cnt=2; 'b': total=50, cnt=1

    // Insert into NULL group
    Spi::run("INSERT INTO ngk (grp, val) VALUES (NULL, 100)").expect("insert null");
    assert_imv_correct("ngk_view", fresh);
    // NULL group: total=170, cnt=3

    // Delete from NULL group
    Spi::run("DELETE FROM ngk WHERE grp IS NULL AND val = 30").expect("delete null");
    assert_imv_correct("ngk_view", fresh);
    // NULL group: total=140, cnt=2

    // Update non-NULL to NULL (move row between groups)
    Spi::run("UPDATE ngk SET grp = NULL WHERE grp = 'b'").expect("move to null");
    assert_imv_correct("ngk_view", fresh);
    // NULL group: total=190, cnt=3; 'b' disappears

    // Update NULL to non-NULL (move row out of NULL group)
    Spi::run("UPDATE ngk SET grp = 'c' WHERE grp IS NULL AND val = 40").expect("move from null");
    assert_imv_correct("ngk_view", fresh);
}

/// NULL GROUP BY with multiple NULL key columns
#[pg_test]
fn test_correctness_null_multi_column_group_key() {
    Spi::run("CREATE TABLE nmk (id SERIAL, g1 TEXT, g2 INT, val INT NOT NULL)").expect("create");
    Spi::run("INSERT INTO nmk (g1, g2, val) VALUES \
        ('a', 1, 10), ('a', NULL, 20), (NULL, 1, 30), (NULL, NULL, 40)").expect("seed");

    crate::create_reflex_ivm("nmk_view",
        "SELECT g1, g2, SUM(val) AS total FROM nmk GROUP BY g1, g2",
        None, None, None);

    let fresh = "SELECT g1, g2, SUM(val) AS total FROM nmk GROUP BY g1, g2";
    assert_imv_correct("nmk_view", fresh);

    // Insert with both NULLs — should merge into existing (NULL, NULL) group
    Spi::run("INSERT INTO nmk (g1, g2, val) VALUES (NULL, NULL, 100)").expect("insert both null");
    assert_imv_correct("nmk_view", fresh);

    // Insert with one NULL
    Spi::run("INSERT INTO nmk (g1, g2, val) VALUES ('a', NULL, 5)").expect("insert one null");
    assert_imv_correct("nmk_view", fresh);

    Spi::run("DELETE FROM nmk WHERE val = 40").expect("delete");
    assert_imv_correct("nmk_view", fresh);
}

/// SUM(price * quantity) — expression inside aggregate
#[pg_test]
fn test_correctness_expression_in_aggregate() {
    Spi::run("CREATE TABLE expr_agg (id SERIAL, grp TEXT, price NUMERIC, qty INT)").expect("create");
    Spi::run("INSERT INTO expr_agg (grp, price, qty) VALUES ('a', 10.5, 2), ('a', 20.0, 3), ('b', 5.0, 10)").expect("seed");

    crate::create_reflex_ivm("expr_view",
        "SELECT grp, SUM(price * qty) AS revenue FROM expr_agg GROUP BY grp",
        None, None, None);
    let fresh = "SELECT grp, SUM(price * qty) AS revenue FROM expr_agg GROUP BY grp";
    assert_imv_correct("expr_view", fresh);

    Spi::run("INSERT INTO expr_agg (grp, price, qty) VALUES ('a', 100.0, 1)").expect("insert");
    assert_imv_correct("expr_view", fresh);

    Spi::run("DELETE FROM expr_agg WHERE price = 20.0").expect("delete");
    assert_imv_correct("expr_view", fresh);

    Spi::run("UPDATE expr_agg SET qty = qty + 1 WHERE grp = 'b'").expect("update");
    assert_imv_correct("expr_view", fresh);
}

// ========================================================================
// Group 8 — More correctness
// ========================================================================

/// BOOL_OR: all values become false after deletes
#[pg_test]
fn test_correctness_bool_or_all_false() {
    Spi::run("CREATE TABLE cc_bor (id SERIAL PRIMARY KEY, grp TEXT, flag BOOLEAN)").expect("create");
    Spi::run("INSERT INTO cc_bor (grp, flag) VALUES ('a', true), ('a', false), ('b', false), ('b', true)").expect("seed");

    let sql = "SELECT grp, BOOL_OR(flag) AS any_true FROM cc_bor GROUP BY grp";
    crate::create_reflex_ivm("cc_bor_v", sql, None, None, None);
    assert_imv_correct("cc_bor_v", sql);

    // Delete all true rows — BOOL_OR should become false for both groups
    Spi::run("DELETE FROM cc_bor WHERE flag = true").expect("delete");
    assert_imv_correct("cc_bor_v", sql);

    // Re-insert a true -> should flip back
    Spi::run("INSERT INTO cc_bor (grp, flag) VALUES ('a', true)").expect("insert");
    assert_imv_correct("cc_bor_v", sql);
}

/// MIN/MAX: entire group deleted, group should disappear
#[pg_test]
fn test_correctness_min_max_all_deleted() {
    Spi::run("CREATE TABLE cc_mm (id SERIAL PRIMARY KEY, grp TEXT, val INT NOT NULL)").expect("create");
    Spi::run("INSERT INTO cc_mm (grp, val) VALUES ('a', 10), ('a', 20), ('b', 5)").expect("seed");

    let sql = "SELECT grp, MIN(val) AS lo, MAX(val) AS hi FROM cc_mm GROUP BY grp";
    crate::create_reflex_ivm("cc_mm_v", sql, None, None, None);
    assert_imv_correct("cc_mm_v", sql);

    // Delete all 'a' rows — group disappears
    Spi::run("DELETE FROM cc_mm WHERE grp = 'a'").expect("delete");
    assert_imv_correct("cc_mm_v", sql);

    // Re-insert into 'a'
    Spi::run("INSERT INTO cc_mm (grp, val) VALUES ('a', 100)").expect("insert");
    assert_imv_correct("cc_mm_v", sql);
}

/// AVG with single-row groups
#[pg_test]
fn test_correctness_avg_single_row_group() {
    Spi::run("CREATE TABLE cc_avg1 (id SERIAL PRIMARY KEY, grp TEXT, val NUMERIC NOT NULL)").expect("create");
    Spi::run("INSERT INTO cc_avg1 (grp, val) VALUES ('a', 10.5), ('b', 20.7), ('c', 30.3)").expect("seed");

    let sql = "SELECT grp, AVG(val) AS mean FROM cc_avg1 GROUP BY grp";
    crate::create_reflex_ivm("cc_avg1_v", sql, None, None, None);
    assert_imv_correct("cc_avg1_v", sql);

    // Add second row to 'a' -> AVG should change
    Spi::run("INSERT INTO cc_avg1 (grp, val) VALUES ('a', 30.5)").expect("insert");
    assert_imv_correct("cc_avg1_v", sql);

    // Delete it back -> single-row again
    Spi::run("DELETE FROM cc_avg1 WHERE grp = 'a' AND val = 30.5").expect("delete");
    assert_imv_correct("cc_avg1_v", sql);
}

/// COUNT(col) where all values are NULL
#[pg_test]
fn test_correctness_count_col_all_null() {
    Spi::run("CREATE TABLE cc_cnull (id SERIAL PRIMARY KEY, grp TEXT, val INT)").expect("create");
    Spi::run("INSERT INTO cc_cnull (grp, val) VALUES ('a', NULL), ('a', NULL), ('b', 1)").expect("seed");

    let sql = "SELECT grp, COUNT(val) AS cnt FROM cc_cnull GROUP BY grp";
    crate::create_reflex_ivm("cc_cnull_v", sql, None, None, None);
    assert_imv_correct("cc_cnull_v", sql);

    // Insert non-null into 'a'
    Spi::run("INSERT INTO cc_cnull (grp, val) VALUES ('a', 5)").expect("insert");
    assert_imv_correct("cc_cnull_v", sql);

    // Delete the non-null -> back to 0
    Spi::run("DELETE FROM cc_cnull WHERE val = 5").expect("delete");
    assert_imv_correct("cc_cnull_v", sql);
}

/// SUM with negative values
#[pg_test]
fn test_correctness_sum_negative_values() {
    Spi::run("CREATE TABLE cc_sneg (id SERIAL PRIMARY KEY, grp TEXT, val INT NOT NULL)").expect("create");
    Spi::run("INSERT INTO cc_sneg (grp, val) VALUES ('a', -10), ('a', 20), ('a', -5), ('b', -100), ('b', 50)").expect("seed");

    let sql = "SELECT grp, SUM(val) AS total FROM cc_sneg GROUP BY grp";
    crate::create_reflex_ivm("cc_sneg_v", sql, None, None, None);
    assert_imv_correct("cc_sneg_v", sql);

    // Insert more negatives
    Spi::run("INSERT INTO cc_sneg (grp, val) VALUES ('a', -30), ('b', -1)").expect("insert");
    assert_imv_correct("cc_sneg_v", sql);

    // Update positive to negative
    Spi::run("UPDATE cc_sneg SET val = -20 WHERE val = 20").expect("update");
    assert_imv_correct("cc_sneg_v", sql);
}

/// Multiple aggregates on same column
#[pg_test]
fn test_correctness_multi_aggregate_same_col() {
    Spi::run("CREATE TABLE cc_magg (id SERIAL PRIMARY KEY, grp TEXT, val INT NOT NULL)").expect("create");
    Spi::run("INSERT INTO cc_magg (grp, val) VALUES ('a', 10), ('a', 20), ('a', 30), ('b', 5), ('b', 15)").expect("seed");

    let sql = "SELECT grp, SUM(val) AS s, AVG(val) AS a, MIN(val) AS lo, MAX(val) AS hi, COUNT(val) AS c FROM cc_magg GROUP BY grp";
    crate::create_reflex_ivm("cc_magg_v", sql, None, None, None);
    assert_imv_correct("cc_magg_v", sql);

    // Insert
    Spi::run("INSERT INTO cc_magg (grp, val) VALUES ('a', 1), ('b', 100)").expect("insert");
    assert_imv_correct("cc_magg_v", sql);

    // Delete min for 'b'
    Spi::run("DELETE FROM cc_magg WHERE grp = 'b' AND val = 5").expect("delete");
    assert_imv_correct("cc_magg_v", sql);

    // Update
    Spi::run("UPDATE cc_magg SET val = 99 WHERE grp = 'a' AND val = 1").expect("update");
    assert_imv_correct("cc_magg_v", sql);
}

/// FULL OUTER JOIN with aggregation
#[pg_test]
fn test_correctness_full_outer_join_aggregate() {
    Spi::run("CREATE TABLE cc_foj1 (id INT PRIMARY KEY, grp TEXT, val INT NOT NULL)").expect("create1");
    Spi::run("CREATE TABLE cc_foj2 (id INT PRIMARY KEY, grp TEXT, val INT NOT NULL)").expect("create2");
    Spi::run("INSERT INTO cc_foj1 VALUES (1, 'a', 10), (2, 'a', 20), (3, 'b', 30)").expect("seed1");
    Spi::run("INSERT INTO cc_foj2 VALUES (1, 'a', 100), (2, 'c', 200)").expect("seed2");

    // FULL OUTER JOIN aggregate — outer-join-secondary detection handles this
    let sql = "SELECT COALESCE(cc_foj1.grp, cc_foj2.grp) AS grp, SUM(cc_foj1.val) AS s1, SUM(cc_foj2.val) AS s2 \
               FROM cc_foj1 FULL OUTER JOIN cc_foj2 ON cc_foj1.grp = cc_foj2.grp \
               GROUP BY COALESCE(cc_foj1.grp, cc_foj2.grp)";
    crate::create_reflex_ivm("cc_foj_v", sql, None, None, None);
    assert_imv_correct("cc_foj_v", sql);

    // Insert into left
    Spi::run("INSERT INTO cc_foj1 VALUES (4, 'a', 5)").expect("insert1");
    assert_imv_correct("cc_foj_v", sql);

    // Insert into right, new matching group
    Spi::run("INSERT INTO cc_foj2 VALUES (3, 'b', 50)").expect("insert2");
    assert_imv_correct("cc_foj_v", sql);

    // Delete from left
    Spi::run("DELETE FROM cc_foj1 WHERE id = 1").expect("delete");
    assert_imv_correct("cc_foj_v", sql);
}

/// CROSS JOIN with mutations
#[pg_test]
fn test_correctness_cross_join() {
    Spi::run("CREATE TABLE cc_cj1 (id INT PRIMARY KEY, x TEXT)").expect("create");
    Spi::run("CREATE TABLE cc_cj2 (id INT PRIMARY KEY, y TEXT)").expect("create");
    Spi::run("INSERT INTO cc_cj1 VALUES (1, 'a'), (2, 'b')").expect("seed1");
    Spi::run("INSERT INTO cc_cj2 VALUES (10, 'x'), (20, 'y')").expect("seed2");

    let sql = "SELECT cc_cj1.id AS l, cc_cj2.id AS r, x, y FROM cc_cj1 CROSS JOIN cc_cj2";
    crate::create_reflex_ivm("cc_cj_v", sql, Some("l, r"), None, None);
    assert_imv_correct("cc_cj_v", sql);

    // Insert one row -> should add N cross products
    Spi::run("INSERT INTO cc_cj1 VALUES (3, 'c')").expect("insert");
    assert_imv_correct("cc_cj_v", sql);

    // Delete from other side
    Spi::run("DELETE FROM cc_cj2 WHERE id = 10").expect("delete");
    assert_imv_correct("cc_cj_v", sql);
}

/// Self-join with aggregation lifecycle
#[pg_test]
fn test_correctness_self_join_aggregate_lifecycle() {
    Spi::run("CREATE TABLE cc_sj (id SERIAL PRIMARY KEY, grp INT NOT NULL, val INT NOT NULL)").expect("create");
    Spi::run("INSERT INTO cc_sj (grp, val) VALUES (1, 10), (1, 20), (2, 30), (2, 40)").expect("seed");

    // Self-join with GROUP BY triggers full refresh (auto-detected)
    let sql = "SELECT t1.grp, SUM(t1.val + t2.val) AS total FROM cc_sj t1 JOIN cc_sj t2 ON t1.grp = t2.grp GROUP BY t1.grp";
    crate::create_reflex_ivm("cc_sj_v", sql, None, None, None);
    assert_imv_correct("cc_sj_v", sql);

    // Insert
    Spi::run("INSERT INTO cc_sj (grp, val) VALUES (1, 5)").expect("insert");
    assert_imv_correct("cc_sj_v", sql);

    // Delete
    Spi::run("DELETE FROM cc_sj WHERE val = 5").expect("delete");
    assert_imv_correct("cc_sj_v", sql);

    // Update
    Spi::run("UPDATE cc_sj SET val = 99 WHERE grp = 2 AND val = 40").expect("update");
    assert_imv_correct("cc_sj_v", sql);
}

/// 3-level CTE chain with mutations
#[pg_test]
fn test_correctness_cte_three_levels() {
    Spi::run("CREATE TABLE cc_cte3 (id SERIAL PRIMARY KEY, grp TEXT, val INT NOT NULL)").expect("create");
    Spi::run("INSERT INTO cc_cte3 (grp, val) VALUES ('a', 10), ('a', 20), ('b', 30), ('b', 40), ('c', 50)").expect("seed");

    let sql = "WITH \
        level1 AS (SELECT grp, SUM(val) AS total FROM cc_cte3 GROUP BY grp), \
        level2 AS (SELECT grp, total FROM level1 WHERE total > 15) \
        SELECT grp, total FROM level2";
    crate::create_reflex_ivm("cc_cte3_v", sql, None, None, None);
    assert_imv_correct("cc_cte3_v", sql);

    // Insert to push 'c' total higher
    Spi::run("INSERT INTO cc_cte3 (grp, val) VALUES ('c', 100)").expect("insert");
    assert_imv_correct("cc_cte3_v", sql);

    // Delete to drop 'a' below threshold
    Spi::run("DELETE FROM cc_cte3 WHERE grp = 'a' AND val = 20").expect("delete");
    assert_imv_correct("cc_cte3_v", sql);
}

/// WHERE that initially matches no rows, then rows appear
#[pg_test]
fn test_correctness_where_excludes_all() {
    Spi::run("CREATE TABLE cc_wex (id SERIAL PRIMARY KEY, grp TEXT, val INT NOT NULL)").expect("create");
    Spi::run("INSERT INTO cc_wex (grp, val) VALUES ('a', 1), ('b', 2)").expect("seed");

    let sql = "SELECT grp, SUM(val) AS total FROM cc_wex WHERE val > 100 GROUP BY grp";
    crate::create_reflex_ivm("cc_wex_v", sql, None, None, None);
    assert_imv_correct("cc_wex_v", sql);

    // IMV should be empty
    assert_eq!(
        Spi::get_one::<i64>("SELECT COUNT(*) FROM cc_wex_v").expect("q").expect("v"),
        0
    );

    // Insert matching rows -> groups should appear
    Spi::run("INSERT INTO cc_wex (grp, val) VALUES ('a', 200), ('c', 300)").expect("insert");
    assert_imv_correct("cc_wex_v", sql);

    // Delete -> back to empty
    Spi::run("DELETE FROM cc_wex WHERE val > 100").expect("delete");
    assert_imv_correct("cc_wex_v", sql);
}

/// HAVING: group bounces above and below threshold
#[pg_test]
fn test_correctness_having_group_enters_exits() {
    Spi::run("CREATE TABLE cc_hav (id SERIAL PRIMARY KEY, grp TEXT, val INT NOT NULL)").expect("create");
    Spi::run("INSERT INTO cc_hav (grp, val) VALUES ('a', 10), ('a', 20), ('b', 5)").expect("seed");

    let sql = "SELECT grp, SUM(val) AS total FROM cc_hav GROUP BY grp HAVING SUM(val) > 25";
    crate::create_reflex_ivm("cc_hav_v", sql, None, None, None);
    assert_imv_correct("cc_hav_v", sql);

    // 'a' total=30 (visible), 'b' total=5 (hidden)
    // Push 'b' above threshold
    Spi::run("INSERT INTO cc_hav (grp, val) VALUES ('b', 25)").expect("insert");
    assert_imv_correct("cc_hav_v", sql);

    // Drop 'a' below threshold
    Spi::run("DELETE FROM cc_hav WHERE grp = 'a' AND val = 20").expect("delete");
    assert_imv_correct("cc_hav_v", sql);

    // Push 'a' back above
    Spi::run("INSERT INTO cc_hav (grp, val) VALUES ('a', 50)").expect("insert2");
    assert_imv_correct("cc_hav_v", sql);
}

/// Window: LAG/LEAD through full lifecycle
#[pg_test]
fn test_correctness_window_lag_lead_mutations() {
    Spi::run("CREATE TABLE cc_wlag (id SERIAL PRIMARY KEY, grp TEXT, val INT NOT NULL)").expect("create");
    Spi::run("INSERT INTO cc_wlag (grp, val) VALUES ('a', 10), ('a', 20), ('a', 30), ('b', 5), ('b', 15)").expect("seed");

    let sql = "SELECT grp, val, LAG(val) OVER (PARTITION BY grp ORDER BY val) AS prev_val, \
               LEAD(val) OVER (PARTITION BY grp ORDER BY val) AS next_val \
               FROM cc_wlag";
    crate::create_reflex_ivm("cc_wlag_v", sql, None, None, None);
    assert_imv_correct("cc_wlag_v", sql);

    // Insert -> windows should recalculate
    Spi::run("INSERT INTO cc_wlag (grp, val) VALUES ('a', 15)").expect("insert");
    assert_imv_correct("cc_wlag_v", sql);

    // Delete
    Spi::run("DELETE FROM cc_wlag WHERE grp = 'a' AND val = 20").expect("delete");
    assert_imv_correct("cc_wlag_v", sql);
}

// ========================================================================
// Group 9 — Randomized tests
// ========================================================================

/// Iterate over multiple aggregate SQL templates, verifying correctness after each mutation
#[pg_test]
fn test_randomized_aggregate_correctness() {
    Spi::run("CREATE TABLE rnd_agg (id SERIAL PRIMARY KEY, grp TEXT NOT NULL, val INT NOT NULL, flag BOOLEAN NOT NULL)").expect("create");
    Spi::run(
        "INSERT INTO rnd_agg (grp, val, flag) \
         SELECT 'g' || (i % 5), (i * 7 + 13) % 100, (i % 3 = 0) \
         FROM generate_series(1, 100) i"
    ).expect("seed");

    let test_cases: Vec<(&str, &str)> = vec![
        ("SELECT grp, SUM(val) AS s FROM rnd_agg GROUP BY grp", "rnd_sum"),
        ("SELECT grp, COUNT(*) AS c FROM rnd_agg GROUP BY grp", "rnd_cnt"),
        ("SELECT grp, COUNT(val) AS c FROM rnd_agg GROUP BY grp", "rnd_cntv"),
        ("SELECT grp, AVG(val) AS a FROM rnd_agg GROUP BY grp", "rnd_avg"),
        ("SELECT grp, MIN(val) AS lo FROM rnd_agg GROUP BY grp", "rnd_min"),
        ("SELECT grp, MAX(val) AS hi FROM rnd_agg GROUP BY grp", "rnd_max"),
        ("SELECT grp, BOOL_OR(flag) AS any FROM rnd_agg GROUP BY grp", "rnd_bor"),
        ("SELECT grp, SUM(val) AS s, COUNT(*) AS c, AVG(val) AS a FROM rnd_agg GROUP BY grp", "rnd_multi"),
        ("SELECT grp, MIN(val) AS lo, MAX(val) AS hi, SUM(val) AS s FROM rnd_agg GROUP BY grp", "rnd_mmsum"),
        ("SELECT grp, SUM(val) AS s FROM rnd_agg WHERE val > 30 GROUP BY grp", "rnd_where"),
        ("SELECT grp, SUM(val) AS s FROM rnd_agg GROUP BY grp HAVING SUM(val) > 200", "rnd_having"),
        ("SELECT grp, SUM(val)::BIGINT AS s FROM rnd_agg GROUP BY grp", "rnd_cast"),
    ];

    for (sql, name) in &test_cases {
        let result = crate::create_reflex_ivm(name, sql, None, None, None);
        assert!(!result.starts_with("ERROR"), "Failed to create IMV '{}': {}", name, result);
        assert_imv_correct(name, sql);

        // INSERT
        Spi::run("INSERT INTO rnd_agg (grp, val, flag) VALUES ('g0', 42, true), ('g3', 77, false)").expect("insert");
        assert_imv_correct(name, sql);

        // DELETE
        Spi::run("DELETE FROM rnd_agg WHERE id IN (SELECT id FROM rnd_agg ORDER BY id LIMIT 3)").expect("delete");
        assert_imv_correct(name, sql);

        // UPDATE
        Spi::run("UPDATE rnd_agg SET val = val + 1 WHERE grp = 'g1'").expect("update");
        assert_imv_correct(name, sql);

        // Cleanup
        crate::drop_reflex_ivm_cascade(name, true);
        // Restore data for next iteration
        Spi::run("DELETE FROM rnd_agg").expect("clear");
        Spi::run(
            "INSERT INTO rnd_agg (grp, val, flag) \
             SELECT 'g' || (i % 5), (i * 7 + 13) % 100, (i % 3 = 0) \
             FROM generate_series(1, 100) i"
        ).expect("reseed");
    }
}

/// Iterate over join types with correctness checks
#[pg_test]
fn test_randomized_join_correctness() {
    Spi::run("CREATE TABLE rnd_j1 (id INT PRIMARY KEY, grp TEXT NOT NULL, val INT NOT NULL)").expect("create1");
    Spi::run("CREATE TABLE rnd_j2 (id INT PRIMARY KEY, grp TEXT NOT NULL, val INT NOT NULL)").expect("create2");
    Spi::run("INSERT INTO rnd_j1 VALUES (1,'a',10),(2,'a',20),(3,'b',30),(4,'c',40)").expect("seed1");
    Spi::run("INSERT INTO rnd_j2 VALUES (1,'a',100),(2,'b',200),(3,'d',300)").expect("seed2");

    // Test aggregate queries over different join types
    // Test INNER JOIN aggregate with mutations to both tables
    let inner_sql = "SELECT rnd_j1.grp, SUM(rnd_j1.val) AS s1, SUM(rnd_j2.val) AS s2 \
             FROM rnd_j1 INNER JOIN rnd_j2 ON rnd_j1.grp = rnd_j2.grp GROUP BY rnd_j1.grp";
    let result = crate::create_reflex_ivm("rnd_inner", inner_sql, None, None, None);
    assert!(!result.starts_with("ERROR"), "Failed to create 'rnd_inner': {}", result);
    assert_imv_correct("rnd_inner", inner_sql);

    // Mutate left table
    Spi::run("INSERT INTO rnd_j1 VALUES (5, 'b', 50)").expect("insert left");
    assert_imv_correct("rnd_inner", inner_sql);

    // Mutate right table
    Spi::run("INSERT INTO rnd_j2 VALUES (4, 'a', 150)").expect("insert right");
    assert_imv_correct("rnd_inner", inner_sql);

    // Delete from left
    Spi::run("DELETE FROM rnd_j1 WHERE id = 5").expect("delete left");
    assert_imv_correct("rnd_inner", inner_sql);

    // Delete from right
    Spi::run("DELETE FROM rnd_j2 WHERE id = 4").expect("delete right");
    assert_imv_correct("rnd_inner", inner_sql);

    crate::drop_reflex_ivm_cascade("rnd_inner", true);

    // Test LEFT JOIN aggregate — only mutate primary (left) table
    let left_sql = "SELECT rnd_j1.grp, SUM(rnd_j1.val) AS s1, COUNT(rnd_j2.val) AS c2 \
             FROM rnd_j1 LEFT JOIN rnd_j2 ON rnd_j1.grp = rnd_j2.grp GROUP BY rnd_j1.grp";
    let result = crate::create_reflex_ivm("rnd_left", left_sql, None, None, None);
    assert!(!result.starts_with("ERROR"), "Failed to create 'rnd_left': {}", result);
    assert_imv_correct("rnd_left", left_sql);

    // Mutate primary (left) table
    Spi::run("INSERT INTO rnd_j1 VALUES (6, 'b', 60)").expect("insert left");
    assert_imv_correct("rnd_left", left_sql);

    Spi::run("DELETE FROM rnd_j1 WHERE id = 6").expect("delete left");
    assert_imv_correct("rnd_left", left_sql);

    crate::drop_reflex_ivm_cascade("rnd_left", true);

    // Test RIGHT JOIN aggregate — only mutate primary (right) table
    let right_sql = "SELECT rnd_j2.grp, COUNT(rnd_j1.val) AS c1, SUM(rnd_j2.val) AS s2 \
             FROM rnd_j1 RIGHT JOIN rnd_j2 ON rnd_j1.grp = rnd_j2.grp GROUP BY rnd_j2.grp";
    let result = crate::create_reflex_ivm("rnd_right", right_sql, None, None, None);
    assert!(!result.starts_with("ERROR"), "Failed to create 'rnd_right': {}", result);
    assert_imv_correct("rnd_right", right_sql);

    // Mutate primary (right) table
    Spi::run("INSERT INTO rnd_j2 VALUES (5, 'b', 250)").expect("insert right");
    assert_imv_correct("rnd_right", right_sql);

    Spi::run("DELETE FROM rnd_j2 WHERE id = 5").expect("delete right");
    assert_imv_correct("rnd_right", right_sql);

    crate::drop_reflex_ivm_cascade("rnd_right", true);
}

/// Stress test: many sequential mutations with periodic correctness checks
#[pg_test]
fn test_randomized_mutation_sequence() {
    Spi::run("CREATE TABLE rnd_mut (id SERIAL PRIMARY KEY, grp TEXT NOT NULL, val INT NOT NULL)").expect("create");
    Spi::run(
        "INSERT INTO rnd_mut (grp, val) \
         SELECT 'g' || (i % 3), i * 11 % 50 \
         FROM generate_series(1, 50) i"
    ).expect("seed");

    let sql = "SELECT grp, SUM(val) AS s, COUNT(*) AS c, MIN(val) AS lo, MAX(val) AS hi FROM rnd_mut GROUP BY grp";
    let result = crate::create_reflex_ivm("rnd_mut_v", sql, None, None, None);
    assert!(!result.starts_with("ERROR"), "Failed to create: {}", result);
    assert_imv_correct("rnd_mut_v", sql);

    // 50 mixed mutations, check every 5th
    for i in 0..50 {
        match i % 3 {
            0 => {
                // Batch INSERT
                let ins_sql = format!(
                    "INSERT INTO rnd_mut (grp, val) VALUES ('g{}', {}), ('g{}', {})",
                    i % 3, (i * 7 + 3) % 100,
                    (i + 1) % 3, (i * 13 + 7) % 100
                );
                Spi::run(&ins_sql).expect("insert");
            }
            1 => {
                // DELETE some rows
                let del_sql = format!(
                    "DELETE FROM rnd_mut WHERE id IN (SELECT id FROM rnd_mut WHERE grp = 'g{}' LIMIT 1)",
                    i % 3
                );
                Spi::run(&del_sql).expect("delete");
            }
            _ => {
                // UPDATE some values
                let upd_sql = format!(
                    "UPDATE rnd_mut SET val = val + 1 WHERE grp = 'g{}'",
                    i % 3
                );
                Spi::run(&upd_sql).expect("update");
            }
        }

        // Check correctness every 5 mutations
        if (i + 1) % 5 == 0 {
            assert_imv_correct("rnd_mut_v", sql);
        }
    }

    // Final check
    assert_imv_correct("rnd_mut_v", sql);
}

// ========================================================================
// Group 10 — Count distinct
// ========================================================================

/// COUNT(DISTINCT val) — basic correctness with oracle
#[pg_test]
fn test_correctness_count_distinct_basic() {
    Spi::run("CREATE TABLE cd_src (id SERIAL PRIMARY KEY, grp TEXT NOT NULL, val INT NOT NULL)").expect("create");
    Spi::run("INSERT INTO cd_src (grp, val) VALUES ('a', 1), ('a', 1), ('a', 2), ('b', 3), ('b', 3), ('b', 3)").expect("seed");

    crate::create_reflex_ivm("cd_view",
        "SELECT grp, COUNT(DISTINCT val) AS cd FROM cd_src GROUP BY grp",
        None, None, None);

    let fresh = "SELECT grp, COUNT(DISTINCT val) AS cd FROM cd_src GROUP BY grp";
    assert_imv_correct("cd_view", fresh);
    // a: 2 distinct (1, 2), b: 1 distinct (3)

    // INSERT duplicate — cd should NOT change
    Spi::run("INSERT INTO cd_src (grp, val) VALUES ('a', 1)").expect("dup");
    assert_imv_correct("cd_view", fresh);

    // INSERT new distinct value
    Spi::run("INSERT INTO cd_src (grp, val) VALUES ('a', 99)").expect("new val");
    assert_imv_correct("cd_view", fresh);
    // a: 3 distinct (1, 2, 99)

    // DELETE one copy of a duplicated value — cd should NOT change
    Spi::run("DELETE FROM cd_src WHERE id = 1").expect("delete dup");
    assert_imv_correct("cd_view", fresh);

    // DELETE all copies of val=1 — cd decreases
    Spi::run("DELETE FROM cd_src WHERE val = 1").expect("delete all val=1");
    assert_imv_correct("cd_view", fresh);
    // a: 2 distinct (2, 99)
}

/// COUNT(DISTINCT) with UPDATE
#[pg_test]
fn test_correctness_count_distinct_update() {
    Spi::run("CREATE TABLE cdu_src (id SERIAL PRIMARY KEY, grp TEXT NOT NULL, val TEXT NOT NULL)").expect("create");
    Spi::run("INSERT INTO cdu_src (grp, val) VALUES ('x', 'a'), ('x', 'b'), ('x', 'c'), ('y', 'a')").expect("seed");

    crate::create_reflex_ivm("cdu_view",
        "SELECT grp, COUNT(DISTINCT val) AS cd FROM cdu_src GROUP BY grp",
        None, None, None);

    let fresh = "SELECT grp, COUNT(DISTINCT val) AS cd FROM cdu_src GROUP BY grp";
    assert_imv_correct("cdu_view", fresh);

    // UPDATE to existing value — cd should decrease
    Spi::run("UPDATE cdu_src SET val = 'a' WHERE val = 'b'").expect("update to dup");
    assert_imv_correct("cdu_view", fresh);
    // x: was {a,b,c}=3, now {a,a,c}=2

    // UPDATE to new value — cd should increase
    Spi::run("UPDATE cdu_src SET val = 'z' WHERE id = (SELECT MIN(id) FROM cdu_src WHERE val = 'a' AND grp = 'x')").expect("update to new");
    assert_imv_correct("cdu_view", fresh);
}

/// COUNT(DISTINCT) fuzz
#[pg_test]
fn test_fuzz_count_distinct() {
    Spi::run("SELECT setseed(0.63)").expect("seed");
    Spi::run("CREATE TABLE cd_fuzz (id SERIAL PRIMARY KEY, grp INT NOT NULL, val INT NOT NULL)").expect("create");
    Spi::run("INSERT INTO cd_fuzz (grp, val) SELECT (random()*5)::int, (random()*10)::int FROM generate_series(1, 200)").expect("seed data");

    crate::create_reflex_ivm("cd_fuzz_view",
        "SELECT grp, COUNT(DISTINCT val) AS cd FROM cd_fuzz GROUP BY grp",
        None, None, None);

    let fresh = "SELECT grp, COUNT(DISTINCT val) AS cd FROM cd_fuzz GROUP BY grp";
    assert_imv_correct("cd_fuzz_view", fresh);

    for _ in 0..15 {
        match Spi::get_one::<i32>("SELECT (random()*2)::int").expect("q").expect("v") {
            0 => Spi::run("INSERT INTO cd_fuzz (grp, val) VALUES ((random()*5)::int, (random()*10)::int)").expect("insert"),
            1 => Spi::run("DELETE FROM cd_fuzz WHERE id = (SELECT id FROM cd_fuzz ORDER BY random() LIMIT 1)").expect("delete"),
            _ => Spi::run("UPDATE cd_fuzz SET val = (random()*10)::int WHERE id = (SELECT id FROM cd_fuzz ORDER BY random() LIMIT 1)").expect("update"),
        };
        assert_imv_correct("cd_fuzz_view", fresh);
    }
}

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
        None, None, None, None);

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
        None, None, None, None);

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
        None, None, None, None);

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
        None, None, None, None);

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
        None, None, None, None);

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
        None, None, None, None);

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
        None, None, None, None);

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
        None, None, None, None);

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
        None, None, None, None);

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

    crate::create_reflex_ivm("cd1_view", "SELECT DISTINCT val FROM cd1", None, None, None, None);
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
        None, None, None, None);

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
        None, None, None, None);

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
        None, None, None, None);

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
        None, None, None, None);

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
        None, None, None, None);

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
        None, None, None, None);

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
        None, None, None, None);

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
        Some("id"), None, None, None);

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
        None, None, None, None);

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
        None, None, None, None);

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
        None, None, None, None);

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
        Some("id"), None, None, None);

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
        None, None, None, None);

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

    crate::create_reflex_ivm("du_view", "SELECT DISTINCT val FROM du", None, None, None, None);
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
        None, None, None, None);

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
        None, None, None, None);

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
        None, None, None, None);

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
        None, None, None, None);
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
        None, None, None, None);

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
        None, None, None, None);

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
        None, None, None, None);

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
        None, None, None, None);

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
        None, None, None, None);

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
        None, None, None, None);

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
        None, None, None, None);

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
        None, None, None, None);

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
        None, None, None, None);

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
        None, None, None, None);
    crate::create_reflex_ivm("m2_view",
        "SELECT grp, COUNT(*) AS cnt FROM msrc GROUP BY grp",
        None, None, None, None);
    crate::create_reflex_ivm("m3_view",
        "SELECT grp, AVG(val) AS avg_val FROM msrc GROUP BY grp",
        None, None, None, None);

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
        None, None, None, None);

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
        None, None, None, None);

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
        None, None, None, None);

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
        None, None, None, None);

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
        None, None, None, None);

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
        None, None, None, None);

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

// ========================================================================
// Group N — null_safe_in outer-column-scope bug regression (2026-05-13)
//
// Three integration tests catching the bug that almost shipped in 1.4.4:
//
//   1. test_aliased_group_by_no_full_refresh — IMV with `SELECT col AS alias`.
//      EXCEPT ALL oracle alone does NOT catch the bug (the buggy
//      DELETE-all + INSERT-all lands at the correct end state). We compare
//      `ctid` snapshots of the target before / after UPDATE: if every row's
//      ctid was rewritten, the trigger silently degenerated to full refresh.
//
//   2. test_plain_group_by_no_full_refresh — same shape, no aliasing — to
//      verify the bug fires even without aliasing (inner-scope-wins always
//      applies when outer and __a share names, which is always true).
//
//   3. test_aliased_group_by_delete_no_full_refresh — DELETE shape, same
//      property check on ctids.
// ========================================================================

/// Helper: count target rows whose ctid is unchanged between two snapshots.
/// After a correct incremental UPDATE most rows keep their ctid; after a
/// buggy full-refresh DELETE+INSERT every ctid changes.
fn count_unchanged_ctids(target: &str, snapshot: &str, key_expr: &str) -> i64 {
    let sql = format!(
        "SELECT COUNT(*) FROM {snapshot} s JOIN {target} t ON {key_expr} WHERE s.__snap_ctid = t.ctid"
    );
    Spi::get_one::<i64>(&sql)
        .expect("count_unchanged_ctids query failed")
        .expect("count returned NULL")
}

/// Aliased GROUP BY column (`SELECT dp.id AS dem_plan_id ...`). Target
/// carries `dem_plan_id`; intermediate / affected carry `id`. Pre-fix, the
/// generated EXISTS predicate was `"id" = __a."id"` — unqualified outer
/// resolves to inner `__a` scope, the DELETE FROM target degenerates to a
/// one-time TRUE filter, and every UPDATE rewrites the whole target.
#[pg_test]
fn pg_test_correctness_aliased_group_by_no_full_refresh() {
    Spi::run("CREATE TABLE alias_dp (id INT NOT NULL PRIMARY KEY, status TEXT NOT NULL)")
        .expect("create alias_dp");
    Spi::run("CREATE TABLE alias_ss (dp_id INT NOT NULL, sku INT NOT NULL, qty INT NOT NULL)")
        .expect("create alias_ss");
    Spi::run("INSERT INTO alias_dp SELECT i, 'validated' FROM generate_series(1, 10) i")
        .expect("seed dp");
    Spi::run(
        "INSERT INTO alias_ss SELECT (i % 10) + 1, (i % 50) + 1, (i % 100) + 1 \
         FROM generate_series(1, 5000) i",
    )
    .expect("seed ss");

    Spi::run(
        "SELECT create_reflex_ivm('alias_imv', \
         'SELECT dp.id AS dem_plan_id, ss.sku, SUM(ss.qty)::BIGINT AS total \
          FROM alias_ss ss JOIN alias_dp dp ON dp.id = ss.dp_id \
          WHERE dp.status = ''validated'' \
          GROUP BY dp.id, ss.sku')",
    )
    .expect("create alias_imv");

    let initial = Spi::get_one::<i64>("SELECT COUNT(*) FROM alias_imv")
        .expect("q")
        .expect("v");
    assert!(initial > 0, "IMV must be populated; got {}", initial);

    Spi::run("DROP TABLE IF EXISTS alias_imv_snapshot").expect("drop snapshot");
    Spi::run(
        "CREATE TABLE alias_imv_snapshot AS \
         SELECT dem_plan_id, sku, ctid AS __snap_ctid FROM alias_imv",
    )
    .expect("snapshot");

    Spi::run("UPDATE alias_dp SET status = 'validated' WHERE id = 1").expect("update");

    // End-state oracle — passes even when buggy.
    assert_imv_correct(
        "alias_imv",
        "SELECT dp.id AS dem_plan_id, ss.sku, SUM(ss.qty)::BIGINT AS total \
         FROM alias_ss ss JOIN alias_dp dp ON dp.id = ss.dp_id \
         WHERE dp.status = 'validated' \
         GROUP BY dp.id, ss.sku",
    );

    // Shape oracle: how many ctids survived the UPDATE? Incremental refresh
    // leaves all non-affected rows' ctids untouched. Buggy full refresh
    // rewrites every row → unchanged == 0.
    let unchanged = count_unchanged_ctids(
        "alias_imv",
        "alias_imv_snapshot",
        "s.dem_plan_id = t.dem_plan_id AND s.sku = t.sku",
    );
    let post_count = Spi::get_one::<i64>("SELECT COUNT(*) FROM alias_imv")
        .expect("post-q")
        .expect("v");

    // dp.id=1 × any sku that has a row → ~50 affected. The other ~450
    // groups must keep their ctid. Allow generous slack (100) for any
    // legitimate rewrites.
    assert!(
        unchanged >= post_count - 100,
        "UPDATE rewrote {} of {} target rows (unchanged={}). \
         This is the 2026-05-13 null_safe_in bug: the EXISTS predicate's \
         unqualified outer col resolved to inner __a scope, so DELETE FROM \
         target wiped the whole target on every UPDATE.",
        post_count - unchanged,
        post_count,
        unchanged
    );
}

/// Plain GROUP BY (no aliasing). Even when target_col == affected_col by
/// name, the unqualified outer ref still resolves to inner __a scope. This
/// test pins the fix for the non-aliased path.
#[pg_test]
fn pg_test_correctness_plain_group_by_no_full_refresh() {
    Spi::run("CREATE TABLE plain_src (id SERIAL, grp INT NOT NULL, val INT NOT NULL)")
        .expect("create plain_src");
    Spi::run("INSERT INTO plain_src (grp, val) SELECT i % 20, i FROM generate_series(1, 2000) i")
        .expect("seed");

    Spi::run(
        "SELECT create_reflex_ivm('plain_imv', \
         'SELECT grp, SUM(val)::BIGINT AS s FROM plain_src GROUP BY grp')",
    )
    .expect("create plain_imv");

    Spi::run("DROP TABLE IF EXISTS plain_imv_snapshot").expect("drop snapshot");
    Spi::run("CREATE TABLE plain_imv_snapshot AS SELECT grp, ctid AS __snap_ctid FROM plain_imv")
        .expect("snapshot");

    Spi::run("UPDATE plain_src SET val = val + 1 WHERE grp = 3").expect("update");

    assert_imv_correct(
        "plain_imv",
        "SELECT grp, SUM(val)::BIGINT AS s FROM plain_src GROUP BY grp",
    );

    let unchanged = count_unchanged_ctids("plain_imv", "plain_imv_snapshot", "s.grp = t.grp");
    let post_count = Spi::get_one::<i64>("SELECT COUNT(*) FROM plain_imv")
        .expect("post-q")
        .expect("v");
    // 1 group changed (grp=3), 19 others should keep their ctid.
    assert!(
        unchanged >= post_count - 2,
        "Plain-grouped UPDATE rewrote {} of {} rows (unchanged={}). \
         Expected ~1 row rewritten (the affected group). \
         null_safe_in outer-scope bug regression.",
        post_count - unchanged,
        post_count,
        unchanged
    );
}

/// DELETE shape on the source — same outer-scope bug surfaces here too
/// (the dead-row cleanup DELETE on intermediate and the DELETE-FROM-target
/// both go through the same buggy null_safe_in).
#[pg_test]
fn pg_test_correctness_aliased_group_by_delete_no_full_refresh() {
    Spi::run("CREATE TABLE del_src (id SERIAL, grp INT NOT NULL, val INT NOT NULL)")
        .expect("create del_src");
    Spi::run("INSERT INTO del_src (grp, val) SELECT i % 20, i FROM generate_series(1, 2000) i")
        .expect("seed");

    Spi::run(
        "SELECT create_reflex_ivm('del_imv', \
         'SELECT grp AS bucket, SUM(val)::BIGINT AS s FROM del_src GROUP BY grp')",
    )
    .expect("create del_imv");

    Spi::run("DROP TABLE IF EXISTS del_imv_snapshot").expect("drop snapshot");
    Spi::run("CREATE TABLE del_imv_snapshot AS SELECT bucket, ctid AS __snap_ctid FROM del_imv")
        .expect("snapshot");

    Spi::run("DELETE FROM del_src WHERE grp = 7").expect("delete");

    assert_imv_correct(
        "del_imv",
        "SELECT grp AS bucket, SUM(val)::BIGINT AS s FROM del_src GROUP BY grp",
    );

    let unchanged = count_unchanged_ctids("del_imv", "del_imv_snapshot", "s.bucket = t.bucket");
    let post_count = Spi::get_one::<i64>("SELECT COUNT(*) FROM del_imv")
        .expect("post-q")
        .expect("v");
    // bucket=7 was deleted, so post_count = 19 and unchanged should be 19.
    assert!(
        unchanged >= post_count,
        "Aliased-output DELETE rewrote {} of {} rows (unchanged={}). \
         Expected 0 rows rewritten (the affected group was deleted entirely from target). \
         null_safe_in outer-scope bug regression.",
        post_count - unchanged,
        post_count,
        unchanged
    );
}

/// Empty INSERT (INSERT ... SELECT ... WHERE FALSE) — 0 rows
#[pg_test]
fn test_correctness_empty_insert() {
    Spi::run("CREATE TABLE ei (id SERIAL, grp TEXT, val INT)").expect("create");
    Spi::run("INSERT INTO ei (grp, val) VALUES ('a', 10)").expect("seed");

    crate::create_reflex_ivm("ei_view",
        "SELECT grp, SUM(val) AS total FROM ei GROUP BY grp",
        None, None, None, None);

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
        Some("id"), None, None, None);

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
        None, None, None, None);

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
        None, None, None, None);

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
        None, None, None, None);

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
        None, None, None, None);

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
        None, None, None, None);

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
        None, None, None, None);

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
        None, None, None, None);

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
        crate::create_reflex_ivm(&view, &query, None, None, None, None);
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
        crate::create_reflex_ivm(&view, &query, None, None, None, None);
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
        crate::create_reflex_ivm(&view, &query, None, None, None, None);
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
        crate::create_reflex_ivm(&view, &query, None, None, None, None);
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
        crate::create_reflex_ivm(&view, &query, None, None, None, None);
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
        crate::create_reflex_ivm(&view, &query, None, None, None, None);
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
        crate::create_reflex_ivm(&view, &query, Some("id"), None, None, None);
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
        None, None, None, None);
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
        None, None, None, None);
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
        None, None, None, None);
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
        None, None, None, None);
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
        None, None, None, None);
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
        None, None, None, None);
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
        None, None, None, None);
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
        None, None, None, None);
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
        None, None, None, None);
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
        None, None, None, None);
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
        None, None, None, None);

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
        None, None, None, None);

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

// =============================================================================
// PS-5 — the target-sync affected-groups match is specialised at runtime: when
// the affected-groups table happens to hold no NULL key, codegen's sargable
// `=` form is used; when it holds one, the NULL-safe `IS NOT DISTINCT FROM`
// form is used. The specialisation is only sound because those two predicates
// agree whenever no affected key is NULL.
//
// THESE TESTS PIN THE SEMANTICS AND MUST NEVER BE WEAKENED. A group whose key
// is NULL must match only other NULL keys. An ungated `=` would silently leave
// the NULL group's stale target row in place and never re-insert it — verified
// directly in SQL: with target {1,2,NULL} and affected {NULL},
// `IS NOT DISTINCT FROM` selects the NULL row and `=` selects nothing.
// =============================================================================

/// The branch boundary itself: drive a nullable-key aggregate through deltas
/// that land the affected set on BOTH sides of the gate, and after every single
/// one require exact agreement with a full recompute.
///
/// Delta shapes exercised, in order:
///   1. affected = {non-NULL} only          -> sargable branch
///   2. affected = {NULL} only              -> NULL-safe branch (the killer case:
///                                             an ungated `=` matches nothing here)
///   3. affected = {NULL, non-NULL} mixed   -> NULL-safe branch, must still
///                                             maintain the non-NULL groups too
///   4. UPDATE moving a row non-NULL -> NULL and NULL -> non-NULL
///   5. deleting the NULL group down to empty (target row must disappear)
///   6. recreating the NULL group from empty (target row must reappear)
#[pg_test]
fn test_correctness_null_group_key_gate_branch_boundary() {
    Spi::run("CREATE TABLE ngkb (id SERIAL PRIMARY KEY, grp TEXT, val INT NOT NULL)")
        .expect("create");
    Spi::run(
        "INSERT INTO ngkb (grp, val) VALUES \
         ('a', 10), ('a', 20), (NULL, 30), (NULL, 40), ('b', 50)",
    )
    .expect("seed");

    crate::create_reflex_ivm(
        "ngkb_view",
        "SELECT grp, SUM(val) AS total, COUNT(*) AS cnt FROM ngkb GROUP BY grp",
        None,
        None,
        None,
        None,
    );
    let fresh = "SELECT grp, SUM(val) AS total, COUNT(*) AS cnt FROM ngkb GROUP BY grp";
    assert_imv_correct("ngkb_view", fresh);

    // 1. affected = {'a'} — no NULL key in the affected set: sargable branch.
    Spi::run("INSERT INTO ngkb (grp, val) VALUES ('a', 7)").expect("insert non-null");
    assert_imv_correct("ngkb_view", fresh);

    // 2. affected = {NULL} ONLY. The killer case. An ungated `=` join matches
    //    zero rows here, so the NULL group would keep its pre-delta total.
    Spi::run("INSERT INTO ngkb (grp, val) VALUES (NULL, 100)").expect("insert null only");
    assert_imv_correct("ngkb_view", fresh);
    let null_total = Spi::get_one::<i64>("SELECT total::BIGINT FROM ngkb_view WHERE grp IS NULL")
        .expect("null group query")
        .expect("null group row missing from IMV");
    assert_eq!(
        null_total, 170,
        "NULL group total must be 30+40+100=170 after a NULL-only delta; \
         a stale 70 means the affected-groups match failed to match the NULL key"
    );

    // 3. affected = {NULL, 'b'} mixed in ONE statement: the NULL-safe branch is
    //    taken, and it must still maintain the non-NULL group in the same set.
    Spi::run("INSERT INTO ngkb (grp, val) VALUES (NULL, 1), ('b', 2)").expect("insert mixed");
    assert_imv_correct("ngkb_view", fresh);

    // 4. UPDATEs across the NULL boundary, both directions.
    Spi::run("UPDATE ngkb SET grp = NULL WHERE grp = 'b'").expect("move to null");
    assert_imv_correct("ngkb_view", fresh);
    Spi::run("UPDATE ngkb SET grp = 'c' WHERE grp IS NULL AND val = 40").expect("move from null");
    assert_imv_correct("ngkb_view", fresh);

    // 5. Empty the NULL group entirely — the target row must be REMOVED, which
    //    is the DELETE half of the target sync matching a NULL key.
    Spi::run("DELETE FROM ngkb WHERE grp IS NULL").expect("empty null group");
    assert_imv_correct("ngkb_view", fresh);
    let remaining = Spi::get_one::<i64>("SELECT COUNT(*) FROM ngkb_view WHERE grp IS NULL")
        .expect("count query")
        .expect("count NULL");
    assert_eq!(
        remaining, 0,
        "the NULL group's target row must be deleted once its last source row is gone"
    );

    // 6. Recreate the NULL group from empty — the INSERT half.
    Spi::run("INSERT INTO ngkb (grp, val) VALUES (NULL, 500)").expect("recreate null group");
    assert_imv_correct("ngkb_view", fresh);
    let recreated = Spi::get_one::<i64>("SELECT total::BIGINT FROM ngkb_view WHERE grp IS NULL")
        .expect("null group query")
        .expect("NULL group row was not re-inserted after being emptied");
    assert_eq!(recreated, 500, "recreated NULL group must total 500");
}

/// Same branch boundary, but multi-column: the gate must consider EVERY key
/// column, so a NULL in the second column alone still has to force the
/// NULL-safe branch. Also covers a delta whose affected set contains a row that
/// is partially NULL — the case where per-column `=` would drop it.
#[pg_test]
fn test_correctness_null_multi_col_gate_branch_boundary() {
    Spi::run("CREATE TABLE nmkb (id SERIAL PRIMARY KEY, g1 TEXT, g2 INT, val INT NOT NULL)")
        .expect("create");
    Spi::run(
        "INSERT INTO nmkb (g1, g2, val) VALUES \
         ('a', 1, 10), ('a', NULL, 20), (NULL, 1, 30), (NULL, NULL, 40), ('b', 2, 50)",
    )
    .expect("seed");

    crate::create_reflex_ivm(
        "nmkb_view",
        "SELECT g1, g2, SUM(val) AS total, COUNT(*) AS cnt FROM nmkb GROUP BY g1, g2",
        None,
        None,
        None,
        None,
    );
    let fresh = "SELECT g1, g2, SUM(val) AS total, COUNT(*) AS cnt FROM nmkb GROUP BY g1, g2";
    assert_imv_correct("nmkb_view", fresh);

    // affected = {('a', 1)} — fully non-NULL: sargable branch.
    Spi::run("INSERT INTO nmkb (g1, g2, val) VALUES ('a', 1, 5)").expect("both non-null");
    assert_imv_correct("nmkb_view", fresh);

    // affected = {('a', NULL)} — NULL in the SECOND column only. The gate must
    // still fire; a first-column-only NULL check would wrongly take `=` and
    // leave this group stale.
    Spi::run("INSERT INTO nmkb (g1, g2, val) VALUES ('a', NULL, 6)").expect("second col null");
    assert_imv_correct("nmkb_view", fresh);
    let partial =
        Spi::get_one::<i64>("SELECT total::BIGINT FROM nmkb_view WHERE g1 = 'a' AND g2 IS NULL")
            .expect("partial-null group query")
            .expect("('a', NULL) group missing from IMV");
    assert_eq!(
        partial, 26,
        "('a', NULL) must total 20+6=26; a stale 20 means the NULL in g2 was not matched"
    );

    // affected = {(NULL, NULL)} — every key column NULL.
    Spi::run("INSERT INTO nmkb (g1, g2, val) VALUES (NULL, NULL, 7)").expect("all null");
    assert_imv_correct("nmkb_view", fresh);

    // affected = {(NULL, 1), ('b', 2)} — one partially-NULL key alongside a
    // fully non-NULL one, in a single statement.
    Spi::run("INSERT INTO nmkb (g1, g2, val) VALUES (NULL, 1, 8), ('b', 2, 9)")
        .expect("mixed nullness");
    assert_imv_correct("nmkb_view", fresh);

    // UPDATE that changes only the nullable second column.
    Spi::run("UPDATE nmkb SET g2 = NULL WHERE g1 = 'b'").expect("null the second col");
    assert_imv_correct("nmkb_view", fresh);

    // Empty an all-NULL group, then recreate it.
    Spi::run("DELETE FROM nmkb WHERE g1 IS NULL AND g2 IS NULL").expect("empty all-null group");
    assert_imv_correct("nmkb_view", fresh);
    Spi::run("INSERT INTO nmkb (g1, g2, val) VALUES (NULL, NULL, 11)").expect("recreate");
    assert_imv_correct("nmkb_view", fresh);
}

/// A NULL-keyed group must never be collateral damage of a delta that touches a
/// DIFFERENT group. This is the inverse failure mode: over-matching. If the
/// affected-groups match ever degenerated to a constant-true predicate (the
/// 2026-05-13 `null_safe_in` bug shape), the NULL group would be wiped and
/// rebuilt on every unrelated flush — correct by luck, but this pins that the
/// NULL group's row is genuinely untouched work.
#[pg_test]
fn test_correctness_null_group_untouched_by_unrelated_delta() {
    Spi::run("CREATE TABLE ngu (id SERIAL PRIMARY KEY, grp TEXT, val INT NOT NULL)")
        .expect("create");
    Spi::run("INSERT INTO ngu (grp, val) VALUES (NULL, 30), ('a', 10)").expect("seed");

    crate::create_reflex_ivm(
        "ngu_view",
        "SELECT grp, SUM(val) AS total FROM ngu GROUP BY grp",
        None,
        None,
        None,
        None,
    );
    let fresh = "SELECT grp, SUM(val) AS total FROM ngu GROUP BY grp";
    assert_imv_correct("ngu_view", fresh);

    // Delta on 'a' only. The NULL group must survive with its value intact.
    Spi::run("INSERT INTO ngu (grp, val) VALUES ('a', 5)").expect("unrelated delta");
    assert_imv_correct("ngu_view", fresh);
    let null_total = Spi::get_one::<i64>("SELECT total::BIGINT FROM ngu_view WHERE grp IS NULL")
        .expect("null group query")
        .expect("NULL group row vanished after a delta on a different group");
    assert_eq!(
        null_total, 30,
        "NULL group must be untouched by a delta on group 'a'"
    );
}

/// SUM(price * quantity) — expression inside aggregate
#[pg_test]
fn test_correctness_expression_in_aggregate() {
    Spi::run("CREATE TABLE expr_agg (id SERIAL, grp TEXT, price NUMERIC, qty INT)").expect("create");
    Spi::run("INSERT INTO expr_agg (grp, price, qty) VALUES ('a', 10.5, 2), ('a', 20.0, 3), ('b', 5.0, 10)").expect("seed");

    crate::create_reflex_ivm("expr_view",
        "SELECT grp, SUM(price * qty) AS revenue FROM expr_agg GROUP BY grp",
        None, None, None, None);
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
    crate::create_reflex_ivm("cc_bor_v", sql, None, None, None, None);
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
    crate::create_reflex_ivm("cc_mm_v", sql, None, None, None, None);
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
    crate::create_reflex_ivm("cc_avg1_v", sql, None, None, None, None);
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
    crate::create_reflex_ivm("cc_cnull_v", sql, None, None, None, None);
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
    crate::create_reflex_ivm("cc_sneg_v", sql, None, None, None, None);
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
    crate::create_reflex_ivm("cc_magg_v", sql, None, None, None, None);
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
    crate::create_reflex_ivm("cc_foj_v", sql, None, None, None, None);
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

/// 2026-07-25 ljgroup bug (untreated_bugs): FULL OUTER JOIN aggregate with a
/// GROUP BY on the PRIMARY side's bare column (not a COALESCE of both sides)
/// looked "stable" to the scoped-recompute heuristics — its qualifier isn't
/// the secondary table being mutated. But a FULL JOIN's unmatched-row
/// semantics mean an insert into the secondary with no matching primary row
/// surfaces a brand-new group the scoped recompute's membership predicate
/// (keyed on the mutated side's join value) never scopes into, silently
/// dropping the new group's row.
#[pg_test]
fn test_correctness_full_outer_join_aggregate_primary_group_by() {
    // `foja.k` and `fojb.kk` are deliberately DIFFERENT column names (not both
    // "k"): `plan.not_null_columns` is a bare-name set with no per-source
    // qualification, so a shared name across both join sides would make a
    // NOT NULL secondary key look like a NOT NULL primary key too, letting an
    // unrelated NULL-safety fallback (not this test's target property) also
    // turn RED under mutation and mask what's actually being pinned.
    Spi::run("CREATE TABLE foja (id INT PRIMARY KEY, k TEXT NOT NULL)").expect("create foja");
    Spi::run("CREATE TABLE fojb (kk TEXT PRIMARY KEY, w INT NOT NULL)").expect("create fojb");
    Spi::run("INSERT INTO foja VALUES (1, 'a'), (2, 'b')").expect("seed foja");
    Spi::run("INSERT INTO fojb VALUES ('a', 100)").expect("seed fojb");

    let sql = "SELECT foja.k AS k, COUNT(*) AS cnt \
               FROM foja FULL JOIN fojb ON foja.k = fojb.kk \
               GROUP BY foja.k";
    crate::create_reflex_ivm("foj_pg_v", sql, None, None, None, None);
    assert_imv_correct("foj_pg_v", sql);

    // Right-only insert: no matching foja row → a brand-new group ('c') that
    // the scoped-recompute heuristics (keyed on fojb's join value) must not
    // silently drop.
    Spi::run("INSERT INTO fojb VALUES ('c', 200)").expect("insert right-only");
    assert_imv_correct("foj_pg_v", sql);

    Spi::run("UPDATE fojb SET w = 999 WHERE kk = 'a'").expect("update");
    assert_imv_correct("foj_pg_v", sql);

    Spi::run("DELETE FROM fojb WHERE kk = 'c'").expect("delete");
    assert_imv_correct("foj_pg_v", sql);
}

/// Symmetric twin of the test above: GROUP BY is on the FULL JOIN's
/// SECONDARY side (`fojb.kk`) and the mutation lands on the PRIMARY side
/// (`foja`) instead. `secondary_ref_identifiers` only names the table the
/// trigger fired on, so a fast/fallback path keyed off "qualifier != mutated
/// table" would classify `fojb.kk` as stable for a `foja` mutation — but a
/// primary-side insert with no matching secondary row still surfaces a brand
/// new NULL group (`fojb.kk IS NULL`) that scoping on `fojb`'s own join value
/// can never reach.
#[pg_test]
fn test_correctness_full_outer_join_aggregate_secondary_group_by() {
    Spi::run("CREATE TABLE fojc (id INT PRIMARY KEY, k TEXT NOT NULL)").expect("create fojc");
    Spi::run("CREATE TABLE fojd (kk TEXT PRIMARY KEY, w INT NOT NULL)").expect("create fojd");
    Spi::run("INSERT INTO fojc VALUES (1, 'a')").expect("seed fojc");
    Spi::run("INSERT INTO fojd VALUES ('a', 100), ('b', 200)").expect("seed fojd");

    let sql = "SELECT fojd.kk AS kk, COUNT(*) AS cnt \
               FROM fojc FULL JOIN fojd ON fojc.k = fojd.kk \
               GROUP BY fojd.kk";
    crate::create_reflex_ivm("foj_sg_v", sql, None, None, None, None);
    assert_imv_correct("foj_sg_v", sql);

    // Left-only insert: no matching fojd row → a brand-new NULL group that a
    // scoped recompute keyed on fojc's own join value must not silently drop.
    Spi::run("INSERT INTO fojc VALUES (2, 'zz')").expect("insert left-only");
    assert_imv_correct("foj_sg_v", sql);

    Spi::run("UPDATE fojc SET k = 'b' WHERE id = 1").expect("update");
    assert_imv_correct("foj_sg_v", sql);

    Spi::run("DELETE FROM fojc WHERE id = 2").expect("delete");
    assert_imv_correct("foj_sg_v", sql);
}

/// CROSS JOIN with mutations
#[pg_test]
fn test_correctness_cross_join() {
    Spi::run("CREATE TABLE cc_cj1 (id INT PRIMARY KEY, x TEXT)").expect("create");
    Spi::run("CREATE TABLE cc_cj2 (id INT PRIMARY KEY, y TEXT)").expect("create");
    Spi::run("INSERT INTO cc_cj1 VALUES (1, 'a'), (2, 'b')").expect("seed1");
    Spi::run("INSERT INTO cc_cj2 VALUES (10, 'x'), (20, 'y')").expect("seed2");

    let sql = "SELECT cc_cj1.id AS l, cc_cj2.id AS r, x, y FROM cc_cj1 CROSS JOIN cc_cj2";
    crate::create_reflex_ivm("cc_cj_v", sql, Some("l, r"), None, None, None);
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
    crate::create_reflex_ivm("cc_sj_v", sql, None, None, None, None);
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
    crate::create_reflex_ivm("cc_cte3_v", sql, None, None, None, None);
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
    crate::create_reflex_ivm("cc_wex_v", sql, None, None, None, None);
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
    crate::create_reflex_ivm("cc_hav_v", sql, None, None, None, None);
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
    crate::create_reflex_ivm("cc_wlag_v", sql, None, None, None, None);
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
        let result = crate::create_reflex_ivm(name, sql, None, None, None, None);
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
    let result = crate::create_reflex_ivm("rnd_inner", inner_sql, None, None, None, None);
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
    let result = crate::create_reflex_ivm("rnd_left", left_sql, None, None, None, None);
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
    let result = crate::create_reflex_ivm("rnd_right", right_sql, None, None, None, None);
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
    let result = crate::create_reflex_ivm("rnd_mut_v", sql, None, None, None, None);
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
        None, None, None, None);

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

/// Bug #3 regression: COUNT(DISTINCT nullable_col) with NULL values.
/// The intermediate key includes `val`; the subtract MERGE must join on
/// (grp, val) using IS NOT DISTINCT FROM so NULL rows match and get
/// removed on DELETE — otherwise orphan counter rows accumulate and the
/// count stays stale.
#[pg_test]
fn test_correctness_count_distinct_nullable() {
    Spi::run("CREATE TABLE cdn_src (id SERIAL PRIMARY KEY, grp TEXT NOT NULL, val INT)").expect("create");
    Spi::run("INSERT INTO cdn_src (grp, val) VALUES ('a', 1), ('a', 2), ('a', NULL), ('a', NULL), ('b', NULL)").expect("seed");

    crate::create_reflex_ivm("cdn_view",
        "SELECT grp, COUNT(DISTINCT val) AS cd FROM cdn_src GROUP BY grp",
        None, None, None, None);

    let fresh = "SELECT grp, COUNT(DISTINCT val) AS cd FROM cdn_src GROUP BY grp";
    assert_imv_correct("cdn_view", fresh);
    // Postgres COUNT(DISTINCT val) ignores NULLs: a → 2, b → 0

    // INSERT another NULL row — count must stay the same (NULL not counted).
    Spi::run("INSERT INTO cdn_src (grp, val) VALUES ('a', NULL)").expect("null insert");
    assert_imv_correct("cdn_view", fresh);

    // DELETE one of the NULL rows — count must stay the same (NULL not counted),
    // but the intermediate compound key for (a, NULL) must match via
    // IS NOT DISTINCT FROM so no orphan row is left behind.
    Spi::run("DELETE FROM cdn_src WHERE id = 3").expect("delete null");
    assert_imv_correct("cdn_view", fresh);

    // DELETE all NULL rows for group a — still no change (NULL not counted).
    Spi::run("DELETE FROM cdn_src WHERE grp = 'a' AND val IS NULL").expect("delete all nulls");
    assert_imv_correct("cdn_view", fresh);

    // DELETE a real distinct value — count must decrement.
    Spi::run("DELETE FROM cdn_src WHERE grp = 'a' AND val = 1").expect("delete val=1");
    assert_imv_correct("cdn_view", fresh);
    // a: {2} → 1
}

/// COUNT(DISTINCT) with UPDATE
#[pg_test]
fn test_correctness_count_distinct_update() {
    Spi::run("CREATE TABLE cdu_src (id SERIAL PRIMARY KEY, grp TEXT NOT NULL, val TEXT NOT NULL)").expect("create");
    Spi::run("INSERT INTO cdu_src (grp, val) VALUES ('x', 'a'), ('x', 'b'), ('x', 'c'), ('y', 'a')").expect("seed");

    crate::create_reflex_ivm("cdu_view",
        "SELECT grp, COUNT(DISTINCT val) AS cd FROM cdu_src GROUP BY grp",
        None, None, None, None);

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
        None, None, None, None);

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

#[pg_test]
fn pg_test_topk_min_basic() {
    Spi::run("CREATE TABLE topk_min_src (id SERIAL, grp TEXT, val NUMERIC)")
        .expect("create");
    Spi::run("INSERT INTO topk_min_src (grp, val) VALUES \
              ('a', 5), ('a', 10), ('a', 15), ('a', 20), \
              ('b', 100), ('b', 50), ('b', 200)")
        .expect("seed");

    Spi::run("SELECT create_reflex_ivm('topk_min_v', \
              'SELECT grp, MIN(val) AS lo FROM topk_min_src GROUP BY grp', \
              NULL, NULL, NULL, 4)")
        .expect("create IMV with topk");

    let fresh = "SELECT grp, MIN(val) AS lo FROM topk_min_src GROUP BY grp";
    assert_imv_correct("topk_min_v", fresh);

    // Top-K column should be populated and sorted ascending.
    let topk_a: Vec<pgrx::AnyNumeric> = Spi::connect(|client| {
        client.select(
            "SELECT \"__min_val_topk\" AS t FROM __reflex_intermediate_topk_min_v WHERE grp = 'a'",
            None, &[],
        )
        .expect("q")
        .first()
        .get_by_name::<Vec<pgrx::AnyNumeric>, _>("t")
        .expect("get topk")
        .expect("topk not null")
    });
    assert_eq!(topk_a.len(), 4, "topk_a should have 4 elements");
    assert_eq!(topk_a[0].to_string(), "5");
    assert_eq!(topk_a[3].to_string(), "20");

    // Insert a new smaller value
    Spi::run("INSERT INTO topk_min_src (grp, val) VALUES ('a', 1)").expect("insert");
    assert_imv_correct("topk_min_v", fresh);

    // Delete the current min — top-K should yield the next-smallest from the heap
    Spi::run("DELETE FROM topk_min_src WHERE val = 1 AND grp = 'a'").expect("delete");
    assert_imv_correct("topk_min_v", fresh);

    // Delete enough to underflow the heap (group 'b' had 3 values, K=4 so heap has 3)
    Spi::run("DELETE FROM topk_min_src WHERE grp = 'b'").expect("delete all b");
    assert_imv_correct("topk_min_v", fresh);
}

#[pg_test]
fn pg_test_topk_max_basic() {
    Spi::run("CREATE TABLE topk_max_src (id SERIAL, grp TEXT, val NUMERIC)")
        .expect("create");
    Spi::run("INSERT INTO topk_max_src (grp, val) VALUES \
              ('a', 5), ('a', 10), ('a', 15), ('a', 20), ('a', 25), \
              ('b', 100), ('b', 50), ('b', 200)")
        .expect("seed");

    Spi::run("SELECT create_reflex_ivm('topk_max_v', \
              'SELECT grp, MAX(val) AS hi FROM topk_max_src GROUP BY grp', \
              NULL, NULL, NULL, 3)")
        .expect("create IMV with topk");

    let fresh = "SELECT grp, MAX(val) AS hi FROM topk_max_src GROUP BY grp";
    assert_imv_correct("topk_max_v", fresh);

    // Top-K should be sorted descending and capped at K=3
    let topk_a: Vec<pgrx::AnyNumeric> = Spi::connect(|client| {
        client.select(
            "SELECT \"__max_val_topk\" AS t FROM __reflex_intermediate_topk_max_v WHERE grp = 'a'",
            None, &[],
        )
        .expect("q")
        .first()
        .get_by_name::<Vec<pgrx::AnyNumeric>, _>("t")
        .expect("get topk")
        .expect("topk not null")
    });
    assert_eq!(topk_a.len(), 3, "topk should be capped at 3");
    assert_eq!(topk_a[0].to_string(), "25");
    assert_eq!(topk_a[2].to_string(), "15");

    // Delete the current max — should yield next-largest
    Spi::run("DELETE FROM topk_max_src WHERE val = 25").expect("delete max");
    assert_imv_correct("topk_max_v", fresh);

    // Delete to underflow heap
    Spi::run("DELETE FROM topk_max_src WHERE val IN (20, 15, 10) AND grp = 'a'")
        .expect("delete all but one");
    assert_imv_correct("topk_max_v", fresh);
}

#[pg_test]
fn pg_test_topk_fuzz_min() {
    Spi::run("SELECT setseed(0.42)").expect("seed");
    Spi::run("CREATE TABLE topk_fuzz (id SERIAL PRIMARY KEY, grp INT NOT NULL, val INT NOT NULL)")
        .expect("create");
    Spi::run("INSERT INTO topk_fuzz (grp, val) \
              SELECT (random()*4)::int, (random()*100)::int FROM generate_series(1, 50)")
        .expect("seed data");

    Spi::run("SELECT create_reflex_ivm('topk_fuzz_v', \
              'SELECT grp, MIN(val) AS lo, MAX(val) AS hi FROM topk_fuzz GROUP BY grp', \
              NULL, NULL, NULL, 8)")
        .expect("create");

    let fresh = "SELECT grp, MIN(val) AS lo, MAX(val) AS hi FROM topk_fuzz GROUP BY grp";
    assert_imv_correct("topk_fuzz_v", fresh);

    for _ in 0..30 {
        let op = Spi::get_one::<i32>("SELECT (random()*3)::int").expect("q").expect("v");
        match op {
            0 => Spi::run("INSERT INTO topk_fuzz (grp, val) VALUES ((random()*4)::int, (random()*100)::int)").expect("insert"),
            1 => Spi::run("DELETE FROM topk_fuzz WHERE id = (SELECT id FROM topk_fuzz ORDER BY random() LIMIT 1)").expect("delete"),
            _ => Spi::run("UPDATE topk_fuzz SET val = (random()*100)::int WHERE id = (SELECT id FROM topk_fuzz ORDER BY random() LIMIT 1)").expect("update"),
        };
        assert_imv_correct("topk_fuzz_v", fresh);
    }
}

#[pg_test]
fn pg_test_scalar_min_max_no_groupby() {
    // Audit unsupported §2: scalar MIN/MAX without GROUP BY (max_order_date_reflex case)
    Spi::run("CREATE TABLE scalar_mm_src (id SERIAL, val NUMERIC)").expect("create");
    Spi::run("INSERT INTO scalar_mm_src (val) VALUES (10), (20), (30), (40)")
        .expect("seed");

    crate::create_reflex_ivm(
        "scalar_mm_v",
        "SELECT MAX(val) AS hi, MIN(val) AS lo FROM scalar_mm_src",
        None, None, None,
        None,
    );

    let fresh = "SELECT MAX(val) AS hi, MIN(val) AS lo FROM scalar_mm_src";
    assert_imv_correct("scalar_mm_v", fresh);

    Spi::run("INSERT INTO scalar_mm_src (val) VALUES (100), (5)").expect("insert");
    assert_imv_correct("scalar_mm_v", fresh);

    // Delete the current MAX — recompute path scans source
    Spi::run("DELETE FROM scalar_mm_src WHERE val = 100").expect("delete max");
    assert_imv_correct("scalar_mm_v", fresh);

    // Delete the current MIN — recompute again
    Spi::run("DELETE FROM scalar_mm_src WHERE val = 5").expect("delete min");
    assert_imv_correct("scalar_mm_v", fresh);

    // Empty the table → MIN/MAX should be NULL
    Spi::run("DELETE FROM scalar_mm_src").expect("delete all");
    assert_imv_correct("scalar_mm_v", fresh);

    // Repopulate
    Spi::run("INSERT INTO scalar_mm_src (val) VALUES (7), (3), (11)").expect("repop");
    assert_imv_correct("scalar_mm_v", fresh);
}

#[pg_test]
fn pg_test_scalar_min_max_with_topk() {
    // Same as above but with top-K enabled — the heap path makes scalar
    // MIN/MAX retraction O(K) instead of O(N) when K is well-stocked.
    Spi::run("CREATE TABLE scalar_topk_src (id SERIAL, val NUMERIC)").expect("create");
    Spi::run("INSERT INTO scalar_topk_src (val) SELECT (random()*100)::int FROM generate_series(1, 30)")
        .expect("seed");

    Spi::run("SELECT create_reflex_ivm('scalar_topk_v', \
              'SELECT MAX(val) AS hi, MIN(val) AS lo FROM scalar_topk_src', \
              NULL, NULL, NULL, 8)")
        .expect("create with topk");

    let fresh = "SELECT MAX(val) AS hi, MIN(val) AS lo FROM scalar_topk_src";
    assert_imv_correct("scalar_topk_v", fresh);

    // Delete a few random rows including possibly the current extremum
    Spi::run("DELETE FROM scalar_topk_src WHERE id IN (SELECT id FROM scalar_topk_src ORDER BY random() LIMIT 5)")
        .expect("delete");
    assert_imv_correct("scalar_topk_v", fresh);
}

/// Top-K element type coverage: TEXT MIN/MAX with topk=K. The 1.3.0 schema
/// builder resolves the source-column type for the scalar; create_ivm.rs
/// (post-fix) propagates the same resolution onto IntermediateColumn.pg_type
/// so the trigger MERGE codegen emits the right `'{}'::TEXT[]` literal in
/// COALESCE on the array column. Without that propagation the MERGE fails
/// with "COALESCE could not convert type numeric[] to text[]".
#[pg_test]
fn pg_test_topk_text_min_max() {
    Spi::run("CREATE TABLE topk_txt_src (id SERIAL, grp TEXT, val TEXT)").expect("create");
    Spi::run(
        "INSERT INTO topk_txt_src (grp, val) VALUES \
         ('a', 'apple'), ('a', 'banana'), ('a', 'cherry'), ('a', 'date'), \
         ('b', 'fig'), ('b', 'grape'), ('b', 'honeydew')",
    )
    .expect("seed");

    Spi::run(
        "SELECT create_reflex_ivm('topk_txt_v', \
         'SELECT grp, MIN(val) AS lo, MAX(val) AS hi FROM topk_txt_src GROUP BY grp', \
         NULL, NULL, NULL, 4)",
    )
    .expect("create");

    let fresh = "SELECT grp, MIN(val) AS lo, MAX(val) AS hi FROM topk_txt_src GROUP BY grp";
    assert_imv_correct("topk_txt_v", fresh);

    // INSERT — heap merge through array_agg of TEXT
    Spi::run("INSERT INTO topk_txt_src (grp, val) VALUES ('a', 'avocado')").expect("insert");
    assert_imv_correct("topk_txt_v", fresh);

    // DELETE current min — multiset_subtract on text[]
    Spi::run("DELETE FROM topk_txt_src WHERE val = 'apple' AND grp = 'a'").expect("delete");
    assert_imv_correct("topk_txt_v", fresh);

    // UPDATE
    Spi::run("UPDATE topk_txt_src SET val = 'aardvark' WHERE val = 'avocado'").expect("update");
    assert_imv_correct("topk_txt_v", fresh);

    // Drain group 'b' to force heap underflow + recompute on text[]
    Spi::run("DELETE FROM topk_txt_src WHERE grp = 'b'").expect("drain b");
    assert_imv_correct("topk_txt_v", fresh);
}

/// Top-K element type coverage: DATE MIN/MAX. Same shape as TEXT but
/// exercises a different array element type to catch type-class assumptions
/// in COALESCE / array_agg / multiset_subtract.
#[pg_test]
fn pg_test_topk_date_min_max() {
    Spi::run("CREATE TABLE topk_date_src (id SERIAL, grp TEXT, dt DATE)").expect("create");
    Spi::run(
        "INSERT INTO topk_date_src (grp, dt) VALUES \
         ('a', '2024-01-15'), ('a', '2024-02-01'), ('a', '2024-03-10'), \
         ('a', '2024-06-30'), ('a', '2024-12-25'), \
         ('b', '2025-01-01'), ('b', '2025-07-04')",
    )
    .expect("seed");

    Spi::run(
        "SELECT create_reflex_ivm('topk_date_v', \
         'SELECT grp, MIN(dt) AS earliest, MAX(dt) AS latest FROM topk_date_src GROUP BY grp', \
         NULL, NULL, NULL, 4)",
    )
    .expect("create");

    let fresh =
        "SELECT grp, MIN(dt) AS earliest, MAX(dt) AS latest FROM topk_date_src GROUP BY grp";
    assert_imv_correct("topk_date_v", fresh);

    Spi::run("INSERT INTO topk_date_src (grp, dt) VALUES ('a', '2023-12-31')").expect("insert");
    assert_imv_correct("topk_date_v", fresh);

    Spi::run("DELETE FROM topk_date_src WHERE dt = '2023-12-31'").expect("delete");
    assert_imv_correct("topk_date_v", fresh);

    Spi::run("UPDATE topk_date_src SET dt = '2024-01-20' WHERE dt = '2024-01-15'").expect("update");
    assert_imv_correct("topk_date_v", fresh);

    Spi::run("DELETE FROM topk_date_src WHERE grp = 'b'").expect("drain b");
    assert_imv_correct("topk_date_v", fresh);
}

/// Regression for the partial-heap-staleness bug fixed 2026-04-26: an UPDATE
/// that retracts a heap element AND leaves unchanged source rows that were
/// never in the heap used to leave the heap in a non-empty-but-wrong state.
/// The next DELETE then read `heap[1]` as authoritative and produced a
/// wrong scalar. The fix forces a recompute after Sub+Add for every affected
/// top-K column. This test reproduces the minimal failing shape from
/// `journal/2026-04-26_topk_default_and_type_fix.md` and asserts the IMV
/// stays correct across two follow-on retractions.
#[pg_test]
fn pg_test_topk_partial_heap_staleness_regression() {
    Spi::run("CREATE TABLE topk_stale_src (id SERIAL PRIMARY KEY, grp TEXT, val NUMERIC)")
        .expect("create");
    // Source: 5 rows in one group, K=2. Heap will hold the 2 smallest;
    // 3 unchanged rows sit outside the heap waiting to be promoted.
    Spi::run(
        "INSERT INTO topk_stale_src (grp, val) VALUES \
         ('a', 1), ('a', 2), ('a', 3), ('a', 4), ('a', 5)",
    )
    .expect("seed");

    Spi::run(
        "SELECT create_reflex_ivm('topk_stale_v', \
         'SELECT grp, MIN(val) AS lo, MAX(val) AS hi FROM topk_stale_src GROUP BY grp', \
         NULL, NULL, NULL, 2)",
    )
    .expect("create");

    let fresh = "SELECT grp, MIN(val) AS lo, MAX(val) AS hi FROM topk_stale_src GROUP BY grp";
    assert_imv_correct("topk_stale_v", fresh);

    // UPDATE val=1 → val=10. delta_old removes heap[1]=1, delta_new adds 10.
    // Algebraic merge would land heap on [2, 10] — true top-2 is [2, 3].
    // Forced post-Add recompute corrects it to [2, 3].
    Spi::run("UPDATE topk_stale_src SET val = 10 WHERE val = 1").expect("update");
    assert_imv_correct("topk_stale_v", fresh);

    // Trigger statement that exposed the bug: DELETE val=2.
    // Pre-fix: heap was [2, 10] (stale), heap[1]=2 → DELETE leaves [10] →
    // scalar 10 ≠ true MIN 3. Post-fix: heap was [2, 3] → DELETE leaves [3] →
    // scalar 3 = true MIN. ✓
    Spi::run("DELETE FROM topk_stale_src WHERE val = 2").expect("delete");
    assert_imv_correct("topk_stale_v", fresh);

    // Repeat with MAX side: UPDATE val=5 → val=0 retracts heap[1] for MAX.
    Spi::run("UPDATE topk_stale_src SET val = 0 WHERE val = 5").expect("update max");
    assert_imv_correct("topk_stale_v", fresh);

    Spi::run("DELETE FROM topk_stale_src WHERE val = 4").expect("delete max heap");
    assert_imv_correct("topk_stale_v", fresh);
}

/// Top-K element type coverage: TIMESTAMP MIN/MAX. Catches the same class of
/// resolution gap as DATE/TEXT for sub-day-precision time columns.
#[pg_test]
fn pg_test_topk_timestamp_min_max() {
    Spi::run("CREATE TABLE topk_ts_src (id SERIAL, grp TEXT, ts TIMESTAMP)").expect("create");
    Spi::run(
        "INSERT INTO topk_ts_src (grp, ts) VALUES \
         ('a', '2026-01-01 09:00:00'), ('a', '2026-01-01 09:30:00'), \
         ('a', '2026-01-01 10:15:00'), ('a', '2026-01-01 11:00:00'), \
         ('b', '2026-01-02 08:00:00'), ('b', '2026-01-02 17:45:00')",
    )
    .expect("seed");

    Spi::run(
        "SELECT create_reflex_ivm('topk_ts_v', \
         'SELECT grp, MIN(ts) AS first_seen, MAX(ts) AS last_seen FROM topk_ts_src GROUP BY grp', \
         NULL, NULL, NULL, 4)",
    )
    .expect("create");

    let fresh =
        "SELECT grp, MIN(ts) AS first_seen, MAX(ts) AS last_seen FROM topk_ts_src GROUP BY grp";
    assert_imv_correct("topk_ts_v", fresh);

    Spi::run("INSERT INTO topk_ts_src (grp, ts) VALUES ('a', '2026-01-01 07:30:00')")
        .expect("insert");
    assert_imv_correct("topk_ts_v", fresh);

    Spi::run("DELETE FROM topk_ts_src WHERE ts = '2026-01-01 07:30:00'").expect("delete");
    assert_imv_correct("topk_ts_v", fresh);

    Spi::run("UPDATE topk_ts_src SET ts = '2026-01-01 12:00:00' WHERE ts = '2026-01-01 09:00:00'")
        .expect("update");
    assert_imv_correct("topk_ts_v", fresh);

    Spi::run("DELETE FROM topk_ts_src WHERE grp = 'b'").expect("drain b");
    assert_imv_correct("topk_ts_v", fresh);
}

/// N1 — heap-shrinkage gate, non-shrink path. UPDATE a row whose value sits
/// strictly outside the K smallest / K largest of its group. The pre-Sub heap
/// has K elements; Sub removes nothing (delta_old not in heap); post-Sub
/// cardinality stays at K. Add then merges delta_new and the heap is correct
/// without any source-scan recompute. Today (forced recompute) and after N1
/// (gated recompute, skipped here) must both yield the same answer.
#[pg_test]
fn pg_test_topk_update_no_heap_shrink_keeps_correctness() {
    Spi::run("CREATE TABLE topk_ns_src (id SERIAL PRIMARY KEY, grp TEXT, val NUMERIC)")
        .expect("create");
    // 100 rows, one group. K=4 → heap holds the 4 smallest (MIN) and 4 largest (MAX).
    // Most rows sit outside both heaps.
    Spi::run(
        "INSERT INTO topk_ns_src (grp, val) \
         SELECT 'a', i FROM generate_series(1, 100) i",
    )
    .expect("seed");

    Spi::run(
        "SELECT create_reflex_ivm('topk_ns_v', \
         'SELECT grp, MIN(val) AS lo, MAX(val) AS hi FROM topk_ns_src GROUP BY grp', \
         NULL, NULL, NULL, 4)",
    )
    .expect("create");

    let fresh = "SELECT grp, MIN(val) AS lo, MAX(val) AS hi FROM topk_ns_src GROUP BY grp";
    assert_imv_correct("topk_ns_v", fresh);

    // val=50 is strictly between heap_min (top-4 = [1..4]) and heap_max
    // (top-4 = [97..100]). Updating to val=60 keeps it outside both heaps.
    Spi::run("UPDATE topk_ns_src SET val = 60 WHERE val = 50").expect("update non-heap row");
    assert_imv_correct("topk_ns_v", fresh);

    // val=60 → val=55: still outside both heaps.
    Spi::run("UPDATE topk_ns_src SET val = 55 WHERE val = 60").expect("update non-heap row 2");
    assert_imv_correct("topk_ns_v", fresh);

    // Cross the heap boundary — UPDATE val=55 → val=2 promotes into MIN heap,
    // forcing the heap to shrink (cardinality=4, but Sub of 55 leaves it at 4
    // because 55 wasn't in heap; Add of 2 displaces 4). Correct via Add.
    // No shrink, no recompute: still the gated path. Heap = [1,2,2,3].
    Spi::run("UPDATE topk_ns_src SET val = 2 WHERE val = 55").expect("update into heap");
    assert_imv_correct("topk_ns_v", fresh);

    // True heap-eligible UPDATE — val=1 → val=200. Sub removes 1 (in heap),
    // post-Sub heap_min cardinality=3 < K=4. Gate fires; recompute reads
    // source and re-derives. Add of 200 enters heap_max.
    Spi::run("UPDATE topk_ns_src SET val = 200 WHERE val = 1").expect("update heap row");
    assert_imv_correct("topk_ns_v", fresh);
}

/// N1 — heap-shrinkage gate, mixed shrink/non-shrink groups within one
/// statement. A single UPDATE statement produces a delta covering two groups:
/// one whose heap shrinks (delta_old.val was in heap), one whose heap stays
/// at K (delta_old.val outside heap). Asserts both groups stay correct after
/// the trigger fires once.
#[pg_test]
fn pg_test_topk_update_mixed_shrink_groups() {
    Spi::run("CREATE TABLE topk_mix_src (id SERIAL PRIMARY KEY, grp TEXT, val NUMERIC)")
        .expect("create");
    // Group 'a': values 1..10 → heap_min = [1, 2] (K=2), heap_max = [10, 9].
    // Group 'b': values 1000..1009 → heap_min = [1000, 1001], heap_max = [1009, 1008].
    Spi::run(
        "INSERT INTO topk_mix_src (grp, val) \
         SELECT 'a', i FROM generate_series(1, 10) i \
         UNION ALL SELECT 'b', 1000+i FROM generate_series(0, 9) i",
    )
    .expect("seed");

    Spi::run(
        "SELECT create_reflex_ivm('topk_mix_v', \
         'SELECT grp, MIN(val) AS lo, MAX(val) AS hi FROM topk_mix_src GROUP BY grp', \
         NULL, NULL, NULL, 2)",
    )
    .expect("create");

    let fresh = "SELECT grp, MIN(val) AS lo, MAX(val) AS hi FROM topk_mix_src GROUP BY grp";
    assert_imv_correct("topk_mix_v", fresh);

    // Single UPDATE statement covering both groups:
    //   - Group 'a': val=1 → val=100. delta_old=1 (in heap_min), shrinks.
    //   - Group 'b': val=1005 → val=1006. delta_old=1005 (NOT in heap_min
    //     [1000,1001] nor heap_max [1009,1008]) — no shrink for 'b'.
    Spi::run(
        "UPDATE topk_mix_src SET val = CASE val WHEN 1 THEN 100 WHEN 1005 THEN 1006 END \
         WHERE val IN (1, 1005)",
    )
    .expect("mixed update");
    assert_imv_correct("topk_mix_v", fresh);

    // Follow up with a multi-row delete to flush the queued partial-heap state
    // for 'b' (mirrors `pg_test_topk_partial_heap_staleness_regression`'s
    // exposure pattern — a stale heap leaves no symptom until a retraction
    // reads heap[1]).
    Spi::run("DELETE FROM topk_mix_src WHERE val IN (2, 1001, 1009)").expect("drain edges");
    assert_imv_correct("topk_mix_v", fresh);
}

/// N1 — heap-shrinkage gate, multi-column top-K (MIN and MAX over the same
/// source column). The capture predicate is an OR over all top-K columns, so
/// a group whose MIN heap shrinks but MAX heap doesn't (or vice-versa) still
/// gets flagged and recomputed correctly. Both columns must stay in sync with
/// the post-update source.
#[pg_test]
fn pg_test_topk_update_multi_column_shrink() {
    Spi::run("CREATE TABLE topk_mc_src (id SERIAL PRIMARY KEY, grp TEXT, val NUMERIC)")
        .expect("create");
    // 10 rows in one group → heap_min = [1,2,3], heap_max = [10,9,8] (K=3).
    Spi::run(
        "INSERT INTO topk_mc_src (grp, val) \
         SELECT 'a', i FROM generate_series(1, 10) i",
    )
    .expect("seed");

    Spi::run(
        "SELECT create_reflex_ivm('topk_mc_v', \
         'SELECT grp, MIN(val) AS lo, MAX(val) AS hi FROM topk_mc_src GROUP BY grp', \
         NULL, NULL, NULL, 3)",
    )
    .expect("create");

    let fresh = "SELECT grp, MIN(val) AS lo, MAX(val) AS hi FROM topk_mc_src GROUP BY grp";
    assert_imv_correct("topk_mc_v", fresh);

    // val=2 is in heap_min (shrinks MIN side) but NOT in heap_max (no shrink
    // there). The OR-gated capture must still flag the group. After Add the
    // heap is potentially partial for MIN — recompute fixes it.
    Spi::run("UPDATE topk_mc_src SET val = 50 WHERE val = 2").expect("update min-side");
    assert_imv_correct("topk_mc_v", fresh);

    // Symmetric: val=9 is in heap_max but not heap_min. Shrinks MAX only.
    Spi::run("UPDATE topk_mc_src SET val = -5 WHERE val = 9").expect("update max-side");
    assert_imv_correct("topk_mc_v", fresh);

    // Neither shrinks: val=5 sits between heaps. Algebraic Sub+Add must give
    // the right answer with no recompute (the gate skips this).
    Spi::run("UPDATE topk_mc_src SET val = 6 WHERE val = 5").expect("update neither-heap");
    assert_imv_correct("topk_mc_v", fresh);

    // Both shrink: val=1 in heap_min AND val=10 in heap_max. Same statement.
    Spi::run(
        "UPDATE topk_mc_src SET val = CASE val WHEN 1 THEN 100 WHEN 10 THEN -100 END \
         WHERE val IN (1, 10)",
    )
    .expect("update both heaps");
    assert_imv_correct("topk_mc_v", fresh);

    // Follow-on retraction: like the partial-heap regression, a DELETE that
    // reads heap[1] would surface staleness if heap is wrong post-UPDATE.
    Spi::run("DELETE FROM topk_mc_src WHERE val IN (3, 8)").expect("drain heaps");
    assert_imv_correct("topk_mc_v", fresh);
}

/// Regression — a carried scalar group-key expression whose normalized name
/// exceeds Postgres's 63-char identifier limit. The intermediate column name and
/// the type-probe `column_types` key must agree after Postgres truncates both to
/// 63 chars; if codegen looks the column type up by the untruncated name it
/// misses and defaults to NUMERIC, so a boolean EXISTS fails to create with
/// "column ... is of type numeric but expression is of type boolean".
#[pg_test]
fn test_carried_exists_boolean_conjunct_long_name() {
    Spi::run("CREATE TABLE cebug_t(g INT PRIMARY KEY, v INT)").expect("t");
    Spi::run("CREATE TABLE cebug_products(product_id INT, is_active BOOL)").expect("pt");
    Spi::run("INSERT INTO cebug_t VALUES (1,10),(2,20),(3,30)").expect("seed t");
    Spi::run("INSERT INTO cebug_products VALUES (1,true),(3,false)").expect("seed pt");
    let body = "SELECT t.g, SUM(t.v) AS s, \
        EXISTS(SELECT 1 FROM cebug_products c WHERE c.product_id = t.g AND c.is_active) AS flag \
        FROM cebug_t t GROUP BY t.g";
    let r = crate::create_reflex_ivm("cebug_v", body, None, None, None, None);
    assert_eq!(r, "CREATE REFLEX INCREMENTAL VIEW", "create failed: {}", r);
    assert_imv_correct("cebug_v", body);
    Spi::run("INSERT INTO cebug_t VALUES (4,40)").expect("ins");
    Spi::run("INSERT INTO cebug_products VALUES (4,true)").expect("ins pt");
    assert_imv_correct("cebug_v", body);
}

/// Regression — same 63-char-truncation root cause, isolated from any boolean
/// term: a LONG carried EXISTS predicate built only from non-boolean comparisons.
/// Proves the trigger is the >63-char normalized name, not a boolean conjunct.
#[pg_test]
fn test_carried_exists_long_predicate_no_boolean() {
    Spi::run("CREATE TABLE celp_t(g INT PRIMARY KEY, v INT)").expect("t");
    Spi::run("CREATE TABLE celp_products(product_id INT, qty INT)").expect("pt");
    Spi::run("INSERT INTO celp_t VALUES (1,10),(2,20)").expect("seed t");
    Spi::run("INSERT INTO celp_products VALUES (1,5)").expect("seed pt");
    // No boolean column anywhere; predicate padded with numeric comparisons so
    // the normalized column name exceeds 63 chars.
    let body = "SELECT t.g, SUM(t.v) AS s, \
        EXISTS(SELECT 1 FROM celp_products c WHERE c.product_id = t.g AND c.qty <> -1 AND c.qty <> -2) AS flag \
        FROM celp_t t GROUP BY t.g";
    let r = crate::create_reflex_ivm("celp_v", body, None, None, None, None);
    assert_eq!(r, "CREATE REFLEX INCREMENTAL VIEW", "create failed: {}", r);
    assert_imv_correct("celp_v", body);
}


/// Regression — a GROUP BY key projected ONLY inside an expression (here
/// `GROUP BY a.sx` with `SELECT COALESCE(a.sx, 0)`) has no column in the result
/// table, so the target index and incremental refresh would reference a
/// nonexistent column (cryptic "column sx does not exist", or a crash on first
/// maintenance). Such a query is rejected up front with an actionable message.
#[pg_test]
fn test_unprojected_group_key_rejected() {
    Spi::run("CREATE TABLE jgw_t(g INT PRIMARY KEY, v INT)").expect("t");
    Spi::run("CREATE TABLE jgw_a(g INT, x INT)").expect("a");
    Spi::run("INSERT INTO jgw_t VALUES (1,10),(2,20)").expect("seed t");
    Spi::run("INSERT INTO jgw_a VALUES (1,5),(1,7)").expect("seed a");
    let body = "WITH agg AS (SELECT g, SUM(x) AS sx FROM jgw_a GROUP BY g) \
                SELECT t.g, SUM(t.v) AS s, COALESCE(a.sx, 0) AS sx0 \
                FROM jgw_t t LEFT JOIN agg a ON a.g = t.g GROUP BY t.g, a.sx";
    let r = crate::create_reflex_ivm("jgw_v", body, None, None, None, Some("g"));
    assert!(
        r.starts_with("ERROR: [reflex-unsupported] GROUP BY key 'a.sx' is not projected"),
        "expected clear unprojected-group-key error, got: {}",
        r
    );
    // No sub-IMV should leak from the rejected CTE decomposition.
    let leaked = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM public.__reflex_ivm_reference WHERE name LIKE 'jgw_v%'",
    )
    .expect("count")
    .unwrap_or(0);
    assert_eq!(leaked, 0, "rejected create leaked {} registry rows", leaked);
}

/// Control — projecting the GROUP BY key `a.sx` bare is NOT rejected (the
/// fail-fast guard targets only unprojected keys) and is correct at creation.
#[pg_test]
fn test_projected_joined_group_key_ok() {
    Spi::run("CREATE TABLE jgo_t(g INT PRIMARY KEY, v INT)").expect("t");
    Spi::run("CREATE TABLE jgo_a(g INT, x INT)").expect("a");
    Spi::run("INSERT INTO jgo_t VALUES (1,10),(2,20)").expect("seed t");
    Spi::run("INSERT INTO jgo_a VALUES (1,5),(1,7)").expect("seed a");
    let body = "WITH agg AS (SELECT g, SUM(x) AS sx FROM jgo_a GROUP BY g) \
                SELECT t.g, SUM(t.v) AS s, a.sx \
                FROM jgo_t t LEFT JOIN agg a ON a.g = t.g GROUP BY t.g, a.sx";
    let r = crate::create_reflex_ivm("jgo_v", body, None, None, None, Some("g"));
    assert_eq!(r, "CREATE REFLEX INCREMENTAL VIEW", "create failed: {}", r);
    assert_imv_correct("jgo_v", body);
}

/// Regression — COUNT(*) over a LEFT JOIN, INSERT into the secondary side.
/// The group key is from the primary table; the non-matching left rows must NOT
/// be re-added (which previously doubled COUNT(*)). SUM survives the old bug but
/// COUNT(*) did not.
#[pg_test]
fn test_left_join_secondary_insert_count() {
    Spi::run("CREATE TABLE ljc_l(grp TEXT PRIMARY KEY)").expect("l");
    Spi::run("CREATE TABLE ljc_r(grp TEXT, val INT)").expect("r");
    Spi::run("INSERT INTO ljc_l VALUES ('a'),('b'),('c')").expect("sl");
    Spi::run("INSERT INTO ljc_r VALUES ('a',10),('b',20)").expect("sr");
    let body = "SELECT l.grp, COUNT(r.val) AS n, COUNT(*) AS rows, SUM(r.val) AS tot \
                FROM ljc_l l LEFT JOIN ljc_r r ON l.grp = r.grp GROUP BY l.grp";
    let r = crate::create_reflex_ivm("ljc_v", body, None, None, None, Some("grp"));
    assert_eq!(r, "CREATE REFLEX INCREMENTAL VIEW", "create: {}", r);
    assert_imv_correct("ljc_v", body);
    Spi::run("INSERT INTO ljc_r VALUES ('c',50)").expect("sec-ins");
    assert_imv_correct("ljc_v", body);
    Spi::run("INSERT INTO ljc_r VALUES ('a',5)").expect("sec-ins2");
    assert_imv_correct("ljc_v", body);
}

/// Regression — a secondary-derived column used as a GROUP BY key, mutated via
/// INSERT/UPDATE/DELETE on the secondary base table. The group key migrates
/// (NULL<->value, value<->value), which must fully rebuild the affected
/// join-key's groups (no stale/phantom rows).
#[pg_test]
fn test_left_join_secondary_group_key_migration() {
    Spi::run("CREATE TABLE ljm_t(g INT PRIMARY KEY, v INT)").expect("t");
    Spi::run("CREATE TABLE ljm_a(g INT, x INT)").expect("a");
    Spi::run("INSERT INTO ljm_t VALUES (1,10),(2,20)").expect("st");
    Spi::run("INSERT INTO ljm_a VALUES (1,5),(2,4)").expect("sa");
    let body = "SELECT t.g, SUM(t.v) AS s, a.x AS ax \
                FROM ljm_t t LEFT JOIN ljm_a a ON a.g = t.g GROUP BY t.g, a.x";
    let r = crate::create_reflex_ivm("ljm_v", body, None, None, None, None);
    assert_eq!(r, "CREATE REFLEX INCREMENTAL VIEW", "create: {}", r);
    assert_imv_correct("ljm_v", body);
    Spi::run("UPDATE ljm_a SET x = 99 WHERE g = 1").expect("upd"); // ax migrates 5->99
    assert_imv_correct("ljm_v", body);
    Spi::run("DELETE FROM ljm_a WHERE g = 2").expect("del");       // ax migrates 4->NULL
    assert_imv_correct("ljm_v", body);
    Spi::run("INSERT INTO ljm_a VALUES (2,7)").expect("ins");      // ax migrates NULL->7
    assert_imv_correct("ljm_v", body);
}

/// Regression — the originally reported case: CTE-decomposed LEFT JOIN +
/// aggregate, joined sub-IMV key projected bare, INSERT into both base tables.
#[pg_test]
fn test_cte_left_join_cascade_maintenance() {
    Spi::run("CREATE TABLE clc_t(g INT PRIMARY KEY, v INT)").expect("t");
    Spi::run("CREATE TABLE clc_a(g INT, x INT)").expect("a");
    Spi::run("INSERT INTO clc_t VALUES (1,10),(2,20)").expect("st");
    Spi::run("INSERT INTO clc_a VALUES (1,5),(1,7)").expect("sa");
    let body = "WITH agg AS (SELECT g, SUM(x) AS sx FROM clc_a GROUP BY g) \
                SELECT t.g, SUM(t.v) AS s, a.sx \
                FROM clc_t t LEFT JOIN agg a ON a.g = t.g GROUP BY t.g, a.sx";
    let r = crate::create_reflex_ivm("clc_v", body, None, None, None, Some("g"));
    assert_eq!(r, "CREATE REFLEX INCREMENTAL VIEW", "create: {}", r);
    assert_imv_correct("clc_v", body);
    Spi::run("INSERT INTO clc_t VALUES (3,30)").expect("ins t");
    assert_imv_correct("clc_v", body);
    Spi::run("INSERT INTO clc_a VALUES (2,4)").expect("ins a2"); // creates agg g=2 (secondary insert)
    assert_imv_correct("clc_v", body);
    Spi::run("INSERT INTO clc_a VALUES (3,9)").expect("ins a3");
    assert_imv_correct("clc_v", body);
}

// =============================================================================
// PS-5 Part B — the intermediate MERGE and the MIN/MAX recompute are gated the
// same way as the target sync. The MERGE gate lives in the USING source, so the
// non-negotiable property is APPLY-EXACTLY-ONCE: the delta must be applied
// exactly once whether or not the scratch/affected set contains a NULL key.
// These are the idempotency tests my (wrong) "MERGE can't be gated" objection
// lacked. They must never be weakened.
// =============================================================================

/// SUM over a nullable group key, driven through INSERT/UPDATE/DELETE deltas
/// that land the SCRATCH set (the MERGE's probe target) on both sides of the
/// gate. The MERGE applies the delta exactly once in each case, so the IMV must
/// equal a full recompute at every step. A double-apply would show as a doubled
/// SUM; a zero-apply as a stale one.
#[pg_test]
fn test_correctness_merge_gate_apply_exactly_once() {
    Spi::run("CREATE TABLE mgo (id SERIAL PRIMARY KEY, grp TEXT, val INT NOT NULL)").expect("create");
    Spi::run("INSERT INTO mgo (grp, val) VALUES ('a', 10), (NULL, 30), (NULL, 40)").expect("seed");
    crate::create_reflex_ivm(
        "mgo_v",
        "SELECT grp, SUM(val) AS total, COUNT(*) AS cnt FROM mgo GROUP BY grp",
        None, None, None, None,
    );
    let fresh = "SELECT grp, SUM(val) AS total, COUNT(*) AS cnt FROM mgo GROUP BY grp";
    assert_imv_correct("mgo_v", fresh);

    // Delta touches ONLY non-NULL groups: scratch has no NULL key, FAST MERGE runs.
    Spi::run("INSERT INTO mgo (grp, val) VALUES ('a', 5), ('b', 1)").expect("non-null delta");
    assert_imv_correct("mgo_v", fresh);
    assert_eq!(
        Spi::get_one::<i64>("SELECT total::BIGINT FROM mgo_v WHERE grp='a'").unwrap().unwrap(),
        15, "group a must be 10+5=15 — not doubled (double-apply) nor stale (zero-apply)"
    );

    // Delta touches ONLY the NULL group: scratch has a NULL key, SAFE MERGE runs.
    Spi::run("INSERT INTO mgo (grp, val) VALUES (NULL, 100)").expect("null delta");
    assert_imv_correct("mgo_v", fresh);
    assert_eq!(
        Spi::get_one::<i64>("SELECT total::BIGINT FROM mgo_v WHERE grp IS NULL").unwrap().unwrap(),
        170, "NULL group must be 30+40+100=170 applied exactly once"
    );

    // Delta touches BOTH in one statement: scratch has a NULL key, SAFE runs and
    // must maintain the non-NULL group too.
    Spi::run("INSERT INTO mgo (grp, val) VALUES (NULL, 1), ('a', 2)").expect("mixed delta");
    assert_imv_correct("mgo_v", fresh);

    // UPDATE and DELETE across the boundary.
    Spi::run("UPDATE mgo SET grp = NULL WHERE grp = 'b'").expect("move to null");
    assert_imv_correct("mgo_v", fresh);
    Spi::run("DELETE FROM mgo WHERE grp IS NULL").expect("empty null group");
    assert_imv_correct("mgo_v", fresh);
    assert_eq!(
        Spi::get_one::<i64>("SELECT COUNT(*) FROM mgo_v WHERE grp IS NULL").unwrap().unwrap(),
        0, "NULL group target row must be gone once its source rows are"
    );
}

/// MIN/MAX over a nullable group key: exercises `build_min_max_recompute_sql`'s
/// gated recompute UPDATE and its gated EXISTS firing gate. A retraction (DELETE
/// of the current MIN) forces the recompute path, which must re-derive the MIN
/// for the affected group.
///
/// SCOPE: this pins the PS-5 join-gating (intermediate ⨝ __src and the EXISTS
/// firing gate now use sargable `=` for a NULL-free affected set). It deliberately
/// does NOT retract a NULL-keyed group's MIN, because that recompute path is
/// defeated by a SEPARATE pre-existing bug — the recompute's affected-scoping uses
/// NULL-unsafe `(cols) IN (SELECT ...)`, so a NULL group is dropped from the
/// scoped source and its MIN stays stale. That bug is filed in
/// `untreated_bugs/2026-07-23_min_max_recompute_scope_null_unsafe_in.md` and is
/// out of PS-5's scope (it is the scoping filter, not the join conditions PS-5
/// gates). The NULL group here is exercised on the algebraic (non-recompute) path,
/// which is correct.
#[pg_test]
fn test_correctness_min_max_recompute_gate_nullable_key() {
    Spi::run("CREATE TABLE mmr (id SERIAL PRIMARY KEY, grp TEXT, val INT NOT NULL)").expect("create");
    Spi::run("INSERT INTO mmr (grp, val) VALUES ('a', 5), ('a', 9), (NULL, 3), (NULL, 8)")
        .expect("seed");
    crate::create_reflex_ivm(
        "mmr_v",
        "SELECT grp, MIN(val) AS lo, MAX(val) AS hi FROM mmr GROUP BY grp",
        None, None, None, None,
    );
    let fresh = "SELECT grp, MIN(val) AS lo, MAX(val) AS hi FROM mmr GROUP BY grp";
    assert_imv_correct("mmr_v", fresh);

    // Retract the current MIN of a NON-NULL group: forces the recompute path, so
    // the gated `intermediate ⨝ __src` join and the gated EXISTS firing gate both
    // run with a NULL-free affected set (grp='a'), i.e. the sargable `=` variant.
    Spi::run("DELETE FROM mmr WHERE grp = 'a' AND val = 5").expect("retract a min");
    assert_imv_correct("mmr_v", fresh);
    assert_eq!(
        Spi::get_one::<i64>("SELECT lo::BIGINT FROM mmr_v WHERE grp = 'a'").unwrap().unwrap(),
        9, "group a MIN must recompute to 9 after 5 is retracted"
    );

    // The NULL group on the ALGEBRAIC path (Add, no recompute): a new lower value
    // must lower its MIN, and this must not corrupt it. Exercises the MERGE gate.
    Spi::run("INSERT INTO mmr (grp, val) VALUES (NULL, 1)").expect("new null-group min");
    assert_imv_correct("mmr_v", fresh);
    assert_eq!(
        Spi::get_one::<i64>("SELECT lo::BIGINT FROM mmr_v WHERE grp IS NULL").unwrap().unwrap(),
        1, "NULL group MIN must lower to 1 on the algebraic Add path"
    );

    // A non-NULL new MAX, algebraic.
    Spi::run("INSERT INTO mmr (grp, val) VALUES ('a', 42)").expect("new a max");
    assert_imv_correct("mmr_v", fresh);
}

/// PS-11: MIN/MAX recompute scoping must be NULL-safe. Retracting the current
/// MIN/MAX of a NULL-keyed group forces the recompute path, whose affected-group
/// scoping filter used `(cols) IN (SELECT ...)` — `(NULL) IN (...)` is never TRUE,
/// so the NULL group was dropped from the scoped source, its extremum never
/// re-derived, and the scalar left NULL by the retraction stayed NULL forever.
/// This is the report's exact repro (silent wrong result). It also covers both
/// MIN and MAX on the NULL group, and a non-NULL regression.
#[pg_test]
fn test_correctness_min_max_recompute_null_group_scope() {
    Spi::run("CREATE TABLE mmrn (id SERIAL PRIMARY KEY, grp TEXT, val INT NOT NULL)")
        .expect("create");
    Spi::run("INSERT INTO mmrn (grp, val) VALUES ('a', 5), ('a', 9), (NULL, 3), (NULL, 8), (NULL, 6)")
        .expect("seed");
    crate::create_reflex_ivm(
        "mmrn_v",
        "SELECT grp, MIN(val) AS lo, MAX(val) AS hi FROM mmrn GROUP BY grp",
        None, None, None, None,
    );
    let fresh = "SELECT grp, MIN(val) AS lo, MAX(val) AS hi FROM mmrn GROUP BY grp";
    assert_imv_correct("mmrn_v", fresh);

    // Retract the NULL group's current MIN (3): Sub sets __min = NULL, forcing the
    // recompute path scoped to the NULL group. With NULL-unsafe IN the NULL group
    // is dropped from the scoped source and lo stays NULL (bug). It must recompute
    // to the next-smallest survivor, 6.
    Spi::run("DELETE FROM mmrn WHERE grp IS NULL AND val = 3").expect("retract null-group min");
    assert_imv_correct("mmrn_v", fresh);
    assert_eq!(
        Spi::get_one::<i64>("SELECT lo::BIGINT FROM mmrn_v WHERE grp IS NULL").unwrap().unwrap(),
        6, "NULL group MIN must recompute to 6 after 3 is retracted"
    );

    // Retract the NULL group's current MAX (8): same recompute path for MAX.
    Spi::run("DELETE FROM mmrn WHERE grp IS NULL AND val = 8").expect("retract null-group max");
    assert_imv_correct("mmrn_v", fresh);
    assert_eq!(
        Spi::get_one::<i64>("SELECT hi::BIGINT FROM mmrn_v WHERE grp IS NULL").unwrap().unwrap(),
        6, "NULL group MAX must recompute to 6 after 8 is retracted"
    );

    // Non-NULL regression: retracting a non-NULL group's MIN still recomputes,
    // proving the common (sargable) path is untouched.
    Spi::run("DELETE FROM mmrn WHERE grp = 'a' AND val = 5").expect("retract a min");
    assert_imv_correct("mmrn_v", fresh);
    assert_eq!(
        Spi::get_one::<i64>("SELECT lo::BIGINT FROM mmrn_v WHERE grp = 'a'").unwrap().unwrap(),
        9, "group 'a' MIN must recompute to 9 after 5 is retracted"
    );
}

/// PS-11: multi-column group key with a NULL component. Pins the per-column
/// `IS NOT DISTINCT FROM` pairing of the NULL-safe scoping EXISTS: raw LHS column
/// must correspond to its own normalized RHS column. A swapped pairing silently
/// rescopes to the wrong groups (verified by self-mutation, see PROGRESS).
#[pg_test]
fn test_correctness_min_max_recompute_null_group_multicol() {
    Spi::run("CREATE TABLE mmrn2 (id SERIAL PRIMARY KEY, g1 TEXT, g2 TEXT, val INT NOT NULL)")
        .expect("create");
    Spi::run(
        "INSERT INTO mmrn2 (g1, g2, val) VALUES \
         ('x', NULL, 5), ('x', NULL, 9), ('y', 'z', 2), ('y', 'z', 7), (NULL, 'q', 4), (NULL, 'q', 1)",
    )
    .expect("seed");
    crate::create_reflex_ivm(
        "mmrn2_v",
        "SELECT g1, g2, MIN(val) AS lo, MAX(val) AS hi FROM mmrn2 GROUP BY g1, g2",
        None, None, None, None,
    );
    let fresh = "SELECT g1, g2, MIN(val) AS lo, MAX(val) AS hi FROM mmrn2 GROUP BY g1, g2";
    assert_imv_correct("mmrn2_v", fresh);

    // Retract the MIN of the ('x', NULL) group: NULL is in the SECOND key column.
    Spi::run("DELETE FROM mmrn2 WHERE g1 = 'x' AND g2 IS NULL AND val = 5")
        .expect("retract x/null min");
    assert_imv_correct("mmrn2_v", fresh);
    assert_eq!(
        Spi::get_one::<i64>("SELECT lo::BIGINT FROM mmrn2_v WHERE g1 = 'x' AND g2 IS NULL")
            .unwrap().unwrap(),
        9, "('x', NULL) group MIN must recompute to 9",
    );

    // Retract the MIN of the (NULL, 'q') group: NULL is in the FIRST key column.
    Spi::run("DELETE FROM mmrn2 WHERE g1 IS NULL AND g2 = 'q' AND val = 1")
        .expect("retract null/q min");
    assert_imv_correct("mmrn2_v", fresh);
    assert_eq!(
        Spi::get_one::<i64>("SELECT lo::BIGINT FROM mmrn2_v WHERE g1 IS NULL AND g2 = 'q'")
            .unwrap().unwrap(),
        4, "(NULL, 'q') group MIN must recompute to 4",
    );
}

/// PS-11: a NULL-keyed group emptied entirely (all its rows retracted) must have
/// its target row removed, not resurrected with a stale scalar by the recompute.
///
/// NOTE: this is a non-discriminating GUARD, not a RED->GREEN pin — it passes with
/// and without the scoping fix (the emptied-group path never enters the affected
/// scoping filter). It guards against a future regression that would resurrect an
/// emptied NULL group; the RED->GREEN pins for this fix are
/// `..._null_group_scope`, `..._null_group_multicol`, and
/// `..._topk_update_unshrunk_null_group`.
#[pg_test]
fn test_correctness_min_max_recompute_null_group_emptied() {
    Spi::run("CREATE TABLE mmrn3 (id SERIAL PRIMARY KEY, grp TEXT, val INT NOT NULL)")
        .expect("create");
    Spi::run("INSERT INTO mmrn3 (grp, val) VALUES ('a', 5), (NULL, 3), (NULL, 8)")
        .expect("seed");
    crate::create_reflex_ivm(
        "mmrn3_v",
        "SELECT grp, MIN(val) AS lo, MAX(val) AS hi FROM mmrn3 GROUP BY grp",
        None, None, None, None,
    );
    let fresh = "SELECT grp, MIN(val) AS lo, MAX(val) AS hi FROM mmrn3 GROUP BY grp";
    assert_imv_correct("mmrn3_v", fresh);

    Spi::run("DELETE FROM mmrn3 WHERE grp IS NULL").expect("empty null group");
    assert_imv_correct("mmrn3_v", fresh);
    assert_eq!(
        Spi::get_one::<i64>("SELECT COUNT(*) FROM mmrn3_v WHERE grp IS NULL").unwrap().unwrap(),
        0, "emptied NULL group must have no target row",
    );
}

/// PS-11 fix-round: top-K MIN/MAX over a NULLABLE group key, UPDATE of a
/// MIDDLE-ranked NULL-group row (rank between the min-heap and the max-heap, so
/// NEITHER heap shrinks). This is the reviewer's reproduced silent-wrong-result:
///
///   Sub sets the NULL group's scalar to NULL -> `build_topk_scalar_refresh_sql`
///   refreshed `scalar = topk[1]` for surviving heaps, but scoped with a
///   NULL-unsafe `(cols) IN (SELECT ...)` that DROPPED the NULL group -> scalar
///   stays NULL -> the Add computes LEAST/GREATEST(NULL, delta) = delta (wrong)
///   -> the forced recompute is scoped to `__reflex_shrunk_*`, empty here (no
///   heap shrank), so nothing re-derives it.
///
/// The DELETE-path recompute fix does NOT backstop this: the unshrunk NULL group
/// is not in the shrunk set. The topk scalar-refresh scoping itself must be
/// NULL-safe.
#[pg_test]
fn test_correctness_min_max_topk_update_unshrunk_null_group() {
    Spi::run("CREATE TABLE tkmm (id SERIAL PRIMARY KEY, grp TEXT, val INT NOT NULL)")
        .expect("create");
    // 40 distinct values per group: min-heap = {1..16}, max-heap = {25..40}.
    // A row valued ~20 is in NEITHER heap, so updating it shrinks no heap.
    Spi::run(
        "INSERT INTO tkmm (grp, val) \
         SELECT NULL, g FROM generate_series(1, 40) g \
         UNION ALL SELECT 'a', 100 + g FROM generate_series(1, 40) g",
    )
    .expect("seed");
    crate::create_reflex_ivm(
        "tkmm_v",
        "SELECT grp, MIN(val) AS lo, MAX(val) AS hi FROM tkmm GROUP BY grp",
        None, None, None, None,
    );
    let fresh = "SELECT grp, MIN(val) AS lo, MAX(val) AS hi FROM tkmm GROUP BY grp";
    assert_imv_correct("tkmm_v", fresh);

    // UPDATE a middle-ranked NULL-group row (20 -> 21): both remain in (16, 25),
    // so neither heap shrinks. True MIN stays 1, true MAX stays 40.
    Spi::run("UPDATE tkmm SET val = 21 WHERE grp IS NULL AND val = 20")
        .expect("middle update null group");
    assert_imv_correct("tkmm_v", fresh);
    assert_eq!(
        Spi::get_one::<i64>("SELECT lo::BIGINT FROM tkmm_v WHERE grp IS NULL").unwrap().unwrap(),
        1, "NULL group MIN must stay 1 after an unshrunk middle UPDATE",
    );
    assert_eq!(
        Spi::get_one::<i64>("SELECT hi::BIGINT FROM tkmm_v WHERE grp IS NULL").unwrap().unwrap(),
        40, "NULL group MAX must stay 40 after an unshrunk middle UPDATE",
    );

    // Non-NULL regression: the same unshrunk middle UPDATE on a NON-NULL group
    // must stay correct (proves the sargable common path is intact).
    Spi::run("UPDATE tkmm SET val = 121 WHERE grp = 'a' AND val = 120")
        .expect("middle update non-null group");
    assert_imv_correct("tkmm_v", fresh);
    assert_eq!(
        Spi::get_one::<i64>("SELECT lo::BIGINT FROM tkmm_v WHERE grp = 'a'").unwrap().unwrap(),
        101, "group 'a' MIN must stay 101",
    );
    assert_eq!(
        Spi::get_one::<i64>("SELECT hi::BIGINT FROM tkmm_v WHERE grp = 'a'").unwrap().unwrap(),
        140, "group 'a' MAX must stay 140",
    );
}

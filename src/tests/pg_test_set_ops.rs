
// ========================================================================
// UNION ALL tests
// ========================================================================

/// Basic UNION ALL of two tables — initial materialization
#[pg_test]
fn test_union_all_basic() {
    Spi::run("CREATE TABLE ua_eu (id SERIAL, city TEXT, amount NUMERIC)").expect("create");
    Spi::run("CREATE TABLE ua_us (id SERIAL, city TEXT, amount NUMERIC)").expect("create");
    Spi::run("INSERT INTO ua_eu (city, amount) VALUES ('Paris', 100), ('Berlin', 200)").expect("seed");
    Spi::run("INSERT INTO ua_us (city, amount) VALUES ('NYC', 300), ('LA', 400)").expect("seed");

    let result = crate::create_reflex_ivm(
        "ua_basic",
        "SELECT city, amount FROM ua_eu UNION ALL SELECT city, amount FROM ua_us",
        None, None, None,
    );
    assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");

    let count = Spi::get_one::<i64>("SELECT COUNT(*) FROM ua_basic")
        .expect("q").expect("v");
    assert_eq!(count, 4);

    // Verify all rows present
    let paris = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM ua_basic WHERE city = 'Paris'",
    ).expect("q").expect("v");
    assert_eq!(paris, 1);

    let nyc = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM ua_basic WHERE city = 'NYC'",
    ).expect("q").expect("v");
    assert_eq!(nyc, 1);
}

/// UNION ALL: INSERT into first source propagates to target
#[pg_test]
fn test_union_all_insert_source_a() {
    Spi::run("CREATE TABLE uaia_eu (id SERIAL, city TEXT, amount NUMERIC)").expect("create");
    Spi::run("CREATE TABLE uaia_us (id SERIAL, city TEXT, amount NUMERIC)").expect("create");
    Spi::run("INSERT INTO uaia_eu (city, amount) VALUES ('Paris', 100)").expect("seed");
    Spi::run("INSERT INTO uaia_us (city, amount) VALUES ('NYC', 200)").expect("seed");

    crate::create_reflex_ivm(
        "uaia_view",
        "SELECT city, amount FROM uaia_eu UNION ALL SELECT city, amount FROM uaia_us",
        None, None, None,
    );

    // INSERT into EU source → should appear in target
    Spi::run("INSERT INTO uaia_eu (city, amount) VALUES ('Berlin', 300)").expect("insert");
    let count = Spi::get_one::<i64>("SELECT COUNT(*) FROM uaia_view")
        .expect("q").expect("v");
    assert_eq!(count, 3);

    let berlin = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT amount FROM uaia_view WHERE city = 'Berlin'",
    ).expect("q").expect("v");
    assert_eq!(berlin.to_string(), "300");
}

/// UNION ALL: INSERT into second source propagates to target
#[pg_test]
fn test_union_all_insert_source_b() {
    Spi::run("CREATE TABLE uaib_eu (id SERIAL, city TEXT, amount NUMERIC)").expect("create");
    Spi::run("CREATE TABLE uaib_us (id SERIAL, city TEXT, amount NUMERIC)").expect("create");
    Spi::run("INSERT INTO uaib_eu (city, amount) VALUES ('Paris', 100)").expect("seed");
    Spi::run("INSERT INTO uaib_us (city, amount) VALUES ('NYC', 200)").expect("seed");

    crate::create_reflex_ivm(
        "uaib_view",
        "SELECT city, amount FROM uaib_eu UNION ALL SELECT city, amount FROM uaib_us",
        None, None, None,
    );

    // INSERT into US source
    Spi::run("INSERT INTO uaib_us (city, amount) VALUES ('LA', 400), ('Chicago', 500)").expect("insert");
    let count = Spi::get_one::<i64>("SELECT COUNT(*) FROM uaib_view")
        .expect("q").expect("v");
    assert_eq!(count, 4);
}

/// UNION ALL: DELETE from one source removes only those rows
#[pg_test]
fn test_union_all_delete() {
    Spi::run("CREATE TABLE uad_a (id SERIAL PRIMARY KEY, val TEXT)").expect("create");
    Spi::run("CREATE TABLE uad_b (id SERIAL PRIMARY KEY, val TEXT)").expect("create");
    Spi::run("INSERT INTO uad_a (val) VALUES ('x'), ('y')").expect("seed");
    Spi::run("INSERT INTO uad_b (val) VALUES ('z')").expect("seed");

    crate::create_reflex_ivm(
        "uad_view",
        "SELECT id, val FROM uad_a UNION ALL SELECT id, val FROM uad_b",
        None, None, None,
    );
    assert_eq!(
        Spi::get_one::<i64>("SELECT COUNT(*) FROM uad_view").expect("q").expect("v"),
        3
    );

    // DELETE from source A
    Spi::run("DELETE FROM uad_a WHERE val = 'x'").expect("delete");
    assert_eq!(
        Spi::get_one::<i64>("SELECT COUNT(*) FROM uad_view").expect("q").expect("v"),
        2
    );

    // Source B rows untouched
    assert_eq!(
        Spi::get_one::<i64>("SELECT COUNT(*) FROM uad_view WHERE val = 'z'").expect("q").expect("v"),
        1
    );
}

/// UNION ALL: UPDATE in one source reflected in target
#[pg_test]
fn test_union_all_update() {
    Spi::run("CREATE TABLE uau_a (id SERIAL PRIMARY KEY, val INT)").expect("create");
    Spi::run("CREATE TABLE uau_b (id SERIAL PRIMARY KEY, val INT)").expect("create");
    Spi::run("INSERT INTO uau_a (val) VALUES (10), (20)").expect("seed");
    Spi::run("INSERT INTO uau_b (val) VALUES (30)").expect("seed");

    crate::create_reflex_ivm(
        "uau_view",
        "SELECT id, val FROM uau_a UNION ALL SELECT id, val FROM uau_b",
        None, None, None,
    );

    Spi::run("UPDATE uau_a SET val = 99 WHERE val = 10").expect("update");
    let updated = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM uau_view WHERE val = 99",
    ).expect("q").expect("v");
    assert_eq!(updated, 1);

    // Old value gone
    assert_eq!(
        Spi::get_one::<i64>("SELECT COUNT(*) FROM uau_view WHERE val = 10").expect("q").expect("v"),
        0
    );
}

/// UNION ALL with 3 operands
#[pg_test]
fn test_union_all_three_operands() {
    Spi::run("CREATE TABLE ua3_a (id SERIAL, val TEXT)").expect("create");
    Spi::run("CREATE TABLE ua3_b (id SERIAL, val TEXT)").expect("create");
    Spi::run("CREATE TABLE ua3_c (id SERIAL, val TEXT)").expect("create");
    Spi::run("INSERT INTO ua3_a (val) VALUES ('a1'), ('a2')").expect("seed");
    Spi::run("INSERT INTO ua3_b (val) VALUES ('b1')").expect("seed");
    Spi::run("INSERT INTO ua3_c (val) VALUES ('c1'), ('c2'), ('c3')").expect("seed");

    let result = crate::create_reflex_ivm(
        "ua3_view",
        "SELECT val FROM ua3_a UNION ALL SELECT val FROM ua3_b UNION ALL SELECT val FROM ua3_c",
        None, None, None,
    );
    assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");

    assert_eq!(
        Spi::get_one::<i64>("SELECT COUNT(*) FROM ua3_view").expect("q").expect("v"),
        6
    );

    // INSERT into middle source
    Spi::run("INSERT INTO ua3_b (val) VALUES ('b2')").expect("insert");
    assert_eq!(
        Spi::get_one::<i64>("SELECT COUNT(*) FROM ua3_view").expect("q").expect("v"),
        7
    );
}

/// UNION ALL with aggregation in sub-queries
#[pg_test]
fn test_union_all_with_aggregates() {
    Spi::run("CREATE TABLE uaag_eu (id SERIAL, city TEXT, amount NUMERIC)").expect("create");
    Spi::run("CREATE TABLE uaag_us (id SERIAL, city TEXT, amount NUMERIC)").expect("create");
    Spi::run("INSERT INTO uaag_eu (city, amount) VALUES ('Paris', 100), ('Paris', 200), ('Berlin', 50)").expect("seed");
    Spi::run("INSERT INTO uaag_us (city, amount) VALUES ('NYC', 300), ('NYC', 100)").expect("seed");

    let result = crate::create_reflex_ivm(
        "uaag_view",
        "SELECT city, SUM(amount) AS total FROM uaag_eu GROUP BY city \
         UNION ALL \
         SELECT city, SUM(amount) AS total FROM uaag_us GROUP BY city",
        None, None, None,
    );
    assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");

    // Should have 3 rows: Paris(300), Berlin(50), NYC(400)
    assert_eq!(
        Spi::get_one::<i64>("SELECT COUNT(*) FROM uaag_view").expect("q").expect("v"),
        3
    );

    let paris = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT total FROM uaag_view WHERE city = 'Paris'",
    ).expect("q").expect("v");
    assert_eq!(paris.to_string(), "300");

    // INSERT into EU → Paris aggregate updates
    Spi::run("INSERT INTO uaag_eu (city, amount) VALUES ('Paris', 50)").expect("insert");
    let paris2 = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT total FROM uaag_view WHERE city = 'Paris'",
    ).expect("q").expect("v");
    assert_eq!(paris2.to_string(), "350");
}

/// UNION ALL with different WHERE clauses on the same table
#[pg_test]
fn test_union_all_same_table_different_filters() {
    Spi::run("CREATE TABLE uaf_src (id SERIAL, category TEXT, val INT)").expect("create");
    Spi::run("INSERT INTO uaf_src (category, val) VALUES \
        ('A', 10), ('A', 20), ('B', 30), ('B', 40), ('C', 50)").expect("seed");

    let result = crate::create_reflex_ivm(
        "uaf_view",
        "SELECT category, SUM(val) AS total FROM uaf_src WHERE category = 'A' GROUP BY category \
         UNION ALL \
         SELECT category, SUM(val) AS total FROM uaf_src WHERE category = 'B' GROUP BY category",
        None, None, None,
    );
    assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");

    // Only A and B, not C
    assert_eq!(
        Spi::get_one::<i64>("SELECT COUNT(*) FROM uaf_view").expect("q").expect("v"),
        2
    );

    // Insert a new A row
    Spi::run("INSERT INTO uaf_src (category, val) VALUES ('A', 5)").expect("insert");
    let a_total = Spi::get_one::<i64>(
        "SELECT total FROM uaf_view WHERE category = 'A'",
    ).expect("q").expect("v");
    assert_eq!(a_total, 35i64); // 10+20+5

    // Insert a C row — should NOT appear (filtered out by both operands)
    Spi::run("INSERT INTO uaf_src (category, val) VALUES ('C', 100)").expect("insert");
    assert_eq!(
        Spi::get_one::<i64>("SELECT COUNT(*) FROM uaf_view").expect("q").expect("v"),
        2
    );
}

/// UNION ALL: TRUNCATE one source clears its rows but not the other
#[pg_test]
fn test_union_all_truncate() {
    Spi::run("CREATE TABLE uat_a (id SERIAL, val TEXT)").expect("create");
    Spi::run("CREATE TABLE uat_b (id SERIAL, val TEXT)").expect("create");
    Spi::run("INSERT INTO uat_a (val) VALUES ('a1'), ('a2')").expect("seed");
    Spi::run("INSERT INTO uat_b (val) VALUES ('b1')").expect("seed");

    crate::create_reflex_ivm(
        "uat_view",
        "SELECT val FROM uat_a UNION ALL SELECT val FROM uat_b",
        None, None, None,
    );

    Spi::run("TRUNCATE uat_a").expect("truncate");
    // Only b1 remains
    assert_eq!(
        Spi::get_one::<i64>("SELECT COUNT(*) FROM uat_view").expect("q").expect("v"),
        1
    );
    assert_eq!(
        Spi::get_one::<i64>("SELECT COUNT(*) FROM uat_view WHERE val = 'b1'").expect("q").expect("v"),
        1
    );
}

// ========================================================================
// UNION (dedup) tests
// ========================================================================

/// Basic UNION dedup — duplicate rows across sources appear once
#[pg_test]
fn test_union_dedup_basic() {
    Spi::run("CREATE TABLE ud_a (id SERIAL, city TEXT)").expect("create");
    Spi::run("CREATE TABLE ud_b (id SERIAL, city TEXT)").expect("create");
    Spi::run("INSERT INTO ud_a (city) VALUES ('Paris'), ('Berlin')").expect("seed");
    Spi::run("INSERT INTO ud_b (city) VALUES ('Paris'), ('NYC')").expect("seed");

    let result = crate::create_reflex_ivm(
        "ud_basic",
        "SELECT city FROM ud_a UNION SELECT city FROM ud_b",
        None, None, None,
    );
    assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");

    // Paris appears in both, but UNION deduplicates → 3 distinct cities
    assert_eq!(
        Spi::get_one::<i64>("SELECT COUNT(*) FROM ud_basic").expect("q").expect("v"),
        3
    );
}

/// UNION dedup: delete from one source — row still visible from other source
#[pg_test]
fn test_union_dedup_delete_one_source() {
    Spi::run("CREATE TABLE udd_a (id SERIAL PRIMARY KEY, city TEXT)").expect("create");
    Spi::run("CREATE TABLE udd_b (id SERIAL PRIMARY KEY, city TEXT)").expect("create");
    Spi::run("INSERT INTO udd_a (city) VALUES ('Paris'), ('Berlin')").expect("seed");
    Spi::run("INSERT INTO udd_b (city) VALUES ('Paris'), ('NYC')").expect("seed");

    crate::create_reflex_ivm(
        "udd_view",
        "SELECT city FROM udd_a UNION SELECT city FROM udd_b",
        None, None, None,
    );

    // Delete Paris from source A — still visible via source B
    Spi::run("DELETE FROM udd_a WHERE city = 'Paris'").expect("delete");
    assert_eq!(
        Spi::get_one::<i64>("SELECT COUNT(*) FROM udd_view WHERE city = 'Paris'").expect("q").expect("v"),
        1,
        "Paris should still be visible via source B"
    );

    // Total: Berlin, Paris, NYC = 3
    assert_eq!(
        Spi::get_one::<i64>("SELECT COUNT(*) FROM udd_view").expect("q").expect("v"),
        3
    );
}

/// UNION dedup: delete from both sources — row disappears
#[pg_test]
fn test_union_dedup_delete_both_sources() {
    Spi::run("CREATE TABLE uddb_a (id SERIAL PRIMARY KEY, city TEXT)").expect("create");
    Spi::run("CREATE TABLE uddb_b (id SERIAL PRIMARY KEY, city TEXT)").expect("create");
    Spi::run("INSERT INTO uddb_a (city) VALUES ('Paris')").expect("seed");
    Spi::run("INSERT INTO uddb_b (city) VALUES ('Paris'), ('NYC')").expect("seed");

    crate::create_reflex_ivm(
        "uddb_view",
        "SELECT city FROM uddb_a UNION SELECT city FROM uddb_b",
        None, None, None,
    );

    // Delete Paris from A
    Spi::run("DELETE FROM uddb_a WHERE city = 'Paris'").expect("delete");
    assert_eq!(
        Spi::get_one::<i64>("SELECT COUNT(*) FROM uddb_view WHERE city = 'Paris'").expect("q").expect("v"),
        1, "Still visible from B"
    );

    // Delete Paris from B too
    Spi::run("DELETE FROM uddb_b WHERE city = 'Paris'").expect("delete");
    assert_eq!(
        Spi::get_one::<i64>("SELECT COUNT(*) FROM uddb_view WHERE city = 'Paris'").expect("q").expect("v"),
        0, "Gone from both sources"
    );

    // Only NYC remains
    assert_eq!(
        Spi::get_one::<i64>("SELECT COUNT(*) FROM uddb_view").expect("q").expect("v"),
        1
    );
}

/// UNION dedup: INSERT a duplicate — count stays the same
#[pg_test]
fn test_union_dedup_insert_duplicate() {
    Spi::run("CREATE TABLE udi_a (id SERIAL, city TEXT)").expect("create");
    Spi::run("CREATE TABLE udi_b (id SERIAL, city TEXT)").expect("create");
    Spi::run("INSERT INTO udi_a (city) VALUES ('Paris')").expect("seed");
    Spi::run("INSERT INTO udi_b (city) VALUES ('NYC')").expect("seed");

    crate::create_reflex_ivm(
        "udi_view",
        "SELECT city FROM udi_a UNION SELECT city FROM udi_b",
        None, None, None,
    );
    assert_eq!(
        Spi::get_one::<i64>("SELECT COUNT(*) FROM udi_view").expect("q").expect("v"),
        2
    );

    // Insert Paris into B — already exists via A, total stays 2
    Spi::run("INSERT INTO udi_b (city) VALUES ('Paris')").expect("insert dup");
    assert_eq!(
        Spi::get_one::<i64>("SELECT COUNT(*) FROM udi_view").expect("q").expect("v"),
        2, "UNION dedup: duplicate across sources should not add a row"
    );
}

/// UNION with aggregates in sub-queries
#[pg_test]
fn test_union_dedup_with_aggregates() {
    Spi::run("CREATE TABLE uda_eu (id SERIAL, region TEXT, amount NUMERIC)").expect("create");
    Spi::run("CREATE TABLE uda_us (id SERIAL, region TEXT, amount NUMERIC)").expect("create");
    Spi::run("INSERT INTO uda_eu (region, amount) VALUES ('West', 100), ('West', 200), ('East', 50)").expect("seed");
    Spi::run("INSERT INTO uda_us (region, amount) VALUES ('West', 300), ('South', 75)").expect("seed");

    let result = crate::create_reflex_ivm(
        "uda_view",
        "SELECT region, SUM(amount) AS total FROM uda_eu GROUP BY region \
         UNION \
         SELECT region, SUM(amount) AS total FROM uda_us GROUP BY region",
        None, None, None,
    );
    assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");

    // West appears in both with DIFFERENT totals (300 from EU, 300 from US)
    // UNION dedup on (region, total): EU-West=300, US-West=300 are same row → deduplicated
    // Result: (West, 300), (East, 50), (South, 75) = 3 rows
    assert_eq!(
        Spi::get_one::<i64>("SELECT COUNT(*) FROM uda_view").expect("q").expect("v"),
        3
    );
}

// ========================================================================
// INTERSECT / EXCEPT tests
// ========================================================================

/// Basic INTERSECT: only rows in both sources appear
#[pg_test]
fn test_intersect_basic() {
    Spi::run("CREATE TABLE ix_a (id SERIAL, city TEXT)").expect("create");
    Spi::run("CREATE TABLE ix_b (id SERIAL, city TEXT)").expect("create");
    Spi::run("INSERT INTO ix_a (city) VALUES ('Paris'), ('Berlin'), ('London')").expect("seed");
    Spi::run("INSERT INTO ix_b (city) VALUES ('Paris'), ('London'), ('NYC')").expect("seed");

    let result = crate::create_reflex_ivm(
        "ix_view",
        "SELECT city FROM ix_a INTERSECT SELECT city FROM ix_b",
        None, None, None,
    );
    assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");

    // Only Paris and London are in both
    assert_eq!(
        Spi::get_one::<i64>("SELECT COUNT(*) FROM ix_view").expect("q").expect("v"),
        2
    );
}

/// INTERSECT with trigger: INSERT into one source may add to result
#[pg_test]
fn test_intersect_insert_trigger() {
    Spi::run("CREATE TABLE ixi_a (id SERIAL, city TEXT)").expect("create");
    Spi::run("CREATE TABLE ixi_b (id SERIAL, city TEXT)").expect("create");
    Spi::run("INSERT INTO ixi_a (city) VALUES ('Paris')").expect("seed");
    Spi::run("INSERT INTO ixi_b (city) VALUES ('London')").expect("seed");

    crate::create_reflex_ivm(
        "ixi_view",
        "SELECT city FROM ixi_a INTERSECT SELECT city FROM ixi_b",
        None, None, None,
    );

    // No intersection initially
    assert_eq!(
        Spi::get_one::<i64>("SELECT COUNT(*) FROM ixi_view").expect("q").expect("v"),
        0
    );

    // Insert Paris into B → now Paris is in both
    Spi::run("INSERT INTO ixi_b (city) VALUES ('Paris')").expect("insert");
    assert_eq!(
        Spi::get_one::<i64>("SELECT COUNT(*) FROM ixi_view WHERE city = 'Paris'").expect("q").expect("v"),
        1
    );
}

/// Basic EXCEPT: rows in A but not in B
#[pg_test]
fn test_except_basic() {
    Spi::run("CREATE TABLE ex_a (id SERIAL, city TEXT)").expect("create");
    Spi::run("CREATE TABLE ex_b (id SERIAL, city TEXT)").expect("create");
    Spi::run("INSERT INTO ex_a (city) VALUES ('Paris'), ('Berlin'), ('London')").expect("seed");
    Spi::run("INSERT INTO ex_b (city) VALUES ('Paris'), ('London')").expect("seed");

    let result = crate::create_reflex_ivm(
        "ex_view",
        "SELECT city FROM ex_a EXCEPT SELECT city FROM ex_b",
        None, None, None,
    );
    assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");

    // Only Berlin is in A but not in B
    assert_eq!(
        Spi::get_one::<i64>("SELECT COUNT(*) FROM ex_view").expect("q").expect("v"),
        1
    );
    assert_eq!(
        Spi::get_one::<i64>("SELECT COUNT(*) FROM ex_view WHERE city = 'Berlin'").expect("q").expect("v"),
        1
    );
}

/// EXCEPT with trigger: DELETE from B adds to result
#[pg_test]
fn test_except_delete_trigger() {
    Spi::run("CREATE TABLE exd_a (id SERIAL, city TEXT)").expect("create");
    Spi::run("CREATE TABLE exd_b (id SERIAL PRIMARY KEY, city TEXT)").expect("create");
    Spi::run("INSERT INTO exd_a (city) VALUES ('Paris'), ('Berlin')").expect("seed");
    Spi::run("INSERT INTO exd_b (city) VALUES ('Paris'), ('Berlin')").expect("seed");

    crate::create_reflex_ivm(
        "exd_view",
        "SELECT city FROM exd_a EXCEPT SELECT city FROM exd_b",
        None, None, None,
    );

    // No rows initially (all in both)
    assert_eq!(
        Spi::get_one::<i64>("SELECT COUNT(*) FROM exd_view").expect("q").expect("v"),
        0
    );

    // Delete Paris from B → Paris now in A but not B
    Spi::run("DELETE FROM exd_b WHERE city = 'Paris'").expect("delete");
    assert_eq!(
        Spi::get_one::<i64>("SELECT COUNT(*) FROM exd_view WHERE city = 'Paris'").expect("q").expect("v"),
        1
    );
}

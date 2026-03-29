
#[pg_test]
fn test_window_function_accepted() {
    Spi::run("CREATE TABLE test_t3 (id INT, amount INT)").expect("create table");
    Spi::run("INSERT INTO test_t3 VALUES (1, 10), (1, 20), (2, 30)").expect("seed");
    let result = crate::create_reflex_ivm(
        "bad_view3",
        "SELECT id, amount, SUM(amount) OVER (PARTITION BY id) AS part_sum FROM test_t3",
        None,
        None,
        None,
    );
    assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");
}

// ========================================================================
// WINDOW function tests
// ========================================================================

/// Simple ROW_NUMBER() over entire result (no PARTITION BY)
#[pg_test]
fn test_window_row_number_no_partition() {
    Spi::run("CREATE TABLE wrn_src (id SERIAL, name TEXT, score INT)").expect("create");
    Spi::run("INSERT INTO wrn_src (name, score) VALUES \
        ('Alice', 90), ('Bob', 80), ('Charlie', 70)").expect("seed");

    let result = crate::create_reflex_ivm(
        "wrn_view",
        "SELECT name, score, ROW_NUMBER() OVER (ORDER BY score DESC) AS rnk FROM wrn_src",
        None, None, None,
    );
    assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");

    // Alice=1, Bob=2, Charlie=3
    let alice_rank = Spi::get_one::<i64>(
        "SELECT rnk FROM wrn_view WHERE name = 'Alice'",
    ).expect("q").expect("v");
    assert_eq!(alice_rank, 1);

    let charlie_rank = Spi::get_one::<i64>(
        "SELECT rnk FROM wrn_view WHERE name = 'Charlie'",
    ).expect("q").expect("v");
    assert_eq!(charlie_rank, 3);
}

/// ROW_NUMBER: INSERT changes rankings
#[pg_test]
fn test_window_row_number_insert_reranks() {
    Spi::run("CREATE TABLE wrni_src (id SERIAL, name TEXT, score INT)").expect("create");
    Spi::run("INSERT INTO wrni_src (name, score) VALUES \
        ('Alice', 90), ('Bob', 80)").expect("seed");

    crate::create_reflex_ivm(
        "wrni_view",
        "SELECT name, score, ROW_NUMBER() OVER (ORDER BY score DESC) AS rnk FROM wrni_src",
        None, None, None,
    );

    // Insert someone who beats Alice
    Spi::run("INSERT INTO wrni_src (name, score) VALUES ('Zara', 95)").expect("insert");

    let zara_rank = Spi::get_one::<i64>(
        "SELECT rnk FROM wrni_view WHERE name = 'Zara'",
    ).expect("q").expect("v");
    assert_eq!(zara_rank, 1, "Zara should be rank 1");

    let alice_rank = Spi::get_one::<i64>(
        "SELECT rnk FROM wrni_view WHERE name = 'Alice'",
    ).expect("q").expect("v");
    assert_eq!(alice_rank, 2, "Alice should drop to rank 2");

    let bob_rank = Spi::get_one::<i64>(
        "SELECT rnk FROM wrni_view WHERE name = 'Bob'",
    ).expect("q").expect("v");
    assert_eq!(bob_rank, 3, "Bob should drop to rank 3");
}

/// ROW_NUMBER: DELETE changes rankings
#[pg_test]
fn test_window_row_number_delete_reranks() {
    Spi::run("CREATE TABLE wrnd_src (id SERIAL PRIMARY KEY, name TEXT, score INT)").expect("create");
    Spi::run("INSERT INTO wrnd_src (name, score) VALUES \
        ('Alice', 90), ('Bob', 80), ('Charlie', 70)").expect("seed");

    crate::create_reflex_ivm(
        "wrnd_view",
        "SELECT name, score, ROW_NUMBER() OVER (ORDER BY score DESC) AS rnk FROM wrnd_src",
        None, None, None,
    );

    // Delete Alice (rank 1)
    Spi::run("DELETE FROM wrnd_src WHERE name = 'Alice'").expect("delete");

    // Bob promoted to rank 1
    let bob_rank = Spi::get_one::<i64>(
        "SELECT rnk FROM wrnd_view WHERE name = 'Bob'",
    ).expect("q").expect("v");
    assert_eq!(bob_rank, 1);

    // Charlie promoted to rank 2
    let charlie_rank = Spi::get_one::<i64>(
        "SELECT rnk FROM wrnd_view WHERE name = 'Charlie'",
    ).expect("q").expect("v");
    assert_eq!(charlie_rank, 2);

    assert_eq!(
        Spi::get_one::<i64>("SELECT COUNT(*) FROM wrnd_view").expect("q").expect("v"),
        2
    );
}

/// ROW_NUMBER with PARTITION BY — partitioned ranking
#[pg_test]
fn test_window_row_number_partition_by() {
    Spi::run("CREATE TABLE wrnp_src (id SERIAL, dept TEXT, name TEXT, score INT)").expect("create");
    Spi::run("INSERT INTO wrnp_src (dept, name, score) VALUES \
        ('eng', 'Alice', 90), ('eng', 'Bob', 80), ('eng', 'Charlie', 70), \
        ('sales', 'Dave', 95), ('sales', 'Eve', 85)").expect("seed");

    let result = crate::create_reflex_ivm(
        "wrnp_view",
        "SELECT dept, name, score, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY score DESC) AS rnk FROM wrnp_src",
        None, None, None,
    );
    assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");

    // eng: Alice=1, Bob=2, Charlie=3
    let alice_rank = Spi::get_one::<i64>(
        "SELECT rnk FROM wrnp_view WHERE name = 'Alice'",
    ).expect("q").expect("v");
    assert_eq!(alice_rank, 1);

    // sales: Dave=1, Eve=2
    let dave_rank = Spi::get_one::<i64>(
        "SELECT rnk FROM wrnp_view WHERE name = 'Dave'",
    ).expect("q").expect("v");
    assert_eq!(dave_rank, 1);

    assert_eq!(
        Spi::get_one::<i64>("SELECT COUNT(*) FROM wrnp_view").expect("q").expect("v"),
        5
    );
}

/// PARTITION BY: INSERT affects only the partition, not other partitions
#[pg_test]
fn test_window_partition_insert_isolation() {
    Spi::run("CREATE TABLE wpi_src (id SERIAL, dept TEXT, name TEXT, score INT)").expect("create");
    Spi::run("INSERT INTO wpi_src (dept, name, score) VALUES \
        ('eng', 'Alice', 90), ('eng', 'Bob', 80), \
        ('sales', 'Dave', 95)").expect("seed");

    crate::create_reflex_ivm(
        "wpi_view",
        "SELECT dept, name, score, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY score DESC) AS rnk FROM wpi_src",
        None, None, None,
    );

    // Insert into eng partition — sales should be unaffected
    Spi::run("INSERT INTO wpi_src (dept, name, score) VALUES ('eng', 'Zara', 95)").expect("insert");

    // eng: Zara=1, Alice=2, Bob=3
    let zara = Spi::get_one::<i64>(
        "SELECT rnk FROM wpi_view WHERE name = 'Zara'",
    ).expect("q").expect("v");
    assert_eq!(zara, 1);

    let alice = Spi::get_one::<i64>(
        "SELECT rnk FROM wpi_view WHERE name = 'Alice'",
    ).expect("q").expect("v");
    assert_eq!(alice, 2);

    // sales: Dave still rank 1 (unaffected)
    let dave = Spi::get_one::<i64>(
        "SELECT rnk FROM wpi_view WHERE name = 'Dave'",
    ).expect("q").expect("v");
    assert_eq!(dave, 1);
}

/// RANK() with ties
#[pg_test]
fn test_window_rank_with_ties() {
    Spi::run("CREATE TABLE wr_src (id SERIAL, name TEXT, score INT)").expect("create");
    Spi::run("INSERT INTO wr_src (name, score) VALUES \
        ('Alice', 90), ('Bob', 90), ('Charlie', 80)").expect("seed");

    let result = crate::create_reflex_ivm(
        "wr_view",
        "SELECT name, score, RANK() OVER (ORDER BY score DESC) AS rnk FROM wr_src",
        None, None, None,
    );
    assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");

    // Alice and Bob tied at rank 1, Charlie at rank 3 (not 2)
    let alice = Spi::get_one::<i64>(
        "SELECT rnk FROM wr_view WHERE name = 'Alice'",
    ).expect("q").expect("v");
    assert_eq!(alice, 1);

    let bob = Spi::get_one::<i64>(
        "SELECT rnk FROM wr_view WHERE name = 'Bob'",
    ).expect("q").expect("v");
    assert_eq!(bob, 1);

    let charlie = Spi::get_one::<i64>(
        "SELECT rnk FROM wr_view WHERE name = 'Charlie'",
    ).expect("q").expect("v");
    assert_eq!(charlie, 3); // RANK skips 2
}

/// DENSE_RANK() — no gaps after ties
#[pg_test]
fn test_window_dense_rank() {
    Spi::run("CREATE TABLE wdr_src (id SERIAL, name TEXT, score INT)").expect("create");
    Spi::run("INSERT INTO wdr_src (name, score) VALUES \
        ('Alice', 90), ('Bob', 90), ('Charlie', 80)").expect("seed");

    let result = crate::create_reflex_ivm(
        "wdr_view",
        "SELECT name, score, DENSE_RANK() OVER (ORDER BY score DESC) AS rnk FROM wdr_src",
        None, None, None,
    );
    assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");

    let charlie = Spi::get_one::<i64>(
        "SELECT rnk FROM wdr_view WHERE name = 'Charlie'",
    ).expect("q").expect("v");
    assert_eq!(charlie, 2); // DENSE_RANK: 1,1,2 not 1,1,3
}

/// SUM() OVER (PARTITION BY) — running/partition aggregate via window
#[pg_test]
fn test_window_sum_partition() {
    Spi::run("CREATE TABLE wsp_src (id SERIAL, dept TEXT, amount NUMERIC)").expect("create");
    Spi::run("INSERT INTO wsp_src (dept, amount) VALUES \
        ('eng', 100), ('eng', 200), ('sales', 50), ('sales', 75)").expect("seed");

    let result = crate::create_reflex_ivm(
        "wsp_view",
        "SELECT dept, amount, SUM(amount) OVER (PARTITION BY dept) AS dept_total FROM wsp_src",
        None, None, None,
    );
    assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");

    // All eng rows should show dept_total = 300
    let eng_totals: Vec<String> = Spi::connect(|client| {
        client
            .select("SELECT dept_total::text FROM wsp_view WHERE dept = 'eng' ORDER BY amount", None, &[])
            .unwrap()
            .map(|row| row.get_by_name::<&str, _>("dept_total").unwrap().unwrap().to_string())
            .collect()
    });
    assert_eq!(eng_totals, vec!["300", "300"]);

    // Insert into eng → dept_total should update for ALL eng rows
    Spi::run("INSERT INTO wsp_src (dept, amount) VALUES ('eng', 50)").expect("insert");
    let eng_total = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT DISTINCT dept_total FROM wsp_view WHERE dept = 'eng'",
    ).expect("q").expect("v");
    assert_eq!(eng_total.to_string(), "350");
}

/// LAG() — access previous row in window
#[pg_test]
fn test_window_lag() {
    Spi::run("CREATE TABLE wl_src (id SERIAL, ts INT, val INT)").expect("create");
    Spi::run("INSERT INTO wl_src (ts, val) VALUES (1, 10), (2, 20), (3, 30)").expect("seed");

    let result = crate::create_reflex_ivm(
        "wl_view",
        "SELECT ts, val, LAG(val) OVER (ORDER BY ts) AS prev_val FROM wl_src",
        None, None, None,
    );
    assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");

    // ts=1 → prev_val = NULL; ts=2 → prev_val = 10; ts=3 → prev_val = 20
    let prev_at_2 = Spi::get_one::<i32>(
        "SELECT prev_val FROM wl_view WHERE ts = 2",
    ).expect("q").expect("v");
    assert_eq!(prev_at_2, 10);

    let prev_at_3 = Spi::get_one::<i32>(
        "SELECT prev_val FROM wl_view WHERE ts = 3",
    ).expect("q").expect("v");
    assert_eq!(prev_at_3, 20);

    // Insert ts=0 → shifts everything
    Spi::run("INSERT INTO wl_src (ts, val) VALUES (0, 5)").expect("insert");
    let prev_at_1 = Spi::get_one::<i32>(
        "SELECT prev_val FROM wl_view WHERE ts = 1",
    ).expect("q").expect("v");
    assert_eq!(prev_at_1, 5, "ts=1 should now lag ts=0 (val=5)");
}

/// LEAD() — access next row in window
#[pg_test]
fn test_window_lead() {
    Spi::run("CREATE TABLE wld_src (id SERIAL, ts INT, val INT)").expect("create");
    Spi::run("INSERT INTO wld_src (ts, val) VALUES (1, 10), (2, 20), (3, 30)").expect("seed");

    let result = crate::create_reflex_ivm(
        "wld_view",
        "SELECT ts, val, LEAD(val) OVER (ORDER BY ts) AS next_val FROM wld_src",
        None, None, None,
    );
    assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");

    let next_at_1 = Spi::get_one::<i32>(
        "SELECT next_val FROM wld_view WHERE ts = 1",
    ).expect("q").expect("v");
    assert_eq!(next_at_1, 20);

    // ts=3 has no next → NULL
    let next_at_3_is_null = Spi::get_one::<bool>(
        "SELECT next_val IS NULL FROM wld_view WHERE ts = 3",
    ).expect("q").expect("v");
    assert!(next_at_3_is_null, "Last row should have NULL next_val");
}

/// GROUP BY + WINDOW: Aggregates maintained incrementally,
/// window recomputed over intermediate result
#[pg_test]
fn test_window_group_by_plus_window() {
    Spi::run("CREATE TABLE wgw_src (id SERIAL, city TEXT, amount NUMERIC)").expect("create");
    Spi::run("INSERT INTO wgw_src (city, amount) VALUES \
        ('Paris', 100), ('Paris', 200), ('London', 50), ('Berlin', 300)").expect("seed");

    let result = crate::create_reflex_ivm(
        "wgw_view",
        "SELECT city, SUM(amount) AS total, \
                RANK() OVER (ORDER BY SUM(amount) DESC) AS rnk \
         FROM wgw_src GROUP BY city",
        None, None, None,
    );
    assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");

    // Berlin=300 rank 1, Paris=300 rank 1, London=50 rank 3
    let berlin_rank = Spi::get_one::<i64>(
        "SELECT rnk FROM wgw_view WHERE city = 'Berlin'",
    ).expect("q").expect("v");
    assert_eq!(berlin_rank, 1);

    let paris_rank = Spi::get_one::<i64>(
        "SELECT rnk FROM wgw_view WHERE city = 'Paris'",
    ).expect("q").expect("v");
    assert_eq!(paris_rank, 1); // Tied with Berlin

    let london_rank = Spi::get_one::<i64>(
        "SELECT rnk FROM wgw_view WHERE city = 'London'",
    ).expect("q").expect("v");
    assert_eq!(london_rank, 3);
}

/// GROUP BY + WINDOW: INSERT changes aggregate and re-ranks
#[pg_test]
fn test_window_group_by_insert_reranks() {
    Spi::run("CREATE TABLE wgwi_src (id SERIAL, city TEXT, amount NUMERIC)").expect("create");
    Spi::run("INSERT INTO wgwi_src (city, amount) VALUES \
        ('Paris', 100), ('London', 200), ('Berlin', 150)").expect("seed");

    crate::create_reflex_ivm(
        "wgwi_view",
        "SELECT city, SUM(amount) AS total, \
                RANK() OVER (ORDER BY SUM(amount) DESC) AS rnk \
         FROM wgwi_src GROUP BY city",
        None, None, None,
    );

    // Initial: London=200(1), Berlin=150(2), Paris=100(3)
    assert_eq!(
        Spi::get_one::<i64>("SELECT rnk FROM wgwi_view WHERE city = 'London'").expect("q").expect("v"),
        1
    );

    // Insert enough to make Paris rank 1
    Spi::run("INSERT INTO wgwi_src (city, amount) VALUES ('Paris', 250)").expect("insert");

    // Now: Paris=350(1), London=200(2), Berlin=150(3)
    assert_eq!(
        Spi::get_one::<i64>("SELECT rnk FROM wgwi_view WHERE city = 'Paris'").expect("q").expect("v"),
        1, "Paris should be rank 1 after insert"
    );
    assert_eq!(
        Spi::get_one::<i64>("SELECT rnk FROM wgwi_view WHERE city = 'London'").expect("q").expect("v"),
        2, "London should drop to rank 2"
    );

    // Verify aggregate is correct
    let paris_total = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT total FROM wgwi_view WHERE city = 'Paris'",
    ).expect("q").expect("v");
    assert_eq!(paris_total.to_string(), "350");
}

/// GROUP BY + WINDOW with PARTITION BY — partitioned ranking over aggregates
#[pg_test]
fn test_window_group_by_partition_by() {
    Spi::run("CREATE TABLE wgwp_src (id SERIAL, region TEXT, city TEXT, amount NUMERIC)").expect("create");
    Spi::run("INSERT INTO wgwp_src (region, city, amount) VALUES \
        ('EU', 'Paris', 100), ('EU', 'Paris', 200), ('EU', 'Berlin', 300), \
        ('US', 'NYC', 400), ('US', 'LA', 150), ('US', 'Chicago', 250)").expect("seed");

    let result = crate::create_reflex_ivm(
        "wgwp_view",
        "SELECT region, city, SUM(amount) AS total, \
                ROW_NUMBER() OVER (PARTITION BY region ORDER BY SUM(amount) DESC) AS rnk \
         FROM wgwp_src GROUP BY region, city",
        None, None, None,
    );
    assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");

    // EU: Berlin=300(1), Paris=300(1 or 2 depending on tie-breaking)
    // US: NYC=400(1), Chicago=250(2), LA=150(3)
    let nyc_rank = Spi::get_one::<i64>(
        "SELECT rnk FROM wgwp_view WHERE city = 'NYC'",
    ).expect("q").expect("v");
    assert_eq!(nyc_rank, 1);

    let la_rank = Spi::get_one::<i64>(
        "SELECT rnk FROM wgwp_view WHERE city = 'LA'",
    ).expect("q").expect("v");
    assert_eq!(la_rank, 3);

    // INSERT into EU → only EU partition should re-rank
    Spi::run("INSERT INTO wgwp_src (region, city, amount) VALUES ('EU', 'Madrid', 500)").expect("insert");
    let madrid_rank = Spi::get_one::<i64>(
        "SELECT rnk FROM wgwp_view WHERE city = 'Madrid'",
    ).expect("q").expect("v");
    assert_eq!(madrid_rank, 1, "Madrid (500) should be rank 1 in EU");

    // US ranks unchanged
    assert_eq!(
        Spi::get_one::<i64>("SELECT rnk FROM wgwp_view WHERE city = 'NYC'").expect("q").expect("v"),
        1, "NYC rank should be unchanged (US partition untouched)"
    );
}

/// GROUP BY + WINDOW: DELETE removes a group and re-ranks
#[pg_test]
fn test_window_group_by_delete_reranks() {
    Spi::run("CREATE TABLE wgwd_src (id SERIAL, city TEXT, amount NUMERIC)").expect("create");
    Spi::run("INSERT INTO wgwd_src (city, amount) VALUES \
        ('A', 100), ('B', 200), ('C', 300)").expect("seed");

    crate::create_reflex_ivm(
        "wgwd_view",
        "SELECT city, SUM(amount) AS total, \
                ROW_NUMBER() OVER (ORDER BY SUM(amount) DESC) AS rnk \
         FROM wgwd_src GROUP BY city",
        None, None, None,
    );

    // Delete the top city
    Spi::run("DELETE FROM wgwd_src WHERE city = 'C'").expect("delete");

    // B should become rank 1, A rank 2
    assert_eq!(
        Spi::get_one::<i64>("SELECT rnk FROM wgwd_view WHERE city = 'B'").expect("q").expect("v"),
        1
    );
    assert_eq!(
        Spi::get_one::<i64>("SELECT rnk FROM wgwd_view WHERE city = 'A'").expect("q").expect("v"),
        2
    );
    assert_eq!(
        Spi::get_one::<i64>("SELECT COUNT(*) FROM wgwd_view").expect("q").expect("v"),
        2
    );
}

/// Multiple window functions in the same query
#[pg_test]
fn test_window_multiple_functions() {
    Spi::run("CREATE TABLE wmf_src (id SERIAL, name TEXT, score INT)").expect("create");
    Spi::run("INSERT INTO wmf_src (name, score) VALUES \
        ('Alice', 90), ('Bob', 80), ('Charlie', 70), ('Dave', 90)").expect("seed");

    let result = crate::create_reflex_ivm(
        "wmf_view",
        "SELECT name, score, \
                ROW_NUMBER() OVER (ORDER BY score DESC, name) AS row_num, \
                RANK() OVER (ORDER BY score DESC) AS rnk, \
                DENSE_RANK() OVER (ORDER BY score DESC) AS dense_rnk \
         FROM wmf_src",
        None, None, None,
    );
    assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");

    // Alice: row_num=1, rank=1, dense_rank=1
    let alice_rn = Spi::get_one::<i64>("SELECT row_num FROM wmf_view WHERE name = 'Alice'").expect("q").expect("v");
    let alice_r = Spi::get_one::<i64>("SELECT rnk FROM wmf_view WHERE name = 'Alice'").expect("q").expect("v");
    let alice_dr = Spi::get_one::<i64>("SELECT dense_rnk FROM wmf_view WHERE name = 'Alice'").expect("q").expect("v");
    assert_eq!(alice_rn, 1);
    assert_eq!(alice_r, 1);
    assert_eq!(alice_dr, 1);

    // Charlie: row_num=4, rank=3, dense_rank=2 (because RANK skips, DENSE_RANK doesn't)
    // Wait: Alice=90, Dave=90, Bob=80, Charlie=70
    // ROW_NUMBER by (score DESC, name): Alice(1), Dave(2), Bob(3), Charlie(4)
    // RANK by score DESC: 1,1,3,4
    // DENSE_RANK by score DESC: 1,1,2,3
    let charlie_rn = Spi::get_one::<i64>("SELECT row_num FROM wmf_view WHERE name = 'Charlie'").expect("q").expect("v");
    let charlie_r = Spi::get_one::<i64>("SELECT rnk FROM wmf_view WHERE name = 'Charlie'").expect("q").expect("v");
    let charlie_dr = Spi::get_one::<i64>("SELECT dense_rnk FROM wmf_view WHERE name = 'Charlie'").expect("q").expect("v");
    assert_eq!(charlie_rn, 4);
    assert_eq!(charlie_r, 4);
    assert_eq!(charlie_dr, 3);
}

/// Window function with UPDATE — value change triggers re-ranking
#[pg_test]
fn test_window_update_reranks() {
    Spi::run("CREATE TABLE wu_src (id SERIAL PRIMARY KEY, name TEXT, score INT)").expect("create");
    Spi::run("INSERT INTO wu_src (name, score) VALUES \
        ('Alice', 90), ('Bob', 80), ('Charlie', 70)").expect("seed");

    crate::create_reflex_ivm(
        "wu_view",
        "SELECT name, score, ROW_NUMBER() OVER (ORDER BY score DESC) AS rnk FROM wu_src",
        None, None, None,
    );

    // Update Charlie to top score
    Spi::run("UPDATE wu_src SET score = 100 WHERE name = 'Charlie'").expect("update");

    assert_eq!(
        Spi::get_one::<i64>("SELECT rnk FROM wu_view WHERE name = 'Charlie'").expect("q").expect("v"),
        1, "Charlie should be rank 1 after score update"
    );
    assert_eq!(
        Spi::get_one::<i64>("SELECT rnk FROM wu_view WHERE name = 'Alice'").expect("q").expect("v"),
        2
    );
}

/// EXCEPT correctness check for GROUP BY + WINDOW
#[pg_test]
fn test_window_group_by_except_correctness() {
    Spi::run("CREATE TABLE wge_src (id SERIAL, city TEXT, amount NUMERIC)").expect("create");
    Spi::run("INSERT INTO wge_src (city, amount) VALUES \
        ('Paris', 100), ('Paris', 200), ('London', 50), ('Berlin', 300), ('Berlin', 50)").expect("seed");

    crate::create_reflex_ivm(
        "wge_view",
        "SELECT city, SUM(amount) AS total, \
                RANK() OVER (ORDER BY SUM(amount) DESC) AS rnk \
         FROM wge_src GROUP BY city",
        None, None, None,
    );

    // Verify via EXCEPT against fresh computation
    let mismatches = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM (
            SELECT city, total, rnk FROM wge_view
            EXCEPT
            SELECT city, SUM(amount) AS total, RANK() OVER (ORDER BY SUM(amount) DESC) AS rnk
            FROM wge_src GROUP BY city
        ) x",
    ).expect("q").expect("v");
    assert_eq!(mismatches, 0, "IMV should match fresh computation");

    // Mutate and re-check
    Spi::run("INSERT INTO wge_src (city, amount) VALUES ('London', 500)").expect("insert");
    Spi::run("DELETE FROM wge_src WHERE city = 'Berlin' AND amount = 50").expect("delete");

    let mismatches2 = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM (
            SELECT city, total, rnk FROM wge_view
            EXCEPT
            SELECT city, SUM(amount) AS total, RANK() OVER (ORDER BY SUM(amount) DESC) AS rnk
            FROM wge_src GROUP BY city
        ) x",
    ).expect("q").expect("v");
    assert_eq!(mismatches2, 0, "IMV should match fresh computation after mutations");
}

/// Window with TRUNCATE — should clear and recompute correctly on re-insert
#[pg_test]
fn test_window_truncate_and_reinsert() {
    Spi::run("CREATE TABLE wtr_src (id SERIAL, val INT)").expect("create");
    Spi::run("INSERT INTO wtr_src (val) VALUES (10), (20), (30)").expect("seed");

    crate::create_reflex_ivm(
        "wtr_view",
        "SELECT val, ROW_NUMBER() OVER (ORDER BY val) AS rnk FROM wtr_src",
        None, None, None,
    );

    Spi::run("TRUNCATE wtr_src").expect("truncate");
    assert_eq!(
        Spi::get_one::<i64>("SELECT COUNT(*) FROM wtr_view").expect("q").expect("v"),
        0
    );

    // Re-insert different data
    Spi::run("INSERT INTO wtr_src (val) VALUES (99), (1)").expect("reinsert");
    assert_eq!(
        Spi::get_one::<i64>("SELECT COUNT(*) FROM wtr_view").expect("q").expect("v"),
        2
    );
    assert_eq!(
        Spi::get_one::<i64>("SELECT rnk FROM wtr_view WHERE val = 1").expect("q").expect("v"),
        1
    );
    assert_eq!(
        Spi::get_one::<i64>("SELECT rnk FROM wtr_view WHERE val = 99").expect("q").expect("v"),
        2
    );
}


#[pg_test]
fn test_join_query_imv() {
    Spi::run("CREATE TABLE test_j_emp (id SERIAL PRIMARY KEY, name TEXT, dept_id INT)")
        .expect("create emp table");
    Spi::run("CREATE TABLE test_j_dept (id SERIAL PRIMARY KEY, dept_name TEXT)")
        .expect("create dept table");
    Spi::run("INSERT INTO test_j_dept (id, dept_name) VALUES (1, 'Engineering'), (2, 'Sales')")
        .expect("insert depts");
    Spi::run("INSERT INTO test_j_emp (name, dept_id) VALUES ('Alice', 1), ('Bob', 1), ('Carol', 2)")
        .expect("insert emps");

    let result = crate::create_reflex_ivm(
        "test_dept_counts",
        "SELECT d.dept_name, COUNT(*) AS emp_count FROM test_j_emp e JOIN test_j_dept d ON e.dept_id = d.id GROUP BY d.dept_name",
        None,
        None,
        None,
    );
    assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");

    let eng_count = Spi::get_one::<i64>(
        "SELECT emp_count FROM test_dept_counts WHERE dept_name = 'Engineering'",
    )
    .expect("query")
    .expect("value");
    assert_eq!(eng_count, 2);

    // Verify both source tables are tracked in depends_on
    let depends = Spi::get_one::<Vec<String>>(
        "SELECT depends_on FROM public.__reflex_ivm_reference WHERE name = 'test_dept_counts'",
    )
    .expect("query")
    .expect("value");
    assert_eq!(depends.len(), 2);
}

#[pg_test]
fn test_e2e_full_lifecycle() {
    Spi::run("CREATE TABLE e2e_src (id SERIAL, city TEXT, amount NUMERIC)")
        .expect("create table");

    crate::create_reflex_ivm(
        "e2e_view",
        "SELECT city, SUM(amount) AS total, COUNT(*) AS cnt FROM e2e_src GROUP BY city",
        None,
        None,
        None,
    );

    // 1. INSERT initial rows
    Spi::run("INSERT INTO e2e_src (city, amount) VALUES ('A', 100), ('A', 200), ('B', 50)")
        .expect("insert");
    let a_total = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT total FROM e2e_view WHERE city = 'A'",
    ).expect("q").expect("v");
    assert_eq!(a_total.to_string(), "300");
    let b_total = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT total FROM e2e_view WHERE city = 'B'",
    ).expect("q").expect("v");
    assert_eq!(b_total.to_string(), "50");

    // 2. INSERT more rows
    Spi::run("INSERT INTO e2e_src (city, amount) VALUES ('A', 50), ('C', 400)")
        .expect("insert more");
    let a2 = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT total FROM e2e_view WHERE city = 'A'",
    ).expect("q").expect("v");
    assert_eq!(a2.to_string(), "350");
    let c = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT total FROM e2e_view WHERE city = 'C'",
    ).expect("q").expect("v");
    assert_eq!(c.to_string(), "400");

    // 3. UPDATE a row's value
    Spi::run("UPDATE e2e_src SET amount = 500 WHERE city = 'C'").expect("update");
    let c2 = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT total FROM e2e_view WHERE city = 'C'",
    ).expect("q").expect("v");
    assert_eq!(c2.to_string(), "500");

    // 4. DELETE one row from group A
    Spi::run("DELETE FROM e2e_src WHERE city = 'A' AND amount = 100").expect("delete one");
    let a3 = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT total FROM e2e_view WHERE city = 'A'",
    ).expect("q").expect("v");
    assert_eq!(a3.to_string(), "250"); // 200 + 50

    // 5. DELETE all rows for group B → group disappears
    Spi::run("DELETE FROM e2e_src WHERE city = 'B'").expect("delete all B");
    let b_gone = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM e2e_view WHERE city = 'B'",
    ).expect("q").expect("v");
    assert_eq!(b_gone, 0);

    // 6. INSERT back into empty group B → reappears
    Spi::run("INSERT INTO e2e_src (city, amount) VALUES ('B', 999)").expect("reinsert B");
    let b_back = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT total FROM e2e_view WHERE city = 'B'",
    ).expect("q").expect("v");
    assert_eq!(b_back.to_string(), "999");
}

#[pg_test]
fn test_e2e_cascading_propagation() {
    // Tests 2-level trigger propagation: source → L1 → L2
    // Triggers on L1's target table fire L2's delta processing automatically.
    Spi::run("CREATE TABLE cascade_src (id SERIAL, category TEXT, amount NUMERIC)")
        .expect("create table");
    Spi::run("INSERT INTO cascade_src (category, amount) VALUES ('X', 10), ('X', 20), ('Y', 30)")
        .expect("seed");

    // L1: SUM by category
    crate::create_reflex_ivm(
        "cascade_l1",
        "SELECT category, SUM(amount) AS total FROM cascade_src GROUP BY category",
        None,
        None,
        None,
    );

    // L2: SUM of L1 totals (grand total across all categories)
    crate::create_reflex_ivm(
        "cascade_l2",
        "SELECT SUM(total) AS grand_total FROM cascade_l1",
        None,
        None,
        None,
    );

    // Verify initial state
    let x1 = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT total FROM cascade_l1 WHERE category = 'X'",
    ).expect("q").expect("v");
    assert_eq!(x1.to_string(), "30"); // 10+20

    let gt = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT grand_total FROM cascade_l2",
    ).expect("q").expect("v");
    assert_eq!(gt.to_string(), "60"); // 30+30

    // INSERT into base → L1 updates → L2 updates via cascade
    Spi::run("INSERT INTO cascade_src (category, amount) VALUES ('X', 70)")
        .expect("insert");
    let x1_after = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT total FROM cascade_l1 WHERE category = 'X'",
    ).expect("q").expect("v");
    assert_eq!(x1_after.to_string(), "100"); // 10+20+70

    let gt_after = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT grand_total FROM cascade_l2",
    ).expect("q").expect("v");
    assert_eq!(gt_after.to_string(), "130"); // 100+30

    // DELETE from base → both levels update
    Spi::run("DELETE FROM cascade_src WHERE amount = 10").expect("delete");
    let x1_del = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT total FROM cascade_l1 WHERE category = 'X'",
    ).expect("q").expect("v");
    assert_eq!(x1_del.to_string(), "90"); // 20+70

    let gt_del = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT grand_total FROM cascade_l2",
    ).expect("q").expect("v");
    assert_eq!(gt_del.to_string(), "120"); // 90+30
}

#[pg_test]
fn test_e2e_join_trigger_insert_delete() {
    Spi::run("CREATE TABLE e2e_emp (id SERIAL PRIMARY KEY, name TEXT, dept_id INT)")
        .expect("create emp");
    Spi::run("CREATE TABLE e2e_dept (id SERIAL PRIMARY KEY, dept_name TEXT)")
        .expect("create dept");
    Spi::run("INSERT INTO e2e_dept (id, dept_name) VALUES (1, 'Eng'), (2, 'Sales')")
        .expect("seed depts");
    Spi::run("INSERT INTO e2e_emp (name, dept_id) VALUES ('Alice', 1), ('Bob', 1), ('Carol', 2)")
        .expect("seed emps");

    crate::create_reflex_ivm(
        "e2e_dept_counts",
        "SELECT d.dept_name, COUNT(*) AS emp_count FROM e2e_emp e JOIN e2e_dept d ON e.dept_id = d.id GROUP BY d.dept_name",
        None,
        None,
        None,
    );

    // Verify initial
    let eng = Spi::get_one::<i64>(
        "SELECT emp_count FROM e2e_dept_counts WHERE dept_name = 'Eng'",
    ).expect("q").expect("v");
    assert_eq!(eng, 2);

    // INSERT new employee → trigger fires, IMV updates
    Spi::run("INSERT INTO e2e_emp (name, dept_id) VALUES ('Dave', 1)")
        .expect("insert emp");
    let eng2 = Spi::get_one::<i64>(
        "SELECT emp_count FROM e2e_dept_counts WHERE dept_name = 'Eng'",
    ).expect("q").expect("v");
    assert_eq!(eng2, 3);

    // DELETE employee → Sales group disappears (ivm_count = 0)
    Spi::run("DELETE FROM e2e_emp WHERE name = 'Carol'").expect("delete emp");
    let sales_gone = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM e2e_dept_counts WHERE dept_name = 'Sales'",
    ).expect("q").expect("v");
    assert_eq!(sales_gone, 0); // Row removed from target
}

#[pg_test]
fn test_one_source_multiple_imvs() {
    // One source table with 3 independent IMVs depending on it.
    // INSERT/DELETE/UPDATE on the source should update all 3 correctly.
    Spi::run("CREATE TABLE multi_src (id SERIAL, city TEXT, amount NUMERIC, qty INTEGER)")
        .expect("create table");
    Spi::run(
        "INSERT INTO multi_src (city, amount, qty) VALUES \
         ('Paris', 100, 2), ('Paris', 200, 3), ('London', 300, 1)",
    ).expect("seed");

    // IMV 1: SUM of amount by city
    crate::create_reflex_ivm(
        "multi_v1",
        "SELECT city, SUM(amount) AS total FROM multi_src GROUP BY city",
        None,
        None,
        None,
    );
    // IMV 2: COUNT by city
    crate::create_reflex_ivm(
        "multi_v2",
        "SELECT city, COUNT(*) AS cnt FROM multi_src GROUP BY city",
        None,
        None,
        None,
    );
    // IMV 3: SUM of qty (no group by — global aggregate)
    crate::create_reflex_ivm(
        "multi_v3",
        "SELECT SUM(qty) AS total_qty FROM multi_src",
        None,
        None,
        None,
    );

    // Verify initial state
    let p_total = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT total FROM multi_v1 WHERE city = 'Paris'",
    ).expect("q").expect("v");
    assert_eq!(p_total.to_string(), "300");

    let p_cnt = Spi::get_one::<i64>(
        "SELECT cnt FROM multi_v2 WHERE city = 'Paris'",
    ).expect("q").expect("v");
    assert_eq!(p_cnt, 2);

    let tq = Spi::get_one::<i64>(
        "SELECT total_qty FROM multi_v3",
    ).expect("q").expect("v");
    assert_eq!(tq, 6i64); // 2+3+1

    // INSERT → all 3 IMVs update
    Spi::run("INSERT INTO multi_src (city, amount, qty) VALUES ('Paris', 50, 5)")
        .expect("insert");

    let p_total2 = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT total FROM multi_v1 WHERE city = 'Paris'",
    ).expect("q").expect("v");
    assert_eq!(p_total2.to_string(), "350");

    let p_cnt2 = Spi::get_one::<i64>(
        "SELECT cnt FROM multi_v2 WHERE city = 'Paris'",
    ).expect("q").expect("v");
    assert_eq!(p_cnt2, 3);

    let tq2 = Spi::get_one::<i64>(
        "SELECT total_qty FROM multi_v3",
    ).expect("q").expect("v");
    assert_eq!(tq2, 11i64); // 6+5

    // DELETE → all 3 update
    Spi::run("DELETE FROM multi_src WHERE amount = 100").expect("delete");

    let p_total3 = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT total FROM multi_v1 WHERE city = 'Paris'",
    ).expect("q").expect("v");
    assert_eq!(p_total3.to_string(), "250"); // 200+50

    let p_cnt3 = Spi::get_one::<i64>(
        "SELECT cnt FROM multi_v2 WHERE city = 'Paris'",
    ).expect("q").expect("v");
    assert_eq!(p_cnt3, 2);

    let tq3 = Spi::get_one::<i64>(
        "SELECT total_qty FROM multi_v3",
    ).expect("q").expect("v");
    assert_eq!(tq3, 9i64); // 11-2

    // UPDATE → all 3 update
    Spi::run("UPDATE multi_src SET amount = 999, qty = 10 WHERE city = 'London'")
        .expect("update");

    let l_total = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT total FROM multi_v1 WHERE city = 'London'",
    ).expect("q").expect("v");
    assert_eq!(l_total.to_string(), "999");

    let tq4 = Spi::get_one::<i64>(
        "SELECT total_qty FROM multi_v3",
    ).expect("q").expect("v");
    assert_eq!(tq4, 18i64); // 9 - 1 + 10

    // Verify consolidated triggers: 3 IMVs on same source → only 4 triggers (not 12)
    let trig_count = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM pg_trigger WHERE tgname LIKE '__reflex_trigger_%_on_multi_src'",
    ).expect("q").expect("v");
    assert_eq!(trig_count, 4); // ins, del, upd, trunc — shared by all 3 IMVs
}

#[pg_test]
fn test_4_level_cascade_chain() {
    // 4-level chain: base → L1 → L2 → L3 → L4
    // Each level aggregates differently to exercise the cascade.
    Spi::run(
        "CREATE TABLE chain4_src (id SERIAL, region TEXT, city TEXT, amount NUMERIC)",
    ).expect("create table");
    Spi::run(
        "INSERT INTO chain4_src (region, city, amount) VALUES \
         ('US', 'NYC', 100), ('US', 'LA', 200), \
         ('EU', 'London', 300), ('EU', 'Paris', 400)",
    ).expect("seed");

    // L1: SUM by region+city (keeps full granularity)
    crate::create_reflex_ivm(
        "chain4_l1",
        "SELECT region, city, SUM(amount) AS city_total FROM chain4_src GROUP BY region, city",
        None,
        None,
        None,
    );

    // L2: SUM by region (rolls up cities)
    crate::create_reflex_ivm(
        "chain4_l2",
        "SELECT region, SUM(city_total) AS region_total FROM chain4_l1 GROUP BY region",
        None,
        None,
        None,
    );

    // L3: COUNT of regions (how many regions have data)
    crate::create_reflex_ivm(
        "chain4_l3",
        "SELECT COUNT(*) AS num_regions FROM chain4_l2",
        None,
        None,
        None,
    );

    // L4: passthrough of L3 (tests cascading through passthrough)
    crate::create_reflex_ivm(
        "chain4_l4",
        "SELECT num_regions FROM chain4_l3",
        None,
        None,
        None,
    );

    // Verify initial state across all levels
    let nyc = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT city_total FROM chain4_l1 WHERE city = 'NYC'",
    ).expect("q").expect("v");
    assert_eq!(nyc.to_string(), "100");

    let us = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT region_total FROM chain4_l2 WHERE region = 'US'",
    ).expect("q").expect("v");
    assert_eq!(us.to_string(), "300"); // 100+200

    let nr = Spi::get_one::<i64>(
        "SELECT num_regions FROM chain4_l3",
    ).expect("q").expect("v");
    assert_eq!(nr, 2); // US, EU

    let nr4 = Spi::get_one::<i64>(
        "SELECT num_regions FROM chain4_l4",
    ).expect("q").expect("v");
    assert_eq!(nr4, 2);

    // INSERT into base → all 4 levels update
    Spi::run(
        "INSERT INTO chain4_src (region, city, amount) VALUES ('US', 'NYC', 50)",
    ).expect("insert");

    let nyc2 = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT city_total FROM chain4_l1 WHERE city = 'NYC'",
    ).expect("q").expect("v");
    assert_eq!(nyc2.to_string(), "150"); // 100+50

    let us2 = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT region_total FROM chain4_l2 WHERE region = 'US'",
    ).expect("q").expect("v");
    assert_eq!(us2.to_string(), "350"); // 150+200

    // Still 2 regions
    let nr2 = Spi::get_one::<i64>(
        "SELECT num_regions FROM chain4_l4",
    ).expect("q").expect("v");
    assert_eq!(nr2, 2);

    // INSERT a new region → L3/L4 should show 3
    Spi::run(
        "INSERT INTO chain4_src (region, city, amount) VALUES ('ASIA', 'Tokyo', 500)",
    ).expect("insert new region");

    let nr3 = Spi::get_one::<i64>(
        "SELECT num_regions FROM chain4_l3",
    ).expect("q").expect("v");
    assert_eq!(nr3, 3);

    let nr3_l4 = Spi::get_one::<i64>(
        "SELECT num_regions FROM chain4_l4",
    ).expect("q").expect("v");
    assert_eq!(nr3_l4, 3);

    // DELETE all rows for ASIA → region disappears, back to 2
    Spi::run("DELETE FROM chain4_src WHERE region = 'ASIA'").expect("delete region");

    let nr_del = Spi::get_one::<i64>(
        "SELECT num_regions FROM chain4_l4",
    ).expect("q").expect("v");
    assert_eq!(nr_del, 2);

    // Verify EU region total unchanged through all operations
    let eu = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT region_total FROM chain4_l2 WHERE region = 'EU'",
    ).expect("q").expect("v");
    assert_eq!(eu.to_string(), "700"); // 300+400
}

#[pg_test]
fn test_schema_qualified_source() {
    Spi::run(
        "CREATE TABLE sq_src (id SERIAL, grp TEXT NOT NULL, val NUMERIC NOT NULL)",
    )
    .expect("create table");
    Spi::run("INSERT INTO sq_src (grp, val) VALUES ('A', 10), ('B', 20)").expect("seed");
    let result = crate::create_reflex_ivm(
        "sq_view",
        "SELECT grp, SUM(val) AS total FROM public.sq_src GROUP BY grp",
        None,
        None,
        None,
    );
    assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");
    let total = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT total FROM sq_view WHERE grp = 'A'",
    )
    .expect("q")
    .expect("v");
    assert_eq!(total.to_string(), "10");
    // Trigger should work with schema-qualified source
    Spi::run("INSERT INTO sq_src (grp, val) VALUES ('A', 5)").expect("insert");
    let total2 = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT total FROM sq_view WHERE grp = 'A'",
    )
    .expect("q")
    .expect("v");
    assert_eq!(total2.to_string(), "15");
}

#[pg_test]
fn test_schema_qualified_view_name() {
    Spi::run("CREATE SCHEMA IF NOT EXISTS test_schema").expect("create schema");
    Spi::run(
        "CREATE TABLE test_schema.sq_src2 (id SERIAL, grp TEXT NOT NULL, val NUMERIC NOT NULL)",
    )
    .expect("create table in schema");
    Spi::run("INSERT INTO test_schema.sq_src2 (grp, val) VALUES ('A', 10), ('B', 20)")
        .expect("seed");

    let result = crate::create_reflex_ivm(
        "test_schema.sq_view2",
        "SELECT grp, SUM(val) AS total FROM test_schema.sq_src2 GROUP BY grp",
        None,
        None,
        None,
    );
    assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");

    // Verify table exists in test_schema and has correct data
    let total = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT total FROM test_schema.sq_view2 WHERE grp = 'A'",
    )
    .expect("q")
    .expect("v");
    assert_eq!(total.to_string(), "10");

    // Verify trigger fires for source table INSERTs
    Spi::run("INSERT INTO test_schema.sq_src2 (grp, val) VALUES ('A', 5)").expect("insert");
    let total2 = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT total FROM test_schema.sq_view2 WHERE grp = 'A'",
    )
    .expect("q")
    .expect("v");
    assert_eq!(total2.to_string(), "15");

    // Verify drop works
    let drop_result = crate::drop_reflex_ivm("test_schema.sq_view2");
    assert_eq!(drop_result, "DROP REFLEX INCREMENTAL VIEW");
}

#[pg_test]
fn test_multi_level_cascade_propagation() {
    // Base table
    Spi::run(
        "CREATE TABLE mlc_src (id SERIAL, region TEXT NOT NULL, amount NUMERIC NOT NULL)",
    )
    .expect("create base table");
    Spi::run(
        "INSERT INTO mlc_src (region, amount) VALUES ('US', 100), ('US', 200), ('EU', 150)",
    )
    .expect("seed");

    // L1: aggregates base by region
    let r1 = crate::create_reflex_ivm(
        "mlc_l1",
        "SELECT region, SUM(amount) AS total, COUNT(*) AS cnt FROM mlc_src GROUP BY region",
        None,
        None,
        None,
    );
    assert_eq!(r1, "CREATE REFLEX INCREMENTAL VIEW");

    // L2: aggregates L1 (re-aggregates totals — tests cascade)
    let r2 = crate::create_reflex_ivm(
        "mlc_l2",
        "SELECT region, SUM(total) AS grand_total FROM mlc_l1 GROUP BY region",
        None,
        None,
        None,
    );
    assert_eq!(r2, "CREATE REFLEX INCREMENTAL VIEW");

    // Verify initial state
    let l1_us = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT total FROM mlc_l1 WHERE region = 'US'",
    ).expect("q").expect("v");
    assert_eq!(l1_us.to_string(), "300", "L1 US = 100 + 200");

    let l2_us = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT grand_total FROM mlc_l2 WHERE region = 'US'",
    ).expect("q").expect("v");
    assert_eq!(l2_us.to_string(), "300", "L2 US = L1 US = 300");

    // INSERT into base → both L1 and L2 should update
    Spi::run("INSERT INTO mlc_src (region, amount) VALUES ('US', 50)").expect("insert");

    let l1_after_ins = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT total FROM mlc_l1 WHERE region = 'US'",
    ).expect("q").expect("v");
    assert_eq!(l1_after_ins.to_string(), "350", "L1 US after insert = 350");

    let l2_after_ins = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT grand_total FROM mlc_l2 WHERE region = 'US'",
    ).expect("q").expect("v");
    assert_eq!(l2_after_ins.to_string(), "350", "L2 US should cascade from L1");

    // UPDATE base (change amount) → both levels should reflect
    Spi::run("UPDATE mlc_src SET amount = 500 WHERE amount = 200").expect("update");

    let l1_after_upd = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT total FROM mlc_l1 WHERE region = 'US'",
    ).expect("q").expect("v");
    assert_eq!(l1_after_upd.to_string(), "650", "L1 US after update = 100 + 500 + 50");

    let l2_after_upd = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT grand_total FROM mlc_l2 WHERE region = 'US'",
    ).expect("q").expect("v");
    assert_eq!(l2_after_upd.to_string(), "650", "L2 US should cascade update");

    // DELETE from base → both levels update
    Spi::run("DELETE FROM mlc_src WHERE amount = 100").expect("delete");

    let l1_after_del = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT total FROM mlc_l1 WHERE region = 'US'",
    ).expect("q").expect("v");
    assert_eq!(l1_after_del.to_string(), "550", "L1 US after delete = 500 + 50");

    let l2_after_del = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT grand_total FROM mlc_l2 WHERE region = 'US'",
    ).expect("q").expect("v");
    assert_eq!(l2_after_del.to_string(), "550", "L2 US should cascade delete");

    // DELETE all US rows → US group should disappear from both levels
    Spi::run("DELETE FROM mlc_src WHERE region = 'US'").expect("delete all US");

    let l1_count = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM mlc_l1 WHERE region = 'US'",
    ).expect("q").expect("v");
    assert_eq!(l1_count, 0, "L1 US group should be gone");

    let l2_count = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM mlc_l2 WHERE region = 'US'",
    ).expect("q").expect("v");
    assert_eq!(l2_count, 0, "L2 US group should cascade-disappear");

    // EU should still be intact at both levels
    let l2_eu = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT grand_total FROM mlc_l2 WHERE region = 'EU'",
    ).expect("q").expect("v");
    assert_eq!(l2_eu.to_string(), "150", "L2 EU untouched");
}

/// Chain: source → passthrough L1 → aggregate L2
/// Tests that DML on the source propagates through a passthrough layer
/// into an aggregate layer, with full correctness checks.
#[pg_test]
fn test_chain_passthrough_then_aggregate() {
    Spi::run(
        "CREATE TABLE cpta_src (id SERIAL PRIMARY KEY, region TEXT NOT NULL, amount INT NOT NULL, active BOOLEAN NOT NULL)",
    ).expect("create table");
    Spi::run(
        "INSERT INTO cpta_src (region, amount, active) VALUES \
         ('US', 100, true), ('US', 200, true), ('US', 50, false), \
         ('EU', 300, true), ('EU', 150, false)",
    ).expect("seed");

    // L1: passthrough with WHERE filter (only active rows)
    crate::create_reflex_ivm(
        "cpta_l1",
        "SELECT id, region, amount FROM cpta_src WHERE active = true",
        None,
        None,
        None,
    );

    // L2: aggregate on L1 (SUM by region)
    crate::create_reflex_ivm(
        "cpta_l2",
        "SELECT region, SUM(amount) AS total, COUNT(*) AS cnt FROM cpta_l1 GROUP BY region",
        None,
        None,
        None,
    );

    // Verify initial state
    let us_total = Spi::get_one::<i64>(
        "SELECT total FROM cpta_l2 WHERE region = 'US'",
    ).expect("q").expect("v");
    assert_eq!(us_total, 300i64); // 100+200 (active only)

    let eu_total = Spi::get_one::<i64>(
        "SELECT total FROM cpta_l2 WHERE region = 'EU'",
    ).expect("q").expect("v");
    assert_eq!(eu_total, 300i64); // 300 (active only)

    // INSERT active row → propagates through L1 passthrough → L2 aggregate updates
    Spi::run("INSERT INTO cpta_src (region, amount, active) VALUES ('US', 400, true)")
        .expect("insert active");
    let us_after_ins = Spi::get_one::<i64>(
        "SELECT total FROM cpta_l2 WHERE region = 'US'",
    ).expect("q").expect("v");
    assert_eq!(us_after_ins, 700i64); // 100+200+400

    // INSERT inactive row → appears in source but NOT in L1 or L2
    Spi::run("INSERT INTO cpta_src (region, amount, active) VALUES ('US', 999, false)")
        .expect("insert inactive");
    let us_after_inactive = Spi::get_one::<i64>(
        "SELECT total FROM cpta_l2 WHERE region = 'US'",
    ).expect("q").expect("v");
    assert_eq!(us_after_inactive, 700i64); // unchanged

    // DELETE an active row → cascades through both levels
    Spi::run("DELETE FROM cpta_src WHERE amount = 100").expect("delete");
    let us_after_del = Spi::get_one::<i64>(
        "SELECT total FROM cpta_l2 WHERE region = 'US'",
    ).expect("q").expect("v");
    assert_eq!(us_after_del, 600i64); // 200+400

    // UPDATE a row to change region → moves between groups at L2
    Spi::run("UPDATE cpta_src SET region = 'EU' WHERE amount = 200").expect("update");
    let us_after_upd = Spi::get_one::<i64>(
        "SELECT total FROM cpta_l2 WHERE region = 'US'",
    ).expect("q").expect("v");
    assert_eq!(us_after_upd, 400i64); // only 400 left in US

    let eu_after_upd = Spi::get_one::<i64>(
        "SELECT total FROM cpta_l2 WHERE region = 'EU'",
    ).expect("q").expect("v");
    assert_eq!(eu_after_upd, 500i64); // 300+200

    // Verify L1 matches source exactly
    let l1_mismatches = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM (
            SELECT id, region, amount FROM cpta_l1
            EXCEPT
            SELECT id, region, amount FROM cpta_src WHERE active = true
        ) x",
    ).expect("q").expect("v");
    assert_eq!(l1_mismatches, 0, "L1 should exactly match filtered source");

    // Verify L2 matches aggregate of filtered source
    let l2_mismatches = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM (
            SELECT region, total, cnt FROM cpta_l2
            EXCEPT
            SELECT region, SUM(amount), COUNT(*)
            FROM cpta_src WHERE active = true GROUP BY region
        ) x",
    ).expect("q").expect("v");
    assert_eq!(l2_mismatches, 0, "L2 should exactly match aggregate of filtered source");
}

/// Chain: source → aggregate L1 → passthrough L2
/// Tests that an aggregate feeds into a passthrough that tracks all its rows.
#[pg_test]
fn test_chain_aggregate_then_passthrough() {
    Spi::run(
        "CREATE TABLE catp_src (id SERIAL, city TEXT NOT NULL, revenue INT NOT NULL)",
    ).expect("create table");
    Spi::run(
        "INSERT INTO catp_src (city, revenue) VALUES \
         ('Paris', 100), ('Paris', 200), ('London', 300), ('Berlin', 50)",
    ).expect("seed");

    // L1: aggregate (SUM by city)
    crate::create_reflex_ivm(
        "catp_l1",
        "SELECT city, SUM(revenue) AS total, COUNT(*) AS cnt FROM catp_src GROUP BY city",
        None,
        None,
        None,
    );

    // L2: passthrough of L1 (reads all rows from the aggregate target)
    crate::create_reflex_ivm(
        "catp_l2",
        "SELECT city, total, cnt FROM catp_l1",
        None,
        None,
        None,
    );

    // Verify initial state
    assert_eq!(
        Spi::get_one::<i64>("SELECT COUNT(*) FROM catp_l2").expect("q").expect("v"),
        3, // Paris, London, Berlin
    );
    let paris = Spi::get_one::<i64>(
        "SELECT total FROM catp_l2 WHERE city = 'Paris'",
    ).expect("q").expect("v");
    assert_eq!(paris, 300i64);

    // INSERT → L1 updates → L2 passthrough picks up change
    Spi::run("INSERT INTO catp_src (city, revenue) VALUES ('Paris', 50)")
        .expect("insert");
    let paris_ins = Spi::get_one::<i64>(
        "SELECT total FROM catp_l2 WHERE city = 'Paris'",
    ).expect("q").expect("v");
    assert_eq!(paris_ins, 350i64);

    // INSERT new city → new group appears in L1 and L2
    Spi::run("INSERT INTO catp_src (city, revenue) VALUES ('Tokyo', 500)")
        .expect("insert new city");
    assert_eq!(
        Spi::get_one::<i64>("SELECT COUNT(*) FROM catp_l2").expect("q").expect("v"),
        4,
    );

    // DELETE all rows for a city → group disappears from L1 and L2
    Spi::run("DELETE FROM catp_src WHERE city = 'Berlin'").expect("delete group");
    assert_eq!(
        Spi::get_one::<i64>("SELECT COUNT(*) FROM catp_l2").expect("q").expect("v"),
        3, // Berlin gone
    );
    let berlin_count = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM catp_l2 WHERE city = 'Berlin'",
    ).expect("q").expect("v");
    assert_eq!(berlin_count, 0, "Berlin should not exist in L2");

    // UPDATE → value change propagates through both levels
    Spi::run("UPDATE catp_src SET revenue = 999 WHERE city = 'London'").expect("update");
    let london = Spi::get_one::<i64>(
        "SELECT total FROM catp_l2 WHERE city = 'London'",
    ).expect("q").expect("v");
    assert_eq!(london, 999i64);

    // Verify L1 matches source
    let l1_mismatches = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM (
            SELECT city, total, cnt FROM catp_l1
            EXCEPT
            SELECT city, SUM(revenue), COUNT(*) FROM catp_src GROUP BY city
        ) x",
    ).expect("q").expect("v");
    assert_eq!(l1_mismatches, 0, "L1 should exactly match aggregate of source");

    // Verify L2 matches L1 exactly
    let l2_mismatches = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM (
            SELECT city, total, cnt FROM catp_l2
            EXCEPT
            SELECT city, total, cnt FROM catp_l1
        ) x",
    ).expect("q").expect("v");
    assert_eq!(l2_mismatches, 0, "L2 should exactly mirror L1");
}

/// Chain: source → passthrough L1 (JOIN) → aggregate L2
/// The passthrough layer is a JOIN, so this tests cascade through a JOIN passthrough.
#[pg_test]
fn test_chain_passthrough_join_then_aggregate() {
    Spi::run("CREATE TABLE cpja_products (id SERIAL PRIMARY KEY, category TEXT NOT NULL)")
        .expect("create products");
    Spi::run("CREATE TABLE cpja_sales (id SERIAL PRIMARY KEY, product_id INT NOT NULL, amount INT NOT NULL)")
        .expect("create sales");
    Spi::run("INSERT INTO cpja_products VALUES (1, 'Electronics'), (2, 'Books'), (3, 'Food')")
        .expect("seed products");
    Spi::run(
        "INSERT INTO cpja_sales (product_id, amount) VALUES \
         (1, 100), (1, 200), (2, 50), (2, 150), (3, 75)",
    ).expect("seed sales");

    // L1: passthrough JOIN (denormalize sales with product category)
    crate::create_reflex_ivm(
        "cpja_l1",
        "SELECT s.id, s.amount, p.category \
         FROM cpja_sales s JOIN cpja_products p ON s.product_id = p.id",
        Some("id"),
        None,
        None,
    );

    // L2: aggregate on L1 (SUM by category)
    crate::create_reflex_ivm(
        "cpja_l2",
        "SELECT category, SUM(amount) AS total, COUNT(*) AS cnt FROM cpja_l1 GROUP BY category",
        None,
        None,
        None,
    );

    // Verify initial
    let elec = Spi::get_one::<i64>(
        "SELECT total FROM cpja_l2 WHERE category = 'Electronics'",
    ).expect("q").expect("v");
    assert_eq!(elec, 300i64); // 100+200

    // INSERT into sales → propagates through L1 JOIN → L2 aggregate
    Spi::run("INSERT INTO cpja_sales (product_id, amount) VALUES (2, 100)")
        .expect("insert sale");
    let books = Spi::get_one::<i64>(
        "SELECT total FROM cpja_l2 WHERE category = 'Books'",
    ).expect("q").expect("v");
    assert_eq!(books, 300i64); // 50+150+100

    // DELETE a product from secondary table → L1 removes rows → L2 group shrinks
    Spi::run("DELETE FROM cpja_products WHERE id = 3").expect("delete product");
    let food_count = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM cpja_l2 WHERE category = 'Food'",
    ).expect("q").expect("v");
    assert_eq!(food_count, 0, "Food category should disappear from L2");

    // DELETE from sales (key-owner) → direct key extraction at L1 → cascades to L2
    Spi::run("DELETE FROM cpja_sales WHERE amount = 100 AND product_id = 1").expect("delete sale");
    let elec_after = Spi::get_one::<i64>(
        "SELECT total FROM cpja_l2 WHERE category = 'Electronics'",
    ).expect("q").expect("v");
    assert_eq!(elec_after, 200i64); // only 200 left

    // UPDATE product category → L1 updates → L2 groups shift
    Spi::run("UPDATE cpja_products SET category = 'Electronics' WHERE id = 2")
        .expect("update product category");
    let elec_final = Spi::get_one::<i64>(
        "SELECT total FROM cpja_l2 WHERE category = 'Electronics'",
    ).expect("q").expect("v");
    assert_eq!(elec_final, 500i64); // 200 + 50+150+100

    // Verify L1 exactly matches direct JOIN
    let l1_mismatches = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM (
            SELECT id, amount, category FROM cpja_l1
            EXCEPT
            SELECT s.id, s.amount, p.category
            FROM cpja_sales s JOIN cpja_products p ON s.product_id = p.id
        ) x",
    ).expect("q").expect("v");
    assert_eq!(l1_mismatches, 0, "L1 should exactly match the JOIN");

    // Verify L2 matches aggregate of L1
    let l2_mismatches = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM (
            SELECT category, total, cnt FROM cpja_l2
            EXCEPT
            SELECT category, SUM(amount), COUNT(*) FROM cpja_l1 GROUP BY category
        ) x",
    ).expect("q").expect("v");
    assert_eq!(l2_mismatches, 0, "L2 should exactly match aggregate of L1");
}

/// Multiple IMVs of different types (aggregate, passthrough, distinct) on the same
/// source table. All must stay correct through INSERT, DELETE, and UPDATE.
#[pg_test]
fn test_multiple_mixed_imvs_on_same_source() {
    Spi::run(
        "CREATE TABLE mmis_src (id SERIAL PRIMARY KEY, dept TEXT NOT NULL, salary INT NOT NULL, active BOOLEAN NOT NULL)",
    ).expect("create table");
    Spi::run(
        "INSERT INTO mmis_src (dept, salary, active) VALUES \
         ('Eng', 100, true), ('Eng', 200, true), ('Eng', 50, false), \
         ('Sales', 300, true), ('Sales', 150, false), \
         ('HR', 80, true)",
    ).expect("seed");

    // IMV1: aggregate — SUM salary by dept
    crate::create_reflex_ivm(
        "mmis_agg",
        "SELECT dept, SUM(salary) AS total, COUNT(*) AS cnt FROM mmis_src GROUP BY dept",
        None,
        None,
        None,
    );

    // IMV2: passthrough — all active employees
    crate::create_reflex_ivm(
        "mmis_active",
        "SELECT id, dept, salary FROM mmis_src WHERE active = true",
        None,
        None,
        None,
    );

    // IMV3: distinct — unique departments
    crate::create_reflex_ivm(
        "mmis_depts",
        "SELECT DISTINCT dept FROM mmis_src",
        None,
        None,
        None,
    );

    // IMV4: aggregate — AVG salary globally
    crate::create_reflex_ivm(
        "mmis_avg",
        "SELECT AVG(salary) AS avg_sal FROM mmis_src",
        None,
        None,
        None,
    );

    // Verify initial state
    let eng_total = Spi::get_one::<i64>(
        "SELECT total FROM mmis_agg WHERE dept = 'Eng'",
    ).expect("q").expect("v");
    assert_eq!(eng_total, 350i64); // 100+200+50

    let active_count = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM mmis_active",
    ).expect("q").expect("v");
    assert_eq!(active_count, 4); // 100,200,300,80

    let dept_count = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM mmis_depts",
    ).expect("q").expect("v");
    assert_eq!(dept_count, 3);

    // INSERT → all 4 IMVs must update correctly
    Spi::run("INSERT INTO mmis_src (dept, salary, active) VALUES ('Eng', 400, true)")
        .expect("insert");

    assert_eq!(
        Spi::get_one::<i64>("SELECT total FROM mmis_agg WHERE dept = 'Eng'")
            .expect("q").expect("v"),
        750i64, // 350+400
    );
    assert_eq!(
        Spi::get_one::<i64>("SELECT COUNT(*) FROM mmis_active").expect("q").expect("v"),
        5,
    );
    assert_eq!(
        Spi::get_one::<i64>("SELECT COUNT(*) FROM mmis_depts").expect("q").expect("v"),
        3, // no new dept
    );

    // INSERT new dept → distinct count changes
    Spi::run("INSERT INTO mmis_src (dept, salary, active) VALUES ('Legal', 250, true)")
        .expect("insert new dept");
    assert_eq!(
        Spi::get_one::<i64>("SELECT COUNT(*) FROM mmis_depts").expect("q").expect("v"),
        4,
    );

    // DELETE → all 4 IMVs update
    Spi::run("DELETE FROM mmis_src WHERE salary = 50").expect("delete inactive eng");
    assert_eq!(
        Spi::get_one::<i64>("SELECT total FROM mmis_agg WHERE dept = 'Eng'")
            .expect("q").expect("v"),
        700i64, // 100+200+400
    );
    // Active count shouldn't change (deleted row was inactive)
    assert_eq!(
        Spi::get_one::<i64>("SELECT COUNT(*) FROM mmis_active").expect("q").expect("v"),
        6,
    );

    // DELETE all rows in a dept → group disappears from aggregate and distinct
    Spi::run("DELETE FROM mmis_src WHERE dept = 'HR'").expect("delete HR");
    let hr_count = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM mmis_agg WHERE dept = 'HR'",
    ).expect("q").expect("v");
    assert_eq!(hr_count, 0, "HR should disappear from aggregate");
    assert_eq!(
        Spi::get_one::<i64>("SELECT COUNT(*) FROM mmis_depts").expect("q").expect("v"),
        3, // HR gone, Legal still there
    );

    // UPDATE → value change propagates to aggregate and avg
    Spi::run("UPDATE mmis_src SET salary = 1000 WHERE dept = 'Legal'").expect("update");
    assert_eq!(
        Spi::get_one::<i64>("SELECT total FROM mmis_agg WHERE dept = 'Legal'")
            .expect("q").expect("v"),
        1000i64,
    );

    // Verify passthrough matches source exactly
    let pt_mismatches = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM (
            SELECT id, dept, salary FROM mmis_active
            EXCEPT
            SELECT id, dept, salary FROM mmis_src WHERE active = true
        ) x",
    ).expect("q").expect("v");
    assert_eq!(pt_mismatches, 0, "Passthrough IMV should exactly match filtered source");

    // Verify aggregate matches source
    let agg_mismatches = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM (
            SELECT dept, total, cnt FROM mmis_agg
            EXCEPT
            SELECT dept, SUM(salary), COUNT(*) FROM mmis_src GROUP BY dept
        ) x",
    ).expect("q").expect("v");
    assert_eq!(agg_mismatches, 0, "Aggregate IMV should match source");

    // Verify distinct matches source
    let dist_mismatches = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM (
            SELECT dept FROM mmis_depts
            EXCEPT
            SELECT DISTINCT dept FROM mmis_src
        ) x",
    ).expect("q").expect("v");
    assert_eq!(dist_mismatches, 0, "Distinct IMV should match source");
}

#[pg_test]
fn test_having_filters_groups() {
    Spi::run(
        "CREATE TABLE hv_src (id SERIAL, region TEXT NOT NULL, amount NUMERIC NOT NULL)",
    )
    .expect("create table");
    Spi::run(
        "INSERT INTO hv_src (region, amount) VALUES \
         ('US', 500), ('US', 600), ('EU', 100), ('EU', 50), ('JP', 2000)",
    )
    .expect("seed");

    // Only regions with SUM > 200 should appear
    crate::create_reflex_ivm(
        "hv_view",
        "SELECT region, SUM(amount) AS total FROM hv_src GROUP BY region HAVING SUM(amount) > 200",
        None,
        None,
        None,
    );

    // US = 1100, JP = 2000 → both > 200. EU = 150 → excluded.
    let count =
        Spi::get_one::<i64>("SELECT COUNT(*) FROM hv_view").expect("q").expect("v");
    assert_eq!(count, 2, "Only US and JP should pass HAVING SUM > 200");

    let eu = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM hv_view WHERE region = 'EU'",
    )
    .expect("q")
    .expect("v");
    assert_eq!(eu, 0, "EU (150) should be excluded by HAVING");
}

#[pg_test]
fn test_having_dynamic_threshold() {
    Spi::run(
        "CREATE TABLE hvd_src (id SERIAL, grp TEXT NOT NULL, val NUMERIC NOT NULL)",
    )
    .expect("create table");
    Spi::run(
        "INSERT INTO hvd_src (grp, val) VALUES ('A', 80), ('B', 40)",
    )
    .expect("seed");

    crate::create_reflex_ivm(
        "hvd_view",
        "SELECT grp, SUM(val) AS total FROM hvd_src GROUP BY grp HAVING SUM(val) > 50",
        None,
        None,
        None,
    );

    // Initially: A=80 passes, B=40 fails
    let count =
        Spi::get_one::<i64>("SELECT COUNT(*) FROM hvd_view").expect("q").expect("v");
    assert_eq!(count, 1, "Only A should pass HAVING > 50");

    // Push B over threshold
    Spi::run("INSERT INTO hvd_src (grp, val) VALUES ('B', 20)").expect("insert B");
    let b_count =
        Spi::get_one::<i64>("SELECT COUNT(*) FROM hvd_view WHERE grp = 'B'")
            .expect("q")
            .expect("v");
    assert_eq!(b_count, 1, "B (60) should now appear after crossing threshold");

    // Pull A below threshold by deleting
    Spi::run("DELETE FROM hvd_src WHERE grp = 'A' AND val = 80").expect("delete A");
    let a_count =
        Spi::get_one::<i64>("SELECT COUNT(*) FROM hvd_view WHERE grp = 'A'")
            .expect("q")
            .expect("v");
    assert_eq!(a_count, 0, "A (0) should disappear after falling below threshold");
}

#[pg_test]
fn test_having_with_aggregate_not_in_select() {
    Spi::run(
        "CREATE TABLE hvn_src (id SERIAL, grp TEXT NOT NULL, val NUMERIC NOT NULL)",
    )
    .expect("create table");
    Spi::run(
        "INSERT INTO hvn_src (grp, val) VALUES \
         ('A', 10), ('A', 20), ('A', 30), ('B', 100), ('B', 200)",
    )
    .expect("seed");

    // SELECT has SUM but HAVING uses COUNT(*) — not in SELECT
    crate::create_reflex_ivm(
        "hvn_view",
        "SELECT grp, SUM(val) AS total FROM hvn_src GROUP BY grp HAVING COUNT(*) > 2",
        None,
        None,
        None,
    );

    // A has 3 rows → passes. B has 2 rows → fails.
    let count =
        Spi::get_one::<i64>("SELECT COUNT(*) FROM hvn_view").expect("q").expect("v");
    assert_eq!(count, 1, "Only A (3 rows) should pass HAVING COUNT(*) > 2");

    let total = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT total FROM hvn_view WHERE grp = 'A'",
    )
    .expect("q")
    .expect("v");
    assert_eq!(total.to_string(), "60", "A total should be 10+20+30=60");
}

#[pg_test]
fn test_matview_source_skip_triggers() {
    // Create a base table and a materialized view on it
    Spi::run("CREATE TABLE mv_base (id SERIAL, grp TEXT NOT NULL, val NUMERIC NOT NULL)")
        .expect("create base");
    Spi::run("INSERT INTO mv_base (grp, val) VALUES ('A', 10), ('B', 20)")
        .expect("seed base");
    Spi::run("CREATE MATERIALIZED VIEW mv_src AS SELECT grp, SUM(val) AS total FROM mv_base GROUP BY grp")
        .expect("create matview");

    // Create an IMV that reads from the materialized view — should succeed
    // (triggers skipped for matview, warning emitted)
    let result = crate::create_reflex_ivm(
        "mv_imv",
        "SELECT grp, SUM(total) AS grand_total FROM mv_src GROUP BY grp",
        None,
        None,
        None,
    );
    assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");

    // Verify initial data is correct
    let a = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT grand_total FROM mv_imv WHERE grp = 'A'",
    )
    .expect("q")
    .expect("v");
    assert_eq!(a.to_string(), "10");

    // Insert into base table and refresh the matview
    Spi::run("INSERT INTO mv_base (grp, val) VALUES ('A', 5)").expect("insert");
    Spi::run("REFRESH MATERIALIZED VIEW mv_src").expect("refresh matview");

    // IMV is stale (no triggers on matview) — still shows old value
    let a_stale = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT grand_total FROM mv_imv WHERE grp = 'A'",
    )
    .expect("q")
    .expect("v");
    assert_eq!(a_stale.to_string(), "10", "IMV should be stale before refresh");

    // Refresh the IMV — should pick up new matview data
    let refresh = crate::refresh_reflex_imv("mv_imv");
    assert_eq!(refresh, "RECONCILED");

    let a_fresh = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT grand_total FROM mv_imv WHERE grp = 'A'",
    )
    .expect("q")
    .expect("v");
    assert_eq!(a_fresh.to_string(), "15", "After refresh, IMV should have 10+5=15");
}

/// BOOL_OR aggregate: INSERT, DELETE (recompute), UPDATE
#[pg_test]
fn test_bool_or_insert_delete_update() {
    Spi::run(
        "CREATE TABLE bo_src (id SERIAL PRIMARY KEY, grp TEXT NOT NULL, flag BOOLEAN NOT NULL)",
    ).expect("create");
    Spi::run(
        "INSERT INTO bo_src (grp, flag) VALUES ('A', false), ('A', false), ('B', true)",
    ).expect("seed");

    crate::create_reflex_ivm(
        "bo_view",
        "SELECT grp, BOOL_OR(flag) AS any_flag FROM bo_src GROUP BY grp",
        None,
        None,
        None,
    );

    // Initial: A=false (both false), B=true
    // BOOL_OR returns NULL-safe boolean, read via i64 count to verify
    let a_val = Spi::get_one::<bool>("SELECT any_flag FROM bo_view WHERE grp = 'A'")
        .expect("initial A query failed")
        .expect("initial A returned no rows");
    assert!(!a_val, "A should be false initially");

    let b_val = Spi::get_one::<bool>("SELECT any_flag FROM bo_view WHERE grp = 'B'")
        .expect("initial B query failed")
        .expect("initial B returned no rows");
    assert!(b_val, "B should be true initially");

    // INSERT true into A → should become true (OR logic)
    Spi::run("INSERT INTO bo_src (grp, flag) VALUES ('A', true)").expect("insert true");
    let a_ins = Spi::get_one::<bool>("SELECT any_flag FROM bo_view WHERE grp = 'A'")
        .expect("after insert query failed")
        .expect("after insert returned no rows");
    assert!(a_ins, "A should be true after inserting true");

    // DELETE the only true row → recompute from source, should become false
    Spi::run("DELETE FROM bo_src WHERE grp = 'A' AND flag = true").expect("delete true");
    let a_after_del = Spi::get_one::<bool>("SELECT any_flag FROM bo_view WHERE grp = 'A'")
        .expect("after delete query failed")
        .expect("after delete no rows");
    assert!(!a_after_del, "A should revert to false after deleting only true row");

    // UPDATE false→true
    Spi::run("UPDATE bo_src SET flag = true WHERE id = (SELECT MIN(id) FROM bo_src WHERE grp = 'A')")
        .expect("update");

    // Final EXCEPT correctness check (no reconcile needed)
    let mismatches = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM (
            SELECT grp, any_flag FROM bo_view
            EXCEPT
            SELECT grp, BOOL_OR(flag) FROM bo_src GROUP BY grp
        ) x",
    ).expect("final except query failed").expect("final except no rows");
    assert_eq!(mismatches, 0, "View should exactly match source");
}

/// LEFT JOIN aggregate: group by a non-nullable column to avoid NULL group key limitation.
/// Tests that LEFT JOIN preserves unmatched rows via the left table.
#[pg_test]
fn test_left_join_aggregate() {
    Spi::run("CREATE TABLE lj_orders (id SERIAL PRIMARY KEY, product_id INT, region TEXT NOT NULL, amount INT NOT NULL)")
        .expect("create orders");
    Spi::run("CREATE TABLE lj_products (id SERIAL PRIMARY KEY, name TEXT NOT NULL)")
        .expect("create products");
    Spi::run("INSERT INTO lj_products VALUES (1, 'Widget'), (2, 'Gadget')").expect("seed products");
    Spi::run(
        "INSERT INTO lj_orders (product_id, region, amount) VALUES \
         (1, 'US', 100), (1, 'EU', 200), (2, 'US', 50), (999, 'US', 75)",
    ).expect("seed orders");

    // GROUP BY region (non-nullable) to avoid NULL group key issue
    crate::create_reflex_ivm(
        "lj_view",
        "SELECT o.region, SUM(o.amount) AS total, COUNT(*) AS cnt \
         FROM lj_orders o LEFT JOIN lj_products p ON o.product_id = p.id \
         GROUP BY o.region",
        None,
        None,
        None,
    );

    // Verify initial: US=100+50+75=225, EU=200
    let us = Spi::get_one::<i64>(
        "SELECT total FROM lj_view WHERE region = 'US'",
    ).expect("q").expect("v");
    assert_eq!(us, 225i64);

    // INSERT unmatched order (no product) → still counted in LEFT JOIN
    Spi::run("INSERT INTO lj_orders (product_id, region, amount) VALUES (NULL, 'EU', 30)")
        .expect("insert unmatched");
    let eu = Spi::get_one::<i64>(
        "SELECT total FROM lj_view WHERE region = 'EU'",
    ).expect("q").expect("v");
    assert_eq!(eu, 230i64); // 200+30

    // DELETE
    Spi::run("DELETE FROM lj_orders WHERE amount = 100").expect("delete");
    let us_del = Spi::get_one::<i64>(
        "SELECT total FROM lj_view WHERE region = 'US'",
    ).expect("q").expect("v");
    assert_eq!(us_del, 125i64); // 50+75

    // EXCEPT correctness
    let mismatches = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM (
            SELECT region, total, cnt FROM lj_view
            EXCEPT
            SELECT o.region, SUM(o.amount), COUNT(*)
            FROM lj_orders o LEFT JOIN lj_products p ON o.product_id = p.id
            GROUP BY o.region
        ) x",
    ).expect("q").expect("v");
    assert_eq!(mismatches, 0, "LEFT JOIN view should match source");
}

/// RIGHT JOIN passthrough: rows from right table even when left has no match
#[pg_test]
fn test_right_join_passthrough() {
    Spi::run("CREATE TABLE rj_items (id SERIAL PRIMARY KEY, cat_id INT, val INT NOT NULL)")
        .expect("create items");
    Spi::run("CREATE TABLE rj_cats (id SERIAL PRIMARY KEY, name TEXT NOT NULL)")
        .expect("create cats");
    Spi::run("INSERT INTO rj_cats VALUES (1, 'A'), (2, 'B'), (3, 'C')").expect("seed cats");
    Spi::run("INSERT INTO rj_items (cat_id, val) VALUES (1, 10), (1, 20)").expect("seed items");

    // RIGHT JOIN: all categories appear, even those with no items
    crate::create_reflex_ivm(
        "rj_view",
        "SELECT i.id AS item_id, i.val, c.name AS cat_name \
         FROM rj_items i RIGHT JOIN rj_cats c ON i.cat_id = c.id",
        None,
        None,
        None,
    );

    // All 3 cats should appear (A has 2 items, B and C have NULL items)
    let count = Spi::get_one::<i64>("SELECT COUNT(*) FROM rj_view")
        .expect("q").expect("v");
    assert_eq!(count, 4); // 2 items + 2 NULL rows for B and C

    // INSERT item for cat B
    Spi::run("INSERT INTO rj_items (cat_id, val) VALUES (2, 30)").expect("insert");
    let b_count = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM rj_view WHERE cat_name = 'B' AND item_id IS NOT NULL",
    ).expect("q").expect("v");
    assert_eq!(b_count, 1);

    // DELETE item from cat A
    Spi::run("DELETE FROM rj_items WHERE val = 10").expect("delete");

    // EXCEPT correctness
    let mismatches = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM (
            SELECT item_id, val, cat_name FROM rj_view
            EXCEPT
            SELECT i.id, i.val, c.name
            FROM rj_items i RIGHT JOIN rj_cats c ON i.cat_id = c.id
        ) x",
    ).expect("q").expect("v");
    assert_eq!(mismatches, 0, "RIGHT JOIN view should match source");
}

/// Cast propagation: SUM(x)::BIGINT should produce correct values and BIGINT column type
#[pg_test]
fn test_cast_propagation_sum_bigint() {
    Spi::run("CREATE TABLE cast_src (id SERIAL, grp TEXT NOT NULL, val INT NOT NULL)")
        .expect("create");
    Spi::run("INSERT INTO cast_src (grp, val) VALUES ('X', 100), ('X', 200), ('Y', 50)")
        .expect("seed");

    crate::create_reflex_ivm(
        "cast_view",
        "SELECT grp, SUM(val)::BIGINT AS total FROM cast_src GROUP BY grp",
        None,
        None,
        None,
    );

    // Verify initial values
    let x = Spi::get_one::<i64>("SELECT total FROM cast_view WHERE grp = 'X'")
        .expect("q").expect("v");
    assert_eq!(x, 300);

    // Verify column type is BIGINT (int8)
    let col_type = Spi::get_one::<String>(
        "SELECT data_type FROM information_schema.columns \
         WHERE table_name = 'cast_view' AND column_name = 'total'",
    ).expect("q").expect("v");
    assert_eq!(col_type, "bigint", "Column should be BIGINT due to cast");

    // INSERT
    Spi::run("INSERT INTO cast_src (grp, val) VALUES ('X', 400)").expect("insert");
    let x_after = Spi::get_one::<i64>("SELECT total FROM cast_view WHERE grp = 'X'")
        .expect("q").expect("v");
    assert_eq!(x_after, 700);

    // DELETE
    Spi::run("DELETE FROM cast_src WHERE val = 200").expect("delete");
    let x_del = Spi::get_one::<i64>("SELECT total FROM cast_view WHERE grp = 'X'")
        .expect("q").expect("v");
    assert_eq!(x_del, 500);

    // UPDATE
    Spi::run("UPDATE cast_src SET val = 999 WHERE grp = 'Y'").expect("update");
    let y = Spi::get_one::<i64>("SELECT total FROM cast_view WHERE grp = 'Y'")
        .expect("q").expect("v");
    assert_eq!(y, 999);

    // EXCEPT correctness
    let mismatches = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM (
            SELECT grp, total FROM cast_view
            EXCEPT
            SELECT grp, SUM(val)::BIGINT FROM cast_src GROUP BY grp
        ) x",
    ).expect("q").expect("v");
    assert_eq!(mismatches, 0, "Cast view should match source");
}

/// Subquery in FROM with a simple filter: the inner table still gets triggers.
/// Note: subqueries with aggregation inside don't work incrementally because
/// the trigger replaces the inner table with the transition table, but the
/// aggregation in the subquery then only sees the delta rows, not the full table.
/// This test uses a simple WHERE filter instead.
#[pg_test]
fn test_subquery_in_from_with_trigger() {
    Spi::run("CREATE TABLE sq_src (id SERIAL PRIMARY KEY, val INT NOT NULL, active BOOLEAN NOT NULL)")
        .expect("create");
    Spi::run("INSERT INTO sq_src (val, active) VALUES (10, true), (20, true), (30, false)")
        .expect("seed");

    // Subquery with simple WHERE filter (no aggregation inside)
    crate::create_reflex_ivm(
        "sq_view",
        "SELECT id, val FROM (SELECT id, val FROM sq_src WHERE active = true) AS sub",
        None,
        None,
        None,
    );

    let count = Spi::get_one::<i64>("SELECT COUNT(*) FROM sq_view")
        .expect("q").expect("v");
    assert_eq!(count, 2); // only active rows

    // INSERT active row → appears via trigger on sq_src
    Spi::run("INSERT INTO sq_src (val, active) VALUES (40, true)").expect("insert active");
    assert_eq!(
        Spi::get_one::<i64>("SELECT COUNT(*) FROM sq_view").expect("q").expect("v"),
        3,
    );

    // INSERT inactive row → does not appear
    Spi::run("INSERT INTO sq_src (val, active) VALUES (50, false)").expect("insert inactive");
    assert_eq!(
        Spi::get_one::<i64>("SELECT COUNT(*) FROM sq_view").expect("q").expect("v"),
        3,
    );

    // DELETE active row
    Spi::run("DELETE FROM sq_src WHERE val = 10").expect("delete");
    assert_eq!(
        Spi::get_one::<i64>("SELECT COUNT(*) FROM sq_view").expect("q").expect("v"),
        2,
    );

    // EXCEPT correctness
    let mismatches = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM (
            SELECT id, val FROM sq_view
            EXCEPT
            SELECT id, val FROM sq_src WHERE active = true
        ) x",
    ).expect("q").expect("v");
    assert_eq!(mismatches, 0, "Subquery view should match filtered source");
}

/// Subquery as only source: the inner table still gets triggers via visitor recursion.
#[pg_test]
fn test_subquery_only_source() {
    Spi::run("CREATE TABLE sqo_src (id SERIAL PRIMARY KEY, val INT NOT NULL)")
        .expect("create");
    Spi::run("INSERT INTO sqo_src (val) VALUES (10), (20), (-5)").expect("seed");

    crate::create_reflex_ivm(
        "sqo_view",
        "SELECT id, val FROM (SELECT id, val FROM sqo_src WHERE val > 0) AS sub",
        None,
        None,
        None,
    );

    // Initial: only positive values
    let count = Spi::get_one::<i64>("SELECT COUNT(*) FROM sqo_view")
        .expect("q").expect("v");
    assert_eq!(count, 2);

    // INSERT positive → appears
    Spi::run("INSERT INTO sqo_src (val) VALUES (30)").expect("insert positive");
    assert_eq!(
        Spi::get_one::<i64>("SELECT COUNT(*) FROM sqo_view").expect("q").expect("v"),
        3,
    );

    // INSERT negative → does not appear (filtered by subquery WHERE)
    Spi::run("INSERT INTO sqo_src (val) VALUES (-10)").expect("insert negative");
    assert_eq!(
        Spi::get_one::<i64>("SELECT COUNT(*) FROM sqo_view").expect("q").expect("v"),
        3,
    );

    // DELETE positive row
    Spi::run("DELETE FROM sqo_src WHERE val = 20").expect("delete");
    assert_eq!(
        Spi::get_one::<i64>("SELECT COUNT(*) FROM sqo_view").expect("q").expect("v"),
        2,
    );

    // EXCEPT correctness
    let mismatches = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM (
            SELECT id, val FROM sqo_view
            EXCEPT
            SELECT id, val FROM sqo_src WHERE val > 0
        ) x",
    ).expect("q").expect("v");
    assert_eq!(mismatches, 0, "Subquery view should match filtered source");
}

// ========================================================================
// Combination tests — features combined
// ========================================================================

/// CTE containing a JOIN, outer query with HAVING
#[pg_test]
fn test_combo_cte_join_having() {
    Spi::run("CREATE TABLE cc_cjh1 (id INT PRIMARY KEY, grp TEXT)").expect("create1");
    Spi::run("CREATE TABLE cc_cjh2 (id INT, val INT NOT NULL)").expect("create2");
    Spi::run("INSERT INTO cc_cjh1 VALUES (1, 'a'), (2, 'a'), (3, 'b')").expect("seed1");
    Spi::run("INSERT INTO cc_cjh2 VALUES (1, 10), (2, 20), (3, 30)").expect("seed2");

    let sql = "WITH joined AS ( \
        SELECT cc_cjh1.grp, cc_cjh2.val FROM cc_cjh1 JOIN cc_cjh2 ON cc_cjh1.id = cc_cjh2.id \
    ) \
    SELECT grp, SUM(val) AS total FROM joined GROUP BY grp HAVING SUM(val) > 20";
    crate::create_reflex_ivm("cc_cjh_v", sql, None, None, None);
    assert_imv_correct("cc_cjh_v", sql);

    // Insert to push 'b' above threshold
    Spi::run("INSERT INTO cc_cjh1 VALUES (4, 'b')").expect("insert1");
    Spi::run("INSERT INTO cc_cjh2 VALUES (4, 25)").expect("insert2");
    assert_imv_correct("cc_cjh_v", sql);
}

/// DISTINCT + WHERE filter
#[pg_test]
fn test_combo_distinct_where() {
    Spi::run("CREATE TABLE cc_dw (id SERIAL PRIMARY KEY, grp TEXT, val INT NOT NULL)").expect("create");
    Spi::run("INSERT INTO cc_dw (grp, val) VALUES ('a', 10), ('a', 10), ('b', 20), ('b', 30), ('a', 20)").expect("seed");

    let sql = "SELECT DISTINCT grp, val FROM cc_dw WHERE val > 5";
    crate::create_reflex_ivm("cc_dw_v", sql, Some("grp, val"), None, None);
    assert_imv_correct("cc_dw_v", sql);

    // Insert duplicate — should not add new row
    Spi::run("INSERT INTO cc_dw (grp, val) VALUES ('a', 10)").expect("insert");
    assert_imv_correct("cc_dw_v", sql);

    // Delete one copy of duplicate — DISTINCT should still show it
    Spi::run("DELETE FROM cc_dw WHERE id = (SELECT MIN(id) FROM cc_dw WHERE grp = 'a' AND val = 10)").expect("delete");
    assert_imv_correct("cc_dw_v", sql);
}

/// UNION ALL where both operands have GROUP BY
#[pg_test]
fn test_combo_union_aggregate_operands() {
    Spi::run("CREATE TABLE cc_ua1 (id SERIAL PRIMARY KEY, grp TEXT, val INT NOT NULL)").expect("create1");
    Spi::run("CREATE TABLE cc_ua2 (id SERIAL PRIMARY KEY, grp TEXT, val INT NOT NULL)").expect("create2");
    Spi::run("INSERT INTO cc_ua1 (grp, val) VALUES ('a', 10), ('a', 20), ('b', 30)").expect("seed1");
    Spi::run("INSERT INTO cc_ua2 (grp, val) VALUES ('a', 100), ('c', 200)").expect("seed2");

    let sql = "SELECT grp, SUM(val) AS total FROM cc_ua1 GROUP BY grp \
               UNION ALL \
               SELECT grp, SUM(val) AS total FROM cc_ua2 GROUP BY grp";
    crate::create_reflex_ivm("cc_ua_v", sql, None, None, None);
    assert_imv_correct("cc_ua_v", sql);

    // Insert into first table
    Spi::run("INSERT INTO cc_ua1 (grp, val) VALUES ('b', 5)").expect("insert1");
    assert_imv_correct("cc_ua_v", sql);

    // Insert into second table
    Spi::run("INSERT INTO cc_ua2 (grp, val) VALUES ('c', 50)").expect("insert2");
    assert_imv_correct("cc_ua_v", sql);

    // Delete from first
    Spi::run("DELETE FROM cc_ua1 WHERE grp = 'a' AND val = 10").expect("delete");
    assert_imv_correct("cc_ua_v", sql);
}

/// Cast on multiple aggregates
#[pg_test]
fn test_combo_cast_multiple_aggregates() {
    Spi::run("CREATE TABLE cc_cast (id SERIAL PRIMARY KEY, grp TEXT, val INT NOT NULL)").expect("create");
    Spi::run("INSERT INTO cc_cast (grp, val) VALUES ('a', 10), ('a', 20), ('b', 30)").expect("seed");

    let sql = "SELECT grp, SUM(val)::BIGINT AS s, COUNT(*)::INT AS c FROM cc_cast GROUP BY grp";
    crate::create_reflex_ivm("cc_cast_v", sql, None, None, None);
    assert_imv_correct("cc_cast_v", sql);

    Spi::run("INSERT INTO cc_cast (grp, val) VALUES ('a', 5)").expect("insert");
    assert_imv_correct("cc_cast_v", sql);

    Spi::run("DELETE FROM cc_cast WHERE grp = 'b'").expect("delete");
    assert_imv_correct("cc_cast_v", sql);
}

/// HAVING with AVG threshold
#[pg_test]
fn test_combo_having_with_avg() {
    Spi::run("CREATE TABLE cc_havg (id SERIAL PRIMARY KEY, grp TEXT, val INT NOT NULL)").expect("create");
    Spi::run("INSERT INTO cc_havg (grp, val) VALUES ('a', 10), ('a', 20), ('a', 30), ('b', 5), ('b', 100)").expect("seed");

    let sql = "SELECT grp, AVG(val) AS mean, COUNT(*) AS cnt FROM cc_havg GROUP BY grp HAVING AVG(val) > 15";
    crate::create_reflex_ivm("cc_havg_v", sql, None, None, None);
    assert_imv_correct("cc_havg_v", sql);

    // Drop 'a' average by adding low values
    Spi::run("INSERT INTO cc_havg (grp, val) VALUES ('a', 1), ('a', 1), ('a', 1)").expect("insert");
    assert_imv_correct("cc_havg_v", sql);

    // Raise 'b' average
    Spi::run("DELETE FROM cc_havg WHERE grp = 'b' AND val = 5").expect("delete");
    assert_imv_correct("cc_havg_v", sql);
}

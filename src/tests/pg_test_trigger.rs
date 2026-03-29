
#[pg_test]
fn test_triggers_created() {
    Spi::run("CREATE TABLE test_trig_src (id SERIAL, grp TEXT, val NUMERIC)")
        .expect("create table");
    Spi::run("INSERT INTO test_trig_src (grp, val) VALUES ('a', 1)").expect("insert");

    crate::create_reflex_ivm(
        "test_trig_view",
        "SELECT grp, SUM(val) AS total FROM test_trig_src GROUP BY grp",
        None,
        None,
        None,
    );

    // Check all 4 consolidated triggers exist (INSERT, DELETE, UPDATE, TRUNCATE)
    let count = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM pg_trigger
         WHERE tgname LIKE '__reflex_trigger_%_on_test_trig_src'",
    )
    .expect("query")
    .expect("count");
    assert_eq!(count, 4);
}

// ---- Trigger behavior tests ----

#[pg_test]
fn test_trigger_insert_updates_view() {
    Spi::run("CREATE TABLE trig_ins_src (id SERIAL, city TEXT, amount NUMERIC)")
        .expect("create table");
    Spi::run("INSERT INTO trig_ins_src (city, amount) VALUES ('Paris', 100), ('London', 200)")
        .expect("seed");

    crate::create_reflex_ivm(
        "trig_ins_view",
        "SELECT city, SUM(amount) AS total FROM trig_ins_src GROUP BY city",
        None,
        None,
        None,
    );

    // Insert more rows AFTER IMV creation — triggers should fire
    Spi::run("INSERT INTO trig_ins_src (city, amount) VALUES ('Paris', 50), ('Berlin', 300)")
        .expect("insert");

    let paris = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT total FROM trig_ins_view WHERE city = 'Paris'",
    )
    .expect("query")
    .expect("value");
    assert_eq!(paris.to_string(), "150"); // 100 + 50

    let berlin = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT total FROM trig_ins_view WHERE city = 'Berlin'",
    )
    .expect("query")
    .expect("value");
    assert_eq!(berlin.to_string(), "300");
}

#[pg_test]
fn test_trigger_delete_updates_view() {
    Spi::run("CREATE TABLE trig_del_src (id SERIAL, city TEXT, amount NUMERIC)")
        .expect("create table");
    Spi::run(
        "INSERT INTO trig_del_src (city, amount) VALUES
         ('Paris', 100), ('Paris', 200), ('London', 300)",
    )
    .expect("seed");

    crate::create_reflex_ivm(
        "trig_del_view",
        "SELECT city, SUM(amount) AS total FROM trig_del_src GROUP BY city",
        None,
        None,
        None,
    );

    // Delete one Paris row
    Spi::run("DELETE FROM trig_del_src WHERE amount = 100").expect("delete");

    let paris = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT total FROM trig_del_view WHERE city = 'Paris'",
    )
    .expect("query")
    .expect("value");
    assert_eq!(paris.to_string(), "200"); // 300 - 100
}

#[pg_test]
fn test_trigger_delete_all_removes_group() {
    Spi::run("CREATE TABLE trig_delall_src (id SERIAL, city TEXT, amount NUMERIC)")
        .expect("create table");
    Spi::run("INSERT INTO trig_delall_src (city, amount) VALUES ('X', 10), ('Y', 20)")
        .expect("seed");

    crate::create_reflex_ivm(
        "trig_delall_view",
        "SELECT city, SUM(amount) AS total FROM trig_delall_src GROUP BY city",
        None,
        None,
        None,
    );

    // Delete all rows for city 'X'
    Spi::run("DELETE FROM trig_delall_src WHERE city = 'X'").expect("delete");

    // 'X' should no longer appear in the target (ivm_count = 0)
    let count = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM trig_delall_view WHERE city = 'X'",
    )
    .expect("query")
    .expect("count");
    assert_eq!(count, 0);

    // 'Y' should still be there
    let y_total = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT total FROM trig_delall_view WHERE city = 'Y'",
    )
    .expect("query")
    .expect("value");
    assert_eq!(y_total.to_string(), "20");
}

#[pg_test]
fn test_trigger_update_correctness() {
    Spi::run("CREATE TABLE trig_upd_src (id SERIAL, city TEXT, amount NUMERIC)")
        .expect("create table");
    Spi::run("INSERT INTO trig_upd_src (city, amount) VALUES ('A', 100), ('B', 200)")
        .expect("seed");

    crate::create_reflex_ivm(
        "trig_upd_view",
        "SELECT city, SUM(amount) AS total FROM trig_upd_src GROUP BY city",
        None,
        None,
        None,
    );

    // Update: change amount for city 'A'
    Spi::run("UPDATE trig_upd_src SET amount = 150 WHERE city = 'A'").expect("update");

    let a_total = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT total FROM trig_upd_view WHERE city = 'A'",
    )
    .expect("query")
    .expect("value");
    assert_eq!(a_total.to_string(), "150");
}

#[pg_test]
fn test_trigger_avg_insert_delete() {
    Spi::run("CREATE TABLE trig_avg_src (id SERIAL, dept TEXT, salary NUMERIC)")
        .expect("create table");
    Spi::run(
        "INSERT INTO trig_avg_src (dept, salary) VALUES ('eng', 100), ('eng', 200)",
    )
    .expect("seed");

    crate::create_reflex_ivm(
        "trig_avg_view",
        "SELECT dept, AVG(salary) AS avg_sal FROM trig_avg_src GROUP BY dept",
        None,
        None,
        None,
    );

    // Insert another row
    Spi::run("INSERT INTO trig_avg_src (dept, salary) VALUES ('eng', 300)")
        .expect("insert");

    // AVG should be (100+200+300)/3 = 200
    let avg = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT ROUND(avg_sal::numeric, 0) FROM trig_avg_view WHERE dept = 'eng'",
    )
    .expect("query")
    .expect("value");
    assert_eq!(avg.to_string(), "200");

    // Delete the 100 row
    Spi::run("DELETE FROM trig_avg_src WHERE salary = 100").expect("delete");

    // AVG should be (200+300)/2 = 250
    let avg2 = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT ROUND(avg_sal::numeric, 0) FROM trig_avg_view WHERE dept = 'eng'",
    )
    .expect("query")
    .expect("value");
    assert_eq!(avg2.to_string(), "250");
}

#[pg_test]
fn test_trigger_distinct_ref_counting() {
    Spi::run("CREATE TABLE trig_dist_src (id SERIAL, country TEXT)")
        .expect("create table");
    Spi::run("INSERT INTO trig_dist_src (country) VALUES ('US'), ('US'), ('FR')")
        .expect("seed");

    crate::create_reflex_ivm(
        "trig_dist_view",
        "SELECT DISTINCT country FROM trig_dist_src",
        None,
        None,
        None,
    );

    // Delete one 'US' — should still appear (ref count > 0)
    Spi::run("DELETE FROM trig_dist_src WHERE id = 1").expect("delete one US");
    let us_count = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM trig_dist_view WHERE country = 'US'",
    )
    .expect("query")
    .expect("count");
    assert_eq!(us_count, 1); // Still visible

    // Delete last 'US' — should disappear
    Spi::run("DELETE FROM trig_dist_src WHERE country = 'US'").expect("delete last US");
    let us_gone = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM trig_dist_view WHERE country = 'US'",
    )
    .expect("query")
    .expect("count");
    assert_eq!(us_gone, 0);

    // FR should still be there
    let fr = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM trig_dist_view WHERE country = 'FR'",
    )
    .expect("query")
    .expect("count");
    assert_eq!(fr, 1);
}

#[pg_test]
fn test_trigger_bulk_insert() {
    Spi::run("CREATE TABLE trig_bulk_src (id SERIAL, grp TEXT, val NUMERIC)")
        .expect("create table");
    Spi::run("INSERT INTO trig_bulk_src (grp, val) VALUES ('X', 1)").expect("seed");

    crate::create_reflex_ivm(
        "trig_bulk_view",
        "SELECT grp, SUM(val) AS total, COUNT(*) AS cnt FROM trig_bulk_src GROUP BY grp",
        None,
        None,
        None,
    );

    // Bulk insert 100 rows for group 'X'
    Spi::run(
        "INSERT INTO trig_bulk_src (grp, val)
         SELECT 'X', generate_series(1, 100)",
    )
    .expect("bulk insert");

    let cnt = Spi::get_one::<i64>(
        "SELECT cnt FROM trig_bulk_view WHERE grp = 'X'",
    )
    .expect("query")
    .expect("count");
    assert_eq!(cnt, 101); // 1 seed + 100 bulk
}

#[pg_test]
fn test_logged_trigger_works() {
    Spi::run("CREATE TABLE log_trg (id SERIAL, region TEXT, amount NUMERIC)")
        .expect("create table");
    Spi::run("INSERT INTO log_trg (region, amount) VALUES ('A', 10), ('B', 20)")
        .expect("insert");

    crate::create_reflex_ivm(
        "log_trg_view",
        "SELECT region, SUM(amount) AS total FROM log_trg GROUP BY region",
        None,
        Some("LOGGED"),
        None,
    );

    // INSERT trigger
    Spi::run("INSERT INTO log_trg (region, amount) VALUES ('A', 5)").expect("insert");
    let a_total = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT total FROM log_trg_view WHERE region = 'A'",
    ).expect("query").expect("value");
    assert_eq!(a_total.to_string(), "15");

    // DELETE trigger
    Spi::run("DELETE FROM log_trg WHERE amount = 20").expect("delete");
    let b_count = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM log_trg_view WHERE region = 'B'",
    ).expect("query").expect("value");
    assert_eq!(b_count, 0, "Region B should be gone after deleting its only row");
}

#[pg_test]
fn test_default_is_unlogged() {
    Spi::run("CREATE TABLE def_orders (id SERIAL, city TEXT, amount NUMERIC)")
        .expect("create table");
    Spi::run("INSERT INTO def_orders (city, amount) VALUES ('Paris', 100)")
        .expect("insert");

    crate::create_reflex_ivm(
        "def_city_totals",
        "SELECT city, SUM(amount) AS total FROM def_orders GROUP BY city",
        None,
        None,
        None,
    );

    // Verify tables are UNLOGGED (relpersistence = 'u')
    let target_persist = Spi::get_one::<String>(
        "SELECT relpersistence::text FROM pg_class WHERE relname = 'def_city_totals'",
    ).expect("query").expect("value");
    assert_eq!(target_persist, "u", "Default target should be unlogged");

    let intermediate_persist = Spi::get_one::<String>(
        "SELECT relpersistence::text FROM pg_class WHERE relname = '__reflex_intermediate_def_city_totals'",
    ).expect("query").expect("value");
    assert_eq!(intermediate_persist, "u", "Default intermediate should be unlogged");
}

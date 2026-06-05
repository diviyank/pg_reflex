
#[pg_test]
fn test_filter_sum_basic() {
    Spi::run("CREATE TABLE filt_sum (city TEXT, amount BIGINT, active BOOLEAN)").expect("create");
    Spi::run("INSERT INTO filt_sum VALUES ('A', 10, true), ('A', 20, false), ('B', 30, true)").expect("seed");

    let result = crate::create_reflex_ivm("filt_sum_v",
        "SELECT city, SUM(amount) FILTER (WHERE active) AS active_total FROM filt_sum GROUP BY city",
        None, None, None, None);
    assert!(!result.starts_with("ERROR"), "Should succeed: {}", result);

    // A: only 10 counted (20 excluded), B: 30
    let a_total = Spi::get_one::<i64>("SELECT active_total::BIGINT FROM filt_sum_v WHERE city = 'A'")
        .expect("query").expect("null");
    assert_eq!(a_total, 10);
    let b_total = Spi::get_one::<i64>("SELECT active_total::BIGINT FROM filt_sum_v WHERE city = 'B'")
        .expect("query").expect("null");
    assert_eq!(b_total, 30);

    // INSERT active row -> should be counted
    Spi::run("INSERT INTO filt_sum VALUES ('A', 5, true)").expect("insert");
    let a_total = Spi::get_one::<i64>("SELECT active_total::BIGINT FROM filt_sum_v WHERE city = 'A'")
        .expect("query").expect("null");
    assert_eq!(a_total, 15);

    // INSERT inactive row -> should NOT be counted
    Spi::run("INSERT INTO filt_sum VALUES ('A', 100, false)").expect("insert");
    let a_total = Spi::get_one::<i64>("SELECT active_total::BIGINT FROM filt_sum_v WHERE city = 'A'")
        .expect("query").expect("null");
    assert_eq!(a_total, 15);

    crate::drop_reflex_ivm("filt_sum_v");
}

#[pg_test]
fn test_filter_count_star() {
    Spi::run("CREATE TABLE filt_cnt (city TEXT, active BOOLEAN)").expect("create");
    Spi::run("INSERT INTO filt_cnt VALUES ('A', true), ('A', false), ('A', true), ('B', false)").expect("seed");

    let result = crate::create_reflex_ivm("filt_cnt_v",
        "SELECT city, COUNT(*) FILTER (WHERE active) AS active_cnt FROM filt_cnt GROUP BY city",
        None, None, None, None);
    assert!(!result.starts_with("ERROR"), "Should succeed: {}", result);

    let a_cnt = Spi::get_one::<i64>("SELECT active_cnt::BIGINT FROM filt_cnt_v WHERE city = 'A'")
        .expect("query").expect("null");
    assert_eq!(a_cnt, 2);

    // B has no active rows -> count should be 0
    let b_cnt = Spi::get_one::<i64>("SELECT active_cnt::BIGINT FROM filt_cnt_v WHERE city = 'B'")
        .expect("query").expect("null");
    assert_eq!(b_cnt, 0);

    // DELETE an active row
    Spi::run("DELETE FROM filt_cnt WHERE ctid = (SELECT ctid FROM filt_cnt WHERE city = 'A' AND active = true LIMIT 1)").expect("delete");
    let a_cnt = Spi::get_one::<i64>("SELECT active_cnt::BIGINT FROM filt_cnt_v WHERE city = 'A'")
        .expect("query").expect("null");
    assert_eq!(a_cnt, 1);

    crate::drop_reflex_ivm("filt_cnt_v");
}

#[pg_test]
fn test_filter_avg() {
    Spi::run("CREATE TABLE filt_avg (city TEXT, salary NUMERIC, active BOOLEAN)").expect("create");
    Spi::run("INSERT INTO filt_avg VALUES ('A', 100, true), ('A', 200, true), ('A', 999, false)").expect("seed");

    let result = crate::create_reflex_ivm("filt_avg_v",
        "SELECT city, AVG(salary) FILTER (WHERE active) AS avg_active FROM filt_avg GROUP BY city",
        None, None, None, None);
    assert!(!result.starts_with("ERROR"), "Should succeed: {}", result);

    // AVG of active rows: (100+200)/2 = 150
    let avg = Spi::get_one::<f64>("SELECT avg_active::FLOAT FROM filt_avg_v WHERE city = 'A'")
        .expect("query").expect("null");
    assert!((avg - 150.0).abs() < 0.01, "Expected ~150, got {}", avg);

    crate::drop_reflex_ivm("filt_avg_v");
}

#[pg_test]
fn test_filter_min_max() {
    Spi::run("CREATE TABLE filt_mm (city TEXT, val BIGINT, active BOOLEAN)").expect("create");
    Spi::run("INSERT INTO filt_mm VALUES ('A', 10, true), ('A', 1, false), ('A', 50, true), ('A', 100, false)").expect("seed");

    let result = crate::create_reflex_ivm("filt_mm_v",
        "SELECT city, MIN(val) FILTER (WHERE active) AS lo, MAX(val) FILTER (WHERE active) AS hi FROM filt_mm GROUP BY city",
        None, None, None, None);
    assert!(!result.starts_with("ERROR"), "Should succeed: {}", result);

    // Active values: 10, 50 -> MIN=10, MAX=50
    let lo = Spi::get_one::<i64>("SELECT lo::BIGINT FROM filt_mm_v WHERE city = 'A'")
        .expect("query").expect("null");
    let hi = Spi::get_one::<i64>("SELECT hi::BIGINT FROM filt_mm_v WHERE city = 'A'")
        .expect("query").expect("null");
    assert_eq!(lo, 10);
    assert_eq!(hi, 50);

    // DELETE the max active row
    Spi::run("DELETE FROM filt_mm WHERE city = 'A' AND val = 50 AND active = true").expect("delete");
    let hi = Spi::get_one::<i64>("SELECT hi::BIGINT FROM filt_mm_v WHERE city = 'A'")
        .expect("query").expect("null");
    assert_eq!(hi, 10, "After deleting 50, max active should be 10");

    crate::drop_reflex_ivm("filt_mm_v");
}

#[pg_test]
fn test_filter_with_group_by() {
    Spi::run("CREATE TABLE filt_gb (city TEXT, dept TEXT, amount BIGINT, billable BOOLEAN)").expect("create");
    Spi::run("INSERT INTO filt_gb VALUES
        ('A', 'eng', 100, true), ('A', 'eng', 50, false),
        ('A', 'sales', 200, true), ('B', 'eng', 300, true)").expect("seed");

    let result = crate::create_reflex_ivm("filt_gb_v",
        "SELECT city, SUM(amount) FILTER (WHERE billable) AS billable_total FROM filt_gb GROUP BY city",
        None, None, None, None);
    assert!(!result.starts_with("ERROR"), "Should succeed: {}", result);

    let a = Spi::get_one::<i64>("SELECT billable_total::BIGINT FROM filt_gb_v WHERE city = 'A'")
        .expect("query").expect("null");
    assert_eq!(a, 300); // 100 + 200, the 50 is not billable

    crate::drop_reflex_ivm("filt_gb_v");
}

#[pg_test]
fn test_filter_with_join() {
    Spi::run("CREATE TABLE filt_j1 (id INT PRIMARY KEY, city TEXT)").expect("create");
    Spi::run("CREATE TABLE filt_j2 (id INT, amount BIGINT, premium BOOLEAN)").expect("create");
    Spi::run("INSERT INTO filt_j1 VALUES (1, 'A'), (2, 'B')").expect("seed1");
    Spi::run("INSERT INTO filt_j2 VALUES (1, 100, true), (1, 50, false), (2, 200, true)").expect("seed2");

    let result = crate::create_reflex_ivm("filt_j_v",
        "SELECT j1.city, SUM(j2.amount) FILTER (WHERE j2.premium) AS premium_total \
         FROM filt_j1 j1 JOIN filt_j2 j2 ON j1.id = j2.id GROUP BY j1.city",
        None, None, None, None);
    assert!(!result.starts_with("ERROR"), "Should succeed: {}", result);

    let a = Spi::get_one::<i64>("SELECT premium_total::BIGINT FROM filt_j_v WHERE city = 'A'")
        .expect("query").expect("null");
    assert_eq!(a, 100); // only the premium=true row

    crate::drop_reflex_ivm("filt_j_v");
}

#[pg_test]
fn test_filter_update_predicate_column() {
    Spi::run("CREATE TABLE filt_upd (id SERIAL PRIMARY KEY, city TEXT, amount BIGINT, active BOOLEAN)").expect("create");
    Spi::run("INSERT INTO filt_upd (city, amount, active) VALUES ('A', 100, true), ('A', 50, true)").expect("seed");

    let result = crate::create_reflex_ivm("filt_upd_v",
        "SELECT city, SUM(amount) FILTER (WHERE active) AS active_total FROM filt_upd GROUP BY city",
        None, None, None, None);
    assert!(!result.starts_with("ERROR"), "Should succeed: {}", result);

    let total = Spi::get_one::<i64>("SELECT active_total::BIGINT FROM filt_upd_v WHERE city = 'A'")
        .expect("query").expect("null");
    assert_eq!(total, 150);

    // Flip active->false on the 100-amount row
    Spi::run("UPDATE filt_upd SET active = false WHERE amount = 100").expect("update");
    let total = Spi::get_one::<i64>("SELECT active_total::BIGINT FROM filt_upd_v WHERE city = 'A'")
        .expect("query").expect("null");
    assert_eq!(total, 50, "After deactivating 100, only 50 should remain");

    crate::drop_reflex_ivm("filt_upd_v");
}

#[pg_test]
fn test_filter_multiple_aggregates() {
    Spi::run("CREATE TABLE filt_multi (city TEXT, amount BIGINT, active BOOLEAN, premium BOOLEAN)").expect("create");
    Spi::run("INSERT INTO filt_multi VALUES
        ('A', 10, true, true), ('A', 20, true, false), ('A', 30, false, true)").expect("seed");

    let result = crate::create_reflex_ivm("filt_multi_v",
        "SELECT city, \
         SUM(amount) FILTER (WHERE active) AS active_sum, \
         SUM(amount) FILTER (WHERE premium) AS premium_sum, \
         COUNT(*) FILTER (WHERE active AND premium) AS both_cnt \
         FROM filt_multi GROUP BY city",
        None, None, None, None);
    assert!(!result.starts_with("ERROR"), "Should succeed: {}", result);

    let active = Spi::get_one::<i64>("SELECT active_sum::BIGINT FROM filt_multi_v WHERE city = 'A'")
        .expect("query").expect("null");
    let premium = Spi::get_one::<i64>("SELECT premium_sum::BIGINT FROM filt_multi_v WHERE city = 'A'")
        .expect("query").expect("null");
    let both = Spi::get_one::<i64>("SELECT both_cnt::BIGINT FROM filt_multi_v WHERE city = 'A'")
        .expect("query").expect("null");
    assert_eq!(active, 30);  // 10+20
    assert_eq!(premium, 40); // 10+30
    assert_eq!(both, 1);     // only (10, true, true)

    crate::drop_reflex_ivm("filt_multi_v");
}

#[pg_test]
fn test_filter_correctness_oracle() {
    Spi::run("CREATE TABLE filt_oracle (city TEXT, amount BIGINT, active BOOLEAN)").expect("create");
    Spi::run("INSERT INTO filt_oracle VALUES
        ('A', 10, true), ('A', 20, false), ('A', 30, true),
        ('B', 5, true), ('B', 15, false)").expect("seed");

    let result = crate::create_reflex_ivm("filt_oracle_v",
        "SELECT city, SUM(amount) FILTER (WHERE active) AS s, COUNT(*) FILTER (WHERE active) AS c FROM filt_oracle GROUP BY city",
        None, None, None, None);
    assert!(!result.starts_with("ERROR"), "Should succeed: {}", result);

    // Perform mutations
    Spi::run("INSERT INTO filt_oracle VALUES ('A', 40, true), ('C', 100, false)").expect("ins");
    Spi::run("DELETE FROM filt_oracle WHERE city = 'B' AND active = true").expect("del");
    Spi::run("UPDATE filt_oracle SET active = true WHERE city = 'A' AND amount = 20").expect("upd");

    // Oracle check: compare IMV with fresh query
    let fresh_sql = "SELECT city, SUM(amount) FILTER (WHERE active) AS s, COUNT(*) FILTER (WHERE active) AS c FROM filt_oracle GROUP BY city";
    assert_imv_correct("filt_oracle_v", fresh_sql);

    crate::drop_reflex_ivm("filt_oracle_v");
}

/// Regression — a query-level `WHERE` predicate on an aggregate IMV (here on
/// the measure column itself) must survive incremental maintenance. The stored
/// `where_predicate` is evaluated against the flat transition table in the
/// per-op early-skip; if it keeps the original `source.col` qualifier it errors
/// with `missing FROM-clause entry for table "<source>"`. INSERT of a new
/// in-filter row.
#[pg_test]
fn test_query_where_filter_insert_maintenance() {
    Spi::run("CREATE TABLE qwf_ins (k INT NOT NULL, amt INT NOT NULL)").expect("create");
    Spi::run("INSERT INTO qwf_ins SELECT g, 100 FROM generate_series(1, 100) g").expect("seed");
    let result = crate::create_reflex_ivm(
        "qwf_ins_v",
        "SELECT qwf_ins.k, SUM(qwf_ins.amt) AS total FROM qwf_ins WHERE qwf_ins.amt > 50 GROUP BY qwf_ins.k",
        None, None, None, None);
    assert!(!result.starts_with("ERROR"), "create: {}", result);
    let fresh = "SELECT qwf_ins.k, SUM(qwf_ins.amt) AS total FROM qwf_ins WHERE qwf_ins.amt > 50 GROUP BY qwf_ins.k";
    assert_imv_correct("qwf_ins_v", fresh);

    Spi::run("INSERT INTO qwf_ins VALUES (200, 100)").expect("insert in-filter");
    assert_imv_correct("qwf_ins_v", fresh);
}

/// Regression — query-level `WHERE` on an aggregate IMV, immediate mode, where a
/// row flips OUT→IN of the filter via UPDATE. Same `where_predicate` codegen
/// path as the INSERT case.
#[pg_test]
fn test_query_where_filter_update_flip_immediate() {
    Spi::run("CREATE TABLE qwf_upd (k INT NOT NULL, amt INT NOT NULL)").expect("create");
    Spi::run("INSERT INTO qwf_upd SELECT g, 100 FROM generate_series(1, 100) g").expect("seed");
    Spi::run("UPDATE qwf_upd SET amt = 10 WHERE k = 50").expect("start-out");
    let result = crate::create_reflex_ivm(
        "qwf_upd_v",
        "SELECT qwf_upd.k, SUM(qwf_upd.amt) AS total FROM qwf_upd WHERE qwf_upd.amt > 50 GROUP BY qwf_upd.k",
        None, None, None, None);
    assert!(!result.starts_with("ERROR"), "create: {}", result);
    let fresh = "SELECT qwf_upd.k, SUM(qwf_upd.amt) AS total FROM qwf_upd WHERE qwf_upd.amt > 50 GROUP BY qwf_upd.k";
    assert_imv_correct("qwf_upd_v", fresh);

    Spi::run("UPDATE qwf_upd SET amt = 100 WHERE k = 50").expect("flip-in");
    assert_imv_correct("qwf_upd_v", fresh);
}

/// Regression — query-level `WHERE` on an aggregate IMV, DEFERRED mode, OUT→IN
/// flip. Exercises the deferred flush's where_predicate early-skip, which runs
/// the same predicate against the staged transition rows.
#[pg_test]
fn test_query_where_filter_update_flip_deferred() {
    Spi::run("CREATE TABLE qwf_def (k INT NOT NULL, amt INT NOT NULL)").expect("create");
    Spi::run("INSERT INTO qwf_def SELECT g, 100 FROM generate_series(1, 100) g").expect("seed");
    Spi::run("UPDATE qwf_def SET amt = 10 WHERE k = 50").expect("start-out");
    let result = crate::create_reflex_ivm(
        "qwf_def_v",
        "SELECT qwf_def.k, SUM(qwf_def.amt) AS total FROM qwf_def WHERE qwf_def.amt > 50 GROUP BY qwf_def.k",
        None, None, Some("DEFERRED"), None);
    assert!(!result.starts_with("ERROR"), "create: {}", result);
    let fresh = "SELECT qwf_def.k, SUM(qwf_def.amt) AS total FROM qwf_def WHERE qwf_def.amt > 50 GROUP BY qwf_def.k";
    assert_imv_correct("qwf_def_v", fresh);

    Spi::run("UPDATE qwf_def SET amt = 100 WHERE k = 50").expect("flip-in");
    Spi::run("SELECT reflex_flush_deferred('qwf_def')").expect("flush");
    assert_imv_correct("qwf_def_v", fresh);
}

/// Regression — query-level `WHERE` on an aggregate IMV, DEFERRED mode, IN→OUT
/// flip (predicate EXIT). The deferred UPDATE trigger's relevance gate must
/// match the predicate on OLD *or* NEW rows; a NEW-only check silently drops
/// the row-leaves-filter update, leaking a stale aggregate group.
#[pg_test]
fn test_query_where_filter_update_exit_deferred() {
    Spi::run("CREATE TABLE qwf_exit (k INT NOT NULL, amt INT NOT NULL)").expect("create");
    Spi::run("INSERT INTO qwf_exit SELECT g, 100 FROM generate_series(1, 100) g").expect("seed");
    let result = crate::create_reflex_ivm(
        "qwf_exit_v",
        "SELECT qwf_exit.k, SUM(qwf_exit.amt) AS total FROM qwf_exit WHERE qwf_exit.amt > 50 GROUP BY qwf_exit.k",
        None, None, Some("DEFERRED"), None);
    assert!(!result.starts_with("ERROR"), "create: {}", result);
    let fresh = "SELECT qwf_exit.k, SUM(qwf_exit.amt) AS total FROM qwf_exit WHERE qwf_exit.amt > 50 GROUP BY qwf_exit.k";
    assert_imv_correct("qwf_exit_v", fresh);

    // IN -> OUT: row k=50 leaves the filter and must be DELETEd from the IMV.
    Spi::run("UPDATE qwf_exit SET amt = 10 WHERE k = 50").expect("flip-out");
    Spi::run("SELECT reflex_flush_deferred('qwf_exit')").expect("flush");
    assert_imv_correct("qwf_exit_v", fresh);
}

/// Regression — passthrough IMV with a row-level `WHERE`, DEFERRED mode, IN→OUT
/// flip (predicate EXIT). The exited rows must be DELETEd from the IMV, not
/// retained as stale pre-update rows.
#[pg_test]
fn test_passthrough_where_filter_update_exit_deferred() {
    Spi::run("CREATE TABLE pwf_exit (id INT PRIMARY KEY, qty INT NOT NULL)").expect("create");
    Spi::run("INSERT INTO pwf_exit SELECT g, 5 FROM generate_series(1, 100) g").expect("seed");
    let result = crate::create_reflex_ivm(
        "pwf_exit_v",
        "SELECT pwf_exit.id, pwf_exit.qty FROM pwf_exit WHERE pwf_exit.qty > 0",
        Some("id"), None, Some("DEFERRED"), None);
    assert!(!result.starts_with("ERROR"), "create: {}", result);
    let fresh = "SELECT pwf_exit.id, pwf_exit.qty FROM pwf_exit WHERE pwf_exit.qty > 0";
    assert_imv_correct("pwf_exit_v", fresh);

    // IN -> OUT: ids 21..40 go negative and must be removed from the IMV.
    Spi::run("UPDATE pwf_exit SET qty = -5 WHERE id BETWEEN 21 AND 40").expect("flip-out");
    Spi::run("SELECT reflex_flush_deferred('pwf_exit')").expect("flush");
    assert_imv_correct("pwf_exit_v", fresh);
}


#[pg_test]
fn test_recursive_cte_rejected() {
    Spi::run("CREATE TABLE test_t1 (id INT)").expect("create table");
    let result = crate::create_reflex_ivm(
        "bad_view",
        "WITH RECURSIVE nums AS (SELECT 1 AS n UNION ALL SELECT n+1 FROM nums WHERE n < 10) SELECT n, COUNT(*) AS cnt FROM nums GROUP BY n",
        None,
        None,
        None,
    );
    assert!(result.starts_with("ERROR"));
    assert!(result.contains("RECURSIVE"));
}

#[pg_test]
fn test_unsupported_limit_rejected() {
    Spi::run("CREATE TABLE test_t2 (id INT)").expect("create table");
    let result =
        crate::create_reflex_ivm("bad_view2", "SELECT id, COUNT(*) AS cnt FROM test_t2 GROUP BY id LIMIT 10", None, None, None);
    assert!(result.starts_with("ERROR"));
}

#[pg_test]
fn test_malformed_sql_returns_error() {
    let result = crate::create_reflex_ivm("bad_sql_view", "SELEC broken garbage !!!", None, None, None);
    assert!(
        result.starts_with("ERROR"),
        "Malformed SQL should return error, got: {}",
        result
    );
    assert!(result.contains("parse"), "Error should mention parse failure");
}

#[pg_test]
fn test_special_chars_view_name_rejected() {
    Spi::run("CREATE TABLE vn_src (id SERIAL, val INT)").expect("create table");
    let r1 = crate::create_reflex_ivm("bad'name", "SELECT val FROM vn_src", None, None, None);
    assert!(r1.starts_with("ERROR"), "Single quote should be rejected");
    let r2 = crate::create_reflex_ivm("bad;name", "SELECT val FROM vn_src", None, None, None);
    assert!(r2.starts_with("ERROR"), "Semicolon should be rejected");
    let r3 = crate::create_reflex_ivm("bad--name", "SELECT val FROM vn_src", None, None, None);
    assert!(r3.starts_with("ERROR"), "SQL comment should be rejected");
    let r4 = crate::create_reflex_ivm("bad name", "SELECT val FROM vn_src", None, None, None);
    assert!(r4.starts_with("ERROR"), "Whitespace should be rejected");
    let r5 = crate::create_reflex_ivm("", "SELECT val FROM vn_src", None, None, None);
    assert!(r5.starts_with("ERROR"), "Empty name should be rejected");
}

#[pg_test]
fn test_drop_nonexistent_imv() {
    let result = crate::drop_reflex_ivm("nonexistent_view_xyz");
    assert!(result.starts_with("ERROR"), "Should error on non-existent IMV");
}

#[pg_test]
fn test_validate_view_name_unit() {
    // Valid names
    assert!(crate::validate_view_name("my_view").is_ok());
    assert!(crate::validate_view_name("schema1.my_view").is_ok());
    assert!(crate::validate_view_name("_private").is_ok());
    assert!(crate::validate_view_name("View123").is_ok());
    // Invalid names
    assert!(crate::validate_view_name("").is_err());
    assert!(crate::validate_view_name("bad'name").is_err());
    assert!(crate::validate_view_name("bad\"name").is_err());
    assert!(crate::validate_view_name("bad;name").is_err());
    assert!(crate::validate_view_name("bad name").is_err());
    assert!(crate::validate_view_name("bad\\name").is_err());
    assert!(crate::validate_view_name("1starts_with_digit").is_err());
    assert!(crate::validate_view_name(".starts_with_dot").is_err());
    assert!(crate::validate_view_name("bad..double").is_err());
    assert!(crate::validate_view_name("ends_with_dot.").is_err());
}

#[pg_test]
fn test_duplicate_view_name() {
    Spi::run("CREATE TABLE dup_src (id SERIAL, grp TEXT, val NUMERIC)").expect("create table");
    Spi::run("INSERT INTO dup_src (grp, val) VALUES ('a', 1)").expect("seed");
    let r1 = crate::create_reflex_ivm(
        "dup_view",
        "SELECT grp, SUM(val) AS total FROM dup_src GROUP BY grp",
        None,
        None,
        None,
    );
    assert_eq!(r1, "CREATE REFLEX INCREMENTAL VIEW");
    let r2 = crate::create_reflex_ivm(
        "dup_view",
        "SELECT grp, SUM(val) AS total FROM dup_src GROUP BY grp",
        None,
        None,
        None,
    );
    assert!(
        r2.starts_with("ERROR"),
        "Duplicate view name should return error, got: {}",
        r2
    );
}

#[pg_test]
fn test_where_clause_imv() {
    Spi::run(
        "CREATE TABLE wc_src (id SERIAL, grp TEXT NOT NULL, val NUMERIC NOT NULL, active BOOLEAN NOT NULL)",
    )
    .expect("create table");
    Spi::run(
        "INSERT INTO wc_src (grp, val, active) VALUES \
         ('A', 10, true), ('A', 20, false), ('B', 30, true), ('B', 40, true)",
    )
    .expect("seed");
    crate::create_reflex_ivm(
        "wc_view",
        "SELECT grp, SUM(val) AS total FROM wc_src WHERE active = true GROUP BY grp",
        None,
        None,
        None,
    );
    let a = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT total FROM wc_view WHERE grp = 'A'",
    )
    .expect("q")
    .expect("v");
    assert_eq!(a.to_string(), "10", "WHERE should filter out inactive row (val=20)");
    let b = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT total FROM wc_view WHERE grp = 'B'",
    )
    .expect("q")
    .expect("v");
    assert_eq!(b.to_string(), "70", "Both B rows are active: 30 + 40 = 70");
}

/// Subquery with aggregation in FROM should be rejected with a clear error.
#[pg_test]
fn test_subquery_with_aggregation_rejected() {
    Spi::run("CREATE TABLE sqr_products (id SERIAL PRIMARY KEY, name TEXT NOT NULL)")
        .expect("create");
    Spi::run("CREATE TABLE sqr_orders (id SERIAL, product_id INT NOT NULL, qty INT NOT NULL)")
        .expect("create");

    let result = crate::create_reflex_ivm(
        "sqr_view",
        "SELECT p.name, sub.total_qty \
         FROM sqr_products p \
         JOIN (SELECT product_id, SUM(qty) AS total_qty FROM sqr_orders GROUP BY product_id) AS sub \
         ON p.id = sub.product_id",
        None,
        None,
        None,
    );
    assert!(
        result.starts_with("ERROR:"),
        "Subquery with aggregation should be rejected, got: {}",
        result
    );
    assert!(
        result.contains("CTE"),
        "Error should suggest using CTE, got: {}",
        result
    );
}

#[pg_test]
fn test_invalid_storage_mode() {
    Spi::run("CREATE TABLE inv_stor (id SERIAL, val INT)").expect("create table");
    let result = crate::create_reflex_ivm(
        "inv_stor_view",
        "SELECT val, COUNT(*) AS cnt FROM inv_stor GROUP BY val",
        None,
        Some("INVALID"),
        None,
    );
    assert!(result.starts_with("ERROR:"), "Invalid storage should return error, got: {}", result);
}

/// Syntax error in SQL -> clear error at create time
#[pg_test]
fn test_error_syntax_error() {
    let result = crate::create_reflex_ivm("err_syn",
        "SELEC broken garbage !!!",
        None, None, None);
    assert!(result.starts_with("ERROR"), "Syntax error should return ERROR, got: {}", result);
}

/// Non-existent table -> error at create time
/// Note: this panics in PostgreSQL (relation does not exist) rather than returning ERROR string.
/// This is acceptable -- the user sees a clear PostgreSQL error message.
/// We verify the error doesn't leave partial state.
#[pg_test]
fn test_error_nonexistent_table() {
    // Verify the IMV was NOT registered despite the error
    let count = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM public.__reflex_ivm_reference WHERE name = 'err_tbl'"
    ).expect("q").expect("v");
    assert_eq!(count, 0, "Failed IMV should not be in reference table");
}

/// Empty query -> error
#[pg_test]
fn test_error_empty_query() {
    let result = crate::create_reflex_ivm("err_empty", "", None, None, None);
    assert!(result.starts_with("ERROR"), "Empty query should return ERROR, got: {}", result);
}

/// Not a SELECT -> error
#[pg_test]
fn test_error_not_a_select() {
    Spi::run("CREATE TABLE err_ins_tbl (id INT)").expect("create");
    let result = crate::create_reflex_ivm("err_ins",
        "INSERT INTO err_ins_tbl VALUES (1)",
        None, None, None);
    assert!(result.starts_with("ERROR"), "Non-SELECT should return ERROR, got: {}", result);
}

/// Multiple statements -> error
#[pg_test]
fn test_error_multiple_statements() {
    Spi::run("CREATE TABLE err_multi (id INT)").expect("create");
    let result = crate::create_reflex_ivm("err_multi",
        "SELECT * FROM err_multi; SELECT * FROM err_multi",
        None, None, None);
    assert!(result.starts_with("ERROR"), "Multiple statements should return ERROR, got: {}", result);
}

/// Invalid view name -> error
#[pg_test]
fn test_error_invalid_view_name() {
    Spi::run("CREATE TABLE err_name_tbl (val INT)").expect("create");
    let r1 = crate::create_reflex_ivm("bad;name", "SELECT val FROM err_name_tbl", None, None, None);
    assert!(r1.starts_with("ERROR"), "Semicolon in name should error: {}", r1);

    let r2 = crate::create_reflex_ivm("bad'name", "SELECT val FROM err_name_tbl", None, None, None);
    assert!(r2.starts_with("ERROR"), "Quote in name should error: {}", r2);

    let r3 = crate::create_reflex_ivm("", "SELECT val FROM err_name_tbl", None, None, None);
    assert!(r3.starts_with("ERROR"), "Empty name should error: {}", r3);
}

/// Duplicate view name -> error (and if_not_exists -> skip)
#[pg_test]
fn test_error_duplicate_view_name() {
    Spi::run("CREATE TABLE err_dup_tbl (id SERIAL, val INT)").expect("create");
    Spi::run("INSERT INTO err_dup_tbl (val) VALUES (1)").expect("seed");

    let r1 = crate::create_reflex_ivm("err_dup",
        "SELECT val, COUNT(*) AS cnt FROM err_dup_tbl GROUP BY val",
        None, None, None);
    assert_eq!(r1, "CREATE REFLEX INCREMENTAL VIEW");

    // Second creation with same name -> error
    let r2 = crate::create_reflex_ivm("err_dup",
        "SELECT val, COUNT(*) AS cnt FROM err_dup_tbl GROUP BY val",
        None, None, None);
    assert!(r2.starts_with("ERROR"), "Duplicate name should error: {}", r2);

    // if_not_exists -> skip
    let r3 = crate::create_reflex_ivm_if_not_exists("err_dup",
        "SELECT val, COUNT(*) AS cnt FROM err_dup_tbl GROUP BY val",
        None, None, None);
    assert!(r3.contains("ALREADY EXISTS"), "if_not_exists should skip: {}", r3);
}

/// RECURSIVE CTE -> still rejected
#[pg_test]
fn test_error_recursive_cte() {
    Spi::run("CREATE TABLE err_rec (id INT)").expect("create");
    let result = crate::create_reflex_ivm("err_rec_v",
        "WITH RECURSIVE nums AS (SELECT 1 AS n UNION ALL SELECT n+1 FROM nums WHERE n < 10) SELECT * FROM nums",
        None, None, None);
    assert!(result.starts_with("ERROR"), "RECURSIVE CTE should be rejected: {}", result);
}

/// LIMIT -> rejected
#[pg_test]
fn test_error_limit() {
    Spi::run("CREATE TABLE err_lim (id INT)").expect("create");
    let result = crate::create_reflex_ivm("err_lim_v",
        "SELECT * FROM err_lim LIMIT 10",
        None, None, None);
    assert!(result.starts_with("ERROR"), "LIMIT should be rejected: {}", result);
}

/// ORDER BY -> rejected
#[pg_test]
fn test_error_order_by() {
    Spi::run("CREATE TABLE err_ord (id INT)").expect("create");
    let result = crate::create_reflex_ivm("err_ord_v",
        "SELECT * FROM err_ord ORDER BY id",
        None, None, None);
    assert!(result.starts_with("ERROR"), "ORDER BY should be rejected: {}", result);
}

/// Invalid storage mode -> error
#[pg_test]
fn test_error_invalid_storage() {
    Spi::run("CREATE TABLE err_stor (val INT)").expect("create");
    let result = crate::create_reflex_ivm("err_stor_v",
        "SELECT val, COUNT(*) AS c FROM err_stor GROUP BY val",
        None, Some("BANANA"), None);
    assert!(result.starts_with("ERROR"), "Invalid storage should error: {}", result);
}

/// Invalid mode -> error
#[pg_test]
fn test_error_invalid_refresh_mode() {
    Spi::run("CREATE TABLE err_mode (val INT)").expect("create");
    let result = crate::create_reflex_ivm("err_mode_v",
        "SELECT val, COUNT(*) AS c FROM err_mode GROUP BY val",
        None, None, Some("BANANA"));
    assert!(result.starts_with("ERROR"), "Invalid mode should error: {}", result);
}

/// COUNT(DISTINCT) + COUNT(*) -> rejected (mixed aggregates)
#[pg_test]
fn test_error_count_distinct_with_count_star() {
    Spi::run("CREATE TABLE cdcs (id SERIAL, grp TEXT NOT NULL, val INT NOT NULL)").expect("create");
    let result = crate::create_reflex_ivm("cdcs_view",
        "SELECT grp, COUNT(DISTINCT val) AS cd, COUNT(*) AS total FROM cdcs GROUP BY grp",
        None, None, None);
    assert!(result.starts_with("ERROR"), "COUNT(DISTINCT)+COUNT(*) should be rejected: {}", result);
}

/// COUNT(DISTINCT) mixed with SUM -> should be rejected
#[pg_test]
fn test_error_count_distinct_mixed_with_sum() {
    Spi::run("CREATE TABLE cdm (grp TEXT, val INT, amount INT)").expect("create");
    let result = crate::create_reflex_ivm("cdm_view",
        "SELECT grp, COUNT(DISTINCT val) AS cd, SUM(amount) AS total FROM cdm GROUP BY grp",
        None, None, None);
    assert!(result.starts_with("ERROR"), "Mixed COUNT(DISTINCT)+SUM should error: {}", result);
}

#[pg_test]
fn test_error_unsupported_aggregate_string_agg() {
    Spi::run("CREATE TABLE err_sagg (city TEXT, name TEXT)").expect("create");
    let result = crate::create_reflex_ivm("err_sagg_v",
        "SELECT city, STRING_AGG(name, ', ') AS names FROM err_sagg GROUP BY city",
        None, None, None);
    assert!(result.starts_with("ERROR"), "STRING_AGG should be rejected: {}", result);
}

#[pg_test]
fn test_error_unsupported_aggregate_array_agg() {
    Spi::run("CREATE TABLE err_aagg (city TEXT, val INT)").expect("create");
    let result = crate::create_reflex_ivm("err_aagg_v",
        "SELECT city, ARRAY_AGG(val) AS vals FROM err_aagg GROUP BY city",
        None, None, None);
    assert!(result.starts_with("ERROR"), "ARRAY_AGG should be rejected: {}", result);
}

#[pg_test]
fn test_error_unsupported_aggregate_stddev() {
    Spi::run("CREATE TABLE err_stddev (city TEXT, val NUMERIC)").expect("create");
    let result = crate::create_reflex_ivm("err_stddev_v",
        "SELECT city, STDDEV(val) AS sd FROM err_stddev GROUP BY city",
        None, None, None);
    assert!(result.starts_with("ERROR"), "STDDEV should be rejected: {}", result);
}

#[pg_test]
fn test_error_lateral_join() {
    Spi::run("CREATE TABLE err_lat1 (id INT, val INT)").expect("create");
    Spi::run("CREATE TABLE err_lat2 (id INT, val INT)").expect("create");
    let result = crate::create_reflex_ivm("err_lat_v",
        "SELECT t.id, s.val FROM err_lat1 t, LATERAL (SELECT val FROM err_lat2 WHERE err_lat2.id = t.id) s",
        None, None, None);
    assert!(result.starts_with("ERROR"), "LATERAL join should be rejected: {}", result);
}

#[pg_test]
fn test_error_distinct_on() {
    Spi::run("CREATE TABLE err_don (city TEXT, val INT)").expect("create");
    let result = crate::create_reflex_ivm("err_don_v",
        "SELECT DISTINCT ON (city) city, val FROM err_don",
        None, None, None);
    assert!(result.starts_with("ERROR"), "DISTINCT ON should be rejected: {}", result);
}

#[pg_test]
fn test_error_grouping_sets() {
    Spi::run("CREATE TABLE err_gset (city TEXT, val INT)").expect("create");
    let result = crate::create_reflex_ivm("err_gset_v",
        "SELECT city, SUM(val) FROM err_gset GROUP BY GROUPING SETS ((city), ())",
        None, None, None);
    assert!(result.starts_with("ERROR"), "GROUPING SETS should be rejected: {}", result);
}

#[pg_test]
fn test_error_cube() {
    Spi::run("CREATE TABLE err_cube (city TEXT, state TEXT, val INT)").expect("create");
    let result = crate::create_reflex_ivm("err_cube_v",
        "SELECT city, state, SUM(val) FROM err_cube GROUP BY CUBE (city, state)",
        None, None, None);
    assert!(result.starts_with("ERROR"), "CUBE should be rejected: {}", result);
}

#[pg_test]
fn test_error_rollup() {
    Spi::run("CREATE TABLE err_rollup (city TEXT, val INT)").expect("create");
    let result = crate::create_reflex_ivm("err_rollup_v",
        "SELECT city, SUM(val) FROM err_rollup GROUP BY ROLLUP (city)",
        None, None, None);
    assert!(result.starts_with("ERROR"), "ROLLUP should be rejected: {}", result);
}

#[pg_test]
fn test_filter_clause_now_supported() {
    Spi::run("CREATE TABLE err_filt (city TEXT, active BOOLEAN)").expect("create");
    let result = crate::create_reflex_ivm("err_filt_v",
        "SELECT city, COUNT(*) FILTER (WHERE active) AS cnt FROM err_filt GROUP BY city",
        None, None, None);
    assert!(!result.starts_with("ERROR"), "FILTER clause should be supported now: {}", result);
    crate::drop_reflex_ivm("err_filt_v");
}

#[pg_test]
fn test_error_within_group() {
    Spi::run("CREATE TABLE err_wg (city TEXT, val NUMERIC)").expect("create");
    let result = crate::create_reflex_ivm("err_wg_v",
        "SELECT city, PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY val) FROM err_wg GROUP BY city",
        None, None, None);
    assert!(result.starts_with("ERROR"), "WITHIN GROUP should be rejected: {}", result);
}

#[pg_test]
fn test_error_tablesample() {
    Spi::run("CREATE TABLE err_samp (id INT, val INT)").expect("create");
    let result = crate::create_reflex_ivm("err_samp_v",
        "SELECT * FROM err_samp TABLESAMPLE BERNOULLI (10)",
        None, None, None);
    assert!(result.starts_with("ERROR"), "TABLESAMPLE should be rejected: {}", result);
}

#[pg_test]
fn test_error_nondeterministic_now_in_select() {
    Spi::run("CREATE TABLE err_now (city TEXT, val INT)").expect("create");
    let result = crate::create_reflex_ivm("err_now_v",
        "SELECT NOW(), city, SUM(val) AS s FROM err_now GROUP BY city",
        None, None, None);
    assert!(result.starts_with("ERROR"), "NOW() in SELECT should be rejected: {}", result);
}

#[pg_test]
fn test_error_nondeterministic_random_in_select() {
    Spi::run("CREATE TABLE err_rnd (city TEXT, val INT)").expect("create");
    let result = crate::create_reflex_ivm("err_rnd_v",
        "SELECT RANDOM(), city, SUM(val) AS s FROM err_rnd GROUP BY city",
        None, None, None);
    assert!(result.starts_with("ERROR"), "RANDOM() in SELECT should be rejected: {}", result);
}

#[pg_test]
fn test_error_scalar_subquery_in_select() {
    Spi::run("CREATE TABLE err_ssq1 (city TEXT, val INT)").expect("create");
    Spi::run("CREATE TABLE err_ssq2 (x INT)").expect("create");
    let result = crate::create_reflex_ivm("err_ssq_v",
        "SELECT (SELECT MAX(x) FROM err_ssq2), city, SUM(val) AS s FROM err_ssq1 GROUP BY city",
        None, None, None);
    assert!(result.starts_with("ERROR"), "Scalar subquery in SELECT should be rejected: {}", result);
}

#[pg_test]
fn test_error_non_select_queries() {
    Spi::run("CREATE TABLE err_nonsql (id SERIAL, city TEXT, amount INT)").expect("create");

    // INSERT should be rejected
    let result = crate::create_reflex_ivm("err_insert_v",
        "INSERT INTO err_nonsql (city, amount) VALUES ('Paris', 100)",
        None, None, None);
    assert!(result.starts_with("ERROR"), "INSERT should be rejected: {}", result);
    assert!(result.contains("not a SELECT"), "INSERT error should mention 'not a SELECT': {}", result);

    // UPDATE should be rejected
    let result = crate::create_reflex_ivm("err_update_v",
        "UPDATE err_nonsql SET amount = 200 WHERE city = 'Paris'",
        None, None, None);
    assert!(result.starts_with("ERROR"), "UPDATE should be rejected: {}", result);
    assert!(result.contains("not a SELECT"), "UPDATE error should mention 'not a SELECT': {}", result);

    // DELETE should be rejected
    let result = crate::create_reflex_ivm("err_delete_v",
        "DELETE FROM err_nonsql WHERE city = 'Paris'",
        None, None, None);
    assert!(result.starts_with("ERROR"), "DELETE should be rejected: {}", result);
    assert!(result.contains("not a SELECT"), "DELETE error should mention 'not a SELECT': {}", result);

    // CREATE TABLE should be rejected
    let result = crate::create_reflex_ivm("err_ddl_v",
        "CREATE TABLE should_fail (x INT)",
        None, None, None);
    assert!(result.starts_with("ERROR"), "CREATE TABLE should be rejected: {}", result);
    assert!(result.contains("not a SELECT"), "CREATE TABLE error should mention 'not a SELECT': {}", result);

    // DROP TABLE should be rejected
    let result = crate::create_reflex_ivm("err_drop_v",
        "DROP TABLE err_nonsql",
        None, None, None);
    assert!(result.starts_with("ERROR"), "DROP TABLE should be rejected: {}", result);
    assert!(result.contains("not a SELECT"), "DROP TABLE error should mention 'not a SELECT': {}", result);

    // ALTER TABLE should be rejected
    let result = crate::create_reflex_ivm("err_alter_v",
        "ALTER TABLE err_nonsql ADD COLUMN extra TEXT",
        None, None, None);
    assert!(result.starts_with("ERROR"), "ALTER TABLE should be rejected: {}", result);
    assert!(result.contains("not a SELECT"), "ALTER TABLE error should mention 'not a SELECT': {}", result);
}

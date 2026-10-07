fn v112_fn_table(table: &str) {
    Spi::run(&format!(
        "CREATE TABLE {table}(id SERIAL PRIMARY KEY, g TEXT, v INT, ts TIMESTAMPTZ DEFAULT '2026-01-01+00')"
    ))
    .expect("create");
    Spi::run(&format!("INSERT INTO {table}(g, v) VALUES ('a',1),('a',2),('b',5)")).expect("seed");
}

fn v112_fn_leftovers(prefix: &str) -> i64 {
    Spi::get_one::<i64>(&format!(
        "SELECT COUNT(*) FROM pg_class WHERE relname LIKE '{prefix}%' AND relname NOT LIKE '%\\_src'"
    ))
    .expect("leftovers")
    .unwrap_or(0)
}

fn v112_fn_assert_rejected(name: &str, q: &str, needle: &str) {
    let res = crate::create_reflex_ivm(name, q, None, None, None, None);
    assert!(res.starts_with("ERROR: [reflex-unsupported]"), "{q} -> {res}");
    assert!(res.contains(needle), "{q} -> {res}");
    assert!(
        res.find("Fix:").or_else(|| res.find("Use ")).or_else(|| res.find("Supported:")).is_some_and(|p| p < 300),
        "remedy must be in the first 300 characters: {res}"
    );
    assert_eq!(v112_fn_leftovers(name), 0, "objects left behind for {name}");
}

#[pg_test]
fn v112_fn_user_volatile_function_rejected_everywhere() {
    v112_fn_table("v112_fn_vol_src");
    Spi::run("CREATE FUNCTION v112_fn_vol() RETURNS float8 LANGUAGE sql VOLATILE AS 'SELECT 0.5::float8'")
        .expect("fn");
    for (i, q) in [
        "SELECT g, SUM(v * v112_fn_vol()) AS s FROM v112_fn_vol_src GROUP BY g",
        "SELECT id, g FROM v112_fn_vol_src WHERE v > v112_fn_vol()",
        "WITH c AS (SELECT id, v112_fn_vol() AS x FROM v112_fn_vol_src) SELECT id, x FROM c",
        "SELECT a.id FROM v112_fn_vol_src a JOIN v112_fn_vol_src b ON b.id = a.id AND b.v > v112_fn_vol()",
        "SELECT id, nextval('v112_fn_vol_src_id_seq') AS n FROM v112_fn_vol_src",
    ]
    .iter()
    .enumerate()
    {
        v112_fn_assert_rejected(&format!("v112_fn_vol_{i}"), q, "v112_fn_vol");
    }
}

#[pg_test]
fn v112_fn_clock_functions_rejected() {
    v112_fn_table("v112_fn_clk_src");
    for (i, q) in [
        "SELECT id, g, transaction_timestamp() AS at FROM v112_fn_clk_src",
        "SELECT id, g, LOCALTIMESTAMP AS at FROM v112_fn_clk_src",
        "SELECT id, g, age(ts) AS a FROM v112_fn_clk_src",
        "SELECT id, g, txid_current() AS x FROM v112_fn_clk_src",
        "SELECT g, SUM(v) AS s FROM v112_fn_clk_src WHERE ts < pg_catalog.now() GROUP BY g",
        "SELECT id, g, current_user AS cu FROM v112_fn_clk_src",
        "SELECT id, g, current_setting('application_name') AS u FROM v112_fn_clk_src",
    ]
    .iter()
    .enumerate()
    {
        v112_fn_assert_rejected(&format!("v112_fn_clk_{i}"), q, "Non-deterministic");
    }
}

#[pg_test]
fn v112_fn_immutable_and_stable_user_functions_accepted() {
    v112_fn_table("v112_fn_ok_src");
    Spi::run("CREATE FUNCTION v112_fn_imm(int) RETURNS int LANGUAGE sql IMMUTABLE AS 'SELECT $1 * 2'")
        .expect("imm");
    Spi::run("CREATE FUNCTION v112_fn_stb(int) RETURNS int LANGUAGE sql STABLE AS 'SELECT $1 + 1'")
        .expect("stb");
    let q = "SELECT g, SUM(v112_fn_imm(v)) AS s, MAX(v112_fn_stb(v)) AS m, MIN(length(g)) AS l \
             FROM v112_fn_ok_src GROUP BY g";
    assert_eq!(
        crate::create_reflex_ivm("v112_fn_ok", q, None, None, None, None),
        "CREATE REFLEX INCREMENTAL VIEW"
    );
    Spi::run("INSERT INTO v112_fn_ok_src(g, v) VALUES ('a',9),('c',4)").expect("insert");
    Spi::run("DELETE FROM v112_fn_ok_src WHERE id = 2").expect("delete");
    assert_imv_correct("v112_fn_ok", q);
}

#[pg_test]
fn v112_fn_date_trunc_is_kept_and_correct() {
    v112_fn_table("v112_fn_dt_src");
    let q = "SELECT date_trunc('day', ts) AS d, SUM(v) AS s FROM v112_fn_dt_src GROUP BY date_trunc('day', ts)";
    let res = crate::create_reflex_ivm("v112_fn_dt", q, None, None, None, None);
    assert_eq!(res, "CREATE REFLEX INCREMENTAL VIEW");
    Spi::run("INSERT INTO v112_fn_dt_src(g, v, ts) VALUES ('a', 7, '2026-01-02+00'), ('a', 8, '2026-01-01+00')")
        .expect("insert");
    Spi::run("DELETE FROM v112_fn_dt_src WHERE id = 1").expect("delete");
    assert_imv_correct("v112_fn_dt", q);
}

#[pg_test]
fn v112_fn_user_defined_aggregate_rejected() {
    v112_fn_table("v112_fn_agg_src");
    Spi::run("CREATE AGGREGATE v112_fn_mysum(int) (sfunc = int4pl, stype = int, initcond = '0')")
        .expect("agg");
    v112_fn_assert_rejected(
        "v112_fn_agg_0",
        "SELECT g, v112_fn_mysum(v) AS ms, COUNT(*) AS c FROM v112_fn_agg_src GROUP BY g",
        "Unsupported aggregate",
    );
}

#[cfg(not(feature = "pg15"))]
#[pg_test]
fn v112_fn_any_value_rejected() {
    v112_fn_table("v112_fn_av_src");
    v112_fn_assert_rejected(
        "v112_fn_av_0",
        "SELECT g, any_value(v) AS av, SUM(v) AS s FROM v112_fn_av_src GROUP BY g",
        "Unsupported aggregate",
    );
}

#[pg_test]
fn v112_fn_constant_beside_global_aggregate_rejected() {
    v112_fn_table("v112_fn_const_src");
    let res = crate::create_reflex_ivm(
        "v112_fn_const_0",
        "SELECT 'x'::text AS tag, SUM(v) AS s FROM v112_fn_const_src",
        None,
        None,
        None,
        None,
    );
    assert!(res.starts_with("ERROR: [reflex-unsupported]"), "{res}");
    assert_eq!(v112_fn_leftovers("v112_fn_const_0"), 0);
}

#[pg_test]
fn v112_fn_supported_imv_still_rebuilds() {
    v112_fn_table("v112_fn_rb_src");
    let q = "SELECT g, SUM(v) AS s, MAX(length(g)) AS u FROM v112_fn_rb_src GROUP BY g";
    assert_eq!(
        crate::create_reflex_ivm("v112_fn_rb", q, None, None, None, None),
        "CREATE REFLEX INCREMENTAL VIEW"
    );
    let rebuilt = Spi::get_one::<String>("SELECT reflex_rebuild_chain('v112_fn_rb')")
        .expect("rebuild")
        .unwrap_or_default();
    assert!(!rebuilt.to_uppercase().starts_with("ERROR"), "{rebuilt}");
    Spi::run("INSERT INTO v112_fn_rb_src(g, v) VALUES ('a',3)").expect("insert");
    assert_imv_correct("v112_fn_rb", q);
}

#[pg_test]
fn v112_fn_volatile_rejection_names_the_function() {
    v112_fn_table("v112_fn_nm_src");
    v112_fn_assert_rejected(
        "v112_fn_nm_0",
        "SELECT id, nextval('v112_fn_nm_src_id_seq') AS n FROM v112_fn_nm_src",
        "Volatile function(s) nextval",
    );
}

#[pg_test]
fn v112_fn_quoted_mixed_case_volatile_function_rejected() {
    v112_fn_table("v112_fn_mc_src");
    Spi::run(
        "CREATE FUNCTION \"v112_fn_McVol\"() RETURNS float8 LANGUAGE sql VOLATILE AS 'SELECT 0.5::float8'",
    )
    .expect("fn");
    v112_fn_assert_rejected(
        "v112_fn_mc_0",
        "SELECT id, v * \"v112_fn_McVol\"() AS x FROM v112_fn_mc_src",
        "Volatile function(s) v112_fn_mcvol",
    );
}

#[pg_test]
fn v112_fn_volatile_in_cte_body_leaves_no_sub_imv() {
    v112_fn_table("v112_fn_cte_src");
    Spi::run("CREATE FUNCTION v112_fn_ctevol() RETURNS int LANGUAGE sql VOLATILE AS 'SELECT 1'")
        .expect("fn");
    v112_fn_assert_rejected(
        "v112_fn_cte_0",
        "WITH c AS (SELECT g, SUM(v + v112_fn_ctevol()) AS s FROM v112_fn_cte_src GROUP BY g) \
         SELECT g, s FROM c",
        "Volatile function(s) v112_fn_ctevol",
    );
    assert_eq!(
        Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM public.__reflex_ivm_reference WHERE name LIKE 'v112_fn_cte_0%'"
        )
        .expect("registry")
        .unwrap_or(0),
        0
    );
}

#[pg_test]
fn v112_fn_schema_qualified_user_aggregate_with_distinct_rejected() {
    v112_fn_table("v112_fn_sagg_src");
    Spi::run("CREATE SCHEMA v112_fn_s").expect("schema");
    Spi::run("CREATE AGGREGATE v112_fn_s.v112_fn_mymax(int) (sfunc = int4larger, stype = int)")
        .expect("agg");
    v112_fn_assert_rejected(
        "v112_fn_sagg_0",
        "SELECT g, v112_fn_s.v112_fn_mymax(DISTINCT v) AS m FROM v112_fn_sagg_src GROUP BY g",
        "Unsupported aggregate(s): V112_FN_MYMAX",
    );
}

#[pg_test]
fn v112_fn_ungrouped_column_rejection_names_the_column_and_remedy() {
    v112_fn_table("v112_fn_ung_src");
    v112_fn_assert_rejected(
        "v112_fn_ung_0",
        "SELECT 'x'::text AS tag, SUM(v) AS s FROM v112_fn_ung_src",
        "'tag' is not an aggregate",
    );
}

#[pg_test]
fn v112_fn_global_aggregate_without_group_by_still_works() {
    v112_fn_table("v112_fn_glob_src");
    let q = "SELECT SUM(v) AS s, COUNT(*) AS c FROM v112_fn_glob_src";
    assert_eq!(
        crate::create_reflex_ivm("v112_fn_glob", q, None, None, None, None),
        "CREATE REFLEX INCREMENTAL VIEW"
    );
    Spi::run("INSERT INTO v112_fn_glob_src(g, v) VALUES ('z', 10)").expect("insert");
    assert_imv_correct("v112_fn_glob", q);
}

#[pg_test]
fn v112_fn_ungrouped_column_over_cte_leaves_no_sub_imv() {
    v112_fn_table("v112_fn_ucte_src");
    v112_fn_assert_rejected(
        "v112_fn_ucte_0",
        "WITH c AS (SELECT g, SUM(v) AS s FROM v112_fn_ucte_src GROUP BY g) \
         SELECT 'x'::text AS tag, SUM(s) AS total FROM c",
        "'tag' is not an aggregate",
    );
    assert_eq!(
        Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM public.__reflex_ivm_reference WHERE name LIKE 'v112_fn_ucte_0%'"
        )
        .expect("registry")
        .unwrap_or(0),
        0
    );
}

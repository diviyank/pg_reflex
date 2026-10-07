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

const V112_FN_NOOP_QUERIES: [&str; 5] = [
    "SELECT g, SUM(v * v112_fn_ine_vol()) AS s FROM v112_fn_ine_src GROUP BY g",
    "SELECT id, g, transaction_timestamp() AS at FROM v112_fn_ine_src",
    "SELECT g, v112_fn_ine_agg(v) AS ms FROM v112_fn_ine_src GROUP BY g",
    "SELECT 'x'::text AS tag, SUM(v) AS s FROM v112_fn_ine_src",
    "SELECT date_trunc('day', ts) AS d, SUM(v) AS s FROM v112_fn_ine_src GROUP BY date_trunc('day', ts)",
];

fn v112_fn_noop_fixture() {
    v112_fn_table("v112_fn_ine_src");
    Spi::run("CREATE FUNCTION v112_fn_ine_vol() RETURNS int LANGUAGE sql VOLATILE AS 'SELECT 1'")
        .expect("fn");
    Spi::run("CREATE AGGREGATE v112_fn_ine_agg(int) (sfunc = int4pl, stype = int, initcond = '0')")
        .expect("agg");
}

#[pg_test]
fn v112_fn_if_not_exists_on_existing_imv_is_a_noop_for_rejected_definitions() {
    v112_fn_noop_fixture();
    let original = "SELECT g, SUM(v) AS s FROM v112_fn_ine_src GROUP BY g";
    assert_eq!(
        crate::create_reflex_ivm("v112_fn_ine", original, None, None, None, None),
        "CREATE REFLEX INCREMENTAL VIEW"
    );
    let ordinary_noop =
        crate::create_reflex_ivm_if_not_exists("v112_fn_ine", original, None, None, None, None);
    assert_eq!(ordinary_noop, "REFLEX INCREMENTAL VIEW ALREADY EXISTS (skipped)");
    let stored_definition = || {
        Spi::get_one::<String>(
            "SELECT sql_query FROM public.__reflex_ivm_reference WHERE name = 'v112_fn_ine'",
        )
        .expect("registry")
        .expect("row")
    };
    let definition_before = stored_definition();
    for q in V112_FN_NOOP_QUERIES {
        assert_eq!(
            crate::create_reflex_ivm_if_not_exists("v112_fn_ine", q, None, None, None, None),
            ordinary_noop,
            "{q}"
        );
    }
    assert_eq!(stored_definition(), definition_before);
    assert_eq!(
        Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM public.__reflex_ivm_reference WHERE name LIKE 'v112_fn_ine%'"
        )
        .expect("registry")
        .unwrap_or(0),
        1
    );
    Spi::run("INSERT INTO v112_fn_ine_src(g, v) VALUES ('a', 9), ('c', 4)").expect("insert");
    Spi::run("DELETE FROM v112_fn_ine_src WHERE id = 2").expect("delete");
    assert_imv_correct("v112_fn_ine", original);
}

#[pg_test]
fn v112_fn_if_not_exists_on_a_new_name_behaves_like_create() {
    v112_fn_noop_fixture();
    for (i, needle) in [
        "Volatile function(s) v112_fn_ine_vol",
        "Non-deterministic",
        "Unsupported aggregate(s): V112_FN_INE_AGG",
        "'tag' is not an aggregate",
    ]
    .iter()
    .enumerate()
    {
        let name = format!("v112_fn_inew_{i}");
        let q = V112_FN_NOOP_QUERIES[i];
        let res = crate::create_reflex_ivm_if_not_exists(&name, q, None, None, None, None);
        assert!(res.starts_with("ERROR: [reflex-unsupported]"), "{q} -> {res}");
        assert!(res.contains(needle), "{q} -> {res}");
        assert_eq!(v112_fn_leftovers(&name), 0, "objects left behind for {name}");
    }
    let q = V112_FN_NOOP_QUERIES[4];
    assert_eq!(
        crate::create_reflex_ivm_if_not_exists("v112_fn_inew_4", q, None, None, None, None),
        "CREATE REFLEX INCREMENTAL VIEW"
    );
    Spi::run("INSERT INTO v112_fn_ine_src(g, v, ts) VALUES ('a', 7, '2026-01-02+00')").expect("insert");
    assert_imv_correct("v112_fn_inew_4", q);
}

#[pg_test]
fn v112_fn_if_not_exists_on_existing_cte_imv_is_a_noop() {
    v112_fn_table("v112_fn_inec_src");
    let q = "WITH c AS (SELECT g, SUM(v) AS s FROM v112_fn_inec_src GROUP BY g) SELECT g, s FROM c";
    assert_eq!(
        crate::create_reflex_ivm("v112_fn_inec", q, None, None, None, None),
        "CREATE REFLEX INCREMENTAL VIEW"
    );
    assert_eq!(
        crate::create_reflex_ivm_if_not_exists("v112_fn_inec", q, None, None, None, None),
        "REFLEX INCREMENTAL VIEW ALREADY EXISTS (skipped)"
    );
    Spi::run("INSERT INTO v112_fn_inec_src(g, v) VALUES ('a', 9), ('c', 4)").expect("insert");
    assert_imv_correct("v112_fn_inec", q);
}

#[pg_test]
fn v112_fn_volatile_function_with_a_dot_in_its_quoted_name_rejected() {
    v112_fn_table("v112_fn_dot_src");
    Spi::run("CREATE FUNCTION \"v112.fn\"() RETURNS int LANGUAGE sql VOLATILE AS 'SELECT 1'")
        .expect("fn");
    v112_fn_assert_rejected(
        "v112_fn_dot_0",
        "SELECT id, v + \"v112.fn\"() AS x FROM v112_fn_dot_src",
        "Volatile function(s) v112.fn",
    );
    v112_fn_assert_rejected(
        "v112_fn_dot_1",
        "SELECT id, v + public.\"v112.fn\"() AS x FROM v112_fn_dot_src",
        "Volatile function(s) v112.fn",
    );
}

#[pg_test]
fn v112_fn_extract_is_kept_and_correct() {
    v112_fn_table("v112_fn_ex_src");
    let q = "SELECT EXTRACT(dow FROM ts) AS d, SUM(v) AS s FROM v112_fn_ex_src GROUP BY EXTRACT(dow FROM ts)";
    assert_eq!(
        crate::create_reflex_ivm("v112_fn_ex", q, None, None, None, None),
        "CREATE REFLEX INCREMENTAL VIEW"
    );
    Spi::run("INSERT INTO v112_fn_ex_src(g, v, ts) VALUES ('a', 7, '2026-01-02+00'), ('a', 8, '2026-01-01+00')")
        .expect("insert");
    Spi::run("DELETE FROM v112_fn_ex_src WHERE id = 1").expect("delete");
    assert_imv_correct("v112_fn_ex", q);
}

#[pg_test]
fn v112_fn_volatile_function_with_non_ascii_upper_case_name_rejected() {
    v112_fn_table("v112_fn_na_src");
    Spi::run("CREATE FUNCTION \"v112_fn_ÉVOL\"() RETURNS int LANGUAGE sql VOLATILE AS 'SELECT 1'")
        .expect("quoted fn");
    Spi::run("CREATE FUNCTION v112_fn_Èvol2() RETURNS int LANGUAGE sql VOLATILE AS 'SELECT 1'")
        .expect("unquoted fn");
    v112_fn_assert_rejected(
        "v112_fn_na_0",
        "SELECT id, v + \"v112_fn_ÉVOL\"() AS x FROM v112_fn_na_src",
        "Volatile function(s) v112_fn_Évol",
    );
    v112_fn_assert_rejected(
        "v112_fn_na_1",
        "SELECT id, v + v112_fn_Èvol2() AS x FROM v112_fn_na_src",
        "Volatile function(s) v112_fn_Èvol2",
    );
}

#[pg_test]
fn v112_fn_immutable_function_with_non_ascii_name_accepted() {
    v112_fn_table("v112_fn_nai_src");
    Spi::run("CREATE FUNCTION \"v112_fn_ÉIMM\"(int) RETURNS int LANGUAGE sql IMMUTABLE AS 'SELECT $1 * 3'")
        .expect("fn");
    let q = "SELECT id, \"v112_fn_ÉIMM\"(v) AS x FROM v112_fn_nai_src";
    assert_eq!(
        crate::create_reflex_ivm("v112_fn_nai", q, None, None, None, None),
        "CREATE REFLEX INCREMENTAL VIEW"
    );
    Spi::run("INSERT INTO v112_fn_nai_src(g, v) VALUES ('a', 9), ('c', 4)").expect("insert");
    Spi::run("DELETE FROM v112_fn_nai_src WHERE id = 2").expect("delete");
    assert_imv_correct("v112_fn_nai", q);
}

#[pg_test]
fn v112_fn_plain_function_sharing_a_name_with_an_aggregate_accepted() {
    v112_fn_table("v112_fn_amb_src");
    Spi::run("CREATE AGGREGATE v112_fn_amb(int) (sfunc = int4pl, stype = int, initcond = '0')")
        .expect("agg");
    Spi::run("CREATE FUNCTION v112_fn_amb(a int, b int) RETURNS int LANGUAGE sql IMMUTABLE AS 'SELECT a + b'")
        .expect("fn");
    let q = "SELECT id, v112_fn_amb(v, id) AS m FROM v112_fn_amb_src";
    assert_eq!(
        crate::create_reflex_ivm("v112_fn_amb_v", q, None, None, None, None),
        "CREATE REFLEX INCREMENTAL VIEW"
    );
    Spi::run("INSERT INTO v112_fn_amb_src(g, v) VALUES ('a', 9), ('c', 4)").expect("insert");
    Spi::run("DELETE FROM v112_fn_amb_src WHERE id = 2").expect("delete");
    assert_imv_correct("v112_fn_amb_v", q);
}

#[pg_test]
fn v112_fn_unlabelled_function_rejection_converges_once_declared_immutable() {
    v112_fn_table("v112_fn_lbl_src");
    Spi::run("CREATE FUNCTION v112_fn_lbl(a int) RETURNS int LANGUAGE sql AS 'SELECT a * 2'")
        .expect("fn");
    let q = "SELECT id, v112_fn_lbl(v) AS d FROM v112_fn_lbl_src";
    let rejected = crate::create_reflex_ivm("v112_fn_lbl_v", q, None, None, None, None);
    assert!(rejected.starts_with("ERROR: [reflex-unsupported]"), "{rejected}");
    assert!(rejected.contains("Volatile function(s) v112_fn_lbl"), "{rejected}");
    assert!(
        rejected.find("ALTER FUNCTION").is_some_and(|p| p < 300),
        "{rejected}"
    );
    assert!(rejected.find("IMMUTABLE").is_some_and(|p| p < 300), "{rejected}");
    assert_eq!(v112_fn_leftovers("v112_fn_lbl_v"), 0);

    Spi::run("ALTER FUNCTION v112_fn_lbl(int) IMMUTABLE").expect("alter");
    assert_eq!(
        crate::create_reflex_ivm("v112_fn_lbl_v", q, None, None, None, None),
        "CREATE REFLEX INCREMENTAL VIEW"
    );
    Spi::run("INSERT INTO v112_fn_lbl_src(g, v) VALUES ('a', 9), ('c', 4)").expect("insert");
    Spi::run("DELETE FROM v112_fn_lbl_src WHERE id = 2").expect("delete");
    assert_imv_correct("v112_fn_lbl_v", q);
}

#[pg_test]
fn v112_fn_backend_and_connection_functions_rejected() {
    v112_fn_table("v112_fn_sess_src");
    for (i, q) in [
        "SELECT id, pg_backend_pid() AS p FROM v112_fn_sess_src",
        "SELECT id, g FROM v112_fn_sess_src WHERE inet_client_addr() IS NULL",
    ]
    .iter()
    .enumerate()
    {
        v112_fn_assert_rejected(&format!("v112_fn_sess_{i}"), q, "Non-deterministic");
    }
}

#[pg_test]
fn v112_fn_ungrouped_aggregate_derived_item_is_not_called_a_non_aggregate() {
    v112_fn_table("v112_fn_ungd_src");
    v112_fn_assert_rejected(
        "v112_fn_ungd_0",
        "SELECT SUM(v) IS NULL AS e, COUNT(*) AS c FROM v112_fn_ungd_src",
        "'e' is not an aggregate expression that pg_reflex supports and is not a group key",
    );
}

fn v112_fn_created(name: &str, q: &str, mode: &str) {
    assert_eq!(
        crate::create_reflex_ivm(name, q, None, None, Some(mode), None),
        "CREATE REFLEX INCREMENTAL VIEW",
        "{q}"
    );
}

fn v112_fn_assert_correct_through_dml(imv: &str, q: &str, table: &str, deferred: bool) {
    let assert_correct = || {
        if deferred {
            Spi::run(&format!("SELECT reflex_flush_deferred('{table}')")).expect("flush");
        }
        assert_imv_correct(imv, q);
    };
    assert_correct();
    for dml in [
        format!("INSERT INTO {table}(g, v) VALUES ('a', 9), ('c', 4)"),
        format!("UPDATE {table} SET v = v + 10 WHERE g = 'b'"),
        format!("DELETE FROM {table} WHERE v = (SELECT MAX(v) FROM {table})"),
        format!("DELETE FROM {table}"),
    ] {
        Spi::run(&dml).expect("dml");
        assert_correct();
    }
}

fn v112_fn_unknown_aggregate(name: &str) {
    Spi::run(&format!("CREATE AGGREGATE {name}(int) (sfunc = int4larger, stype = int)"))
        .expect("agg");
}

#[pg_test]
fn v112_fn_ungrouped_unknown_aggregate_created_and_correct() {
    v112_fn_table("v112_fn_ua_src");
    v112_fn_unknown_aggregate("v112_fn_ua_agg");
    let q = "SELECT v112_fn_ua_agg(v) AS m FROM v112_fn_ua_src";
    v112_fn_created("v112_fn_ua", q, "IMMEDIATE");
    v112_fn_assert_correct_through_dml("v112_fn_ua", q, "v112_fn_ua_src", false);
}

#[pg_test]
fn v112_fn_ungrouped_unknown_aggregate_created_and_correct_deferred() {
    v112_fn_table("v112_fn_uad_src");
    v112_fn_unknown_aggregate("v112_fn_uad_agg");
    let q = "SELECT v112_fn_uad_agg(v) AS m FROM v112_fn_uad_src";
    v112_fn_created("v112_fn_uad", q, "DEFERRED");
    v112_fn_assert_correct_through_dml("v112_fn_uad", q, "v112_fn_uad_src", true);
}

#[pg_test]
fn v112_fn_ungrouped_unknown_aggregate_over_join_created_and_correct() {
    v112_fn_table("v112_fn_uja_src");
    v112_fn_table("v112_fn_ujb_src");
    v112_fn_unknown_aggregate("v112_fn_uj_agg");
    let q = "SELECT v112_fn_uj_agg(a.v + b.v) AS m \
             FROM v112_fn_uja_src a JOIN v112_fn_ujb_src b ON b.g = a.g";
    v112_fn_created("v112_fn_uj", q, "IMMEDIATE");
    assert_imv_correct("v112_fn_uj", q);
    for dml in [
        "INSERT INTO v112_fn_uja_src(g, v) VALUES ('b', 40), ('c', 4)",
        "INSERT INTO v112_fn_ujb_src(g, v) VALUES ('c', 90)",
        "UPDATE v112_fn_ujb_src SET v = v - 100 WHERE g = 'c'",
        "DELETE FROM v112_fn_uja_src WHERE g = 'b'",
        "DELETE FROM v112_fn_ujb_src",
    ] {
        Spi::run(dml).expect("dml");
        assert_imv_correct("v112_fn_uj", q);
    }
}

#[pg_test]
fn v112_fn_unknown_aggregate_with_group_by_or_beside_supported_aggregate_still_rejected() {
    v112_fn_table("v112_fn_ug_src");
    v112_fn_unknown_aggregate("v112_fn_ug_agg");
    for (i, q) in [
        "SELECT g, v112_fn_ug_agg(v) AS m FROM v112_fn_ug_src GROUP BY g",
        "SELECT v112_fn_ug_agg(v) AS m, SUM(v) AS s FROM v112_fn_ug_src",
        "SELECT v112_fn_ug_agg(v) + SUM(v) AS m FROM v112_fn_ug_src",
    ]
    .iter()
    .enumerate()
    {
        v112_fn_assert_rejected(
            &format!("v112_fn_ug_{i}"),
            q,
            "Unsupported aggregate(s): V112_FN_UG_AGG",
        );
    }
}

#[pg_test]
fn v112_fn_unknown_aggregate_outside_a_single_level_passthrough_still_rejected() {
    v112_fn_table("v112_fn_un_src");
    v112_fn_unknown_aggregate("v112_fn_un_agg");
    for (i, q) in [
        "WITH c AS (SELECT v112_fn_un_agg(v) AS m FROM v112_fn_un_src) SELECT m FROM c",
        "SELECT id, v FROM v112_fn_un_src WHERE v >= (SELECT v112_fn_un_agg(v) FROM v112_fn_un_src)",
        "SELECT m FROM (SELECT v112_fn_un_agg(v) AS m FROM v112_fn_un_src) d",
        "SELECT v112_fn_un_agg(v) AS m FROM v112_fn_un_src \
         UNION ALL SELECT v112_fn_un_agg(v) AS m FROM v112_fn_un_src",
        "SELECT v112_fn_un_agg(v) AS m FROM v112_fn_un_src HAVING v112_fn_un_agg(v) > 0",
        "SELECT DISTINCT v112_fn_un_agg(v) AS m FROM v112_fn_un_src",
        "SELECT v112_fn_un_agg(v) AS m, row_number() OVER () AS rn FROM v112_fn_un_src",
        "SELECT DISTINCT ON (m) v112_fn_un_agg(v) AS m FROM v112_fn_un_src ORDER BY m",
    ]
    .iter()
    .enumerate()
    {
        v112_fn_assert_rejected(
            &format!("v112_fn_un_{i}"),
            q,
            "Unsupported aggregate(s): V112_FN_UN_AGG",
        );
    }
}

fn v112_fn_same_name_in_two_schemas(tag: &str) {
    for (schema, label) in [("s1", "IMMUTABLE"), ("s2", "")] {
        Spi::run(&format!("CREATE SCHEMA {tag}_{schema}")).expect("schema");
        Spi::run(&format!(
            "CREATE FUNCTION {tag}_{schema}.{tag}_f(int) RETURNS int LANGUAGE sql {label} AS 'SELECT $1 + 1'"
        ))
        .expect("fn");
    }
}

fn v112_fn_printed_alter_function(message: &str) -> String {
    let start = message.find("ALTER FUNCTION").expect("no ALTER FUNCTION in the message");
    let length = message[start..].find(" IMMUTABLE").expect("no IMMUTABLE after ALTER FUNCTION")
        + " IMMUTABLE".len();
    message[start..start + length].to_string()
}

fn v112_fn_rejection(name: &str, q: &str) -> &'static str {
    let res = crate::create_reflex_ivm(name, q, None, None, None, None);
    assert!(res.starts_with("ERROR: [reflex-unsupported]"), "{q} -> {res}");
    assert_eq!(v112_fn_leftovers(name), 0, "objects left behind for {name}");
    res
}

#[pg_test]
fn v112_fn_qualified_call_ignores_a_same_name_function_in_another_schema() {
    v112_fn_table("v112_fn_qa_src");
    v112_fn_same_name_in_two_schemas("v112_fn_qa");
    let q = "SELECT id, v112_fn_qa_s1.v112_fn_qa_f(v) AS x FROM v112_fn_qa_src";
    v112_fn_created("v112_fn_qa", q, "IMMEDIATE");
    v112_fn_assert_correct_through_dml("v112_fn_qa", q, "v112_fn_qa_src", false);
}

#[pg_test]
fn v112_fn_volatile_rejection_prints_the_offending_signature_and_its_remedy_first() {
    v112_fn_table("v112_fn_qr_src");
    v112_fn_same_name_in_two_schemas("v112q");
    for (i, q) in [
        "SELECT id, v112q_s2.v112q_f(v) AS x FROM v112_fn_qr_src",
        "SELECT id, v112q_f(v) AS x FROM v112_fn_qr_src",
    ]
    .iter()
    .enumerate()
    {
        let res = v112_fn_rejection(&format!("v112_fn_qr_{i}"), q);
        assert!(res.contains("Volatile function(s) v112q_f"), "{res}");
        assert!(res.contains(": v112q_s2.v112q_f(integer). Fix:"), "{res}");
        let remedy = "ALTER FUNCTION v112q_s2.v112q_f(integer) IMMUTABLE";
        assert!(
            res.find(remedy).is_some_and(|p| p + remedy.len() <= 200),
            "remedy must end within the first 200 characters: {res}"
        );
    }
}

#[pg_test]
fn v112_fn_mixed_case_function_remedy_converges_as_printed() {
    v112_fn_table("v112_fn_mx_src");
    Spi::run("CREATE FUNCTION \"V112_fn_Mixed\"(int) RETURNS int LANGUAGE sql AS 'SELECT $1 + 1'")
        .expect("fn");
    let q = "SELECT id, \"V112_fn_Mixed\"(v) AS x FROM v112_fn_mx_src";
    let res = v112_fn_rejection("v112_fn_mx_v", q);
    assert!(res.contains("Volatile function(s) v112_fn_mixed"), "{res}");
    Spi::run(&v112_fn_printed_alter_function(res)).expect("the printed remedy must run as printed");
    v112_fn_created("v112_fn_mx_v", q, "IMMEDIATE");
    v112_fn_assert_correct_through_dml("v112_fn_mx_v", q, "v112_fn_mx_src", false);
}

#[pg_test]
fn v112_fn_function_outside_the_search_path_remedy_converges_as_printed() {
    v112_fn_table("v112_fn_sp_src");
    v112_fn_same_name_in_two_schemas("v112_fn_sp");
    let q = "SELECT id, v112_fn_sp_s2.v112_fn_sp_f(v) AS x FROM v112_fn_sp_src";
    let res = v112_fn_rejection("v112_fn_sp_v", q);
    Spi::run(&v112_fn_printed_alter_function(res)).expect("the printed remedy must run as printed");
    v112_fn_created("v112_fn_sp_v", q, "IMMEDIATE");
    v112_fn_assert_correct_through_dml("v112_fn_sp_v", q, "v112_fn_sp_src", false);
}

#[pg_test]
fn v112_fn_volatile_overload_rejection_names_it_and_converges() {
    v112_fn_table("v112_fn_ov_src");
    Spi::run("CREATE FUNCTION v112_fn_ov(int) RETURNS int LANGUAGE sql IMMUTABLE AS 'SELECT $1 + 1'")
        .expect("int overload");
    Spi::run("CREATE FUNCTION v112_fn_ov(text) RETURNS int LANGUAGE sql VOLATILE AS 'SELECT length($1)'")
        .expect("text overload");
    let q = "SELECT id, v112_fn_ov(v) AS x FROM v112_fn_ov_src";
    let res = v112_fn_rejection("v112_fn_ov_v", q);
    assert!(res.contains("v112_fn_ov(text)"), "{res}");
    assert!(!res.contains("v112_fn_ov(integer)"), "{res}");
    Spi::run(&v112_fn_printed_alter_function(res)).expect("the printed remedy must run as printed");
    v112_fn_created("v112_fn_ov_v", q, "IMMEDIATE");
    v112_fn_assert_correct_through_dml("v112_fn_ov_v", q, "v112_fn_ov_src", false);
}

#[pg_test]
fn v112_fn_quoted_upper_case_schema_is_matched_exactly() {
    v112_fn_table("v112_fn_up_src");
    Spi::run("CREATE SCHEMA \"V112_fn_Up\"").expect("quoted schema");
    Spi::run("CREATE SCHEMA v112_fn_up").expect("folded schema");
    Spi::run(
        "CREATE FUNCTION \"V112_fn_Up\".v112_fn_up_f(int) RETURNS int LANGUAGE sql IMMUTABLE AS 'SELECT $1 + 1'",
    )
    .expect("immutable fn");
    Spi::run("CREATE FUNCTION v112_fn_up.v112_fn_up_f(int) RETURNS int LANGUAGE sql AS 'SELECT $1 + 1'")
        .expect("unlabelled fn");
    for (i, q) in [
        "SELECT id, v112_fn_up.v112_fn_up_f(v) AS x FROM v112_fn_up_src",
        "SELECT id, V112_FN_UP.v112_fn_up_f(v) AS x FROM v112_fn_up_src",
        "SELECT id, \"v112_fn_up\".v112_fn_up_f(v) AS x FROM v112_fn_up_src",
    ]
    .iter()
    .enumerate()
    {
        let res = v112_fn_rejection(&format!("v112_fn_up_{i}"), q);
        assert!(res.contains("v112_fn_up.v112_fn_up_f(integer)"), "{res}");
    }
    let q = "SELECT id, \"V112_fn_Up\".v112_fn_up_f(v) AS x FROM v112_fn_up_src";
    v112_fn_created("v112_fn_up_ok", q, "IMMEDIATE");
    v112_fn_assert_correct_through_dml("v112_fn_up_ok", q, "v112_fn_up_src", false);
}

#[pg_test]
fn v112_fn_call_qualified_with_an_unknown_schema_is_matched_in_every_schema() {
    v112_fn_table("v112_fn_ns_src");
    Spi::run("CREATE FUNCTION v112_fn_ns_f(int) RETURNS int LANGUAGE sql VOLATILE AS 'SELECT $1 + 1'")
        .expect("fn");
    let res = v112_fn_rejection(
        "v112_fn_ns_v",
        "SELECT id, v112_fn_ns_absent.v112_fn_ns_f(v) AS x FROM v112_fn_ns_src",
    );
    assert!(res.contains("Volatile function(s) v112_fn_ns_f"), "{res}");
}

#[pg_test]
fn v112_fn_many_volatile_overloads_are_listed_up_to_a_cap() {
    v112_fn_table("v112_fn_cap_src");
    for argument in ["int", "bigint", "text", "numeric", "boolean"] {
        Spi::run(&format!(
            "CREATE FUNCTION v112_fn_cap_f({argument}) RETURNS int LANGUAGE sql VOLATILE AS 'SELECT 1'"
        ))
        .expect("fn");
    }
    let res = v112_fn_rejection(
        "v112_fn_cap_v",
        "SELECT id, v112_fn_cap_f(v) AS x FROM v112_fn_cap_src",
    );
    assert_eq!(res.matches("v112_fn_cap_f(").count(), 4, "{res}");
    assert!(res.contains(" and 2 more. Fix:"), "{res}");
    assert!(res.find("IMMUTABLE").is_some_and(|p| p < 300), "{res}");
}

#[pg_test]
fn v112_fn_user_functions_named_like_clock_functions_accepted_when_schema_qualified() {
    Spi::run("CREATE SCHEMA v112_fn_cq_s1").expect("schema");
    Spi::run("CREATE FUNCTION v112_fn_cq_s1.age(int) RETURNS int LANGUAGE sql IMMUTABLE AS 'SELECT $1 + 1'")
        .expect("age");
    Spi::run("CREATE FUNCTION v112_fn_cq_s1.now() RETURNS int LANGUAGE sql IMMUTABLE AS 'SELECT 7'")
        .expect("now");
    v112_fn_table("v112_fn_cqa_src");
    let age = "SELECT id, v112_fn_cq_s1.age(v) AS x FROM v112_fn_cqa_src";
    v112_fn_created("v112_fn_cqa", age, "IMMEDIATE");
    v112_fn_assert_correct_through_dml("v112_fn_cqa", age, "v112_fn_cqa_src", false);
    v112_fn_table("v112_fn_cqn_src");
    let now = "SELECT id, v + v112_fn_cq_s1.now() AS x FROM v112_fn_cqn_src";
    v112_fn_created("v112_fn_cqn", now, "IMMEDIATE");
    v112_fn_assert_correct_through_dml("v112_fn_cqn", now, "v112_fn_cqn_src", false);
}

#[pg_test]
fn v112_fn_volatile_user_function_in_a_user_schema_rejected_by_the_catalog() {
    v112_fn_table("v112_fn_cv_src");
    Spi::run("CREATE SCHEMA v112_fn_cv_s1").expect("schema");
    Spi::run(
        "CREATE FUNCTION v112_fn_cv_s1.random_like() RETURNS int LANGUAGE sql VOLATILE AS 'SELECT 1'",
    )
    .expect("random_like");
    Spi::run("CREATE FUNCTION v112_fn_cv_s1.now() RETURNS int LANGUAGE sql VOLATILE AS 'SELECT 1'")
        .expect("now");
    v112_fn_assert_rejected(
        "v112_fn_cv_0",
        "SELECT id, v + v112_fn_cv_s1.random_like() AS x FROM v112_fn_cv_src",
        "Volatile function(s) random_like",
    );
    v112_fn_assert_rejected(
        "v112_fn_cv_1",
        "SELECT id, v + v112_fn_cv_s1.now() AS x FROM v112_fn_cv_src",
        "ALTER FUNCTION v112_fn_cv_s1.now() IMMUTABLE",
    );
}

#[pg_test]
fn v112_fn_builtin_clock_calls_rejected_however_qualified() {
    v112_fn_table("v112_fn_cb_src");
    Spi::run("CREATE SCHEMA v112_fn_cb_s1").expect("schema");
    Spi::run(
        "CREATE FUNCTION v112_fn_cb_s1.now() RETURNS timestamptz LANGUAGE sql IMMUTABLE AS 'SELECT ''2026-01-01+00''::timestamptz'",
    )
    .expect("now");
    let queries = [
        "SELECT id, g FROM v112_fn_cb_src WHERE ts < pg_catalog.now()",
        "SELECT id, g FROM v112_fn_cb_src WHERE ts < PG_CATALOG.NOW()",
        "SELECT id, g FROM v112_fn_cb_src WHERE ts < \"pg_catalog\".\"now\"()",
        "SELECT id, g FROM v112_fn_cb_src WHERE ts < now()",
        "SELECT id, g, CURRENT_TIMESTAMP AS at FROM v112_fn_cb_src",
        "SELECT id, g, LOCALTIME AS at FROM v112_fn_cb_src",
    ];
    for (i, q) in queries.iter().enumerate() {
        v112_fn_assert_rejected(&format!("v112_fn_cb_{i}"), q, "Non-deterministic");
    }
    Spi::run("SET LOCAL search_path = v112_fn_cb_s1, public, pg_catalog").expect("search_path");
    for (i, q) in queries.iter().enumerate() {
        v112_fn_assert_rejected(&format!("v112_fn_cbp_{i}"), q, "Non-deterministic");
    }
}

#[pg_test]
fn v112_fn_clock_rejection_names_the_matched_function() {
    v112_fn_table("v112_fn_cn_src");
    v112_fn_assert_rejected(
        "v112_fn_cn_0",
        "SELECT id, g, age(ts) AS a FROM v112_fn_cn_src",
        "Fix: remove age from the IMV query",
    );
    v112_fn_assert_rejected(
        "v112_fn_cn_1",
        "SELECT id, g, age(ts) AS a, pg_catalog.now() AS n, CURRENT_DATE AS d FROM v112_fn_cn_src",
        "Fix: remove age, now, current_date from the IMV query",
    );
}

#[pg_test]
fn v112_fn_ungrouped_column_rejection_prints_the_cte_remedy_first() {
    v112_fn_table("v112_fn_ro_src");
    let res = v112_fn_rejection(
        "v112_fn_ro_v",
        "SELECT COUNT(DISTINCT v) IN (1, 2) AS e, COUNT(*) AS n FROM v112_fn_ro_src",
    );
    let cte_remedy = res.find("outer passthrough layer over a CTE").expect("CTE remedy");
    let group_by_remedy = res.find("GROUP BY clause").expect("GROUP BY remedy");
    assert!(cte_remedy < group_by_remedy, "{res}");
}

fn v112_fn_assert_keyed_unknown_aggregate_rejected(name: &str, q: &str, key: Option<&str>, mode: &str) {
    let res = crate::create_reflex_ivm(name, q, key, None, Some(mode), None);
    assert!(res.starts_with("ERROR: [reflex-unsupported]"), "{q} -> {res}");
    assert!(res.contains("Unsupported aggregate(s): V112_FN_UK_AGG"), "{q} -> {res}");
    assert!(res.find("Fix:").is_some_and(|p| p < 300), "{res}");
    assert_eq!(v112_fn_leftovers(name), 0, "objects left behind for {name}");
    assert_eq!(
        Spi::get_one::<i64>(&format!(
            "SELECT COUNT(*) FROM public.__reflex_ivm_reference WHERE name LIKE '{name}%'"
        ))
        .expect("registry")
        .unwrap_or(0),
        0,
        "registry rows left behind for {name}"
    );
    assert_eq!(
        Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid \
             WHERE NOT t.tgisinternal AND c.relname LIKE 'v112\\_fn\\_uk%\\_src'"
        )
        .expect("triggers")
        .unwrap_or(0),
        0,
        "triggers left behind for {name}"
    );
}

#[pg_test]
fn v112_fn_ungrouped_unknown_aggregate_aliased_like_the_primary_key_rejected() {
    v112_fn_table("v112_fn_uk_src");
    v112_fn_unknown_aggregate("v112_fn_uk_agg");
    for (i, mode) in ["IMMEDIATE", "DEFERRED"].iter().enumerate() {
        v112_fn_assert_keyed_unknown_aggregate_rejected(
            &format!("v112_fn_uk_a{i}"),
            "SELECT v112_fn_uk_agg(v) AS id FROM v112_fn_uk_src",
            None,
            mode,
        );
        v112_fn_assert_keyed_unknown_aggregate_rejected(
            &format!("v112_fn_uk_b{i}"),
            "SELECT v112_fn_uk_agg(id) AS id, v112_fn_uk_agg(v) AS m FROM v112_fn_uk_src",
            None,
            mode,
        );
    }
    let renamed = "SELECT v112_fn_uk_agg(v) AS m FROM v112_fn_uk_src";
    v112_fn_created("v112_fn_uk_a0", renamed, "IMMEDIATE");
    v112_fn_assert_correct_through_dml("v112_fn_uk_a0", renamed, "v112_fn_uk_src", false);
}

#[pg_test]
fn v112_fn_ungrouped_unknown_aggregate_with_a_declared_key_rejected() {
    v112_fn_table("v112_fn_ukd_src");
    v112_fn_table("v112_fn_ukj_src");
    v112_fn_unknown_aggregate("v112_fn_uk_agg");
    for (i, mode) in ["IMMEDIATE", "DEFERRED"].iter().enumerate() {
        v112_fn_assert_keyed_unknown_aggregate_rejected(
            &format!("v112_fn_uk_d{i}"),
            "SELECT v112_fn_uk_agg(v) AS m FROM v112_fn_ukd_src",
            Some("m"),
            mode,
        );
        v112_fn_assert_keyed_unknown_aggregate_rejected(
            &format!("v112_fn_uk_j{i}"),
            "SELECT v112_fn_uk_agg(b.id) AS id, v112_fn_uk_agg(a.v) AS m \
             FROM v112_fn_ukd_src a JOIN v112_fn_ukj_src b ON a.id = b.id",
            Some("id"),
            mode,
        );
    }
    let keyless = "SELECT v112_fn_uk_agg(v) AS m FROM v112_fn_ukd_src";
    v112_fn_created("v112_fn_uk_d1", keyless, "DEFERRED");
    v112_fn_assert_correct_through_dml("v112_fn_uk_d1", keyless, "v112_fn_ukd_src", true);
}

#[pg_test]
fn v112_fn_builtin_volatile_function_rejection_does_not_advise_altering_it() {
    v112_fn_table("v112_fn_bv_src");
    for (i, q) in [
        "SELECT id, nextval('v112_fn_bv_src_id_seq') AS n FROM v112_fn_bv_src",
        "SELECT id, pg_catalog.nextval('v112_fn_bv_src_id_seq') AS n FROM v112_fn_bv_src",
    ]
    .iter()
    .enumerate()
    {
        let res = v112_fn_rejection(&format!("v112_fn_bv_{i}"), q);
        assert!(res.contains("nextval(regclass)"), "{res}");
        assert!(!res.contains("ALTER FUNCTION"), "{res}");
        assert!(res.contains("volatile built-in"), "{res}");
        assert!(res.find("Fix: remove").is_some_and(|p| p < 300), "{res}");
    }
}

#[pg_test]
fn v112_fn_remedy_names_only_the_user_function_beside_a_builtin_volatile_one() {
    v112_fn_table("v112_fn_bm_src");
    Spi::run("CREATE FUNCTION v112_fn_bm_f(int) RETURNS int LANGUAGE sql AS 'SELECT $1 + 1'")
        .expect("fn");
    let q = "SELECT id, v112_fn_bm_f(v) AS x, nextval('v112_fn_bm_src_id_seq') AS n FROM v112_fn_bm_src";
    let res = v112_fn_rejection("v112_fn_bm_v", q);
    assert!(res.contains("nextval(regclass)"), "{res}");
    assert!(res.contains("volatile built-in"), "{res}");
    assert_eq!(
        v112_fn_printed_alter_function(res),
        "ALTER FUNCTION v112_fn_bm_f(integer) IMMUTABLE"
    );
    assert_eq!(res.matches("ALTER FUNCTION").count(), 1, "{res}");
}

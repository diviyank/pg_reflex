fn v112_distinct_table(table: &str) {
    Spi::run(&format!("CREATE TABLE {table}(id SERIAL PRIMARY KEY, g TEXT, v INT)")).expect("create");
    Spi::run(&format!("INSERT INTO {table}(g, v) VALUES ('a',1),('a',1),('a',2),('b',5),('b',5)"))
        .expect("seed");
}

fn v112_no_relation(name: &str) -> bool {
    Spi::get_one::<bool>(&format!("SELECT to_regclass('{name}') IS NULL"))
        .expect("regclass")
        .unwrap_or(false)
}

#[pg_test]
fn v112_distinct_spaced_sum_is_rejected_before_ddl() {
    v112_distinct_table("v112_dist_a_src");
    for (i, q) in [
        "SELECT g, SUM( DISTINCT v) AS s FROM v112_dist_a_src GROUP BY g",
        "SELECT g, AVG(\n DISTINCT v) AS s FROM v112_dist_a_src GROUP BY g",
        "SELECT g, COUNT(*) AS c FROM v112_dist_a_src GROUP BY g HAVING MAX( DISTINCT v) > 1",
        "WITH c AS (SELECT g, MIN( DISTINCT v) AS s FROM v112_dist_a_src GROUP BY g) SELECT g, s FROM c",
    ]
    .iter()
    .enumerate()
    {
        let name = format!("v112_dist_a_{i}");
        let res = crate::create_reflex_ivm(&name, q, None, None, None, None);
        assert!(res.starts_with("ERROR: [reflex-unsupported]"), "{q} -> {res}");
        assert!(res.contains("DISTINCT modifier"), "{q} -> {res}");
        assert!(v112_no_relation(&name), "{name} left behind");
        assert!(v112_no_relation(&format!("{name}__cte_c")), "sub-IMV left behind");
    }
}

#[pg_test]
fn v112_distinct_count_distinct_still_created_and_correct() {
    v112_distinct_table("v112_dist_c_src");
    let q = "SELECT g, COUNT( DISTINCT v) AS c FROM v112_dist_c_src GROUP BY g";
    assert_eq!(
        crate::create_reflex_ivm("v112_dist_c", q, None, None, None, None),
        "CREATE REFLEX INCREMENTAL VIEW"
    );
    Spi::run("INSERT INTO v112_dist_c_src(g, v) VALUES ('a',2),('a',9),('c',1)").expect("insert");
    Spi::run("DELETE FROM v112_dist_c_src WHERE id = 3").expect("delete");
    assert_imv_correct("v112_dist_c", q);
}

#[pg_test]
fn v112_distinct_keyword_inside_literal_is_accepted() {
    v112_distinct_table("v112_dist_l_src");
    let q = "SELECT g, SUM(v) AS s FROM v112_dist_l_src WHERE g <> 'max (distinct' GROUP BY g";
    assert_eq!(
        crate::create_reflex_ivm("v112_dist_l", q, None, None, None, None),
        "CREATE REFLEX INCREMENTAL VIEW"
    );
    Spi::run("INSERT INTO v112_dist_l_src(g, v) VALUES ('a',7)").expect("insert");
    Spi::run("UPDATE v112_dist_l_src SET v = v + 1 WHERE g = 'b'").expect("update");
    assert_imv_correct("v112_dist_l", q);
}

#[pg_test]
fn v112_distinct_in_window_call_is_rejected_before_ddl() {
    v112_distinct_table("v112_dist_w_src");
    for (i, q) in [
        "SELECT g, v, SUM(DISTINCT v) OVER () AS s FROM v112_dist_w_src",
        "SELECT g, v, SUM( DISTINCT v) OVER (PARTITION BY g) AS s FROM v112_dist_w_src",
    ]
    .iter()
    .enumerate()
    {
        let name = format!("v112_dist_w_{i}");
        let res = crate::create_reflex_ivm(&name, q, None, None, None, None);
        assert!(res.starts_with("ERROR: [reflex-unsupported]"), "{q} -> {res}");
        assert!(res.contains("DISTINCT modifier"), "{q} -> {res}");
        assert!(v112_no_relation(&name), "{name} left behind");
        assert!(v112_no_relation(&format!("{name}__base")), "{name}__base left behind");
    }
}

#[pg_test]
fn v112_nested_count_distinct_is_rejected_before_ddl() {
    v112_distinct_table("v112_dist_n_src");
    for (i, q) in [
        "SELECT g, COUNT(DISTINCT v) + 1 AS c FROM v112_dist_n_src GROUP BY g",
        "SELECT g, COALESCE(COUNT(DISTINCT v), 0) AS c FROM v112_dist_n_src GROUP BY g",
        "SELECT g, CASE WHEN COUNT(DISTINCT v) > 1 THEN 1 ELSE 0 END AS c FROM v112_dist_n_src GROUP BY g",
        "SELECT g, COUNT( DISTINCT v)::int + 1 AS c FROM v112_dist_n_src GROUP BY g",
    ]
    .iter()
    .enumerate()
    {
        let name = format!("v112_dist_n_{i}");
        let res = crate::create_reflex_ivm(&name, q, None, None, None, None);
        assert!(res.starts_with("ERROR: [reflex-unsupported]"), "{q} -> {res}");
        assert!(res.contains("COUNT(DISTINCT) nested inside an expression"), "{q} -> {res}");
        assert!(v112_no_relation(&name), "{name} left behind");
    }
}

#[pg_test]
fn v112_top_level_count_distinct_with_cast_stays_correct() {
    v112_distinct_table("v112_dist_k_src");
    let q = "SELECT g, COUNT(DISTINCT v)::int AS c FROM v112_dist_k_src GROUP BY g";
    assert_eq!(
        crate::create_reflex_ivm("v112_dist_k", q, None, None, None, None),
        "CREATE REFLEX INCREMENTAL VIEW"
    );
    Spi::run("INSERT INTO v112_dist_k_src(g, v) VALUES ('a',2),('a',9),('c',1)").expect("insert");
    Spi::run("UPDATE v112_dist_k_src SET v = 7 WHERE id = 1").expect("update");
    Spi::run("DELETE FROM v112_dist_k_src WHERE id = 4").expect("delete");
    assert_imv_correct("v112_dist_k", q);
}

#[pg_test]
fn v112_count_distinct_cte_remedy_stays_correct() {
    v112_distinct_table("v112_dist_r_src");
    let q = "WITH d_cnt AS (SELECT g, COUNT(DISTINCT v) AS n FROM v112_dist_r_src GROUP BY g) \
             SELECT g, n + 1 AS c FROM d_cnt";
    assert_eq!(
        crate::create_reflex_ivm("v112_dist_r", q, None, None, None, None),
        "CREATE REFLEX INCREMENTAL VIEW"
    );
    assert_imv_correct("v112_dist_r", q);
    Spi::run("INSERT INTO v112_dist_r_src(g, v) VALUES ('a',2),('a',9),('c',1)").expect("insert");
    assert_imv_correct("v112_dist_r", q);
    Spi::run("UPDATE v112_dist_r_src SET v = 7 WHERE id = 1").expect("update");
    Spi::run("DELETE FROM v112_dist_r_src WHERE id = 4").expect("delete");
    assert_imv_correct("v112_dist_r", q);
}

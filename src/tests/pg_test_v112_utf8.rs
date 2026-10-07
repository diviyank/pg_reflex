fn v112_utf8_dml(table: &str) {
    Spi::run(&format!(
        "INSERT INTO {table}(kind, season, v) VALUES ('a','Été',10),('b','Hiver',20)"
    ))
    .expect("insert");
    Spi::run(&format!("UPDATE {table} SET v = v + 100 WHERE id = 1")).expect("update");
    Spi::run(&format!("DELETE FROM {table} WHERE id = 3")).expect("delete");
}

fn v112_utf8_table(table: &str) {
    Spi::run(&format!(
        "CREATE TABLE {table}(id SERIAL PRIMARY KEY, kind TEXT, season TEXT, v INT)"
    ))
    .expect("create");
    Spi::run(&format!(
        "INSERT INTO {table}(kind, season, v) VALUES ('a','Été',1),('b','Hiver',2),('a','Été',3)"
    ))
    .expect("seed");
}

#[pg_test]
fn v112_utf8_aggregate_literal_stays_correct() {
    v112_utf8_table("v112_utf8_agg_src");
    let q = "SELECT season, SUM(v) AS s FROM v112_utf8_agg_src WHERE season = 'Été' GROUP BY season";
    assert_eq!(
        crate::create_reflex_ivm("v112_utf8_agg", q, None, None, None, None),
        "CREATE REFLEX INCREMENTAL VIEW"
    );
    v112_utf8_dml("v112_utf8_agg_src");
    assert_imv_correct("v112_utf8_agg", q);
}

#[pg_test]
fn v112_utf8_passthrough_literal_stays_correct() {
    v112_utf8_table("v112_utf8_pt_src");
    let q = "SELECT id, season, v FROM v112_utf8_pt_src WHERE season = 'Été'";
    assert_eq!(
        crate::create_reflex_ivm("v112_utf8_pt", q, None, None, None, None),
        "CREATE REFLEX INCREMENTAL VIEW"
    );
    v112_utf8_dml("v112_utf8_pt_src");
    assert_imv_correct("v112_utf8_pt", q);
}

#[pg_test]
fn v112_utf8_join_literal_stays_correct() {
    v112_utf8_table("v112_utf8_j_src");
    Spi::run("CREATE TABLE v112_utf8_j_dim(kind TEXT PRIMARY KEY, label TEXT)").expect("dim");
    Spi::run("INSERT INTO v112_utf8_j_dim VALUES ('a','Catégorie A'),('b','Catégorie B')")
        .expect("dim seed");
    let q = "SELECT d.label, SUM(s.v) AS s FROM v112_utf8_j_src s \
             JOIN v112_utf8_j_dim d ON d.kind = s.kind \
             WHERE d.label <> 'Catégorie Z' AND s.season = 'Été' GROUP BY d.label";
    assert_eq!(
        crate::create_reflex_ivm("v112_utf8_j", q, None, None, None, None),
        "CREATE REFLEX INCREMENTAL VIEW"
    );
    v112_utf8_dml("v112_utf8_j_src");
    Spi::run("UPDATE v112_utf8_j_dim SET label = 'Catégorie Ä' WHERE kind = 'a'").expect("dim upd");
    assert_imv_correct("v112_utf8_j", q);
}

#[pg_test]
fn v112_utf8_deferred_literal_stays_correct() {
    v112_utf8_table("v112_utf8_d_src");
    let q = "SELECT season, COUNT(*) AS c FROM v112_utf8_d_src WHERE season = 'Été' GROUP BY season";
    assert_eq!(
        crate::create_reflex_ivm("v112_utf8_d", q, None, None, Some("DEFERRED"), None),
        "CREATE REFLEX INCREMENTAL VIEW"
    );
    v112_utf8_dml("v112_utf8_d_src");
    Spi::run("SELECT reflex_flush_deferred('v112_utf8_d_src')").expect("flush");
    assert_imv_correct("v112_utf8_d", q);
}

#[pg_test]
fn v112_utf8_cte_literal_stays_correct() {
    v112_utf8_table("v112_utf8_c_src");
    let q = "WITH e AS (SELECT kind, SUM(v) AS s FROM v112_utf8_c_src WHERE season = 'Été' GROUP BY kind) \
             SELECT kind, s FROM e WHERE kind <> 'ñ'";
    assert_eq!(
        crate::create_reflex_ivm("v112_utf8_c", q, None, None, None, None),
        "CREATE REFLEX INCREMENTAL VIEW"
    );
    v112_utf8_dml("v112_utf8_c_src");
    assert_imv_correct("v112_utf8_c", q);
}

#[pg_test]
fn v112_utf8_non_ascii_column_name_stays_correct() {
    Spi::run("CREATE TABLE v112_utf8_col_src(id SERIAL PRIMARY KEY, \"été\" TEXT, v INT)")
        .expect("create");
    Spi::run("INSERT INTO v112_utf8_col_src(\"été\", v) VALUES ('x',1),('y',2),('x',3)")
        .expect("seed");
    let q = "SELECT \"été\", SUM(v) AS s FROM v112_utf8_col_src GROUP BY \"été\"";
    let res = crate::create_reflex_ivm("v112_utf8_col", q, None, None, None, None);
    if res.starts_with("ERROR") {
        // A clean refusal of a non-ASCII column name is acceptable; silent divergence is not.
        return;
    }
    Spi::run("INSERT INTO v112_utf8_col_src(\"été\", v) VALUES ('x',10)").expect("insert");
    Spi::run("DELETE FROM v112_utf8_col_src WHERE id = 3").expect("delete");
    assert_imv_correct("v112_utf8_col", q);
}

const V112_CM_CREATED: &str = "CREATE REFLEX INCREMENTAL VIEW";

fn v112_cm_flush(source: &str, mode: &str) {
    if mode == "DEFERRED" {
        Spi::run(&format!("SELECT reflex_flush_deferred('{source}')")).expect("flush");
    }
}

/// Inside a test transaction no COMMIT flushes: the source is flushed first,
/// then each CTE sub-IMV in creation order, as each is a source of the next,
/// registered under its double-quoted name.
fn v112_cm_flush_through_cte_sub_imvs(source: &str, imv: &str, mode: &str) {
    v112_cm_flush(source, mode);
    if mode == "DEFERRED" {
        Spi::run(&format!(
            "SELECT reflex_flush_deferred('\"' || c.relname || '\"') FROM \
             (SELECT relname FROM pg_class WHERE relname LIKE '{imv}\\_\\_cte\\_%' \
              AND relkind = 'r' ORDER BY oid) c"
        ))
        .expect("flush sub-IMVs");
    }
}

/// Seeds `{prefix}_t`, creates `{prefix}_v` from `query` (which names its
/// source `{src}`) and checks it against a fresh run of the same query after
/// create and after every kind of write.
fn v112_cm_assert_cte_imv_correct(prefix: &str, mode: &str, query: &str) {
    let src = format!("{prefix}_t");
    let imv = format!("{prefix}_v");
    let query = query.replace("{src}", &src);
    Spi::run(&format!(
        "CREATE TABLE {src}(id SERIAL PRIMARY KEY, season TEXT, v INT)"
    ))
    .expect("source");
    Spi::run(&format!(
        "INSERT INTO {src}(season, v) VALUES ('e',1),('y',2),('e',3),('a-e-b',4),('s',5)"
    ))
    .expect("seed");
    assert_eq!(
        crate::create_reflex_ivm(&imv, &query, None, None, Some(mode), None),
        V112_CM_CREATED,
        "creating {imv}"
    );
    assert_imv_correct(&imv, &query);
    for write in [
        "INSERT INTO {src}(season, v) VALUES ('e',10),('y',20),('z',30),('a-e-b',40),('s',50)",
        "UPDATE {src} SET v = v + 100 WHERE id IN (1, 2)",
        "UPDATE {src} SET season = 'e' WHERE season = 'z'",
        "UPDATE {src} SET season = 'w' WHERE id = 3",
        "DELETE FROM {src} WHERE id IN (1, 4)",
        "DELETE FROM {src}",
        "INSERT INTO {src}(season, v) VALUES ('e',7),('y',8),('s',9)",
    ] {
        Spi::run(&write.replace("{src}", &src)).expect("write");
        v112_cm_flush_through_cte_sub_imvs(&src, &imv, mode);
        assert_imv_correct(&imv, &query);
    }
}

const V112_CM_LITERAL_IN_MAIN_BODY: &str =
    "WITH e AS (SELECT season, SUM(v) AS s FROM {src} GROUP BY season) \
     SELECT season, s FROM e WHERE season <> 'e'";

const V112_CM_LITERAL_IN_LATER_CTE: &str =
    "WITH e AS (SELECT season, SUM(v) AS s FROM {src} GROUP BY season), \
     f AS (SELECT season, s FROM e WHERE season <> 'e') \
     SELECT season, s FROM f";

const V112_CM_LITERAL_EMBEDDING_ALIAS: &str =
    "WITH e AS (SELECT season, SUM(v) AS s FROM {src} GROUP BY season) \
     SELECT season, s FROM e WHERE season <> 'a-e-b'";

const V112_CM_DOLLAR_QUOTED_LITERAL: &str =
    "WITH e AS (SELECT season, SUM(v) AS s FROM {src} GROUP BY season) \
     SELECT season, s FROM e WHERE season <> $q$e$q$";

const V112_CM_PROJECTED_LITERAL: &str =
    "WITH e AS (SELECT season, SUM(v) AS s FROM {src} GROUP BY season) \
     SELECT season, s, 'e' AS tag FROM e";

#[pg_test]
fn v112_cm_literal_equal_to_cte_alias_in_main_body_immediate() {
    v112_cm_assert_cte_imv_correct("v112_cm_lm", "IMMEDIATE", V112_CM_LITERAL_IN_MAIN_BODY);
}

#[pg_test]
fn v112_cm_literal_equal_to_cte_alias_in_main_body_deferred() {
    v112_cm_assert_cte_imv_correct("v112_cm_lmd", "DEFERRED", V112_CM_LITERAL_IN_MAIN_BODY);
}

#[pg_test]
fn v112_cm_literal_equal_to_cte_alias_in_later_cte_immediate() {
    v112_cm_assert_cte_imv_correct("v112_cm_lc", "IMMEDIATE", V112_CM_LITERAL_IN_LATER_CTE);
}

#[pg_test]
fn v112_cm_literal_equal_to_cte_alias_in_later_cte_deferred() {
    v112_cm_assert_cte_imv_correct("v112_cm_lcd", "DEFERRED", V112_CM_LITERAL_IN_LATER_CTE);
}

#[pg_test]
fn v112_cm_literal_embedding_cte_alias_stays_correct() {
    v112_cm_assert_cte_imv_correct("v112_cm_le", "IMMEDIATE", V112_CM_LITERAL_EMBEDDING_ALIAS);
}

#[pg_test]
fn v112_cm_dollar_quoted_literal_equal_to_cte_alias_stays_correct() {
    v112_cm_assert_cte_imv_correct("v112_cm_dq", "IMMEDIATE", V112_CM_DOLLAR_QUOTED_LITERAL);
}

#[pg_test]
fn v112_cm_projected_literal_equal_to_cte_alias_stays_correct() {
    v112_cm_assert_cte_imv_correct("v112_cm_pl", "IMMEDIATE", V112_CM_PROJECTED_LITERAL);
}

/// The control: nothing in these queries but relation references and
/// qualifiers spells the alias.
#[pg_test]
fn v112_cm_cte_alias_as_relation_and_qualifier_stays_correct() {
    v112_cm_assert_cte_imv_correct(
        "v112_cm_ct",
        "IMMEDIATE",
        "WITH e AS (SELECT season, SUM(v) AS s FROM {src} GROUP BY season), \
         f AS (SELECT e.season, e.s FROM e WHERE e.season <> 'x') \
         SELECT f.season, x.s FROM f JOIN e AS x ON x.season = f.season WHERE f.s IS NOT NULL",
    );
}

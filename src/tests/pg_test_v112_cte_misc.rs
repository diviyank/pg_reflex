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

const V112_CM_ALIAS_NAMES_AGGREGATE_COLUMN: &str =
    "WITH s AS (SELECT season, SUM(v) AS s FROM {src} GROUP BY season) SELECT season, s FROM s";

const V112_CM_ALIAS_NAMES_DISTINCT_COUNT_COLUMN: &str =
    "WITH s AS (SELECT season, COUNT(DISTINCT v) AS s FROM {src} GROUP BY season) \
     SELECT season, s FROM s";

const V112_CM_ALIAS_NAMES_PLAIN_COLUMN: &str =
    "WITH v AS (SELECT id, season, v FROM {src}) SELECT id, season, v FROM v WHERE v > 2";

const V112_CM_ALIAS_NAMES_GROUPING_COLUMN: &str =
    "WITH season AS (SELECT id, season, v FROM {src}) \
     SELECT season, SUM(v) AS total FROM season GROUP BY season";

const V112_CM_ALIAS_NAMES_COLUMN_OF_EARLIER_CTE: &str =
    "WITH s AS (SELECT season, SUM(v) AS s FROM {src} GROUP BY season), \
     f AS (SELECT season, s FROM s WHERE s > 2) SELECT season, s FROM f";

#[pg_test]
fn v112_cm_cte_alias_naming_its_aggregate_column_immediate() {
    v112_cm_assert_cte_imv_correct(
        "v112_cm_ca",
        "IMMEDIATE",
        V112_CM_ALIAS_NAMES_AGGREGATE_COLUMN,
    );
}

#[pg_test]
fn v112_cm_cte_alias_naming_its_aggregate_column_deferred() {
    v112_cm_assert_cte_imv_correct(
        "v112_cm_cad",
        "DEFERRED",
        V112_CM_ALIAS_NAMES_AGGREGATE_COLUMN,
    );
}

#[pg_test]
fn v112_cm_cte_alias_naming_its_distinct_count_column_stays_correct() {
    v112_cm_assert_cte_imv_correct(
        "v112_cm_cd",
        "IMMEDIATE",
        V112_CM_ALIAS_NAMES_DISTINCT_COUNT_COLUMN,
    );
}

#[pg_test]
fn v112_cm_cte_alias_naming_a_plain_column_stays_correct() {
    v112_cm_assert_cte_imv_correct("v112_cm_cp", "IMMEDIATE", V112_CM_ALIAS_NAMES_PLAIN_COLUMN);
}

#[pg_test]
fn v112_cm_cte_alias_naming_the_grouping_column_stays_correct() {
    v112_cm_assert_cte_imv_correct(
        "v112_cm_cg",
        "IMMEDIATE",
        V112_CM_ALIAS_NAMES_GROUPING_COLUMN,
    );
}

#[pg_test]
fn v112_cm_cte_alias_naming_a_column_read_by_a_later_cte_stays_correct() {
    v112_cm_assert_cte_imv_correct(
        "v112_cm_ce",
        "IMMEDIATE",
        V112_CM_ALIAS_NAMES_COLUMN_OF_EARLIER_CTE,
    );
}

#[pg_test]
fn v112_cm_cte_alias_naming_its_column_keeps_the_column_type() {
    v112_cm_assert_cte_imv_correct(
        "v112_cm_cy",
        "IMMEDIATE",
        V112_CM_ALIAS_NAMES_AGGREGATE_COLUMN,
    );
    let column_type = Spi::get_one::<String>(
        "SELECT format_type(atttypid, atttypmod) FROM pg_attribute \
         WHERE attrelid = 'v112_cm_cy_v'::regclass AND attname = 's'",
    )
    .expect("type")
    .expect("column s");
    assert_eq!(column_type, "bigint");
}

/// A LIST-partitioned source on `column`, with one partition per bound and a
/// DEFAULT one, mirrored by the keyed passthrough IMV `{prefix}_v`.
fn v112_cm_partitioned_passthrough(prefix: &str, column: &str, bounds: &[&str]) -> String {
    let src = format!("{prefix}_s");
    Spi::run(&format!(
        "CREATE TABLE {src}({column} TEXT NOT NULL, id INT NOT NULL, v INT) PARTITION BY LIST ({column})"
    ))
    .expect("source");
    for (i, bound) in bounds.iter().enumerate() {
        Spi::run(&format!(
            "CREATE TABLE {src}_p{i} PARTITION OF {src} FOR VALUES IN ('{bound}')"
        ))
        .expect("partition");
        Spi::run(&format!(
            "INSERT INTO {src} SELECT '{bound}', g, g FROM generate_series(1, 20) g"
        ))
        .expect("seed");
    }
    Spi::run(&format!("CREATE TABLE {src}_pd PARTITION OF {src} DEFAULT"))
        .expect("default partition");
    Spi::run(&format!(
        "INSERT INTO {src} SELECT 'elsewhere', g, g FROM generate_series(1, 5) g"
    ))
    .expect("seed default");
    let query = format!("SELECT {column}, id, v FROM {src}");
    let created = Spi::get_one::<String>(&format!(
        "SELECT create_reflex_ivm('{prefix}_v', '{query}', '{column}, id', NULL, 'IMMEDIATE', \
         NULL, ARRAY['{column}'])"
    ))
    .expect("create")
    .expect("result");
    assert_eq!(created, V112_CM_CREATED);
    assert_imv_correct(&format!("{prefix}_v"), &query);
    query
}

/// Makes the IMV's rows for `bound` wrong in the three possible ways: a
/// changed row, a missing row and a row the source does not have.
fn v112_cm_drift(imv: &str, column: &str, bound: &str) {
    Spi::run(&format!(
        "UPDATE {imv} SET v = v + 1000 WHERE {column} = '{bound}' AND id = 7"
    ))
    .expect("drift update");
    Spi::run(&format!(
        "DELETE FROM {imv} WHERE {column} = '{bound}' AND id = 8"
    ))
    .expect("drift delete");
    Spi::run(&format!("INSERT INTO {imv} VALUES ('{bound}', 999, 1)")).expect("drift insert");
}

fn v112_cm_assert_reconcile_by_key_repairs(prefix: &str, column: &str, bounds: &[&str]) {
    let imv = format!("{prefix}_v");
    let query = v112_cm_partitioned_passthrough(prefix, column, bounds);
    for bound in bounds {
        v112_cm_drift(&imv, column, bound);
        let reconciled = Spi::get_one::<String>(&format!(
            "SELECT reflex_reconcile_partition('{imv}', '{bound}')"
        ))
        .expect("reconcile")
        .expect("result");
        assert!(
            reconciled.starts_with("RECONCILED"),
            "key {bound}: {reconciled}"
        );
        assert_imv_correct(&imv, &query);
    }
}

#[pg_test]
fn v112_cm_reconcile_by_key_whose_bound_spells_the_partition_column() {
    v112_cm_assert_reconcile_by_key_repairs("v112_cm_rk", "k", &["a k b", "k", "K k", "plain"]);
}

#[pg_test]
fn v112_cm_reconcile_by_key_whose_multibyte_bound_holds_the_partition_column() {
    v112_cm_assert_reconcile_by_key_repairs("v112_cm_rm", "t", &["Été", "Hiver", "t夏t"]);
}

#[pg_test]
fn v112_cm_reconcile_by_key_holding_a_quote_next_to_the_partition_column() {
    v112_cm_assert_reconcile_by_key_repairs("v112_cm_rq", "k", &["it''s k", "k''k"]);
}

/// A populated leaf of an IMV that another IMV reads is rebuilt as a row diff
/// scoped by the leaf's partition constraint.
fn v112_cm_assert_leaf_diff_repairs(prefix: &str, column: &str, bounds: &[&str]) {
    let imv = format!("{prefix}_v");
    let dependent = format!("{prefix}_d");
    let query = v112_cm_partitioned_passthrough(prefix, column, bounds);
    let dependent_query =
        format!("SELECT {column}, SUM(v) AS total, COUNT(*) AS n FROM {{}} GROUP BY {column}");
    assert_eq!(
        crate::create_reflex_ivm(
            &dependent,
            &dependent_query.replace("{}", &imv),
            None,
            None,
            None,
            None
        ),
        V112_CM_CREATED
    );
    let fresh_dependent = dependent_query.replace("{}", &format!("{prefix}_s"));
    for (i, bound) in bounds.iter().enumerate() {
        v112_cm_drift(&imv, column, bound);
        let reconciled = Spi::get_one::<String>(&format!(
            "SELECT reflex_reconcile_partition('{imv}', '', '{prefix}_s_p{i}')"
        ))
        .expect("reconcile")
        .expect("result");
        assert!(
            reconciled.starts_with("RECONCILED"),
            "partition of {bound}: {reconciled}"
        );
        assert_imv_correct(&imv, &query);
        assert_imv_correct(&dependent, &fresh_dependent);
    }
}

#[pg_test]
fn v112_cm_leaf_diff_of_bound_spelling_the_partition_column_repairs_the_leaf() {
    v112_cm_assert_leaf_diff_repairs("v112_cm_dk", "k", &["a k b", "plain"]);
}

#[pg_test]
fn v112_cm_leaf_diff_of_multibyte_bound_holding_the_partition_column_repairs_the_leaf() {
    v112_cm_assert_leaf_diff_repairs("v112_cm_dm", "t", &["Été", "Hiver"]);
}

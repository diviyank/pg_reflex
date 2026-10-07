fn v112_sr_seeded_table(table: &str, literal: &str) {
    Spi::run(&format!(
        "CREATE TABLE {table}(id SERIAL PRIMARY KEY, season TEXT, v INT)"
    ))
    .expect("create");
    Spi::run(&format!(
        "INSERT INTO {table}(season, v) VALUES ('{literal}',1),('y',2),('{literal}',3)"
    ))
    .expect("seed");
}

fn v112_sr_imv(name: &str, query: &str) {
    assert_eq!(
        crate::create_reflex_ivm(name, query, None, None, None, None),
        "CREATE REFLEX INCREMENTAL VIEW",
        "creating {name}"
    );
}

fn v112_sr_assert_all_correct(imvs: &[(&str, &str)]) {
    for (imv, query) in imvs {
        assert_imv_correct(imv, query);
    }
}

fn v112_sr_assert_correct_through_dml(table: &str, literal: &str, imvs: &[(&str, &str)]) {
    v112_sr_assert_all_correct(imvs);
    Spi::run(&format!(
        "INSERT INTO {table}(season, v) VALUES ('{literal}',10),('y',20)"
    ))
    .expect("insert");
    v112_sr_assert_all_correct(imvs);
    Spi::run(&format!("UPDATE {table} SET v = v + 100 WHERE id = 1")).expect("update");
    v112_sr_assert_all_correct(imvs);
    Spi::run(&format!("DELETE FROM {table} WHERE id = 3")).expect("delete");
    v112_sr_assert_all_correct(imvs);
}

fn v112_sr_aggregate_over_literal(table: &str, sql_literal: &str) -> String {
    format!("SELECT season, SUM(v) AS s FROM {table} WHERE season = {sql_literal} GROUP BY season")
}

#[pg_test]
fn v112_sr_literal_wrapping_source_name_in_multibyte_chars_stays_correct() {
    v112_sr_seeded_table("v112_sr_lc", "év112_sr_lcé");
    let q = v112_sr_aggregate_over_literal("v112_sr_lc", "'év112_sr_lcé'");
    v112_sr_imv("v112_sr_lc_agg", &q);
    v112_sr_assert_correct_through_dml("v112_sr_lc", "év112_sr_lcé", &[("v112_sr_lc_agg", &q)]);
}

#[pg_test]
fn v112_sr_literal_prefixing_source_name_with_multibyte_char_stays_correct() {
    v112_sr_seeded_table("v112_sr_lf", "év112_sr_lf");
    let q = v112_sr_aggregate_over_literal("v112_sr_lf", "'év112_sr_lf'");
    v112_sr_imv("v112_sr_lf_agg", &q);
    v112_sr_assert_correct_through_dml("v112_sr_lf", "év112_sr_lf", &[("v112_sr_lf_agg", &q)]);
}

#[pg_test]
fn v112_sr_ascii_literal_embedding_source_name_stays_correct() {
    v112_sr_seeded_table("v112_sr_le", "a-v112_sr_le-b");
    let agg = v112_sr_aggregate_over_literal("v112_sr_le", "'a-v112_sr_le-b'");
    let passthrough = "SELECT id, season, v FROM v112_sr_le WHERE season = 'a-v112_sr_le-b'";
    v112_sr_imv("v112_sr_le_agg", &agg);
    v112_sr_imv("v112_sr_le_pt", passthrough);
    v112_sr_assert_correct_through_dml(
        "v112_sr_le",
        "a-v112_sr_le-b",
        &[("v112_sr_le_agg", &agg), ("v112_sr_le_pt", passthrough)],
    );
}

#[pg_test]
fn v112_sr_short_source_name_inside_accented_literal_stays_correct() {
    Spi::run("CREATE SCHEMA v112_sr_sh").expect("schema");
    v112_sr_seeded_table("v112_sr_sh.t", "été");
    let q = v112_sr_aggregate_over_literal("v112_sr_sh.t", "'été'");
    v112_sr_imv("v112_sr_sh_agg", &q);
    v112_sr_assert_correct_through_dml("v112_sr_sh.t", "été", &[("v112_sr_sh_agg", &q)]);
}

#[pg_test]
fn v112_sr_literal_equal_to_source_name_stays_correct() {
    v112_sr_seeded_table("v112_sr_product", "v112_sr_product");
    let q = v112_sr_aggregate_over_literal("v112_sr_product", "'v112_sr_product'");
    v112_sr_imv("v112_sr_product_agg", &q);
    v112_sr_assert_correct_through_dml(
        "v112_sr_product",
        "v112_sr_product",
        &[("v112_sr_product_agg", &q)],
    );
}

#[pg_test]
fn v112_sr_literal_embedding_schema_qualified_source_name_stays_correct() {
    Spi::run("CREATE SCHEMA v112_sr_q").expect("schema");
    v112_sr_seeded_table("v112_sr_q.prod", "a-v112_sr_q.prod-b");
    let q = v112_sr_aggregate_over_literal("v112_sr_q.prod", "'a-v112_sr_q.prod-b'");
    v112_sr_imv("v112_sr_q_agg", &q);
    v112_sr_assert_correct_through_dml(
        "v112_sr_q.prod",
        "a-v112_sr_q.prod-b",
        &[("v112_sr_q_agg", &q)],
    );
}

#[pg_test]
fn v112_sr_dollar_quoted_literal_embedding_source_name_stays_correct() {
    v112_sr_seeded_table("v112_sr_dq", "a-v112_sr_dq-b");
    let agg = v112_sr_aggregate_over_literal("v112_sr_dq", "$$a-v112_sr_dq-b$$");
    let passthrough = "SELECT id, season, v FROM v112_sr_dq WHERE season = $$a-v112_sr_dq-b$$";
    v112_sr_imv("v112_sr_dq_agg", &agg);
    v112_sr_imv("v112_sr_dq_pt", passthrough);
    v112_sr_assert_correct_through_dml(
        "v112_sr_dq",
        "a-v112_sr_dq-b",
        &[("v112_sr_dq_agg", &agg), ("v112_sr_dq_pt", passthrough)],
    );
}

fn v112_sr_assert_grouping_column_correct_through_dml(table: &str, column: &str) {
    Spi::run(&format!(
        "CREATE TABLE {table}(id SERIAL PRIMARY KEY, {column} TEXT, v INT)"
    ))
    .expect("create");
    Spi::run(&format!(
        "INSERT INTO {table}({column}, v) VALUES ('x',1),('y',2),('x',3)"
    ))
    .expect("seed");
    let q = format!("SELECT {column}, SUM(v) AS s FROM {table} GROUP BY {column}");
    let imv = format!("{table}_agg");
    v112_sr_imv(&imv, &q);
    Spi::run(&format!("INSERT INTO {table}({column}, v) VALUES ('x',10),('z',20)")).expect("insert");
    assert_imv_correct(&imv, &q);
    Spi::run(&format!("UPDATE {table} SET v = v + 100 WHERE id = 1")).expect("update");
    assert_imv_correct(&imv, &q);
    Spi::run(&format!("DELETE FROM {table} WHERE id = 3")).expect("delete");
    assert_imv_correct(&imv, &q);
}

#[pg_test]
fn v112_sr_column_extending_source_name_with_multibyte_suffix_stays_correct() {
    v112_sr_assert_grouping_column_correct_through_dml("v112_sr_ct", "v112_sr_cté");
}

#[pg_test]
fn v112_sr_column_extending_source_name_with_multibyte_prefix_stays_correct() {
    v112_sr_assert_grouping_column_correct_through_dml("v112_sr_bp", "év112_sr_bp");
}

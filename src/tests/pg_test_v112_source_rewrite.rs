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

fn v112_sr_assert_mixed_spelling_correct(tag: &str, schema_ddl: &str, table_ref: &str) {
    Spi::run(&format!("CREATE SCHEMA {schema_ddl}")).expect("schema");
    v112_sr_seeded_table(table_ref, "a");
    let agg = format!("SELECT season, SUM(v) AS s FROM {table_ref} GROUP BY season");
    let passthrough = format!("SELECT id, season, v FROM {table_ref} WHERE v <> 2");
    let agg_imv = format!("v112_sr_{tag}_agg");
    let passthrough_imv = format!("v112_sr_{tag}_pt");
    v112_sr_imv(&agg_imv, &agg);
    v112_sr_imv(&passthrough_imv, &passthrough);
    v112_sr_assert_correct_through_dml(
        table_ref,
        "a",
        &[(&agg_imv, &agg), (&passthrough_imv, &passthrough)],
    );
}

#[pg_test]
fn v112_sr_quoted_schema_with_unquoted_table_stays_correct() {
    v112_sr_assert_mixed_spelling_correct("qs", "v112_sr_qs", "\"v112_sr_qs\".prod");
}

#[pg_test]
fn v112_sr_unquoted_schema_with_quoted_table_stays_correct() {
    v112_sr_assert_mixed_spelling_correct("qt", "v112_sr_qt", "v112_sr_qt.\"prod\"");
}

#[pg_test]
fn v112_sr_quoted_multibyte_schema_with_unquoted_table_stays_correct() {
    v112_sr_assert_mixed_spelling_correct("qm", "\"v112_sr_schéma\"", "\"v112_sr_schéma\".prod");
}

#[pg_test]
fn v112_sr_join_with_one_mixed_spelling_side_stays_correct() {
    Spi::run("CREATE SCHEMA v112_sr_js").expect("schema");
    Spi::run("CREATE TABLE v112_sr_js.fact(id SERIAL PRIMARY KEY, kind TEXT, v INT)").expect("fact");
    Spi::run("CREATE TABLE v112_sr_jdim(kind TEXT PRIMARY KEY, label TEXT)").expect("dim");
    Spi::run("INSERT INTO v112_sr_jdim VALUES ('a','A'),('b','B')").expect("dim seed");
    Spi::run("INSERT INTO v112_sr_js.fact(kind, v) VALUES ('a',1),('b',2),('a',3)").expect("seed");
    let aliased = "SELECT d.label, SUM(f.v) AS s FROM \"v112_sr_js\".fact f \
                   JOIN v112_sr_jdim d ON d.kind = f.kind GROUP BY d.label";
    let unaliased = "SELECT d.label, SUM(fact.v) AS s FROM \"v112_sr_js\".fact \
                     JOIN v112_sr_jdim d ON d.kind = fact.kind GROUP BY d.label";
    v112_sr_imv("v112_sr_js_aliased", aliased);
    v112_sr_imv("v112_sr_js_unaliased", unaliased);
    let imvs = [("v112_sr_js_aliased", aliased), ("v112_sr_js_unaliased", unaliased)];
    Spi::run("INSERT INTO v112_sr_js.fact(kind, v) VALUES ('a',10),('b',20)").expect("insert");
    v112_sr_assert_all_correct(&imvs);
    Spi::run("UPDATE v112_sr_js.fact SET v = v + 100 WHERE id = 1").expect("update");
    v112_sr_assert_all_correct(&imvs);
    Spi::run("DELETE FROM v112_sr_js.fact WHERE id = 3").expect("delete");
    v112_sr_assert_all_correct(&imvs);
    Spi::run("UPDATE v112_sr_jdim SET label = 'A2' WHERE kind = 'a'").expect("dim update");
    v112_sr_assert_all_correct(&imvs);
}

#[pg_test]
fn v112_sr_multibyte_identifier_in_join_condition_stays_correct() {
    Spi::run("CREATE TABLE v112_sr_da_s(id SERIAL PRIMARY KEY, kind TEXT, v INT)").expect("s");
    Spi::run("CREATE TABLE v112_sr_da_d(\"clé\" TEXT PRIMARY KEY, label TEXT)").expect("d");
    Spi::run("INSERT INTO v112_sr_da_d VALUES ('a','A'),('b','B')").expect("d seed");
    Spi::run("INSERT INTO v112_sr_da_s(kind, v) VALUES ('a',1),('b',2),('a',3)").expect("s seed");
    let q = "SELECT d.label, SUM(s.v) AS s FROM v112_sr_da_s s \
             JOIN v112_sr_da_d d ON d.\"clé\" = s.kind GROUP BY d.label";
    v112_sr_imv("v112_sr_da_v", q);
    Spi::run("INSERT INTO v112_sr_da_s(kind, v) VALUES ('a',10),('b',20)").expect("insert");
    assert_imv_correct("v112_sr_da_v", q);
    Spi::run("UPDATE v112_sr_da_s SET v = v + 100 WHERE id = 1").expect("update");
    assert_imv_correct("v112_sr_da_v", q);
    Spi::run("DELETE FROM v112_sr_da_s WHERE id = 3").expect("delete");
    assert_imv_correct("v112_sr_da_v", q);
}

#[pg_test]
fn v112_sr_length_changing_literal_in_join_condition_keeps_null_group_correct() {
    Spi::run(
        "CREATE TABLE v112_sr_db_s(id SERIAL PRIMARY KEY, b_id INT, xk INT, tag TEXT, v INT)",
    )
    .expect("s");
    Spi::run("CREATE TABLE v112_sr_db_b(id INT PRIMARY KEY, k INT)").expect("b");
    Spi::run("CREATE TABLE v112_sr_db_xb(k INT PRIMARY KEY)").expect("xb");
    Spi::run("INSERT INTO v112_sr_db_b VALUES (1,NULL),(2,5),(3,NULL)").expect("b seed");
    Spi::run("INSERT INTO v112_sr_db_xb VALUES (1),(2)").expect("xb seed");
    Spi::run("INSERT INTO v112_sr_db_s(b_id, xk, tag, v) VALUES (1,1,'t',1),(2,1,'t',2),(3,2,'t',4)")
        .expect("s seed");
    let q = "SELECT b.k, SUM(s.v) AS s FROM v112_sr_db_s s \
             JOIN v112_sr_db_b b ON b.id = s.b_id \
             JOIN v112_sr_db_xb xb ON s.tag <> 'ŉ' AND xb.k = s.xk GROUP BY b.k";
    v112_sr_imv("v112_sr_db_v", q);
    Spi::run("INSERT INTO v112_sr_db_s(b_id, xk, tag, v) VALUES (1,1,'t',10),(2,2,'t',20)")
        .expect("insert");
    assert_imv_correct("v112_sr_db_v", q);
    Spi::run("UPDATE v112_sr_db_s SET v = v + 100 WHERE id = 1").expect("update");
    assert_imv_correct("v112_sr_db_v", q);
    Spi::run("DELETE FROM v112_sr_db_s WHERE id = 3").expect("delete");
    assert_imv_correct("v112_sr_db_v", q);
}

#[pg_test]
fn v112_sr_long_multibyte_source_name_stays_correct() {
    let table = format!("\"v112_sr_{}\"", "é".repeat(26));
    v112_sr_seeded_table(&table, "Été");
    let q = v112_sr_aggregate_over_literal(&table, "'Été'");
    v112_sr_imv("v112_sr_long_agg", &q);
    v112_sr_assert_correct_through_dml(&table, "Été", &[("v112_sr_long_agg", &q)]);
}

#[pg_test]
fn v112_sr_deferred_multibyte_literal_stays_correct() {
    v112_sr_seeded_table("v112_sr_df", "Été");
    let q = v112_sr_aggregate_over_literal("v112_sr_df", "'Été'");
    assert_eq!(
        crate::create_reflex_ivm("v112_sr_df_agg", &q, None, None, Some("DEFERRED"), None),
        "CREATE REFLEX INCREMENTAL VIEW"
    );
    Spi::run("INSERT INTO v112_sr_df(season, v) VALUES ('Été',10),('y',20)").expect("insert");
    Spi::run("SELECT reflex_flush_deferred('v112_sr_df')").expect("flush");
    assert_imv_correct("v112_sr_df_agg", &q);
}

#[pg_test]
fn v112_sr_non_public_schema_multibyte_literal_stays_correct() {
    Spi::run("CREATE SCHEMA v112_sr_np").expect("schema");
    v112_sr_seeded_table("v112_sr_np.prod", "Été");
    let q = v112_sr_aggregate_over_literal("v112_sr_np.prod", "'Été'");
    v112_sr_imv("v112_sr_np_agg", &q);
    v112_sr_assert_correct_through_dml("v112_sr_np.prod", "Été", &[("v112_sr_np_agg", &q)]);
}

#[pg_test]
fn v112_sr_partition_reconcile_with_multibyte_key_stays_correct() {
    Spi::run(
        "CREATE TABLE v112_sr_pp(season TEXT NOT NULL, id INT NOT NULL, v INT) \
         PARTITION BY LIST (season)",
    )
    .expect("source");
    Spi::run("CREATE TABLE v112_sr_pp_e PARTITION OF v112_sr_pp FOR VALUES IN ('Été')").expect("e");
    Spi::run("CREATE TABLE v112_sr_pp_h PARTITION OF v112_sr_pp FOR VALUES IN ('Hiver')")
        .expect("h");
    Spi::run(
        "INSERT INTO v112_sr_pp SELECT s, g, g FROM generate_series(1,20) g \
         CROSS JOIN (VALUES ('Été'),('Hiver')) x(s)",
    )
    .expect("seed");
    let q = "SELECT season, id, v FROM v112_sr_pp";
    let created = Spi::get_one::<String>(
        "SELECT create_reflex_ivm('v112_sr_pp_v', 'SELECT season, id, v FROM v112_sr_pp', \
         'season, id', NULL, 'IMMEDIATE', NULL, ARRAY['season'])",
    )
    .expect("create")
    .expect("result");
    assert_eq!(created, "CREATE REFLEX INCREMENTAL VIEW");
    Spi::run("UPDATE v112_sr_pp_v SET v = v + 1000 WHERE season = 'Été' AND id = 7")
        .expect("drift update");
    Spi::run("DELETE FROM v112_sr_pp_v WHERE season = 'Été' AND id = 8").expect("drift delete");
    Spi::run("INSERT INTO v112_sr_pp_v VALUES ('Été', 999, 1)").expect("drift insert");
    let reconciled =
        Spi::get_one::<String>("SELECT reflex_reconcile_partition('v112_sr_pp_v', 'Été')")
            .expect("reconcile")
            .expect("result");
    assert!(reconciled.starts_with("RECONCILED"), "{reconciled}");
    assert_imv_correct("v112_sr_pp_v", q);
}

fn v112_sr_assert_self_join_correct(tag: &str, table_ref: &str) {
    let table = format!("v112_sr_sj{tag}.t");
    Spi::run(&format!("CREATE SCHEMA v112_sr_sj{tag}")).expect("schema");
    v112_sr_seeded_table(&table, "e");
    let q = format!(
        "SELECT a.season, COUNT(*) AS c, SUM(b.v) AS s FROM {table_ref} a \
         JOIN {table_ref} b ON a.season = b.season GROUP BY a.season"
    );
    let imv = format!("v112_sr_sj{tag}_v");
    v112_sr_imv(&imv, &q);
    v112_sr_assert_correct_through_dml(&table, "e", &[(&imv, &q)]);
}

#[pg_test]
fn v112_sr_self_join_unquoted_stays_correct() {
    v112_sr_assert_self_join_correct("u", "v112_sr_sju.t");
}

#[pg_test]
fn v112_sr_self_join_fully_quoted_stays_correct() {
    v112_sr_assert_self_join_correct("q", "\"v112_sr_sjq\".\"t\"");
}

#[pg_test]
fn v112_sr_self_join_quoted_schema_stays_correct() {
    v112_sr_assert_self_join_correct("m", "\"v112_sr_sjm\".t");
}

#[pg_test]
fn v112_sr_self_join_quoted_table_stays_correct() {
    v112_sr_assert_self_join_correct("n", "v112_sr_sjn.\"t\"");
}

fn v112_sr_outer_join_fixture(tag: &str) -> (String, String) {
    let dim = format!("v112_sr_oj{tag}.dim");
    let fact = format!("v112_sr_oj{tag}_fact");
    Spi::run(&format!("CREATE SCHEMA v112_sr_oj{tag}")).expect("schema");
    Spi::run(&format!("CREATE TABLE {dim}(kind TEXT PRIMARY KEY, label TEXT)")).expect("dim");
    Spi::run(&format!("CREATE TABLE {fact}(id SERIAL PRIMARY KEY, kind TEXT, v INT)"))
        .expect("fact");
    Spi::run(&format!("INSERT INTO {dim} VALUES ('a','A'),('b','B'),('u','U')")).expect("dim seed");
    Spi::run(&format!(
        "INSERT INTO {fact}(kind, v) VALUES ('a',1),('b',2),('c',3),('a',4)"
    ))
    .expect("fact seed");
    (dim, fact)
}

fn v112_sr_assert_correct_through_dim_dml(dim: &str, imv: &str, q: &str) {
    assert_imv_correct(imv, q);
    Spi::run(&format!("INSERT INTO {dim} VALUES ('c','C'),('w','W')")).expect("dim insert");
    assert_imv_correct(imv, q);
    Spi::run(&format!("UPDATE {dim} SET label = 'A2' WHERE kind = 'a'")).expect("dim update");
    assert_imv_correct(imv, q);
    Spi::run(&format!("DELETE FROM {dim} WHERE kind = 'b'")).expect("dim delete");
    assert_imv_correct(imv, q);
}

fn v112_sr_assert_correct_through_fact_dml(fact: &str, imv: &str, q: &str) {
    Spi::run(&format!("INSERT INTO {fact}(kind, v) VALUES ('a',10),('z',20)")).expect("fact insert");
    assert_imv_correct(imv, q);
    Spi::run(&format!("UPDATE {fact} SET v = v + 100 WHERE id = 1")).expect("fact update");
    assert_imv_correct(imv, q);
    Spi::run(&format!("DELETE FROM {fact} WHERE id = 3")).expect("fact delete");
    assert_imv_correct(imv, q);
}

fn v112_sr_assert_correct_through_outer_join_dml(dim: &str, fact: &str, imv: &str, q: &str) {
    v112_sr_assert_correct_through_dim_dml(dim, imv, q);
    v112_sr_assert_correct_through_fact_dml(fact, imv, q);
}

fn v112_sr_assert_left_join_secondary_correct(tag: &str, dim_ref: &str) {
    let (dim, fact) = v112_sr_outer_join_fixture(tag);
    let q = format!(
        "SELECT d.label, COUNT(*) AS c, SUM(f.v) AS s FROM {fact} f \
         LEFT JOIN {dim_ref} d ON d.kind = f.kind GROUP BY d.label"
    );
    let imv = format!("v112_sr_oj{tag}_v");
    v112_sr_imv(&imv, &q);
    v112_sr_assert_correct_through_outer_join_dml(&dim, &fact, &imv, &q);
}

#[pg_test]
fn v112_sr_left_join_secondary_unquoted_stays_correct() {
    v112_sr_assert_left_join_secondary_correct("u", "v112_sr_oju.dim");
}

#[pg_test]
fn v112_sr_left_join_secondary_fully_quoted_stays_correct() {
    v112_sr_assert_left_join_secondary_correct("q", "\"v112_sr_ojq\".\"dim\"");
}

#[pg_test]
fn v112_sr_left_join_secondary_quoted_schema_stays_correct() {
    v112_sr_assert_left_join_secondary_correct("m", "\"v112_sr_ojm\".dim");
}

#[pg_test]
fn v112_sr_left_join_secondary_quoted_table_stays_correct() {
    v112_sr_assert_left_join_secondary_correct("n", "v112_sr_ojn.\"dim\"");
}

#[pg_test]
fn v112_sr_right_join_with_mixed_spelling_stays_correct() {
    let (dim, fact) = v112_sr_outer_join_fixture("r");
    let q = format!(
        "SELECT d.label, COUNT(*) AS c, SUM(f.v) AS s FROM {fact} f \
         RIGHT JOIN \"v112_sr_ojr\".dim d ON d.kind = f.kind GROUP BY d.label"
    );
    v112_sr_imv("v112_sr_ojr_v", &q);
    v112_sr_assert_correct_through_dim_dml(&dim, "v112_sr_ojr_v", &q);
}

#[pg_test]
fn v112_sr_full_join_with_mixed_spelling_stays_correct() {
    let (dim, fact) = v112_sr_outer_join_fixture("f");
    let q = format!(
        "SELECT d.label, COUNT(*) AS c, SUM(f.v) AS s FROM {fact} f \
         FULL JOIN v112_sr_ojf.\"dim\" d ON d.kind = f.kind GROUP BY d.label"
    );
    v112_sr_imv("v112_sr_ojf_v", &q);
    v112_sr_assert_correct_through_outer_join_dml(&dim, &fact, "v112_sr_ojf_v", &q);
}

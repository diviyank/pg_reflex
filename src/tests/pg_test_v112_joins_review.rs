// ---------------------------------------------------------------------------
// NATURAL and USING joins, and a comment inside the join keyword: aggregates
// ---------------------------------------------------------------------------

macro_rules! v112_jr_join_spelling_cells {
    ($deferred:expr; $($name:ident => $select:expr,)+) => {
        v112_j_cells! { V112_J_STAR, V112JNaming::Public, $deferred; $($name => $select,)+ }
    };
}

v112_jr_join_spelling_cells! { false;
    v112_jr_nat_inner => "SELECT d.label, COUNT(*) AS c, SUM(f.v) AS s FROM {f} f NATURAL JOIN {d} d GROUP BY d.label",
    v112_jr_nat_inner_keyword => "SELECT d.label, COUNT(*) AS c, SUM(f.v) AS s FROM {f} f NATURAL INNER JOIN {d} d GROUP BY d.label",
    v112_jr_nat_inner_by_join_column => "SELECT kind, COUNT(*) AS c, SUM(f.v) AS s FROM {f} f NATURAL JOIN {d} d GROUP BY kind",
    v112_jr_nat_left => "SELECT d.label, COUNT(*) AS c, SUM(f.v) AS s FROM {d} d NATURAL LEFT JOIN {f} f GROUP BY d.label",
    v112_jr_nat_right => "SELECT d.label, COUNT(*) AS c, SUM(f.v) AS s FROM {f} f NATURAL RIGHT JOIN {d} d GROUP BY d.label",
    v112_jr_nat_full => "SELECT d.label, COUNT(*) AS c, SUM(f.v) AS s FROM {f} f NATURAL FULL JOIN {d} d GROUP BY d.label",
    v112_jr_nat_inner_pt => "SELECT f.id, f.v, d.label FROM {f} f NATURAL JOIN {d} d",
    v112_jr_nat_right_pt => "SELECT f.id, f.v, d.label FROM {f} f NATURAL RIGHT JOIN {d} d",
    v112_jr_using_inner => "SELECT d.label, COUNT(*) AS c, SUM(f.v) AS s FROM {f} f JOIN {d} d USING (kind) GROUP BY d.label",
    v112_jr_using_inner_by_join_column => "SELECT kind, COUNT(*) AS c, SUM(f.v) AS s FROM {f} f JOIN {d} d USING (kind) GROUP BY kind",
    v112_jr_using_left => "SELECT d.label, COUNT(*) AS c, SUM(f.v) AS s FROM {d} d LEFT JOIN {f} f USING (kind) GROUP BY d.label",
    v112_jr_using_right => "SELECT d.label, COUNT(*) AS c, SUM(f.v) AS s FROM {f} f RIGHT JOIN {d} d USING (kind) GROUP BY d.label",
    v112_jr_using_full => "SELECT d.label, COUNT(*) AS c, SUM(f.v) AS s FROM {f} f FULL JOIN {d} d USING (kind) GROUP BY d.label",
    v112_jr_using_inner_pt => "SELECT f.id, f.v, d.label FROM {f} f JOIN {d} d USING (kind)",
    v112_jr_comment_right => "SELECT d.label, COUNT(*) AS c, SUM(f.v) AS s FROM {f} f RIGHT /* c */ JOIN {d} d ON d.kind = f.kind GROUP BY d.label",
    v112_jr_comment_right_outer => "SELECT d.label, COUNT(*) AS c, SUM(f.v) AS s FROM {f} f RIGHT /* c */ OUTER /* c */ JOIN {d} d ON d.kind = f.kind GROUP BY d.label",
    v112_jr_comment_left => "SELECT d.label, COUNT(*) AS c, SUM(f.v) AS s FROM {d} d LEFT /* c */ JOIN {f} f ON d.kind = f.kind GROUP BY d.label",
    v112_jr_line_comment_left => "SELECT d.label, COUNT(*) AS c, SUM(f.v) AS s FROM {d} d LEFT -- c\n JOIN {f} f ON d.kind = f.kind GROUP BY d.label",
    v112_jr_comment_full => "SELECT d.label, COUNT(*) AS c, SUM(f.v) AS s FROM {f} f FULL /* c */ JOIN {d} d ON d.kind = f.kind GROUP BY d.label",
    v112_jr_comment_right_pt => "SELECT f.id, f.v, d.kind, d.label FROM {f} f RIGHT /* c */ JOIN {d} d ON d.kind = f.kind",
    v112_jr_comment_full_pt => "SELECT f.id, f.v, d.kind, d.label FROM {f} f FULL /* c */ JOIN {d} d ON d.kind = f.kind",
    v112_jr_literal_naming_a_right_join => "SELECT d.label, COUNT(*) AS c, SUM(f.v) AS s FROM {f} f JOIN {d} d ON d.kind = f.kind WHERE d.label <> 'right join' GROUP BY d.label",
}

v112_jr_join_spelling_cells! { true;
    v112_jr_nat_inner_deferred => "SELECT d.label, COUNT(*) AS c, SUM(f.v) AS s FROM {f} f NATURAL JOIN {d} d GROUP BY d.label",
    v112_jr_nat_left_deferred => "SELECT d.label, COUNT(*) AS c, SUM(f.v) AS s FROM {d} d NATURAL LEFT JOIN {f} f GROUP BY d.label",
    v112_jr_nat_right_deferred => "SELECT d.label, COUNT(*) AS c, SUM(f.v) AS s FROM {f} f NATURAL RIGHT JOIN {d} d GROUP BY d.label",
    v112_jr_nat_full_deferred => "SELECT d.label, COUNT(*) AS c, SUM(f.v) AS s FROM {f} f NATURAL FULL JOIN {d} d GROUP BY d.label",
    v112_jr_nat_inner_pt_deferred => "SELECT f.id, f.v, d.label FROM {f} f NATURAL JOIN {d} d",
    v112_jr_using_inner_deferred => "SELECT d.label, COUNT(*) AS c, SUM(f.v) AS s FROM {f} f JOIN {d} d USING (kind) GROUP BY d.label",
    v112_jr_using_left_deferred => "SELECT d.label, COUNT(*) AS c, SUM(f.v) AS s FROM {d} d LEFT JOIN {f} f USING (kind) GROUP BY d.label",
    v112_jr_using_right_deferred => "SELECT d.label, COUNT(*) AS c, SUM(f.v) AS s FROM {f} f RIGHT JOIN {d} d USING (kind) GROUP BY d.label",
    v112_jr_using_full_deferred => "SELECT d.label, COUNT(*) AS c, SUM(f.v) AS s FROM {f} f FULL JOIN {d} d USING (kind) GROUP BY d.label",
    v112_jr_comment_right_deferred => "SELECT d.label, COUNT(*) AS c, SUM(f.v) AS s FROM {f} f RIGHT /* c */ JOIN {d} d ON d.kind = f.kind GROUP BY d.label",
    v112_jr_comment_left_deferred => "SELECT d.label, COUNT(*) AS c, SUM(f.v) AS s FROM {d} d LEFT /* c */ JOIN {f} f ON d.kind = f.kind GROUP BY d.label",
}

v112_j_cells! { V112_J_DERIVED, V112JNaming::Public, false;
    v112_jr_comment_left_derived => "SELECT s.g, COUNT(*) AS c, SUM(d.w) AS w, SUM(s.v) AS v FROM {f} s LEFT /* c */ JOIN (SELECT g, w FROM {d} WHERE w > 0) d ON d.g = s.g GROUP BY s.g",
    v112_jr_comment_left_derived_pt => "SELECT s.id, s.g, s.v, d.w FROM {f} s LEFT /* c */ JOIN (SELECT g, w FROM {d} WHERE w > 0) d ON d.g = s.g",
}

// ---------------------------------------------------------------------------
// The same join spellings in a passthrough keyed by the join column
// ---------------------------------------------------------------------------

/// `{f}(kind PRIMARY KEY, v)` joined to `{d}(kind PRIMARY KEY, label)`, the IMV
/// keyed by `kind`: INSERT, value UPDATE, join-column UPDATE and DELETE on each
/// table, the IMV compared with its query after each.
fn v112_jr_keyed_cell(tag: &str, select_template: &str, deferred: bool) -> String {
    let fact = format!("v112_jr_{tag}_fact");
    let dim = format!("v112_jr_{tag}_dim");
    Spi::run(&format!("CREATE TABLE {fact}(kind TEXT PRIMARY KEY, v INT)")).expect("fact");
    Spi::run(&format!("CREATE TABLE {dim}(kind TEXT PRIMARY KEY, label TEXT)")).expect("dim");
    Spi::run(&format!("INSERT INTO {fact} VALUES ('a',1),('b',2),('c',3),('z',4)")).expect("seed");
    Spi::run(&format!(
        "INSERT INTO {dim} VALUES ('a','A'),('b','B'),('c','C'),('u','U')"
    ))
    .expect("seed");
    let imv = format!("v112_jr_{tag}");
    let query = select_template.replace("{f}", &fact).replace("{d}", &dim);
    let mode = if deferred { "DEFERRED" } else { "IMMEDIATE" };
    let created = v112_j_try(&format!(
        "SELECT create_reflex_ivm('{imv}', $v112sel${query}$v112sel$, 'kind', 'UNLOGGED', '{mode}')"
    ));
    if created != "CREATE REFLEX INCREMENTAL VIEW" {
        return format!("NOT-CREATED {created}");
    }
    let mut outcomes = vec![("create".to_string(), v112_j_oracle(&imv, &query))];
    let steps = [
        ("f_ins", format!("INSERT INTO {fact} VALUES ('u',7),('y',8)")),
        ("f_upd", format!("UPDATE {fact} SET v = v + 10 WHERE kind = 'a'")),
        ("f_move", format!("UPDATE {fact} SET kind = 'q' WHERE kind = 'c'")),
        ("f_del", format!("DELETE FROM {fact} WHERE kind = 'b'")),
        ("d_ins", format!("INSERT INTO {dim} VALUES ('z','Z'),('w','W')")),
        ("d_upd", format!("UPDATE {dim} SET label = 'AA' WHERE kind = 'a'")),
        ("d_move", format!("UPDATE {dim} SET kind = 'q' WHERE kind = 'w'")),
        ("d_del", format!("DELETE FROM {dim} WHERE kind IN ('a', 'u')")),
        ("f_back", format!("INSERT INTO {fact} VALUES ('b',20)")),
    ];
    for (label, dml) in steps {
        let ran = v112_j_try_dml(&dml);
        if ran != "OK" {
            outcomes.push((label.to_string(), ran));
            break;
        }
        if deferred {
            let flushed = v112_j_flush_every_source(&imv);
            if flushed.contains("RAISED") || flushed.contains("ERROR") {
                outcomes.push((label.to_string(), flushed));
                break;
            }
        }
        outcomes.push((label.to_string(), v112_j_oracle(&imv, &query)));
    }
    if outcomes.iter().all(|(_, outcome)| outcome == "0") {
        return "ok".to_string();
    }
    outcomes
        .iter()
        .map(|(label, outcome)| format!("{label}={outcome}"))
        .collect::<Vec<_>>()
        .join(" ")
}

macro_rules! v112_jr_keyed_cells {
    ($deferred:expr; $($name:ident => $select:expr,)+) => {
        $(
            #[pg_test]
            fn $name() {
                let tag = stringify!($name).trim_start_matches("v112_jr_");
                assert_eq!(v112_jr_keyed_cell(tag, $select, $deferred), "ok", "{}", $select);
            }
        )+
    };
}

v112_jr_keyed_cells! { false;
    v112_jr_kp_right_block_comment => "SELECT d.kind, d.label, f.v FROM {f} f RIGHT /* c */ JOIN {d} d ON d.kind = f.kind",
    v112_jr_kp_right_line_comment => "SELECT d.kind, d.label, f.v FROM {f} f RIGHT -- c\n JOIN {d} d ON d.kind = f.kind",
    v112_jr_kp_right_comment_outer => "SELECT d.kind, d.label, f.v FROM {f} f RIGHT /* c */ OUTER JOIN {d} d ON d.kind = f.kind",
    v112_jr_kp_right_outer_comment => "SELECT d.kind, d.label, f.v FROM {f} f RIGHT OUTER /* c */ JOIN {d} d ON d.kind = f.kind",
    v112_jr_kp_left_block_comment => "SELECT d.kind, d.label, f.v FROM {d} d LEFT /* c */ JOIN {f} f ON d.kind = f.kind",
    v112_jr_kp_left_line_comment => "SELECT d.kind, d.label, f.v FROM {d} d LEFT -- c\n JOIN {f} f ON d.kind = f.kind",
    v112_jr_kp_left_comment_outer => "SELECT d.kind, d.label, f.v FROM {d} d LEFT /* c */ OUTER JOIN {f} f ON d.kind = f.kind",
    v112_jr_kp_left_outer_comment => "SELECT d.kind, d.label, f.v FROM {d} d LEFT OUTER /* c */ JOIN {f} f ON d.kind = f.kind",
    v112_jr_kp_left_comment_before_table => "SELECT d.kind, d.label, f.v FROM {d} d LEFT JOIN /* c */ {f} f ON d.kind = f.kind",
    v112_jr_kp_left_plain => "SELECT d.kind, d.label, f.v FROM {d} d LEFT JOIN {f} f ON d.kind = f.kind",
    v112_jr_kp_right_plain => "SELECT d.kind, d.label, f.v FROM {f} f RIGHT JOIN {d} d ON d.kind = f.kind",
    v112_jr_kp_inner_plain => "SELECT d.kind, d.label, f.v FROM {f} f JOIN {d} d ON d.kind = f.kind",
    v112_jr_kp_nat_inner => "SELECT kind, d.label, f.v FROM {f} f NATURAL JOIN {d} d",
    v112_jr_kp_nat_left => "SELECT kind, d.label, f.v FROM {d} d NATURAL LEFT JOIN {f} f",
    v112_jr_kp_nat_right => "SELECT kind, d.label, f.v FROM {f} f NATURAL RIGHT JOIN {d} d",
    v112_jr_kp_nat_full => "SELECT kind, d.label, f.v FROM {f} f NATURAL FULL JOIN {d} d",
    v112_jr_kp_using_inner => "SELECT kind, d.label, f.v FROM {f} f JOIN {d} d USING (kind)",
    v112_jr_kp_using_left => "SELECT kind, d.label, f.v FROM {d} d LEFT JOIN {f} f USING (kind)",
    v112_jr_kp_using_right => "SELECT kind, d.label, f.v FROM {f} f RIGHT JOIN {d} d USING (kind)",
    v112_jr_kp_using_full => "SELECT kind, d.label, f.v FROM {f} f FULL JOIN {d} d USING (kind)",
}

v112_jr_keyed_cells! { true;
    v112_jr_kp_right_block_comment_deferred => "SELECT d.kind, d.label, f.v FROM {f} f RIGHT /* c */ JOIN {d} d ON d.kind = f.kind",
    v112_jr_kp_right_line_comment_deferred => "SELECT d.kind, d.label, f.v FROM {f} f RIGHT -- c\n JOIN {d} d ON d.kind = f.kind",
    v112_jr_kp_left_block_comment_deferred => "SELECT d.kind, d.label, f.v FROM {d} d LEFT /* c */ JOIN {f} f ON d.kind = f.kind",
    v112_jr_kp_left_comment_outer_deferred => "SELECT d.kind, d.label, f.v FROM {d} d LEFT /* c */ OUTER JOIN {f} f ON d.kind = f.kind",
    v112_jr_kp_left_plain_deferred => "SELECT d.kind, d.label, f.v FROM {d} d LEFT JOIN {f} f ON d.kind = f.kind",
    v112_jr_kp_nat_inner_deferred => "SELECT kind, d.label, f.v FROM {f} f NATURAL JOIN {d} d",
    v112_jr_kp_nat_left_deferred => "SELECT kind, d.label, f.v FROM {d} d NATURAL LEFT JOIN {f} f",
    v112_jr_kp_nat_right_deferred => "SELECT kind, d.label, f.v FROM {f} f NATURAL RIGHT JOIN {d} d",
    v112_jr_kp_nat_full_deferred => "SELECT kind, d.label, f.v FROM {f} f NATURAL FULL JOIN {d} d",
    v112_jr_kp_using_inner_deferred => "SELECT kind, d.label, f.v FROM {f} f JOIN {d} d USING (kind)",
    v112_jr_kp_using_left_deferred => "SELECT kind, d.label, f.v FROM {d} d LEFT JOIN {f} f USING (kind)",
    v112_jr_kp_using_right_deferred => "SELECT kind, d.label, f.v FROM {f} f RIGHT JOIN {d} d USING (kind)",
    v112_jr_kp_using_full_deferred => "SELECT kind, d.label, f.v FROM {f} f FULL JOIN {d} d USING (kind)",
}

// ---------------------------------------------------------------------------
// One table read by several IMVs under different spellings
// ---------------------------------------------------------------------------

/// Creates one IMV per `(suffix, query)` over a `season TEXT, v INT` table, in
/// order, then writes the table, comparing every IMV with its query after each
/// statement. `{t}` is the table's bare name.
fn v112_jr_spellings_cell(tag: &str, queries: &[(&str, &str)], deferred: bool) -> String {
    let table = format!("v112_jr_{tag}_t");
    v112_j_season_table(&table);
    let mode = if deferred { "DEFERRED" } else { "IMMEDIATE" };
    let imvs: Vec<(String, String)> = queries
        .iter()
        .map(|(suffix, query)| {
            (
                format!("v112_jr_{tag}_{suffix}"),
                query.replace("{t}", &table),
            )
        })
        .collect();
    for (imv, query) in &imvs {
        let created = v112_j_try(&format!(
            "SELECT create_reflex_ivm('{imv}', $v112sel${query}$v112sel$, NULL, 'UNLOGGED', '{mode}')"
        ));
        if created != "CREATE REFLEX INCREMENTAL VIEW" {
            return format!("NOT-CREATED {imv} {created}");
        }
    }
    let steps = [
        ("ins", format!("INSERT INTO {table}(season, v) VALUES ('e',10),('y',20),('n',30)")),
        ("upd", format!("UPDATE {table} SET v = v + 100 WHERE id = 1")),
        ("move", format!("UPDATE {table} SET season = 'y' WHERE id = 3")),
        ("del", format!("DELETE FROM {table} WHERE id = 2")),
    ];
    let mut wrong: Vec<String> = Vec::new();
    for (label, dml) in steps {
        let ran = v112_j_try_dml(&dml);
        if ran != "OK" {
            wrong.push(format!("{label}={ran}"));
            break;
        }
        for (imv, query) in &imvs {
            if deferred {
                let flushed = v112_j_flush_every_source(imv);
                if flushed.contains("RAISED") || flushed.contains("ERROR") {
                    wrong.push(format!("{label}@{imv}={flushed}"));
                }
            }
            let outcome = v112_j_oracle(imv, query);
            if outcome != "0" {
                wrong.push(format!("{label}@{imv}={outcome}"));
            }
        }
    }
    if wrong.is_empty() {
        "ok".to_string()
    } else {
        wrong.join(" ")
    }
}

const V112_JR_AGG_BARE: &str = "SELECT season, SUM(v) AS s FROM {t} GROUP BY season";
const V112_JR_AGG_QUOTED: &str = "SELECT season, COUNT(*) AS c FROM \"{t}\" GROUP BY season";
const V112_JR_PT_BARE: &str = "SELECT id, season, v FROM {t} WHERE v > 0";
const V112_JR_PT_QUOTED: &str = "SELECT id, season FROM \"{t}\" WHERE v > 1";

macro_rules! v112_jr_spellings_cells {
    ($($name:ident => ($queries:expr, $deferred:expr),)+) => {
        $(
            #[pg_test]
            fn $name() {
                let tag = stringify!($name).trim_start_matches("v112_jr_");
                assert_eq!(v112_jr_spellings_cell(tag, &$queries, $deferred), "ok");
            }
        )+
    };
}

v112_jr_spellings_cells! {
    v112_jr_sp_bare_then_quoted => ([("a", V112_JR_AGG_BARE), ("b", V112_JR_AGG_QUOTED)], false),
    v112_jr_sp_quoted_then_bare => ([("a", V112_JR_AGG_QUOTED), ("b", V112_JR_AGG_BARE)], false),
    v112_jr_sp_bare_then_quoted_pt => ([("a", V112_JR_PT_BARE), ("b", V112_JR_PT_QUOTED)], false),
    v112_jr_sp_quoted_then_bare_pt => ([("a", V112_JR_PT_QUOTED), ("b", V112_JR_PT_BARE)], false),
    v112_jr_sp_bare_then_quoted_deferred => ([("a", V112_JR_AGG_BARE), ("b", V112_JR_AGG_QUOTED)], true),
    v112_jr_sp_quoted_then_bare_deferred => ([("a", V112_JR_AGG_QUOTED), ("b", V112_JR_AGG_BARE)], true),
    v112_jr_sp_bare_then_quoted_pt_deferred => ([("a", V112_JR_PT_BARE), ("b", V112_JR_PT_QUOTED)], true),
    v112_jr_sp_immediate_then_quoted_three => ([("a", V112_JR_AGG_BARE), ("b", V112_JR_PT_QUOTED), ("c", "SELECT season, MAX(v) AS m FROM public.\"{t}\" GROUP BY season")], false),
    v112_jr_sp_bare_then_public => ([("a", V112_JR_AGG_BARE), ("b", "SELECT season, COUNT(*) AS c FROM public.{t} GROUP BY season")], false),
    v112_jr_sp_bare_then_bare => ([("a", V112_JR_AGG_BARE), ("b", "SELECT season, COUNT(*) AS c FROM {t} GROUP BY season")], false),
    v112_jr_sp_quoted_only => ([("a", V112_JR_AGG_QUOTED), ("b", V112_JR_PT_QUOTED)], false),
    v112_jr_sp_union_all_bare_then_quoted => ([("a", "SELECT season, v FROM {t} UNION ALL SELECT season, id FROM \"{t}\"")], false),
    v112_jr_sp_union_all_quoted_then_bare => ([("a", "SELECT season, v FROM \"{t}\" UNION ALL SELECT season, id FROM {t}")], false),
    v112_jr_sp_union_all_aggregate => ([("a", "SELECT u.season, SUM(u.v) AS s FROM (SELECT season, v FROM {t} UNION ALL SELECT season, id AS v FROM \"{t}\") u GROUP BY u.season")], false),
}

#[pg_test]
fn v112_jr_sp_schema_bare_then_quoted() {
    Spi::run("CREATE SCHEMA v112_jr_sps").expect("schema");
    v112_j_season_table("v112_jr_sps.t");
    let bare = "SELECT season, SUM(v) AS s FROM v112_jr_sps.t GROUP BY season";
    let quoted = "SELECT season, COUNT(*) AS c FROM \"v112_jr_sps\".\"t\" GROUP BY season";
    for (imv, query) in [("v112_jr_sps_a", bare), ("v112_jr_sps_b", quoted)] {
        assert_eq!(
            crate::create_reflex_ivm(imv, query, None, None, None, None),
            "CREATE REFLEX INCREMENTAL VIEW"
        );
    }
    Spi::run("INSERT INTO v112_jr_sps.t(season, v) VALUES ('e',10),('n',30)").expect("insert");
    Spi::run("UPDATE v112_jr_sps.t SET season = 'y' WHERE id = 3").expect("update");
    assert_imv_correct("v112_jr_sps_a", bare);
    assert_imv_correct("v112_jr_sps_b", quoted);
}

/// Dropping the IMV that installed the source triggers leaves the other one,
/// written with the other spelling, maintained.
#[pg_test]
fn v112_jr_sp_drop_first_keeps_second_maintained() {
    v112_j_season_table("v112_jr_spd_t");
    let bare = "SELECT season, SUM(v) AS s FROM v112_jr_spd_t GROUP BY season";
    let quoted = "SELECT season, COUNT(*) AS c FROM \"v112_jr_spd_t\" GROUP BY season";
    for (imv, query) in [("v112_jr_spd_a", bare), ("v112_jr_spd_b", quoted)] {
        assert_eq!(
            crate::create_reflex_ivm(imv, query, None, None, None, None),
            "CREATE REFLEX INCREMENTAL VIEW"
        );
    }
    Spi::run("SELECT drop_reflex_ivm('v112_jr_spd_a')").expect("drop");
    Spi::run("INSERT INTO v112_jr_spd_t(season, v) VALUES ('e',10),('n',30)").expect("insert");
    assert_imv_correct("v112_jr_spd_b", quoted);
    Spi::run("DELETE FROM v112_jr_spd_t WHERE id = 1").expect("delete");
    assert_imv_correct("v112_jr_spd_b", quoted);
}

#[pg_test]
fn v112_jr_sp_every_spelling_registers_one_source() {
    v112_j_season_table("v112_jr_spr_t");
    let queries = [
        ("v112_jr_spr_a", "SELECT season, SUM(v) AS s FROM v112_jr_spr_t GROUP BY season"),
        ("v112_jr_spr_b", "SELECT season, SUM(v) AS s FROM \"v112_jr_spr_t\" GROUP BY season"),
    ];
    for (imv, query) in queries {
        assert_eq!(
            crate::create_reflex_ivm(imv, query, None, None, None, None),
            "CREATE REFLEX INCREMENTAL VIEW"
        );
    }
    let spellings = Spi::get_one::<i64>(
        "SELECT COUNT(DISTINCT d) FROM public.__reflex_ivm_reference, unnest(depends_on) d \
         WHERE name IN ('v112_jr_spr_a', 'v112_jr_spr_b')",
    )
    .expect("registry")
    .unwrap_or(0);
    assert_eq!(spellings, 1);
}

// ---------------------------------------------------------------------------
// Several IMVs, several spellings, any creation order
// ---------------------------------------------------------------------------

/// `{p}` is a per-cell prefix, so every cell owns its tables.
struct V112JrShape {
    setup: &'static [&'static str],
    /// `(suffix, query)`, created in order.
    imvs: &'static [(&'static str, &'static str)],
    drop_first: bool,
    writes: &'static [&'static str],
}

fn v112_jr_shape_cell(tag: &str, shape: &V112JrShape, deferred: bool) -> String {
    let prefix = format!("v112_jr_{tag}_");
    let mode = if deferred { "DEFERRED" } else { "IMMEDIATE" };
    for statement in shape.setup {
        Spi::run(&statement.replace("{p}", &prefix)).expect("setup");
    }
    let mut imvs: Vec<(String, String)> = shape
        .imvs
        .iter()
        .map(|(suffix, query)| (format!("{prefix}{suffix}"), query.replace("{p}", &prefix)))
        .collect();
    for (imv, query) in &imvs {
        let created = v112_j_try(&format!(
            "SELECT create_reflex_ivm('{imv}', $v112sel${query}$v112sel$, NULL, 'UNLOGGED', '{mode}')"
        ));
        if created != "CREATE REFLEX INCREMENTAL VIEW" {
            return format!("NOT-CREATED {imv} {created}");
        }
    }
    if shape.drop_first {
        let (first, _) = imvs.remove(0);
        let dropped = v112_j_try(&format!("SELECT drop_reflex_ivm('{first}')"));
        if dropped.contains("RAISED") || dropped.contains("ERROR") {
            return format!("NOT-DROPPED {dropped}");
        }
    }
    let mut wrong: Vec<String> = Vec::new();
    for (step, write) in shape.writes.iter().enumerate() {
        let ran = v112_j_try_dml(&write.replace("{p}", &prefix));
        if ran != "OK" {
            wrong.push(format!("write{step}={ran}"));
            break;
        }
        for (imv, query) in &imvs {
            if deferred {
                let flushed = v112_j_flush_every_source(imv);
                if flushed.contains("RAISED") || flushed.contains("ERROR") {
                    wrong.push(format!("write{step}@{imv}={flushed}"));
                }
            }
            let outcome = v112_j_oracle(imv, query);
            if outcome != "0" {
                wrong.push(format!("write{step}@{imv}={outcome}"));
            }
        }
    }
    if wrong.is_empty() {
        "ok".to_string()
    } else {
        wrong.join(" ")
    }
}

macro_rules! v112_jr_shape_cells {
    ($shape:expr; $($immediate:ident, $deferred:ident;)+) => {
        $(
            #[pg_test]
            fn $immediate() {
                let tag = stringify!($immediate).trim_start_matches("v112_jr_");
                assert_eq!(v112_jr_shape_cell(tag, &$shape, false), "ok");
            }

            #[pg_test]
            fn $deferred() {
                let tag = stringify!($deferred).trim_start_matches("v112_jr_");
                assert_eq!(v112_jr_shape_cell(tag, &$shape, true), "ok");
            }
        )+
    };
}

const V112_JR_SEASON_SETUP: &[&str] = &[
    "CREATE TABLE {p}t(id SERIAL PRIMARY KEY, season TEXT, v INT, w INT)",
    "INSERT INTO {p}t(season, v, w) VALUES ('e',1,10),('y',2,20),('e',3,30)",
];
const V112_JR_SEASON_WRITES: &[&str] = &[
    "INSERT INTO {p}t(season, v, w) VALUES ('e',10,1),('y',20,2),('n',30,3)",
    "UPDATE {p}t SET v = v + 100 WHERE id = 1",
    "UPDATE {p}t SET season = 'y' WHERE id = 3",
    "DELETE FROM {p}t WHERE id = 2",
];
const V112_JR_SUM_BARE: (&str, &str) = ("a", "SELECT season, SUM(v) AS s FROM {p}t GROUP BY season");
const V112_JR_COUNT_QUOTED: (&str, &str) = (
    "b",
    "SELECT season, COUNT(*) AS c, SUM(w) AS s FROM \"{p}t\" GROUP BY season",
);
const V112_JR_ROWS_BARE: (&str, &str) = ("c", "SELECT id, season, v FROM {p}t WHERE v > 0");
const V112_JR_ROWS_QUOTED: (&str, &str) = ("d", "SELECT id, season FROM \"{p}t\" WHERE v > 1");

/// An IMV over `"t"` first, then a LEFT JOIN passthrough whose joined table is
/// written `t`: the second gets the key it would get alone, which is none.
const V112_JR_QUOTED_THEN_LEFT_JOINED: V112JrShape = V112JrShape {
    setup: &[
        "CREATE TABLE {p}t(id SERIAL PRIMARY KEY, season TEXT, v INT, w INT)",
        "INSERT INTO {p}t(season, v, w) VALUES ('e',1,10),('y',2,20),('e',3,30)",
        "CREATE TABLE {p}dim(season TEXT PRIMARY KEY, label TEXT)",
        "INSERT INTO {p}dim VALUES ('e','E'),('y','Y'),('x','X')",
    ],
    imvs: &[
        V112_JR_COUNT_QUOTED,
        ("j", "SELECT d.season, d.label, {p}t.id, {p}t.v FROM {p}dim d LEFT JOIN {p}t {p}t ON d.season = {p}t.season"),
    ],
    drop_first: false,
    writes: &[
        "INSERT INTO {p}dim VALUES ('n','N'),('k','K')",
        "INSERT INTO {p}t(season, v, w) VALUES ('n',5,50),('y',6,60)",
        "UPDATE {p}t SET season = 'k' WHERE id = 1",
        "DELETE FROM {p}t WHERE id = 2",
        "DELETE FROM {p}dim WHERE season = 'x'",
    ],
};

/// A first IMV naming the table under both spellings, then one written bare.
const V112_JR_BOTH_SPELLINGS_THEN_BARE: V112JrShape = V112JrShape {
    setup: V112_JR_SEASON_SETUP,
    imvs: &[
        ("j", "SELECT a.season, COUNT(*) AS c, SUM(b.v) AS s FROM {p}t a JOIN \"{p}t\" b ON a.season = b.season GROUP BY a.season"),
        V112_JR_SUM_BARE,
    ],
    drop_first: false,
    writes: V112_JR_SEASON_WRITES,
};
const V112_JR_BOTH_SPELLINGS_THEN_QUOTED: V112JrShape = V112JrShape {
    setup: V112_JR_SEASON_SETUP,
    imvs: &[
        ("j", "SELECT a.season, COUNT(*) AS c, SUM(b.v) AS s FROM \"{p}t\" a JOIN {p}t b ON a.season = b.season GROUP BY a.season"),
        V112_JR_COUNT_QUOTED,
    ],
    drop_first: false,
    writes: V112_JR_SEASON_WRITES,
};

const V112_JR_COLUMN_SETUP: &[&str] = &[
    "CREATE TABLE {p}t(id SERIAL PRIMARY KEY, season TEXT, v INT, {p}t INT)",
    "INSERT INTO {p}t(season, v, {p}t) VALUES ('e',1,7),('y',2,8)",
];
const V112_JR_COLUMN_WRITES: &[&str] = &[
    "INSERT INTO {p}t(season, v, {p}t) VALUES ('e',10,1),('n',30,3)",
    "UPDATE {p}t SET {p}t = {p}t + 5 WHERE id = 1",
    "DELETE FROM {p}t WHERE id = 2",
];
/// A column named like its own table, the table written quoted or bare.
const V112_JR_OWN_COLUMN_QUOTED_TABLE: V112JrShape = V112JrShape {
    setup: V112_JR_COLUMN_SETUP,
    imvs: &[("a", "SELECT season, SUM({p}t) AS s, COUNT(*) AS c FROM \"{p}t\" GROUP BY season")],
    drop_first: false,
    writes: V112_JR_COLUMN_WRITES,
};
const V112_JR_OWN_COLUMN_BARE_TABLE: V112JrShape = V112JrShape {
    setup: V112_JR_COLUMN_SETUP,
    imvs: &[("a", "SELECT season, SUM({p}t) AS s, COUNT(*) AS c FROM {p}t GROUP BY season")],
    drop_first: false,
    writes: V112_JR_COLUMN_WRITES,
};
const V112_JR_OWN_COLUMN_ROWS: V112JrShape = V112JrShape {
    setup: V112_JR_COLUMN_SETUP,
    imvs: &[("a", "SELECT id, season, {p}t AS x FROM \"{p}t\" WHERE {p}t > 0")],
    drop_first: false,
    writes: V112_JR_COLUMN_WRITES,
};

const V112_JR_OTHER_COLUMN_SETUP: &[&str] = &[
    "CREATE TABLE {p}dim(id SERIAL PRIMARY KEY, season TEXT, v INT)",
    "CREATE TABLE {p}fact(id SERIAL PRIMARY KEY, season TEXT, v INT, {p}dim INT)",
    "INSERT INTO {p}dim(season, v) VALUES ('e',1),('y',2)",
    "INSERT INTO {p}fact(season, v, {p}dim) VALUES ('e',1,1),('y',2,NULL)",
];
const V112_JR_OTHER_COLUMN_WRITES: &[&str] = &[
    "INSERT INTO {p}dim(season, v) VALUES ('n',3)",
    "INSERT INTO {p}fact(season, v, {p}dim) VALUES ('n',3,3)",
    "UPDATE {p}dim SET v = v + 10 WHERE id = 1",
    "UPDATE {p}fact SET {p}dim = 4 WHERE id = 2",
    "DELETE FROM {p}dim WHERE id = 2",
];
/// A column of another table named like the joined source.
const V112_JR_OTHER_COLUMN_QUOTED_TABLE: V112JrShape = V112JrShape {
    setup: V112_JR_OTHER_COLUMN_SETUP,
    imvs: &[("a", "SELECT f.season, COUNT({p}dim) AS c, SUM(a.v) AS s FROM {p}fact f JOIN \"{p}dim\" a ON a.id = f.id GROUP BY f.season")],
    drop_first: false,
    writes: V112_JR_OTHER_COLUMN_WRITES,
};
const V112_JR_OTHER_COLUMN_BARE_TABLE: V112JrShape = V112JrShape {
    setup: V112_JR_OTHER_COLUMN_SETUP,
    imvs: &[("a", "SELECT f.season, COUNT({p}dim) AS c, SUM(a.v) AS s FROM {p}fact f JOIN {p}dim a ON a.id = f.id GROUP BY f.season")],
    drop_first: false,
    writes: V112_JR_OTHER_COLUMN_WRITES,
};
const V112_JR_OTHER_COLUMN_SCHEMA_TABLE: V112JrShape = V112JrShape {
    setup: &[
        "CREATE SCHEMA {p}s",
        "CREATE TABLE {p}s.dim(id SERIAL PRIMARY KEY, season TEXT, v INT)",
        "CREATE TABLE {p}fact(id SERIAL PRIMARY KEY, season TEXT, v INT, dim INT)",
        "INSERT INTO {p}s.dim(season, v) VALUES ('e',1),('y',2)",
        "INSERT INTO {p}fact(season, v, dim) VALUES ('e',1,1),('y',2,NULL)",
    ],
    imvs: &[("a", "SELECT f.season, COUNT(dim) AS c, SUM(a.v) AS s FROM {p}fact f JOIN {p}s.dim a ON a.id = f.id GROUP BY f.season")],
    drop_first: false,
    writes: &[
        "INSERT INTO {p}s.dim(season, v) VALUES ('n',3)",
        "INSERT INTO {p}fact(season, v, dim) VALUES ('n',3,3)",
        "UPDATE {p}s.dim SET v = v + 10 WHERE id = 1",
        "DELETE FROM {p}s.dim WHERE id = 2",
    ],
};

const V112_JR_DROP_BARE_KEEP_QUOTED: V112JrShape = V112JrShape {
    setup: V112_JR_SEASON_SETUP,
    imvs: &[V112_JR_SUM_BARE, V112_JR_COUNT_QUOTED],
    drop_first: true,
    writes: V112_JR_SEASON_WRITES,
};
const V112_JR_DROP_QUOTED_KEEP_BARE: V112JrShape = V112JrShape {
    setup: V112_JR_SEASON_SETUP,
    imvs: &[V112_JR_COUNT_QUOTED, V112_JR_SUM_BARE],
    drop_first: true,
    writes: V112_JR_SEASON_WRITES,
};
const V112_JR_DROP_BARE_ROWS_KEEP_QUOTED: V112JrShape = V112JrShape {
    setup: V112_JR_SEASON_SETUP,
    imvs: &[V112_JR_ROWS_BARE, V112_JR_ROWS_QUOTED],
    drop_first: true,
    writes: V112_JR_SEASON_WRITES,
};
const V112_JR_DROP_QUOTED_ROWS_KEEP_BARE: V112JrShape = V112JrShape {
    setup: V112_JR_SEASON_SETUP,
    imvs: &[V112_JR_ROWS_QUOTED, V112_JR_ROWS_BARE],
    drop_first: true,
    writes: V112_JR_SEASON_WRITES,
};
/// Four spellings of one `public` table, in both orders.
const V112_JR_FOUR_SPELLINGS: V112JrShape = V112JrShape {
    setup: V112_JR_SEASON_SETUP,
    imvs: &[
        V112_JR_SUM_BARE,
        V112_JR_COUNT_QUOTED,
        ("e", "SELECT season, MAX(v) AS m FROM public.{p}t GROUP BY season"),
        ("f", "SELECT id, season, w FROM \"public\".\"{p}t\""),
    ],
    drop_first: false,
    writes: V112_JR_SEASON_WRITES,
};
const V112_JR_FOUR_SPELLINGS_REVERSED: V112JrShape = V112JrShape {
    setup: V112_JR_SEASON_SETUP,
    imvs: &[
        ("f", "SELECT id, season, w FROM \"public\".\"{p}t\""),
        ("e", "SELECT season, MAX(v) AS m FROM public.{p}t GROUP BY season"),
        V112_JR_COUNT_QUOTED,
        V112_JR_SUM_BARE,
    ],
    drop_first: true,
    writes: V112_JR_SEASON_WRITES,
};

v112_jr_shape_cells! { V112_JR_QUOTED_THEN_LEFT_JOINED; v112_jr_n1_left_joined, v112_jr_n1_left_joined_deferred; }
v112_jr_shape_cells! { V112_JR_BOTH_SPELLINGS_THEN_BARE; v112_jr_n2_then_bare, v112_jr_n2_then_bare_deferred; }
v112_jr_shape_cells! { V112_JR_BOTH_SPELLINGS_THEN_QUOTED; v112_jr_n2_then_quoted, v112_jr_n2_then_quoted_deferred; }
v112_jr_shape_cells! { V112_JR_OWN_COLUMN_QUOTED_TABLE; v112_jr_n3_own_quoted, v112_jr_n3_own_quoted_deferred; }
v112_jr_shape_cells! { V112_JR_OWN_COLUMN_BARE_TABLE; v112_jr_n3_own_bare, v112_jr_n3_own_bare_deferred; }
v112_jr_shape_cells! { V112_JR_OWN_COLUMN_ROWS; v112_jr_n3_own_rows, v112_jr_n3_own_rows_deferred; }
v112_jr_shape_cells! { V112_JR_OTHER_COLUMN_QUOTED_TABLE; v112_jr_n3_other_quoted, v112_jr_n3_other_quoted_deferred; }
v112_jr_shape_cells! { V112_JR_OTHER_COLUMN_BARE_TABLE; v112_jr_n3_other_bare, v112_jr_n3_other_bare_deferred; }
v112_jr_shape_cells! { V112_JR_OTHER_COLUMN_SCHEMA_TABLE; v112_jr_n3_other_schema, v112_jr_n3_other_schema_deferred; }
v112_jr_shape_cells! { V112_JR_DROP_BARE_KEEP_QUOTED; v112_jr_drop_bare_keep_quoted, v112_jr_drop_bare_keep_quoted_deferred; }
v112_jr_shape_cells! { V112_JR_DROP_QUOTED_KEEP_BARE; v112_jr_drop_quoted_keep_bare, v112_jr_drop_quoted_keep_bare_deferred; }
v112_jr_shape_cells! { V112_JR_DROP_BARE_ROWS_KEEP_QUOTED; v112_jr_drop_bare_rows, v112_jr_drop_bare_rows_deferred; }
v112_jr_shape_cells! { V112_JR_DROP_QUOTED_ROWS_KEEP_BARE; v112_jr_drop_quoted_rows, v112_jr_drop_quoted_rows_deferred; }
v112_jr_shape_cells! { V112_JR_FOUR_SPELLINGS; v112_jr_four_spellings, v112_jr_four_spellings_deferred; }
v112_jr_shape_cells! { V112_JR_FOUR_SPELLINGS_REVERSED; v112_jr_four_spellings_reversed, v112_jr_four_spellings_reversed_deferred; }

/// `"V112_jr_case_T"` and `v112_jr_case_t` are two relations and stay two sources.
#[pg_test]
fn v112_jr_names_differing_by_case_stay_two_sources() {
    for table in ["\"V112_jr_case_T\"", "v112_jr_case_t"] {
        v112_j_season_table(table);
    }
    let upper = "SELECT season, SUM(v) AS s FROM \"V112_jr_case_T\" GROUP BY season";
    let lower = "SELECT season, SUM(v) AS s FROM \"v112_jr_case_t\" GROUP BY season";
    for (imv, query) in [("v112_jr_case_upper", upper), ("v112_jr_case_lower", lower)] {
        assert_eq!(
            crate::create_reflex_ivm(imv, query, None, None, None, None),
            "CREATE REFLEX INCREMENTAL VIEW"
        );
        assert_imv_correct(imv, query);
    }
    let spellings = Spi::get_one::<String>(
        "SELECT string_agg(name || '=' || array_to_string(depends_on, ','), ' ' ORDER BY name) \
         FROM public.__reflex_ivm_reference \
         WHERE name IN ('v112_jr_case_upper', 'v112_jr_case_lower')",
    )
    .expect("registry")
    .unwrap_or_default();
    assert_eq!(
        spellings,
        "v112_jr_case_lower=v112_jr_case_t v112_jr_case_upper=\"V112_jr_case_T\""
    );
}

const V112_JR_CALLER_SPELLINGS: [&str; 4] = ["{t}", "\"{t}\"", "public.{t}", "\"public\".\"{t}\""];

fn v112_jr_three_imvs_over(table: &str, mode: &str) -> Vec<(String, String)> {
    v112_j_season_table(table);
    let imvs = vec![
        (format!("{table}_a"), format!("SELECT season, SUM(v) AS s FROM \"{table}\" GROUP BY season")),
        (format!("{table}_b"), format!("SELECT id, season, v FROM {table}")),
        (format!("{table}_c"), format!("SELECT season, COUNT(*) AS c FROM public.{table} GROUP BY season")),
    ];
    for (imv, query) in &imvs {
        assert_eq!(
            crate::create_reflex_ivm(imv, query, None, None, Some(mode), None),
            "CREATE REFLEX INCREMENTAL VIEW"
        );
    }
    imvs
}

#[pg_test]
fn v112_jr_refresh_depending_on_reaches_every_spelling() {
    v112_jr_three_imvs_over("v112_jr_reach_t", "IMMEDIATE");
    for spelling in V112_JR_CALLER_SPELLINGS {
        let caller = spelling.replace("{t}", "v112_jr_reach_t");
        assert_eq!(
            crate::refresh_imv_depending_on(&caller),
            "REFRESHED 3 IMVs",
            "{caller}"
        );
    }
    assert_eq!(crate::refresh_imv_depending_on("\"V112_jr_reach_t\""), "REFRESHED 0 IMVs");
}

fn v112_jr_flush_reaches_every_imv(tag: &str, spelling: &str) {
    let table = format!("v112_jr_flush_{tag}_t");
    let imvs = v112_jr_three_imvs_over(&table, "DEFERRED");
    Spi::run(&format!("INSERT INTO {table}(season, v) VALUES ('e',10),('n',30)")).expect("insert");
    Spi::run(&format!("DELETE FROM {table} WHERE id = 2")).expect("delete");
    let caller = spelling.replace("{t}", &table);
    let flushed = v112_j_try(&format!("SELECT reflex_flush_deferred('{caller}')"));
    assert!(!flushed.contains("RAISED") && !flushed.contains("ERROR"), "{flushed}");
    for (imv, query) in &imvs {
        assert_eq!(v112_j_oracle(imv, query), "0", "{imv} after flush of {caller}: {flushed}");
    }
}

#[pg_test]
fn v112_jr_flush_reaches_every_imv_bare() {
    v112_jr_flush_reaches_every_imv("bare", V112_JR_CALLER_SPELLINGS[0]);
}

#[pg_test]
fn v112_jr_flush_reaches_every_imv_quoted() {
    v112_jr_flush_reaches_every_imv("quoted", V112_JR_CALLER_SPELLINGS[1]);
}

#[pg_test]
fn v112_jr_flush_reaches_every_imv_public() {
    v112_jr_flush_reaches_every_imv("public", V112_JR_CALLER_SPELLINGS[2]);
}

#[pg_test]
fn v112_jr_flush_reaches_every_imv_public_quoted() {
    v112_jr_flush_reaches_every_imv("pubq", V112_JR_CALLER_SPELLINGS[3]);
}

/// The Path C statement of an IMV is built once per backend and source: the
/// second call parses nothing.
#[pg_test]
fn v112_jr_path_c_statement_is_cached_per_backend() {
    Spi::run("CREATE TABLE v112_jr_pc_d(kind TEXT PRIMARY KEY, label TEXT)").expect("dim");
    Spi::run("CREATE TABLE v112_jr_pc_f(id SERIAL PRIMARY KEY, kind TEXT, v INT)").expect("fact");
    Spi::run("INSERT INTO v112_jr_pc_d VALUES ('a','A'),('b','B')").expect("seed");
    Spi::run("INSERT INTO v112_jr_pc_f(kind, v) VALUES ('a',1),('b',2)").expect("seed");
    assert_eq!(
        crate::create_reflex_ivm(
            "v112_jr_pc_v",
            "SELECT f.kind, SUM(f.v) AS s FROM v112_jr_pc_f f JOIN v112_jr_pc_d d ON d.kind = f.kind GROUP BY f.kind",
            None,
            None,
            None,
            None
        ),
        "CREATE REFLEX INCREMENTAL VIEW"
    );
    let sources = ["v112_jr_pc_f", "v112_jr_pc_d"];
    let build = || {
        sources
            .map(|source| crate::trigger::reflex_build_path_c_explain_sql("v112_jr_pc_v", source))
    };
    crate::trigger::reset_delta_sql_cache();
    let first = build();
    assert!(first.iter().any(|sql| sql.contains("__reflex_new_")), "{first:?}");
    assert_eq!(crate::trigger::delta_sql_cache_len(), sources.len());
    assert_eq!(build(), first);
    assert_eq!(crate::trigger::delta_sql_cache_len(), sources.len());
    assert_eq!(
        crate::trigger::reflex_build_path_c_explain_sql("v112_jr_pc_missing", "v112_jr_pc_f"),
        ""
    );
    assert_eq!(crate::trigger::delta_sql_cache_len(), sources.len());
}

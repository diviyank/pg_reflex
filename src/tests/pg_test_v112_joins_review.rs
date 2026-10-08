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

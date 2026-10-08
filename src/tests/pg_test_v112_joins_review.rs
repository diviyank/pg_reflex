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

struct V112SqStep {
    label: &'static str,
    /// Placeholders of the tables the statements write, in flush order.
    writes: &'static [&'static str],
    statements: &'static [&'static str],
}

struct V112SqFixture {
    /// `(placeholder, bare table name, column list, seed rows)`.
    tables: &'static [(&'static str, &'static str, &'static str, &'static str)],
    steps: &'static [V112SqStep],
}

const fn v112_sq_step(
    label: &'static str,
    writes: &'static [&'static str],
    statements: &'static [&'static str],
) -> V112SqStep {
    V112SqStep {
        label,
        writes,
        statements,
    }
}

/// A main table `{s}` and a table `{t}` the queries read through a subquery.
/// `{t}` holds a duplicated `g`, values of `g` absent from `{s}` and the
/// reverse, so a write to `{t}` can keep, add or remove the membership of rows
/// of `{s}`.
const V112_SQ_MEMBERSHIP: V112SqFixture = V112SqFixture {
    tables: &[
        (
            "{s}",
            "s",
            "id SERIAL PRIMARY KEY, g TEXT, v INT, w INT",
            "INSERT INTO {s}(g, v, w) VALUES ('a',1,2),('a',2,NULL),('a',3,3),('a',NULL,NULL),\
             ('b',5,NULL),('b',9,1),('n',NULL,4),('z',0,5),('m',-4,2),('k',6,7)",
        ),
        (
            "{t}",
            "t",
            "id SERIAL PRIMARY KEY, g TEXT, label TEXT, v INT",
            "INSERT INTO {t}(g, label, v) VALUES ('a','A',1),('b','B',2),('c','C',3),('m','M',4),\
             ('zz','ZZ',5),('a','A2',0)",
        ),
    ],
    steps: &[
        v112_sq_step("s_ins", &["{s}"], &["INSERT INTO {s}(g, v, w) VALUES ('a',10,1),('c',7,NULL),('q',3,2),('b',8,3)"]),
        v112_sq_step("t_ins_new", &["{t}"], &["INSERT INTO {t}(g, label, v) VALUES ('z','Z',0),('k','K',1)"]),
        v112_sq_step("t_ins_dup", &["{t}"], &["INSERT INTO {t}(g, label, v) VALUES ('b','B2',1)"]),
        v112_sq_step("t_upd_label", &["{t}"], &["UPDATE {t} SET label = label || '!' WHERE g IN ('a','b')"]),
        v112_sq_step("t_upd_hide", &["{t}"], &["UPDATE {t} SET label = 'x' WHERE g = 'k'"]),
        v112_sq_step("t_move", &["{t}"], &["UPDATE {t} SET g = 'n' WHERE g = 'c'"]),
        v112_sq_step("s_upd", &["{s}"], &["UPDATE {s} SET v = v + 100 WHERE g = 'a' AND v = 1"]),
        v112_sq_step("s_move", &["{s}"], &["UPDATE {s} SET g = 'm' WHERE g = 'q'"]),
        v112_sq_step("t_del_dup", &["{t}"], &["DELETE FROM {t} WHERE label LIKE 'B2%'"]),
        v112_sq_step("t_del", &["{t}"], &["DELETE FROM {t} WHERE g = 'm'"]),
        v112_sq_step("t_upd_v", &["{t}"], &["UPDATE {t} SET v = v + 4 WHERE g = 'zz'"]),
        v112_sq_step("t_upd_v_down", &["{t}"], &["UPDATE {t} SET v = 2 WHERE g = 'zz'"]),
        v112_sq_step("s_del", &["{s}"], &["DELETE FROM {s} WHERE g = 'a' AND v = 2"]),
        v112_sq_step(
            "both_s_then_t",
            &["{s}", "{t}"],
            &[
                "INSERT INTO {s}(g, v, w) VALUES ('y',4,1),('a',11,NULL)",
                "INSERT INTO {t}(g, label, v) VALUES ('y','Y',1)",
                "DELETE FROM {t} WHERE g = 'b'",
            ],
        ),
        v112_sq_step(
            "both_t_then_s",
            &["{t}", "{s}"],
            &[
                "DELETE FROM {t} WHERE g = 'y'",
                "UPDATE {s} SET v = v + 1 WHERE g IN ('y','a')",
                "INSERT INTO {t}(g, label, v) VALUES ('b','B',2)",
            ],
        ),
        v112_sq_step("t_empty", &["{t}"], &["DELETE FROM {t}"]),
        v112_sq_step("t_refill", &["{t}"], &["INSERT INTO {t}(g, label, v) VALUES ('a','A',1),('n','N',2)"]),
        v112_sq_step("s_empty", &["{s}"], &["DELETE FROM {s}"]),
        v112_sq_step("s_refill", &["{s}"], &["INSERT INTO {s}(g, v, w) VALUES ('a',1,NULL),('n',2,1),('p',3,NULL)"]),
    ],
};

/// A dimension `{d}` LEFT JOINed to a fact `{f}` keyed by `kind`, with matched
/// and unmatched rows on both sides and fact values on both sides of zero.
const V112_SQ_NULL_EXTENDED: V112SqFixture = V112SqFixture {
    tables: &[
        (
            "{f}",
            "fact",
            "kind TEXT PRIMARY KEY, v INT",
            "INSERT INTO {f} VALUES ('a',1),('b',-2),('c',-3),('z',-4)",
        ),
        (
            "{d}",
            "dim",
            "kind TEXT PRIMARY KEY, label TEXT",
            "INSERT INTO {d} VALUES ('a','A'),('b','B'),('c','C'),('u','U'),('e','E')",
        ),
        (
            "{o}",
            "other",
            "kind TEXT PRIMARY KEY, w INT",
            "INSERT INTO {o} VALUES ('a',1),('b',2),('c',3),('z',4)",
        ),
    ],
    steps: &[
        v112_sq_step("f_enter", &["{f}"], &["UPDATE {f} SET v = 5 WHERE kind IN ('b','c')"]),
        v112_sq_step("f_enter_unmatched", &["{f}"], &["UPDATE {f} SET v = 6 WHERE kind = 'z'"]),
        v112_sq_step("f_leave", &["{f}"], &["UPDATE {f} SET v = -1 WHERE kind = 'a'"]),
        v112_sq_step("f_reenter", &["{f}"], &["UPDATE {f} SET v = 9 WHERE kind = 'a'"]),
        v112_sq_step("f_stay", &["{f}"], &["UPDATE {f} SET v = v + 1 WHERE kind = 'b'"]),
        v112_sq_step("f_mixed", &["{f}"], &["UPDATE {f} SET v = -v WHERE kind IN ('a','b','z')"]),
        v112_sq_step("f_ins_failing", &["{f}"], &["INSERT INTO {f} VALUES ('u',-7)"]),
        v112_sq_step("f_ins_passing", &["{f}"], &["INSERT INTO {f} VALUES ('e',7),('x',1)"]),
        v112_sq_step("f_del_failing", &["{f}"], &["DELETE FROM {f} WHERE kind = 'u'"]),
        v112_sq_step("f_del_passing", &["{f}"], &["DELETE FROM {f} WHERE kind = 'c'"]),
        v112_sq_step("f_move", &["{f}"], &["UPDATE {f} SET kind = 'c' WHERE kind = 'e'"]),
        v112_sq_step(
            "f_batch_outside_filter",
            &["{f}"],
            &[
                "INSERT INTO {f} VALUES ('w',-8)",
                "UPDATE {f} SET kind = 'u' WHERE kind = 'w'",
                "DELETE FROM {f} WHERE kind = 'x'",
            ],
        ),
        v112_sq_step("f_del_batch_row", &["{f}"], &["DELETE FROM {f} WHERE kind = 'u'"]),
        v112_sq_step("o_del", &["{o}"], &["DELETE FROM {o} WHERE kind = 'b'"]),
        v112_sq_step("o_ins", &["{o}"], &["INSERT INTO {o} VALUES ('b',5),('x',6)"]),
        v112_sq_step("f_move_back", &["{f}"], &["UPDATE {f} SET kind = 'e' WHERE kind = 'c'"]),
        v112_sq_step("f_upd_two", &["{f}"], &["UPDATE {f} SET v = v + 10 WHERE kind IN ('a','x')"]),
        v112_sq_step("d_ins", &["{d}"], &["INSERT INTO {d} VALUES ('z','Z'),('w','W')"]),
        v112_sq_step("d_upd", &["{d}"], &["UPDATE {d} SET label = 'BB' WHERE kind = 'b'"]),
        v112_sq_step("d_del", &["{d}"], &["DELETE FROM {d} WHERE kind IN ('a','u')"]),
        v112_sq_step("f_empty", &["{f}"], &["DELETE FROM {f}"]),
        v112_sq_step("f_refill", &["{f}"], &["INSERT INTO {f} VALUES ('b',-1),('c',2),('q',3)"]),
        v112_sq_step("f_ins_unmatched_failing", &["{f}"], &["INSERT INTO {f} VALUES ('y',-5)"]),
        v112_sq_step("f_enter_second_unmatched", &["{f}"], &["UPDATE {f} SET v = 5 WHERE kind = 'y'"]),
        v112_sq_step("d_empty", &["{d}"], &["DELETE FROM {d}"]),
    ],
};

fn v112_sq_resolve(template: &str, fixture: &V112SqFixture, tag: &str) -> String {
    fixture
        .tables
        .iter()
        .fold(template.to_string(), |sql, (placeholder, bare, _, _)| {
            sql.replace(placeholder, &format!("v112_sq_{tag}_{bare}"))
        })
}

/// Creates the IMV over a fresh fixture, then runs every step that writes a
/// table of the query, comparing the IMV with the query after each. A step
/// that left the IMV wrong is followed by a reconcile, so each outcome is that
/// of its own step. Returns "ok" when every comparison matched, otherwise the
/// steps that did not.
fn v112_sq_cell(
    fixture: &V112SqFixture,
    tag: &str,
    select_template: &str,
    unique_key: Option<&str>,
    deferred: bool,
) -> String {
    for (placeholder, _, columns, seed) in fixture.tables {
        let table = v112_sq_resolve(placeholder, fixture, tag);
        Spi::run(&format!("CREATE TABLE {table}({columns})")).expect("create table");
        Spi::run(&v112_sq_resolve(seed, fixture, tag)).expect("seed");
    }
    let imv = format!("v112_sq_{tag}");
    let fresh = v112_sq_resolve(select_template, fixture, tag);
    let mode = if deferred { "DEFERRED" } else { "IMMEDIATE" };
    let key = unique_key.map_or("NULL".to_string(), |k| format!("'{k}'"));
    let created = v112_j_try(&format!(
        "SELECT create_reflex_ivm('{imv}', $v112sel${fresh}$v112sel$, {key}, 'UNLOGGED', '{mode}')"
    ));
    if created != "CREATE REFLEX INCREMENTAL VIEW" {
        return format!("NOT-CREATED {created}");
    }
    let mut failures: Vec<String> = Vec::new();
    let created_mismatches = v112_j_oracle(&imv, &fresh);
    if created_mismatches != "0" {
        failures.push(format!("create={created_mismatches}"));
    }
    'steps: for step in fixture.steps {
        if !step.writes.iter().all(|w| select_template.contains(w)) {
            continue;
        }
        for statement in step.statements {
            let ran = v112_j_try_dml(&v112_sq_resolve(statement, fixture, tag));
            if ran != "OK" {
                failures.push(format!("{}={ran}", step.label));
                continue 'steps;
            }
        }
        if deferred {
            for written in step.writes {
                let table = v112_sq_resolve(written, fixture, tag);
                let flushed = v112_j_try(&format!("SELECT reflex_flush_deferred('{table}')"));
                if flushed.starts_with("RAISED") || flushed.starts_with("ERROR") {
                    failures.push(format!("{}={flushed}", step.label));
                    break 'steps;
                }
            }
        }
        let mismatches = v112_j_oracle(&imv, &fresh);
        if mismatches != "0" {
            failures.push(format!("{}={mismatches}", step.label));
            let reconciled = v112_j_try(&format!("SELECT reflex_reconcile('{imv}')"));
            if v112_j_oracle(&imv, &fresh) != "0" {
                failures.push(format!("reconcile-after-{}={reconciled}", step.label));
                break;
            }
        }
    }
    if failures.is_empty() {
        "ok".to_string()
    } else {
        failures.join(" ")
    }
}

macro_rules! v112_sq_cells {
    ($fixture:expr, $key:expr, $deferred:expr; $($name:ident => $select:expr,)+) => {
        $(
            #[pg_test]
            fn $name() {
                let tag = stringify!($name).trim_start_matches("v112_sq_");
                assert_eq!(
                    v112_sq_cell(&$fixture, tag, $select, $key, $deferred),
                    "ok",
                    "{}",
                    $select
                );
            }
        )+
    };
}

// ---------------------------------------------------------------------------
// A table read only through a WHERE subquery: aggregates
// ---------------------------------------------------------------------------

v112_sq_cells! { V112_SQ_MEMBERSHIP, None, false;
    v112_sq_sum_in => "SELECT g, SUM(v) AS sv, COUNT(*) AS c FROM {s} WHERE g IN (SELECT g FROM {t}) GROUP BY g",
    v112_sq_sum_in_or => "SELECT g, SUM(v) AS sv, COUNT(*) AS c FROM {s} WHERE g IN (SELECT g FROM {t}) OR w IS NULL GROUP BY g",
    v112_sq_sum_in_and => "SELECT g, SUM(v) AS sv, COUNT(*) AS c FROM {s} WHERE g IN (SELECT g FROM {t}) AND v > 0 GROUP BY g",
    v112_sq_sum_in_filtered => "SELECT g, SUM(v) AS sv, COUNT(*) AS c FROM {s} WHERE g IN (SELECT g FROM {t} WHERE label <> 'x') GROUP BY g",
    v112_sq_sum_not_in => "SELECT g, SUM(v) AS sv, COUNT(*) AS c FROM {s} WHERE g NOT IN (SELECT g FROM {t}) GROUP BY g",
    v112_sq_sum_exists => "SELECT m.g, SUM(m.v) AS sv, COUNT(*) AS c FROM {s} m WHERE EXISTS (SELECT 1 FROM {t} x WHERE x.g = m.g) GROUP BY m.g",
    v112_sq_sum_exists_or => "SELECT m.g, SUM(m.v) AS sv, COUNT(*) AS c FROM {s} m WHERE EXISTS (SELECT 1 FROM {t} x WHERE x.g = m.g) OR m.w IS NULL GROUP BY m.g",
    v112_sq_sum_not_exists => "SELECT m.g, SUM(m.v) AS sv, COUNT(*) AS c FROM {s} m WHERE NOT EXISTS (SELECT 1 FROM {t} x WHERE x.g = m.g) GROUP BY m.g",
    v112_sq_sum_scalar => "SELECT g, SUM(v) AS sv, COUNT(*) AS c FROM {s} WHERE v >= (SELECT max(v) FROM {t}) GROUP BY g",
    v112_sq_sum_scalar_or => "SELECT g, SUM(v) AS sv, COUNT(*) AS c FROM {s} WHERE v >= (SELECT max(v) FROM {t}) OR w IS NULL GROUP BY g",
    v112_sq_sum_any => "SELECT g, SUM(v) AS sv, COUNT(*) AS c FROM {s} WHERE g = ANY (SELECT g FROM {t}) GROUP BY g",
    v112_sq_sum_ungrouped_in => "SELECT SUM(v) AS sv, COUNT(*) AS c FROM {s} WHERE g IN (SELECT g FROM {t})",
    v112_sq_sum_ungrouped_scalar_or => "SELECT SUM(v) AS sv, COUNT(*) AS c FROM {s} WHERE v >= (SELECT max(v) FROM {t}) OR w IS NULL",
    v112_sq_minmax_in => "SELECT g, MIN(v) AS mn, MAX(v) AS mx FROM {s} WHERE g IN (SELECT g FROM {t}) GROUP BY g",
    v112_sq_minmax_in_or => "SELECT g, MIN(v) AS mn, MAX(v) AS mx FROM {s} WHERE g IN (SELECT g FROM {t}) OR w IS NULL GROUP BY g",
    v112_sq_max_in_or_filtered => "SELECT g, MAX(v) AS mx FROM {s} WHERE g IN (SELECT g FROM {t} WHERE label <> 'x' OR v > 2) OR w IS NULL GROUP BY g",
    v112_sq_minmax_not_in => "SELECT g, MIN(v) AS mn, MAX(v) AS mx FROM {s} WHERE g NOT IN (SELECT g FROM {t}) GROUP BY g",
    v112_sq_minmax_exists => "SELECT m.g, MIN(m.v) AS mn, MAX(m.v) AS mx FROM {s} m WHERE EXISTS (SELECT 1 FROM {t} x WHERE x.g = m.g) GROUP BY m.g",
    v112_sq_minmax_not_exists_or => "SELECT m.g, MIN(m.v) AS mn, MAX(m.v) AS mx FROM {s} m WHERE NOT EXISTS (SELECT 1 FROM {t} x WHERE x.g = m.g) OR m.w IS NULL GROUP BY m.g",
    v112_sq_minmax_scalar => "SELECT g, MIN(v) AS mn, MAX(v) AS mx FROM {s} WHERE v >= (SELECT max(v) FROM {t}) GROUP BY g",
    v112_sq_minmax_scalar_or => "SELECT g, MIN(v) AS mn, MAX(v) AS mx FROM {s} WHERE v >= (SELECT max(v) FROM {t}) OR w IS NULL GROUP BY g",
    v112_sq_avg_in => "SELECT g, AVG(v) AS av, COUNT(v) AS c FROM {s} WHERE g IN (SELECT g FROM {t}) GROUP BY g",
    v112_sq_distinct_in => "SELECT DISTINCT g FROM {s} WHERE g IN (SELECT g FROM {t})",
    v112_sq_sum_select_scalar => "SELECT g, SUM(v + (SELECT max(v) FROM {t})) AS sv, COUNT(*) AS c FROM {s} GROUP BY g",
    v112_sq_sum_lowercase_in => "select g, sum(v) as sv, count(*) as c from {s} where g in (select g from {t}) group by g",
    v112_sq_sum_join_and_in => "SELECT m.g, SUM(m.v) AS sv, COUNT(*) AS c FROM {s} m JOIN {t} j ON j.g = m.g WHERE m.v > 0 GROUP BY m.g",
}

v112_sq_cells! { V112_SQ_MEMBERSHIP, None, true;
    v112_sq_sum_in_deferred => "SELECT g, SUM(v) AS sv, COUNT(*) AS c FROM {s} WHERE g IN (SELECT g FROM {t}) GROUP BY g",
    v112_sq_sum_in_or_deferred => "SELECT g, SUM(v) AS sv, COUNT(*) AS c FROM {s} WHERE g IN (SELECT g FROM {t}) OR w IS NULL GROUP BY g",
    v112_sq_sum_not_in_deferred => "SELECT g, SUM(v) AS sv, COUNT(*) AS c FROM {s} WHERE g NOT IN (SELECT g FROM {t}) GROUP BY g",
    v112_sq_sum_exists_deferred => "SELECT m.g, SUM(m.v) AS sv, COUNT(*) AS c FROM {s} m WHERE EXISTS (SELECT 1 FROM {t} x WHERE x.g = m.g) GROUP BY m.g",
    v112_sq_sum_scalar_deferred => "SELECT g, SUM(v) AS sv, COUNT(*) AS c FROM {s} WHERE v >= (SELECT max(v) FROM {t}) GROUP BY g",
    v112_sq_sum_ungrouped_in_deferred => "SELECT SUM(v) AS sv, COUNT(*) AS c FROM {s} WHERE g IN (SELECT g FROM {t})",
    v112_sq_minmax_in_deferred => "SELECT g, MIN(v) AS mn, MAX(v) AS mx FROM {s} WHERE g IN (SELECT g FROM {t}) GROUP BY g",
    v112_sq_minmax_in_or_deferred => "SELECT g, MIN(v) AS mn, MAX(v) AS mx FROM {s} WHERE g IN (SELECT g FROM {t}) OR w IS NULL GROUP BY g",
    v112_sq_max_in_or_filtered_deferred => "SELECT g, MAX(v) AS mx FROM {s} WHERE g IN (SELECT g FROM {t} WHERE label <> 'x' OR v > 2) OR w IS NULL GROUP BY g",
    v112_sq_minmax_scalar_or_deferred => "SELECT g, MIN(v) AS mn, MAX(v) AS mx FROM {s} WHERE v >= (SELECT max(v) FROM {t}) OR w IS NULL GROUP BY g",
    v112_sq_distinct_in_deferred => "SELECT DISTINCT g FROM {s} WHERE g IN (SELECT g FROM {t})",
    v112_sq_sum_join_and_in_deferred => "SELECT m.g, SUM(m.v) AS sv, COUNT(*) AS c FROM {s} m JOIN {t} j ON j.g = m.g WHERE m.v > 0 GROUP BY m.g",
}

// ---------------------------------------------------------------------------
// A table read only through a subquery: passthrough, keyless and keyed
// ---------------------------------------------------------------------------

v112_sq_cells! { V112_SQ_MEMBERSHIP, None, false;
    v112_sq_pt_in => "SELECT id, g, v FROM {s} WHERE g IN (SELECT g FROM {t})",
    v112_sq_pt_in_or => "SELECT id, g, v FROM {s} WHERE g IN (SELECT g FROM {t}) OR w IS NULL",
    v112_sq_pt_not_in => "SELECT id, g, v FROM {s} WHERE g NOT IN (SELECT g FROM {t})",
    v112_sq_pt_exists => "SELECT m.id, m.g, m.v FROM {s} m WHERE EXISTS (SELECT 1 FROM {t} x WHERE x.g = m.g)",
    v112_sq_pt_not_exists => "SELECT m.id, m.g, m.v FROM {s} m WHERE NOT EXISTS (SELECT 1 FROM {t} x WHERE x.g = m.g)",
    v112_sq_pt_scalar => "SELECT id, g, v FROM {s} WHERE v >= (SELECT max(v) FROM {t})",
    v112_sq_pt_scalar_or => "SELECT id, g, v FROM {s} WHERE v >= (SELECT max(v) FROM {t}) OR w IS NULL",
    v112_sq_pt_select_scalar => "SELECT id, g, v, (SELECT max(v) FROM {t}) AS top FROM {s}",
    v112_sq_pt_select_correlated => "SELECT m.id, m.g, (SELECT count(*) FROM {t} x WHERE x.g = m.g) AS members FROM {s} m",
}

v112_sq_cells! { V112_SQ_MEMBERSHIP, Some("id"), false;
    v112_sq_kp_in => "SELECT id, g, v FROM {s} WHERE g IN (SELECT g FROM {t})",
    v112_sq_kp_in_or => "SELECT id, g, v FROM {s} WHERE g IN (SELECT g FROM {t}) OR w IS NULL",
    v112_sq_kp_in_filtered => "SELECT id, g, v FROM {s} WHERE g IN (SELECT g FROM {t} WHERE label <> 'x')",
    v112_sq_kp_not_in => "SELECT id, g, v FROM {s} WHERE g NOT IN (SELECT g FROM {t})",
    v112_sq_kp_exists => "SELECT m.id, m.g, m.v FROM {s} m WHERE EXISTS (SELECT 1 FROM {t} x WHERE x.g = m.g)",
    v112_sq_kp_not_exists_or => "SELECT m.id, m.g, m.v FROM {s} m WHERE NOT EXISTS (SELECT 1 FROM {t} x WHERE x.g = m.g) OR m.w IS NULL",
    v112_sq_kp_scalar => "SELECT id, g, v FROM {s} WHERE v >= (SELECT max(v) FROM {t})",
    v112_sq_kp_scalar_or => "SELECT id, g, v FROM {s} WHERE v >= (SELECT max(v) FROM {t}) OR w IS NULL",
    v112_sq_kp_select_scalar => "SELECT id, g, v, (SELECT max(v) FROM {t}) AS top FROM {s}",
    v112_sq_kp_select_correlated => "SELECT m.id, m.g, (SELECT count(*) FROM {t} x WHERE x.g = m.g) AS members FROM {s} m",
    v112_sq_kp_two_line_in => "SELECT id, g, v FROM {s} WHERE g IN\n (SELECT g\n FROM\n {t})",
}

v112_sq_cells! { V112_SQ_MEMBERSHIP, None, true;
    v112_sq_pt_in_deferred => "SELECT id, g, v FROM {s} WHERE g IN (SELECT g FROM {t})",
    v112_sq_pt_not_exists_deferred => "SELECT m.id, m.g, m.v FROM {s} m WHERE NOT EXISTS (SELECT 1 FROM {t} x WHERE x.g = m.g)",
    v112_sq_pt_scalar_or_deferred => "SELECT id, g, v FROM {s} WHERE v >= (SELECT max(v) FROM {t}) OR w IS NULL",
}

v112_sq_cells! { V112_SQ_MEMBERSHIP, Some("id"), true;
    v112_sq_kp_in_deferred => "SELECT id, g, v FROM {s} WHERE g IN (SELECT g FROM {t})",
    v112_sq_kp_in_or_deferred => "SELECT id, g, v FROM {s} WHERE g IN (SELECT g FROM {t}) OR w IS NULL",
    v112_sq_kp_exists_deferred => "SELECT m.id, m.g, m.v FROM {s} m WHERE EXISTS (SELECT 1 FROM {t} x WHERE x.g = m.g)",
    v112_sq_kp_scalar_deferred => "SELECT id, g, v FROM {s} WHERE v >= (SELECT max(v) FROM {t})",
    v112_sq_kp_select_scalar_deferred => "SELECT id, g, v, (SELECT max(v) FROM {t}) AS top FROM {s}",
}

// ---------------------------------------------------------------------------
// A WHERE that filters the null-extended table of a LEFT JOIN
// ---------------------------------------------------------------------------

v112_sq_cells! { V112_SQ_NULL_EXTENDED, None, false;
    v112_sq_lj_admits_null => "SELECT d.kind, COUNT(*) AS c, SUM(f.v) AS s FROM {d} d LEFT JOIN {f} f ON d.kind = f.kind WHERE f.v > 0 OR f.v IS NULL GROUP BY d.kind",
    v112_sq_lj_admits_null_by_label => "SELECT d.label, COUNT(*) AS c, SUM(f.v) AS s FROM {d} d LEFT JOIN {f} f ON d.kind = f.kind WHERE f.v > 0 OR f.v IS NULL GROUP BY d.label",
    v112_sq_lj_admits_null_ungrouped => "SELECT COUNT(*) AS c, SUM(f.v) AS s FROM {d} d LEFT JOIN {f} f ON d.kind = f.kind WHERE f.v > 0 OR f.v IS NULL",
    v112_sq_lj_admits_null_minmax => "SELECT d.kind, MIN(f.v) AS mn, MAX(f.v) AS mx, COUNT(*) AS c FROM {d} d LEFT JOIN {f} f ON d.kind = f.kind WHERE f.v > 0 OR f.v IS NULL GROUP BY d.kind",
    v112_sq_lj_only_null => "SELECT d.kind, COUNT(*) AS c FROM {d} d LEFT JOIN {f} f ON d.kind = f.kind WHERE f.kind IS NULL GROUP BY d.kind",
    v112_sq_lj_coalesce => "SELECT d.kind, COUNT(*) AS c, SUM(f.v) AS s FROM {d} d LEFT JOIN {f} f ON d.kind = f.kind WHERE COALESCE(f.v, 1) > 0 GROUP BY d.kind",
    v112_sq_lj_is_distinct => "SELECT d.kind, COUNT(*) AS c, SUM(f.v) AS s FROM {d} d LEFT JOIN {f} f ON d.kind = f.kind WHERE f.v IS DISTINCT FROM -1 GROUP BY d.kind",
    v112_sq_lj_rejects_null => "SELECT d.kind, COUNT(*) AS c, SUM(f.v) AS s FROM {d} d LEFT JOIN {f} f ON d.kind = f.kind WHERE f.v > 0 GROUP BY d.kind",
    v112_sq_lj_rejects_null_and_dim => "SELECT d.kind, COUNT(*) AS c, SUM(f.v) AS s FROM {d} d LEFT JOIN {f} f ON d.kind = f.kind WHERE f.v > 0 AND d.label <> 'Q' GROUP BY d.kind",
    v112_sq_lj_dim_filter_only => "SELECT d.kind, COUNT(*) AS c, SUM(f.v) AS s FROM {d} d LEFT JOIN {f} f ON d.kind = f.kind WHERE d.label <> 'Q' GROUP BY d.kind",
    v112_sq_lj_no_where => "SELECT d.kind, COUNT(*) AS c, SUM(f.v) AS s FROM {d} d LEFT JOIN {f} f ON d.kind = f.kind GROUP BY d.kind",
    v112_sq_lj_on_filter => "SELECT d.kind, COUNT(*) AS c, SUM(f.v) AS s FROM {d} d LEFT JOIN {f} f ON d.kind = f.kind AND f.v > 0 GROUP BY d.kind",
    v112_sq_lj_outer_keyword_admits_null => "SELECT d.kind, COUNT(*) AS c, SUM(f.v) AS s FROM {d} d LEFT OUTER JOIN {f} f ON d.kind = f.kind WHERE f.v > 0 OR f.v IS NULL GROUP BY d.kind",
    v112_sq_lj_unaliased_admits_null => "SELECT {d}.kind, COUNT(*) AS c, SUM({f}.v) AS s FROM {d} LEFT JOIN {f} ON {d}.kind = {f}.kind WHERE {f}.v > 0 OR {f}.v IS NULL GROUP BY {d}.kind",
    v112_sq_ij_admits_null => "SELECT d.kind, COUNT(*) AS c, SUM(f.v) AS s FROM {d} d JOIN {f} f ON d.kind = f.kind WHERE f.v > 0 OR f.v IS NULL GROUP BY d.kind",
    v112_sq_fj_admits_null => "SELECT d.kind, COUNT(*) AS c, SUM(f.v) AS s FROM {d} d FULL JOIN {f} f ON d.kind = f.kind WHERE f.v > 0 OR f.v IS NULL GROUP BY d.kind",
    v112_sq_lj_pt_admits_null => "SELECT d.kind, d.label, f.v FROM {d} d LEFT JOIN {f} f ON d.kind = f.kind WHERE f.v > 0 OR f.v IS NULL",
    v112_sq_lj_pt_rejects_null => "SELECT d.kind, d.label, f.v FROM {d} d LEFT JOIN {f} f ON d.kind = f.kind WHERE f.v > 0",
    v112_sq_lj_pt_only_null => "SELECT d.kind, d.label FROM {d} d LEFT JOIN {f} f ON d.kind = f.kind WHERE f.kind IS NULL",
}

v112_sq_cells! { V112_SQ_NULL_EXTENDED, Some("kind"), false;
    v112_sq_lj_kp_admits_null => "SELECT d.kind, d.label, f.v FROM {d} d LEFT JOIN {f} f ON d.kind = f.kind WHERE f.v > 0 OR f.v IS NULL",
    v112_sq_lj_kp_rejects_null => "SELECT d.kind, d.label, f.v FROM {d} d LEFT JOIN {f} f ON d.kind = f.kind WHERE f.v > 0",
}

v112_sq_cells! { V112_SQ_NULL_EXTENDED, None, true;
    v112_sq_lj_admits_null_deferred => "SELECT d.kind, COUNT(*) AS c, SUM(f.v) AS s FROM {d} d LEFT JOIN {f} f ON d.kind = f.kind WHERE f.v > 0 OR f.v IS NULL GROUP BY d.kind",
    v112_sq_lj_admits_null_minmax_deferred => "SELECT d.kind, MIN(f.v) AS mn, MAX(f.v) AS mx, COUNT(*) AS c FROM {d} d LEFT JOIN {f} f ON d.kind = f.kind WHERE f.v > 0 OR f.v IS NULL GROUP BY d.kind",
    v112_sq_lj_only_null_deferred => "SELECT d.kind, COUNT(*) AS c FROM {d} d LEFT JOIN {f} f ON d.kind = f.kind WHERE f.kind IS NULL GROUP BY d.kind",
    v112_sq_lj_rejects_null_deferred => "SELECT d.kind, COUNT(*) AS c, SUM(f.v) AS s FROM {d} d LEFT JOIN {f} f ON d.kind = f.kind WHERE f.v > 0 GROUP BY d.kind",
    v112_sq_lj_pt_admits_null_deferred => "SELECT d.kind, d.label, f.v FROM {d} d LEFT JOIN {f} f ON d.kind = f.kind WHERE f.v > 0 OR f.v IS NULL",
}

v112_sq_cells! { V112_SQ_MEMBERSHIP, None, false;
    v112_sq_ctl_minmax_or => "SELECT g, MIN(v) AS mn, MAX(v) AS mx FROM {s} WHERE v > 2 OR w IS NULL GROUP BY g",
    v112_sq_ctl_sum_or => "SELECT g, SUM(v) AS sv, COUNT(*) AS c FROM {s} WHERE v > 2 OR w IS NULL GROUP BY g",
    v112_sq_sum_correlated_scalar => "SELECT m.g, SUM(m.v) AS sv, COUNT(*) AS c FROM {s} m WHERE m.v > (SELECT count(*) FROM {t} x WHERE x.g = m.g) GROUP BY m.g",
    v112_sq_sum_unaliased_not_exists_or => "SELECT {s}.g, SUM({s}.v) AS sv, COUNT(*) AS c FROM {s} WHERE NOT EXISTS (SELECT 1 FROM {t} WHERE {t}.g = {s}.g) OR {s}.w IS NULL GROUP BY {s}.g",
}

v112_sq_cells! { V112_SQ_MEMBERSHIP, Some("id"), false;
    v112_sq_ctl_kp_or => "SELECT id, g, v FROM {s} WHERE v > 2 OR w IS NULL",
    v112_sq_kp_correlated_scalar => "SELECT m.id, m.g, m.v FROM {s} m WHERE m.v > (SELECT count(*) FROM {t} x WHERE x.g = m.g)",
}

v112_sq_cells! { V112_SQ_MEMBERSHIP, None, false;
    v112_sq_pt_exists_on_unprojected => "SELECT m.id, m.g FROM {s} m WHERE EXISTS (SELECT 1 FROM {t} x WHERE x.v = m.v)",
    v112_sq_pt_in_on_unprojected => "SELECT id, g FROM {s} WHERE v IN (SELECT v FROM {t})",
    v112_sq_sum_exists_on_unprojected => "SELECT m.g, COUNT(*) AS c FROM {s} m WHERE EXISTS (SELECT 1 FROM {t} x WHERE x.v = m.v) GROUP BY m.g",
    v112_sq_ctl_pt_cross_source_conjunct => "SELECT m.id, m.g, j.label FROM {s} m JOIN {t} j ON j.g = m.g WHERE m.v > j.v",
    v112_sq_ctl_sum_cross_source_conjunct => "SELECT m.g, COUNT(*) AS c FROM {s} m JOIN {t} j ON j.g = m.g WHERE m.v > j.v GROUP BY m.g",
}

v112_sq_cells! { V112_SQ_MEMBERSHIP, None, true;
    v112_sq_pt_exists_on_unprojected_deferred => "SELECT m.id, m.g FROM {s} m WHERE EXISTS (SELECT 1 FROM {t} x WHERE x.v = m.v)",
    v112_sq_kp_not_exists_or_deferred => "SELECT m.id, m.g, m.v FROM {s} m WHERE NOT EXISTS (SELECT 1 FROM {t} x WHERE x.g = m.g) OR m.w IS NULL",
    v112_sq_ctl_sum_cross_source_conjunct_deferred => "SELECT m.g, COUNT(*) AS c FROM {s} m JOIN {t} j ON j.g = m.g WHERE m.v > j.v GROUP BY m.g",
}

v112_sq_cells! { V112_SQ_NULL_EXTENDED, None, false;
    v112_sq_lj_rejects_null_by_label => "SELECT d.label, COUNT(*) AS c, SUM(f.v) AS s FROM {d} d LEFT JOIN {f} f ON d.kind = f.kind WHERE f.v > 0 GROUP BY d.label",
    v112_sq_lj_no_where_by_label => "SELECT d.label, COUNT(*) AS c, SUM(f.v) AS s FROM {d} d LEFT JOIN {f} f ON d.kind = f.kind GROUP BY d.label",
    v112_sq_lj_dim_filter_by_label => "SELECT d.label, COUNT(*) AS c, SUM(f.v) AS s FROM {d} d LEFT JOIN {f} f ON d.kind = f.kind WHERE d.kind <> 'q' GROUP BY d.label",
    v112_sq_lj_cross_filter_by_label => "SELECT d.label, COUNT(*) AS c, SUM(f.v) AS s FROM {d} d LEFT JOIN {f} f ON d.kind = f.kind WHERE f.v > 0 OR d.label = 'U' GROUP BY d.label",
    v112_sq_lj_cross_filter => "SELECT d.kind, COUNT(*) AS c, SUM(f.v) AS s FROM {d} d LEFT JOIN {f} f ON d.kind = f.kind WHERE f.v > 0 OR d.label = 'U' GROUP BY d.kind",
    v112_sq_lj_unqualified_filter => "SELECT d.kind, COUNT(*) AS c, SUM(v) AS s FROM {d} d LEFT JOIN {f} f ON d.kind = f.kind WHERE v > 0 OR v IS NULL GROUP BY d.kind",
    v112_sq_lj_admits_null_and_dim => "SELECT d.kind, COUNT(*) AS c, SUM(f.v) AS s FROM {d} d LEFT JOIN {f} f ON d.kind = f.kind WHERE (f.v > 0 OR f.v IS NULL) AND d.label <> 'Q' GROUP BY d.kind",
    v112_sq_lj_rejects_null_between => "SELECT d.kind, COUNT(*) AS c, SUM(f.v) AS s FROM {d} d LEFT JOIN {f} f ON d.kind = f.kind WHERE f.v BETWEEN 1 AND 100 GROUP BY d.kind",
    v112_sq_lj_rejects_null_is_not_null => "SELECT d.kind, COUNT(*) AS c, SUM(f.v) AS s FROM {d} d LEFT JOIN {f} f ON d.kind = f.kind WHERE f.kind IS NOT NULL AND abs(f.v) > 3 GROUP BY d.kind",
    v112_sq_fj_dim_admits_null => "SELECT f.kind, COUNT(*) AS c, COUNT(d.label) AS n FROM {d} d FULL JOIN {f} f ON d.kind = f.kind WHERE d.label <> 'BB' OR d.label IS NULL GROUP BY f.kind",
    v112_sq_fj_rejects_null => "SELECT d.kind, COUNT(*) AS c, SUM(f.v) AS s FROM {d} d FULL JOIN {f} f ON d.kind = f.kind WHERE f.v > 0 GROUP BY d.kind",
    v112_sq_fj_rejects_null_by_fact => "SELECT f.kind, COUNT(*) AS c, COUNT(d.label) AS n FROM {d} d FULL JOIN {f} f ON d.kind = f.kind WHERE f.v > 0 GROUP BY f.kind",
    v112_sq_rj_only_null => "SELECT d.kind, COUNT(*) AS c FROM {f} f RIGHT JOIN {d} d ON d.kind = f.kind WHERE f.kind IS NULL GROUP BY d.kind",
    v112_sq_rj_admits_null => "SELECT d.kind, COUNT(*) AS c, SUM(f.v) AS s FROM {f} f RIGHT JOIN {d} d ON d.kind = f.kind WHERE f.v > 0 OR f.v IS NULL GROUP BY d.kind",
}

v112_sq_cells! { V112_SQ_NULL_EXTENDED, None, true;
    v112_sq_lj_admits_null_by_label_deferred => "SELECT d.label, COUNT(*) AS c, SUM(f.v) AS s FROM {d} d LEFT JOIN {f} f ON d.kind = f.kind WHERE f.v > 0 OR f.v IS NULL GROUP BY d.label",
    v112_sq_lj_rejects_null_by_label_deferred => "SELECT d.label, COUNT(*) AS c, SUM(f.v) AS s FROM {d} d LEFT JOIN {f} f ON d.kind = f.kind WHERE f.v > 0 GROUP BY d.label",
    v112_sq_fj_rejects_null_deferred => "SELECT d.kind, COUNT(*) AS c, SUM(f.v) AS s FROM {d} d FULL JOIN {f} f ON d.kind = f.kind WHERE f.v > 0 GROUP BY d.kind",
    v112_sq_rj_only_null_deferred => "SELECT d.kind, COUNT(*) AS c FROM {f} f RIGHT JOIN {d} d ON d.kind = f.kind WHERE f.kind IS NULL GROUP BY d.kind",
}

// ---------------------------------------------------------------------------
// A later join that filters on the null-extended table
// ---------------------------------------------------------------------------

v112_sq_cells! { V112_SQ_NULL_EXTENDED, None, false;
    v112_sq_lj_then_inner_on_fact_by_label => "SELECT d.label, COUNT(*) AS c, SUM(f.v) AS s FROM {d} d LEFT JOIN {f} f ON d.kind = f.kind JOIN {o} o ON o.kind = f.kind GROUP BY d.label",
    v112_sq_lj_then_inner_on_fact => "SELECT d.kind, COUNT(*) AS c, SUM(f.v) AS s FROM {d} d LEFT JOIN {f} f ON d.kind = f.kind JOIN {o} o ON o.kind = f.kind GROUP BY d.kind",
    v112_sq_lj_then_left_on_fact_by_label => "SELECT d.label, COUNT(*) AS c, SUM(f.v) AS s, SUM(o.w) AS w FROM {d} d LEFT JOIN {f} f ON d.kind = f.kind LEFT JOIN {o} o ON o.kind = f.kind GROUP BY d.label",
    v112_sq_lj_then_inner_on_dim_by_label => "SELECT d.label, COUNT(*) AS c, SUM(f.v) AS s, SUM(o.w) AS w FROM {d} d LEFT JOIN {f} f ON d.kind = f.kind JOIN {o} o ON o.kind = d.kind GROUP BY d.label",
}

v112_sq_cells! { V112_SQ_NULL_EXTENDED, None, true;
    v112_sq_lj_then_inner_on_fact_by_label_deferred => "SELECT d.label, COUNT(*) AS c, SUM(f.v) AS s FROM {d} d LEFT JOIN {f} f ON d.kind = f.kind JOIN {o} o ON o.kind = f.kind GROUP BY d.label",
}

// ---------------------------------------------------------------------------
// Sources that keep their scoped delta
// ---------------------------------------------------------------------------

fn v112_sq_stored_base_query_and_sources(imv: &str, query: &str) -> (String, Vec<String>) {
    assert_eq!(
        crate::create_reflex_ivm(imv, query, None, None, None, None),
        "CREATE REFLEX INCREMENTAL VIEW",
        "{query}"
    );
    let base_query = Spi::get_one::<String>(&format!(
        "SELECT base_query FROM public.__reflex_ivm_reference WHERE name = '{imv}'"
    ))
    .expect("registry")
    .expect("base_query");
    let sources = Spi::get_one::<Vec<String>>(&format!(
        "SELECT depends_on FROM public.__reflex_ivm_reference WHERE name = '{imv}'"
    ))
    .expect("registry")
    .expect("depends_on");
    (base_query, sources)
}

#[pg_test]
fn v112_sq_only_the_table_inside_the_subquery_loses_its_scoped_delta() {
    let fixture = &V112_SQ_MEMBERSHIP;
    let shapes = [
        "SELECT g, SUM(v) AS sv, MIN(v) AS mn, COUNT(*) AS c FROM {s} WHERE g IN (SELECT g FROM {t}) OR w IS NULL GROUP BY g",
        "SELECT m.g, AVG(m.v) AS av FROM {s} m WHERE NOT EXISTS (SELECT 1 FROM {t} x WHERE x.g = m.g) GROUP BY m.g",
        "SELECT COUNT(*) AS c FROM {s} WHERE v >= (SELECT max(v) FROM {t})",
        "SELECT DISTINCT g FROM {s} WHERE g = ANY (SELECT g FROM {t})",
        "SELECT id, g, v, (SELECT max(v) FROM {t}) AS top FROM {s}",
        "SELECT id, g\n  FROM {s}\n  WHERE g IN\n    (SELECT g FROM {t})",
    ];
    for (placeholder, _, columns, seed) in fixture.tables {
        let table = v112_sq_resolve(placeholder, fixture, "scope");
        Spi::run(&format!("CREATE TABLE {table}({columns})")).expect("create table");
        Spi::run(&v112_sq_resolve(seed, fixture, "scope")).expect("seed");
    }
    let main_table = v112_sq_resolve("{s}", fixture, "scope");
    let subquery_table = v112_sq_resolve("{t}", fixture, "scope");
    for (position, shape) in shapes.iter().enumerate() {
        let query = v112_sq_resolve(shape, fixture, "scope");
        let (base_query, sources) =
            v112_sq_stored_base_query_and_sources(&format!("v112_sq_scope_{position}"), &query);
        assert!(sources.contains(&main_table) && sources.contains(&subquery_table), "{query}");
        assert!(
            !crate::trigger::has_no_scoped_delta(&base_query, &main_table),
            "{base_query}"
        );
        assert!(
            crate::trigger::has_no_scoped_delta(&base_query, &subquery_table),
            "{base_query}"
        );
    }
}

#[pg_test]
fn v112_sq_query_without_an_expression_subquery_keeps_every_scoped_delta() {
    let fixture = &V112_SQ_MEMBERSHIP;
    let shapes = [
        "SELECT m.g, SUM(m.v) AS sv, MIN(j.v) AS mn, COUNT(*) AS c FROM {s} m JOIN {t} j ON j.g = m.g WHERE m.v > 0 OR m.w IS NULL GROUP BY m.g",
        "SELECT x.g, SUM(m.v) AS sv FROM {s} m JOIN (SELECT g, v FROM {t} WHERE v > 0) x ON x.g = m.g GROUP BY x.g",
        "SELECT u.g, SUM(u.v) AS sv FROM (SELECT g, v FROM {s} UNION ALL SELECT g, v FROM {t}) u GROUP BY u.g",
        "SELECT m.id, m.g, j.label FROM {s} m JOIN {t} j ON j.g = m.g WHERE m.g IN ('a', 'b') AND m.v = ANY (ARRAY[1, 2, 3])",
        "SELECT m.g, COUNT(*) FILTER (WHERE j.v > 1) AS c FROM {s} m LEFT JOIN {t} j ON j.g = m.g GROUP BY m.g",
    ];
    for (placeholder, _, columns, seed) in fixture.tables {
        let table = v112_sq_resolve(placeholder, fixture, "keep");
        Spi::run(&format!("CREATE TABLE {table}({columns})")).expect("create table");
        Spi::run(&v112_sq_resolve(seed, fixture, "keep")).expect("seed");
    }
    for (position, shape) in shapes.iter().enumerate() {
        let query = v112_sq_resolve(shape, fixture, "keep");
        let (base_query, sources) =
            v112_sq_stored_base_query_and_sources(&format!("v112_sq_keep_{position}"), &query);
        assert!(sources.len() >= 2, "{query}");
        for source in sources {
            assert!(
                !crate::trigger::has_no_scoped_delta(&base_query, &source),
                "{source} of {base_query}"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// The same filters under RIGHT JOIN and FULL JOIN
// ---------------------------------------------------------------------------

v112_sq_cells! { V112_SQ_NULL_EXTENDED, None, false;
    v112_sq_rj_coalesce => "SELECT d.kind, COUNT(*) AS c, SUM(f.v) AS s FROM {f} f RIGHT JOIN {d} d ON d.kind = f.kind WHERE COALESCE(f.v, 1) > 0 GROUP BY d.kind",
    v112_sq_rj_rejects_null => "SELECT d.kind, COUNT(*) AS c, SUM(f.v) AS s FROM {f} f RIGHT JOIN {d} d ON d.kind = f.kind WHERE f.v > 0 GROUP BY d.kind",
    v112_sq_rj_pt_only_null => "SELECT d.kind, d.label FROM {f} f RIGHT JOIN {d} d ON d.kind = f.kind WHERE f.kind IS NULL",
    v112_sq_rj_pt_admits_null => "SELECT d.kind, d.label, f.v FROM {f} f RIGHT JOIN {d} d ON d.kind = f.kind WHERE f.v > 0 OR f.v IS NULL",
    v112_sq_rj_outer_keyword_only_null => "SELECT d.kind, COUNT(*) AS c FROM {f} f RIGHT OUTER JOIN {d} d ON d.kind = f.kind WHERE f.kind IS NULL GROUP BY d.kind",
    v112_sq_rj3_only_null => "SELECT d.kind, COUNT(*) AS c FROM {f} f JOIN {o} o ON o.kind = f.kind RIGHT JOIN {d} d ON d.kind = f.kind WHERE o.kind IS NULL GROUP BY d.kind",
    v112_sq_fj_only_null_fact => "SELECT d.kind, COUNT(*) AS c FROM {d} d FULL JOIN {f} f ON d.kind = f.kind WHERE f.kind IS NULL GROUP BY d.kind",
    v112_sq_fj_only_null_dim => "SELECT f.kind, COUNT(*) AS c, SUM(f.v) AS s FROM {d} d FULL JOIN {f} f ON d.kind = f.kind WHERE d.kind IS NULL GROUP BY f.kind",
    v112_sq_fj_coalesce => "SELECT d.kind, COUNT(*) AS c, SUM(f.v) AS s FROM {f} f FULL OUTER JOIN {d} d ON d.kind = f.kind WHERE COALESCE(f.v, 1) > 0 GROUP BY d.kind",
    v112_sq_fj_pt_only_null_fact => "SELECT d.kind, d.label FROM {d} d FULL JOIN {f} f ON d.kind = f.kind WHERE f.kind IS NULL",
}

v112_sq_cells! { V112_SQ_NULL_EXTENDED, None, true;
    v112_sq_rj_admits_null_deferred => "SELECT d.kind, COUNT(*) AS c, SUM(f.v) AS s FROM {f} f RIGHT JOIN {d} d ON d.kind = f.kind WHERE f.v > 0 OR f.v IS NULL GROUP BY d.kind",
    v112_sq_rj_coalesce_deferred => "SELECT d.kind, COUNT(*) AS c, SUM(f.v) AS s FROM {f} f RIGHT JOIN {d} d ON d.kind = f.kind WHERE COALESCE(f.v, 1) > 0 GROUP BY d.kind",
    v112_sq_rj_rejects_null_deferred => "SELECT d.kind, COUNT(*) AS c, SUM(f.v) AS s FROM {f} f RIGHT JOIN {d} d ON d.kind = f.kind WHERE f.v > 0 GROUP BY d.kind",
    v112_sq_rj_pt_only_null_deferred => "SELECT d.kind, d.label FROM {f} f RIGHT JOIN {d} d ON d.kind = f.kind WHERE f.kind IS NULL",
    v112_sq_fj_admits_null_deferred => "SELECT d.kind, COUNT(*) AS c, SUM(f.v) AS s FROM {d} d FULL JOIN {f} f ON d.kind = f.kind WHERE f.v > 0 OR f.v IS NULL GROUP BY d.kind",
    v112_sq_fj_only_null_fact_deferred => "SELECT d.kind, COUNT(*) AS c FROM {d} d FULL JOIN {f} f ON d.kind = f.kind WHERE f.kind IS NULL GROUP BY d.kind",
    v112_sq_fj_only_null_dim_deferred => "SELECT f.kind, COUNT(*) AS c, SUM(f.v) AS s FROM {d} d FULL JOIN {f} f ON d.kind = f.kind WHERE d.kind IS NULL GROUP BY f.kind",
    v112_sq_fj_dim_admits_null_deferred => "SELECT f.kind, COUNT(*) AS c, COUNT(d.label) AS n FROM {d} d FULL JOIN {f} f ON d.kind = f.kind WHERE d.label <> 'BB' OR d.label IS NULL GROUP BY f.kind",
    v112_sq_lj_pt_only_null_deferred => "SELECT d.kind, d.label FROM {d} d LEFT JOIN {f} f ON d.kind = f.kind WHERE f.kind IS NULL",
    v112_sq_lj_coalesce_deferred => "SELECT d.kind, COUNT(*) AS c, SUM(f.v) AS s FROM {d} d LEFT JOIN {f} f ON d.kind = f.kind WHERE COALESCE(f.v, 1) > 0 GROUP BY d.kind",
}

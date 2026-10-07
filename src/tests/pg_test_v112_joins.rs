fn v112_j_probe(body: &str) -> String {
    Spi::run(&format!(
        "DO $v112j$ DECLARE r text; BEGIN {body}; \
         PERFORM set_config('v112_j.out', COALESCE(r, 'OK'), true); \
         EXCEPTION WHEN OTHERS THEN \
         PERFORM set_config('v112_j.out', 'RAISED: ' || SQLERRM, true); END $v112j$"
    ))
    .expect("probe");
    Spi::get_one::<String>("SELECT current_setting('v112_j.out')")
        .expect("probe result")
        .unwrap_or_default()
}

fn v112_j_try(select: &str) -> String {
    v112_j_probe(&format!("EXECUTE $v112q${select}$v112q$ INTO r"))
}

fn v112_j_try_dml(dml: &str) -> String {
    v112_j_probe(&format!("EXECUTE $v112q${dml}$v112q$"))
}

fn v112_j_oracle(imv: &str, fresh_sql: &str) -> String {
    v112_j_try(&format!(
        "SELECT COUNT(*)::text FROM (\
            (SELECT * FROM {imv} EXCEPT ALL SELECT * FROM ({fresh_sql}) AS __fresh1) \
            UNION ALL \
            (SELECT * FROM ({fresh_sql}) AS __fresh2 EXCEPT ALL SELECT * FROM {imv}) \
         ) __oracle"
    ))
}

/// One DML step: its label, the placeholder of the table it writes, the statement.
type V112JStep = (&'static str, &'static str, &'static str);

struct V112JFixture {
    /// `(placeholder, bare table name, column list, seed rows)`.
    tables: &'static [(&'static str, &'static str, &'static str, &'static str)],
    steps: &'static [V112JStep],
}

/// A fact table, a dimension keyed by `kind` and a second dimension, seeded so
/// both sides of every join hold matched and unmatched rows.
const V112_J_STAR: V112JFixture = V112JFixture {
    tables: &[
        (
            "{f}",
            "fact",
            "id SERIAL PRIMARY KEY, kind TEXT, v INT",
            "INSERT INTO {f}(kind, v) VALUES ('a',1),('b',2),('c',3),('a',4),(NULL,5)",
        ),
        (
            "{d}",
            "dim",
            "kind TEXT PRIMARY KEY, label TEXT",
            "INSERT INTO {d} VALUES ('a','A'),('b','B'),('u','U')",
        ),
        (
            "{g}",
            "grp",
            "kind TEXT PRIMARY KEY, w INT",
            "INSERT INTO {g} VALUES ('a',10),('b',20),('q',30)",
        ),
    ],
    steps: &[
        ("f_ins", "{f}", "INSERT INTO {f}(kind, v) VALUES ('a',10),('z',20),('u',30)"),
        ("f_upd", "{f}", "UPDATE {f} SET v = v + 100 WHERE id = 1"),
        ("f_move", "{f}", "UPDATE {f} SET kind = 'q' WHERE id = 2"),
        ("f_del", "{f}", "DELETE FROM {f} WHERE id IN (3, 4)"),
        ("d_ins", "{d}", "INSERT INTO {d} VALUES ('c','C'),('w','W'),('z','A')"),
        ("d_upd", "{d}", "UPDATE {d} SET label = 'BB' WHERE kind = 'b'"),
        ("d_move", "{d}", "UPDATE {d} SET kind = 'q' WHERE kind = 'w'"),
        ("d_del", "{d}", "DELETE FROM {d} WHERE kind IN ('a', 'c')"),
        ("g_ins", "{g}", "INSERT INTO {g} VALUES ('z',40),('u',50)"),
        ("g_upd", "{g}", "UPDATE {g} SET w = w + 1 WHERE kind = 'q'"),
        ("g_move", "{g}", "UPDATE {g} SET kind = 'x' WHERE kind = 'b'"),
        ("g_del", "{g}", "DELETE FROM {g} WHERE kind = 'z'"),
        ("f_ins2", "{f}", "INSERT INTO {f}(kind, v) VALUES ('a',7),('q',8)"),
        ("f_empty", "{f}", "DELETE FROM {f}"),
        ("d_empty", "{d}", "DELETE FROM {d}"),
        ("f_refill", "{f}", "INSERT INTO {f}(kind, v) VALUES ('a',1),('q',2)"),
        ("d_refill", "{d}", "INSERT INTO {d} VALUES ('a','A'),('k','K')"),
    ],
};

#[derive(Clone, Copy)]
enum V112JNaming {
    Public,
    OwnSchema,
}

fn v112_j_table(tag: &str, naming: V112JNaming, bare: &str) -> String {
    match naming {
        V112JNaming::Public => format!("v112_j_{tag}_{bare}"),
        V112JNaming::OwnSchema => format!("v112_j_{tag}_s.{bare}"),
    }
}

fn v112_j_resolve(
    template: &str,
    fixture: &V112JFixture,
    tag: &str,
    naming: V112JNaming,
) -> String {
    fixture
        .tables
        .iter()
        .fold(template.to_string(), |sql, (placeholder, bare, _, _)| {
            sql.replace(placeholder, &v112_j_table(tag, naming, bare))
        })
}

/// Creates the IMV over a fresh fixture, then runs every DML step that writes a
/// table of the query, comparing the IMV with the query after each. Returns
/// "ok" when every comparison matched, otherwise the per-step outcomes.
fn v112_j_cell(
    fixture: &V112JFixture,
    tag: &str,
    naming: V112JNaming,
    select_template: &str,
    deferred: bool,
) -> String {
    if let V112JNaming::OwnSchema = naming {
        Spi::run(&format!("CREATE SCHEMA v112_j_{tag}_s")).expect("schema");
    }
    for (placeholder, _, columns, seed) in fixture.tables {
        let table = v112_j_resolve(placeholder, fixture, tag, naming);
        Spi::run(&format!("CREATE TABLE {table}({columns})")).expect("create table");
        Spi::run(&v112_j_resolve(seed, fixture, tag, naming)).expect("seed");
    }
    let imv = format!("v112_j_{tag}");
    let fresh = v112_j_resolve(select_template, fixture, tag, naming);
    let mode = if deferred { "DEFERRED" } else { "IMMEDIATE" };
    let created = v112_j_try(&format!(
        "SELECT create_reflex_ivm('{imv}', $v112sel${fresh}$v112sel$, NULL, 'UNLOGGED', '{mode}')"
    ));
    if created != "CREATE REFLEX INCREMENTAL VIEW" {
        return format!("NOT-CREATED {created}");
    }
    let mut outcomes = vec![("create".to_string(), v112_j_oracle(&imv, &fresh))];
    for (label, written, dml) in fixture.steps {
        if !select_template.contains(written) {
            continue;
        }
        let ran = v112_j_try_dml(&v112_j_resolve(dml, fixture, tag, naming));
        if ran != "OK" {
            outcomes.push((label.to_string(), ran));
            break;
        }
        if deferred {
            let table = v112_j_resolve(written, fixture, tag, naming);
            let flushed = v112_j_try(&format!("SELECT reflex_flush_deferred('{table}')"));
            if flushed.starts_with("RAISED") || flushed.starts_with("ERROR") {
                outcomes.push((label.to_string(), flushed));
                break;
            }
        }
        outcomes.push((label.to_string(), v112_j_oracle(&imv, &fresh)));
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

macro_rules! v112_j_cells {
    ($fixture:expr, $naming:expr, $deferred:expr; $($name:ident => $select:expr,)+) => {
        $(
            #[pg_test]
            fn $name() {
                let tag = stringify!($name).trim_start_matches("v112_j_");
                assert_eq!(
                    v112_j_cell(&$fixture, tag, $naming, $select, $deferred),
                    "ok",
                    "{}",
                    $select
                );
            }
        )+
    };
}

// ---------------------------------------------------------------------------
// RIGHT / FULL JOIN: a write to either side, grouped by a column of either side
// ---------------------------------------------------------------------------

v112_j_cells! { V112_J_STAR, V112JNaming::Public, false;
    v112_j_rj_fd_by_d => "SELECT d.label, COUNT(*) AS c, SUM(f.v) AS s FROM {f} f RIGHT JOIN {d} d ON d.kind = f.kind GROUP BY d.label",
    v112_j_rj_df_by_d => "SELECT d.label, COUNT(*) AS c, SUM(f.v) AS s FROM {d} d RIGHT JOIN {f} f ON d.kind = f.kind GROUP BY d.label",
    v112_j_rj_fd_by_f => "SELECT f.kind, COUNT(*) AS c, SUM(f.v) AS s FROM {f} f RIGHT JOIN {d} d ON d.kind = f.kind GROUP BY f.kind",
    v112_j_rj_df_by_f => "SELECT f.kind, COUNT(*) AS c, COUNT(d.label) AS n FROM {d} d RIGHT JOIN {f} f ON d.kind = f.kind GROUP BY f.kind",
    v112_j_rj_fd_by_dkey => "SELECT d.kind, COUNT(*) AS c, SUM(f.v) AS s FROM {f} f RIGHT JOIN {d} d ON d.kind = f.kind GROUP BY d.kind",
    v112_j_rj_fd_by_both => "SELECT d.label, f.kind, COUNT(*) AS c, SUM(f.v) AS s FROM {f} f RIGHT JOIN {d} d ON d.kind = f.kind GROUP BY d.label, f.kind",
    v112_j_rj_fd_ungrouped => "SELECT COUNT(*) AS c, SUM(f.v) AS s, COUNT(d.label) AS n FROM {f} f RIGHT JOIN {d} d ON d.kind = f.kind",
    v112_j_rj_fd_minmax => "SELECT d.label, MIN(f.v) AS mn, MAX(f.v) AS mx, COUNT(f.id) AS c FROM {f} f RIGHT JOIN {d} d ON d.kind = f.kind GROUP BY d.label",
    v112_j_rj_outer_fd => "SELECT d.label, COUNT(*) AS c, SUM(f.v) AS s FROM {f} f RIGHT OUTER JOIN {d} d ON d.kind = f.kind GROUP BY d.label",
    v112_j_rj_outer_df => "SELECT f.kind, COUNT(*) AS c, COUNT(d.label) AS n FROM {d} d RIGHT OUTER JOIN {f} f ON d.kind = f.kind GROUP BY f.kind",
    v112_j_rj_unaliased => "SELECT {d}.label, COUNT(*) AS c, SUM({f}.v) AS s FROM {f} RIGHT JOIN {d} ON {d}.kind = {f}.kind GROUP BY {d}.label",
    v112_j_rj_lowercase => "select d.label, count(*) as c, sum(f.v) as s from {f} f right join {d} d on d.kind = f.kind group by d.label",
    v112_j_fj_fd_by_d => "SELECT d.label, COUNT(*) AS c, SUM(f.v) AS s FROM {f} f FULL JOIN {d} d ON d.kind = f.kind GROUP BY d.label",
    v112_j_fj_fd_by_f => "SELECT f.kind, COUNT(*) AS c, COUNT(d.label) AS n FROM {f} f FULL OUTER JOIN {d} d ON d.kind = f.kind GROUP BY f.kind",
    v112_j_lj_df_by_d => "SELECT d.label, COUNT(*) AS c, SUM(f.v) AS s FROM {d} d LEFT JOIN {f} f ON d.kind = f.kind GROUP BY d.label",
    v112_j_lj_df_by_f => "SELECT f.kind, COUNT(*) AS c, COUNT(d.label) AS n FROM {d} d LEFT JOIN {f} f ON d.kind = f.kind GROUP BY f.kind",
    v112_j_rj_pt_fd => "SELECT f.id, f.v, d.kind, d.label FROM {f} f RIGHT JOIN {d} d ON d.kind = f.kind",
    v112_j_rj_pt_df => "SELECT f.id, f.v, f.kind AS fkind, d.label FROM {d} d RIGHT JOIN {f} f ON d.kind = f.kind",
    v112_j_rj_outer_pt_fd => "SELECT f.id, f.v, d.kind, d.label FROM {f} f RIGHT OUTER JOIN {d} d ON d.kind = f.kind",
    v112_j_fj_pt_fd => "SELECT f.id, f.v, d.kind, d.label FROM {f} f FULL JOIN {d} d ON d.kind = f.kind",
    v112_j_rj3_fd_inner_g => "SELECT d.label, COUNT(*) AS c, SUM(f.v) AS s, SUM(g.w) AS w FROM {f} f RIGHT JOIN {d} d ON d.kind = f.kind JOIN {g} g ON g.kind = d.kind GROUP BY d.label",
    v112_j_rj3_fg_right_d => "SELECT d.label, COUNT(*) AS c, SUM(f.v) AS s, SUM(g.w) AS w FROM {f} f JOIN {g} g ON g.kind = f.kind RIGHT JOIN {d} d ON d.kind = f.kind GROUP BY d.label",
    v112_j_rj3_fd_left_g => "SELECT d.label, COUNT(*) AS c, SUM(f.v) AS s, SUM(g.w) AS w FROM {f} f RIGHT JOIN {d} d ON d.kind = f.kind LEFT JOIN {g} g ON g.kind = d.kind GROUP BY d.label",
    v112_j_rj3_gf_left_right_d => "SELECT d.label, COUNT(*) AS c, SUM(f.v) AS s, SUM(g.w) AS w FROM {g} g LEFT JOIN {f} f ON g.kind = f.kind RIGHT JOIN {d} d ON d.kind = g.kind GROUP BY d.label",
    v112_j_rj3_double_right => "SELECT g.kind, COUNT(*) AS c, SUM(f.v) AS s, COUNT(d.label) AS n FROM {f} f RIGHT JOIN {d} d ON d.kind = f.kind RIGHT JOIN {g} g ON g.kind = d.kind GROUP BY g.kind",
    v112_j_rj3_pt_fd_inner_g => "SELECT f.id, f.v, d.kind, d.label, g.w FROM {f} f RIGHT JOIN {d} d ON d.kind = f.kind JOIN {g} g ON g.kind = d.kind",
    v112_j_rj3_pt_fg_right_d => "SELECT f.id, f.v, d.kind, d.label, g.w FROM {f} f JOIN {g} g ON g.kind = f.kind RIGHT JOIN {d} d ON d.kind = f.kind",
    v112_j_rj_paren_nullable => "SELECT d.label, COUNT(*) AS c, SUM(f.v) AS s, SUM(g.w) AS w FROM ({f} f JOIN {g} g ON g.kind = f.kind) RIGHT JOIN {d} d ON d.kind = f.kind GROUP BY d.label",
}

v112_j_cells! { V112_J_STAR, V112JNaming::Public, true;
    v112_j_rj_fd_by_d_deferred => "SELECT d.label, COUNT(*) AS c, SUM(f.v) AS s FROM {f} f RIGHT JOIN {d} d ON d.kind = f.kind GROUP BY d.label",
    v112_j_rj_df_by_d_deferred => "SELECT d.label, COUNT(*) AS c, SUM(f.v) AS s FROM {d} d RIGHT JOIN {f} f ON d.kind = f.kind GROUP BY d.label",
    v112_j_rj_fd_by_f_deferred => "SELECT f.kind, COUNT(*) AS c, SUM(f.v) AS s FROM {f} f RIGHT JOIN {d} d ON d.kind = f.kind GROUP BY f.kind",
    v112_j_rj_outer_fd_deferred => "SELECT d.label, COUNT(*) AS c, SUM(f.v) AS s FROM {f} f RIGHT OUTER JOIN {d} d ON d.kind = f.kind GROUP BY d.label",
    v112_j_fj_fd_by_d_deferred => "SELECT d.label, COUNT(*) AS c, SUM(f.v) AS s FROM {f} f FULL JOIN {d} d ON d.kind = f.kind GROUP BY d.label",
    v112_j_rj_pt_fd_deferred => "SELECT f.id, f.v, d.kind, d.label FROM {f} f RIGHT JOIN {d} d ON d.kind = f.kind",
    v112_j_rj_pt_df_deferred => "SELECT f.id, f.v, f.kind AS fkind, d.label FROM {d} d RIGHT JOIN {f} f ON d.kind = f.kind",
    v112_j_rj3_fd_inner_g_deferred => "SELECT d.label, COUNT(*) AS c, SUM(f.v) AS s, SUM(g.w) AS w FROM {f} f RIGHT JOIN {d} d ON d.kind = f.kind JOIN {g} g ON g.kind = d.kind GROUP BY d.label",
    v112_j_rj3_fg_right_d_deferred => "SELECT d.label, COUNT(*) AS c, SUM(f.v) AS s, SUM(g.w) AS w FROM {f} f JOIN {g} g ON g.kind = f.kind RIGHT JOIN {d} d ON d.kind = f.kind GROUP BY d.label",
}

v112_j_cells! { V112_J_STAR, V112JNaming::OwnSchema, false;
    v112_j_rj_schema_fd_by_d => "SELECT d.label, COUNT(*) AS c, SUM(f.v) AS s FROM {f} f RIGHT JOIN {d} d ON d.kind = f.kind GROUP BY d.label",
    v112_j_rj_schema_df_by_d => "SELECT d.label, COUNT(*) AS c, SUM(f.v) AS s FROM {d} d RIGHT JOIN {f} f ON d.kind = f.kind GROUP BY d.label",
    v112_j_rj_schema_fd_by_f => "SELECT f.kind, COUNT(*) AS c, SUM(f.v) AS s FROM {f} f RIGHT JOIN {d} d ON d.kind = f.kind GROUP BY f.kind",
    v112_j_rj_schema_outer_fd => "SELECT d.label, COUNT(*) AS c, SUM(f.v) AS s FROM {f} f RIGHT OUTER JOIN {d} d ON d.kind = f.kind GROUP BY d.label",
    v112_j_rj_schema_unaliased => "SELECT {d}.label, COUNT(*) AS c, SUM({f}.v) AS s FROM {f} RIGHT JOIN {d} ON {d}.kind = {f}.kind GROUP BY {d}.label",
    v112_j_fj_schema_fd_by_d => "SELECT d.label, COUNT(*) AS c, SUM(f.v) AS s FROM {f} f FULL JOIN {d} d ON d.kind = f.kind GROUP BY d.label",
    v112_j_rj_schema_pt_fd => "SELECT f.id, f.v, d.kind, d.label FROM {f} f RIGHT JOIN {d} d ON d.kind = f.kind",
    v112_j_rj3_schema_fd_inner_g => "SELECT d.label, COUNT(*) AS c, SUM(f.v) AS s, SUM(g.w) AS w FROM {f} f RIGHT JOIN {d} d ON d.kind = f.kind JOIN {g} g ON g.kind = d.kind GROUP BY d.label",
    v112_j_rj3_schema_fg_right_d => "SELECT d.label, COUNT(*) AS c, SUM(f.v) AS s, SUM(g.w) AS w FROM {f} f JOIN {g} g ON g.kind = f.kind RIGHT JOIN {d} d ON d.kind = f.kind GROUP BY d.label",
}

v112_j_cells! { V112_J_STAR, V112JNaming::OwnSchema, true;
    v112_j_rj_schema_fd_by_d_deferred => "SELECT d.label, COUNT(*) AS c, SUM(f.v) AS s FROM {f} f RIGHT JOIN {d} d ON d.kind = f.kind GROUP BY d.label",
    v112_j_rj_schema_pt_fd_deferred => "SELECT f.id, f.v, d.kind, d.label FROM {f} f RIGHT JOIN {d} d ON d.kind = f.kind",
}

v112_j_cells! { V112_J_STAR, V112JNaming::Public, false;
    v112_j_lj_pt_df => "SELECT f.id, f.v, d.kind, d.label FROM {d} d LEFT JOIN {f} f ON d.kind = f.kind",
}

/// An UPDATE that brings rows of `{f}` into a WHERE that null-extended rows
/// pass too. `forced` selects the bulk path of the UPDATE trigger: a zero wipe
/// threshold over an analysed intermediate.
fn v112_j_bulk_filter_entering_update_cell(tag: &str, from_template: &str, forced: bool) -> String {
    let fact = format!("v112_j_{tag}_fact");
    let dim = format!("v112_j_{tag}_dim");
    Spi::run(&format!("CREATE TABLE {fact}(kind TEXT PRIMARY KEY, v INT)")).expect("fact");
    Spi::run(&format!("CREATE TABLE {dim}(kind TEXT PRIMARY KEY, label TEXT)")).expect("dim");
    Spi::run(&format!(
        "INSERT INTO {fact} VALUES ('a',1),('b',-2),('c',-3),('z',-4)"
    ))
    .expect("seed fact");
    Spi::run(&format!(
        "INSERT INTO {dim} VALUES ('a','A'),('b','B'),('c','C'),('u','U')"
    ))
    .expect("seed dim");
    let imv = format!("v112_j_{tag}");
    let query = format!(
        "SELECT d.kind, COUNT(*) AS c, SUM(f.v) AS s FROM {} WHERE f.v > 0 OR f.v IS NULL GROUP BY d.kind",
        from_template.replace("{f}", &fact).replace("{d}", &dim)
    );
    let created = v112_j_try(&format!(
        "SELECT create_reflex_ivm('{imv}', $v112sel${query}$v112sel$)"
    ));
    if created != "CREATE REFLEX INCREMENTAL VIEW" {
        return format!("NOT-CREATED {created}");
    }
    let intermediate = Spi::get_one::<String>(&format!(
        "SELECT reflex_intermediate_table_name('{imv}')"
    ))
    .expect("intermediate name")
    .expect("intermediate name");
    if forced {
        Spi::run(&format!("ANALYZE {intermediate}")).expect("analyze");
        Spi::run("SET LOCAL reflex.wipe_threshold = '0'").expect("threshold");
    }
    let mut outcomes = vec![("create".to_string(), v112_j_oracle(&imv, &query))];
    let steps = [
        ("enter", format!("UPDATE {fact} SET v = 5 WHERE kind IN ('b', 'c')")),
        ("enter_unmatched", format!("UPDATE {fact} SET v = 6 WHERE kind = 'z'")),
        ("leave", format!("UPDATE {fact} SET v = -1 WHERE kind = 'a'")),
        ("reenter", format!("UPDATE {fact} SET v = 9 WHERE kind = 'a'")),
    ];
    for (label, dml) in steps {
        let ran = v112_j_try_dml(&dml);
        if ran != "OK" {
            outcomes.push((label.to_string(), ran));
            break;
        }
        outcomes.push((label.to_string(), v112_j_oracle(&imv, &query)));
    }
    Spi::run("RESET reflex.wipe_threshold").expect("reset");
    if outcomes.iter().all(|(_, outcome)| outcome == "0") {
        return "ok".to_string();
    }
    outcomes
        .iter()
        .map(|(label, outcome)| format!("{label}={outcome}"))
        .collect::<Vec<_>>()
        .join(" ")
}

#[pg_test]
fn v112_j_bulk_filter_entering_update_right_join_null_extended_table() {
    assert_eq!(
        v112_j_bulk_filter_entering_update_cell("bfe_rj", "{f} f RIGHT JOIN {d} d ON d.kind = f.kind", true),
        "ok"
    );
}

#[pg_test]
fn v112_j_bulk_filter_entering_update_inner_join() {
    assert_eq!(
        v112_j_bulk_filter_entering_update_cell("bfe_ij", "{d} d JOIN {f} f ON d.kind = f.kind", true),
        "ok"
    );
}

#[pg_test]
fn v112_j_filter_entering_update_right_join_unforced() {
    assert_eq!(
        v112_j_bulk_filter_entering_update_cell(
            "fe_rj",
            "{f} f RIGHT JOIN {d} d ON d.kind = f.kind",
            false
        ),
        "ok"
    );
}

/// A passthrough keyed by the join column, its join keyword split over two
/// lines: the stored query is the caller's text, whitespace included.
fn v112_j_keyed_passthrough_cell(tag: &str, from_template: &str) -> String {
    let fact = format!("v112_j_{tag}_fact");
    let dim = format!("v112_j_{tag}_dim");
    Spi::run(&format!("CREATE TABLE {fact}(kind TEXT PRIMARY KEY, v INT)")).expect("fact");
    Spi::run(&format!("CREATE TABLE {dim}(kind TEXT PRIMARY KEY, label TEXT)")).expect("dim");
    Spi::run(&format!("INSERT INTO {fact} VALUES ('a',1),('b',2),('c',3),('z',4)")).expect("seed");
    Spi::run(&format!(
        "INSERT INTO {dim} VALUES ('a','A'),('b','B'),('c','C'),('u','U')"
    ))
    .expect("seed");
    let imv = format!("v112_j_{tag}");
    let query = format!(
        "SELECT d.kind, d.label, f.v FROM {}",
        from_template.replace("{f}", &fact).replace("{d}", &dim)
    );
    let created = v112_j_try(&format!(
        "SELECT create_reflex_ivm('{imv}', $v112sel${query}$v112sel$, 'kind')"
    ));
    if created != "CREATE REFLEX INCREMENTAL VIEW" {
        return format!("NOT-CREATED {created}");
    }
    let mut outcomes = vec![("create".to_string(), v112_j_oracle(&imv, &query))];
    let steps = [
        ("f_ins", format!("INSERT INTO {fact} VALUES ('u',7),('y',8)")),
        ("f_upd", format!("UPDATE {fact} SET v = v + 10 WHERE kind = 'a'")),
        ("f_del", format!("DELETE FROM {fact} WHERE kind = 'b'")),
        ("d_ins", format!("INSERT INTO {dim} VALUES ('z','Z'),('w','W')")),
        ("d_upd", format!("UPDATE {dim} SET label = 'CC' WHERE kind = 'c'")),
        ("d_del", format!("DELETE FROM {dim} WHERE kind IN ('a', 'w')")),
    ];
    for (label, dml) in steps {
        let ran = v112_j_try_dml(&dml);
        if ran != "OK" {
            outcomes.push((label.to_string(), ran));
            break;
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

macro_rules! v112_j_keyed_passthrough_cells {
    ($($name:ident => $from:expr,)+) => {
        $(
            #[pg_test]
            fn $name() {
                let tag = stringify!($name).trim_start_matches("v112_j_");
                assert_eq!(v112_j_keyed_passthrough_cell(tag, $from), "ok", "{}", $from);
            }
        )+
    };
}

v112_j_keyed_passthrough_cells! {
    v112_j_kp_left_join => "{d} d LEFT JOIN {f} f ON d.kind = f.kind",
    v112_j_kp_left_join_on_two_lines => "{d} d LEFT\n    JOIN {f} f ON d.kind = f.kind",
    v112_j_kp_left_outer_join_tabbed => "{d} d left\touter\tjoin {f} f ON d.kind = f.kind",
    v112_j_kp_right_join => "{f} f RIGHT JOIN {d} d ON d.kind = f.kind",
    v112_j_kp_right_join_on_two_lines => "{f} f RIGHT\n    JOIN {d} d ON d.kind = f.kind",
    v112_j_kp_right_outer_join_tabbed => "{f} f right\touter\tjoin {d} d ON d.kind = f.kind",
}

/// The bulk path of an UPDATE that brings rows of the table after a RIGHT JOIN
/// into the WHERE filter, forced by a zero wipe threshold over an analysed
/// intermediate. Their group key is read from the null-extended table, so the
/// unmatched ones land in the NULL group that already exists.
#[pg_test]
fn v112_j_bulk_filter_entering_update_right_join_preserved_table() {
    Spi::run("CREATE TABLE v112_j_bfp_fact(kind TEXT PRIMARY KEY, v INT)").expect("fact");
    Spi::run("CREATE TABLE v112_j_bfp_dim(kind TEXT PRIMARY KEY, label TEXT)").expect("dim");
    Spi::run("INSERT INTO v112_j_bfp_fact VALUES ('a',1),('b',2),('z',4)").expect("seed");
    Spi::run(
        "INSERT INTO v112_j_bfp_dim VALUES ('a','A'),('b','B'),('u','U'),('p','x'),('q','x'),('z','x')",
    )
    .expect("seed");
    let query = "SELECT f.kind, COUNT(*) AS c, COUNT(d.label) AS n FROM v112_j_bfp_fact f \
                 RIGHT JOIN v112_j_bfp_dim d ON d.kind = f.kind WHERE d.label <> 'x' GROUP BY f.kind";
    assert_eq!(
        crate::create_reflex_ivm("v112_j_bfp", query, None, None, None, None),
        "CREATE REFLEX INCREMENTAL VIEW"
    );
    assert_imv_correct("v112_j_bfp", query);
    let intermediate =
        Spi::get_one::<String>("SELECT reflex_intermediate_table_name('v112_j_bfp')")
            .expect("intermediate name")
            .expect("intermediate name");
    Spi::run(&format!("ANALYZE {intermediate}")).expect("analyze");
    Spi::run("SET LOCAL reflex.wipe_threshold = '0'").expect("threshold");
    let entered = v112_j_try_dml("UPDATE v112_j_bfp_dim SET label = 'Y' WHERE label = 'x'");
    Spi::run("RESET reflex.wipe_threshold").expect("reset");
    assert_eq!(entered, "OK");
    assert_imv_correct("v112_j_bfp", query);
}

/// The fixture of the derived-table report: `{f}` groups joined to a filtered
/// `{d}`, several `{d}` rows per group.
const V112_J_DERIVED: V112JFixture = V112JFixture {
    tables: &[
        (
            "{f}",
            "fact",
            "id SERIAL PRIMARY KEY, g TEXT, city TEXT, v INT",
            "INSERT INTO {f}(g, city, v) SELECT 'g' || (i % 3), \
             CASE WHEN i % 2 = 0 THEN 'Lyon' ELSE 'Paris' END, i FROM generate_series(1, 30) i",
        ),
        (
            "{d}",
            "dim",
            "id SERIAL PRIMARY KEY, g TEXT, w INT",
            "INSERT INTO {d}(g, w) VALUES ('g0',5),('g1',7),('g1',3),('g1',-4),('g9',1)",
        ),
        (
            "{g}",
            "grp",
            "g TEXT PRIMARY KEY, z INT",
            "INSERT INTO {g} VALUES ('g0',1),('g1',2),('g7',3)",
        ),
    ],
    steps: &[
        ("d_del", "{d}", "DELETE FROM {d} WHERE w = 3"),
        ("d_ins", "{d}", "INSERT INTO {d}(g, w) VALUES ('g2',9),('g1',1),('g8',2),('g0',-1)"),
        ("d_upd", "{d}", "UPDATE {d} SET w = w + 10 WHERE g = 'g1'"),
        ("d_move", "{d}", "UPDATE {d} SET g = 'g2' WHERE g = 'g0'"),
        ("d_filter", "{d}", "UPDATE {d} SET w = -w WHERE g = 'g2'"),
        ("f_ins", "{f}", "INSERT INTO {f}(g, city, v) VALUES ('g9','Nice',100),('g1','Nice',200)"),
        ("f_upd", "{f}", "UPDATE {f} SET v = v + 1000 WHERE id = 3"),
        ("f_move", "{f}", "UPDATE {f} SET g = 'g8' WHERE id IN (1, 2)"),
        ("f_del", "{f}", "DELETE FROM {f} WHERE g = 'g2'"),
        ("g_ins", "{g}", "INSERT INTO {g} VALUES ('g2',4),('g8',5)"),
        ("g_upd", "{g}", "UPDATE {g} SET z = z + 1 WHERE g = 'g1'"),
        ("g_del", "{g}", "DELETE FROM {g} WHERE g = 'g1'"),
        ("d_empty", "{d}", "DELETE FROM {d}"),
        ("d_refill", "{d}", "INSERT INTO {d}(g, w) VALUES ('g1',2),('g8',6)"),
        ("f_empty", "{f}", "DELETE FROM {f}"),
    ],
};

// ---------------------------------------------------------------------------
// Outer join onto a derived table or a parenthesised join: a write to a table
// inside it
// ---------------------------------------------------------------------------

v112_j_cells! { V112_J_DERIVED, V112JNaming::Public, false;
    v112_j_dt_lj_minmax_by_f => "SELECT s.g, MIN(d.w) AS mn, MAX(s.v) AS mx FROM {f} s LEFT JOIN (SELECT g, w FROM {d} WHERE w > 0) d ON d.g = s.g GROUP BY s.g",
    v112_j_dt_lj_sum_by_f => "SELECT s.g, COUNT(*) AS c, SUM(d.w) AS w, SUM(s.v) AS v FROM {f} s LEFT JOIN (SELECT g, w FROM {d} WHERE w > 0) d ON d.g = s.g GROUP BY s.g",
    v112_j_dt_lj_sum_by_d => "SELECT d.g, COUNT(*) AS c, SUM(d.w) AS w, SUM(s.v) AS v FROM {f} s LEFT JOIN (SELECT g, w FROM {d} WHERE w > 0) d ON d.g = s.g GROUP BY d.g",
    v112_j_dt_lj_outer_sum => "SELECT s.g, COUNT(*) AS c, SUM(d.w) AS w FROM {f} s LEFT OUTER JOIN (SELECT g, w FROM {d} WHERE w > 0) d ON d.g = s.g GROUP BY s.g",
    v112_j_dt_lj_ungrouped => "SELECT COUNT(*) AS c, SUM(d.w) AS w, SUM(s.v) AS v FROM {f} s LEFT JOIN (SELECT g, w FROM {d} WHERE w > 0) d ON d.g = s.g",
    v112_j_dt_lj_pt => "SELECT s.id, s.g, s.v, d.w FROM {f} s LEFT JOIN (SELECT g, w FROM {d} WHERE w > 0) d ON d.g = s.g",
    v112_j_dt_rj_sum_by_f => "SELECT s.g, COUNT(*) AS c, SUM(d.w) AS w, SUM(s.v) AS v FROM (SELECT g, w FROM {d} WHERE w > 0) d RIGHT JOIN {f} s ON d.g = s.g GROUP BY s.g",
    v112_j_dt_rj_derived_preserved => "SELECT d.g, COUNT(*) AS c, SUM(d.w) AS w, SUM(s.v) AS v FROM {f} s RIGHT JOIN (SELECT g, w FROM {d} WHERE w > 0) d ON d.g = s.g GROUP BY d.g",
    v112_j_dt_fj_sum => "SELECT s.g, COUNT(*) AS c, SUM(d.w) AS w FROM {f} s FULL JOIN (SELECT g, w FROM {d} WHERE w > 0) d ON d.g = s.g GROUP BY s.g",
    v112_j_dt_inner_sum => "SELECT s.g, COUNT(*) AS c, SUM(d.w) AS w, SUM(s.v) AS v FROM {f} s JOIN (SELECT g, w FROM {d} WHERE w > 0) d ON d.g = s.g GROUP BY s.g",
    v112_j_dt_lj_paren_join => "SELECT s.g, COUNT(*) AS c, SUM(d.w) AS w, SUM(k.z) AS z FROM {f} s LEFT JOIN ({d} d JOIN {g} k ON k.g = d.g) ON d.g = s.g GROUP BY s.g",
    v112_j_dt_lj_derived_join => "SELECT s.g, COUNT(*) AS c, SUM(d.w) AS w, SUM(d.z) AS z FROM {f} s LEFT JOIN (SELECT x.g, x.w, k.z FROM {d} x JOIN {g} k ON k.g = x.g) d ON d.g = s.g GROUP BY s.g",
    v112_j_dt_lj_then_inner => "SELECT s.g, COUNT(*) AS c, SUM(d.w) AS w, SUM(k.z) AS z FROM {f} s LEFT JOIN (SELECT g, w FROM {d} WHERE w > 0) d ON d.g = s.g JOIN {g} k ON k.g = s.g GROUP BY s.g",
}

v112_j_cells! { V112_J_DERIVED, V112JNaming::Public, true;
    v112_j_dt_lj_minmax_by_f_deferred => "SELECT s.g, MIN(d.w) AS mn, MAX(s.v) AS mx FROM {f} s LEFT JOIN (SELECT g, w FROM {d} WHERE w > 0) d ON d.g = s.g GROUP BY s.g",
    v112_j_dt_lj_sum_by_f_deferred => "SELECT s.g, COUNT(*) AS c, SUM(d.w) AS w, SUM(s.v) AS v FROM {f} s LEFT JOIN (SELECT g, w FROM {d} WHERE w > 0) d ON d.g = s.g GROUP BY s.g",
    v112_j_dt_lj_pt_deferred => "SELECT s.id, s.g, s.v, d.w FROM {f} s LEFT JOIN (SELECT g, w FROM {d} WHERE w > 0) d ON d.g = s.g",
}

v112_j_cells! { V112_J_DERIVED, V112JNaming::OwnSchema, false;
    v112_j_dt_schema_lj_sum_by_f => "SELECT s.g, COUNT(*) AS c, SUM(d.w) AS w, SUM(s.v) AS v FROM {f} s LEFT JOIN (SELECT g, w FROM {d} WHERE w > 0) d ON d.g = s.g GROUP BY s.g",
}

/// The stored query of every LEFT JOIN shape below parses and keeps its scoped
/// delta for each source: the derived-table guard recomputes only what it names.
#[pg_test]
fn v112_j_left_join_onto_a_plain_table_keeps_its_scoped_delta() {
    let shapes = [
        "SELECT d.label, COUNT(*) AS c, SUM(f.v) AS s, AVG(f.v) AS a, MIN(f.v) AS mn, MAX(f.v) AS mx FROM {d} d LEFT JOIN {f} f ON d.kind = f.kind GROUP BY d.label",
        "SELECT d.label, COUNT(*) FILTER (WHERE f.v > 1) AS c, BOOL_OR(f.v > 2) AS b FROM {d} d LEFT OUTER JOIN {f} f ON d.kind = f.kind GROUP BY d.label",
        "SELECT d.label, COUNT(DISTINCT f.v) AS c FROM {d} d LEFT JOIN {f} f ON d.kind = f.kind GROUP BY d.label",
        "SELECT upper(d.label) AS l, SUM(f.v::numeric) AS s, SUM(CASE WHEN f.v > 1 THEN f.v ELSE 0 END) AS t FROM {d} d LEFT JOIN {f} f ON d.kind = f.kind WHERE d.label LIKE '%' AND d.kind IN ('a', 'b', 'u') AND COALESCE(f.v, 0) BETWEEN 0 AND 100 GROUP BY upper(d.label)",
        "SELECT d.label, SUM(f.v) AS s FROM {d} d LEFT JOIN {f} f ON d.kind = f.kind GROUP BY d.label HAVING SUM(f.v) > 0",
        "SELECT d.label, SUM(f.v) AS s, SUM(g.w) AS w FROM {d} d JOIN {g} g ON g.kind = d.kind LEFT JOIN {f} f ON d.kind = f.kind GROUP BY d.label",
        "SELECT COUNT(*) AS c, SUM(f.v) AS s FROM {d} d LEFT JOIN {f} f ON d.kind = f.kind",
        "SELECT DISTINCT d.label, f.kind FROM {d} d LEFT JOIN {f} f ON d.kind = f.kind",
        "SELECT f.id, f.v, d.kind, d.label FROM {d} d LEFT JOIN {f} f ON d.kind = f.kind",
        "SELECT f.id, f.v, d.label\n  FROM {f} f\n  LEFT JOIN {d} d\n    ON d.kind = f.kind\n  WHERE f.v > 0",
    ];
    for (placeholder, _, columns, seed) in V112_J_STAR.tables {
        let table = v112_j_resolve(placeholder, &V112_J_STAR, "keep", V112JNaming::Public);
        Spi::run(&format!("CREATE TABLE {table}({columns})")).expect("create table");
        Spi::run(&v112_j_resolve(seed, &V112_J_STAR, "keep", V112JNaming::Public)).expect("seed");
    }
    for (position, shape) in shapes.iter().enumerate() {
        let imv = format!("v112_j_keep_{position}");
        let query = v112_j_resolve(shape, &V112_J_STAR, "keep", V112JNaming::Public);
        assert_eq!(
            crate::create_reflex_ivm(&imv, &query, None, None, None, None),
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
        assert!(sources.len() >= 2, "{query}");
        for source in sources {
            assert!(
                !crate::trigger::has_no_scoped_delta(&base_query, &source),
                "{source} of {base_query}"
            );
        }
    }
}

// A FULL JOIN passthrough's delta scoped by the COALESCE columns its first join
// equates, against the constructions where that proof must not be trusted.
// Rows are compared as text so case and blank-padding differences show.

struct FjsImv {
    name: String,
    query: String,
    /// The source whose delta is pinned, and whether it may be scoped.
    pinned: Option<(String, bool)>,
}

fn fjs_scoped(name: &str, source: &str) -> bool {
    let delta = Spi::get_one::<String>(&format!(
        "SELECT reflex_build_delta_sql(r.name, {}, 'UPDATE', r.base_query, \
           COALESCE(r.end_query, ''), r.aggregations::text, r.base_query) \
         FROM public.__reflex_ivm_reference r WHERE r.name = {}",
        sql_lit(source),
        sql_lit(name)
    ))
    .expect("delta sql")
    .unwrap_or_default();
    !delta.contains("reflex_rebuild_target_rows")
}

fn fjs_mismatches(name: &str, query: &str) -> i64 {
    Spi::get_one::<i64>(&format!(
        "SELECT count(*) FROM ((SELECT x::text FROM {name} x EXCEPT ALL SELECT x::text FROM ({query}) x) \
         UNION ALL (SELECT x::text FROM ({query}) x EXCEPT ALL SELECT x::text FROM {name} x)) o"
    ))
    .expect("oracle")
    .unwrap_or(-1)
}

fn fjs_run(mode: &str, ddl: &[String], imvs: &[FjsImv], sources: &[String], steps: &[String]) {
    for statement in ddl {
        Spi::run(statement).unwrap_or_else(|e| panic!("fixture <{statement}>: {e}"));
    }
    for imv in imvs {
        let created = Spi::get_one::<String>(&format!(
            "SELECT create_reflex_ivm({}, {}, NULL, NULL, '{mode}')",
            sql_lit(&imv.name),
            sql_lit(&imv.query)
        ))
        .expect("create")
        .unwrap_or_default();
        assert_eq!(created, "CREATE REFLEX INCREMENTAL VIEW", "{}", imv.name);
    }
    for step in steps {
        Spi::run(step).unwrap_or_else(|e| panic!("{mode} step <{step}>: {e}"));
        if mode == "DEFERRED" {
            for source in sources {
                Spi::run(&format!("SELECT reflex_flush_deferred({})", sql_lit(source)))
                    .expect("flush");
            }
        }
        for imv in imvs {
            assert_eq!(
                fjs_mismatches(&imv.name, &imv.query),
                0,
                "{mode} {} after <{step}>",
                imv.name
            );
        }
    }
    for imv in imvs {
        if let Some((source, scoped)) = &imv.pinned {
            assert_eq!(
                fjs_scoped(&imv.name, source),
                *scoped,
                "{}: a write to {source} should {}be applied to the rows of its values only",
                imv.name,
                if *scoped { "" } else { "not " }
            );
        }
    }
}

fn fjs_lines(lines: &[&str], p: &str) -> Vec<String> {
    lines.iter().map(|line| line.replace("{p}", p)).collect()
}

/// The FULL-JOINed source shares its bare name with a LEFT-JOINed table of
/// another schema, joined on another column: neither may be read as the other.
fn fjs_same_bare_name(p: &str, mode: &str, f_schema: &str, g_schema: &str) {
    let ddl = vec![
        format!("CREATE SCHEMA IF NOT EXISTS {p}_s"),
        format!("CREATE TABLE {f_schema}.{p}_t (id INT PRIMARY KEY, k INT, j INT, q INT)"),
        format!("CREATE TABLE {g_schema}.{p}_t (id INT PRIMARY KEY, k INT, j INT, q INT)"),
        format!("CREATE TABLE {p}_h (id INT PRIMARY KEY, k INT, s INT)"),
        format!("INSERT INTO {f_schema}.{p}_t VALUES (1, 1, 0, 1), (2, 2, 0, 2)"),
        format!("INSERT INTO {p}_h VALUES (1, 2, 20), (2, 3, 30)"),
        format!("INSERT INTO {g_schema}.{p}_t VALUES (1, 50, 1, 500)"),
    ];
    let imvs = [FjsImv {
        name: format!("{p}_v"),
        query: format!(
            "SELECT COALESCE(f.k, h.k) AS k, f.q, h.s, g.q AS gq FROM {f_schema}.{p}_t f \
             FULL JOIN {p}_h h ON f.k = h.k LEFT JOIN {g_schema}.{p}_t g ON g.j = COALESCE(f.k, h.k)"
        ),
        pinned: Some((format!("{g_schema}.{p}_t"), false)),
    }];
    let steps = vec![
        format!("INSERT INTO {g_schema}.{p}_t VALUES (2, 99, 2, 600)"),
        format!("UPDATE {g_schema}.{p}_t SET q = q + 1"),
        format!("UPDATE {g_schema}.{p}_t SET j = 3 WHERE id = 1"),
        format!("DELETE FROM {g_schema}.{p}_t WHERE id = 2"),
        format!("UPDATE {f_schema}.{p}_t SET k = 3 WHERE id = 1"),
    ];
    let sources = vec![
        format!("{f_schema}.{p}_t"),
        format!("{g_schema}.{p}_t"),
        format!("{p}_h"),
    ];
    fjs_run(mode, &ddl, &imvs, &sources, &steps);
}

#[pg_test]
fn v112_fjs_left_joined_table_of_the_same_bare_name_is_not_the_full_joined_one_immediate() {
    fjs_same_bare_name("fjs_ni", "IMMEDIATE", "fjs_ni_s", "public");
}

#[pg_test]
fn v112_fjs_left_joined_table_of_the_same_bare_name_is_not_the_full_joined_one_deferred() {
    fjs_same_bare_name("fjs_nd", "DEFERRED", "public", "fjs_nd_s");
}

/// A quoted mixed-case output column beside its lower-case twin: only the
/// column the join equates follows the source.
fn fjs_case_twins(p: &str, mode: &str) {
    let ddl = fjs_lines(
        &[
            "CREATE TABLE {p}_f (id INT PRIMARY KEY, k INT, \"K\" INT, q INT)",
            "CREATE TABLE {p}_h (id INT PRIMARY KEY, k INT, \"K\" INT, s INT)",
            "INSERT INTO {p}_f VALUES (1, 1, 10, 1)",
            "INSERT INTO {p}_h VALUES (1, 1, 20, 2), (2, 2, 30, 3)",
        ],
        p,
    );
    let imvs = [FjsImv {
        name: format!("{p}_v"),
        query: format!(
            "SELECT COALESCE(f.k, h.k) AS k, COALESCE(f.\"K\", h.\"K\") AS \"K\", f.q, h.s \
             FROM {p}_f f FULL JOIN {p}_h h ON f.k = h.k"
        ),
        pinned: Some((format!("{p}_f"), true)),
    }];
    let steps = fjs_lines(
        &[
            "DELETE FROM {p}_f WHERE id = 1",
            "INSERT INTO {p}_f VALUES (2, 2, 99, 4)",
            "UPDATE {p}_f SET \"K\" = \"K\" + 1",
            "UPDATE {p}_h SET s = s + 1",
        ],
        p,
    );
    let sources = vec![format!("{p}_f"), format!("{p}_h")];
    fjs_run(mode, &ddl, &imvs, &sources, &steps);
}

#[pg_test]
fn v112_fjs_quoted_mixed_case_column_is_not_its_lower_case_twin_immediate() {
    fjs_case_twins("fjs_ci", "IMMEDIATE");
}

#[pg_test]
fn v112_fjs_quoted_mixed_case_column_is_not_its_lower_case_twin_deferred() {
    fjs_case_twins("fjs_cd", "DEFERRED");
}

/// A join whose `=` is coarser than the equality of the COALESCE result
/// (varchar against char ignores trailing blanks) cannot scope the delta; two
/// columns of one type can.
fn fjs_join_equality(p: &str, mode: &str) {
    let ddl = fjs_lines(
        &[
            "CREATE TABLE {p}_f (id INT PRIMARY KEY, k VARCHAR(5), q INT)",
            "CREATE TABLE {p}_h (id INT PRIMARY KEY, k CHAR(5), s INT)",
            "CREATE TABLE {p}_g (id INT PRIMARY KEY, k VARCHAR(5), s INT)",
            "INSERT INTO {p}_f VALUES (1, 'a ', 1), (2, 'b', 2)",
            "INSERT INTO {p}_h VALUES (1, 'a', 10), (2, 'c', 20)",
            "INSERT INTO {p}_g VALUES (1, 'a', 10), (2, 'c', 20)",
        ],
        p,
    );
    let query = |other: &str| {
        format!(
            "SELECT COALESCE(f.k, o.k) AS k, f.q, o.s FROM {p}_f f FULL JOIN {p}_{other} o ON f.k = o.k"
        )
    };
    let imvs = [
        FjsImv {
            name: format!("{p}_mixed"),
            query: query("h"),
            pinned: Some((format!("{p}_f"), false)),
        },
        FjsImv {
            name: format!("{p}_same"),
            query: query("g"),
            pinned: Some((format!("{p}_f"), true)),
        },
    ];
    let steps = fjs_lines(
        &[
            "INSERT INTO {p}_f VALUES (4, 'c ', 4)",
            "UPDATE {p}_h SET s = 11 WHERE id = 1",
            "INSERT INTO {p}_h VALUES (3, 'b  ', 30)",
            "UPDATE {p}_f SET q = q + 1",
            "DELETE FROM {p}_f WHERE id = 2",
            "UPDATE {p}_f SET k = 'a' WHERE id = 1",
        ],
        p,
    );
    let sources = vec![format!("{p}_f"), format!("{p}_h"), format!("{p}_g")];
    fjs_run(mode, &ddl, &imvs, &sources, &steps);
}

#[pg_test]
fn v112_fjs_join_equality_coarser_than_the_output_column_is_not_scoped_immediate() {
    fjs_join_equality("fjs_ei", "IMMEDIATE");
}

#[pg_test]
fn v112_fjs_join_equality_coarser_than_the_output_column_is_not_scoped_deferred() {
    fjs_join_equality("fjs_ed", "DEFERRED");
}

/// A second FULL JOIN can output a row whose COALESCE value is no source
/// row's: a write to the first source must not be scoped by it.
fn fjs_full_then_full(p: &str, mode: &str) {
    let ddl = fjs_lines(
        &[
            "CREATE TABLE {p}_f (id INT PRIMARY KEY, k INT, q INT)",
            "CREATE TABLE {p}_h (id INT PRIMARY KEY, k INT, s INT)",
            "CREATE TABLE {p}_g (id INT PRIMARY KEY, j INT, v INT)",
            "INSERT INTO {p}_f VALUES (1, 1, 1)",
            "INSERT INTO {p}_h VALUES (1, 2, 2)",
            "INSERT INTO {p}_g VALUES (1, 3, 3)",
        ],
        p,
    );
    let imvs = [FjsImv {
        name: format!("{p}_v"),
        query: format!(
            "SELECT COALESCE(f.k, h.k) AS k, f.q, h.s, g.v FROM {p}_f f FULL JOIN {p}_h h ON f.k = h.k \
             FULL JOIN {p}_g g ON g.j = COALESCE(f.k, h.k)"
        ),
        pinned: Some((format!("{p}_f"), false)),
    }];
    let steps = fjs_lines(
        &[
            "INSERT INTO {p}_f VALUES (2, 3, 9)",
            "DELETE FROM {p}_f WHERE id = 2",
            "UPDATE {p}_h SET k = 3",
        ],
        p,
    );
    let sources = vec![format!("{p}_f"), format!("{p}_h"), format!("{p}_g")];
    fjs_run(mode, &ddl, &imvs, &sources, &steps);
}

#[pg_test]
fn v112_fjs_full_join_after_the_first_is_not_scoped_immediate() {
    fjs_full_then_full("fjs_fi", "IMMEDIATE");
}

#[pg_test]
fn v112_fjs_full_join_after_the_first_is_not_scoped_deferred() {
    fjs_full_then_full("fjs_fd", "DEFERRED");
}

/// A third table, LEFT-JOINed on another of its columns than the one named
/// like the scoped output: its writes do not follow the COALESCE.
fn fjs_third_table(p: &str, mode: &str) {
    let ddl = fjs_lines(
        &[
            "CREATE TABLE {p}_f (id INT PRIMARY KEY, k INT, q INT)",
            "CREATE TABLE {p}_h (id INT PRIMARY KEY, k INT, s INT)",
            "CREATE TABLE {p}_a (id INT PRIMARY KEY, j INT, k INT, w INT)",
            "INSERT INTO {p}_f VALUES (1, 1, 1), (2, 2, 2)",
            "INSERT INTO {p}_h VALUES (1, 2, 20), (2, 3, 30)",
            "INSERT INTO {p}_a VALUES (1, 1, 50, 500)",
        ],
        p,
    );
    let imvs = [FjsImv {
        name: format!("{p}_v"),
        query: format!(
            "SELECT COALESCE(f.k, h.k) AS k, f.q, h.s, a.w FROM {p}_f f FULL JOIN {p}_h h ON f.k = h.k \
             LEFT JOIN {p}_a a ON a.j = COALESCE(f.k, h.k)"
        ),
        pinned: Some((format!("{p}_a"), false)),
    }];
    let steps = fjs_lines(
        &[
            "INSERT INTO {p}_a VALUES (2, 2, 99, 600)",
            "UPDATE {p}_a SET w = w + 1",
            "UPDATE {p}_a SET j = 3 WHERE id = 1",
            "DELETE FROM {p}_a WHERE id = 2",
        ],
        p,
    );
    let sources = vec![format!("{p}_f"), format!("{p}_h"), format!("{p}_a")];
    fjs_run(mode, &ddl, &imvs, &sources, &steps);
}

#[pg_test]
fn v112_fjs_third_table_joined_on_another_column_is_not_scoped_immediate() {
    fjs_third_table("fjs_ti", "IMMEDIATE");
}

#[pg_test]
fn v112_fjs_third_table_joined_on_another_column_is_not_scoped_deferred() {
    fjs_third_table("fjs_td", "DEFERRED");
}

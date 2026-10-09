// Every scenario here commits each statement through a remote `dblink` session in
// a private database (helpers of `pg_test_partition_attach_locks.rs`,
// `pg_test_rebuild_commit.rs` and `pg_test_v112_deferred_partition.rs`), so a
// DEFERRED IMV is flushed by a real COMMIT and a statement that raises is
// reported instead of aborting the test.

const V112_SH_CREATED: &str = "CREATE REFLEX INCREMENTAL VIEW";

/// One IMV over its own copy of the tables: `{p}` is the prefix of the copy,
/// `{P}` the same prefix in upper case.
struct V112ShCell {
    name: &'static str,
    setup: &'static [&'static str],
    query: &'static str,
    key: Option<&'static str>,
    steps: &'static [(&'static str, &'static str)],
}

fn v112_sh_fill(template: &str, prefix: &str) -> String {
    template
        .replace("{p}", prefix)
        .replace("{P}", &prefix.to_uppercase())
}

/// The text a single-row SELECT returns on the worker, or the error it raised.
fn v112_sh_remote(sql: &str) -> Result<String, String> {
    let value = Spi::get_one::<String>(&format!(
        "SELECT (SELECT v FROM dblink('reflex_lock_worker', {}, false) AS t(v text))",
        sql_lit(&format!("SELECT ({sql})::text"))
    ))
    .unwrap_or_else(|e| panic!("v112_sh_remote failed for <{sql}>: {e}"));
    match value {
        Some(text) => Ok(text),
        None => Err(Spi::get_one::<String>(
            "SELECT dblink_error_message('reflex_lock_worker')",
        )
        .expect("dblink_error_message")
        .unwrap_or_default()),
    }
}

/// What `create_reflex_ivm` answered, a raised error included.
fn v112_sh_create(name: &str, query: &str, key: Option<&str>, mode: &str) -> String {
    let key = key.map_or("NULL".to_string(), sql_lit);
    match v112_sh_remote(&format!(
        "create_reflex_ivm({}, {}, {key}, NULL, {})",
        sql_lit(name),
        sql_lit(query),
        sql_lit(mode)
    )) {
        Ok(answer) => answer,
        Err(error) => format!("RAISED: {error}"),
    }
}

/// The oracle on the worker; a comparison that cannot run counts as a mismatch.
fn v112_sh_mismatch(imv: &str, fresh: &str) -> Result<i64, String> {
    v112_sh_remote(&format!(
        "SELECT count(*) FROM ((SELECT * FROM {imv} EXCEPT ALL ({fresh})) \
         UNION ALL (({fresh}) EXCEPT ALL SELECT * FROM {imv})) x"
    ))
    .map(|count| count.parse::<i64>().expect("count"))
}

fn v112_sh_check(failures: &mut Vec<String>, at: &str, imv: &str, fresh: &str) {
    match v112_sh_mismatch(imv, fresh) {
        Ok(0) => {}
        Ok(mismatches) => failures.push(format!("{at}: {imv} {mismatches} mismatches")),
        Err(error) => failures.push(format!("{at}: {imv} oracle raised {error}")),
    }
    if v112_sh_remote(&format!(
        "SELECT COALESCE(bool_or(known_stale), FALSE) FROM public.__reflex_ivm_reference \
         WHERE name = {}",
        sql_lit(imv)
    )) == Ok("true".to_string())
    {
        failures.push(format!("{at}: {imv} left known_stale"));
    }
}

/// Runs each committed step against every IMV in `imvs` (name, fresh query).
fn v112_sh_steps(
    failures: &mut Vec<String>,
    tag: &str,
    prefix: &str,
    steps: &[(&str, &str)],
    imvs: &[(String, String)],
) {
    for (imv, fresh) in imvs {
        v112_sh_check(failures, &format!("{tag} create"), imv, fresh);
    }
    for (label, step) in steps {
        if let Err(error) = v112_dp_try(&v112_sh_fill(step, prefix)) {
            failures.push(format!("{tag} {label}: raised {error}"));
            continue;
        }
        for (imv, fresh) in imvs {
            v112_sh_check(failures, &format!("{tag} {label}"), imv, fresh);
        }
    }
}

fn v112_sh_cell_failures(cell: &V112ShCell, mode: &str) -> Vec<String> {
    let prefix = format!("{}{}_", &mode[..1].to_lowercase(), cell.name);
    let tag = format!("{mode} {}", cell.name);
    for ddl in cell.setup {
        worker_exec(&v112_sh_fill(ddl, &prefix));
    }
    let imv = format!("{prefix}v");
    let query = v112_sh_fill(cell.query, &prefix);
    let created = v112_sh_create(&imv, &query, cell.key, mode);
    if created != V112_SH_CREATED {
        return vec![format!("{tag}: not created: {created}")];
    }
    let mut failures = Vec::new();
    v112_sh_steps(&mut failures, &tag, &prefix, cell.steps, &[(imv, query)]);
    failures
}

fn v112_sh_assert_cells(dbname: &str, cells: &[V112ShCell]) {
    probe_db_open(dbname);
    let mut failures = Vec::new();
    for cell in cells {
        for mode in ["IMMEDIATE", "DEFERRED"] {
            failures.extend(v112_sh_cell_failures(cell, mode));
        }
    }
    probe_db_close(dbname);
    assert!(failures.is_empty(), "\n{}", failures.join("\n"));
}

const V112_SH_CORR_SETUP: &[&str] = &[
    "CREATE TABLE {p}t(id SERIAL PRIMARY KEY, g TEXT, k INT, arr INT[])",
    "INSERT INTO {p}t(g, k, arr) VALUES ('a',1,'{1,2}'),('b',2,'{3}'),('c',3,'{}')",
    "CREATE TABLE {p}u(id SERIAL PRIMARY KEY, k INT, label TEXT)",
    "INSERT INTO {p}u(k, label) VALUES (1,'one'),(9,'nine')",
];

const V112_SH_CORR_STEPS: &[(&str, &str)] = &[
    ("ins", "INSERT INTO {p}t(g, k, arr) VALUES ('d',4,'{7,8,9}'),('e',NULL,NULL)"),
    ("array gains the value", "UPDATE {p}t SET arr = arr || 5 WHERE g = 'a'"),
    ("key gains a match", "UPDATE {p}t SET k = 9 WHERE g = 'b'"),
    ("key loses its match", "UPDATE {p}t SET k = 7 WHERE g = 'a'"),
    ("array loses the value", "UPDATE {p}t SET arr = '{1}' WHERE g = 'a'"),
    ("two arrays gain the value", "UPDATE {p}t SET arr = '{5,5}' WHERE g IN ('c', 'd')"),
    ("second match of a key", "INSERT INTO {p}u(k, label) VALUES (9,'neuf'),(3,'three')"),
    ("match moves", "UPDATE {p}u SET k = 4 WHERE label = 'three'"),
    ("one of two matches leaves", "DELETE FROM {p}u WHERE label = 'nine'"),
    ("last match leaves", "DELETE FROM {p}u WHERE label = 'neuf'"),
    ("del", "DELETE FROM {p}t WHERE g = 'b'"),
    ("empty", "DELETE FROM {p}t"),
    ("refill", "INSERT INTO {p}t(g, k, arr) VALUES ('a',1,'{5}'),('b',4,'{4}'),('b',1,'{5,6}')"),
];

macro_rules! v112_sh_corr_cell {
    ($name:literal, $key:expr, $query:literal) => {
        V112ShCell {
            name: $name,
            setup: V112_SH_CORR_SETUP,
            query: $query,
            key: $key,
            steps: V112_SH_CORR_STEPS,
        }
    };
}

/// A column of the outer table read only inside a subquery: an UPDATE of that
/// column alone changes the result.
#[pg_test]
fn v112_sh_column_read_only_inside_a_subquery() {
    v112_sh_assert_cells(
        "reflex_sh_corr",
        &[
            v112_sh_corr_cell!("un_pt", None, "SELECT id, g FROM {p}t WHERE EXISTS (SELECT 1 FROM unnest(arr) AS e WHERE e = 5)"),
            v112_sh_corr_cell!("un_keyless", None, "SELECT g FROM {p}t WHERE EXISTS (SELECT 1 FROM unnest(arr) AS e WHERE e = 5)"),
            v112_sh_corr_cell!("un_agg", None, "SELECT g, COUNT(*) AS c FROM {p}t WHERE EXISTS (SELECT 1 FROM unnest(arr) AS e WHERE e = 5) GROUP BY g"),
            v112_sh_corr_cell!("un_in", None, "SELECT id, g FROM {p}t WHERE 5 IN (SELECT unnest(arr))"),
            v112_sh_corr_cell!("un_sel", None, "SELECT id, (SELECT max(e) FROM unnest(arr) e) AS m FROM {p}t"),
            v112_sh_corr_cell!("ex_pt", None, "SELECT id, g FROM {p}t t WHERE EXISTS (SELECT 1 FROM {p}u u WHERE u.k = t.k)"),
            v112_sh_corr_cell!("ex_keyless", None, "SELECT g FROM {p}t t WHERE EXISTS (SELECT 1 FROM {p}u u WHERE u.k = t.k)"),
            v112_sh_corr_cell!("ex_agg", None, "SELECT g, COUNT(*) AS c, SUM(k) AS sk FROM {p}t t WHERE EXISTS (SELECT 1 FROM {p}u u WHERE u.k = t.k) GROUP BY g"),
            v112_sh_corr_cell!("ex_agg_bare", None, "SELECT g, COUNT(*) AS c FROM {p}t WHERE NOT EXISTS (SELECT 1 FROM {p}u u WHERE u.k = {p}t.k) GROUP BY g"),
            v112_sh_corr_cell!("sc_sel", None, "SELECT id, (SELECT max(label) FROM {p}u u WHERE u.k = t.k) AS l FROM {p}t t"),
            v112_sh_corr_cell!("sc_where", None, "SELECT id, g FROM {p}t t WHERE (SELECT count(*) FROM {p}u u WHERE u.k = t.k) > 1"),
            v112_sh_corr_cell!("in_ctl", None, "SELECT id, g FROM {p}t WHERE k IN (SELECT k FROM {p}u)"),
            v112_sh_corr_cell!("in_agg_ctl", None, "SELECT g, COUNT(*) AS c FROM {p}t WHERE k IN (SELECT k FROM {p}u) GROUP BY g"),
        ],
    );
}

const V112_SH_KIND_SETUP: &[&str] = &[
    "CREATE TABLE {p}fact(kind TEXT PRIMARY KEY, v INT)",
    "CREATE TABLE {p}dim(kind TEXT PRIMARY KEY, label TEXT)",
    "CREATE TABLE {p}grp(kind TEXT PRIMARY KEY, w INT)",
    "INSERT INTO {p}fact VALUES ('a',1),('b',2),('c',3),('z',4)",
    "INSERT INTO {p}dim VALUES ('a','A'),('b','B'),('c','C'),('u','U')",
    "INSERT INTO {p}grp VALUES ('a',10),('b',20),('u',30),('q',40)",
];

const V112_SH_KIND_STEPS: &[(&str, &str)] = &[
    ("f_ins", "INSERT INTO {p}fact VALUES ('u',7),('y',8)"),
    ("f_upd", "UPDATE {p}fact SET v = v + 10 WHERE kind = 'a'"),
    ("f_move", "UPDATE {p}fact SET kind = 'q' WHERE kind = 'c'"),
    ("f_del", "DELETE FROM {p}fact WHERE kind = 'b'"),
    ("d_ins", "INSERT INTO {p}dim VALUES ('z','Z'),('w','W')"),
    ("d_upd", "UPDATE {p}dim SET label = 'AA' WHERE kind = 'a'"),
    ("d_move", "UPDATE {p}dim SET kind = 'q' WHERE kind = 'w'"),
    ("d_del", "DELETE FROM {p}dim WHERE kind IN ('a', 'u')"),
    ("f_back", "INSERT INTO {p}fact VALUES ('b',20)"),
    ("g_ins", "INSERT INTO {p}grp VALUES ('z',1)"),
    ("g_move", "UPDATE {p}grp SET kind = 'c' WHERE kind = 'q'"),
    ("g_del", "DELETE FROM {p}grp WHERE kind = 'a'"),
    ("d_empty", "DELETE FROM {p}dim"),
    ("d_refill", "INSERT INTO {p}dim VALUES ('b','B'),('c','C'),('z','Z')"),
];

const V112_SH_PAIR_SETUP: &[&str] = &[
    "CREATE TABLE {p}a(id SERIAL PRIMARY KEY, k INT, v INT, x INT)",
    "CREATE TABLE {p}b(k INT, v INT, label TEXT, PRIMARY KEY (k, v))",
    "INSERT INTO {p}a(k, v, x) VALUES (1,1,10),(1,2,20),(2,1,30),(2,2,40),(3,1,50)",
    "INSERT INTO {p}b VALUES (1,1,'A'),(1,2,'B'),(2,1,'C'),(9,9,'U')",
];

const V112_SH_PAIR_STEPS: &[(&str, &str)] = &[
    ("a_v", "UPDATE {p}a SET v = 2 WHERE id = 1"),
    ("a_k", "UPDATE {p}a SET k = 2 WHERE id = 2"),
    ("b_v", "UPDATE {p}b SET v = 2 WHERE k = 2 AND v = 1"),
    ("b_k", "UPDATE {p}b SET k = 3, v = 1 WHERE k = 9"),
    ("a_x", "UPDATE {p}a SET x = x + 1"),
    ("a_ins", "INSERT INTO {p}a(k, v, x) VALUES (1,1,60),(9,9,70)"),
    ("b_ins", "INSERT INTO {p}b VALUES (9,9,'N')"),
    ("a_v2", "UPDATE {p}a SET v = 9 WHERE k = 9 OR id = 3"),
    ("a_del", "DELETE FROM {p}a WHERE id IN (1, 4)"),
    ("b_del", "DELETE FROM {p}b WHERE label = 'B'"),
];

macro_rules! v112_sh_cell {
    ($setup:ident, $steps:ident, $name:literal, $key:expr, $query:literal) => {
        V112ShCell {
            name: $name,
            setup: $setup,
            query: $query,
            key: $key,
            steps: $steps,
        }
    };
}

/// A join condition written in WHERE: an UPDATE of a compared column moves rows
/// in and out of the result.
#[pg_test]
fn v112_sh_join_condition_written_in_where() {
    v112_sh_assert_cells(
        "reflex_sh_wherejoin",
        &[
            v112_sh_cell!(V112_SH_KIND_SETUP, V112_SH_KIND_STEPS, "comma_agg", None, "SELECT d.label, COUNT(*) AS c, SUM(g.w) AS w FROM {p}grp g, {p}dim d WHERE g.kind = d.kind GROUP BY d.label"),
            v112_sh_cell!(V112_SH_KIND_SETUP, V112_SH_KIND_STEPS, "comma_keyless", None, "SELECT d.kind, d.label, g.w FROM {p}grp g, {p}dim d WHERE g.kind = d.kind"),
            v112_sh_cell!(V112_SH_KIND_SETUP, V112_SH_KIND_STEPS, "comma_keyed", Some("kind"), "SELECT d.kind, d.label, g.w FROM {p}grp g, {p}dim d WHERE g.kind = d.kind"),
            v112_sh_cell!(V112_SH_KIND_SETUP, V112_SH_KIND_STEPS, "comma_three", Some("kind"), "SELECT d.kind, d.label, f.v, g.w FROM {p}grp g, {p}fact f JOIN {p}dim d ON d.kind = f.kind WHERE g.kind = d.kind"),
            v112_sh_cell!(V112_SH_KIND_SETUP, V112_SH_KIND_STEPS, "comma_outer", Some("kind"), "SELECT d.kind, d.label, f.v, g.w FROM {p}grp g, {p}fact f RIGHT JOIN {p}dim d ON d.kind = f.kind WHERE g.kind = d.kind"),
            v112_sh_cell!(V112_SH_KIND_SETUP, V112_SH_KIND_STEPS, "on_true", None, "SELECT d.label, COUNT(*) AS c, SUM(g.w) AS w FROM {p}grp g JOIN {p}dim d ON TRUE WHERE g.kind = d.kind GROUP BY d.label"),
            v112_sh_cell!(V112_SH_KIND_SETUP, V112_SH_KIND_STEPS, "cross", None, "SELECT d.label, MIN(g.w) AS lo FROM {p}grp g CROSS JOIN {p}dim d WHERE d.kind = g.kind GROUP BY d.label"),
            v112_sh_cell!(V112_SH_KIND_SETUP, V112_SH_KIND_STEPS, "on_ctl", None, "SELECT d.label, COUNT(*) AS c, SUM(g.w) AS w FROM {p}grp g JOIN {p}dim d ON g.kind = d.kind GROUP BY d.label"),
            v112_sh_cell!(V112_SH_PAIR_SETUP, V112_SH_PAIR_STEPS, "using_where", None, "SELECT b.label, COUNT(*) AS c, SUM(a.x) AS sx FROM {p}a a JOIN {p}b b USING (k) WHERE a.v = b.v GROUP BY b.label"),
            v112_sh_cell!(V112_SH_PAIR_SETUP, V112_SH_PAIR_STEPS, "on_where", None, "SELECT b.label, COUNT(*) AS c, SUM(a.x) AS sx FROM {p}a a JOIN {p}b b ON a.k = b.k WHERE a.v = b.v GROUP BY b.label"),
            v112_sh_cell!(V112_SH_PAIR_SETUP, V112_SH_PAIR_STEPS, "on_where_keyed", Some("id"), "SELECT a.id, a.x, b.label FROM {p}a a JOIN {p}b b ON a.k = b.k WHERE a.v = b.v"),
            v112_sh_cell!(V112_SH_PAIR_SETUP, V112_SH_PAIR_STEPS, "on_where_ineq", None, "SELECT b.label, COUNT(*) AS c, SUM(a.x) AS sx FROM {p}a a JOIN {p}b b ON a.k = b.k WHERE a.v < b.v + 1 GROUP BY b.label"),
        ],
    );
}

const V112_SH_SEASON_SETUP: &[&str] = &[
    "CREATE TABLE {p}t(id SERIAL PRIMARY KEY, season TEXT, v INT, w INT)",
    "INSERT INTO {p}t(season, v, w) VALUES ('e',1,10),('y',2,20),('e',3,30)",
    "CREATE TABLE {p}dim(season TEXT PRIMARY KEY, label TEXT)",
    "INSERT INTO {p}dim VALUES ('e','E'),('y','Y'),('x','X')",
];

const V112_SH_SEASON_STEPS: &[(&str, &str)] = &[
    ("ins", "INSERT INTO {p}t(season, v, w) VALUES ('e',10,1),('y',20,2),('n',30,3)"),
    ("updv", "UPDATE {p}t SET v = v + 100 WHERE id = 1"),
    ("updw", "UPDATE {p}t SET w = w + 1 WHERE id = 2"),
    ("move", "UPDATE {p}t SET season = 'y' WHERE id = 3"),
    ("del", "DELETE FROM {p}t WHERE id = 2"),
    ("updv2", "UPDATE {p}t SET v = -v WHERE season = 'y'"),
    ("d_ins", "INSERT INTO {p}dim VALUES ('n','N'),('k','K')"),
    ("d_upd", "UPDATE {p}dim SET label = 'EE' WHERE season = 'e'"),
    ("d_move", "UPDATE {p}dim SET season = 'q' WHERE season = 'x'"),
    ("d_del", "DELETE FROM {p}dim WHERE season = 'e'"),
    ("ins2", "INSERT INTO {p}t(season, v, w) VALUES ('q',5,5),('e',6,6)"),
    ("t_empty", "DELETE FROM {p}t"),
    ("t_refill", "INSERT INTO {p}t(season, v, w) VALUES ('y',1,1),('y',2,2),('q',3,3)"),
];

const V112_SH_STAR_SETUP: &[&str] = &[
    "CREATE TABLE {p}fact(id SERIAL PRIMARY KEY, kind TEXT, v INT)",
    "INSERT INTO {p}fact(kind, v) VALUES ('a',1),('b',2),('c',3),('a',4),(NULL,5)",
    "CREATE TABLE {p}dim(kind TEXT PRIMARY KEY, label TEXT)",
    "INSERT INTO {p}dim VALUES ('a','A'),('b','B'),('u','U')",
    "CREATE TABLE {p}grp(kind TEXT PRIMARY KEY, w INT)",
    "INSERT INTO {p}grp VALUES ('a',10),('b',20),('q',30)",
];

const V112_SH_STAR_STEPS: &[(&str, &str)] = &[
    ("f_ins", "INSERT INTO {p}fact(kind, v) VALUES ('a',10),('z',20),('u',30)"),
    ("f_upd", "UPDATE {p}fact SET v = v + 100 WHERE id = 1"),
    ("f_move", "UPDATE {p}fact SET kind = 'q' WHERE id = 2"),
    ("f_move2", "UPDATE {p}fact SET kind = 'b' WHERE id = 1"),
    ("f_del", "DELETE FROM {p}fact WHERE id IN (3, 4)"),
    ("d_ins", "INSERT INTO {p}dim VALUES ('c','C'),('w','W'),('z','A')"),
    ("d_upd", "UPDATE {p}dim SET label = 'BB' WHERE kind = 'b'"),
    ("d_move", "UPDATE {p}dim SET kind = 'q' WHERE kind = 'w'"),
    ("d_del", "DELETE FROM {p}dim WHERE kind IN ('a', 'c')"),
    ("g_ins", "INSERT INTO {p}grp VALUES ('z',40),('u',50)"),
    ("g_upd", "UPDATE {p}grp SET w = w + 1 WHERE kind = 'q'"),
    ("g_move", "UPDATE {p}grp SET kind = 'x' WHERE kind = 'b'"),
    ("g_move2", "UPDATE {p}grp SET kind = 'b' WHERE kind = 'q'"),
    ("g_del", "DELETE FROM {p}grp WHERE kind = 'z'"),
    ("f_move3", "UPDATE {p}fact SET kind = 'u' WHERE kind = 'z'"),
    ("f_empty", "DELETE FROM {p}fact"),
    ("f_refill", "INSERT INTO {p}fact(kind, v) VALUES ('b',1),('b',2),('u',3)"),
];

/// EXISTS, IN and their negations over a table holding several rows per key:
/// a second matching row must not count the outer row twice.
#[pg_test]
fn v112_sh_exists_over_several_rows_per_key() {
    v112_sh_assert_cells(
        "reflex_sh_exists",
        &[
            v112_sh_cell!(V112_SH_STAR_SETUP, V112_SH_STAR_STEPS, "ex_agg", None, "SELECT g.kind, SUM(g.w) AS w FROM {p}grp g WHERE EXISTS (SELECT 1 FROM {p}fact f WHERE f.kind = g.kind) GROUP BY g.kind"),
            v112_sh_cell!(V112_SH_STAR_SETUP, V112_SH_STAR_STEPS, "ex_join", None, "SELECT g.kind, SUM(g.w) AS w FROM {p}grp g WHERE EXISTS (SELECT 1 FROM {p}fact f JOIN {p}dim d ON d.kind = f.kind WHERE f.kind = g.kind) GROUP BY g.kind"),
            v112_sh_cell!(V112_SH_STAR_SETUP, V112_SH_STAR_STEPS, "ex_total", None, "SELECT COUNT(*) AS c, SUM(g.w) AS w FROM {p}grp g WHERE EXISTS (SELECT 1 FROM {p}fact f WHERE f.kind = g.kind)"),
            v112_sh_cell!(V112_SH_STAR_SETUP, V112_SH_STAR_STEPS, "ex_minmax", None, "SELECT g.kind, MIN(g.w) AS lo, MAX(g.w) AS hi FROM {p}grp g WHERE EXISTS (SELECT 1 FROM {p}fact f WHERE f.kind = g.kind) GROUP BY g.kind"),
            v112_sh_cell!(V112_SH_STAR_SETUP, V112_SH_STAR_STEPS, "ex_keyed", Some("kind"), "SELECT g.kind, g.w FROM {p}grp g WHERE EXISTS (SELECT 1 FROM {p}fact f WHERE f.kind = g.kind)"),
            v112_sh_cell!(V112_SH_STAR_SETUP, V112_SH_STAR_STEPS, "ex_keyless", None, "SELECT g.w FROM {p}grp g WHERE EXISTS (SELECT 1 FROM {p}fact f WHERE f.kind = g.kind)"),
            v112_sh_cell!(V112_SH_STAR_SETUP, V112_SH_STAR_STEPS, "notex_agg", None, "SELECT g.kind, SUM(g.w) AS w FROM {p}grp g WHERE NOT EXISTS (SELECT 1 FROM {p}fact f WHERE f.kind = g.kind) GROUP BY g.kind"),
            v112_sh_cell!(V112_SH_STAR_SETUP, V112_SH_STAR_STEPS, "in_agg", None, "SELECT g.kind, SUM(g.w) AS w FROM {p}grp g WHERE g.kind IN (SELECT f.kind FROM {p}fact f) GROUP BY g.kind"),
            v112_sh_cell!(V112_SH_STAR_SETUP, V112_SH_STAR_STEPS, "ex_join_outer", None, "SELECT d.label, SUM(g.w) AS w FROM {p}grp g JOIN {p}dim d ON d.kind = g.kind WHERE EXISTS (SELECT 1 FROM {p}fact f WHERE f.kind = g.kind) GROUP BY d.label"),
        ],
    );
}

const V112_SH_ROWFN_SETUP: &[&str] = &[
    "CREATE TABLE {p}s(id SERIAL PRIMARY KEY, g TEXT, v INT)",
    "INSERT INTO {p}s(g, v) VALUES ('a',1),('a',2),('b',5)",
    "CREATE FUNCTION {p}f({p}s) RETURNS int LANGUAGE sql IMMUTABLE AS 'SELECT $1.v + 1'",
    "CREATE TABLE {p}o(g TEXT PRIMARY KEY, label TEXT)",
    "INSERT INTO {p}o VALUES ('a','A'),('b','B')",
];

const V112_SH_ROWFN_STEPS: &[(&str, &str)] = &[
    ("ins", "INSERT INTO {p}s(g, v) VALUES ('a',10),('c',7)"),
    ("upd", "UPDATE {p}s SET v = v + 100 WHERE g = 'a'"),
    ("move", "UPDATE {p}s SET g = 'b' WHERE v = 7"),
    ("o_ins", "INSERT INTO {p}o VALUES ('c','C')"),
    ("o_upd", "UPDATE {p}o SET label = 'BB' WHERE g = 'b'"),
    ("del", "DELETE FROM {p}s WHERE g = 'b'"),
    ("empty", "DELETE FROM {p}s"),
    ("refill", "INSERT INTO {p}s(g, v) VALUES ('a',1),('b',2)"),
];

const V112_SH_THREE_SETUP: &[&str] = &[
    "CREATE TABLE {p}fact(kind TEXT PRIMARY KEY, v INT)",
    "CREATE TABLE {p}dim(kind TEXT PRIMARY KEY, label TEXT)",
    "CREATE TABLE {p}other(kind TEXT PRIMARY KEY, w INT)",
    "CREATE TABLE {p}fact2(fk TEXT PRIMARY KEY, v INT)",
    "INSERT INTO {p}fact VALUES ('a',1),('b',-2),('c',-3),('z',-4)",
    "INSERT INTO {p}fact2 VALUES ('a',1),('b',-2),('c',-3),('z',-4)",
    "INSERT INTO {p}dim VALUES ('a','A'),('b','B'),('c','C'),('u','U'),('e','E')",
    "INSERT INTO {p}other VALUES ('a',1),('b',2),('c',3),('z',4)",
];

const V112_SH_THREE_STEPS: &[(&str, &str)] = &[
    ("f_upd", "UPDATE {p}fact SET v = 5 WHERE kind IN ('b', 'c'); UPDATE {p}fact2 SET v = 5 WHERE fk IN ('b', 'c')"),
    ("f_ins", "INSERT INTO {p}fact VALUES ('u',7),('y',8); INSERT INTO {p}fact2 VALUES ('u',7),('y',8)"),
    ("f_move", "UPDATE {p}fact SET kind = 'e' WHERE kind = 'c'; UPDATE {p}fact2 SET fk = 'e' WHERE fk = 'c'"),
    ("f_del", "DELETE FROM {p}fact WHERE kind = 'b'; DELETE FROM {p}fact2 WHERE fk = 'b'"),
    ("d_ins", "INSERT INTO {p}dim VALUES ('z','Z'),('y','Y')"),
    ("d_upd", "UPDATE {p}dim SET label = 'AA' WHERE kind = 'a'"),
    ("d_move", "UPDATE {p}dim SET kind = 'q' WHERE kind = 'y'"),
    ("d_del", "DELETE FROM {p}dim WHERE kind = 'u'"),
    ("o_ins", "INSERT INTO {p}other VALUES ('u',9),('e',5)"),
    ("o_upd", "UPDATE {p}other SET w = w + 10 WHERE kind = 'a'"),
    ("o_move", "UPDATE {p}other SET kind = 'y' WHERE kind = 'z'"),
    ("o_del", "DELETE FROM {p}other WHERE kind = 'a'"),
    ("f_empty", "DELETE FROM {p}fact; DELETE FROM {p}fact2"),
    ("f_refill", "INSERT INTO {p}fact VALUES ('e',1),('u',2); INSERT INTO {p}fact2 VALUES ('e',1),('u',2)"),
];

const V112_SH_DERIVED_SETUP: &[&str] = &[
    "CREATE TABLE {p}s(id SERIAL PRIMARY KEY, g TEXT, v INT, w INT)",
    "INSERT INTO {p}s(g, v, w) VALUES ('a',1,2),('b',5,NULL),('c',4,1)",
    "CREATE TABLE {p}t(id SERIAL PRIMARY KEY, g TEXT, label TEXT, v INT)",
    "INSERT INTO {p}t(g, label, v) VALUES ('a','A',1),('b','B',2)",
];

const V112_SH_DERIVED_STEPS: &[(&str, &str)] = &[
    ("s_ins", "INSERT INTO {p}s(g, v, w) VALUES ('a',10,1),('q',3,3)"),
    ("s_upd", "UPDATE {p}s SET v = v + 100 WHERE g = 'b'"),
    ("s_move", "UPDATE {p}s SET g = 'a' WHERE g = 'c'"),
    ("t_ins", "INSERT INTO {p}t(g, label, v) VALUES ('q','Q',3),('a','A2',4)"),
    ("t_upd", "UPDATE {p}t SET v = v + 1 WHERE g = 'a'"),
    ("t_move", "UPDATE {p}t SET g = 'z' WHERE g = 'b'"),
    ("both", "INSERT INTO {p}s(g, v, w) VALUES ('z',7,7); INSERT INTO {p}t(g, label, v) VALUES ('b','B3',9)"),
    ("t_del", "DELETE FROM {p}t WHERE label = 'A'"),
    ("s_del", "DELETE FROM {p}s WHERE g = 'q'"),
    ("s_empty", "DELETE FROM {p}s"),
    ("s_refill", "INSERT INTO {p}s(g, v, w) VALUES ('a',1,1),('z',2,2),('z',3,3)"),
];

const V112_SH_KEY_SETUP: &[&str] = &[
    "CREATE TABLE {p}t(id SERIAL PRIMARY KEY, g TEXT, v INT, arr INT[])",
    "INSERT INTO {p}t(g, v, arr) VALUES ('a',1,'{1}'),('b',2,'{3}'),('c',3,'{}')",
];

const V112_SH_KEY_STEPS: &[(&str, &str)] = &[
    ("two elements and a repeated value", "INSERT INTO {p}t(g, v, arr) VALUES ('d',1,'{7,8,9}'),('e',NULL,NULL)"),
    ("upd", "UPDATE {p}t SET v = 2, arr = arr || 4 WHERE g = 'a'"),
    ("move", "UPDATE {p}t SET g = 'b' WHERE g = 'c'"),
    ("del", "DELETE FROM {p}t WHERE g = 'd'"),
    ("empty", "DELETE FROM {p}t"),
    ("refill", "INSERT INTO {p}t(g, v, arr) VALUES ('a',1,'{1,2}'),('b',1,'{3}')"),
];

/// The registered sources of an IMV, as one string.
fn v112_sh_depends_on(imv: &str) -> String {
    v112_sh_remote(&format!(
        "SELECT array_to_string(depends_on, ',') FROM public.__reflex_ivm_reference WHERE name = {}",
        sql_lit(imv)
    ))
    .unwrap_or_else(|error| format!("RAISED: {error}"))
}

/// Creates the IMVs (name, query, key) over tables already set up, then runs the
/// steps against all of them. `fresh` rewrites a query into the one the oracle runs.
fn v112_sh_group_failures(
    tag: &str,
    mode: &str,
    prefix: &str,
    imvs: &[(&str, &str, Option<&str>)],
    steps: &[(&str, &str)],
) -> Vec<String> {
    let mut failures = Vec::new();
    let mut created = Vec::new();
    for (name, query, key) in imvs {
        let imv = v112_sh_fill(name, prefix);
        let query = v112_sh_fill(query, prefix);
        let answer = v112_sh_create(&imv, &query, *key, mode);
        if answer == V112_SH_CREATED {
            created.push((imv, query));
        } else {
            failures.push(format!("{tag} {mode}: {imv} not created: {answer}"));
        }
    }
    v112_sh_steps(&mut failures, &format!("{tag} {mode}"), prefix, steps, &created);
    failures
}

const V112_SH_KEYWORD_STEPS: &[(&str, &str)] = &[
    ("ins", "INSERT INTO \"{p}\"(season, v) VALUES ('e',10),('n',30)"),
    ("upd", "UPDATE \"{p}\" SET v = v + 100 WHERE season = 'e'"),
    ("move", "UPDATE \"{p}\" SET season = 'y' WHERE season = 'n'"),
    ("del", "DELETE FROM \"{p}\" WHERE v = 2"),
    ("drop the first IMV", "SELECT drop_reflex_ivm('{p}_a')"),
    ("ins after the drop", "INSERT INTO \"{p}\"(season, v) VALUES ('e',1),('k',4)"),
    ("upd after the drop", "UPDATE \"{p}\" SET v = -v WHERE season = 'y'"),
    ("empty", "DELETE FROM \"{p}\""),
    ("refill", "INSERT INTO \"{p}\"(season, v) VALUES ('e',1),('e',2)"),
];

const V112_SH_CASE_STEPS: &[(&str, &str)] = &[
    ("ins lower", "INSERT INTO {p}t(season, v, w) VALUES ('e',10,1),('n',30,3)"),
    ("upd lower", "UPDATE {p}t SET v = v + 100, w = w + 1 WHERE season = 'e'"),
    ("move lower", "UPDATE {p}t SET season = 'y' WHERE season = 'n'"),
    ("del lower", "DELETE FROM {p}t WHERE v = 2"),
    ("ins mixed", "INSERT INTO \"{p}T\"(season, v, w) VALUES ('u',100,1),('e',5,5)"),
    ("upd mixed", "UPDATE \"{p}T\" SET v = v + 1, w = w * 2 WHERE season = 'e'"),
    ("del mixed", "DELETE FROM \"{p}T\" WHERE season = 'u'"),
    ("both", "INSERT INTO \"{p}T\"(season, v, w) VALUES ('k',1,1); INSERT INTO {p}t(season, v, w) VALUES ('k',2,2)"),
    ("empty lower", "DELETE FROM {p}t"),
    ("refill lower", "INSERT INTO {p}t(season, v, w) VALUES ('e',1,1),('e',2,2)"),
];

/// The reproduction of the report: a DEFERRED IMV over a CTE whose body joins
/// two tables, both written in one transaction.
#[pg_test]
fn v112_sh_deferred_cte_over_two_tables_written_in_one_transaction() {
    const QUERY: &str = "WITH c AS (SELECT m.g, m.v FROM {p}s m JOIN {p}t j ON j.g = m.g) \
                         SELECT g, SUM(v) AS sv, COUNT(*) AS c FROM c GROUP BY g";
    const SUBQUERY: &str = "WITH c AS (SELECT m.g, m.v FROM {p}s m WHERE m.g IN (SELECT g FROM {p}t)) \
                            SELECT g, SUM(v) AS sv, COUNT(*) AS c FROM c GROUP BY g";
    probe_db_open("reflex_sh_cte2");
    for ddl in [
        "CREATE TABLE cs(id SERIAL PRIMARY KEY, g TEXT, v INT, w INT)",
        "INSERT INTO cs(g,v,w) VALUES ('a',1,2),('a',2,NULL),('a',3,3),('a',NULL,NULL),('b',5,NULL),\
         ('b',9,1),('n',NULL,4),('z',0,5),('m',-4,2),('k',6,7)",
        "CREATE TABLE ct(id SERIAL PRIMARY KEY, g TEXT, label TEXT, v INT)",
        "INSERT INTO ct(g,label,v) VALUES ('a','A',1),('b','B',2),('c','C',3),('m','M',4),('zz','ZZ',5),('a','A2',0)",
    ] {
        worker_exec(ddl);
    }
    let failures = v112_sh_group_failures(
        "cte2",
        "DEFERRED",
        "c",
        &[("{p}_join", QUERY, None), ("{p}_sub", SUBQUERY, None)],
        &[
            (
                "both tables",
                "INSERT INTO {p}s(g,v,w) VALUES ('a',10,1),('c',7,NULL),('q',3,2),('b',8,3); \
                 INSERT INTO {p}t(g,label,v) VALUES ('z','Z',0),('k','K',1); \
                 INSERT INTO {p}t(g,label,v) VALUES ('b','B2',1)",
            ),
            (
                "both tables again",
                "DELETE FROM {p}t WHERE g = 'a'; UPDATE {p}s SET v = v + 1 WHERE g IN ('b', 'k')",
            ),
        ],
    );
    probe_db_close("reflex_sh_cte2");
    assert!(failures.is_empty(), "\n{}", failures.join("\n"));
}

const V112_SH_CD_STEPS: &[(&str, &str)] = &[
    ("ins", "INSERT INTO {p}s(g, v, n) VALUES ('a',7,8),(NULL,3,3),('q',6,6),(NULL,NULL,9)"),
    ("upd", "UPDATE {p}s SET v = v + 10, n = n + 10 WHERE g IS NULL"),
    ("null key gains rows", "UPDATE {p}s SET g = NULL WHERE g = 'b'"),
    ("null key loses rows", "UPDATE {p}s SET g = 'r' WHERE g IS NULL AND v > 12"),
    ("del", "DELETE FROM {p}s WHERE g = 'a' AND v = 1"),
    ("null key leaves", "DELETE FROM {p}s WHERE g IS NULL"),
    ("null key returns", "INSERT INTO {p}s(g, v, n) VALUES (NULL,1,1),(NULL,1,2)"),
    ("empty", "DELETE FROM {p}s"),
    ("refill", "INSERT INTO {p}s(g, v, n) VALUES (NULL,4,4),('a',5,5),(NULL,5,4)"),
];

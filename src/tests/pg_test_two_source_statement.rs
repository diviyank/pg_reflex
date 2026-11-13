// Two sources of a join IMV V = A ⋈ B changed so that each one's delta is applied
// against the other's new state: Δ(A ⋈ B) = ΔA ⋈ B_old + A_new ⋈ ΔB holds only
// while B does not yet hold ΔB when ΔA is applied. One statement writing both
// (a writable CTE, a user trigger), a flush fired inside that statement by
// `SET CONSTRAINTS ALL IMMEDIATE`, and a partition change beside a write to the
// other source all break it, and ΔA ⋈ ΔB is counted twice. Each test commits in
// a worker session, so the IMMEDIATE state is checked inside the transaction and
// after COMMIT, and the DEFERRED one after the real COMMIT flush.

const TS_TWO_TABLES_DDL: [&str; 4] = [
    "CREATE TABLE ts_a (k INT PRIMARY KEY, a INT)",
    "CREATE TABLE ts_b (k INT PRIMARY KEY, b INT)",
    "INSERT INTO ts_a SELECT g, g % 2 FROM generate_series(1, 10) g",
    "INSERT INTO ts_b SELECT g, g FROM generate_series(1, 10) g",
];
const TS_GRP_SQL: &str = "SELECT ts_b.b % 3 AS g, COUNT(*) AS n, SUM(ts_a.a) AS s \
     FROM ts_a JOIN ts_b ON ts_b.k = ts_a.k GROUP BY ts_b.b % 3";
const TS_TOT_SQL: &str = "SELECT COUNT(*) AS n FROM ts_a JOIN ts_b ON ts_b.k = ts_a.k";
const TS_PT_SQL: &str = "SELECT ts_a.k, ts_a.a, ts_b.b FROM ts_a JOIN ts_b ON ts_b.k = ts_a.k";
const TS_CTE_WRITES: [&str; 3] = [
    "WITH x AS (INSERT INTO ts_a VALUES (100, 1) RETURNING 1) INSERT INTO ts_b VALUES (100, 5)",
    "WITH x AS (UPDATE ts_a SET a = a + 10 WHERE k = 4 RETURNING 1) \
     UPDATE ts_b SET b = b + 1 WHERE k = 4",
    "WITH x AS (DELETE FROM ts_a WHERE k = 3 RETURNING 1) DELETE FROM ts_b WHERE k = 3",
];

fn ts_create(name: &str, sql: &str, key: Option<&str>, mode: &str) {
    let created = rbc_select(&format!(
        "create_reflex_ivm({}, {}, {}, NULL, {})",
        sql_lit(name),
        sql_lit(sql),
        key.map(sql_lit).unwrap_or_else(|| "NULL".to_string()),
        sql_lit(mode)
    ));
    assert!(!created.starts_with("ERROR"), "create {name}: {created}");
}

/// ts_a, ts_b and three IMVs over ts_a ⋈ ts_b: grouped by a column of ts_b,
/// ungrouped, and a keyed passthrough.
fn ts_two_tables_open(dbname: &str, mode: &str) {
    probe_db_open(dbname);
    for statement in TS_TWO_TABLES_DDL {
        worker_exec(statement);
    }
    ts_create("ts_grp", TS_GRP_SQL, None, mode);
    ts_create("ts_tot", TS_TOT_SQL, None, mode);
    ts_create("ts_pt", TS_PT_SQL, Some("k"), mode);
}

fn ts_assert_two_tables_correct(when: &str) {
    for (imv, fresh) in [
        ("ts_grp", TS_GRP_SQL),
        ("ts_tot", TS_TOT_SQL),
        ("ts_pt", TS_PT_SQL),
    ] {
        assert_eq!(rbc_mismatch(imv, fresh), 0, "{imv} differs from its query {when}");
        assert!(!rbc_known_stale(imv), "{imv} flagged stale {when}");
    }
}

/// Number of rows of `imv`'s intermediate table that are not the rows it held
/// when `xmins_before` was read: a recompute rewrites them all, a delta only the
/// groups it touches.
fn ts_rewritten_intermediate_rows(imv: &str, xmins_before: &str) -> i64 {
    worker_scalar_i64(&format!(
        "SELECT count(*)::int8 FROM {} WHERE NOT (xmin::text = ANY(string_to_array('{xmins_before}', ',')))",
        rbc_select(&format!("public.reflex_intermediate_table_name('{imv}')"))
    ))
}

fn ts_intermediate_xmins(imv: &str) -> String {
    rbc_select(&format!(
        "(SELECT string_agg(DISTINCT xmin::text, ',') FROM {})",
        rbc_select(&format!("public.reflex_intermediate_table_name('{imv}')"))
    ))
}

fn ts_rebuild_count(imv: &str) -> i64 {
    worker_scalar_i64(&format!(
        "SELECT COALESCE(rebuild_count, 0)::int8 FROM public.__reflex_ivm_reference \
         WHERE name = '{imv}'"
    ))
}

/// 10-02: a writable CTE writes both sources of IMMEDIATE join IMVs.
#[pg_test]
fn pg_two_source_writable_cte_immediate() {
    const DBNAME: &str = "reflex_ts_cte_imm";
    ts_two_tables_open(DBNAME, "IMMEDIATE");
    worker_exec("BEGIN");
    for statement in TS_CTE_WRITES {
        worker_exec(statement);
        ts_assert_two_tables_correct(&format!("inside the transaction after <{statement}>"));
    }
    worker_exec("COMMIT");
    ts_assert_two_tables_correct("after COMMIT");
    probe_db_close(DBNAME);
}

/// 10-02: a user AFTER ROW trigger on ts_a writes ts_b, so ts_b's statement
/// trigger runs first, against ts_a already holding the row.
#[pg_test]
fn pg_two_source_user_trigger_immediate() {
    const DBNAME: &str = "reflex_ts_utrig_imm";
    ts_two_tables_open(DBNAME, "IMMEDIATE");
    worker_exec(
        "CREATE FUNCTION ts_mirror() RETURNS trigger LANGUAGE plpgsql AS \
         $$ BEGIN INSERT INTO ts_b VALUES (NEW.k, NEW.k * 7); RETURN NULL; END $$",
    );
    worker_exec(
        "CREATE TRIGGER ts_mirror AFTER INSERT ON ts_a FOR EACH ROW EXECUTE FUNCTION ts_mirror()",
    );
    worker_exec("BEGIN");
    worker_exec("INSERT INTO ts_a VALUES (200, 1), (201, 0)");
    ts_assert_two_tables_correct("inside the transaction");
    worker_exec("COMMIT");
    ts_assert_two_tables_correct("after COMMIT");
    probe_db_close(DBNAME);
}

/// 10-01: under `SET CONSTRAINTS ALL IMMEDIATE` the flush of ts_a's staged delta
/// runs inside the CTE statement, before ts_b's staging trigger.
#[pg_test]
fn pg_two_source_set_constraints_immediate_deferred() {
    const DBNAME: &str = "reflex_ts_sc_def";
    ts_two_tables_open(DBNAME, "DEFERRED");
    worker_exec("BEGIN");
    worker_exec("SET CONSTRAINTS ALL IMMEDIATE");
    for statement in TS_CTE_WRITES {
        worker_exec(statement);
    }
    worker_exec("COMMIT");
    ts_assert_two_tables_correct("after COMMIT");
    probe_db_close(DBNAME);
}

const TS_PJ_SQL: &str = "SELECT ts_d.label, COUNT(*) AS c, SUM(ts_s.v) AS sv \
     FROM ts_s JOIN ts_d ON ts_d.g = ts_s.g GROUP BY ts_d.label";

/// ts_s partitioned by g, ts_d keyed by g, and an unpartitioned IMV over ts_s ⋈ ts_d;
/// `with_j` attaches a filled partition 'j' and its ts_d row before the IMV exists,
/// otherwise the filled table ts_t_j is left to attach.
fn ts_partition_open(dbname: &str, mode: &str, with_j: bool) {
    probe_db_open(dbname);
    worker_exec("CREATE TABLE ts_s (id INT NOT NULL, g TEXT NOT NULL, v INT) PARTITION BY LIST (g)");
    worker_exec("CREATE TABLE ts_s_pa PARTITION OF ts_s FOR VALUES IN ('a')");
    worker_exec("CREATE TABLE ts_d (g TEXT PRIMARY KEY, label TEXT)");
    worker_exec("INSERT INTO ts_s SELECT g, 'a', g FROM generate_series(1, 5) g");
    worker_exec("INSERT INTO ts_d VALUES ('a', 'aa'), ('b', 'bb'), ('c', 'cc')");
    worker_exec("CREATE TABLE ts_t_j (LIKE ts_s INCLUDING ALL)");
    worker_exec("INSERT INTO ts_t_j VALUES (10, 'j', 2), (11, 'j', 3)");
    if with_j {
        worker_exec("ALTER TABLE ts_s ATTACH PARTITION ts_t_j FOR VALUES IN ('j')");
        worker_exec("INSERT INTO ts_d VALUES ('j', 'jj')");
    }
    ts_create("ts_pj", TS_PJ_SQL, None, mode);
    assert_eq!(
        worker_scalar_i64(
            "SELECT count(*)::int8 FROM public.__reflex_ivm_reference WHERE name = 'ts_pj' \
             AND COALESCE(array_length(partition_columns, 1), 0) = 0"
        ),
        1,
        "fixture: ts_pj is not partitioned"
    );
}

fn ts_partition_commit(dbname: &str, mode: &str, with_j: bool, statements: &[&str]) {
    ts_partition_open(dbname, mode, with_j);
    worker_exec("BEGIN");
    for statement in statements {
        worker_exec(statement);
    }
    worker_exec("COMMIT");
    assert_eq!(
        rbc_mismatch("ts_pj", TS_PJ_SQL),
        0,
        "ts_pj ({mode}) differs from its query after COMMIT of {statements:?}"
    );
    assert!(!rbc_known_stale("ts_pj"), "ts_pj ({mode}) flagged stale");
    probe_db_close(dbname);
}

const TS_ATTACH_J: &str = "ALTER TABLE ts_s ATTACH PARTITION ts_t_j FOR VALUES IN ('j')";
const TS_INSERT_D_J: &str = "INSERT INTO ts_d VALUES ('j', 'jj')";

/// 10-10: a filled partition attached, then a write to the other join source.
#[pg_test]
fn pg_two_source_partition_attach_then_other_source_write_immediate() {
    ts_partition_commit(
        "reflex_ts_pa_w_imm",
        "IMMEDIATE",
        false,
        &[TS_ATTACH_J, TS_INSERT_D_J],
    );
}

#[pg_test]
fn pg_two_source_partition_attach_then_other_source_write_deferred() {
    ts_partition_commit(
        "reflex_ts_pa_w_def",
        "DEFERRED",
        false,
        &[TS_ATTACH_J, TS_INSERT_D_J],
    );
}

#[pg_test]
fn pg_two_source_other_source_write_then_partition_attach_deferred() {
    ts_partition_commit(
        "reflex_ts_w_pa_def",
        "DEFERRED",
        false,
        &[TS_INSERT_D_J, TS_ATTACH_J],
    );
}

#[pg_test]
fn pg_two_source_other_source_write_then_partition_attach_immediate() {
    ts_partition_commit(
        "reflex_ts_w_pa_imm",
        "IMMEDIATE",
        false,
        &[TS_INSERT_D_J, TS_ATTACH_J],
    );
}

/// 10-10: a filled partition detached, then its row of the other source updated.
#[pg_test]
fn pg_two_source_partition_detach_then_other_source_update_immediate() {
    ts_partition_commit(
        "reflex_ts_pd_u_imm",
        "IMMEDIATE",
        true,
        &[
            "ALTER TABLE ts_s DETACH PARTITION ts_t_j",
            "UPDATE ts_d SET label = 'jx' WHERE g = 'j'",
        ],
    );
}

#[pg_test]
fn pg_two_source_partition_detach_then_other_source_update_deferred() {
    ts_partition_commit(
        "reflex_ts_pd_u_def",
        "DEFERRED",
        true,
        &[
            "ALTER TABLE ts_s DETACH PARTITION ts_t_j",
            "UPDATE ts_d SET label = 'jx' WHERE g = 'j'",
        ],
    );
}

/// Perf guard: writes reaching a two-source IMMEDIATE IMV from one source per
/// statement — one source, then the other, and a source with an IMV over it —
/// keep it incremental: no rebuild counted, the untouched groups not rewritten.
#[pg_test]
fn pg_two_source_single_source_write_no_recompute() {
    const DBNAME: &str = "reflex_ts_guard";
    const PG_SQL: &str = "SELECT a.grp, COUNT(*) AS n, SUM(b.val) AS s \
         FROM ts_ga a JOIN ts_gb b ON b.k = a.k GROUP BY a.grp";
    const GU_SQL: &str = "SELECT grp, SUM(k) AS total FROM ts_ga GROUP BY grp";
    const GV_SQL: &str = "SELECT a.grp, SUM(a.k * u.total) AS w \
         FROM ts_ga a JOIN ts_gu u ON u.grp = a.grp GROUP BY a.grp";
    const GV_FRESH: &str = "SELECT a.grp, SUM(a.k * u.total) AS w \
         FROM ts_ga a JOIN (SELECT grp, SUM(k) AS total FROM ts_ga GROUP BY grp) u \
           ON u.grp = a.grp GROUP BY a.grp";
    probe_db_open(DBNAME);
    worker_exec("CREATE TABLE ts_ga (k INT PRIMARY KEY, grp INT NOT NULL)");
    worker_exec("CREATE TABLE ts_gb (k INT PRIMARY KEY, val INT NOT NULL)");
    worker_exec("INSERT INTO ts_ga SELECT g, g % 5 FROM generate_series(1, 100) g");
    worker_exec("INSERT INTO ts_gb SELECT g, g FROM generate_series(1, 100) g");
    ts_create("ts_gp", PG_SQL, None, "IMMEDIATE");
    ts_create("ts_gu", GU_SQL, None, "IMMEDIATE");
    ts_create("ts_gv", GV_SQL, None, "IMMEDIATE");
    let gp_xmins = ts_intermediate_xmins("ts_gp");
    let gv_xmins = ts_intermediate_xmins("ts_gv");
    let gp_rebuilds = ts_rebuild_count("ts_gp");
    let gv_rebuilds = ts_rebuild_count("ts_gv");

    worker_exec("BEGIN");
    worker_exec("INSERT INTO ts_ga VALUES (1001, 1)");
    worker_exec("INSERT INTO ts_gb VALUES (1001, 7)");
    worker_exec("UPDATE ts_gb SET val = val + 1 WHERE k = 6");
    worker_exec("COMMIT");

    assert_eq!(rbc_mismatch("ts_gp", PG_SQL), 0, "ts_gp differs from its query");
    assert_eq!(rbc_mismatch("ts_gu", GU_SQL), 0, "ts_gu differs from its query");
    assert_eq!(rbc_mismatch("ts_gv", GV_FRESH), 0, "ts_gv differs from its query");
    assert_eq!(ts_rebuild_count("ts_gp"), gp_rebuilds, "ts_gp rebuild_count changed");
    assert_eq!(ts_rebuild_count("ts_gv"), gv_rebuilds, "ts_gv rebuild_count changed");
    assert_eq!(
        ts_rewritten_intermediate_rows("ts_gp", &gp_xmins),
        1,
        "ts_gp: only group 1 may be rewritten by one-source-per-statement writes"
    );
    assert_eq!(
        ts_rewritten_intermediate_rows("ts_gv", &gv_xmins),
        1,
        "ts_gv: a write to ts_ga reaching it again through ts_gu must stay a delta of group 1"
    );
    probe_db_close(DBNAME);
}

/// Perf guard: a partition attached with no other source written stays a delta.
#[pg_test]
fn pg_two_source_partition_change_alone_no_recompute() {
    const DBNAME: &str = "reflex_ts_pguard";
    ts_partition_open(DBNAME, "IMMEDIATE", false);
    let xmins = ts_intermediate_xmins("ts_pj");
    worker_exec("INSERT INTO ts_d VALUES ('j', 'jj')");
    worker_exec("BEGIN");
    worker_exec(TS_ATTACH_J);
    worker_exec("COMMIT");
    assert_eq!(rbc_mismatch("ts_pj", TS_PJ_SQL), 0, "ts_pj differs from its query");
    assert_eq!(
        ts_rewritten_intermediate_rows("ts_pj", &xmins),
        1,
        "ts_pj: an attach alone must add the partition as a delta, not recompute"
    );
    probe_db_close(DBNAME);
}

/// IMMEDIATE U1(X), U2(X) and V(X ⋈ U1 ⋈ U2): exact only if every statement
/// trigger maintains V before the IMVs it reads.
#[pg_test]
fn pg_two_source_immediate_diamond() {
    const V_FRESH: &str = "SELECT x.p, SUM(x.q::int8 * u1.t1 * u2.c2) AS w, COUNT(*) AS c \
         FROM tsd_x x \
         JOIN (SELECT p, SUM(q) AS t1 FROM tsd_x GROUP BY p) u1 ON u1.p = x.p \
         JOIN (SELECT p, COUNT(*) AS c2 FROM tsd_x GROUP BY p) u2 ON u2.p = x.p \
         GROUP BY x.p";
    const U1_SQL: &str = "SELECT p, SUM(q) AS t1 FROM tsd_x GROUP BY p";
    const U2_SQL: &str = "SELECT p, COUNT(*) AS c2 FROM tsd_x GROUP BY p";
    Spi::run("CREATE TABLE tsd_x (id INT PRIMARY KEY, p INT NOT NULL, q INT NOT NULL)")
        .expect("source");
    Spi::run("INSERT INTO tsd_x SELECT g, g % 4, g FROM generate_series(1, 200) g").expect("rows");
    crate::create_reflex_ivm("tsd_u1", U1_SQL, None, None, None, None);
    crate::create_reflex_ivm("tsd_u2", U2_SQL, None, None, None, None);
    crate::create_reflex_ivm(
        "tsd_v",
        "SELECT x.p, SUM(x.q::int8 * u1.t1 * u2.c2) AS w, COUNT(*) AS c \
         FROM tsd_x x JOIN tsd_u1 u1 ON u1.p = x.p JOIN tsd_u2 u2 ON u2.p = x.p \
         GROUP BY x.p",
        None,
        None,
        None,
        None,
    );
    assert_eq!(
        Spi::get_one::<i64>(
            "SELECT count(*) FROM public.__reflex_ivm_reference \
             WHERE name = 'tsd_v' AND refresh_mode = 'IMMEDIATE' \
               AND depends_on @> ARRAY['tsd_x', 'tsd_u1', 'tsd_u2']"
        )
        .expect("registry"),
        Some(1),
        "fixture: tsd_v is IMMEDIATE and reads tsd_x, tsd_u1 and tsd_u2"
    );
    for statement in [
        "UPDATE tsd_x SET q = q + 1000 WHERE id = 42",
        "INSERT INTO tsd_x VALUES (1001, 1, 5), (1002, 3, 9)",
        "DELETE FROM tsd_x WHERE id IN (7, 8)",
    ] {
        Spi::run(statement).expect(statement);
        assert_imv_correct("tsd_u1", U1_SQL);
        assert_imv_correct("tsd_u2", U2_SQL);
        assert_imv_correct("tsd_v", V_FRESH);
    }
}

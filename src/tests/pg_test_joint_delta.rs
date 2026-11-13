// An IMV V joining source X with a DEFERRED IMV U over X: one transaction's ΔX
// reaches V twice at COMMIT — directly from X's flush and through U's flush as ΔU.
// Exact only as ΔX ⋈ U_old + X_new ⋈ ΔU, so X's flush must apply ΔX to V before U
// absorbs it. Each test commits in a worker session so the real COMMIT flush runs.
// V's aggregate multiplies X and U columns: in a sum of X terms plus U terms the
// joint term of a row updated in place cancels out, and the double count hides.

const JD_X_DDL: &str = "CREATE TABLE jd_x (id INT PRIMARY KEY, product_id INT NOT NULL, \
     location_id INT NOT NULL, qty INT NOT NULL)";
const JD_X_ROWS: &str = "INSERT INTO jd_x SELECT g, g % 7, g % 5, g FROM generate_series(1, 700) g";
const JD_U_SQL: &str = "SELECT product_id, location_id, SUM(qty) AS total, COUNT(*) AS n \
     FROM jd_x GROUP BY product_id, location_id";
const JD_V_AGG_SQL: &str = "SELECT x.product_id, SUM(x.qty * u.total) AS w, COUNT(*) AS c \
     FROM jd_x x JOIN jd_u u ON u.product_id = x.product_id AND u.location_id = x.location_id \
     GROUP BY x.product_id";
const JD_V_AGG_FRESH: &str = "SELECT x.product_id, SUM(x.qty * u.total) AS w, COUNT(*) AS c \
     FROM jd_x x JOIN (SELECT product_id, location_id, SUM(qty) AS total FROM jd_x \
                       GROUP BY product_id, location_id) u \
       ON u.product_id = x.product_id AND u.location_id = x.location_id \
     GROUP BY x.product_id";
const JD_V_PT_SQL: &str = "SELECT x.id, x.product_id, x.location_id, x.qty, u.total \
     FROM jd_x x JOIN jd_u u ON u.product_id = x.product_id AND u.location_id = x.location_id";
const JD_V_PT_FRESH: &str = "SELECT x.id, x.product_id, x.location_id, x.qty, u.total \
     FROM jd_x x JOIN (SELECT product_id, location_id, SUM(qty) AS total FROM jd_x \
                       GROUP BY product_id, location_id) u \
       ON u.product_id = x.product_id AND u.location_id = x.location_id";

fn jd_create_deferred(name: &str, sql: &str, key: Option<&str>) {
    let created = rbc_select(&format!(
        "create_reflex_ivm({}, {}, {}, NULL, 'DEFERRED')",
        sql_lit(name),
        sql_lit(sql),
        key.map(sql_lit).unwrap_or_else(|| "NULL".to_string())
    ));
    assert!(!created.starts_with("ERROR"), "create {name}: {created}");
}

/// X, U = DEFERRED aggregate over X, V = DEFERRED X ⋈ U (aggregate or keyed passthrough).
fn jd_prod_shape_open(dbname: &str, passthrough: bool) {
    probe_db_open(dbname);
    worker_exec(JD_X_DDL);
    worker_exec(JD_X_ROWS);
    jd_create_deferred("jd_u", JD_U_SQL, None);
    if passthrough {
        jd_create_deferred("jd_v", JD_V_PT_SQL, Some("id"));
    } else {
        jd_create_deferred("jd_v", JD_V_AGG_SQL, None);
    }
    assert_eq!(
        worker_scalar_i64(
            "SELECT count(*)::int8 FROM public.__reflex_ivm_reference \
             WHERE name = 'jd_v' AND depends_on @> ARRAY['jd_x', 'jd_u'] \
               AND refresh_mode = 'DEFERRED'"
        ),
        1,
        "fixture: jd_v is DEFERRED and reads both jd_x and jd_u"
    );
}

fn jd_commit(statements: &[&str]) {
    worker_exec("BEGIN");
    for statement in statements {
        worker_exec(statement);
    }
    worker_exec("COMMIT");
}

fn jd_assert_correct(imv: &str, fresh: &str) {
    assert_eq!(
        rbc_mismatch(imv, fresh),
        0,
        "{imv} differs from its query after the COMMIT flush"
    );
}

fn jd_rebuild_marks(imv: &str) -> (i64, f64, bool) {
    let rebuild_count = worker_scalar_i64(&format!(
        "SELECT COALESCE(rebuild_count, 0)::int8 FROM public.__reflex_ivm_reference \
         WHERE name = '{imv}'"
    ));
    (
        rebuild_count,
        rbc_last_rebuilt_epoch(imv),
        rbc_known_stale(imv),
    )
}

#[pg_test]
fn pg_joint_delta_aggregate_one_row_update() {
    const DBNAME: &str = "reflex_jd_agg_one";
    jd_prod_shape_open(DBNAME, false);
    jd_commit(&["UPDATE jd_x SET qty = qty + 1000 WHERE id = 42"]);
    jd_assert_correct("jd_u", JD_U_SQL);
    jd_assert_correct("jd_v", JD_V_AGG_FRESH);
    probe_db_close(DBNAME);
}

#[pg_test]
fn pg_joint_delta_passthrough_one_row_update() {
    const DBNAME: &str = "reflex_jd_pt_one";
    jd_prod_shape_open(DBNAME, true);
    jd_commit(&["UPDATE jd_x SET qty = qty + 1000 WHERE id = 42"]);
    jd_assert_correct("jd_u", JD_U_SQL);
    jd_assert_correct("jd_v", JD_V_PT_FRESH);
    probe_db_close(DBNAME);
}

#[pg_test]
fn pg_joint_delta_whole_table_update() {
    const DBNAME: &str = "reflex_jd_whole";
    jd_prod_shape_open(DBNAME, false);
    jd_commit(&["UPDATE jd_x SET qty = qty * 2 + 1"]);
    jd_assert_correct("jd_u", JD_U_SQL);
    jd_assert_correct("jd_v", JD_V_AGG_FRESH);
    probe_db_close(DBNAME);
}

#[pg_test]
fn pg_joint_delta_insert_and_delete() {
    const DBNAME: &str = "reflex_jd_ins_del";
    jd_prod_shape_open(DBNAME, false);
    jd_commit(&[
        "INSERT INTO jd_x VALUES (701, 3, 2, 5000), (702, 4, 1, 6000)",
        "DELETE FROM jd_x WHERE id IN (10, 11, 12)",
    ]);
    jd_assert_correct("jd_u", JD_U_SQL);
    jd_assert_correct("jd_v", JD_V_AGG_FRESH);
    probe_db_close(DBNAME);
}

/// U2 DEFERRED over U1 DEFERRED over X; V reads X, U1 and U2.
#[pg_test]
fn pg_joint_delta_two_level() {
    const DBNAME: &str = "reflex_jd_two_level";
    const U2_SQL: &str = "SELECT product_id, SUM(total) AS ptotal FROM jd_u GROUP BY product_id";
    const V_SQL: &str = "SELECT x.product_id, SUM(x.qty * u.total + x.qty * p.ptotal) AS w \
         FROM jd_x x \
         JOIN jd_u u ON u.product_id = x.product_id AND u.location_id = x.location_id \
         JOIN jd_u2 p ON p.product_id = x.product_id \
         GROUP BY x.product_id";
    const V_FRESH: &str = "SELECT x.product_id, SUM(x.qty * u.total + x.qty * p.ptotal) AS w \
         FROM jd_x x \
         JOIN (SELECT product_id, location_id, SUM(qty) AS total FROM jd_x \
               GROUP BY product_id, location_id) u \
           ON u.product_id = x.product_id AND u.location_id = x.location_id \
         JOIN (SELECT product_id, SUM(qty) AS ptotal FROM jd_x GROUP BY product_id) p \
           ON p.product_id = x.product_id \
         GROUP BY x.product_id";
    probe_db_open(DBNAME);
    worker_exec(JD_X_DDL);
    worker_exec(JD_X_ROWS);
    jd_create_deferred("jd_u", JD_U_SQL, None);
    jd_create_deferred("jd_u2", U2_SQL, None);
    jd_create_deferred("jd_v", V_SQL, None);
    assert_eq!(
        worker_scalar_i64(
            "SELECT count(*)::int8 FROM public.__reflex_ivm_reference \
             WHERE name = 'jd_v' AND depends_on @> ARRAY['jd_x', 'jd_u', 'jd_u2']"
        ),
        1,
        "fixture: jd_v reads jd_x, jd_u and jd_u2"
    );
    jd_commit(&["UPDATE jd_x SET qty = qty + 1000 WHERE id = 42"]);
    jd_assert_correct("jd_u", JD_U_SQL);
    jd_assert_correct(
        "jd_u2",
        "SELECT product_id, SUM(qty) AS ptotal FROM jd_x GROUP BY product_id",
    );
    jd_assert_correct("jd_v", V_FRESH);
    probe_db_close(DBNAME);
}

/// V reads only U, U reads X: no shared base, so V is maintained from ΔU alone,
/// incrementally — neither flagged stale nor rebuilt.
#[pg_test]
fn pg_joint_delta_pure_chain_unchanged() {
    const DBNAME: &str = "reflex_jd_pure_chain";
    const V_SQL: &str = "SELECT product_id, SUM(total) AS t, SUM(n) AS n FROM jd_u GROUP BY product_id";
    const V_FRESH: &str = "SELECT product_id, SUM(qty) AS t, COUNT(*) AS n FROM jd_x GROUP BY product_id";
    probe_db_open(DBNAME);
    worker_exec(JD_X_DDL);
    worker_exec(JD_X_ROWS);
    jd_create_deferred("jd_u", JD_U_SQL, None);
    jd_create_deferred("jd_v", V_SQL, None);
    let before = jd_rebuild_marks("jd_v");
    jd_commit(&["UPDATE jd_x SET qty = qty + 1000 WHERE id = 42"]);
    jd_assert_correct("jd_u", JD_U_SQL);
    jd_assert_correct("jd_v", V_FRESH);
    assert_eq!(
        jd_rebuild_marks("jd_v"),
        before,
        "jd_v must be maintained incrementally: rebuild_count, last rebuild and known_stale unchanged"
    );
    probe_db_close(DBNAME);
}

/// The prod shape stays incremental: a one-row write rebuilds neither V nor U.
#[pg_test]
fn pg_joint_delta_no_full_rebuild() {
    const DBNAME: &str = "reflex_jd_no_rebuild";
    jd_prod_shape_open(DBNAME, false);
    let v_before = jd_rebuild_marks("jd_v");
    let u_before = jd_rebuild_marks("jd_u");
    jd_commit(&["UPDATE jd_x SET qty = qty + 1000 WHERE id = 42"]);
    jd_assert_correct("jd_v", JD_V_AGG_FRESH);
    assert_eq!(
        jd_rebuild_marks("jd_v"),
        v_before,
        "jd_v must be maintained incrementally: rebuild_count, last rebuild and known_stale unchanged"
    );
    assert_eq!(
        jd_rebuild_marks("jd_u"),
        u_before,
        "jd_u must be maintained incrementally: rebuild_count, last rebuild and known_stale unchanged"
    );
    probe_db_close(DBNAME);
}

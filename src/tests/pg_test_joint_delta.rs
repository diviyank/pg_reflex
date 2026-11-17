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

/// A row moves to another group, and one row is updated twice in the transaction.
#[pg_test]
fn pg_joint_delta_group_move() {
    const DBNAME: &str = "reflex_jd_group_move";
    jd_prod_shape_open(DBNAME, false);
    jd_commit(&[
        "UPDATE jd_x SET product_id = 6, location_id = 4 WHERE id = 42",
        "UPDATE jd_x SET qty = qty + 7 WHERE id = 43",
        "UPDATE jd_x SET qty = qty * 3 WHERE id = 43",
    ]);
    jd_assert_correct("jd_u", JD_U_SQL);
    jd_assert_correct("jd_v", JD_V_AGG_FRESH);
    probe_db_close(DBNAME);
}

/// W reads X and V; V reads X and U; U reads X — every level joins the base again.
#[pg_test]
fn pg_joint_delta_three_level_reader_of_source() {
    const DBNAME: &str = "reflex_jd_three_level";
    const W_SQL: &str = "SELECT x.product_id, SUM(x.qty * v.w) AS ww \
         FROM jd_x x JOIN jd_v v ON v.product_id = x.product_id GROUP BY x.product_id";
    let w_fresh = format!(
        "SELECT x.product_id, SUM(x.qty * v.w) AS ww \
         FROM jd_x x JOIN ({JD_V_AGG_FRESH}) v ON v.product_id = x.product_id \
         GROUP BY x.product_id"
    );
    jd_prod_shape_open(DBNAME, false);
    jd_create_deferred("jd_w", W_SQL, None);
    jd_commit(&["UPDATE jd_x SET qty = qty + 1000 WHERE id = 42"]);
    jd_assert_correct("jd_u", JD_U_SQL);
    jd_assert_correct("jd_v", JD_V_AGG_FRESH);
    jd_assert_correct("jd_w", &w_fresh);
    probe_db_close(DBNAME);
}

/// V reads X and two DEFERRED IMVs U1, U2 both over X: no order makes every
/// delta exact, and the cross-source guard's COMMIT-time rebuild of V must.
#[pg_test]
fn pg_joint_delta_diamond() {
    const DBNAME: &str = "reflex_jd_diamond";
    const U2_SQL: &str = "SELECT product_id, MAX(qty) AS top FROM jd_x GROUP BY product_id";
    const V_SQL: &str = "SELECT x.product_id, SUM(x.qty * u.total + x.qty * t.top) AS w \
         FROM jd_x x \
         JOIN jd_u u ON u.product_id = x.product_id AND u.location_id = x.location_id \
         JOIN jd_u2 t ON t.product_id = x.product_id \
         GROUP BY x.product_id";
    const V_FRESH: &str = "SELECT x.product_id, SUM(x.qty * u.total + x.qty * t.top) AS w \
         FROM jd_x x \
         JOIN (SELECT product_id, location_id, SUM(qty) AS total FROM jd_x \
               GROUP BY product_id, location_id) u \
           ON u.product_id = x.product_id AND u.location_id = x.location_id \
         JOIN (SELECT product_id, MAX(qty) AS top FROM jd_x GROUP BY product_id) t \
           ON t.product_id = x.product_id \
         GROUP BY x.product_id";
    probe_db_open(DBNAME);
    worker_exec(JD_X_DDL);
    worker_exec(JD_X_ROWS);
    jd_create_deferred("jd_u", JD_U_SQL, None);
    jd_create_deferred("jd_u2", U2_SQL, None);
    jd_create_deferred("jd_v", V_SQL, None);
    jd_commit(&["UPDATE jd_x SET qty = qty + 1000 WHERE id = 42"]);
    jd_assert_correct("jd_u", JD_U_SQL);
    jd_assert_correct("jd_u2", U2_SQL);
    jd_assert_correct("jd_v", V_FRESH);
    probe_db_close(DBNAME);
}

// IMMEDIATE: the statement trigger on X maintains each IMV in turn, and U's target
// write fires V's trigger on U at once. V must take ΔX before U does, as above.

const JDI_V_FRESH: &str = "SELECT x.product_id, SUM(x.qty * u.total) AS w, COUNT(*) AS c \
     FROM jdi_x x JOIN (SELECT product_id, location_id, SUM(qty) AS total FROM jdi_x \
                        GROUP BY product_id, location_id) u \
       ON u.product_id = x.product_id AND u.location_id = x.location_id \
     GROUP BY x.product_id";
const JDI_U_SQL: &str = "SELECT product_id, location_id, SUM(qty) AS total, COUNT(*) AS n \
     FROM jdi_x GROUP BY product_id, location_id";

/// X, U = IMMEDIATE aggregate over X, V = IMMEDIATE aggregate X ⋈ U. With
/// `deferred_neighbour`, an unrelated DEFERRED IMV on X gives X the trigger
/// bodies that also stage deferred deltas.
fn jdi_open(prefix: &str, deferred_neighbour: bool) -> (String, String) {
    let rename = |sql: &str| sql.replace("jdi_", &format!("{prefix}_"));
    Spi::run(&rename(
        "CREATE TABLE jdi_x (id INT PRIMARY KEY, product_id INT NOT NULL, \
         location_id INT NOT NULL, qty INT NOT NULL)",
    ))
    .expect("source");
    Spi::run(&rename(
        "INSERT INTO jdi_x SELECT g, g % 7, g % 5, g FROM generate_series(1, 700) g",
    ))
    .expect("rows");
    crate::create_reflex_ivm(&rename("jdi_u"), &rename(JDI_U_SQL), None, None, None, None);
    crate::create_reflex_ivm(
        &rename("jdi_v"),
        &rename(
            "SELECT x.product_id, SUM(x.qty * u.total) AS w, COUNT(*) AS c \
             FROM jdi_x x JOIN jdi_u u \
               ON u.product_id = x.product_id AND u.location_id = x.location_id \
             GROUP BY x.product_id",
        ),
        None,
        None,
        None,
        None,
    );
    if deferred_neighbour {
        crate::create_reflex_ivm(
            &rename("jdi_n"),
            &rename("SELECT location_id, COUNT(*) AS c FROM jdi_x GROUP BY location_id"),
            None,
            None,
            Some("DEFERRED"),
            None,
        );
    }
    assert_eq!(
        Spi::get_one::<i64>(&rename(
            "SELECT count(*) FROM public.__reflex_ivm_reference \
             WHERE name IN ('jdi_u', 'jdi_v') AND refresh_mode = 'IMMEDIATE'"
        ))
        .expect("registry"),
        Some(2),
        "fixture: U and V are IMMEDIATE"
    );
    (rename(JDI_U_SQL), rename(JDI_V_FRESH))
}

#[pg_test]
fn pg_joint_delta_immediate_aggregate_one_row_update() {
    let (u_fresh, v_fresh) = jdi_open("jdi1", false);
    Spi::run("UPDATE jdi1_x SET qty = qty + 1000 WHERE id = 42").expect("update");
    assert_imv_correct("jdi1_u", &u_fresh);
    assert_imv_correct("jdi1_v", &v_fresh);
}

#[pg_test]
fn pg_joint_delta_immediate_insert_and_delete() {
    let (u_fresh, v_fresh) = jdi_open("jdi2", false);
    Spi::run("INSERT INTO jdi2_x VALUES (701, 3, 2, 5000), (702, 4, 1, 6000)").expect("insert");
    assert_imv_correct("jdi2_v", &v_fresh);
    Spi::run("DELETE FROM jdi2_x WHERE id IN (10, 11, 12)").expect("delete");
    assert_imv_correct("jdi2_u", &u_fresh);
    assert_imv_correct("jdi2_v", &v_fresh);
}

#[pg_test]
fn pg_joint_delta_immediate_truncate() {
    let (u_fresh, v_fresh) = jdi_open("jdi3", false);
    Spi::run("TRUNCATE jdi3_x").expect("truncate");
    assert_imv_correct("jdi3_v", &v_fresh);
    Spi::run("INSERT INTO jdi3_x SELECT g, g % 7, g % 5, g FROM generate_series(1, 50) g")
        .expect("refill");
    assert_imv_correct("jdi3_u", &u_fresh);
    assert_imv_correct("jdi3_v", &v_fresh);
}

/// Same, with a DEFERRED IMV also on X: the INSERT, DELETE and UPDATE bodies
/// that stage deferred deltas maintain U and V.
#[pg_test]
fn pg_joint_delta_immediate_beside_deferred() {
    let (u_fresh, v_fresh) = jdi_open("jdi4", true);
    Spi::run("UPDATE jdi4_x SET qty = qty + 1000 WHERE id = 42").expect("update");
    assert_imv_correct("jdi4_v", &v_fresh);
    Spi::run("INSERT INTO jdi4_x VALUES (701, 3, 2, 5000)").expect("insert");
    assert_imv_correct("jdi4_v", &v_fresh);
    Spi::run("DELETE FROM jdi4_x WHERE id IN (10, 11)").expect("delete");
    assert_imv_correct("jdi4_u", &u_fresh);
    assert_imv_correct("jdi4_v", &v_fresh);
    Spi::run("TRUNCATE jdi4_x").expect("truncate");
    assert_imv_correct("jdi4_v", &v_fresh);
}

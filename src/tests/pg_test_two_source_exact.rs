// One statement writing both sources of an IMMEDIATE join IMV V = X ⋈ Y applies
// each source's delta against the other's new state, so the joint term ΔX ⋈ ΔY
// is applied by both deltas (or, for deletes on both, by neither). V must stay
// exact, and stay incremental: the delta of the second source is corrected by
// −(ΔX ⋈ ΔY) instead of recomputing V. Shapes the correction cannot handle
// (MIN/MAX, outer joins) recompute and stay exact.

const TSX_FK_DDL: [&str; 6] = [
    "CREATE TABLE jp (k INT PRIMARY KEY, g INT NOT NULL)",
    "CREATE TABLE jc (id INT PRIMARY KEY, \
       k INT REFERENCES jp ON DELETE CASCADE ON UPDATE CASCADE, v INT NOT NULL)",
    "CREATE TABLE jn (id INT PRIMARY KEY, \
       k INT REFERENCES jp ON DELETE SET NULL ON UPDATE SET NULL, v INT NOT NULL)",
    "INSERT INTO jp SELECT g, g % 20 FROM generate_series(1, 100) g",
    "INSERT INTO jc SELECT g, (g % 100) + 1, g FROM generate_series(1, 300) g",
    "INSERT INTO jn SELECT g, (g % 100) + 1, g FROM generate_series(1, 300) g",
];
const TSX_SUM: &str = "SELECT p.g, COUNT(*) AS n, SUM(c.v) AS s \
     FROM jp p JOIN jc c ON c.k = p.k GROUP BY p.g";
const TSX_AVG: &str = "SELECT p.g, AVG(c.v) AS a FROM jp p JOIN jc c ON c.k = p.k GROUP BY p.g";
const TSX_NULL: &str = "SELECT p.g, COUNT(*) AS n, SUM(c.v) AS s \
     FROM jp p JOIN jn c ON c.k = p.k GROUP BY p.g";
const TSX_PT: &str = "SELECT c.id, c.k, p.g, c.v FROM jp p JOIN jc c ON c.k = p.k";
const TSX_MAX: &str = "SELECT p.g, MAX(c.v) AS m, MIN(c.v) AS l \
     FROM jp p JOIN jc c ON c.k = p.k GROUP BY p.g";
const TSX_LEFT: &str = "SELECT p.g, COUNT(c.id) AS n, SUM(c.v) AS s \
     FROM jp p LEFT JOIN jc c ON c.k = p.k GROUP BY p.g";

/// IMVs kept exact by the correction, whose untouched groups must not be rewritten.
const TSX_FK_EXACT: [(&str, &str); 3] = [
    ("tsx_sum", TSX_SUM),
    ("tsx_avg", TSX_AVG),
    ("tsx_null", TSX_NULL),
];
/// IMVs checked by the oracle only: a passthrough over a keyless source, which
/// rebuilds on that source's writes, and the shapes that recompute.
const TSX_FK_ORACLE_ONLY: [(&str, &str); 3] = [
    ("tsx_pt", TSX_PT),
    ("tsx_max", TSX_MAX),
    ("tsx_left", TSX_LEFT),
];

fn tsx_fk_open(dbname: &str) {
    probe_db_open(dbname);
    for statement in TSX_FK_DDL {
        worker_exec(statement);
    }
    for (imv, sql) in TSX_FK_EXACT.iter().chain(TSX_FK_ORACLE_ONLY.iter()) {
        let key = (*imv == "tsx_pt").then_some("id");
        ts_create(imv, sql, key, "IMMEDIATE");
    }
}

fn tsx_assert_correct(imvs: &[(&str, &str)], when: &str) {
    for (imv, fresh) in imvs {
        assert_eq!(rbc_mismatch(imv, fresh), 0, "{imv} differs from its query {when}");
        assert!(!rbc_known_stale(imv), "{imv} flagged stale {when}");
    }
}

fn tsx_target_xmins(imv: &str) -> String {
    rbc_select(&format!("(SELECT string_agg(DISTINCT xmin::text, ',') FROM {imv})"))
}

fn tsx_rewritten_target_rows(imv: &str, xmins_before: &str) -> i64 {
    worker_scalar_i64(&format!(
        "SELECT count(*)::int8 FROM {imv} \
         WHERE NOT (xmin::text = ANY(string_to_array('{xmins_before}', ',')))"
    ))
}

/// Runs `statement` in a transaction: every IMV is exact after it and after
/// COMMIT, and each `incremental` IMV rewrote at most `groups` rows of its
/// intermediate table (a recompute rewrites all of them).
fn tsx_step(
    exact: &[(&str, &str)],
    oracle_only: &[(&str, &str)],
    statement: &str,
    groups: i64,
) {
    let xmins: Vec<String> = exact.iter().map(|(imv, _)| ts_intermediate_xmins(imv)).collect();
    worker_exec("BEGIN");
    worker_exec(statement);
    let when = format!("inside the transaction of <{statement}>");
    tsx_assert_correct(exact, &when);
    tsx_assert_correct(oracle_only, &when);
    worker_exec("COMMIT");
    let when = format!("after COMMIT of <{statement}>");
    tsx_assert_correct(exact, &when);
    tsx_assert_correct(oracle_only, &when);
    for ((imv, _), before) in exact.iter().zip(&xmins) {
        let rewritten = ts_rewritten_intermediate_rows(imv, before);
        assert!(
            rewritten <= groups,
            "{imv}: <{statement}> rewrote {rewritten} intermediate rows, more than the \
             {groups} groups it touches (a recompute)"
        );
    }
}

/// ON DELETE CASCADE, ON UPDATE CASCADE and SET NULL: one statement on jp writes
/// the other source of every IMV.
#[pg_test]
fn pg_two_source_exact_fk_cascades() {
    const DBNAME: &str = "reflex_tsx_fk";
    tsx_fk_open(DBNAME);
    for (statement, groups) in [
        ("DELETE FROM jp WHERE k = 81", 1),
        ("UPDATE jp SET k = 2082 WHERE k = 82", 1),
        ("UPDATE jp SET k = 3083, g = 5 WHERE k = 83", 2),
        ("DELETE FROM jp WHERE k IN (84, 85)", 2),
        ("UPDATE jp SET k = 4086 WHERE k = 86", 1),
    ] {
        tsx_step(&TSX_FK_EXACT, &TSX_FK_ORACLE_ONLY, statement, groups);
    }
    probe_db_close(DBNAME);
}

/// Writable CTEs and a user trigger writing jp and jc in one statement.
#[pg_test]
fn pg_two_source_exact_cte_and_user_trigger_fk() {
    const DBNAME: &str = "reflex_tsx_cte_fk";
    tsx_fk_open(DBNAME);
    worker_exec(
        "CREATE FUNCTION tsx_child() RETURNS trigger LANGUAGE plpgsql AS \
         $$ BEGIN INSERT INTO jc VALUES (NEW.k + 50000, NEW.k, 9); RETURN NULL; END $$",
    );
    worker_exec(
        "CREATE TRIGGER tsx_child AFTER INSERT ON jp FOR EACH ROW EXECUTE FUNCTION tsx_child()",
    );
    for (statement, groups) in [
        (
            "WITH x AS (INSERT INTO jp VALUES (1001, 1) RETURNING 1) \
             INSERT INTO jc VALUES (5001, 1001, 7), (5002, 1001, 8)",
            1,
        ),
        (
            "WITH x AS (UPDATE jp SET g = 2 WHERE k = 21 RETURNING 1) \
             UPDATE jc SET v = v + 1 WHERE k = 21",
            2,
        ),
        (
            "WITH x AS (DELETE FROM jc WHERE k = 41 RETURNING 1) DELETE FROM jp WHERE k = 41",
            1,
        ),
        (
            "WITH x AS (INSERT INTO jp VALUES (1002, 3) RETURNING 1) \
             UPDATE jc SET k = 1002 WHERE id IN (7, 8)",
            3,
        ),
        ("INSERT INTO jp VALUES (1003, 4), (1004, 4)", 1),
    ] {
        tsx_step(&TSX_FK_EXACT, &TSX_FK_ORACLE_ONLY, statement, groups);
    }
    probe_db_close(DBNAME);
}

const TSX_PAIR_DDL: [&str; 4] = [
    "CREATE TABLE ra (k INT PRIMARY KEY, g INT NOT NULL, a INT NOT NULL)",
    "CREATE TABLE rb (k INT PRIMARY KEY, b INT NOT NULL)",
    "INSERT INTO ra SELECT g, g % 10, g FROM generate_series(1, 200) g",
    "INSERT INTO rb SELECT g, g * 2 FROM generate_series(1, 150) g",
];
const TSX_R_SUM: &str = "SELECT ra.g, COUNT(*) AS n, SUM(rb.b) AS s, SUM(ra.a) AS sa \
     FROM ra JOIN rb ON rb.k = ra.k GROUP BY ra.g";
const TSX_R_AVG: &str = "SELECT ra.g, AVG(rb.b) AS ab FROM ra JOIN rb ON rb.k = ra.k GROUP BY ra.g";
const TSX_R_TOT: &str = "SELECT COUNT(*) AS n, SUM(rb.b - ra.a) AS d FROM ra JOIN rb ON rb.k = ra.k";
const TSX_R_PT: &str = "SELECT ra.k, ra.a, rb.b FROM ra JOIN rb ON rb.k = ra.k";
const TSX_R_MAX: &str = "SELECT ra.g, MAX(rb.b) AS m FROM ra JOIN rb ON rb.k = ra.k GROUP BY ra.g";

const TSX_R_GROUPED: [(&str, &str); 2] = [("tsx_r_sum", TSX_R_SUM), ("tsx_r_avg", TSX_R_AVG)];
const TSX_R_OTHERS: [(&str, &str); 3] = [
    ("tsx_r_tot", TSX_R_TOT),
    ("tsx_r_pt", TSX_R_PT),
    ("tsx_r_max", TSX_R_MAX),
];

fn tsx_pair_open(dbname: &str) {
    probe_db_open(dbname);
    for statement in TSX_PAIR_DDL {
        worker_exec(statement);
    }
    for (imv, sql) in TSX_R_GROUPED.iter().chain(TSX_R_OTHERS.iter()) {
        let key = (*imv == "tsx_r_pt").then_some("k");
        ts_create(imv, sql, key, "IMMEDIATE");
    }
}

/// Writable CTEs over a 1:1 join: grouped, AVG, ungrouped and keyed passthrough
/// IMVs stay exact, and the passthrough rewrites only the rows of the keys written.
#[pg_test]
fn pg_two_source_exact_cte_pair() {
    const DBNAME: &str = "reflex_tsx_pair";
    tsx_pair_open(DBNAME);
    for (statement, groups, pt_rows) in [
        (
            "WITH x AS (INSERT INTO ra VALUES (1001, 1, 5) RETURNING 1) INSERT INTO rb VALUES (1001, 7)",
            1,
            1,
        ),
        (
            "WITH x AS (UPDATE ra SET a = a + 10 WHERE k = 11 RETURNING 1) \
             UPDATE rb SET b = b + 1 WHERE k = 11",
            1,
            1,
        ),
        (
            "WITH x AS (DELETE FROM ra WHERE k = 21 RETURNING 1) DELETE FROM rb WHERE k = 21",
            1,
            0,
        ),
        (
            "WITH x AS (UPDATE ra SET g = 2 WHERE k = 31 RETURNING 1) \
             UPDATE rb SET b = b + 5 WHERE k = 31",
            2,
            1,
        ),
        (
            "WITH x AS (INSERT INTO rb VALUES (151, 9) RETURNING 1) \
             UPDATE ra SET a = a + 1 WHERE k = 151",
            1,
            1,
        ),
        (
            "WITH x AS (INSERT INTO rb VALUES (152, 4), (153, 6) RETURNING 1) \
             DELETE FROM ra WHERE k IN (152, 51)",
            3,
            1,
        ),
    ] {
        let pt_xmins = tsx_target_xmins("tsx_r_pt");
        tsx_step(&TSX_R_GROUPED, &TSX_R_OTHERS, statement, groups);
        let rewritten = tsx_rewritten_target_rows("tsx_r_pt", &pt_xmins);
        assert!(
            rewritten <= pt_rows,
            "tsx_r_pt: <{statement}> rewrote {rewritten} rows, more than the {pt_rows} it writes"
        );
    }
    probe_db_close(DBNAME);
}

/// A seeded random mix of writable CTEs over both sources, upserts, FK cascades
/// and single-source writes, each IMV checked against its query after every
/// statement.
#[pg_test]
fn pg_two_source_exact_randomised() {
    const DBNAME: &str = "reflex_tsx_rand";
    const TSX_F_SUM: &str = "SELECT ra.g, COUNT(*) AS n, SUM(rf.f) AS s \
         FROM ra JOIN rf ON rf.k = ra.k GROUP BY ra.g";
    tsx_pair_open(DBNAME);
    worker_exec(
        "CREATE TABLE rf (id SERIAL PRIMARY KEY, \
           k INT REFERENCES ra ON DELETE CASCADE ON UPDATE CASCADE, f INT NOT NULL)",
    );
    worker_exec("INSERT INTO rf (k, f) SELECT (g % 200) + 1, g FROM generate_series(1, 400) g");
    ts_create("tsx_f_sum", TSX_F_SUM, None, "IMMEDIATE");
    let imvs: Vec<(&str, &str)> = TSX_R_GROUPED
        .iter()
        .chain(TSX_R_OTHERS.iter())
        .copied()
        .chain([("tsx_f_sum", TSX_F_SUM)])
        .collect();

    let mut state: u64 = 0x5eed_2026_1117;
    let mut next = |bound: u64| -> u64 {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (state >> 33) % bound
    };
    let ra_write = |next: &mut dyn FnMut(u64) -> u64| -> String {
        let k = next(320) + 1;
        match next(4) {
            0 => format!(
                "INSERT INTO ra VALUES ({k}, {}, {}) ON CONFLICT DO NOTHING",
                next(10),
                next(50)
            ),
            1 => format!("UPDATE ra SET a = a + {}, g = (g + {}) % 10 WHERE k IN ({k}, {})", next(9) + 1, next(3), k + 1),
            2 => format!("DELETE FROM ra WHERE k = {k}"),
            _ => format!(
                "UPDATE ra SET k = k + 400 WHERE k = {k} \
                 AND NOT EXISTS (SELECT 1 FROM ra WHERE k = {})",
                k + 400
            ),
        }
    };
    let rb_write = |next: &mut dyn FnMut(u64) -> u64| -> String {
        let k = next(320) + 1;
        match next(4) {
            0 => format!("INSERT INTO rb VALUES ({k}, {}) ON CONFLICT DO NOTHING", next(40)),
            1 => format!("UPDATE rb SET b = b + {} WHERE k IN ({k}, {})", next(9) + 1, k + 2),
            2 => format!("DELETE FROM rb WHERE k = {k}"),
            _ => format!(
                "INSERT INTO rb VALUES ({k}, {}), ({}, 1) ON CONFLICT (k) DO UPDATE SET b = rb.b + 1",
                next(40),
                k + 1
            ),
        }
    };
    let returning = |write: String| -> String {
        if write.starts_with("INSERT") && write.contains("ON CONFLICT (k)") {
            write
        } else {
            format!("{write} RETURNING 1")
        }
    };
    let mut corrected: Vec<(&str, i64)> = ["tsx_r_sum", "tsx_r_avg", "tsx_r_tot", "tsx_r_pt", "tsx_f_sum"]
        .into_iter()
        .map(|imv| (imv, 0))
        .collect();
    for iteration in 0..90 {
        let statement = match next(6) {
            0 | 1 => format!(
                "WITH x AS ({}) {}",
                returning(ra_write(&mut next)),
                rb_write(&mut next)
            ),
            2 => format!(
                "WITH x AS ({}) {}",
                returning(rb_write(&mut next)),
                ra_write(&mut next)
            ),
            3 => format!(
                "WITH x AS (INSERT INTO rf (k, f) SELECT k, {} FROM ra WHERE k = {} RETURNING 1) {}",
                next(30),
                next(320) + 1,
                ra_write(&mut next)
            ),
            4 => ra_write(&mut next),
            _ => rb_write(&mut next),
        };
        worker_exec("BEGIN");
        worker_exec(&statement);
        for (imv, total) in corrected.iter_mut() {
            *total += tsx_corrected(imv);
        }
        worker_exec("COMMIT");
        tsx_assert_correct(&imvs, &format!("after statement {iteration} <{statement}>"));
    }
    for (imv, total) in corrected {
        assert!(total > 0, "{imv}: no delta of the run was corrected, every one recomputed");
    }
    probe_db_close(DBNAME);
}

/// The deltas of `imv` corrected in the running transaction of the worker.
fn tsx_corrected(imv: &str) -> i64 {
    worker_scalar_i64(&format!(
        "SELECT COALESCE(NULLIF(current_setting('{}', true), ''), '0')::int8",
        crate::trigger::joint::corrections_setting(imv)
    ))
}

/// False positive (a): a user DEFERRED constraint event stays queued on rb while
/// a later statement writes only ra, then another writes rb. Nothing is owed: no
/// recompute.
#[pg_test]
fn pg_two_source_exact_pending_deferred_event_needs_no_correction() {
    const DBNAME: &str = "reflex_tsx_pin_a";
    tsx_pair_open(DBNAME);
    worker_exec(
        "CREATE FUNCTION tsx_noop() RETURNS trigger LANGUAGE plpgsql AS \
         $$ BEGIN RETURN NULL; END $$",
    );
    worker_exec(
        "CREATE CONSTRAINT TRIGGER tsx_later AFTER INSERT ON rb \
         DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION tsx_noop()",
    );
    let xmins: Vec<String> = TSX_R_GROUPED
        .iter()
        .map(|(imv, _)| ts_intermediate_xmins(imv))
        .collect();
    worker_exec("BEGIN");
    for statement in [
        "INSERT INTO rb VALUES (161, 3)",
        "INSERT INTO ra VALUES (1001, 1, 4)",
        "INSERT INTO rb VALUES (1001, 8)",
    ] {
        worker_exec(statement);
        tsx_assert_correct(&TSX_R_GROUPED, &format!("after <{statement}>"));
        tsx_assert_correct(&TSX_R_OTHERS, &format!("after <{statement}>"));
    }
    worker_exec("COMMIT");
    tsx_assert_correct(&TSX_R_GROUPED, "after COMMIT");
    for ((imv, _), before) in TSX_R_GROUPED.iter().zip(&xmins) {
        assert_eq!(
            ts_rewritten_intermediate_rows(imv, before),
            1,
            "{imv}: writes of group 1 must stay a delta of group 1"
        );
    }
    probe_db_close(DBNAME);
}

/// False positive (b): a user AFTER STATEMENT trigger on ra, firing after ra's
/// pg_reflex trigger, writes rb: ra's delta saw none of rb's writes.
#[pg_test]
fn pg_two_source_exact_user_trigger_after_reflex_trigger() {
    const DBNAME: &str = "reflex_tsx_pin_b";
    tsx_pair_open(DBNAME);
    worker_exec(
        "CREATE FUNCTION tsx_after() RETURNS trigger LANGUAGE plpgsql AS \
         $$ BEGIN INSERT INTO rb SELECT k, 5 FROM new_ra ON CONFLICT DO NOTHING; RETURN NULL; END $$",
    );
    worker_exec(
        "CREATE TRIGGER zz_tsx_after AFTER INSERT ON ra REFERENCING NEW TABLE AS new_ra \
         FOR EACH STATEMENT EXECUTE FUNCTION tsx_after()",
    );
    for (statement, groups) in [
        ("INSERT INTO ra VALUES (1001, 1, 4), (1011, 1, 6)", 1),
        (
            "WITH x AS (INSERT INTO ra VALUES (1002, 2, 4) RETURNING 1) \
             UPDATE rb SET b = b + 1 WHERE k = 12",
            1,
        ),
    ] {
        tsx_step(&TSX_R_GROUPED, &TSX_R_OTHERS, statement, groups);
    }
    probe_db_close(DBNAME);
}

/// (c): the deltas of a statement writing both sources run in a subtransaction
/// that rolls back; then the same number of rows is written again, so a stash
/// surviving the rollback would match the counts.
#[pg_test]
fn pg_two_source_exact_subtransaction_rollback() {
    const DBNAME: &str = "reflex_tsx_pin_c";
    tsx_pair_open(DBNAME);
    worker_exec(
        "CREATE FUNCTION tsx_fail() RETURNS trigger LANGUAGE plpgsql AS \
         $$ BEGIN IF current_setting('tsx.fail', true) = 'on' THEN \
              RAISE EXCEPTION 'tsx abort'; END IF; RETURN NULL; END $$",
    );
    for table in ["ra", "rb"] {
        worker_exec(&format!(
            "CREATE TRIGGER zz_tsx_fail_{table} AFTER INSERT ON {table} \
             FOR EACH STATEMENT EXECUTE FUNCTION tsx_fail()"
        ));
    }
    worker_exec("BEGIN");
    worker_exec(
        "DO $$ BEGIN \
           PERFORM set_config('tsx.fail', 'on', true); \
           BEGIN \
             WITH x AS (INSERT INTO ra VALUES (1001, 1, 4) RETURNING 1) \
             INSERT INTO rb VALUES (1001, 8); \
           EXCEPTION WHEN raise_exception THEN NULL; \
           END; \
           PERFORM set_config('tsx.fail', 'off', true); \
         END $$",
    );
    tsx_assert_correct(&TSX_R_GROUPED, "after the rolled back subtransaction");
    for statement in [
        "INSERT INTO ra VALUES (1001, 1, 4)",
        "INSERT INTO rb VALUES (1001, 8)",
    ] {
        worker_exec(statement);
        tsx_assert_correct(&TSX_R_GROUPED, &format!("after <{statement}>"));
        tsx_assert_correct(&TSX_R_OTHERS, &format!("after <{statement}>"));
    }
    worker_exec("COMMIT");
    tsx_assert_correct(&TSX_R_GROUPED, "after COMMIT");
    tsx_assert_correct(&TSX_R_OTHERS, "after COMMIT");
    probe_db_close(DBNAME);
}

/// (c), with the stash tables already created by an earlier statement of the
/// transaction: bookkeeping surviving the rollback would apply that statement's
/// stashed rows of rb to the rows of ra written again.
#[pg_test]
fn pg_two_source_exact_subtransaction_rollback_over_earlier_stash() {
    const DBNAME: &str = "reflex_tsx_pin_c2";
    tsx_pair_open(DBNAME);
    worker_exec(
        "CREATE FUNCTION tsx_fail() RETURNS trigger LANGUAGE plpgsql AS \
         $$ BEGIN IF current_setting('tsx.fail', true) = 'on' THEN \
              RAISE EXCEPTION 'tsx abort'; END IF; RETURN NULL; END $$",
    );
    worker_exec(
        "CREATE TRIGGER zz_tsx_fail_rb AFTER INSERT ON rb \
         FOR EACH STATEMENT EXECUTE FUNCTION tsx_fail()",
    );
    worker_exec("BEGIN");
    worker_exec(
        "WITH x AS (INSERT INTO ra VALUES (1002, 2, 4) RETURNING 1) INSERT INTO rb VALUES (1001, 8)",
    );
    tsx_assert_correct(&TSX_R_GROUPED, "after the earlier statement");
    worker_exec(
        "DO $$ BEGIN \
           PERFORM set_config('tsx.fail', 'on', true); \
           BEGIN \
             WITH x AS (INSERT INTO ra VALUES (1001, 1, 4) RETURNING 1) \
             INSERT INTO rb VALUES (1003, 8); \
           EXCEPTION WHEN raise_exception THEN NULL; \
           END; \
           PERFORM set_config('tsx.fail', 'off', true); \
         END $$",
    );
    tsx_assert_correct(&TSX_R_GROUPED, "after the rolled back subtransaction");
    for statement in [
        "INSERT INTO ra VALUES (1001, 1, 4)",
        "INSERT INTO rb VALUES (1003, 8)",
    ] {
        worker_exec(statement);
        tsx_assert_correct(&TSX_R_GROUPED, &format!("after <{statement}>"));
        tsx_assert_correct(&TSX_R_OTHERS, &format!("after <{statement}>"));
    }
    worker_exec("COMMIT");
    tsx_assert_correct(&TSX_R_GROUPED, "after COMMIT");
    tsx_assert_correct(&TSX_R_OTHERS, "after COMMIT");
    probe_db_close(DBNAME);
}

/// (e): a consumed stash is cleared, so later single-source writes of the same
/// transaction stay deltas of the group they write.
#[pg_test]
fn pg_two_source_exact_consumed_stash_leaves_later_writes_incremental() {
    const DBNAME: &str = "reflex_tsx_pin_e";
    tsx_pair_open(DBNAME);
    let xmins: Vec<String> = TSX_R_GROUPED
        .iter()
        .map(|(imv, _)| ts_intermediate_xmins(imv))
        .collect();
    worker_exec("BEGIN");
    for statement in [
        "WITH x AS (INSERT INTO ra VALUES (1001, 1, 4) RETURNING 1) INSERT INTO rb VALUES (1001, 8)",
        "UPDATE rb SET b = b + 1 WHERE k = 11",
        "UPDATE ra SET a = a + 1 WHERE k = 21",
    ] {
        worker_exec(statement);
        tsx_assert_correct(&TSX_R_GROUPED, &format!("after <{statement}>"));
        tsx_assert_correct(&TSX_R_OTHERS, &format!("after <{statement}>"));
    }
    worker_exec("COMMIT");
    tsx_assert_correct(&TSX_R_GROUPED, "after COMMIT");
    for ((imv, _), before) in TSX_R_GROUPED.iter().zip(&xmins) {
        assert_eq!(
            ts_rewritten_intermediate_rows(imv, before),
            1,
            "{imv}: writes of group 1 must stay a delta of group 1"
        );
    }
    probe_db_close(DBNAME);
}

/// (d): MERGE inserting and updating rb, in one statement with a write to ra.
#[pg_test]
fn pg_two_source_exact_merge_and_upsert_with_other_source() {
    const DBNAME: &str = "reflex_tsx_pin_d";
    tsx_pair_open(DBNAME);
    for statement in [
        "WITH x AS (INSERT INTO ra VALUES (1001, 1, 4) RETURNING 1) \
         MERGE INTO rb USING (VALUES (1001, 7), (11, 3)) s(k, b) ON rb.k = s.k \
         WHEN MATCHED THEN UPDATE SET b = s.b WHEN NOT MATCHED THEN INSERT VALUES (s.k, s.b)",
        "WITH x AS (UPDATE ra SET a = a + 1 WHERE k IN (1001, 12) RETURNING 1) \
         INSERT INTO rb VALUES (1001, 2), (12, 2) ON CONFLICT (k) DO UPDATE SET b = rb.b + 1",
        "WITH x AS (DELETE FROM ra WHERE k = 13 RETURNING 1) \
         MERGE INTO rb USING (VALUES (13, 0), (14, 1), (1013, 1)) s(k, d) ON rb.k = s.k \
         WHEN MATCHED AND s.d = 0 THEN DELETE WHEN MATCHED THEN UPDATE SET b = b + 1 \
         WHEN NOT MATCHED THEN INSERT VALUES (s.k, 3)",
    ] {
        worker_exec("BEGIN");
        worker_exec(statement);
        tsx_assert_correct(&TSX_R_GROUPED, &format!("inside <{statement}>"));
        tsx_assert_correct(&TSX_R_OTHERS, &format!("inside <{statement}>"));
        worker_exec("COMMIT");
        tsx_assert_correct(&TSX_R_GROUPED, &format!("after COMMIT of <{statement}>"));
        tsx_assert_correct(&TSX_R_OTHERS, &format!("after COMMIT of <{statement}>"));
    }
    probe_db_close(DBNAME);
}

/// A role that may not create temporary tables writes both sources in one
/// statement: the correction, which stashes rows in temporary tables, gives way
/// to the recompute instead of raising.
#[pg_test]
fn pg_two_source_exact_role_without_temp_recomputes() {
    const DBNAME: &str = "reflex_tsx_notemp";
    const ROLE: &str = "tsx_notemp_writer";
    tsx_pair_open(DBNAME);
    worker_exec(&format!("REVOKE TEMP ON DATABASE {DBNAME} FROM PUBLIC"));
    worker_exec(&format!(
        "DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{ROLE}') THEN \
           CREATE ROLE {ROLE}; END IF; END $$"
    ));
    worker_exec(&format!("GRANT USAGE ON SCHEMA public TO {ROLE}"));
    worker_exec(&format!("GRANT ALL ON ALL TABLES IN SCHEMA public TO {ROLE}"));
    worker_exec(&format!("GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO {ROLE}"));
    worker_exec(&format!("SET ROLE {ROLE}"));
    let may_create_temp =
        worker_scalar_i64("SELECT has_database_privilege(current_database(), 'TEMP')::int::int8");
    let mut outcomes = Vec::new();
    for statement in [
        "INSERT INTO rb VALUES (1002, 3)",
        "WITH x AS (INSERT INTO ra VALUES (1001, 1, 5) RETURNING 1) INSERT INTO rb VALUES (1001, 7)",
        "WITH x AS (UPDATE ra SET a = a + 10 WHERE k = 11 RETURNING 1) \
         UPDATE rb SET b = b + 1 WHERE k = 11",
    ] {
        let outcome = Spi::get_one::<String>(&format!(
            "SELECT CASE WHEN dblink_exec('reflex_lock_worker', {}, false) = 'ERROR' \
                    THEN dblink_error_message('reflex_lock_worker') ELSE 'OK' END",
            sql_lit(statement)
        ))
        .expect("dblink")
        .unwrap_or_default();
        outcomes.push((statement, outcome));
    }
    worker_exec("RESET ROLE");
    worker_exec(&format!("REVOKE ALL ON ALL TABLES IN SCHEMA public FROM {ROLE}"));
    worker_exec(&format!("REVOKE ALL ON ALL SEQUENCES IN SCHEMA public FROM {ROLE}"));
    worker_exec(&format!("REVOKE USAGE ON SCHEMA public FROM {ROLE}"));
    assert_eq!(may_create_temp, 0, "fixture: the role may not create temporary tables");
    for (statement, outcome) in outcomes {
        assert_eq!(outcome, "OK", "<{statement}> as a role without TEMP raised");
    }
    tsx_assert_correct(&TSX_R_GROUPED, "after the writes of the role without TEMP");
    tsx_assert_correct(&TSX_R_OTHERS, "after the writes of the role without TEMP");
    probe_db_close(DBNAME);
    admin_exec(&format!("DROP ROLE IF EXISTS {ROLE}"));
}

const TSX_BR_SUM: &str = "SELECT xa.g, COUNT(*) AS n, SUM(xa.a) AS sa, SUM(sb.b) AS sbb \
     FROM xa JOIN sb ON sb.k = xa.k GROUP BY xa.g";
const TSX_BR_TOT: &str = "SELECT COUNT(*) AS n, SUM(xa.a + sb.b) AS s FROM xa JOIN sb ON sb.k = xa.k";
const TSX_BR_PT: &str = "SELECT sb.id, xa.g, xa.a, sb.b FROM xa JOIN sb ON sb.k = xa.k";
const TSX_BR_IMVS: [(&str, &str); 3] = [
    ("tsx_br_sum", TSX_BR_SUM),
    ("tsx_br_tot", TSX_BR_TOT),
    ("tsx_br_pt", TSX_BR_PT),
];

/// A BEFORE ROW trigger on sb writes xa: each of its UPDATEs of xa applies xa's
/// delta while the rows sb inserted before it are visible but sb's delta is not
/// queued yet. `foreign_key` adds sb's RI trigger, whose queued event on sb
/// hides that sb holds unapplied writes.
fn tsx_before_row_trigger_case(dbname: &str, foreign_key: bool) {
    probe_db_open(dbname);
    let reference = if foreign_key { "REFERENCES xa" } else { "" };
    for statement in [
        "CREATE TABLE xa (k INT PRIMARY KEY, g INT NOT NULL, a INT NOT NULL)".to_string(),
        format!("CREATE TABLE sb (id INT PRIMARY KEY, k INT NOT NULL {reference}, b INT NOT NULL)"),
        "INSERT INTO xa SELECT g, g % 10, g FROM generate_series(1, 100) g".to_string(),
        "INSERT INTO sb SELECT g, (g % 100) + 1, g FROM generate_series(1, 200) g".to_string(),
        "CREATE FUNCTION tsx_bump() RETURNS trigger LANGUAGE plpgsql AS \
         $$ BEGIN UPDATE xa SET a = a + 1 WHERE k = NEW.k; RETURN NEW; END $$"
            .to_string(),
        "CREATE TRIGGER tsx_bump BEFORE INSERT OR UPDATE ON sb \
         FOR EACH ROW EXECUTE FUNCTION tsx_bump()"
            .to_string(),
    ] {
        worker_exec(&statement);
    }
    for (imv, sql) in TSX_BR_IMVS {
        let key = (imv == "tsx_br_pt").then_some("id");
        ts_create(imv, sql, key, "IMMEDIATE");
    }
    for statement in [
        "INSERT INTO sb VALUES (1001, 7, 1), (1002, 7, 1), (1003, 7, 1)",
        "INSERT INTO sb VALUES (1004, 8, 1), (1005, 9, 1)",
        "INSERT INTO sb VALUES (1006, 10, 1)",
        "UPDATE sb SET b = b + 1 WHERE k = 11",
        "UPDATE sb SET k = 12 WHERE id IN (1001, 1002)",
    ] {
        tsx_step(&[], &TSX_BR_IMVS, statement, 0);
    }
    probe_db_close(dbname);
}

#[pg_test]
fn pg_two_source_exact_before_row_trigger_writing_other_source() {
    tsx_before_row_trigger_case("reflex_tsx_br", false);
}

#[pg_test]
fn pg_two_source_exact_before_row_trigger_writing_other_source_with_fk() {
    tsx_before_row_trigger_case("reflex_tsx_br_fk", true);
}

/// Writes to sc whose delta every IMV skips (an UPDATE of a column no IMV reads,
/// a source the IMV ignores) leave no unapplied writes behind: the later writes
/// of xa and sc in the same transaction stay deltas of the group they write.
#[pg_test]
fn pg_two_source_exact_skipped_delta_leaves_later_writes_incremental() {
    const DBNAME: &str = "reflex_tsx_skip";
    const SUM: &str = "SELECT xa.g, COUNT(*) AS n, SUM(sc.b) AS s, SUM(xa.a) AS sa \
         FROM xa JOIN sc ON sc.k = xa.k GROUP BY xa.g";
    const IGNORING: &str = "SELECT xa.g, COUNT(*) AS n, SUM(sc.b) AS s, SUM(xa.a) AS sa \
         FROM xa JOIN sc ON sc.k = xa.k GROUP BY xa.g";
    probe_db_open(DBNAME);
    for statement in [
        "CREATE TABLE xa (k INT PRIMARY KEY, g INT NOT NULL, a INT NOT NULL)",
        "CREATE TABLE sc (k INT PRIMARY KEY, b INT NOT NULL, note TEXT)",
        "INSERT INTO xa SELECT g, g % 10, g FROM generate_series(1, 200) g",
        "INSERT INTO sc SELECT g, g * 2, NULL FROM generate_series(1, 150) g",
    ] {
        worker_exec(statement);
    }
    ts_create("tsx_sk_sum", SUM, None, "IMMEDIATE");
    let ignoring = rbc_select(&format!(
        "create_reflex_ivm('tsx_sk_ign', {}, NULL, NULL, 'IMMEDIATE', '!sc')",
        sql_lit(IGNORING)
    ));
    assert!(!ignoring.starts_with("ERROR"), "create tsx_sk_ign: {ignoring}");
    let sc = worker_scalar_i64("SELECT 'sc'::regclass::oid::int8");
    let stash_of_sc = crate::trigger::joint::stash_setting(
        "tsx_sk_ign",
        pg_sys::Oid::from(u32::try_from(sc).expect("oid")),
    );
    let xmins = ts_intermediate_xmins("tsx_sk_sum");
    worker_exec("BEGIN");
    for statement in [
        "UPDATE sc SET note = 'skipped' WHERE k IN (5, 6)",
        "UPDATE sc SET b = b + 1 WHERE k = 1",
        "UPDATE xa SET a = a + 1 WHERE k = 11",
        "UPDATE sc SET b = b + 1 WHERE k = 21",
    ] {
        worker_exec(statement);
        tsx_assert_correct(&[("tsx_sk_sum", SUM)], &format!("after <{statement}>"));
    }
    let stash_in_worker = worker_scalar_i64(&format!(
        "SELECT length(COALESCE(current_setting('{stash_of_sc}', true), ''))::int8"
    ));
    worker_exec("COMMIT");
    tsx_assert_correct(&[("tsx_sk_sum", SUM)], "after COMMIT");
    assert_eq!(
        ts_rewritten_intermediate_rows("tsx_sk_sum", &xmins),
        1,
        "tsx_sk_sum: writes of group 1 after skipped deltas must stay a delta of group 1"
    );
    assert_eq!(
        stash_in_worker, 0,
        "tsx_sk_ign ignores sc: xa's delta must not stash rows for sc"
    );
    probe_db_close(DBNAME);
}

/// A recompute counts every write of the other source as applied, including a
/// write whose delta has not fired yet. When that delta is then skipped (rb's
/// recompute fires first, ra's UPDATE touches no column the IMV reads), the
/// skip must not count its rows a second time: a later statement writing both
/// sources would then miss ra's write and apply the joint term twice.
#[pg_test]
fn pg_two_source_exact_skip_after_recompute_keeps_later_writes_exact() {
    const DBNAME: &str = "reflex_tsx_skip_recompute";
    tsx_pair_open(DBNAME);
    worker_exec("BEGIN");
    worker_exec(
        "DO $$ BEGIN \
           WITH x AS (INSERT INTO ra SELECT g, g % 10, g FROM generate_series(20001, 30001) g RETURNING 1) \
           INSERT INTO rb SELECT g, g FROM generate_series(20001, 30001) g; \
           WITH x AS (UPDATE ra SET a = a + 1 WHERE k = 1 RETURNING 1) \
           UPDATE rb SET b = b + 1 WHERE k = 1; \
         END $$",
    );
    tsx_assert_correct(&TSX_R_GROUPED, "after a recompute and a skipped delta");
    worker_exec(
        "WITH x AS (INSERT INTO ra VALUES (40006, 6, 6) RETURNING 1) INSERT INTO rb VALUES (40006, 6)",
    );
    tsx_assert_correct(&TSX_R_GROUPED, "after a later statement writing both sources");
    tsx_assert_correct(&TSX_R_OTHERS, "after a later statement writing both sources");
    worker_exec("COMMIT");
    tsx_assert_correct(&TSX_R_GROUPED, "after COMMIT");
    probe_db_close(DBNAME);
}

/// 2000 statements writing both sources in one transaction keep the IMVs exact
/// and take no lock per statement: the lock count does not grow with the
/// number of statements.
#[pg_test]
fn pg_two_source_exact_many_corrected_statements_hold_few_locks() {
    const DBNAME: &str = "reflex_tsx_locks";
    let corrected_loop = |from: i32, to: i32| {
        format!(
            "DO $$ BEGIN FOR i IN {from}..{to} LOOP \
               WITH x AS (INSERT INTO ra VALUES (10000 + i, i % 10, i) RETURNING 1) \
               INSERT INTO rb VALUES (10000 + i, i); \
               WITH x AS (UPDATE ra SET a = a + 1 WHERE k = 10000 + i RETURNING 1) \
               UPDATE rb SET b = b + 1 WHERE k = 10000 + i; \
             END LOOP; END $$"
        )
    };
    let own_locks = "SELECT count(*)::int8 FROM pg_locks WHERE pid = pg_backend_pid()";
    tsx_pair_open(DBNAME);
    worker_exec("BEGIN");
    worker_exec(&corrected_loop(1, 5));
    let locks_after_few = worker_scalar_i64(own_locks);
    worker_exec(&corrected_loop(6, 1000));
    let locks_after_many = worker_scalar_i64(own_locks);
    tsx_assert_correct(&TSX_R_GROUPED, "after 2000 statements writing both sources");
    tsx_assert_correct(&TSX_R_OTHERS, "after 2000 statements writing both sources");
    worker_exec("COMMIT");
    tsx_assert_correct(&TSX_R_GROUPED, "after COMMIT");
    tsx_assert_correct(&TSX_R_OTHERS, "after COMMIT");
    assert!(
        locks_after_many <= locks_after_few + 4,
        "2000 statements hold {locks_after_many} locks, after the first 10 they held {locks_after_few}"
    );
    probe_db_close(DBNAME);
}

/// A transaction writing both sources in one statement can be prepared and
/// committed as a two-phase transaction.
#[pg_test]
fn pg_two_source_exact_prepared_transaction() {
    const DBNAME: &str = "reflex_tsx_2pc";
    tsx_pair_open(DBNAME);
    let try_exec = |statement: &str| -> String {
        Spi::get_one::<String>(&format!(
            "SELECT CASE WHEN dblink_exec('reflex_lock_worker', {}, false) = 'ERROR' \
                    THEN dblink_error_message('reflex_lock_worker') ELSE 'OK' END",
            sql_lit(statement)
        ))
        .expect("dblink")
        .unwrap_or_default()
    };
    worker_exec("BEGIN");
    worker_exec(
        "WITH x AS (INSERT INTO ra VALUES (1001, 1, 5) RETURNING 1) INSERT INTO rb VALUES (1001, 7)",
    );
    worker_exec(
        "WITH x AS (UPDATE ra SET a = a + 10 WHERE k = 11 RETURNING 1) \
         UPDATE rb SET b = b + 1 WHERE k = 11",
    );
    let prepared = try_exec("PREPARE TRANSACTION 'tsx_2pc'");
    if prepared != "OK" {
        try_exec("ROLLBACK");
    }
    let committed = try_exec("COMMIT PREPARED 'tsx_2pc'");
    assert_eq!(prepared, "OK", "PREPARE TRANSACTION after writes of both sources");
    assert_eq!(committed, "OK", "COMMIT PREPARED");
    tsx_assert_correct(&TSX_R_GROUPED, "after COMMIT PREPARED");
    tsx_assert_correct(&TSX_R_OTHERS, "after COMMIT PREPARED");
    probe_db_close(DBNAME);
}

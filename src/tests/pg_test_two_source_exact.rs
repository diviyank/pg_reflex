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
        worker_exec(&statement);
        tsx_assert_correct(&imvs, &format!("after statement {iteration} <{statement}>"));
    }
    probe_db_close(DBNAME);
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

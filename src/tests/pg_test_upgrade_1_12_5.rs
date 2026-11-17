// The 1.12.4 → 1.12.5 migration rewrites the per-source delta trigger bodies a
// 1.12.4 library rendered into exactly what a 1.12.5 create renders, and a
// two-source statement is then corrected incrementally instead of recomputed.
//
// The 1.12.4 bodies in `fixtures/trigger_bodies_1.12.4.sql` were dumped with
// `pg_get_functiondef` from a 1.12.4 install (main 2e8817a, pg17) after running
// the DDL and creates below there.

const UPG_DDL: [&str; 6] = [
    "CREATE TABLE upg_a (k INT PRIMARY KEY, g INT NOT NULL, a INT NOT NULL)",
    "CREATE TABLE upg_b (k INT PRIMARY KEY, b INT NOT NULL)",
    "CREATE TABLE upg_d (k INT PRIMARY KEY, g INT NOT NULL, v INT NOT NULL)",
    "INSERT INTO upg_a SELECT g, g % 10, g FROM generate_series(1, 200) g",
    "INSERT INTO upg_b SELECT g, g * 2 FROM generate_series(1, 150) g",
    "INSERT INTO upg_d SELECT g, g % 7, g FROM generate_series(1, 100) g",
];
const UPG_SUM: &str = "SELECT upg_a.g, COUNT(*) AS n, SUM(upg_b.b) AS s, SUM(upg_a.a) AS sa \
     FROM upg_a JOIN upg_b ON upg_b.k = upg_a.k GROUP BY upg_a.g";
const UPG_PT: &str = "SELECT upg_a.k, upg_a.a, upg_b.b FROM upg_a JOIN upg_b ON upg_b.k = upg_a.k";
const UPG_D_DEFERRED: &str = "SELECT g, SUM(v) AS s, COUNT(*) AS n FROM upg_d GROUP BY g";
const UPG_D_IMMEDIATE: &str = "SELECT g, MAX(v) AS m FROM upg_d GROUP BY g";

const UPG_JOIN_IMVS: [(&str, &str); 2] = [("upg_sum", UPG_SUM), ("upg_pt", UPG_PT)];
const UPG_D_IMVS: [(&str, &str); 2] = [
    ("upg_d_deferred", UPG_D_DEFERRED),
    ("upg_d_immediate", UPG_D_IMMEDIATE),
];

const UPG_TRIGGER_FUNCTIONS: [&str; 9] = [
    "__reflex_ins_trigger_on_upg_a",
    "__reflex_del_trigger_on_upg_a",
    "__reflex_upd_trigger_on_upg_a",
    "__reflex_ins_trigger_on_upg_b",
    "__reflex_del_trigger_on_upg_b",
    "__reflex_upd_trigger_on_upg_b",
    "__reflex_ins_trigger_on_upg_d",
    "__reflex_del_trigger_on_upg_d",
    "__reflex_upd_trigger_on_upg_d",
];

const UPG_TWO_SOURCE_CTE: &str =
    "WITH x AS (UPDATE upg_a SET a = a + 10 WHERE k = 11 RETURNING 1) \
     UPDATE upg_b SET b = b + 1 WHERE k = 11";

fn upg_open(dbname: &str) {
    probe_db_open(dbname);
    for statement in UPG_DDL {
        worker_exec(statement);
    }
    ts_create("upg_sum", UPG_SUM, None, "IMMEDIATE");
    ts_create("upg_pt", UPG_PT, Some("k"), "IMMEDIATE");
    ts_create("upg_d_deferred", UPG_D_DEFERRED, None, "DEFERRED");
    ts_create("upg_d_immediate", UPG_D_IMMEDIATE, None, "IMMEDIATE");
}

fn upg_body(function: &str) -> String {
    rbc_select(&format!(
        "(SELECT prosrc FROM pg_proc WHERE oid = 'public.{function}()'::regprocedure)"
    ))
}

fn upg_bodies() -> Vec<String> {
    UPG_TRIGGER_FUNCTIONS.iter().map(|f| upg_body(f)).collect()
}

fn upg_install_1_12_4_bodies() {
    let fixture = include_str!("fixtures/trigger_bodies_1.12.4.sql");
    let mut installed = 0;
    for statement in fixture.split("\n-- next function\n") {
        if statement.trim().is_empty() {
            continue;
        }
        worker_exec(statement);
        installed += 1;
    }
    assert_eq!(installed, UPG_TRIGGER_FUNCTIONS.len(), "fixture holds one body per function");
}

fn upg_assert_correct(imvs: &[(&str, &str)], when: &str) {
    for (imv, fresh) in imvs {
        assert_eq!(rbc_mismatch(imv, fresh), 0, "{imv} differs from its query {when}");
        assert!(!rbc_known_stale(imv), "{imv} flagged stale {when}");
    }
}

/// Runs the two-source CTE in a transaction, checks every join IMV against its
/// query inside it and after COMMIT, and returns how many deltas of upg_sum were
/// corrected and how many of its intermediate rows the statement rewrote.
fn upg_two_source_statement() -> (i64, i64) {
    let xmins = ts_intermediate_xmins("upg_sum");
    worker_exec("BEGIN");
    worker_exec(UPG_TWO_SOURCE_CTE);
    upg_assert_correct(&UPG_JOIN_IMVS, "inside the transaction of the two-source CTE");
    let corrected = tsx_corrected("upg_sum");
    worker_exec("COMMIT");
    upg_assert_correct(&UPG_JOIN_IMVS, "after COMMIT of the two-source CTE");
    (corrected, ts_rewritten_intermediate_rows("upg_sum", &xmins))
}

#[pg_test]
fn pg_upgrade_1_12_5_rewrites_1_12_4_trigger_bodies_to_the_fresh_rendering() {
    const DBNAME: &str = "reflex_upg_1125";
    upg_open(DBNAME);
    let fresh = upg_bodies();

    upg_install_1_12_4_bodies();
    for (function, (old, new)) in UPG_TRIGGER_FUNCTIONS.iter().zip(upg_bodies().iter().zip(&fresh)) {
        assert_ne!(old, new, "precondition: {function} holds its 1.12.4 body");
        assert!(
            old.contains("ORDER BY graph_depth, name") && !old.contains("_resolving("),
            "precondition: {function} is a 1.12.4 body"
        );
    }
    let (corrected, _) = upg_two_source_statement();
    assert_eq!(corrected, 0, "precondition: a 1.12.4 body corrects no two-source delta");

    worker_exec(smd_migration_1_12_5_trigger_rewrite());
    for (function, (migrated, new)) in UPG_TRIGGER_FUNCTIONS.iter().zip(upg_bodies().iter().zip(&fresh)) {
        assert_eq!(migrated, new, "{function} differs from what a 1.12.5 create renders");
    }

    let (corrected, rewritten) = upg_two_source_statement();
    assert!(corrected > 0, "the two-source CTE was recomputed, not corrected, after the upgrade");
    assert!(
        rewritten <= 1,
        "upg_sum: the two-source CTE rewrote {rewritten} intermediate rows of the 1 group it touches (a recompute)"
    );

    worker_exec("BEGIN");
    worker_exec("UPDATE upg_d SET v = v + 100 WHERE k IN (3, 4)");
    worker_exec("INSERT INTO upg_d VALUES (1001, 3, 7)");
    worker_exec("DELETE FROM upg_d WHERE k = 5");
    worker_exec("COMMIT");
    upg_assert_correct(&UPG_D_IMVS, "after the writes to the DEFERRED-flavour source");

    worker_exec(smd_migration_1_12_5_trigger_rewrite());
    assert_eq!(upg_bodies(), fresh, "a second run of the rewrite changed a current body");
    probe_db_close(DBNAME);
}

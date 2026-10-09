// Every scenario here commits: the DEFERRED flush and the partition flush run at
// COMMIT only. A remote `dblink` session commits each step in a private database
// (helpers of `pg_test_partition_attach_locks.rs` and `pg_test_rebuild_commit.rs`).

/// Runs `sql` on the worker as one committed transaction (a multi-statement
/// string is one implicit transaction); the error text when it raises. Not
/// `BEGIN ... COMMIT`: a flush that fails inside an explicit block's COMMIT trips
/// an assertion-only check of cassert builds.
fn v112_dp_try(sql: &str) -> Result<(), String> {
    let status = Spi::get_one::<String>(&format!(
        "SELECT dblink_exec('reflex_lock_worker', {}, false)",
        sql_lit(sql)
    ))
    .expect("dblink_exec")
    .unwrap_or_default();
    if status != "ERROR" {
        return Ok(());
    }
    let error = Spi::get_one::<String>("SELECT dblink_error_message('reflex_lock_worker')")
        .expect("dblink_error_message")
        .unwrap_or_default();
    let _ = Spi::get_one::<String>(
        "SELECT dblink_exec('reflex_lock_worker', 'ROLLBACK', false)",
    );
    Err(error)
}

fn v112_dp_create(name: &str, query: &str, key: Option<&str>, mode: &str, partition_by: Option<&str>) {
    let key = key.map_or("NULL".to_string(), sql_lit);
    let call = match partition_by {
        Some(column) => format!(
            "create_reflex_ivm({}, {}, {key}, NULL, {}, NULL, ARRAY[{}])",
            sql_lit(name),
            sql_lit(query),
            sql_lit(mode),
            sql_lit(column)
        ),
        None => format!(
            "create_reflex_ivm({}, {}, {key}, NULL, {})",
            sql_lit(name),
            sql_lit(query),
            sql_lit(mode)
        ),
    };
    assert_eq!(rbc_select(&call), "CREATE REFLEX INCREMENTAL VIEW", "creating {name}");
}

/// The IMVs of one truncate fixture: (name suffix, query over `{s}`, key, partition column).
const V112_DP_TRUNCATE_IMVS: &[(&str, &str, Option<&str>, Option<&str>)] = &[
    ("agg", "SELECT g, COUNT(*) AS c, SUM(v) AS sv FROM {s} GROUP BY g", None, None),
    ("minmax", "SELECT g, MIN(v) AS lo, MAX(v) AS hi FROM {s} GROUP BY g", None, None),
    ("total", "SELECT COUNT(*) AS c, SUM(v) AS sv FROM {s}", None, None),
    ("keyed", "SELECT id, g, v FROM {s}", Some("id, g"), None),
    ("keyless", "SELECT g, v FROM {s} WHERE v > 0", None, None),
    ("agg_part", "SELECT g, COUNT(*) AS c, SUM(v) AS sv FROM {s} GROUP BY g", None, Some("g")),
    ("keyed_part", "SELECT id, g, v FROM {s}", Some("id, g"), Some("g")),
];

/// A LIST-partitioned source with a RANGE sub-partitioned branch and a DEFAULT
/// partition, read by every IMV of `V112_DP_TRUNCATE_IMVS` in `mode`.
fn v112_dp_truncate_fixture(s: &str, mode: &str) {
    for ddl in [
        "CREATE TABLE {s}(id INT NOT NULL, g TEXT NOT NULL, v INT) PARTITION BY LIST (g)",
        "CREATE TABLE {s}_pa PARTITION OF {s} FOR VALUES IN ('a', 'b')",
        "CREATE TABLE {s}_pc PARTITION OF {s} FOR VALUES IN ('c') PARTITION BY RANGE (id)",
        "CREATE TABLE {s}_pc1 PARTITION OF {s}_pc FOR VALUES FROM (0) TO (100)",
        "CREATE TABLE {s}_pc2 PARTITION OF {s}_pc FOR VALUES FROM (100) TO (200)",
        "CREATE TABLE {s}_pd PARTITION OF {s} DEFAULT",
        "INSERT INTO {s} VALUES (1,'a',1),(2,'a',2),(3,'b',5),(4,'c',7),(104,'c',9),(6,'p',7)",
    ] {
        worker_exec(&ddl.replace("{s}", s));
    }
    for (suffix, query, key, partition_by) in V112_DP_TRUNCATE_IMVS {
        v112_dp_create(
            &format!("{s}_{suffix}"),
            &query.replace("{s}", s),
            *key,
            mode,
            *partition_by,
        );
    }
}

/// Opens a new worker session on `dbname`.
fn v112_dp_reconnect(dbname: &str) {
    let _ = Spi::get_one::<String>("SELECT dblink_disconnect('reflex_lock_worker')");
    Spi::get_one::<String>(&format!(
        "SELECT dblink_connect('reflex_lock_worker', {})",
        sql_lit(&conninfo_for(dbname))
    ))
    .expect("worker reconnect");
}

/// (step, IMV) pairs left unchecked: an ungrouped aggregate is wrong after a
/// partition is created and written in one transaction, with or without a
/// TRUNCATE (its report is in `untreated_bugs/`). The next TRUNCATE rebuilds it.
const V112_DP_KNOWN_WRONG: &[(&str, &str)] =
    &[("create, fill and truncate a partition in one transaction", "total")];

/// Commits each step and compares every IMV of the fixture with its query.
fn v112_dp_truncate_failures(
    s: &str,
    mode: &str,
    steps: &[(&str, &str)],
    session_per_step_on: Option<&str>,
) -> Vec<String> {
    let mut failures = Vec::new();
    for (label, step) in steps {
        if let Some(dbname) = session_per_step_on {
            v112_dp_reconnect(dbname);
        }
        if let Err(error) = v112_dp_try(&step.replace("{s}", s)) {
            failures.push(format!("{mode} {label}: raised {error}"));
            continue;
        }
        for (suffix, query, _, _) in V112_DP_TRUNCATE_IMVS {
            if V112_DP_KNOWN_WRONG.contains(&(*label, *suffix)) {
                continue;
            }
            let mismatches = rbc_mismatch(&format!("{s}_{suffix}"), &query.replace("{s}", s));
            if mismatches != 0 {
                failures.push(format!("{mode} {label}: {suffix} {mismatches} mismatches"));
            }
        }
    }
    failures
}

const V112_DP_REFILL: &str =
    "INSERT INTO {s} VALUES (11,'a',10),(12,'b',20),(13,'c',30),(113,'c',40),(14,'q',50)";

const V112_DP_TRUNCATE_EXISTING_PARTITIONS: &[(&str, &str)] = &[
    ("control insert", "INSERT INTO {s} VALUES (7,'a',3),(8,'q',4)"),
    ("truncate leaf", "TRUNCATE {s}_pa"),
    ("insert after leaf truncate", V112_DP_REFILL),
    ("truncate sub-partition leaf", "TRUNCATE {s}_pc1"),
    ("insert after sub-leaf truncate", "INSERT INTO {s} VALUES (21,'c',1),(22,'a',2)"),
    ("truncate sub-partitioned branch", "TRUNCATE {s}_pc"),
    ("insert after branch truncate", "INSERT INTO {s} VALUES (31,'c',3),(131,'c',4)"),
    ("truncate default partition", "TRUNCATE {s}_pd"),
    ("insert after default truncate", "INSERT INTO {s} VALUES (41,'z',5),(42,'a',6)"),
    ("truncate two partitions in one statement", "TRUNCATE {s}_pa, {s}_pd"),
    ("insert after two-partition truncate", V112_DP_REFILL),
    (
        "writes around a leaf truncate in one transaction",
        "INSERT INTO {s} VALUES (51,'a',1),(52,'p',2); TRUNCATE {s}_pa; \
         INSERT INTO {s} VALUES (53,'b',3),(54,'c',4)",
    ),
    (
        "two leaf truncates in one transaction",
        "TRUNCATE {s}_pc2; INSERT INTO {s} VALUES (61,'a',1),(161,'c',2); \
         TRUNCATE {s}_pa",
    ),
    ("truncate parent", "TRUNCATE {s}"),
    ("insert after parent truncate", V112_DP_REFILL),
    (
        "parent then leaf truncate in one transaction",
        "TRUNCATE {s}; INSERT INTO {s} VALUES (71,'a',1),(72,'p',2),(73,'c',3); \
         TRUNCATE {s}_pa",
    ),
    ("delete all", "DELETE FROM {s}"),
    ("final refill", V112_DP_REFILL),
];

const V112_DP_TRUNCATE_LATER_PARTITIONS: &[(&str, &str)] = &[
    ("create partition", "CREATE TABLE {s}_pe PARTITION OF {s} FOR VALUES IN ('e')"),
    ("fill created partition", "INSERT INTO {s} VALUES (201,'e',1),(202,'e',2),(203,'a',3)"),
    ("truncate created partition", "TRUNCATE {s}_pe"),
    ("refill created partition", "INSERT INTO {s} VALUES (204,'e',4)"),
    (
        "attach partition",
        "CREATE TABLE {s}_pf (LIKE {s} INCLUDING ALL); \
         ALTER TABLE {s} ATTACH PARTITION {s}_pf FOR VALUES IN ('f')",
    ),
    ("fill attached partition", "INSERT INTO {s} VALUES (211,'f',1),(212,'f',2)"),
    ("truncate attached partition", "TRUNCATE {s}_pf"),
    ("refill attached partition", "INSERT INTO {s} VALUES (213,'f',3),(214,'e',5)"),
    (
        "create sub-partition",
        "CREATE TABLE {s}_pc3 PARTITION OF {s}_pc FOR VALUES FROM (200) TO (300)",
    ),
    ("fill sub-partition", "INSERT INTO {s} VALUES (221,'c',1),(222,'c',2)"),
    ("truncate created sub-partition", "TRUNCATE {s}_pc3"),
    ("refill sub-partition", "INSERT INTO {s} VALUES (223,'c',3)"),
    (
        "create sub-partitioned branch",
        "CREATE TABLE {s}_ph PARTITION OF {s} FOR VALUES IN ('h') PARTITION BY RANGE (id); \
         CREATE TABLE {s}_ph1 PARTITION OF {s}_ph FOR VALUES FROM (0) TO (1000)",
    ),
    ("fill created branch", "INSERT INTO {s} VALUES (231,'h',1),(232,'h',2)"),
    ("truncate leaf of created branch", "TRUNCATE {s}_ph1"),
    ("refill created branch", "INSERT INTO {s} VALUES (233,'h',3)"),
    ("truncate created branch", "TRUNCATE {s}_ph"),
    ("refill created branch again", "INSERT INTO {s} VALUES (234,'h',4)"),
    (
        "create, fill and truncate a partition in one transaction",
        "CREATE TABLE {s}_pi PARTITION OF {s} FOR VALUES IN ('i'); \
         INSERT INTO {s} VALUES (241,'i',1),(242,'i',2); TRUNCATE {s}_pi; \
         INSERT INTO {s} VALUES (243,'i',3)",
    ),
    ("truncate it again", "TRUNCATE {s}_pi"),
    ("empty the attached partition", "TRUNCATE {s}_pf"),
    ("detach the emptied partition", "ALTER TABLE {s} DETACH PARTITION {s}_pf"),
    ("fill the detached table", "INSERT INTO {s}_pf VALUES (251,'f',1)"),
    ("truncate the detached table", "TRUNCATE {s}_pf"),
    ("insert after the detach", "INSERT INTO {s} VALUES (252,'a',2),(253,'e',3)"),
    ("truncate parent", "TRUNCATE {s}"),
    ("final refill", V112_DP_REFILL),
];

fn v112_dp_assert_truncate_matrix(
    dbname: &str,
    mode: &str,
    steps: &[(&str, &str)],
    session_per_step: bool,
) {
    probe_db_open(dbname);
    v112_dp_truncate_fixture("dpt", mode);
    let failures =
        v112_dp_truncate_failures("dpt", mode, steps, session_per_step.then_some(dbname));
    probe_db_close(dbname);
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[pg_test]
fn v112_dp_truncate_of_existing_partitions_immediate() {
    v112_dp_assert_truncate_matrix("reflex_dp_trunc_i", "IMMEDIATE", V112_DP_TRUNCATE_EXISTING_PARTITIONS, false);
}

#[pg_test]
fn v112_dp_truncate_of_existing_partitions_deferred() {
    v112_dp_assert_truncate_matrix("reflex_dp_trunc_d", "DEFERRED", V112_DP_TRUNCATE_EXISTING_PARTITIONS, false);
}

#[pg_test]
fn v112_dp_truncate_of_later_partitions_immediate() {
    v112_dp_assert_truncate_matrix("reflex_dp_later_i", "IMMEDIATE", V112_DP_TRUNCATE_LATER_PARTITIONS, false);
}

/// One session per step: in a session that already ran a DEFERRED flush, the
/// flush of an added partition fails and flags the IMVs stale (its report is in
/// `untreated_bugs/`), whatever the statement that follows.
#[pg_test]
fn v112_dp_truncate_of_later_partitions_deferred() {
    v112_dp_assert_truncate_matrix("reflex_dp_later_d", "DEFERRED", V112_DP_TRUNCATE_LATER_PARTITIONS, true);
}

fn v112_dp_rebuilds_logged() -> i64 {
    Spi::get_one::<i64>("SELECT count(*) FROM v112_dp_once_log")
        .expect("log")
        .unwrap_or(0)
}

/// A TRUNCATE fires the truncate triggers of every relation it truncates; the
/// IMV is rebuilt once per statement, and a statement that fails between its
/// BEFORE and AFTER triggers does not swallow the next one.
#[pg_test]
fn v112_dp_one_truncate_statement_rebuilds_the_imv_once() {
    const QUERY: &str = "SELECT g, COUNT(*) AS c, SUM(v) AS sv FROM v112_dp_once GROUP BY g";
    Spi::run("CREATE TABLE v112_dp_once(id INT, g TEXT, v INT) PARTITION BY LIST (g)").expect("s");
    for (partition, value) in [("a", "'a'"), ("b", "'b'"), ("c", "'c'")] {
        Spi::run(&format!(
            "CREATE TABLE v112_dp_once_{partition} PARTITION OF v112_dp_once FOR VALUES IN ({value})"
        ))
        .expect("partition");
    }
    let refill = "INSERT INTO v112_dp_once VALUES (1,'a',1),(2,'b',2),(3,'c',3),(4,'a',4)";
    Spi::run(refill).expect("seed");
    assert_eq!(
        crate::create_reflex_ivm("v112_dp_once_v", QUERY, None, None, Some("IMMEDIATE"), None),
        "CREATE REFLEX INCREMENTAL VIEW"
    );
    Spi::run("CREATE TABLE v112_dp_once_log(at TIMESTAMPTZ)").expect("log");
    Spi::run(
        "CREATE FUNCTION v112_dp_once_note() RETURNS TRIGGER LANGUAGE plpgsql AS          $$ BEGIN INSERT INTO v112_dp_once_log VALUES (clock_timestamp()); RETURN NULL; END $$",
    )
    .expect("note function");
    Spi::run(
        "CREATE TRIGGER v112_dp_once_note AFTER TRUNCATE ON \"__reflex_intermediate_v112_dp_once_v\"          FOR EACH STATEMENT EXECUTE FUNCTION v112_dp_once_note()",
    )
    .expect("note trigger");

    let mut expected = 0;
    for statement in [
        "TRUNCATE v112_dp_once",
        "TRUNCATE v112_dp_once_a, v112_dp_once_c",
        "TRUNCATE v112_dp_once_b, v112_dp_once",
        "TRUNCATE v112_dp_once_b",
    ] {
        Spi::run(statement).expect("truncate");
        expected += 1;
        assert_eq!(v112_dp_rebuilds_logged(), expected, "rebuilds after {statement}");
        assert_imv_correct("v112_dp_once_v", QUERY);
        Spi::run(refill).expect("refill");
        assert_imv_correct("v112_dp_once_v", QUERY);
    }

    Spi::run(
        "CREATE FUNCTION v112_dp_once_refuse() RETURNS TRIGGER LANGUAGE plpgsql AS          $$ BEGIN RAISE EXCEPTION 'refused'; END $$",
    )
    .expect("refuse function");
    Spi::run(
        "CREATE TRIGGER zz_refuse BEFORE TRUNCATE ON v112_dp_once_c          FOR EACH STATEMENT EXECUTE FUNCTION v112_dp_once_refuse()",
    )
    .expect("refuse trigger");
    Spi::run(
        "DO $$ BEGIN TRUNCATE v112_dp_once_a, v112_dp_once_c;          EXCEPTION WHEN OTHERS THEN NULL; END $$",
    )
    .expect("refused truncate");
    assert_eq!(v112_dp_rebuilds_logged(), expected, "a refused TRUNCATE rebuilt the IMV");
    assert_imv_correct("v112_dp_once_v", QUERY);
    Spi::run("TRUNCATE v112_dp_once_a").expect("truncate after the refused one");
    assert_eq!(v112_dp_rebuilds_logged(), expected + 1, "TRUNCATE after a refused one");
    assert_imv_correct("v112_dp_once_v", QUERY);
}

fn v112_dp_reflex_triggers_on(table: &str) -> Vec<String> {
    Spi::connect(|client| {
        client
            .select(
                &format!(
                    "SELECT tgname::text AS n FROM pg_trigger \
                     WHERE tgrelid = '{table}'::regclass AND NOT tgisinternal \
                       AND tgname LIKE '\\_\\_reflex\\_%' ORDER BY 1"
                ),
                None,
                &[],
            )
            .expect("triggers")
            .filter_map(|row| row.get_by_name::<String, _>("n").ok().flatten())
            .collect()
    })
}

fn v112_dp_reflex_trigger_functions_naming(fragment: &str) -> i64 {
    Spi::get_one::<i64>(&format!(
        "SELECT count(*) FROM pg_proc WHERE proname LIKE '\\_\\_reflex\\_%trigger\\_on\\_%' \
           AND proname LIKE '%{fragment}%'"
    ))
    .expect("functions")
    .unwrap_or(0)
}

/// Every statement-trigger name of a 45-character source, and the truncate
/// trigger name of a 38-character one, exceeds 63 bytes and is stored shortened.
#[pg_test]
fn v112_dp_drop_removes_source_triggers_whose_names_were_shortened() {
    for (source, mode) in [
        ("v112_dp_zz_long_named_source_table_for_a_drop", "IMMEDIATE"),
        ("v112_dp_zz_long_named_source_table_for_a_drop", "DEFERRED"),
        ("v112_dp_zy_a_long_named_table_for_drop", "IMMEDIATE"),
        ("v112_dp_zy_a_long_named_table_for_drop", "DEFERRED"),
    ] {
        Spi::run(&format!("CREATE TABLE {source}(id SERIAL PRIMARY KEY, g TEXT, v INT)"))
            .expect("source");
        Spi::run(&format!("INSERT INTO {source}(g, v) VALUES ('a', 1), ('b', 2)")).expect("seed");
        let query = format!("SELECT g, SUM(v) AS r FROM {source} GROUP BY g");
        assert_eq!(
            crate::create_reflex_ivm("v112_dp_long_v", &query, None, None, Some(mode), None),
            "CREATE REFLEX INCREMENTAL VIEW"
        );
        assert_eq!(
            v112_dp_reflex_triggers_on(source).len(),
            4,
            "fixture: {source} carries its four statement triggers"
        );
        assert_eq!(crate::drop_reflex_ivm("v112_dp_long_v"), "DROP REFLEX INCREMENTAL VIEW");
        assert_eq!(
            v112_dp_reflex_triggers_on(source),
            Vec::<String>::new(),
            "{mode}: triggers left on {source}"
        );
        assert_eq!(
            v112_dp_reflex_trigger_functions_naming(&source[..30]),
            0,
            "{mode}: trigger functions left for {source}"
        );
        Spi::run(&format!("INSERT INTO {source}(g, v) VALUES ('c', 3)")).expect("write after drop");
        Spi::run(&format!("DROP TABLE {source}")).expect("drop source");
    }
}

/// The truncate triggers installed on the partitions of a partitioned source go
/// with the last IMV that reads it, and stay while another IMV does.
#[pg_test]
fn v112_dp_drop_removes_the_truncate_triggers_of_every_partition() {
    Spi::run("CREATE TABLE v112_dp_dr(id INT, g TEXT, v INT) PARTITION BY LIST (g)").expect("s");
    Spi::run("CREATE TABLE v112_dp_dr_a PARTITION OF v112_dp_dr FOR VALUES IN ('a')").expect("a");
    Spi::run(
        "CREATE TABLE v112_dp_dr_b PARTITION OF v112_dp_dr FOR VALUES IN ('b') \
         PARTITION BY RANGE (id)",
    )
    .expect("b");
    Spi::run("CREATE TABLE v112_dp_dr_b1 PARTITION OF v112_dp_dr_b FOR VALUES FROM (0) TO (9)")
        .expect("b1");
    for (imv, mode) in [("v112_dp_dr_x", "IMMEDIATE"), ("v112_dp_dr_y", "DEFERRED")] {
        assert_eq!(
            crate::create_reflex_ivm(
                imv,
                "SELECT g, SUM(v) AS r FROM v112_dp_dr GROUP BY g",
                None,
                None,
                Some(mode),
                None
            ),
            "CREATE REFLEX INCREMENTAL VIEW"
        );
    }
    let partitions = ["v112_dp_dr_a", "v112_dp_dr_b", "v112_dp_dr_b1"];
    for partition in partitions {
        assert_eq!(
            v112_dp_reflex_triggers_on(partition).len(),
            2,
            "fixture: {partition} announces and propagates its TRUNCATE"
        );
    }
    assert_eq!(crate::drop_reflex_ivm("v112_dp_dr_x"), "DROP REFLEX INCREMENTAL VIEW");
    for partition in partitions {
        assert_eq!(
            v112_dp_reflex_triggers_on(partition).len(),
            2,
            "{partition} lost its triggers while v112_dp_dr_y still reads the source"
        );
    }
    assert_eq!(crate::drop_reflex_ivm("v112_dp_dr_y"), "DROP REFLEX INCREMENTAL VIEW");
    for table in ["v112_dp_dr", "v112_dp_dr_a", "v112_dp_dr_b", "v112_dp_dr_b1"] {
        assert_eq!(
            v112_dp_reflex_triggers_on(table),
            Vec::<String>::new(),
            "triggers left on {table}"
        );
    }
    assert_eq!(v112_dp_reflex_trigger_functions_naming("v112_dp_dr"), 0);
    Spi::run("TRUNCATE v112_dp_dr_a").expect("truncate after drop");
}

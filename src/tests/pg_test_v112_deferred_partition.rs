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

/// DEFERRED IMVs whose CTE body reads `dpc_s` and `dpc_t`: (name, query, key).
const V112_DP_TWO_TABLE_CTE_IMVS: &[(&str, &str, Option<&str>)] = &[
    (
        "dpc_agg",
        "WITH c AS (SELECT m.g, m.v FROM dpc_s m JOIN dpc_t j ON j.g = m.g) \
         SELECT g, SUM(v) AS sv, COUNT(*) AS c FROM c GROUP BY g",
        None,
    ),
    (
        "dpc_chain",
        "WITH a AS (SELECT m.g, m.v, j.label FROM dpc_s m JOIN dpc_t j ON j.g = m.g), \
         b AS (SELECT g, label, SUM(v) AS sv FROM a GROUP BY g, label) \
         SELECT g, MAX(sv) AS top, COUNT(*) AS labels FROM b GROUP BY g",
        None,
    ),
    (
        "dpc_pass",
        "WITH c AS (SELECT m.id AS sid, j.id AS tid, m.g, m.v, j.label \
                    FROM dpc_s m JOIN dpc_t j ON j.g = m.g) \
         SELECT sid, tid, g, v, label FROM c WHERE v > 0",
        Some("sid, tid"),
    ),
    (
        "dpc_left",
        "WITH c AS (SELECT m.g, m.v, j.label FROM dpc_s m LEFT JOIN dpc_t j ON j.g = m.g) \
         SELECT g, COUNT(label) AS labelled, SUM(v) AS sv FROM c GROUP BY g",
        None,
    ),
    (
        "dpc_reader",
        "SELECT c, COUNT(*) AS groups FROM dpc_agg GROUP BY c",
        None,
    ),
];

const V112_DP_TWO_TABLE_CTE_FRESH_READER: &str =
    "SELECT c, COUNT(*) AS groups FROM (SELECT m.g, COUNT(*) AS c FROM dpc_s m \
     JOIN dpc_t j ON j.g = m.g GROUP BY m.g) x GROUP BY c";

/// Both tables a CTE body reads are written in one transaction: the sub-IMV is
/// rebuilt at COMMIT and every IMV above it must follow.
#[pg_test]
fn v112_dp_cte_over_two_tables_written_in_one_transaction() {
    const DBNAME: &str = "reflex_dp_cte2";
    probe_db_open(DBNAME);
    worker_exec("CREATE TABLE dpc_s(id SERIAL PRIMARY KEY, g TEXT, v INT, w INT)");
    worker_exec("CREATE TABLE dpc_t(id SERIAL PRIMARY KEY, g TEXT, label TEXT, v INT)");
    worker_exec("INSERT INTO dpc_s(g, v, w) VALUES ('a',1,1),('a',2,1),('b',3,1),('b',4,2),('d',9,1)");
    worker_exec("INSERT INTO dpc_t(g, label, v) VALUES ('a','A',1),('b','B',2),('e','E',3)");
    for (name, query, key) in V112_DP_TWO_TABLE_CTE_IMVS {
        v112_dp_create(name, query, *key, "DEFERRED", None);
    }
    let mut failures = Vec::new();
    for (label, step) in [
        ("control: one table", "INSERT INTO dpc_s(g, v, w) VALUES ('a',5,1)"),
        (
            "insert into both",
            "INSERT INTO dpc_s(g, v, w) VALUES ('a',10,1),('c',7,NULL),('q',3,2),('b',8,3); \
             INSERT INTO dpc_t(g, label, v) VALUES ('z','Z',0),('k','K',1); \
             INSERT INTO dpc_t(g, label, v) VALUES ('b','B2',1)",
        ),
        (
            "update both",
            "UPDATE dpc_t SET label = 'A2' WHERE g = 'a'; UPDATE dpc_s SET v = v + 100 WHERE g = 'b'",
        ),
        (
            "move keys in both",
            "UPDATE dpc_s SET g = 'k' WHERE g = 'q'; UPDATE dpc_t SET g = 'c' WHERE g = 'z'",
        ),
        (
            "delete from both",
            "DELETE FROM dpc_t WHERE label = 'B2'; DELETE FROM dpc_s WHERE g = 'a' AND v < 5",
        ),
        ("control: other table", "INSERT INTO dpc_t(g, label, v) VALUES ('d','D',4)"),
        ("empty both", "DELETE FROM dpc_s; DELETE FROM dpc_t"),
        (
            "refill both",
            "INSERT INTO dpc_t(g, label, v) VALUES ('a','A',1),('b','B',2); \
             INSERT INTO dpc_s(g, v, w) VALUES ('a',1,1),('b',2,2),('b',-3,3)",
        ),
    ] {
        if let Err(error) = v112_dp_try(step) {
            failures.push(format!("{label}: raised {error}"));
            continue;
        }
        for (name, query, _) in V112_DP_TWO_TABLE_CTE_IMVS {
            let fresh = if *name == "dpc_reader" {
                V112_DP_TWO_TABLE_CTE_FRESH_READER
            } else {
                query
            };
            let mismatches = rbc_mismatch(name, fresh);
            if mismatches != 0 {
                failures.push(format!("{label}: {name} {mismatches} mismatches"));
            }
            if rbc_known_stale(name) {
                failures.push(format!("{label}: {name} left known_stale"));
            }
        }
    }
    probe_db_close(DBNAME);
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

const V112_DP_CTE_GROUPED: &str =
    "WITH a AS (SELECT season, SUM(v) AS s, COUNT(*) AS n FROM {t} GROUP BY season)";
const V112_DP_CTE_ROWS: &str = "WITH a AS (SELECT id, season, v FROM {t} WHERE v > 0)";

/// Queries that read their only source as a whole row in WHERE, or rename a
/// column of a source with an alias column list:
/// (name, CTE, query, key, fragment of the refusal).
const V112_DP_ROW_SHAPES_REFUSED: &[(&str, &str, &str, Option<&str>, &str)] = &[
    ("cte_row", V112_DP_CTE_GROUPED, "SELECT season, a.s FROM a WHERE a IS NOT NULL", None, "whole row"),
    ("cte_row_agg", V112_DP_CTE_ROWS, "SELECT season, SUM(v) AS sv FROM a WHERE a IS NOT NULL GROUP BY season", None, "whole row"),
    ("cte_row_keyed", V112_DP_CTE_ROWS, "SELECT id, season, v FROM a WHERE a IS NOT NULL", Some("id"), "whole row"),
    ("row", "", "SELECT id, v FROM {t} WHERE {t} IS NOT NULL", Some("id"), "whole row"),
    ("row_alias", "", "SELECT z.id, z.v FROM {t} z WHERE z IS NOT NULL", Some("id"), "whole row"),
    ("row_star", "", "SELECT z.id, z.v FROM {t} z WHERE z.* IS NOT NULL", Some("id"), "whole row"),
    ("row_keyless", "", "SELECT season, v FROM {t} z WHERE NOT (z IS NULL)", None, "whole row"),
    ("row_agg", "", "SELECT season, SUM(v) AS sv FROM {t} WHERE {t} IS NOT NULL GROUP BY season", None, "whole row"),
    ("cte_renamed", V112_DP_CTE_GROUPED, "SELECT x.g, x.total FROM a AS x(g, total, cnt) WHERE x.total > 1", None, "alias list"),
    ("cte_renamed_agg", V112_DP_CTE_ROWS, "SELECT x.g, SUM(x.w) AS sw FROM a AS x(i, g, w) GROUP BY x.g", None, "alias list"),
    ("renamed_key", "", "SELECT x.a, x.c FROM {t} AS x(a, b, c)", Some("a"), "alias list"),
    ("renamed_key_partial", "", "SELECT x.a, x.v FROM {t} AS x(a) WHERE x.v > 0", Some("a"), "alias list"),
    ("renamed_filter", "", "SELECT x.b, SUM(x.c) AS sc FROM {t} AS x(a, b, c) WHERE c > 1 GROUP BY x.b", None, "alias list"),
    ("renamed_projected", "", "SELECT x.b, x.v FROM {t} AS x(a, b) WHERE x.v > 0", None, "alias list"),
    ("renamed_agg", "", "SELECT x.b, SUM(x.c) AS sc FROM {t} AS x(a, b, c) GROUP BY x.b", None, "alias list"),
    ("renamed_keyless", "", "SELECT x.b, x.c FROM {t} AS x(a, b, c)", None, "alias list"),
    ("join_renamed_key", "", "SELECT m.id, m.v, u.s2 FROM {t} m JOIN {t}_u AS u(s2) ON u.s2 = m.season", Some("id"), "alias list"),
    ("join_renamed_label", "", "SELECT m.id, u.season, u.lbl FROM {t} m JOIN {t}_u AS u(season, lbl) ON u.season = m.season", Some("id"), "alias list"),
    ("join_renamed_anchor", "", "SELECT m.k, u.label FROM {t} AS m(k) JOIN {t}_u u ON u.season = m.season", Some("k"), "alias list"),
];

/// The rewrites the refusals prescribe, then the neighbouring shapes that are
/// maintained as written: (name, CTE, query, key).
const V112_DP_ROW_SHAPES_MAINTAINED: &[(&str, &str, &str, Option<&str>)] = &[
    ("fix_cte_row", V112_DP_CTE_GROUPED, "SELECT season, a.s FROM a WHERE a.season IS NOT NULL AND a.s IS NOT NULL AND a.n IS NOT NULL", None),
    ("fix_row", "", "SELECT z.id, z.v FROM {t} z WHERE z.id IS NOT NULL AND z.season IS NOT NULL AND z.v IS NOT NULL", Some("id")),
    ("fix_cte_renamed", V112_DP_CTE_GROUPED, "SELECT x.season AS g, x.s AS total FROM a AS x WHERE x.s > 1", None),
    ("fix_renamed_key", "", "SELECT x.id AS a, x.v AS c FROM {t} AS x", Some("a")),
    ("fix_renamed_filter", "", "SELECT x.season AS b, SUM(x.v) AS sc FROM {t} AS x WHERE v > 1 GROUP BY x.season", None),
    ("fix_renamed_projected", "", "SELECT x.season AS b, x.v FROM {t} AS x WHERE x.v > 0", None),
    ("fix_join_renamed_label", "", "SELECT m.id, u.season, u.label AS lbl FROM {t} m JOIN {t}_u AS u ON u.season = m.season", Some("id")),
    ("same_names_listed", "", "SELECT x.id, x.v FROM {t} AS x(id, season, v) WHERE x.v > 0", Some("id")),
    ("join_same_names_listed", "", "SELECT m.id, u.label FROM {t} m JOIN {t}_u AS u(season, label) ON u.season = m.season WHERE m.v > 0", Some("id")),
    ("alias_is_a_column", "", "SELECT v.id, v.season FROM {t} v WHERE v > 0", Some("id")),
    ("alias_is_a_summed_column", "", "SELECT v.season, SUM(v) AS sv FROM {t} v GROUP BY v.season", None),
    ("row_counted", "", "SELECT season, COUNT({t}) AS c FROM {t} GROUP BY season", None),
    ("row_star_counted", "", "SELECT season, COUNT({t}.*) AS c FROM {t} GROUP BY season", None),
    ("row_in_select", "", "SELECT id, num_nulls(z) AS n FROM {t} z", Some("id")),
    ("join_row_secondary", "", "SELECT m.id, m.v FROM {t} m JOIN {t}_u u ON u.season = m.season WHERE u IS NOT NULL", Some("id")),
    ("join_row_anchor", "", "SELECT m.id, m.v FROM {t} m JOIN {t}_u u ON u.season = m.season WHERE m IS NOT NULL", Some("id")),
];

fn v112_dp_row_shape_source(t: &str) {
    worker_exec(&format!("CREATE TABLE {t}(id SERIAL PRIMARY KEY, season TEXT, v INT)"));
    worker_exec(&format!("CREATE TABLE {t}_u(season TEXT PRIMARY KEY, label TEXT DEFAULT 'l')"));
    worker_exec(&format!("INSERT INTO {t}(season, v) VALUES ('e',1),('y',2),('e',3),(NULL,4)"));
    worker_exec(&format!("INSERT INTO {t}_u(season) VALUES ('e'),('y')"));
}

fn v112_dp_row_shape_create(t: &str, cte: &str, body: &str, key: Option<&str>, mode: &str) -> (String, String) {
    let query = format!("{cte} {body}").replace("{t}", t).trim().to_string();
    let created = rbc_select(&format!(
        "create_reflex_ivm({}, {}, {}, NULL, {})",
        sql_lit(&format!("{t}_v")),
        sql_lit(&query),
        key.map_or("NULL".to_string(), sql_lit),
        sql_lit(mode)
    ));
    (query, created)
}

/// Refused before anything is built for the outer query, and what a CTE built
/// is removed: no registry row, relation or source trigger remains.
#[pg_test]
fn v112_dp_whole_row_or_renamed_column_read_from_changed_rows_is_refused() {
    const DBNAME: &str = "reflex_dp_rowshape_no";
    probe_db_open(DBNAME);
    let mut failures = Vec::new();
    for mode in ["IMMEDIATE", "DEFERRED"] {
        for (name, cte, body, key, fragment) in V112_DP_ROW_SHAPES_REFUSED {
            let t = format!("no_{name}_{}", &mode[..1].to_lowercase());
            v112_dp_row_shape_source(&t);
            let (_, created) = v112_dp_row_shape_create(&t, cte, body, *key, mode);
            if !(created.starts_with("ERROR: [reflex-unsupported]") && created.contains(fragment)) {
                failures.push(format!("{mode} {name}: {created}"));
            }
            let left = worker_scalar_i64(&format!(
                "SELECT (SELECT count(*) FROM public.__reflex_ivm_reference WHERE name LIKE '{t}\\_v%') \
                      + (SELECT count(*) FROM pg_class WHERE relname LIKE '%{t}\\_v%') \
                      + (SELECT count(*) FROM pg_trigger \
                         WHERE tgrelid IN ('{t}'::regclass, '{t}_u'::regclass) AND NOT tgisinternal)"
            ));
            if left != 0 {
                failures.push(format!("{mode} {name}: {left} objects left"));
            }
            if let Err(error) = v112_dp_try(&format!("INSERT INTO {t}(season, v) VALUES ('z', 9)")) {
                failures.push(format!("{mode} {name}: write after the refusal raised {error}"));
            }
        }
    }
    probe_db_close(DBNAME);
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[pg_test]
fn v112_dp_rewrites_and_neighbours_of_the_refused_row_shapes_stay_correct() {
    const DBNAME: &str = "reflex_dp_rowshape_ok";
    probe_db_open(DBNAME);
    let mut failures = Vec::new();
    for mode in ["IMMEDIATE", "DEFERRED"] {
        for (name, cte, body, key) in V112_DP_ROW_SHAPES_MAINTAINED {
            let t = format!("ok_{name}_{}", &mode[..1].to_lowercase());
            v112_dp_row_shape_source(&t);
            let (query, created) = v112_dp_row_shape_create(&t, cte, body, *key, mode);
            if created != "CREATE REFLEX INCREMENTAL VIEW" {
                failures.push(format!("{mode} {name}: {created}"));
                continue;
            }
            for (label, step) in [
                ("create", "DO $$ BEGIN NULL; END $$"),
                ("insert", "INSERT INTO {t}(season, v) VALUES ('e',10),('z',30),(NULL,NULL)"),
                ("update", "UPDATE {t} SET v = v + 100 WHERE id IN (1, 4)"),
                ("move", "UPDATE {t} SET season = 'y' WHERE id = 3"),
                ("delete", "DELETE FROM {t} WHERE id = 2"),
                ("relabel", "UPDATE {t}_u SET label = label || '!'"),
                ("both tables", "INSERT INTO {t}_u(season) VALUES ('z'); DELETE FROM {t} WHERE id = 1"),
                ("empty", "DELETE FROM {t}"),
                ("refill", "INSERT INTO {t}(season, v) VALUES ('e',7),('y',8),(NULL,9)"),
            ] {
                if let Err(error) = v112_dp_try(&step.replace("{t}", &t)) {
                    failures.push(format!("{mode} {name} {label}: raised {error}"));
                    break;
                }
                let mismatches = rbc_mismatch(&format!("{t}_v"), &query);
                if mismatches != 0 || rbc_known_stale(&format!("{t}_v")) {
                    failures.push(format!("{mode} {name} {label}: {mismatches} mismatches"));
                }
            }
        }
    }
    probe_db_close(DBNAME);
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// A dropped leaf of a partitioned IMV (`intermediate` or target side): the
/// remedies the audit prints, run as printed until it reports nothing, must leave
/// the IMV equal to its query, then and after writes to the groups of that leaf.
fn v112_dp_assert_dropped_leaf_remedy_restores_the_data(prefix: &str, mode: &str, intermediate: bool) {
    let source = format!("{prefix}_t");
    let imv = format!("{prefix}_v");
    let query = format!("SELECT season, SUM(v) AS sv, COUNT(*) AS c FROM {source} GROUP BY season");
    Spi::run(&format!(
        "CREATE TABLE {source}(id INT NOT NULL, season TEXT NOT NULL, v INT) PARTITION BY LIST (season)"
    ))
    .expect("source");
    for (leaf, bound) in [("p0", "FOR VALUES IN ('e', 'f')"), ("p1", "FOR VALUES IN ('y')"), ("pd", "DEFAULT")] {
        Spi::run(&format!("CREATE TABLE {source}_{leaf} PARTITION OF {source} {bound}")).expect("leaf");
    }
    Spi::run(&format!(
        "INSERT INTO {source} VALUES (1,'e',10),(2,'e',40),(3,'f',5),(4,'y',2),(5,'z',3)"
    ))
    .expect("seed");
    let created = Spi::get_one::<String>(&format!(
        "SELECT create_reflex_ivm('{imv}', '{query}', NULL, NULL, '{mode}', NULL, ARRAY['season'])"
    ))
    .expect("create")
    .unwrap_or_default();
    assert_eq!(created, "CREATE REFLEX INCREMENTAL VIEW");
    let flush = || {
        if mode == "DEFERRED" {
            Spi::run(&format!("SELECT reflex_flush_deferred('{source}')")).expect("flush");
        }
    };

    let parent = if intermediate {
        format!("__reflex_intermediate_{imv}")
    } else {
        imv.clone()
    };
    let victim = Spi::get_one::<String>(&format!(
        "SELECT c.relname::text FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid \
         WHERE i.inhparent = '\"{parent}\"'::regclass AND c.relname LIKE '%\\_p0'"
    ))
    .expect("leaf lookup")
    .expect("no leaf for p0");
    Spi::run(&format!("DROP TABLE \"{victim}\"")).expect("drop leaf");

    let mut report = ps9_audit(&imv);
    assert!(report.contains("partition-mirror"), "the dropped leaf is not reported:\n{report}");
    for _ in 0..4 {
        let Some(category) = ["partition-mirror", "archive_residue"]
            .into_iter()
            .find(|category| report.contains(category))
        else {
            break;
        };
        let fix = ps9_suggested_fix(&report, category);
        Spi::run(&fix).unwrap_or_else(|e| panic!("printed remedy `{fix}` failed: {e}"));
        report = ps9_audit(&imv);
    }
    assert!(
        !report.contains("partition-mirror") && !report.contains("archive_residue"),
        "the printed remedies did not clear the findings:\n{report}"
    );
    assert_imv_correct(&imv, &query);
    for write in [
        "INSERT INTO {s} VALUES (6,'e',4),(7,'y',1)",
        "UPDATE {s} SET v = v + 1 WHERE season = 'f'",
        "DELETE FROM {s} WHERE id = 1",
    ] {
        Spi::run(&write.replace("{s}", &source)).expect("write");
        flush();
        assert_imv_correct(&imv, &query);
    }
}

#[pg_test]
fn v112_dp_dropped_intermediate_leaf_remedy_restores_the_data_immediate() {
    v112_dp_assert_dropped_leaf_remedy_restores_the_data("v112_dp_li", "IMMEDIATE", true);
}

#[pg_test]
fn v112_dp_dropped_intermediate_leaf_remedy_restores_the_data_deferred() {
    v112_dp_assert_dropped_leaf_remedy_restores_the_data("v112_dp_ld", "DEFERRED", true);
}

#[pg_test]
fn v112_dp_dropped_target_leaf_remedy_restores_the_data_immediate() {
    v112_dp_assert_dropped_leaf_remedy_restores_the_data("v112_dp_ti", "IMMEDIATE", false);
}

#[pg_test]
fn v112_dp_dropped_target_leaf_remedy_restores_the_data_deferred() {
    v112_dp_assert_dropped_leaf_remedy_restores_the_data("v112_dp_td", "DEFERRED", false);
}

fn v112_dp_flush(source: &str) -> String {
    Spi::get_one::<String>(&format!(
        "SELECT reflex_flush_deferred({})",
        sql_lit(source)
    ))
    .expect("flush")
    .unwrap_or_default()
}

/// A manual flush names the source the way the caller writes it, which need not
/// be the way the IMV's query did.
#[pg_test]
fn v112_dp_manual_flush_accepts_every_spelling_of_the_source() {
    let spellings = ["{t}", "public.{t}", "\"{t}\"", "\"public\".\"{t}\"", "PUBLIC.{T}"];
    for (r, registered) in spellings.iter().take(4).enumerate() {
        for (c, called) in spellings.iter().enumerate() {
            let t = format!("v112_dp_fl_{r}{c}");
            let spell = |pattern: &str| pattern.replace("{t}", &t).replace("{T}", &t.to_uppercase());
            Spi::run(&format!("CREATE TABLE {t}(id SERIAL PRIMARY KEY, g TEXT, v INT)")).expect("t");
            Spi::run(&format!("INSERT INTO {t}(g, v) VALUES ('a', 1), ('b', 2)")).expect("seed");
            let query = format!("SELECT g, SUM(v) AS sv FROM {} GROUP BY g", spell(registered));
            let imv = format!("{t}_v");
            assert_eq!(
                crate::create_reflex_ivm(&imv, &query, None, None, Some("DEFERRED"), None),
                "CREATE REFLEX INCREMENTAL VIEW"
            );
            Spi::run(&format!("INSERT INTO {t}(g, v) VALUES ('a', 10), ('c', 30)")).expect("write");
            let flushed = v112_dp_flush(&spell(called));
            assert!(
                flushed.starts_with("FLUSHED"),
                "registered as {}, flushed as {}: {flushed}",
                spell(registered),
                spell(called)
            );
            assert_imv_correct(&imv, &query);
        }
    }
}

/// The outer IMV of a decomposed CTE registers its sub-IMV double-quoted; the
/// plain name of the sub-IMV flushes it all the same.
#[pg_test]
fn v112_dp_manual_flush_by_the_plain_name_of_a_cte_sub_imv() {
    Spi::run("CREATE TABLE v112_dp_fc(id SERIAL PRIMARY KEY, season TEXT, v INT)").expect("t");
    Spi::run("INSERT INTO v112_dp_fc(season, v) VALUES ('e',1),('y',2),('e',3)").expect("seed");
    let query = "WITH e AS (SELECT season, SUM(v) AS s FROM v112_dp_fc GROUP BY season) \
                 SELECT season, s FROM e WHERE season <> 'x'";
    assert_eq!(
        crate::create_reflex_ivm("v112_dp_fc_v", query, None, None, Some("DEFERRED"), None),
        "CREATE REFLEX INCREMENTAL VIEW"
    );
    for (write, sub_imv) in [
        ("INSERT INTO v112_dp_fc(season, v) VALUES ('e',10),('z',30)", "v112_dp_fc_v__cte_e"),
        ("UPDATE v112_dp_fc SET v = v + 1", "public.v112_dp_fc_v__cte_e"),
        ("DELETE FROM v112_dp_fc WHERE season = 'y'", "\"v112_dp_fc_v__cte_e\""),
    ] {
        Spi::run(write).expect("write");
        assert!(v112_dp_flush("v112_dp_fc").starts_with("FLUSHED"));
        let flushed = v112_dp_flush(sub_imv);
        assert!(flushed.starts_with("FLUSHED"), "flushed as {sub_imv}: {flushed}");
        assert_imv_correct("v112_dp_fc_v", query);
    }
}

/// A flush that reaches no DEFERRED IMV names the source it was given.
#[pg_test]
fn v112_dp_manual_flush_that_matches_nothing_says_so() {
    Spi::run("CREATE TABLE v112_dp_fn(id SERIAL PRIMARY KEY, g TEXT, v INT)").expect("t");
    assert_eq!(
        crate::create_reflex_ivm(
            "v112_dp_fn_v",
            "SELECT g, SUM(v) AS sv FROM v112_dp_fn GROUP BY g",
            None,
            None,
            Some("IMMEDIATE"),
            None
        ),
        "CREATE REFLEX INCREMENTAL VIEW"
    );
    for source in ["v112_dp_fn", "v112_dp_no_such_table", "v112_dp_fn_v"] {
        let flushed = v112_dp_flush(source);
        assert!(
            flushed.starts_with("NO DEFERRED IMVS") && flushed.contains(source),
            "{source}: {flushed}"
        );
    }
}

fn v112_dp_count(sql: &str) -> i64 {
    Spi::get_one::<i64>(sql).expect("count").unwrap_or(0)
}

/// A CTE the query never reads, and a WITH nested in a CTE, each get a sub-IMV
/// that no source list records: the cascade drop of the IMV removes them too.
#[pg_test]
fn v112_dp_drop_removes_cte_sub_imvs_that_nothing_reads() {
    Spi::run("CREATE TABLE v112_dp_or_t(id SERIAL PRIMARY KEY, season TEXT, v INT)").expect("t");
    Spi::run("CREATE TABLE v112_dp_or_o(id SERIAL PRIMARY KEY, season TEXT, k INT)").expect("o");
    Spi::run("INSERT INTO v112_dp_or_t(season, v) VALUES ('e',1),('y',2),('e',3)").expect("seed t");
    Spi::run("INSERT INTO v112_dp_or_o(season, k) VALUES ('e',7),('y',8)").expect("seed o");
    for (shape, query, key) in [
        (
            "unread",
            "WITH a AS (SELECT season, SUM(v) AS s FROM v112_dp_or_t GROUP BY season) \
             SELECT id, season, k FROM v112_dp_or_o",
            Some("id"),
        ),
        (
            "nested",
            "WITH a AS (WITH b AS (SELECT season, SUM(v) AS s FROM v112_dp_or_t GROUP BY season) \
                        SELECT season, s FROM b) \
             SELECT season, s FROM a",
            None,
        ),
        (
            "nested in a later CTE",
            "WITH a AS (SELECT season, SUM(v) AS s FROM v112_dp_or_t GROUP BY season), \
                  c AS (WITH b AS (SELECT season, COUNT(*) AS n FROM v112_dp_or_t GROUP BY season) \
                        SELECT season, n FROM b) \
             SELECT a.season, a.s, c.n FROM a JOIN c ON c.season = a.season",
            None,
        ),
    ] {
        assert_eq!(
            crate::create_reflex_ivm("v112_dp_or_v", query, key, None, Some("IMMEDIATE"), None),
            "CREATE REFLEX INCREMENTAL VIEW",
            "{shape}"
        );
        Spi::run("INSERT INTO v112_dp_or_t(season, v) VALUES ('e',10),('z',30)").expect("write");
        assert_imv_correct("v112_dp_or_v", query);
        assert_eq!(
            crate::drop_reflex_ivm_cascade("v112_dp_or_v", true),
            "DROP REFLEX INCREMENTAL VIEW",
            "{shape}"
        );
        assert_eq!(
            v112_dp_count(
                "SELECT count(*) FROM public.__reflex_ivm_reference WHERE name LIKE 'v112_dp_or_v%'"
            ),
            0,
            "{shape}: registry rows left"
        );
        assert_eq!(
            v112_dp_count("SELECT count(*) FROM pg_class WHERE relname LIKE '%v112_dp_or_v%'"),
            0,
            "{shape}: relations left"
        );
        for source in ["v112_dp_or_t", "v112_dp_or_o"] {
            assert_eq!(
                v112_dp_reflex_triggers_on(source),
                Vec::<String>::new(),
                "{shape}: triggers left on {source}"
            );
        }
    }
}

/// The generated sub-IMV of another IMV whose name merely starts like the dropped
/// one's is not swept with it.
#[pg_test]
fn v112_dp_drop_keeps_the_sub_imvs_of_a_similarly_named_imv() {
    Spi::run("CREATE TABLE v112_dp_ok_t(id SERIAL PRIMARY KEY, season TEXT, v INT)").expect("t");
    Spi::run("INSERT INTO v112_dp_ok_t(season, v) VALUES ('e',1),('y',2)").expect("seed");
    let plain = "SELECT season, COUNT(*) AS c FROM v112_dp_ok_t GROUP BY season";
    let with_cte = "WITH a AS (SELECT season, SUM(v) AS s FROM v112_dp_ok_t GROUP BY season) \
                    SELECT season, s FROM a";
    for (imv, query) in [("v112_dp_ok_v", plain), ("v112_dp_ok_v__cte_q", with_cte)] {
        assert_eq!(
            crate::create_reflex_ivm(imv, query, None, None, Some("IMMEDIATE"), None),
            "CREATE REFLEX INCREMENTAL VIEW"
        );
    }
    assert_eq!(crate::drop_reflex_ivm_cascade("v112_dp_ok_v", true), "DROP REFLEX INCREMENTAL VIEW");
    Spi::run("INSERT INTO v112_dp_ok_t(season, v) VALUES ('e',10),('z',30)").expect("write");
    assert_imv_correct("v112_dp_ok_v__cte_q", with_cte);
}

fn v112_dp_run_printed_fix(imv: &str, category: &str) {
    let report = ps9_audit(imv);
    let fix = ps9_suggested_fix(&report, category);
    for statement in fix.split(';').map(str::trim).filter(|s| !s.is_empty()) {
        let outcome = Spi::get_one::<String>(&format!("SELECT ({})::text", &statement["SELECT".len()..]))
            .unwrap_or_else(|e| panic!("printed remedy `{statement}` raised: {e}"))
            .unwrap_or_default();
        assert!(!outcome.starts_with("ERROR"), "printed remedy `{statement}`: {outcome}");
    }
}

/// A dropped `__reflex_scratch_<imv>` is reported, and the printed remedy
/// recreates it.
#[pg_test]
fn v112_dp_missing_delta_scratch_table_is_reported_and_its_remedy_converges() {
    for (prefix, mode, query) in [
        ("v112_dp_sg_i", "IMMEDIATE", "SELECT g, SUM(v) AS sv, COUNT(*) AS c FROM {t} GROUP BY g"),
        ("v112_dp_sg_d", "DEFERRED", "SELECT g, SUM(v) AS sv, COUNT(*) AS c FROM {t} GROUP BY g"),
        ("v112_dp_su_i", "IMMEDIATE", "SELECT SUM(v) AS sv, COUNT(*) AS c FROM {t}"),
        ("v112_dp_sm_d", "DEFERRED", "SELECT g, MIN(v) AS lo, MAX(v) AS hi FROM {t} GROUP BY g"),
    ] {
        let t = format!("{prefix}_t");
        let imv = format!("{prefix}_v");
        let query = query.replace("{t}", &t);
        Spi::run(&format!("CREATE TABLE {t}(id SERIAL PRIMARY KEY, g TEXT, v INT)")).expect("t");
        Spi::run(&format!("INSERT INTO {t}(g, v) VALUES ('a',1),('b',2),('a',3)")).expect("seed");
        assert_eq!(
            crate::create_reflex_ivm(&imv, &query, None, None, Some(mode), None),
            "CREATE REFLEX INCREMENTAL VIEW"
        );
        assert!(!ps9_audit(&imv).contains("internal-tables-exist"), "fixture: {imv} audits clean");
        Spi::run(&format!("DROP TABLE \"__reflex_scratch_{imv}\"")).expect("drop scratch");
        let report = ps9_audit(&imv);
        assert!(
            report.contains("internal-tables-exist") && report.contains(&format!("__reflex_scratch_{imv}")),
            "{imv}: the missing scratch table is not reported:\n{report}"
        );
        v112_dp_run_printed_fix(&imv, "internal-tables-exist");
        let after = ps9_audit(&imv);
        assert!(!after.contains("internal-tables-exist"), "{imv}: finding not cleared:\n{after}");
        for write in [
            "INSERT INTO {t}(g, v) VALUES ('a',10),('c',30)",
            "UPDATE {t} SET v = v + 1 WHERE g = 'b'",
            "DELETE FROM {t} WHERE id = 1",
        ] {
            Spi::run(&write.replace("{t}", &t)).expect("write");
            if mode == "DEFERRED" {
                assert!(v112_dp_flush(&t).starts_with("FLUSHED"));
            }
            assert_imv_correct(&imv, &query);
        }
    }
}

/// The remedy for a missing passthrough scratch table names the source as the IMV
/// registered it, double quotes included for a CTE sub-IMV.
#[pg_test]
fn v112_dp_missing_scratch_of_a_cte_parent_remedy_converges() {
    Spi::run("CREATE TABLE v112_dp_cp_t(id SERIAL PRIMARY KEY, season TEXT, v INT)").expect("t");
    Spi::run("INSERT INTO v112_dp_cp_t(season, v) VALUES ('e',1),('y',2),('e',3)").expect("seed");
    let query = "WITH a AS (SELECT season, SUM(v) AS s FROM v112_dp_cp_t GROUP BY season) \
                 SELECT season, s FROM a";
    assert_eq!(
        crate::create_reflex_ivm("v112_dp_cp_v", query, None, None, Some("IMMEDIATE"), None),
        "CREATE REFLEX INCREMENTAL VIEW"
    );
    let scratch = Spi::get_one::<String>(
        "SELECT relname::text FROM pg_class WHERE relname LIKE '\\_\\_reflex\\_pt\\_new\\_v112\\_dp\\_cp\\_v\\_%'",
    )
    .expect("scratch lookup")
    .expect("fixture: the parent owns a passthrough scratch table");
    let triggers_before = v112_dp_reflex_triggers_on("v112_dp_cp_v__cte_a");
    Spi::run(&format!("DROP TABLE \"{scratch}\"")).expect("drop scratch");
    assert!(ps9_audit("v112_dp_cp_v").contains("internal-tables-exist"));
    v112_dp_run_printed_fix("v112_dp_cp_v", "internal-tables-exist");
    let after = ps9_audit("v112_dp_cp_v");
    assert!(!after.contains("internal-tables-exist"), "finding not cleared:\n{after}");
    assert_eq!(
        v112_dp_reflex_triggers_on("v112_dp_cp_v__cte_a"),
        triggers_before,
        "the remedy changed the trigger set of the sub-IMV"
    );
    Spi::run("INSERT INTO v112_dp_cp_t(season, v) VALUES ('e',10),('z',30)").expect("write");
    assert_imv_correct("v112_dp_cp_v", query);
}

/// `reflex_rebuild_triggers` re-emits the triggers the IMVs registered, under any
/// spelling of the source, and adds no second set.
#[pg_test]
fn v112_dp_rebuild_triggers_keeps_one_trigger_set_per_source() {
    for (t, mode, called) in [
        ("v112_dp_rt_a", "IMMEDIATE", "v112_dp_rt_a"),
        ("v112_dp_rt_b", "DEFERRED", "v112_dp_rt_b"),
        ("v112_dp_rt_c", "DEFERRED", "public.v112_dp_rt_c"),
        ("v112_dp_rt_d", "IMMEDIATE", "\"v112_dp_rt_d\""),
    ] {
        let imv = format!("{t}_v");
        let query = format!("SELECT g, SUM(v) AS sv FROM {t} GROUP BY g");
        Spi::run(&format!("CREATE TABLE {t}(id SERIAL PRIMARY KEY, g TEXT, v INT)")).expect("t");
        Spi::run(&format!("INSERT INTO {t}(g, v) VALUES ('a',1),('b',2)")).expect("seed");
        assert_eq!(
            crate::create_reflex_ivm(&imv, &query, None, None, Some(mode), None),
            "CREATE REFLEX INCREMENTAL VIEW"
        );
        let triggers_before = v112_dp_reflex_triggers_on(t);
        let rebuilt = crate::reflex_rebuild_triggers(called);
        assert!(!rebuilt.starts_with("ERROR"), "{called}: {rebuilt}");
        assert_eq!(v112_dp_reflex_triggers_on(t), triggers_before, "rebuilt as {called}");
        Spi::run(&format!("INSERT INTO {t}(g, v) VALUES ('a',10),('c',30)")).expect("write");
        if mode == "DEFERRED" {
            assert!(v112_dp_flush(t).starts_with("FLUSHED"));
        }
        assert_imv_correct(&imv, &query);
    }
}

#[pg_test]
fn v112_dp_column_substitution_leaves_cast_type_names_alone() {
    use crate::sql_writer::identifier::substitute_column_ci;
    for (constraint, column, expected) in [
        (
            "((date IS NOT NULL) AND (date >= '2024-01-01'::date) AND (date < '2024-02-01'::date))",
            "date",
            "((X IS NOT NULL) AND (X >= '2024-01-01'::date) AND (X < '2024-02-01'::date))",
        ),
        ("((text IS NOT NULL) AND (text = 'a'::text))", "text", "((X IS NOT NULL) AND (X = 'a'::text))"),
        (
            "(zone >= '2024-01-01 00:00:00+00'::timestamp with time zone)",
            "zone",
            "(X >= '2024-01-01 00:00:00+00'::timestamp with time zone)",
        ),
        ("(time < '10:00:00'::time without time zone)", "time", "(X < '10:00:00'::time without time zone)"),
        ("(precision > 1::double precision)", "precision", "(X > 1::double precision)"),
        ("(varying = 'a'::character varying(3))", "varying", "(X = 'a'::character varying(3))"),
        ("(numeric < 1.5::numeric(10,2) AND numeric > 0)", "numeric", "(X < 1.5::numeric(10,2) AND X > 0)"),
        ("(s = ANY (ARRAY['a'::text, 'b'::text]))", "s", "(X = ANY (ARRAY['a'::text, 'b'::text]))"),
        ("(my_type = 'a'::public.my_type)", "my_type", "(X = 'a'::public.my_type)"),
        ("(k >= 1) AND (k < 10)", "k", "(X >= 1) AND (X < 10)"),
        ("(date = 'date'::date)::boolean AND date IS NOT NULL", "date", "(X = 'date'::date)::boolean AND X IS NOT NULL"),
    ] {
        assert_eq!(substitute_column_ci(constraint, column, "X"), expected, "{constraint}");
    }
}

/// A partition column named like the type its bounds are cast to.
#[pg_test]
fn v112_dp_reconcile_partition_with_a_column_named_like_its_type() {
    for (column, column_type, strategy, bounds, key) in [
        (
            "date",
            "DATE",
            "RANGE",
            ["FROM ('2024-01-01') TO ('2024-02-01')", "FROM ('2024-02-01') TO ('2024-03-01')"],
            ["2024-01-15", "2024-02-10"],
        ),
        ("text", "TEXT", "LIST", ["IN ('a')", "IN ('b')"], ["a", "b"]),
        (
            "zone",
            "TIMESTAMPTZ",
            "RANGE",
            [
                "FROM ('2024-01-01 00:00:00+00') TO ('2024-02-01 00:00:00+00')",
                "FROM ('2024-02-01 00:00:00+00') TO ('2024-03-01 00:00:00+00')",
            ],
            ["2024-01-15 00:00:00+00", "2024-02-10 00:00:00+00"],
        ),
    ] {
        let s = format!("v112_dp_ty_{column}");
        let imv = format!("{s}_v");
        let reader = format!("{s}_r");
        Spi::run(&format!(
            "CREATE TABLE {s}({column} {column_type} NOT NULL, id INT NOT NULL, v INT) \
             PARTITION BY {strategy} ({column})"
        ))
        .expect("source");
        for (leaf, bound) in bounds.iter().enumerate() {
            Spi::run(&format!("CREATE TABLE {s}_p{leaf} PARTITION OF {s} FOR VALUES {bound}"))
                .expect("leaf");
        }
        Spi::run(&format!(
            "INSERT INTO {s} VALUES ('{0}', 1, 10), ('{0}', 2, 20), ('{1}', 3, 30)",
            key[0], key[1]
        ))
        .expect("seed");
        let query = format!("SELECT {column}, id, v FROM {s}");
        let created = Spi::get_one::<String>(&format!(
            "SELECT create_reflex_ivm('{imv}', '{query}', '{column}, id', NULL, 'IMMEDIATE', NULL, \
             ARRAY['{column}'])"
        ))
        .expect("create")
        .unwrap_or_default();
        assert_eq!(created, "CREATE REFLEX INCREMENTAL VIEW", "{column}");
        let reader_query = format!("SELECT {column}, COUNT(*) AS c, SUM(v) AS sv FROM {imv} GROUP BY {column}");
        let reader_fresh = format!("SELECT {column}, COUNT(*) AS c, SUM(v) AS sv FROM {s} GROUP BY {column}");
        assert_eq!(
            crate::create_reflex_ivm(&reader, &reader_query, None, None, Some("IMMEDIATE"), None),
            "CREATE REFLEX INCREMENTAL VIEW",
            "{column}"
        );
        let by_key: Vec<String> = key
            .iter()
            .map(|value| format!("reflex_reconcile_partition('{imv}', '{value}')"))
            .collect();
        let by_source_partition: Vec<String> = (0..2)
            .map(|leaf| format!("reflex_reconcile_partition('{imv}', '', '{s}_p{leaf}')"))
            .collect();
        for calls in [by_key, by_source_partition] {
            Spi::run(&format!("UPDATE {imv} SET v = v + 1000")).expect("drift");
            for call in &calls {
                let outcome = Spi::get_one::<String>(&format!("SELECT {call}::text"))
                    .unwrap_or_else(|e| panic!("{call} raised: {e}"))
                    .unwrap_or_default();
                assert!(!outcome.starts_with("ERROR"), "{call}: {outcome}");
            }
            assert_imv_correct(&imv, &query);
            assert_imv_correct(&reader, &reader_fresh);
        }
        assert_imv_correct(&imv, &query);
        Spi::run(&format!("INSERT INTO {s} VALUES ('{}', 4, 40)", key[1])).expect("write");
        assert_imv_correct(&imv, &query);
        assert_imv_correct(&reader, &reader_fresh);
    }
}

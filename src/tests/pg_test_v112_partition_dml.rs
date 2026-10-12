// Writes addressed to a partition of a partitioned source. Every matrix step is
// one committed transaction on a `dblink` worker in a private database (helpers
// of `pg_test_partition_attach_locks.rs`, `pg_test_rebuild_commit.rs` and
// `pg_test_v112_deferred_partition.rs`), so the DEFERRED flush and the partition
// flush are the ones COMMIT runs.

/// (name suffix, query over `{s}`, key, partition column). `total` is the
/// COUNT / SUM oracle of single application: a row applied twice or not at all
/// moves it.
const V112_PD_IMVS: &[(&str, &str, Option<&str>, Option<&str>)] = &[
    ("agg", "SELECT g, COUNT(*) AS c, SUM(v) AS sv FROM {s} GROUP BY g", None, None),
    ("minmax", "SELECT g, MIN(v) AS lo, MAX(v) AS hi FROM {s} GROUP BY g", None, None),
    ("total", "SELECT COUNT(*) AS c, SUM(v) AS sv FROM {s}", None, None),
    ("filtered", "SELECT g, COUNT(*) AS c, SUM(v) AS sv FROM {s} WHERE v > 2 GROUP BY g", None, None),
    ("keyed", "SELECT id, g, v FROM {s}", Some("id, g"), None),
    ("star", "SELECT * FROM {s}", Some("id, g"), None),
    ("keyless", "SELECT g, v FROM {s} WHERE v > 0", None, None),
    ("agg_part", "SELECT g, COUNT(*) AS c, SUM(v) AS sv FROM {s} GROUP BY g", None, Some("g")),
    ("keyed_part", "SELECT id, g, v FROM {s}", Some("id, g"), Some("g")),
];

/// A LIST-partitioned source with a RANGE sub-partitioned branch, a DEFAULT
/// partition and a unique index, read by every IMV of `V112_PD_IMVS` in `mode`.
fn v112_pd_fixture(s: &str, mode: &str) {
    for ddl in [
        "CREATE TABLE {s}(id INT NOT NULL, g TEXT NOT NULL, v INT) PARTITION BY LIST (g)",
        "CREATE UNIQUE INDEX {s}_uq ON {s}(id, g)",
        "CREATE TABLE {s}_pa PARTITION OF {s} FOR VALUES IN ('a', 'b')",
        "CREATE TABLE {s}_pc PARTITION OF {s} FOR VALUES IN ('c') PARTITION BY RANGE (id)",
        "CREATE TABLE {s}_pc1 PARTITION OF {s}_pc FOR VALUES FROM (0) TO (100)",
        "CREATE TABLE {s}_pc2 PARTITION OF {s}_pc FOR VALUES FROM (100) TO (200)",
        "CREATE TABLE {s}_pd PARTITION OF {s} DEFAULT",
        "INSERT INTO {s} VALUES (1,'a',1),(2,'a',2),(3,'b',5),(4,'c',7),(104,'c',9),(6,'p',7)",
    ] {
        worker_exec(&ddl.replace("{s}", s));
    }
    for (suffix, query, key, partition_by) in V112_PD_IMVS {
        v112_dp_create(
            &format!("{s}_{suffix}"),
            &query.replace("{s}", s),
            *key,
            mode,
            *partition_by,
        );
    }
}

/// The rows only the IMV holds and the rows only its query returns.
fn v112_pd_difference(imv: &str, fresh: &str) -> String {
    rbc_select(&format!(
        "SELECT 'extra ' || COALESCE((SELECT string_agg(x::text, ' ') FROM \
           (SELECT * FROM {imv} EXCEPT ALL ({fresh})) x), '-') || ' missing ' || \
         COALESCE((SELECT string_agg(x::text, ' ') FROM (({fresh}) EXCEPT ALL SELECT * FROM {imv}) x), '-')"
    ))
}

/// Commits each step and compares every IMV of the fixture with its query.
fn v112_pd_failures(s: &str, mode: &str, steps: &[(&str, &str)]) -> Vec<String> {
    let mut failures = Vec::new();
    for (label, step) in steps {
        if let Err(error) = v112_dp_try(&step.replace("{s}", s)) {
            failures.push(format!("{mode} {label}: raised {error}"));
            continue;
        }
        for (suffix, query, _, _) in V112_PD_IMVS {
            let imv = format!("{s}_{suffix}");
            let mismatches = rbc_mismatch(&imv, &query.replace("{s}", s));
            if mismatches != 0 {
                failures.push(format!(
                    "{mode} {label}: {suffix} {mismatches} mismatches {}",
                    v112_pd_difference(&imv, &query.replace("{s}", s))
                ));
            }
            if rbc_known_stale(&imv) {
                failures.push(format!("{mode} {label}: {suffix} flagged stale"));
            }
        }
    }
    failures
}

fn v112_pd_assert_matrix(dbname: &str, mode: &str, steps: &[(&str, &str)]) {
    probe_db_open(dbname);
    v112_pd_fixture("pd", mode);
    let failures = v112_pd_failures("pd", mode, steps);
    probe_db_close(dbname);
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

const V112_PD_EXISTING_PARTITIONS: &[(&str, &str)] = &[
    ("control: insert on the root", "INSERT INTO {s} VALUES (7,'a',3),(8,'q',4),(9,'c',1)"),
    ("insert into a leaf", "INSERT INTO {s}_pa VALUES (301,'a',1),(302,'b',2),(303,'b',9)"),
    ("update a leaf", "UPDATE {s}_pa SET v = v + 5 WHERE g = 'a'"),
    ("update a leaf moving rows between its groups", "UPDATE {s}_pa SET g = 'b' WHERE id = 301"),
    ("delete from a leaf", "DELETE FROM {s}_pa WHERE id IN (1, 302)"),
    ("insert into the default partition", "INSERT INTO {s}_pd VALUES (311,'q',3),(312,'r',0)"),
    ("update the default partition", "UPDATE {s}_pd SET v = v + 1, g = 'r' WHERE id = 311"),
    ("delete from the default partition", "DELETE FROM {s}_pd WHERE id = 6"),
    ("insert into a sub-partition leaf", "INSERT INTO {s}_pc1 VALUES (21,'c',4),(22,'c',6)"),
    ("update a sub-partition leaf", "UPDATE {s}_pc1 SET v = v * 2"),
    ("delete from a sub-partition leaf", "DELETE FROM {s}_pc1 WHERE id = 21"),
    (
        "insert into the sub-partitioned branch",
        "INSERT INTO {s}_pc VALUES (31,'c',3),(131,'c',5),(132,'c',1)",
    ),
    ("update the sub-partitioned branch", "UPDATE {s}_pc SET v = v + 10 WHERE id > 100"),
    (
        "branch update moving a row between its leaves",
        "UPDATE {s}_pc SET id = id + 150 WHERE id = 4",
    ),
    ("delete from the sub-partitioned branch", "DELETE FROM {s}_pc WHERE id IN (22, 131)"),
    ("root update moving a row from a leaf to the branch", "UPDATE {s} SET g = 'c' WHERE id = 2"),
    ("root update moving rows to the default partition", "UPDATE {s} SET g = 'z' WHERE g = 'b'"),
    (
        "root update moving rows back and changing them",
        "UPDATE {s} SET g = 'a', v = v + 1 WHERE g = 'z'",
    ),
    (
        "insert on conflict do update into a leaf",
        "INSERT INTO {s}_pa VALUES (7,'a',100),(321,'a',2) \
         ON CONFLICT (id, g) DO UPDATE SET v = EXCLUDED.v",
    ),
    (
        "insert on conflict do nothing into a leaf",
        "INSERT INTO {s}_pa VALUES (7,'a',55),(322,'b',3) ON CONFLICT DO NOTHING",
    ),
    (
        "insert on conflict into the branch",
        "INSERT INTO {s}_pc VALUES (31,'c',40),(133,'c',2) \
         ON CONFLICT (id, g) DO UPDATE SET v = {s}_pc.v + EXCLUDED.v",
    ),
    (
        "merge into a leaf",
        "MERGE INTO {s}_pa t USING (VALUES (7,'a',1),(321,'a',0),(331,'b',8)) AS n(id, g, v) \
           ON t.id = n.id AND t.g = n.g \
         WHEN MATCHED AND n.v = 0 THEN DELETE \
         WHEN MATCHED THEN UPDATE SET v = t.v + n.v \
         WHEN NOT MATCHED THEN INSERT VALUES (n.id, n.g, n.v)",
    ),
    (
        "merge into a sub-partition leaf",
        "MERGE INTO {s}_pc2 t USING (VALUES (104,'c',1),(141,'c',6)) AS n(id, g, v) \
           ON t.id = n.id AND t.g = n.g \
         WHEN MATCHED THEN UPDATE SET v = t.v + n.v \
         WHEN NOT MATCHED THEN INSERT VALUES (n.id, n.g, n.v)",
    ),
    ("copy into a leaf", "COPY {s}_pa FROM PROGRAM 'echo 341,a,4; echo 342,b,6' WITH (FORMAT csv)"),
    ("copy into the branch", "COPY {s}_pc FROM PROGRAM 'echo 41,c,4; echo 142,c,6' WITH (FORMAT csv)"),
    (
        "data-modifying WITH moving rows between two leaves",
        "WITH moved AS (DELETE FROM {s}_pa WHERE id = 341 RETURNING id, v) \
         INSERT INTO {s}_pd SELECT id, 'w', v FROM moved",
    ),
    (
        "root and partition statements in one transaction",
        "INSERT INTO {s} VALUES (51,'a',1),(52,'p',2),(53,'c',3); \
         INSERT INTO {s}_pa VALUES (54,'b',4); UPDATE {s}_pa SET v = v + 1 WHERE id IN (51, 54); \
         DELETE FROM {s} WHERE id = 52; UPDATE {s}_pc1 SET v = 0 WHERE id = 53; \
         DELETE FROM {s}_pd WHERE g = 'w'",
    ),
    (
        "the same rows written twice through a leaf in one transaction",
        "INSERT INTO {s}_pa VALUES (61,'a',1); UPDATE {s}_pa SET v = 7 WHERE id = 61; \
         DELETE FROM {s}_pa WHERE id = 61; INSERT INTO {s}_pa VALUES (61,'a',2)",
    ),
    ("a leaf statement touching no row", "UPDATE {s}_pa SET v = 0 WHERE id = -1"),
    ("empty a leaf", "DELETE FROM {s}_pa"),
    ("empty the branch", "DELETE FROM {s}_pc"),
    ("empty the default partition", "DELETE FROM {s}_pd"),
    (
        "refill through the partitions",
        "INSERT INTO {s}_pa VALUES (71,'a',10),(72,'b',20); \
         INSERT INTO {s}_pc VALUES (73,'c',30),(173,'c',40); \
         INSERT INTO {s}_pd VALUES (74,'q',50)",
    ),
    ("control: delete on the root", "DELETE FROM {s} WHERE id IN (71, 173)"),
    (
        "detach and attach a partition again in one transaction, then write to it",
        "ALTER TABLE {s} DETACH PARTITION {s}_pa; \
         ALTER TABLE {s} ATTACH PARTITION {s}_pa FOR VALUES IN ('a', 'b'); \
         INSERT INTO {s}_pa VALUES (81,'a',8); UPDATE {s} SET v = v + 1 WHERE id = 72",
    ),
    ("write to it afterwards", "DELETE FROM {s}_pa WHERE id = 81; INSERT INTO {s}_pa VALUES (82,'b',2)"),
];

const V112_PD_LATER_PARTITIONS: &[(&str, &str)] = &[
    ("create a partition", "CREATE TABLE {s}_pe PARTITION OF {s} FOR VALUES IN ('e')"),
    ("insert into the created partition", "INSERT INTO {s}_pe VALUES (201,'e',1),(202,'e',2)"),
    ("update the created partition", "UPDATE {s}_pe SET v = v + 3"),
    ("delete from the created partition", "DELETE FROM {s}_pe WHERE id = 201"),
    (
        "attach a partition that holds rows",
        "CREATE TABLE {s}_pf (LIKE {s} INCLUDING ALL); \
         INSERT INTO {s}_pf VALUES (211,'f',1),(212,'f',2); \
         ALTER TABLE {s} ATTACH PARTITION {s}_pf FOR VALUES IN ('f')",
    ),
    ("insert into the attached partition", "INSERT INTO {s}_pf VALUES (213,'f',3)"),
    ("update the attached partition", "UPDATE {s}_pf SET v = v + 1 WHERE id <> 212"),
    ("delete from the attached partition", "DELETE FROM {s}_pf WHERE id = 211"),
    (
        "attach a partition whose columns are laid out differently",
        "CREATE TABLE {s}_pr (v INT, junk INT, g TEXT NOT NULL, id INT NOT NULL); \
         ALTER TABLE {s}_pr DROP COLUMN junk; \
         INSERT INTO {s}_pr (id, g, v) VALUES (221,'r',1),(222,'r',2); \
         ALTER TABLE {s} ATTACH PARTITION {s}_pr FOR VALUES IN ('r')",
    ),
    (
        "insert through the root into the differently laid out partition",
        "INSERT INTO {s} VALUES (223,'r',3),(224,'r',9)",
    ),
    ("update it through the root", "UPDATE {s} SET v = v + 2 WHERE g = 'r' AND id > 221"),
    ("delete from it through the root", "DELETE FROM {s} WHERE id = 222"),
    ("root update moving a row into it", "UPDATE {s} SET g = 'r' WHERE id = 1"),
    ("root update moving a row out of it", "UPDATE {s} SET g = 'a', v = 6 WHERE id = 223"),
    (
        "create a sub-partition",
        "CREATE TABLE {s}_pc3 PARTITION OF {s}_pc FOR VALUES FROM (200) TO (300)",
    ),
    ("insert into the created sub-partition", "INSERT INTO {s}_pc3 VALUES (231,'c',1),(232,'c',2)"),
    ("branch update reaching the created sub-partition", "UPDATE {s}_pc SET v = v + 1 WHERE id > 200"),
    ("delete from the created sub-partition", "DELETE FROM {s}_pc3 WHERE id = 231"),
    (
        "create a sub-partitioned branch",
        "CREATE TABLE {s}_ph PARTITION OF {s} FOR VALUES IN ('h') PARTITION BY RANGE (id); \
         CREATE TABLE {s}_ph1 PARTITION OF {s}_ph FOR VALUES FROM (0) TO (1000)",
    ),
    ("insert into the created branch", "INSERT INTO {s}_ph VALUES (241,'h',1),(242,'h',2)"),
    ("insert into the leaf of the created branch", "INSERT INTO {s}_ph1 VALUES (243,'h',3)"),
    ("update the leaf of the created branch", "UPDATE {s}_ph1 SET v = v * 3"),
    ("delete from the created branch", "DELETE FROM {s}_ph WHERE id = 242"),
    (
        "create a partition and fill it directly in one transaction",
        "CREATE TABLE {s}_pi PARTITION OF {s} FOR VALUES IN ('i'); \
         INSERT INTO {s}_pi VALUES (251,'i',1),(252,'i',2); UPDATE {s}_pi SET v = 5 WHERE id = 252",
    ),
    ("write to it afterwards", "INSERT INTO {s}_pi VALUES (253,'i',3); DELETE FROM {s}_pi WHERE id = 251"),
    (
        "create a partition and fill it through the root in one transaction",
        "CREATE TABLE {s}_pj PARTITION OF {s} FOR VALUES IN ('j'); \
         INSERT INTO {s} VALUES (261,'j',1),(262,'j',2),(263,'a',3)",
    ),
    (
        "create a partition and fill it both ways in one transaction",
        "CREATE TABLE {s}_pk PARTITION OF {s} FOR VALUES IN ('k'); \
         INSERT INTO {s} VALUES (271,'k',1); INSERT INTO {s}_pk VALUES (272,'k',2); \
         UPDATE {s} SET v = v + 1 WHERE g = 'k'",
    ),
    (
        "attach a filled partition and write to it in one transaction",
        "CREATE TABLE {s}_pl (LIKE {s} INCLUDING ALL); INSERT INTO {s}_pl VALUES (281,'l',1); \
         ALTER TABLE {s} ATTACH PARTITION {s}_pl FOR VALUES IN ('l'); \
         INSERT INTO {s}_pl VALUES (282,'l',2); UPDATE {s}_pl SET v = v + 4 WHERE id = 281; \
         INSERT INTO {s} VALUES (283,'l',3)",
    ),
    ("detach a partition that holds rows", "ALTER TABLE {s} DETACH PARTITION {s}_pf"),
    (
        "write to the detached table",
        "INSERT INTO {s}_pf VALUES (291,'f',1); UPDATE {s}_pf SET v = 0; DELETE FROM {s}_pf WHERE id = 212; \
         TRUNCATE {s}_pf; INSERT INTO {s}_pf VALUES (212,'f',2),(213,'f',3)",
    ),
    ("write to the root after the detach", "INSERT INTO {s} VALUES (292,'a',2),(293,'e',3)"),
    (
        "attach the detached table again",
        "ALTER TABLE {s} ATTACH PARTITION {s}_pf FOR VALUES IN ('f')",
    ),
    ("write to the table attached again", "INSERT INTO {s}_pf VALUES (294,'f',4); UPDATE {s}_pf SET v = v + 1"),
    (
        "detach a partition and write to the root in one transaction",
        "INSERT INTO {s} VALUES (295,'l',5); ALTER TABLE {s} DETACH PARTITION {s}_pl; \
         INSERT INTO {s} VALUES (296,'a',6)",
    ),
    ("write to that detached table", "INSERT INTO {s}_pl VALUES (297,'l',7); DELETE FROM {s}_pl WHERE id = 281"),
    (
        "detach and drop a partition that holds rows",
        "ALTER TABLE {s} DETACH PARTITION {s}_pe; DROP TABLE {s}_pe",
    ),
    ("write through every level", "INSERT INTO {s} VALUES (298,'q',8); INSERT INTO {s}_pc VALUES (98,'c',8); INSERT INTO {s}_pa VALUES (299,'a',9)"),
];

#[pg_test]
fn v112_pd_writes_to_existing_partitions_immediate() {
    v112_pd_assert_matrix("reflex_pd_exist_i", "IMMEDIATE", V112_PD_EXISTING_PARTITIONS);
}

#[pg_test]
fn v112_pd_writes_to_existing_partitions_deferred() {
    v112_pd_assert_matrix("reflex_pd_exist_d", "DEFERRED", V112_PD_EXISTING_PARTITIONS);
}

#[pg_test]
fn v112_pd_writes_to_later_partitions_immediate() {
    v112_pd_assert_matrix("reflex_pd_later_i", "IMMEDIATE", V112_PD_LATER_PARTITIONS);
}

#[pg_test]
fn v112_pd_writes_to_later_partitions_deferred() {
    v112_pd_assert_matrix("reflex_pd_later_d", "DEFERRED", V112_PD_LATER_PARTITIONS);
}

/// Triggers on `table` running a write or guard function of `source`, sorted.
fn v112_pd_write_triggers_on(table: &str, source: &str) -> Vec<String> {
    let mut names: Vec<String> = Spi::connect(|client| {
        client
            .select(
                &format!(
                    "SELECT t.tgname::text AS n FROM pg_trigger t JOIN pg_proc f ON f.oid = t.tgfoid \
                     WHERE t.tgrelid = '{table}'::regclass AND NOT t.tgisinternal \
                       AND f.proname IN ('__reflex_ins_trigger_on_{source}', \
                           '__reflex_del_trigger_on_{source}', '__reflex_upd_trigger_on_{source}', \
                           '__reflex_guard_trigger_on_{source}')"
                ),
                None,
                &[],
            )
            .expect("triggers")
            .filter_map(|row| row.get_by_name::<String, _>("n").ok().flatten())
            .collect()
    });
    names.sort();
    names
}

fn v112_pd_reflex_triggers_on(table: &str) -> Vec<String> {
    Spi::connect(|client| {
        client
            .select(
                &format!(
                    "SELECT t.tgname::text AS n FROM pg_trigger t \
                     WHERE t.tgrelid = '{table}'::regclass AND NOT t.tgisinternal \
                       AND t.tgname LIKE '%reflex%'"
                ),
                None,
                &[],
            )
            .expect("triggers")
            .filter_map(|row| row.get_by_name::<String, _>("n").ok().flatten())
            .collect()
    })
}

fn v112_pd_create(name: &str, query: &str, mode: &str) {
    assert_eq!(
        crate::create_reflex_ivm(name, query, None, None, Some(mode), None),
        "CREATE REFLEX INCREMENTAL VIEW",
        "creating {name}"
    );
}

/// Each table of the partition tree carries the three write triggers of the
/// source exactly once, however often they are synced; they go with the last
/// IMV of the source, and `reflex_rebuild_triggers` puts back one dropped by hand.
#[pg_test]
fn v112_pd_every_partition_carries_the_write_triggers_once() {
    const QUERY: &str = "SELECT g, COUNT(*) AS c, SUM(v) AS sv FROM v112_pd_tr GROUP BY g";
    Spi::run("CREATE TABLE v112_pd_tr(id INT, g TEXT, v INT) PARTITION BY LIST (g)").expect("s");
    Spi::run("CREATE TABLE v112_pd_tr_a PARTITION OF v112_pd_tr FOR VALUES IN ('a')").expect("a");
    Spi::run(
        "CREATE TABLE v112_pd_tr_b PARTITION OF v112_pd_tr FOR VALUES IN ('b') \
         PARTITION BY RANGE (id)",
    )
    .expect("b");
    Spi::run("CREATE TABLE v112_pd_tr_b1 PARTITION OF v112_pd_tr_b FOR VALUES FROM (0) TO (99)")
        .expect("b1");
    Spi::run("INSERT INTO v112_pd_tr VALUES (1,'a',1),(2,'b',2)").expect("seed");
    let partitions = ["v112_pd_tr_a", "v112_pd_tr_b", "v112_pd_tr_b1"];
    let on_source: Vec<String> = [
        "__reflex_trigger_del_on_v112_pd_tr",
        "__reflex_trigger_ins_on_v112_pd_tr",
        "__reflex_trigger_upd_on_v112_pd_tr",
        "reflex_partitioned_source_v112_pd_tr",
    ]
    .map(String::from)
    .to_vec();
    let on_partition: Vec<String> = ["del", "guard", "ins", "upd"]
        .iter()
        .map(|op| format!("reflex_partition_{op}_on_v112_pd_tr"))
        .collect();
    let assert_tree = |when: &str| {
        assert_eq!(v112_pd_write_triggers_on("v112_pd_tr", "v112_pd_tr"), on_source, "{when}");
        for partition in partitions {
            assert_eq!(
                v112_pd_write_triggers_on(partition, "v112_pd_tr"),
                on_partition,
                "{when}: {partition}"
            );
        }
    };

    v112_pd_create("v112_pd_tr_x", QUERY, "IMMEDIATE");
    assert_tree("after create");
    v112_pd_create("v112_pd_tr_y", QUERY, "DEFERRED");
    Spi::run("SELECT reflex_rebuild_triggers('v112_pd_tr')").expect("rebuild triggers");
    assert_tree("after a second IMV");

    Spi::run("DROP TRIGGER reflex_partition_ins_on_v112_pd_tr ON v112_pd_tr_b1").expect("drop");
    Spi::run("DROP TRIGGER reflex_partition_guard_on_v112_pd_tr ON v112_pd_tr_a").expect("drop");
    Spi::run("SELECT reflex_rebuild_triggers('v112_pd_tr')").expect("rebuild triggers");
    assert_tree("after the rebuild of dropped triggers");
    Spi::run("INSERT INTO v112_pd_tr_b1 VALUES (3,'b',5)").expect("leaf insert");
    Spi::run("SELECT reflex_flush_deferred('v112_pd_tr')").expect("flush");
    assert_imv_correct("v112_pd_tr_x", QUERY);
    assert_imv_correct("v112_pd_tr_y", QUERY);

    assert_eq!(crate::drop_reflex_ivm("v112_pd_tr_x"), "DROP REFLEX INCREMENTAL VIEW");
    assert_tree("one IMV left");
    Spi::run("INSERT INTO v112_pd_tr_a VALUES (4,'a',7)").expect("leaf insert");
    Spi::run("SELECT reflex_flush_deferred('v112_pd_tr')").expect("flush");
    assert_imv_correct("v112_pd_tr_y", QUERY);

    assert_eq!(crate::drop_reflex_ivm("v112_pd_tr_y"), "DROP REFLEX INCREMENTAL VIEW");
    for table in ["v112_pd_tr", "v112_pd_tr_a", "v112_pd_tr_b", "v112_pd_tr_b1"] {
        assert_eq!(
            v112_pd_reflex_triggers_on(table),
            Vec::<String>::new(),
            "after the last drop: {table}"
        );
    }
    assert_eq!(v112_dp_reflex_trigger_functions_naming("v112_pd_tr"), 0, "functions left");
    Spi::run("INSERT INTO v112_pd_tr_a VALUES (5,'a',1)").expect("leaf write after the drop");
    Spi::run("UPDATE v112_pd_tr_b SET v = 0").expect("branch write after the drop");
}

/// A source that is neither partitioned nor a partition keeps its four triggers,
/// and an unrelated table gets none.
#[pg_test]
fn v112_pd_plain_source_keeps_its_four_triggers() {
    Spi::run("CREATE TABLE v112_pd_plain(id INT, g TEXT, v INT)").expect("s");
    Spi::run("CREATE TABLE v112_pd_plain_other(id INT, g TEXT, v INT)").expect("other");
    for (imv, mode) in [("v112_pd_plain_x", "IMMEDIATE"), ("v112_pd_plain_y", "DEFERRED")] {
        v112_pd_create(imv, "SELECT g, SUM(v) AS sv FROM v112_pd_plain GROUP BY g", mode);
        assert_eq!(v112_pd_reflex_triggers_on("v112_pd_plain").len(), 4, "{mode}");
        assert_eq!(v112_pd_reflex_triggers_on("v112_pd_plain_other").len(), 0, "{mode}");
    }
}

/// A partition of a source is refused as a source of its own, nothing created; a
/// write addressed to the partition reaches the IMV over the root, once.
#[pg_test]
fn v112_pd_partition_refused_as_a_source_still_feeds_the_root_imv() {
    const ROOT_QUERY: &str = "SELECT g, COUNT(*) AS c, SUM(v) AS sv FROM v112_pd_two GROUP BY g";
    const LEAF_QUERY: &str = "SELECT COUNT(*) AS c, SUM(v) AS sv FROM v112_pd_two_a";
    for mode in ["IMMEDIATE", "DEFERRED"] {
        Spi::run("CREATE TABLE v112_pd_two(id INT, g TEXT, v INT) PARTITION BY LIST (g)").expect("s");
        Spi::run("CREATE TABLE v112_pd_two_a PARTITION OF v112_pd_two FOR VALUES IN ('a', 'b')")
            .expect("a");
        Spi::run("CREATE TABLE v112_pd_two_d PARTITION OF v112_pd_two DEFAULT").expect("d");
        Spi::run("INSERT INTO v112_pd_two VALUES (1,'a',1),(2,'b',2),(3,'z',3)").expect("seed");
        v112_pd_create("v112_pd_two_root", ROOT_QUERY, mode);
        let refused = crate::create_reflex_ivm("v112_pd_two_leaf", LEAF_QUERY, None, None, Some(mode), None);
        assert!(
            refused.starts_with(
                "ERROR: [reflex-unsupported] the source v112_pd_two_a is a partition of v112_pd_two:"
            ),
            "{mode}: {refused}"
        );
        let left_behind = Spi::get_one::<i64>(
            "SELECT (SELECT count(*) FROM public.__reflex_ivm_reference WHERE name LIKE 'v112_pd_two_leaf%') \
                  + (SELECT count(*) FROM pg_class WHERE relname LIKE '%v112_pd_two_leaf%')",
        )
        .expect("left behind");
        assert_eq!(left_behind, Some(0), "{mode}");
        for statement in [
            "INSERT INTO v112_pd_two_a VALUES (4,'a',4),(5,'b',5)",
            "UPDATE v112_pd_two_a SET v = v + 1 WHERE g = 'a'",
            "DELETE FROM v112_pd_two_a WHERE id = 2",
        ] {
            Spi::run(statement).expect("leaf write");
            Spi::run("SELECT reflex_flush_deferred('v112_pd_two')").expect("flush root");
            assert_imv_correct("v112_pd_two_root", ROOT_QUERY);
        }
        assert_eq!(crate::drop_reflex_ivm("v112_pd_two_root"), "DROP REFLEX INCREMENTAL VIEW");
        Spi::run("DROP TABLE v112_pd_two").expect("drop source");
    }
}

/// A partition whose columns are ordered differently hands its trigger rows in
/// its own order: a statement addressed to it is refused with an error naming
/// the source, nothing is lost, and the same write through the source is correct.
#[pg_test]
fn v112_pd_statement_on_a_reordered_partition_is_refused() {
    const IMVS: &[(&str, &str, Option<&str>)] = &[
        ("agg", "SELECT g, COUNT(*) AS c, SUM(v) AS sv FROM v112_pd_ro GROUP BY g", None),
        ("keyed", "SELECT id, g, v FROM v112_pd_ro", Some("id, g")),
    ];
    for mode in ["IMMEDIATE", "DEFERRED"] {
        Spi::run("CREATE TABLE v112_pd_ro(id INT NOT NULL, g TEXT NOT NULL, v INT) PARTITION BY LIST (g)")
            .expect("s");
        Spi::run("CREATE UNIQUE INDEX ON v112_pd_ro(id, g)").expect("unique");
        Spi::run("CREATE TABLE v112_pd_ro_a PARTITION OF v112_pd_ro FOR VALUES IN ('a')").expect("a");
        Spi::run("CREATE TABLE v112_pd_ro_r (v INT, g TEXT NOT NULL, id INT NOT NULL)").expect("r");
        Spi::run("INSERT INTO v112_pd_ro_r (id, g, v) VALUES (1,'r',10),(2,'r',20)").expect("seed r");
        Spi::run("ALTER TABLE v112_pd_ro ATTACH PARTITION v112_pd_ro_r FOR VALUES IN ('r')")
            .expect("attach");
        Spi::run("INSERT INTO v112_pd_ro VALUES (3,'a',30)").expect("seed");
        for (suffix, query, key) in IMVS {
            assert_eq!(
                crate::create_reflex_ivm(&format!("v112_pd_ro_{suffix}"), query, *key, None, Some(mode), None),
                "CREATE REFLEX INCREMENTAL VIEW"
            );
        }
        let assert_correct = |after: &str| {
            Spi::run("SELECT reflex_flush_deferred('v112_pd_ro')").expect("flush");
            for (suffix, query, _) in IMVS {
                let mismatches = Spi::get_one::<i64>(&format!(
                    "SELECT count(*) FROM ((SELECT * FROM v112_pd_ro_{suffix} EXCEPT ALL ({query})) \
                     UNION ALL (({query}) EXCEPT ALL SELECT * FROM v112_pd_ro_{suffix})) x"
                ))
                .expect("oracle")
                .unwrap_or(-1);
                assert_eq!(mismatches, 0, "{mode} {suffix} after {after}");
            }
        };
        for statement in [
            "INSERT INTO v112_pd_ro_r (id, g, v) VALUES (4,'r',40)",
            "UPDATE v112_pd_ro_r SET v = v + 1",
            "DELETE FROM v112_pd_ro_r WHERE id = 1",
            "DELETE FROM v112_pd_ro_r WHERE id = -1",
            "INSERT INTO v112_pd_ro_r (id, g, v) VALUES (1,'r',5) ON CONFLICT (id, g) DO UPDATE SET v = 5",
            "MERGE INTO v112_pd_ro_r t USING (VALUES (1)) n(id) ON t.id = n.id WHEN MATCHED THEN DELETE",
            "COPY v112_pd_ro_r (id, g, v) FROM PROGRAM 'echo 9,r,9' WITH (FORMAT csv)",
        ] {
            Spi::run(&format!(
                "DO $$ BEGIN {statement}; RAISE EXCEPTION 'accepted'; \
                 EXCEPTION WHEN OTHERS THEN PERFORM set_config('v112_pd.error', SQLERRM, true); END $$"
            ))
            .expect("refused statement");
            let error = Spi::get_one::<String>("SELECT current_setting('v112_pd.error')")
                .expect("error text")
                .unwrap_or_default();
            assert!(
                error.contains("cannot maintain the incremental views of v112_pd_ro")
                    && error.contains("v112_pd_ro_r"),
                "{mode} <{statement}> was not refused: {error}"
            );
            assert_correct(statement);
        }
        assert_eq!(
            Spi::get_one::<i64>("SELECT count(*) FROM v112_pd_ro_r").expect("rows"),
            Some(2),
            "{mode}: a refused statement changed the partition"
        );
        for statement in [
            "INSERT INTO v112_pd_ro VALUES (4,'r',40),(5,'a',50)",
            "UPDATE v112_pd_ro SET v = v + 1 WHERE g = 'r'",
            "UPDATE v112_pd_ro SET g = 'r' WHERE id = 5",
            "UPDATE v112_pd_ro SET g = 'a' WHERE id = 1",
            "DELETE FROM v112_pd_ro WHERE id = 2",
            "INSERT INTO v112_pd_ro_a VALUES (6,'a',60)",
            "TRUNCATE v112_pd_ro_r",
            "INSERT INTO v112_pd_ro VALUES (7,'r',70)",
        ] {
            Spi::run(statement).expect("accepted statement");
            assert_correct(statement);
        }
        for (suffix, _, _) in IMVS {
            assert_eq!(
                crate::drop_reflex_ivm(&format!("v112_pd_ro_{suffix}")),
                "DROP REFLEX INCREMENTAL VIEW"
            );
        }
        assert_eq!(v112_pd_reflex_triggers_on("v112_pd_ro_r"), Vec::<String>::new(), "{mode}");
        assert_eq!(v112_dp_reflex_trigger_functions_naming("v112_pd_ro"), 0, "{mode}: functions left");
        Spi::run("INSERT INTO v112_pd_ro_r (id, g, v) VALUES (8,'r',80)").expect("write after the drop");
        Spi::run("DROP TABLE v112_pd_ro").expect("drop source");
    }
}

/// One session: a DEFERRED flush, then partitions added, attached with rows and
/// detached. The partition flush applies each of them, leaves nothing pending
/// and flags no IMV.
#[pg_test]
fn v112_pd_partition_flush_in_a_session_that_ran_a_deferred_flush() {
    const DBNAME: &str = "reflex_pd_session";
    const IMVS: &[(&str, &str, Option<&str>, &str, Option<&str>)] = &[
        ("agg_d", "SELECT g, COUNT(*) AS c, SUM(v) AS sv FROM ps GROUP BY g", None, "DEFERRED", None),
        ("total_d", "SELECT COUNT(*) AS c, SUM(v) AS sv FROM ps", None, "DEFERRED", None),
        ("keyed_d", "SELECT id, g, v FROM ps", Some("id, g"), "DEFERRED", None),
        ("keyed_part_d", "SELECT id, g, v FROM ps", Some("id, g"), "DEFERRED", Some("g")),
        ("agg_i", "SELECT g, COUNT(*) AS c, SUM(v) AS sv FROM ps GROUP BY g", None, "IMMEDIATE", None),
    ];
    probe_db_open(DBNAME);
    for ddl in [
        "CREATE TABLE ps(id INT NOT NULL, g TEXT NOT NULL, v INT) PARTITION BY LIST (g)",
        "CREATE TABLE ps_pa PARTITION OF ps FOR VALUES IN ('a', 'b')",
        "CREATE TABLE ps_pc PARTITION OF ps FOR VALUES IN ('c') PARTITION BY RANGE (id)",
        "CREATE TABLE ps_pc1 PARTITION OF ps_pc FOR VALUES FROM (0) TO (100)",
        "CREATE TABLE ps_pd PARTITION OF ps DEFAULT",
        "INSERT INTO ps VALUES (1,'a',1),(2,'a',2),(3,'b',5),(4,'c',7),(6,'p',7)",
    ] {
        worker_exec(ddl);
    }
    for (suffix, query, key, mode, partition_by) in IMVS {
        v112_dp_create(&format!("ps_{suffix}"), query, *key, mode, *partition_by);
    }
    let mut failures = Vec::new();
    for (label, step) in [
        ("a deferred flush", "INSERT INTO ps VALUES (203,'a',3)"),
        (
            "attach a partition that holds rows",
            "CREATE TABLE ps_pf (LIKE ps INCLUDING ALL); \
             INSERT INTO ps_pf VALUES (211,'f',1),(212,'f',2); \
             ALTER TABLE ps ATTACH PARTITION ps_pf FOR VALUES IN ('f')",
        ),
        ("write after the attach", "INSERT INTO ps VALUES (213,'f',3),(214,'a',4)"),
        (
            "create a partition and fill it in one transaction",
            "CREATE TABLE ps_pi PARTITION OF ps FOR VALUES IN ('i'); \
             INSERT INTO ps VALUES (241,'i',1),(242,'i',2)",
        ),
        (
            "create a sub-partition",
            "CREATE TABLE ps_pc3 PARTITION OF ps_pc FOR VALUES FROM (200) TO (300)",
        ),
        ("fill the sub-partition", "INSERT INTO ps VALUES (221,'c',1),(222,'c',2)"),
        ("detach a partition that holds rows", "ALTER TABLE ps DETACH PARTITION ps_pf"),
        ("write after the detach", "UPDATE ps SET v = v + 1 WHERE g IN ('a', 'i')"),
    ] {
        if let Err(error) = v112_dp_try(step) {
            failures.push(format!("{label}: raised {error}"));
            continue;
        }
        for (suffix, query, _, _, _) in IMVS {
            let mismatches = rbc_mismatch(&format!("ps_{suffix}"), query);
            if mismatches != 0 {
                failures.push(format!("{label}: {suffix} {mismatches} mismatches"));
            }
            if rbc_known_stale(&format!("ps_{suffix}")) {
                failures.push(format!("{label}: {suffix} flagged stale"));
            }
        }
        let pending = worker_scalar_i64("SELECT count(*)::int8 FROM public.__reflex_partition_pending");
        if pending != 0 {
            failures.push(format!("{label}: {pending} partition root(s) left pending"));
        }
    }
    probe_db_close(DBNAME);
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// A partition added and written in the transaction that adds it, on a source
/// large enough that the added rows are a small share of it: rows the statement
/// triggers applied are not applied again by the partition flush, and rows they
/// did not see are not lost.
fn v112_pd_assert_partition_added_and_written_in_one_transaction(dbname: &str, mode: &str) {
    const IMVS: &[(&str, &str, Option<&str>, Option<&str>)] = &[
        ("agg", "SELECT g, COUNT(*) AS c, SUM(v) AS sv FROM pw GROUP BY g", None, None),
        ("minmax", "SELECT g, MIN(v) AS lo, MAX(v) AS hi FROM pw GROUP BY g", None, None),
        ("total", "SELECT COUNT(*) AS c, SUM(v) AS sv FROM pw", None, None),
        ("total_minmax", "SELECT MIN(v) AS lo, MAX(v) AS hi, COUNT(v) AS c FROM pw", None, None),
        ("bucket", "SELECT v % 7 AS b, COUNT(*) AS c, SUM(id) AS si FROM pw GROUP BY v % 7", None, None),
        ("keyed", "SELECT id, g, v FROM pw", Some("id, g"), None),
        ("keyless", "SELECT g, v FROM pw WHERE v > 0", None, None),
        ("agg_part", "SELECT g, COUNT(*) AS c, SUM(v) AS sv FROM pw GROUP BY g", None, Some("g")),
    ];
    probe_db_open(dbname);
    for ddl in [
        "CREATE TABLE pw(id INT NOT NULL, g TEXT NOT NULL, v INT) PARTITION BY LIST (g)",
        "CREATE TABLE pw_pa PARTITION OF pw FOR VALUES IN ('a', 'b')",
        "CREATE TABLE pw_pd PARTITION OF pw DEFAULT",
        "INSERT INTO pw SELECT i, CASE WHEN i % 3 = 0 THEN 'a' WHEN i % 3 = 1 THEN 'b' ELSE 'z' END, i % 50 \
           FROM generate_series(1, 6000) i",
        "ANALYZE pw",
    ] {
        worker_exec(ddl);
    }
    for (suffix, query, key, partition_by) in IMVS {
        v112_dp_create(&format!("pw_{suffix}"), query, *key, mode, *partition_by);
    }
    let mut failures = Vec::new();
    for (label, step) in [
        (
            "create and fill through the root",
            "CREATE TABLE pw_pi PARTITION OF pw FOR VALUES IN ('i'); \
             INSERT INTO pw VALUES (9001,'i',1),(9002,'i',2),(9003,'a',3)",
        ),
        (
            "create and fill directly",
            "CREATE TABLE pw_pj PARTITION OF pw FOR VALUES IN ('j'); \
             INSERT INTO pw_pj VALUES (9011,'j',1),(9012,'j',2)",
        ),
        (
            "create and fill both ways, then change the rows",
            "CREATE TABLE pw_pk PARTITION OF pw FOR VALUES IN ('k'); \
             INSERT INTO pw VALUES (9021,'k',1),(9022,'k',2); INSERT INTO pw_pk VALUES (9023,'k',3); \
             UPDATE pw SET v = v + 10 WHERE g = 'k'; DELETE FROM pw WHERE id = 9021; \
             DELETE FROM pw_pk WHERE id = 9023",
        ),
        (
            "attach a filled partition, then write through the root",
            "CREATE TABLE pw_pl (LIKE pw INCLUDING ALL); INSERT INTO pw_pl VALUES (9031,'l',1),(9032,'l',2); \
             ALTER TABLE pw ATTACH PARTITION pw_pl FOR VALUES IN ('l'); \
             INSERT INTO pw VALUES (9033,'l',3),(9034,'b',4); UPDATE pw SET v = 40 WHERE id = 9031",
        ),
        (
            "write through the root, then attach a filled partition",
            "INSERT INTO pw VALUES (9041,'a',1); CREATE TABLE pw_pm (LIKE pw INCLUDING ALL); \
             INSERT INTO pw_pm VALUES (9042,'m',2); ALTER TABLE pw ATTACH PARTITION pw_pm FOR VALUES IN ('m')",
        ),
        (
            "move rows of the default partition into a created partition",
            "CREATE TABLE pw_pz (LIKE pw INCLUDING ALL); \
             WITH moved AS (DELETE FROM pw WHERE g = 'z' AND id < 100 RETURNING *) \
               INSERT INTO pw_pz SELECT id, 'y', v FROM moved; \
             ALTER TABLE pw ATTACH PARTITION pw_pz FOR VALUES IN ('y'); \
             INSERT INTO pw VALUES (9051,'y',5)",
        ),
        (
            "detach a partition after writing to it through the root",
            "INSERT INTO pw VALUES (9061,'l',6); UPDATE pw SET v = v + 1 WHERE g = 'l'; \
             ALTER TABLE pw DETACH PARTITION pw_pl",
        ),
        ("control", "INSERT INTO pw VALUES (9071,'i',7),(9072,'a',8); DELETE FROM pw WHERE id = 9002"),
    ] {
        if let Err(error) = v112_dp_try(step) {
            failures.push(format!("{mode} {label}: raised {error}"));
            continue;
        }
        for (suffix, query, _, _) in IMVS {
            let mismatches = rbc_mismatch(&format!("pw_{suffix}"), query);
            if mismatches != 0 {
                failures.push(format!("{mode} {label}: {suffix} {mismatches} mismatches"));
            }
            if rbc_known_stale(&format!("pw_{suffix}")) {
                failures.push(format!("{mode} {label}: {suffix} flagged stale"));
            }
        }
    }
    probe_db_close(dbname);
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[pg_test]
fn v112_pd_partition_added_and_written_in_one_transaction_immediate() {
    v112_pd_assert_partition_added_and_written_in_one_transaction("reflex_pd_added_i", "IMMEDIATE");
}

#[pg_test]
fn v112_pd_partition_added_and_written_in_one_transaction_deferred() {
    v112_pd_assert_partition_added_and_written_in_one_transaction("reflex_pd_added_d", "DEFERRED");
}

/// A table detached from the source keeps the source's triggers until the
/// detaching transaction commits. A write to it in that transaction is refused:
/// its rows are no longer rows of the source, and the COMMIT-time flush removes
/// from the IMVs the rows the table holds then. After the COMMIT it is a plain
/// table again.
#[pg_test]
fn v112_pd_write_to_a_table_detached_in_the_same_transaction_is_refused() {
    const IMVS: &[(&str, &str, Option<&str>, Option<&str>)] = &[
        ("agg", "SELECT g, COUNT(*) AS c, SUM(v) AS sv FROM {s} GROUP BY g", None, None),
        ("minmax", "SELECT g, MIN(v) AS lo, MAX(v) AS hi FROM {s} GROUP BY g", None, None),
        ("keyed", "SELECT id, g, v FROM {s}", Some("id, g"), None),
        ("keyed_part", "SELECT id, g, v FROM {s}", Some("id, g"), Some("g")),
    ];
    let mut failures = Vec::new();
    for (dbname, mode) in [("reflex_pd_detach_i", "IMMEDIATE"), ("reflex_pd_detach_d", "DEFERRED")] {
        probe_db_open(dbname);
        for ddl in [
            "CREATE TABLE pt(id INT NOT NULL, g TEXT NOT NULL, v INT) PARTITION BY LIST (g)",
            "CREATE TABLE pt_pa PARTITION OF pt FOR VALUES IN ('a')",
            "CREATE TABLE pt_pl PARTITION OF pt FOR VALUES IN ('l')",
            "CREATE TABLE pt_pm PARTITION OF pt FOR VALUES IN ('m') PARTITION BY RANGE (id)",
            "CREATE TABLE pt_pm1 PARTITION OF pt_pm FOR VALUES FROM (0) TO (1000)",
            "CREATE TABLE pt_pd PARTITION OF pt DEFAULT",
            "INSERT INTO pt VALUES (1,'a',1),(281,'l',5),(282,'l',2),(301,'m',3),(4,'z',4)",
        ] {
            worker_exec(ddl);
        }
        for (suffix, query, key, partition_by) in IMVS {
            v112_dp_create(&format!("pt_{suffix}"), &query.replace("{s}", "pt"), *key, mode, *partition_by);
        }
        let check = |label: &str, failures: &mut Vec<String>| {
            for (suffix, query, _, _) in IMVS {
                let imv = format!("pt_{suffix}");
                let mismatches = rbc_mismatch(&imv, &query.replace("{s}", "pt"));
                if mismatches != 0 || rbc_known_stale(&imv) {
                    failures.push(format!(
                        "{mode} {label}: {suffix} {mismatches} mismatches {}",
                        v112_pd_difference(&imv, &query.replace("{s}", "pt"))
                    ));
                }
            }
        };
        for (label, refused) in [
            (
                "insert",
                "ALTER TABLE pt DETACH PARTITION pt_pl; INSERT INTO pt_pl VALUES (295,'l',5)",
            ),
            (
                "delete",
                "INSERT INTO pt VALUES (296,'a',6); ALTER TABLE pt DETACH PARTITION pt_pl; \
                 DELETE FROM pt_pl WHERE id = 281",
            ),
            (
                "update, then attached again",
                "ALTER TABLE pt DETACH PARTITION pt_pl; UPDATE pt_pl SET v = v + 1; \
                 ALTER TABLE pt ATTACH PARTITION pt_pl FOR VALUES IN ('l')",
            ),
            (
                "leaf of a detached branch",
                "ALTER TABLE pt DETACH PARTITION pt_pm; INSERT INTO pt_pm1 VALUES (302,'m',9)",
            ),
            (
                "a statement touching no row",
                "ALTER TABLE pt DETACH PARTITION pt_pl; DELETE FROM pt_pl WHERE id = -1",
            ),
            (
                "detached branch",
                "ALTER TABLE pt DETACH PARTITION pt_pm; UPDATE pt_pm SET v = 0",
            ),
        ] {
            match v112_dp_try(refused) {
                Ok(()) => failures.push(format!("{mode} {label}: accepted")),
                Err(error) => {
                    if !(error.contains("cannot maintain the incremental views of pt")
                        && error.contains("no longer a partition"))
                    {
                        failures.push(format!("{mode} {label}: raised {error}"));
                    }
                }
            }
            check(label, &mut failures);
        }
        for (label, accepted) in [
            ("detach and read", "ALTER TABLE pt DETACH PARTITION pt_pl; CREATE TABLE pt_archive AS SELECT * FROM pt_pl"),
            ("write after the commit", "INSERT INTO pt_pl VALUES (295,'l',5); UPDATE pt_pl SET v = 0; DELETE FROM pt_pl WHERE id = 281"),
            ("detach a branch", "ALTER TABLE pt DETACH PARTITION pt_pm"),
            ("write to the detached branch after the commit", "INSERT INTO pt_pm VALUES (303,'m',1); UPDATE pt_pm1 SET v = 7"),
            ("write to the source", "INSERT INTO pt VALUES (5,'l',5),(6,'m',6); UPDATE pt SET v = v + 1"),
        ] {
            if let Err(error) = v112_dp_try(accepted) {
                failures.push(format!("{mode} {label}: raised {error}"));
            }
            check(label, &mut failures);
        }
        probe_db_close(dbname);
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// A partition flush that failed is retried by a later transaction. Rows written
/// through the source into the added partition in between are already in the
/// IMVs: the retry does not apply them a second time.
#[pg_test]
fn v112_pd_partition_flush_retried_after_a_failure_does_not_reapply_rows() {
    const DBNAME: &str = "reflex_pd_retry";
    const IMVS: &[(&str, &str)] = &[
        ("total", "SELECT COUNT(*) AS c, SUM(v) AS sv FROM pr"),
        ("bucket", "SELECT v % 3 AS b, COUNT(*) AS c, SUM(id) AS si FROM pr GROUP BY v % 3"),
    ];
    probe_db_open(DBNAME);
    for ddl in [
        "CREATE TABLE pr(id INT NOT NULL, g TEXT NOT NULL, v INT) PARTITION BY LIST (g)",
        "CREATE TABLE pr_pa PARTITION OF pr FOR VALUES IN ('a')",
        "INSERT INTO pr VALUES (1,'a',1),(2,'a',2),(3,'a',3)",
    ] {
        worker_exec(ddl);
    }
    for (suffix, query) in IMVS {
        v112_dp_create(&format!("pr_{suffix}"), query, None, "IMMEDIATE", None);
    }
    for step in [
        "CREATE FUNCTION pr_refuse() RETURNS TRIGGER LANGUAGE plpgsql AS \
           $f$ BEGIN RAISE EXCEPTION 'refused'; END $f$",
        "CREATE TRIGGER pr_refuse BEFORE INSERT OR UPDATE OR DELETE ON pr_total \
           FOR EACH STATEMENT EXECUTE FUNCTION pr_refuse()",
        "CREATE TABLE pr_pf (LIKE pr INCLUDING ALL); INSERT INTO pr_pf VALUES (11,'f',1),(12,'f',2); \
         ALTER TABLE pr ATTACH PARTITION pr_pf FOR VALUES IN ('f')",
    ] {
        v112_dp_try(step).expect("step");
    }
    assert_eq!(
        worker_scalar_i64("SELECT count(*)::int8 FROM public.__reflex_partition_pending WHERE failures > 0"),
        1,
        "fixture: the flush of the attached partition failed and is pending"
    );
    for step in [
        "DROP TRIGGER pr_refuse ON pr_total",
        "INSERT INTO pr VALUES (13,'f',3),(4,'a',4)",
        "DO $f$ BEGIN PERFORM reflex_flush_partition_source('public.pr'); END $f$",
    ] {
        v112_dp_try(step).expect("step");
    }
    let mut failures = Vec::new();
    for (suffix, query) in IMVS {
        let imv = format!("pr_{suffix}");
        let mismatches = rbc_mismatch(&imv, query);
        if mismatches != 0 {
            failures.push(format!("{suffix} {mismatches} mismatches {}", v112_pd_difference(&imv, query)));
        }
    }
    let pending = worker_scalar_i64("SELECT count(*)::int8 FROM public.__reflex_partition_pending");
    probe_db_close(DBNAME);
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    assert_eq!(pending, 0, "the retried flush left its root pending");
}

/// A partition leaf of the IMV dropped by hand and recreated by a bare
/// `reflex_sync_partitions`: its sibling on the other side of the IMV still holds
/// the partition's rows, so the sync refills the pair from the source.
fn v112_pd_assert_sync_refills_a_recreated_leaf(prefix: &str, mode: &str, intermediate: bool) {
    let source = format!("{prefix}_t");
    let imv = format!("{prefix}_v");
    let query = format!("SELECT season, SUM(v) AS sv, COUNT(*) AS c FROM {source} GROUP BY season");
    Spi::run(&format!(
        "CREATE TABLE {source}(id INT NOT NULL, season TEXT NOT NULL, v INT) PARTITION BY LIST (season)"
    ))
    .expect("source");
    for (leaf, bound) in [
        ("p0", "FOR VALUES IN ('e', 'f')"),
        ("p1", "FOR VALUES IN ('y')"),
        ("pd", "DEFAULT"),
    ] {
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

    let parent = if intermediate {
        format!("__reflex_intermediate_{imv}")
    } else {
        imv.clone()
    };
    let leaves_of = |parent: &str| -> i64 {
        Spi::get_one::<i64>(&format!(
            "SELECT count(*) FROM pg_inherits WHERE inhparent = '\"{parent}\"'::regclass"
        ))
        .expect("leaves")
        .unwrap_or(0)
    };
    let victim = Spi::get_one::<String>(&format!(
        "SELECT c.relname::text FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid \
         WHERE i.inhparent = '\"{parent}\"'::regclass AND c.relname LIKE '%\\_p0'"
    ))
    .expect("leaf lookup")
    .expect("no leaf for p0");
    Spi::run(&format!("DROP TABLE \"{victim}\"")).expect("drop leaf");
    assert_eq!(leaves_of(&parent), 2, "fixture: the leaf is gone");

    Spi::run(&format!("SELECT reflex_sync_partitions('{imv}')")).expect("sync");
    assert_eq!(leaves_of(&parent), 3, "the sync recreates the leaf");
    assert_imv_correct(&imv, &query);
    for statement in [
        format!("INSERT INTO {source} VALUES (6,'e',4),(7,'y',1)"),
        format!("UPDATE {source} SET v = v + 1 WHERE season = 'f'"),
        format!("DELETE FROM {source} WHERE id = 2"),
        format!("INSERT INTO {source}_p0 VALUES (8,'f',8)"),
    ] {
        Spi::run(&statement).expect("write");
        if mode == "DEFERRED" {
            Spi::run(&format!("SELECT reflex_flush_deferred('{source}')")).expect("flush");
        }
        assert_imv_correct(&imv, &query);
    }
    let report = ps9_audit(&imv);
    assert!(
        !report.contains("partition-mirror") && !report.contains("archive_residue"),
        "{mode}: the audit still reports the IMV:\n{report}"
    );
}

#[pg_test]
fn v112_pd_sync_refills_a_recreated_intermediate_leaf_immediate() {
    v112_pd_assert_sync_refills_a_recreated_leaf("v112_pd_si", "IMMEDIATE", true);
}

#[pg_test]
fn v112_pd_sync_refills_a_recreated_intermediate_leaf_deferred() {
    v112_pd_assert_sync_refills_a_recreated_leaf("v112_pd_sd", "DEFERRED", true);
}

#[pg_test]
fn v112_pd_sync_refills_a_recreated_target_leaf_immediate() {
    v112_pd_assert_sync_refills_a_recreated_leaf("v112_pd_ui", "IMMEDIATE", false);
}

#[pg_test]
fn v112_pd_sync_refills_a_recreated_target_leaf_deferred() {
    v112_pd_assert_sync_refills_a_recreated_leaf("v112_pd_ud", "DEFERRED", false);
}

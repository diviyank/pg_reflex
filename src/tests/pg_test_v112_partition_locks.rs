// Maintaining the IMVs of one schema must never wait on, or keep a lock on, a
// relation of another schema. Two `dblink` sessions share a private database
// (helpers of `pg_test_partition_attach_locks.rs`, `pg_test_rebuild_commit.rs`
// and `pg_test_v112_deferred_partition.rs`). The worker maintains schema `tb`
// twice: first alone, in transactions whose locks are listed at COMMIT (a lock
// kept to COMMIT shows there); then under a `lock_timeout` while the holder
// has swapped a leaf of schema `ta` and keeps `ACCESS EXCLUSIVE` on every table
// of `ta` in an open transaction (a lock taken and released at once waits there).

const V112_PL_HELD_SCHEMA: &str = "ta";
const V112_PL_MAINTAINED_SCHEMA: &str = "tb";

fn v112_pl_holder_exec(sql: &str) {
    Spi::get_one::<String>(&format!(
        "SELECT dblink_exec('reflex_lock_holder', {})",
        sql_lit(sql)
    ))
    .unwrap_or_else(|e| panic!("holder failed for <{sql}>: {e}"));
}

/// Evaluates `expr` on the worker; the error text when it raises or when it
/// reports a failure as an `ERROR…` value.
fn v112_pl_call(expr: &str) -> Result<String, String> {
    const RAISED: &str = "__raised__";
    let value = Spi::get_one::<String>(&format!(
        "SELECT COALESCE(string_agg(COALESCE(v, ''), ''), '{RAISED}') \
         FROM dblink('reflex_lock_worker', {}, false) AS t(v text)",
        sql_lit(&format!("SELECT ({expr})::text"))
    ))
    .expect("worker call")
    .unwrap_or_default();
    if value == RAISED {
        return Err(
            Spi::get_one::<String>("SELECT dblink_error_message('reflex_lock_worker')")
                .expect("dblink_error_message")
                .unwrap_or_default(),
        );
    }
    let reports_a_failure = value.starts_with("ERROR")
        || value.contains("lock timeout")
        || value.contains("check-errored");
    if reports_a_failure {
        return Err(value);
    }
    Ok(value)
}

/// A deferred trigger that writes down, as `relation:mode`, the relation locks
/// the committing backend holds in the held schema. A flush queued at COMMIT
/// runs after the triggers queued before it, so the census queues itself again
/// several times and keeps the last reading: locks are only added until COMMIT.
fn v112_pl_install_commit_lock_census() {
    worker_exec(
        "CREATE TABLE public.v112_pl_lock_census(held text NOT NULL); \
         INSERT INTO public.v112_pl_lock_census VALUES (''); \
         CREATE TABLE public.v112_pl_census_request(readings_left int NOT NULL)",
    );
    worker_exec(&format!(
        "CREATE FUNCTION public.v112_pl_take_lock_census() RETURNS trigger LANGUAGE plpgsql AS $census$ \
         BEGIN \
           UPDATE public.v112_pl_lock_census SET held = ( \
             SELECT COALESCE(string_agg(c.relname || ':' || l.mode, ', ' ORDER BY c.relname, l.mode), '') \
               FROM pg_locks l \
               JOIN pg_class c ON c.oid = l.relation \
               JOIN pg_namespace n ON n.oid = c.relnamespace \
              WHERE l.pid = pg_backend_pid() AND l.locktype = 'relation' \
                AND l.database = (SELECT oid FROM pg_database WHERE datname = current_database()) \
                AND n.nspname = '{V112_PL_HELD_SCHEMA}'); \
           IF NEW.readings_left > 0 THEN \
             INSERT INTO public.v112_pl_census_request VALUES (NEW.readings_left - 1); \
           END IF; \
           RETURN NULL; \
         END $census$"
    ));
    worker_exec(
        "CREATE CONSTRAINT TRIGGER v112_pl_lock_census AFTER INSERT ON public.v112_pl_census_request \
         DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.v112_pl_take_lock_census()",
    );
}

/// Runs `operation` in a transaction of the worker and returns the locks that
/// transaction holds in the held schema when its COMMIT has fired every
/// deferred trigger.
fn v112_pl_locks_held_in_other_schema_at_commit(operation: &V112PlOperation) -> Result<String, String> {
    worker_exec("BEGIN");
    worker_exec("INSERT INTO public.v112_pl_census_request VALUES (8)");
    let committed = v112_pl_run(operation).and_then(|()| v112_dp_try("COMMIT"));
    if let Err(error) = committed {
        let _ = v112_dp_try("ROLLBACK");
        return Err(error);
    }
    Ok(rbc_select("SELECT held FROM public.v112_pl_lock_census"))
}

/// The two-level source of `v112_dp_truncate_fixture` and its seven IMVs, in `schema`.
fn v112_pl_schema_fixture(schema: &str, mode: &str) {
    worker_exec(&format!("CREATE SCHEMA {schema}"));
    v112_dp_truncate_fixture(&format!("{schema}.sim"), mode);
}

/// Opens the holder session, swaps a leaf of the held schema in a transaction
/// left open, and locks every table of that schema `ACCESS EXCLUSIVE`.
fn v112_pl_hold_other_schema(dbname: &str) {
    Spi::get_one::<String>(&format!(
        "SELECT dblink_connect('reflex_lock_holder', {})",
        sql_lit(&conninfo_for(dbname))
    ))
    .expect("holder connect");
    let s = format!("{V112_PL_HELD_SCHEMA}.sim");
    v112_pl_holder_exec("BEGIN");
    v112_pl_holder_exec(&format!(
        "CREATE TABLE {s}_swap (LIKE {s} INCLUDING ALL); \
         INSERT INTO {s}_swap VALUES (901,'a',9),(902,'b',8); \
         ALTER TABLE {s} DETACH PARTITION {s}_pa; \
         ALTER TABLE {s} ATTACH PARTITION {s}_swap FOR VALUES IN ('a', 'b')"
    ));
    v112_pl_holder_exec(&format!(
        "DO $$ DECLARE r record; BEGIN \
           FOR r IN SELECT c.oid::regclass AS rel FROM pg_class c \
                    JOIN pg_namespace n ON n.oid = c.relnamespace \
                    WHERE n.nspname = '{V112_PL_HELD_SCHEMA}' AND c.relkind IN ('r', 'p') \
                    ORDER BY c.oid LOOP \
             EXECUTE format('LOCK TABLE %s IN ACCESS EXCLUSIVE MODE', r.rel); \
           END LOOP; END $$"
    ));
}

/// Commits the holder's swap and returns the mismatches of the held schema's IMVs.
fn v112_pl_release_other_schema() -> Vec<String> {
    let s = format!("{V112_PL_HELD_SCHEMA}.sim");
    v112_pl_holder_exec(&format!("DROP TABLE {s}_pa"));
    v112_pl_holder_exec("COMMIT");
    let _ = Spi::get_one::<String>("SELECT dblink_disconnect('reflex_lock_holder')");
    let mut failures = Vec::new();
    for (suffix, query, _, _) in V112_DP_TRUNCATE_IMVS {
        let mismatches = rbc_mismatch(&format!("{s}_{suffix}"), &query.replace("{s}", &s));
        if mismatches != 0 {
            failures.push(format!("held schema after its commit: {suffix} {mismatches} mismatches"));
        }
    }
    failures
}

enum V112PlOperation {
    Statements(String),
    Call(String),
}

/// Everything the maintained schema does in one pass. `round` keeps the ids
/// and relation names of the second pass apart from those of the first.
fn v112_pl_operations(mode: &str, round: usize) -> Vec<(String, V112PlOperation)> {
    use V112PlOperation::{Call, Statements};
    let s = format!("{V112_PL_MAINTAINED_SCHEMA}.sim");
    let base = 1000 * (round + 1);
    let swapped_out = if round == 0 { format!("{s}_pa") } else { format!("{s}_swap{}", round - 1) };
    let swapped_in = format!("{s}_swap{round}");
    let added_imv = format!("{s}_added{round}");
    let partitioned_imvs = ["agg_part", "keyed_part"];
    let mut operations = vec![
        (
            "write through the source".to_string(),
            Statements(format!(
                "INSERT INTO {s} VALUES ({a},'a',1),({b},'c',2),({c},'q',3),({d},'c',4); \
                 UPDATE {s} SET v = v + 1 WHERE g = 'a'; DELETE FROM {s} WHERE id = {b}",
                a = base + 1,
                b = 50 + round,
                c = base + 3,
                d = 150 + round
            )),
        ),
        (
            "leaf swap".to_string(),
            Statements(format!(
                "CREATE TABLE {swapped_in} (LIKE {s} INCLUDING ALL); \
                 INSERT INTO {swapped_in} VALUES ({a},'a',7),({b},'b',8); \
                 ALTER TABLE {s} DETACH PARTITION {swapped_out}; \
                 ALTER TABLE {s} ATTACH PARTITION {swapped_in} FOR VALUES IN ('a', 'b'); \
                 DROP TABLE {swapped_out}",
                a = base + 11,
                b = base + 12
            )),
        ),
        (
            "write to the swapped leaf".to_string(),
            Statements(format!(
                "INSERT INTO {s} VALUES ({a},'b',5); UPDATE {swapped_in} SET v = v + 2 WHERE g = 'a'",
                a = base + 21
            )),
        ),
        (
            "add a partition for a new key".to_string(),
            Statements(format!(
                "CREATE TABLE {s}_k{round} PARTITION OF {s} FOR VALUES IN ('k{round}'); \
                 INSERT INTO {s} VALUES ({a},'k{round}',6)",
                a = base + 31
            )),
        ),
        (
            "reflex_flush_partition_source".to_string(),
            Call(format!("reflex_flush_partition_source({})", sql_lit(&s))),
        ),
        ("reflex_flush_partitions".to_string(), Call("reflex_flush_partitions()".to_string())),
    ];
    for suffix in partitioned_imvs {
        let imv = sql_lit(&format!("{s}_{suffix}"));
        operations.push((
            format!("reflex_sync_partitions {suffix}"),
            Call(format!("reflex_sync_partitions({imv})")),
        ));
        operations.push((
            format!("reflex_reconcile_partition by key {suffix}"),
            Call(format!("reflex_reconcile_partition({imv}, 'a')")),
        ));
        operations.push((
            format!("reflex_reconcile_partition by source partition {suffix}"),
            Call(format!("reflex_reconcile_partition({imv}, '', {})", sql_lit(&swapped_in))),
        ));
    }
    for (suffix, _, _, _) in V112_DP_TRUNCATE_IMVS {
        let imv = sql_lit(&format!("{s}_{suffix}"));
        operations.push((format!("reflex_reconcile {suffix}"), Call(format!("reflex_reconcile({imv})"))));
        operations.push((format!("reflex_audit {suffix}"), Call(format!("reflex_audit({imv})"))));
    }
    for (function, argument) in [
        ("reflex_rebuild_imv", format!("{s}_agg_part")),
        ("refresh_reflex_imv", format!("{s}_keyed_part")),
        ("refresh_imv_depending_on", s.clone()),
        ("reflex_rebuild_triggers", s.clone()),
        ("reflex_rebuild_imv_metadata", format!("{s}_agg_part")),
    ] {
        operations.push((function.to_string(), Call(format!("{function}({})", sql_lit(&argument)))));
    }
    operations.push((
        "create a partitioned IMV".to_string(),
        Call(format!(
            "create_reflex_ivm({}, {}, NULL, NULL, {}, NULL, ARRAY['g'])",
            sql_lit(&added_imv),
            sql_lit(&V112_PL_ADDED_IMV_QUERY.replace("{s}", &s)),
            sql_lit(mode)
        )),
    ));
    operations.push((
        "create an unpartitioned IMV".to_string(),
        Call(format!(
            "create_reflex_ivm({}, {}, NULL, NULL, {})",
            sql_lit(&format!("{added_imv}_flat")),
            sql_lit(&V112_PL_ADDED_IMV_QUERY.replace("{s}", &s)),
            sql_lit(mode)
        )),
    ));
    operations.push((
        "write after the creates".to_string(),
        Statements(format!(
            "INSERT INTO {s} VALUES ({a},'a',3),({b},'z',4); DELETE FROM {s} WHERE id = {c}",
            a = base + 41,
            b = base + 42,
            c = base + 11
        )),
    ));
    operations.push((
        "drop an IMV".to_string(),
        Call(format!("drop_reflex_ivm({})", sql_lit(&format!("{added_imv}_flat")))),
    ));
    operations
}

const V112_PL_ADDED_IMV_QUERY: &str =
    "SELECT g, COUNT(*) AS c, MIN(v) AS lo, MAX(v) AS hi FROM {s} WHERE v IS NOT NULL GROUP BY g";

/// Mismatches, stale flags and failed partition flushes of the maintained schema.
fn v112_pl_maintained_schema_defects(added_rounds: usize) -> Vec<String> {
    let s = format!("{V112_PL_MAINTAINED_SCHEMA}.sim");
    let mut imvs: Vec<(String, String)> = V112_DP_TRUNCATE_IMVS
        .iter()
        .map(|(suffix, query, _, _)| (format!("{s}_{suffix}"), query.replace("{s}", &s)))
        .collect();
    for round in 0..added_rounds {
        for flat in ["", "_flat"] {
            imvs.push((format!("{s}_added{round}{flat}"), V112_PL_ADDED_IMV_QUERY.replace("{s}", &s)));
        }
    }
    let mut defects = Vec::new();
    for (imv, fresh) in imvs {
        if rbc_select(&format!("to_regclass({}) IS NULL", sql_lit(&imv))) == "true" {
            continue;
        }
        let mismatches = rbc_mismatch(&imv, &fresh);
        if mismatches != 0 {
            defects.push(format!("{imv} {mismatches} mismatches"));
        }
    }
    let stale = rbc_select(&format!(
        "SELECT COALESCE(string_agg(name || ' (' || COALESCE(stale_reason, '') || ')', ', '), '') \
         FROM public.__reflex_ivm_reference WHERE known_stale AND name LIKE '{V112_PL_MAINTAINED_SCHEMA}.%'"
    ));
    if !stale.is_empty() {
        defects.push(format!("known_stale: {stale}"));
    }
    let failed_flushes = rbc_select(
        "SELECT COALESCE(string_agg(source_root || ' failures=' || failures || ' (' \
                || COALESCE(last_error, '') || ')', ', '), '') \
         FROM public.__reflex_partition_pending WHERE failures > 0 OR last_error IS NOT NULL",
    );
    if !failed_flushes.is_empty() {
        defects.push(format!("partition flush failed: {failed_flushes}"));
    }
    defects
}

fn v112_pl_run(operation: &V112PlOperation) -> Result<(), String> {
    match operation {
        V112PlOperation::Statements(sql) => v112_dp_try(sql),
        V112PlOperation::Call(expr) => v112_pl_call(expr).map(|_| ()),
    }
}

/// Every operation of the maintained schema: first alone, in a transaction
/// whose locks in the other schema are listed at COMMIT, then as its own
/// transaction while the other schema is held.
fn v112_pl_failures_of_maintaining_beside_another_schema(dbname: &str, mode: &str) -> Vec<String> {
    const LOCKS_INSPECTED: usize = 0;
    const OTHER_SCHEMA_HELD: usize = 1;
    probe_db_open(dbname);
    v112_pl_schema_fixture(V112_PL_HELD_SCHEMA, mode);
    v112_pl_schema_fixture(V112_PL_MAINTAINED_SCHEMA, mode);
    let mut failures = Vec::new();

    v112_pl_install_commit_lock_census();
    let census_of_a_lock_taken = v112_pl_locks_held_in_other_schema_at_commit(&V112PlOperation::Statements(
        format!("LOCK TABLE {V112_PL_HELD_SCHEMA}.sim_pd IN ACCESS SHARE MODE"),
    ));
    if census_of_a_lock_taken != Ok("sim_pd:AccessShareLock".to_string()) {
        failures.push(format!(
            "fixture: the census of a transaction that locks one table of the held schema reads {census_of_a_lock_taken:?}"
        ));
    }

    for round in [LOCKS_INSPECTED, OTHER_SCHEMA_HELD] {
        let pass = if round == LOCKS_INSPECTED { "locks inspected" } else { "other schema held" };
        if round == OTHER_SCHEMA_HELD {
            v112_pl_hold_other_schema(dbname);
            worker_exec("SET lock_timeout = '4s'");
        }
        for (label, operation) in v112_pl_operations(mode, round) {
            let outcome = if round == LOCKS_INSPECTED {
                v112_pl_locks_held_in_other_schema_at_commit(&operation)
            } else {
                v112_pl_run(&operation).map(|()| String::new())
            };
            match outcome {
                Err(error) => failures.push(format!("{mode}, {pass}, {label}: {error}")),
                Ok(held) if !held.is_empty() => failures.push(format!(
                    "{mode}, {pass}, {label}: holds locks in the other schema: {held}"
                )),
                Ok(_) => {}
            }
            for defect in v112_pl_maintained_schema_defects(round + 1) {
                failures.push(format!("{mode}, {pass}, {label}: {defect}"));
            }
        }
    }
    worker_exec("RESET lock_timeout");
    failures.extend(v112_pl_release_other_schema());
    for defect in v112_pl_maintained_schema_defects(2) {
        failures.push(format!("{mode}, after the other schema committed: {defect}"));
    }
    probe_db_close(dbname);
    failures
}

#[pg_test]
fn v112_pl_maintaining_one_schema_takes_no_lock_in_another_immediate() {
    let failures = v112_pl_failures_of_maintaining_beside_another_schema("reflex_pl_imm", "IMMEDIATE");
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[pg_test]
fn v112_pl_maintaining_one_schema_takes_no_lock_in_another_deferred() {
    let failures = v112_pl_failures_of_maintaining_beside_another_schema("reflex_pl_def", "DEFERRED");
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// The DEFAULT partitions of a tree, as the bound of each relation states it.
const V112_PL_DEFAULTS_BY_BOUND_TEXT: &str =
    "SELECT quote_ident(n.nspname) || '.' || quote_ident(c.relname) AS q, t.level AS lvl \
       FROM pg_partition_tree($1) t \
       JOIN pg_class c ON c.oid = t.relid \
       JOIN pg_namespace n ON n.oid = c.relnamespace \
      WHERE pg_get_expr(c.relpartbound, c.oid) = 'DEFAULT'";

fn v112_pl_defaults(enumeration: &str, root: &str) -> Vec<String> {
    let listed = Spi::get_one::<String>(&format!(
        "SELECT COALESCE(string_agg(q || '@' || lvl, ',' ORDER BY q), '') FROM ({}) d",
        enumeration.replace("$1", &sql_lit(root))
    ))
    .expect("default enumeration")
    .unwrap_or_default();
    listed.split(',').filter(|s| !s.is_empty()).map(str::to_string).collect()
}

#[pg_test]
fn v112_pl_drain_enumerates_exactly_the_default_partitions_of_its_tree() {
    for ddl in [
        "CREATE SCHEMA \"v112 Pl\"",
        "CREATE TABLE v112_pl_two(id INT, g TEXT, v INT) PARTITION BY LIST (g)",
        "CREATE TABLE v112_pl_two_a PARTITION OF v112_pl_two FOR VALUES IN ('a')",
        "CREATE TABLE v112_pl_two_c PARTITION OF v112_pl_two FOR VALUES IN ('c') PARTITION BY RANGE (id)",
        "CREATE TABLE v112_pl_two_c1 PARTITION OF v112_pl_two_c FOR VALUES FROM (0) TO (100)",
        "CREATE TABLE \"v112 Pl\".\"Two C rest\" PARTITION OF v112_pl_two_c DEFAULT",
        "CREATE TABLE v112_pl_two_rest PARTITION OF v112_pl_two DEFAULT",
        "CREATE TABLE v112_pl_none(id INT, g TEXT, v INT) PARTITION BY LIST (g)",
        "CREATE TABLE v112_pl_none_a PARTITION OF v112_pl_none FOR VALUES IN ('a')",
        "CREATE TABLE v112_pl_none_c PARTITION OF v112_pl_none FOR VALUES IN ('c') PARTITION BY RANGE (id)",
        "CREATE TABLE v112_pl_none_c1 PARTITION OF v112_pl_none_c FOR VALUES FROM (0) TO (100)",
        "CREATE TABLE v112_pl_nested(id INT, g TEXT, v INT) PARTITION BY LIST (g)",
        "CREATE TABLE v112_pl_nested_a PARTITION OF v112_pl_nested FOR VALUES IN ('a')",
        "CREATE TABLE v112_pl_nested_rest PARTITION OF v112_pl_nested DEFAULT PARTITION BY RANGE (id)",
        "CREATE TABLE v112_pl_nested_rest1 PARTITION OF v112_pl_nested_rest FOR VALUES FROM (0) TO (100)",
        "CREATE TABLE v112_pl_nested_rest_rest PARTITION OF v112_pl_nested_rest DEFAULT",
        "CREATE TABLE v112_pl_plain(id INT, g TEXT, v INT)",
    ] {
        Spi::run(ddl).expect(ddl);
    }
    let expected: &[(&str, &[&str])] = &[
        ("v112_pl_two", &["\"v112 Pl\".\"Two C rest\"@2", "public.v112_pl_two_rest@1"]),
        ("v112_pl_two_c", &["\"v112 Pl\".\"Two C rest\"@1"]),
        ("v112_pl_two_rest", &["public.v112_pl_two_rest@0"]),
        ("v112_pl_two_a", &[]),
        ("v112_pl_none", &[]),
        ("v112_pl_nested", &["public.v112_pl_nested_rest@1", "public.v112_pl_nested_rest_rest@2"]),
        ("v112_pl_nested_rest", &["public.v112_pl_nested_rest@0", "public.v112_pl_nested_rest_rest@1"]),
        ("v112_pl_plain", &[]),
    ];
    for (root, defaults) in expected {
        let found = v112_pl_defaults(crate::partition::TREE_DEFAULT_PARTITIONS_SQL, root);
        assert_eq!(&found, defaults, "DEFAULT partitions in the tree of {root}");
        assert_eq!(
            found,
            v112_pl_defaults(V112_PL_DEFAULTS_BY_BOUND_TEXT, root),
            "DEFAULT partitions in the tree of {root}, against the bound text"
        );
    }

    Spi::run(
        "INSERT INTO v112_pl_two VALUES (1,'a',1),(2,'c',2),(500,'c',3),(501,'c',4),(3,'x',5),(4,'y',6); \
         INSERT INTO v112_pl_nested VALUES (1,'a',1),(2,'x',2),(700,'y',3),(701,'z',4)",
    )
    .expect("rows in every default");
    for (root, build, landed) in [
        (
            "v112_pl_two",
            "CREATE TABLE v112_pl_two_x PARTITION OF v112_pl_two FOR VALUES IN ('x'); \
             CREATE TABLE v112_pl_two_c5 PARTITION OF v112_pl_two_c FOR VALUES FROM (500) TO (501)",
            "SELECT (SELECT count(*) FROM v112_pl_two_x) = 1 AND (SELECT count(*) FROM v112_pl_two_c5) = 1 \
                AND (SELECT count(*) FROM v112_pl_two_rest) = 1 \
                AND (SELECT count(*) FROM \"v112 Pl\".\"Two C rest\") = 1 \
                AND (SELECT count(*) FROM v112_pl_two) = 6",
        ),
        (
            "v112_pl_nested",
            "CREATE TABLE v112_pl_nested_rest7 PARTITION OF v112_pl_nested_rest FOR VALUES FROM (700) TO (701)",
            "SELECT (SELECT count(*) FROM v112_pl_nested_rest7) = 1 \
                AND (SELECT count(*) FROM v112_pl_nested_rest_rest) = 1 \
                AND (SELECT count(*) FROM v112_pl_nested_rest1) = 1 \
                AND (SELECT count(*) FROM v112_pl_nested) = 4",
        ),
    ] {
        let outcome = Spi::get_one::<String>(&format!(
            "SELECT __reflex_test_drain_build_refill({}, {})",
            sql_lit(root),
            sql_lit(build)
        ))
        .expect("drain, build, refill")
        .unwrap_or_default();
        assert_eq!(outcome, "OK", "drain, build and refill of {root}");
        assert_eq!(
            Spi::get_one::<bool>(landed).expect("row placement"),
            Some(true),
            "rows of {root} after the drain, the new partitions and the refill"
        );
    }
}

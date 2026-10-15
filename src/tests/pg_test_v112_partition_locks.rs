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

/// The error the last call over `session` raised, if any. `dblink(..., false)`
/// turns errors, a failed COMMIT included, into NOTICEs and may still return the
/// rows of an earlier statement, so the rows alone never prove the call succeeded.
fn dblink_last_error(session: &str) -> Option<String> {
    let message = Spi::get_one::<String>(&format!("SELECT dblink_error_message('{session}')"))
        .expect("dblink_error_message")
        .unwrap_or_default();
    (!message.is_empty() && message != "OK").then_some(message)
}

/// Evaluates `expr` on the worker; the error text when it raises or when it
/// reports a failure as an `ERROR…` value.
fn v112_pl_call(expr: &str) -> Result<String, String> {
    v112_pl_call_on("reflex_lock_worker", expr)
}

fn v112_pl_call_on(session: &str, expr: &str) -> Result<String, String> {
    const RAISED: &str = "__raised__";
    let value = Spi::get_one::<String>(&format!(
        "SELECT COALESCE(string_agg(COALESCE(v, ''), ''), '{RAISED}') \
         FROM dblink('{session}', {}, false) AS t(v text)",
        sql_lit(&format!("SELECT ({expr})::text"))
    ))
    .expect("call over dblink")
    .unwrap_or_default();
    if let Some(error) = dblink_last_error(session) {
        return Err(error);
    }
    if value == RAISED {
        return Err(String::new());
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
    worker_exec(&format!(
        "CREATE TABLE {schema}.dim(g TEXT PRIMARY KEY, label TEXT NOT NULL); \
         INSERT INTO {schema}.dim VALUES ('a','first'),('b','first'),('c','second'),('p','rest'),('z','rest')"
    ));
    for (suffix, query, key) in V112_PL_JOIN_AND_CHAIN_IMVS {
        v112_dp_create(&format!("{schema}.{suffix}"), &query.replace("{schema}", schema), *key, mode, None);
    }
    for round in 0..2 {
        worker_exec(&format!(
            "CREATE TABLE {schema}.side{round}(id INT PRIMARY KEY, v INT); \
             INSERT INTO {schema}.side{round} VALUES (1, 1), (2, 2)"
        ));
        v112_dp_create(
            &format!("{schema}.side{round}_sum"),
            &format!("SELECT COUNT(*) AS c, SUM(v) AS sv FROM {schema}.side{round}"),
            None,
            mode,
            None,
        );
    }
}

/// A join with a second table and a chain of two CTEs, each kept as a sub-IMV:
/// (name, query over `{schema}`, key).
const V112_PL_JOIN_AND_CHAIN_IMVS: &[(&str, &str, Option<&str>)] = &[
    (
        "sim_join",
        "SELECT d.label, COUNT(*) AS c, SUM(m.v) AS sv FROM {schema}.sim m \
         JOIN {schema}.dim d ON d.g = m.g GROUP BY d.label",
        None,
    ),
    (
        "sim_chain",
        "WITH per AS (SELECT g, SUM(v) AS s, COUNT(*) AS n FROM {schema}.sim GROUP BY g), \
              kept AS (SELECT g, s FROM per WHERE n > 0) \
         SELECT g, s FROM kept WHERE s IS NOT NULL",
        None,
    ),
];

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
    let schema = V112_PL_MAINTAINED_SCHEMA;
    operations.push((
        "write to the joined table".to_string(),
        Statements(format!(
            "UPDATE {schema}.dim SET label = 'moved{round}' WHERE g = 'b'; \
             INSERT INTO {schema}.dim VALUES ('k{round}', 'new')"
        )),
    ));
    operations.push((
        "truncate a leaf".to_string(),
        Statements(format!("TRUNCATE {s}_pd; INSERT INTO {s} VALUES ({a},'p',1)", a = base + 51)),
    ));
    operations.push((
        "truncate the source".to_string(),
        Statements(format!(
            "TRUNCATE {s}; INSERT INTO {s} VALUES ({a},'a',1),({b},'b',2),({c},'c',3),({d},'p',4)",
            a = base + 61,
            b = base + 62,
            c = 60 + round,
            d = base + 64
        )),
    ));
    operations.push((
        "reflex_flush_deferred".to_string(),
        Call(format!("reflex_flush_deferred({})", sql_lit(&s))),
    ));
    operations.push((
        "reflex_heal_ignored_sources".to_string(),
        Call(format!("reflex_heal_ignored_sources({})", sql_lit(&format!("{s}_agg")))),
    ));
    operations.push((
        "drop a source table".to_string(),
        Statements(format!("DROP TABLE {schema}.side{round}")),
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
    let schema = V112_PL_MAINTAINED_SCHEMA;
    for (suffix, query, _) in V112_PL_JOIN_AND_CHAIN_IMVS {
        imvs.push((format!("{schema}.{suffix}"), query.replace("{schema}", schema)));
    }
    for round in 0..2 {
        imvs.push((
            format!("{schema}.side{round}_sum"),
            format!("SELECT COUNT(*) AS c, SUM(v) AS sv FROM {schema}.side{round}"),
        ));
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

/// The source of `v112_dp_truncate_fixture` and its seven IMVs under the same
/// names in `public` (registered without a schema) and in each of `schemas`.
const V112_PL_SAME_NAME_SCHEMAS: &[&str] = &["public", "ta", "tb"];

fn v112_pl_sim_of(schema: &str) -> String {
    if schema == "public" { "sim".to_string() } else { format!("{schema}.sim") }
}

fn v112_pl_same_name_fixture(mode: &str) {
    for schema in V112_PL_SAME_NAME_SCHEMAS {
        if *schema != "public" {
            worker_exec(&format!("CREATE SCHEMA {schema}"));
        }
        v112_dp_truncate_fixture(&v112_pl_sim_of(schema), mode);
    }
    // An IMV of public whose unqualified name is the name of a partitioned
    // source of another schema, over a table of public that other schemas
    // also have under the same name, partitioned and read by no IMV.
    worker_exec("CREATE TABLE public.plain(id INT PRIMARY KEY, v INT); INSERT INTO public.plain VALUES (1, 1)");
    v112_dp_create("zed", "SELECT id, v FROM plain", Some("id"), mode, None);
    for schema in ["ta", "tb"] {
        worker_exec(&format!(
            "CREATE TABLE {schema}.plain(id INT NOT NULL, v INT) PARTITION BY RANGE (id); \
             CREATE TABLE {schema}.plain_low PARTITION OF {schema}.plain FOR VALUES FROM (0) TO (100); \
             CREATE TABLE {schema}.zed(id INT NOT NULL, g TEXT NOT NULL, v INT) PARTITION BY LIST (g); \
             CREATE TABLE {schema}.zed_a PARTITION OF {schema}.zed FOR VALUES IN ('a'); \
             INSERT INTO {schema}.zed VALUES (1,'a',1),(2,'a',2)"
        ));
        for (suffix, query, key, partition_by) in V112_PL_ZED_IMVS {
            v112_dp_create(
                &format!("{schema}.zed_{suffix}"),
                &query.replace("{s}", &format!("{schema}.zed")),
                *key,
                mode,
                *partition_by,
            );
        }
    }
}

const V112_PL_ZED_IMVS: &[(&str, &str, Option<&str>, Option<&str>)] = &[
    ("agg", "SELECT g, COUNT(*) AS c, SUM(v) AS sv FROM {s} GROUP BY g", None, None),
    ("total", "SELECT COUNT(*) AS c, SUM(v) AS sv FROM {s}", None, None),
    ("part", "SELECT id, g, v FROM {s}", Some("id, g"), Some("g")),
];

/// Every IMV of the same-name fixture read by `schema`, with its query.
fn v112_pl_same_name_imvs(schema: &str) -> Vec<(String, String)> {
    let sim = v112_pl_sim_of(schema);
    let mut imvs: Vec<(String, String)> = V112_DP_TRUNCATE_IMVS
        .iter()
        .map(|(suffix, query, _, _)| (format!("{sim}_{suffix}"), query.replace("{s}", &sim)))
        .collect();
    if schema == "public" {
        imvs.push(("zed".to_string(), "SELECT id, v FROM plain".to_string()));
    } else {
        let zed = format!("{schema}.zed");
        for (suffix, query, _, _) in V112_PL_ZED_IMVS {
            imvs.push((format!("{zed}_{suffix}"), query.replace("{s}", &zed)));
        }
    }
    imvs
}

/// Every kind of partition DDL on the `sim` of `schema`, then on its `zed`.
fn v112_pl_partition_ddl_of(schema: &str) -> Vec<(&'static str, String)> {
    let s = v112_pl_sim_of(schema);
    let mut steps = vec![
        (
            "create a partition",
            format!("CREATE TABLE {s}_n PARTITION OF {s} FOR VALUES IN ('n'); INSERT INTO {s} VALUES (301,'n',1)"),
        ),
        (
            "attach a partition that holds rows",
            format!(
                "CREATE TABLE {s}_t (LIKE {s} INCLUDING ALL); INSERT INTO {s}_t VALUES (311,'t',1),(312,'t',2); \
                 ALTER TABLE {s} ATTACH PARTITION {s}_t FOR VALUES IN ('t')"
            ),
        ),
        (
            "leaf swap",
            format!(
                "CREATE TABLE {s}_swap (LIKE {s} INCLUDING ALL); INSERT INTO {s}_swap VALUES (321,'a',7),(322,'b',8); \
                 ALTER TABLE {s} DETACH PARTITION {s}_pa; \
                 ALTER TABLE {s} ATTACH PARTITION {s}_swap FOR VALUES IN ('a', 'b'); DROP TABLE {s}_pa"
            ),
        ),
        (
            "create a sub-partition",
            format!("CREATE TABLE {s}_pc3 PARTITION OF {s}_pc FOR VALUES FROM (200) TO (300)"),
        ),
        ("insert into the sub-partition", format!("INSERT INTO {s} VALUES (250,'c',5)")),
        (
            "detach a partition that holds rows",
            format!("ALTER TABLE {s} DETACH PARTITION {s}_t"),
        ),
        (
            "detach and drop a partition",
            format!("ALTER TABLE {s} DETACH PARTITION {s}_n; DROP TABLE {s}_n"),
        ),
        (
            "create and drop an empty partition",
            format!("CREATE TABLE {s}_e PARTITION OF {s} FOR VALUES IN ('e'); DROP TABLE {s}_e"),
        ),
        ("write after the partition changes", format!("INSERT INTO {s} VALUES (331,'a',1),(332,'q',2)")),
    ];
    if schema != "public" {
        let z = format!("{schema}.zed");
        steps.extend([
            (
                "attach to the source named like an IMV of public",
                format!(
                    "CREATE TABLE {z}_att (LIKE {z} INCLUDING ALL); INSERT INTO {z}_att VALUES (5,'z',5),(6,'z',6); \
                     ALTER TABLE {z} ATTACH PARTITION {z}_att FOR VALUES IN ('z')"
                ),
            ),
            (
                "swap a leaf of the source named like an IMV of public",
                format!(
                    "CREATE TABLE {z}_swap (LIKE {z} INCLUDING ALL); INSERT INTO {z}_swap VALUES (7,'a',7); \
                     ALTER TABLE {z} DETACH PARTITION {z}_a; \
                     ALTER TABLE {z} ATTACH PARTITION {z}_swap FOR VALUES IN ('a'); DROP TABLE {z}_a"
                ),
            ),
            (
                "create a partition of the source named like an IMV of public",
                format!("CREATE TABLE {z}_b PARTITION OF {z} FOR VALUES IN ('b'); INSERT INTO {z} VALUES (8,'b',8)"),
            ),
            (
                "add a partition to a table no IMV of its schema reads",
                format!(
                    "CREATE TABLE {schema}.plain_high PARTITION OF {schema}.plain FOR VALUES FROM (100) TO (200); \
                     INSERT INTO {schema}.plain VALUES (150, 1)"
                ),
            ),
            (
                "alter a table no IMV of its schema reads, under the error policy",
                format!(
                    "SET LOCAL pg_reflex.alter_source_policy = 'error'; \
                     ALTER TABLE {schema}.plain ADD COLUMN note TEXT"
                ),
            ),
        ]);
    }
    steps
}

/// The partitions of every IMV-side tree of `schema`, to tell whether a
/// partition change elsewhere synchronised them.
fn v112_pl_imv_partitions_of(schema: &str) -> String {
    rbc_select(&format!(
        "SELECT COALESCE(string_agg(c.relname, ',' ORDER BY c.relname), '') \
         FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid \
         JOIN pg_class p ON p.oid = i.inhparent JOIN pg_namespace n ON n.oid = p.relnamespace \
         WHERE n.nspname = {} AND p.relname NOT IN ('sim', 'sim_pc', 'zed')",
        sql_lit(schema)
    ))
}

/// Stale IMVs and failed partition flushes, whatever the schema.
fn v112_pl_registry_defects() -> Vec<String> {
    let mut defects = Vec::new();
    let stale = rbc_select(
        "SELECT COALESCE(string_agg(name || ' (' || COALESCE(stale_reason, '') || ')', ', ' ORDER BY name), '') \
         FROM public.__reflex_ivm_reference WHERE known_stale",
    );
    if !stale.is_empty() {
        defects.push(format!("known_stale: {stale}"));
    }
    let pending = rbc_select(
        "SELECT COALESCE(string_agg(source_root || ' failures=' || failures || ' (' \
                || COALESCE(last_error, '') || ')', ', ' ORDER BY source_root), '') \
         FROM public.__reflex_partition_pending",
    );
    if !pending.is_empty() {
        defects.push(format!("partition changes left pending: {pending}"));
    }
    defects
}

fn v112_pl_mismatches_of(schema: &str) -> Vec<String> {
    v112_pl_same_name_imvs(schema)
        .into_iter()
        .filter_map(|(imv, fresh)| {
            let mismatches = rbc_mismatch(&imv, &fresh);
            (mismatches != 0).then(|| format!("{imv} {mismatches} mismatches"))
        })
        .collect()
}

/// Opens the holder session and locks `ACCESS EXCLUSIVE` every table of the
/// same-name fixture outside `active`, the tables of public that every schema
/// shares excepted.
fn v112_pl_hold_schemas_other_than(dbname: &str, active: &str) {
    Spi::get_one::<String>(&format!(
        "SELECT dblink_connect('reflex_lock_holder', {})",
        sql_lit(&conninfo_for(dbname))
    ))
    .expect("holder connect");
    v112_pl_holder_exec("BEGIN");
    v112_pl_holder_exec(&format!(
        "DO $$ DECLARE r record; BEGIN \
           FOR r IN SELECT c.oid::regclass AS rel FROM pg_class c \
                    JOIN pg_namespace n ON n.oid = c.relnamespace \
                    WHERE n.nspname IN ('public', 'ta', 'tb') AND n.nspname <> '{active}' \
                      AND c.relkind IN ('r', 'p') \
                      AND c.relname NOT IN ('__reflex_ivm_reference', '__reflex_event_log', \
                          '__reflex_source_partition_snapshot', '__reflex_partition_pending', \
                          '__reflex_heal_pending', '__reflex_deferred_pending', \
                          'v112_pl_lock_census', 'v112_pl_census_request') \
                    ORDER BY c.oid LOOP \
             EXECUTE format('LOCK TABLE %s IN ACCESS EXCLUSIVE MODE', r.rel); \
           END LOOP; END $$"
    ));
}

fn v112_pl_release_holder() {
    v112_pl_holder_exec("COMMIT");
    let _ = Spi::get_one::<String>("SELECT dblink_disconnect('reflex_lock_holder')");
}

/// Partition DDL on the `sim` of one schema while the same-named sources and
/// IMVs of the two other schemas are held: it waits on none of them, leaves
/// them as they were, and its own IMVs follow.
fn v112_pl_failures_of_partition_ddl_beside_same_named_sources(dbname: &str, mode: &str) -> Vec<String> {
    probe_db_open(dbname);
    v112_pl_same_name_fixture(mode);
    let mut failures = Vec::new();
    for active in ["tb", "public"] {
        let others: Vec<&str> =
            V112_PL_SAME_NAME_SCHEMAS.iter().copied().filter(|schema| *schema != active).collect();
        let partitions_before: Vec<String> =
            others.iter().map(|schema| v112_pl_imv_partitions_of(schema)).collect();
        v112_pl_hold_schemas_other_than(dbname, active);
        worker_exec("SET lock_timeout = '4s'");
        for (label, step) in v112_pl_partition_ddl_of(active) {
            if let Err(error) = v112_dp_try(&step) {
                failures.push(format!("{mode}, {active}, {label}: {error}"));
            }
            let defects = v112_pl_mismatches_of(active).into_iter().chain(v112_pl_registry_defects());
            failures.extend(defects.map(|defect| format!("{mode}, {active}, {label}: {defect}")));
        }
        worker_exec("RESET lock_timeout");
        v112_pl_release_holder();
        for (schema, before) in others.iter().zip(partitions_before) {
            let after = v112_pl_imv_partitions_of(schema);
            if after != before {
                failures.push(format!(
                    "{mode}, {active}: the IMV partitions of {schema} changed from [{before}] to [{after}]"
                ));
            }
            let defects = v112_pl_mismatches_of(schema).into_iter();
            failures.extend(defects.map(|defect| format!("{mode}, after the partition DDL of {active}: {defect}")));
        }
    }
    failures.extend(v112_pl_imvs_flagged_outside("tb", dbname).into_iter().map(|flagged| {
        format!("{mode}, a partition flush of tb.sim that fails flags an IMV of another schema: {flagged}")
    }));
    probe_db_close(dbname);
    failures
}

/// Makes a partition flush of `schema`'s `sim` fail on a lock of its own IMV
/// and returns the IMVs flagged stale that do not belong to `schema`.
fn v112_pl_imvs_flagged_outside(schema: &str, dbname: &str) -> Vec<String> {
    Spi::get_one::<String>(&format!(
        "SELECT dblink_connect('reflex_lock_holder', {})",
        sql_lit(&conninfo_for(dbname))
    ))
    .expect("holder connect");
    v112_pl_holder_exec("BEGIN");
    v112_pl_holder_exec(&format!("LOCK TABLE {schema}.sim_agg IN ACCESS EXCLUSIVE MODE"));
    worker_exec("SET lock_timeout = '1s'");
    let _ = v112_dp_try(&format!(
        "CREATE TABLE {schema}.sim_w PARTITION OF {schema}.sim FOR VALUES IN ('w')"
    ));
    worker_exec("RESET lock_timeout");
    v112_pl_release_holder();
    let flagged = rbc_select(
        "SELECT COALESCE(string_agg(name, ',' ORDER BY name), '') FROM public.__reflex_ivm_reference WHERE known_stale",
    );
    let flagged: Vec<String> = flagged.split(',').filter(|name| !name.is_empty()).map(str::to_string).collect();
    let mut outside: Vec<String> =
        flagged.iter().filter(|name| !name.starts_with(&format!("{schema}."))).cloned().collect();
    if flagged.len() == outside.len() {
        outside.push(format!("(fixture: no IMV of {schema} was flagged, the flush did not fail)"));
    }
    outside
}

#[pg_test]
fn v112_pl_partition_ddl_reaches_only_the_imvs_of_its_own_source_immediate() {
    let failures = v112_pl_failures_of_partition_ddl_beside_same_named_sources("reflex_pl_same_imm", "IMMEDIATE");
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[pg_test]
fn v112_pl_partition_ddl_reaches_only_the_imvs_of_its_own_source_deferred() {
    let failures = v112_pl_failures_of_partition_ddl_beside_same_named_sources("reflex_pl_same_def", "DEFERRED");
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

const V112_PL_DEFERRED_CREATE_QUERY: &str =
    "SELECT g, COUNT(*) AS c, SUM(v) AS sv FROM {s} WHERE v IS NOT NULL GROUP BY g";

fn v112_pl_deferred_create(imv: &str, source: &str) -> String {
    format!(
        "create_reflex_ivm({}, {}, NULL, NULL, 'DEFERRED')",
        sql_lit(imv),
        sql_lit(&V112_PL_DEFERRED_CREATE_QUERY.replace("{s}", source))
    )
}

/// A DEFERRED create left open in one schema, and DEFERRED writes left open in
/// one schema, wait on nothing and hold up nothing of another schema.
#[pg_test]
fn v112_pl_deferred_create_and_deferred_writes_of_two_schemas_do_not_wait_on_each_other() {
    const DBNAME: &str = "reflex_pl_defcreate";
    probe_db_open(DBNAME);
    for schema in ["ta", "tb"] {
        worker_exec(&format!("CREATE SCHEMA {schema}"));
        v112_dp_truncate_fixture(&format!("{schema}.sim"), "DEFERRED");
    }
    let mut failures = Vec::new();
    let mut tb_imvs: Vec<(String, String)> = V112_DP_TRUNCATE_IMVS
        .iter()
        .map(|(suffix, query, _, _)| (format!("tb.sim_{suffix}"), query.replace("{s}", "tb.sim")))
        .collect();
    let check_tb = |failures: &mut Vec<String>, label: &str, imvs: &[(String, String)]| {
        for (imv, fresh) in imvs {
            let mismatches = rbc_mismatch(imv, fresh);
            if mismatches != 0 {
                failures.push(format!("{label}: {imv} {mismatches} mismatches"));
            }
        }
        failures.extend(v112_pl_registry_defects().into_iter().map(|defect| format!("{label}: {defect}")));
    };
    Spi::get_one::<String>(&format!(
        "SELECT dblink_connect('reflex_lock_holder', {})",
        sql_lit(&conninfo_for(DBNAME))
    ))
    .expect("holder connect");
    worker_exec("SET lock_timeout = '4s'");

    v112_pl_holder_exec("SET lock_timeout = '4s'");
    v112_pl_holder_exec("BEGIN");
    if let Err(error) = v112_pl_call_on("reflex_lock_holder", &v112_pl_deferred_create("ta.sim_open", "ta.sim")) {
        failures.push(format!("fixture: the DEFERRED create left open raised {error}"));
    }
    for (label, operation) in [
        (
            "writes",
            V112PlOperation::Statements(
                "INSERT INTO tb.sim VALUES (401,'a',1),(402,'q',2); UPDATE tb.sim SET v = v + 1 WHERE g = 'a'; \
                 DELETE FROM tb.sim WHERE id = 402"
                    .to_string(),
            ),
        ),
        (
            "truncate a leaf",
            V112PlOperation::Statements("TRUNCATE tb.sim_pd; INSERT INTO tb.sim VALUES (403,'p',3)".to_string()),
        ),
        ("reflex_flush_deferred", V112PlOperation::Call("reflex_flush_deferred('tb.sim')".to_string())),
        (
            "create a partition",
            V112PlOperation::Statements(
                "CREATE TABLE tb.sim_n PARTITION OF tb.sim FOR VALUES IN ('n'); INSERT INTO tb.sim VALUES (404,'n',4)"
                    .to_string(),
            ),
        ),
        (
            "leaf swap",
            V112PlOperation::Statements(
                "CREATE TABLE tb.sim_swap (LIKE tb.sim INCLUDING ALL); INSERT INTO tb.sim_swap VALUES (405,'a',5); \
                 ALTER TABLE tb.sim DETACH PARTITION tb.sim_pa; \
                 ALTER TABLE tb.sim ATTACH PARTITION tb.sim_swap FOR VALUES IN ('a', 'b'); DROP TABLE tb.sim_pa"
                    .to_string(),
            ),
        ),
        ("DEFERRED create", V112PlOperation::Call(v112_pl_deferred_create("tb.sim_beside_create", "tb.sim"))),
        ("write after the create", V112PlOperation::Statements("INSERT INTO tb.sim VALUES (406,'b',6)".to_string())),
    ] {
        let ran = v112_pl_run(&operation);
        if let Err(error) = &ran {
            failures.push(format!("beside an open DEFERRED create, {label}: {error}"));
        }
        if label == "DEFERRED create" && ran.is_ok() {
            tb_imvs.push((
                "tb.sim_beside_create".to_string(),
                V112_PL_DEFERRED_CREATE_QUERY.replace("{s}", "tb.sim"),
            ));
        }
        check_tb(&mut failures, &format!("beside an open DEFERRED create, {label}"), &tb_imvs);
    }
    v112_pl_holder_exec("COMMIT");

    v112_pl_holder_exec("BEGIN");
    v112_pl_holder_exec("INSERT INTO ta.sim VALUES (501,'a',1),(502,'q',2)");
    if let Err(error) = v112_pl_call(&v112_pl_deferred_create("tb.sim_beside_writes", "tb.sim")) {
        failures.push(format!("DEFERRED create beside open DEFERRED writes: {error}"));
    } else {
        tb_imvs.push(("tb.sim_beside_writes".to_string(), V112_PL_DEFERRED_CREATE_QUERY.replace("{s}", "tb.sim")));
    }
    check_tb(&mut failures, "DEFERRED create beside open DEFERRED writes", &tb_imvs);

    // Both schemas now hold DEFERRED writes open. A create over a source this
    // transaction wrote is refused before it touches anything; committed first,
    // it is accepted.
    worker_exec("BEGIN");
    worker_exec("INSERT INTO tb.sim VALUES (407,'a',7)");
    for (session, schema) in [("reflex_lock_worker", "tb"), ("reflex_lock_holder", "ta")] {
        let imv = format!("{schema}.sim_after_own_write");
        let source = format!("{schema}.sim");
        match v112_pl_call_on(session, &v112_pl_deferred_create(&imv, &source)) {
            Err(refusal) if refusal.contains("already wrote to its source") => {}
            other => failures.push(format!("DEFERRED create of {imv} after a write to its source: {other:?}")),
        }
        let left_behind = Spi::get_one::<String>(&format!(
            "SELECT v FROM dblink('{session}', {}) AS t(v text)",
            sql_lit(&format!(
                "SELECT (to_regclass('{imv}') IS NOT NULL OR EXISTS (SELECT 1 FROM \
                 public.__reflex_ivm_reference WHERE name = '{imv}'))::text"
            ))
        ))
        .expect("refused create leftovers")
        .unwrap_or_default();
        if left_behind != "false" {
            failures.push(format!("the refused create of {imv} left something behind"));
        }
        let committed = Spi::get_one::<String>(&format!("SELECT dblink_exec('{session}', 'COMMIT', false)"))
            .expect("commit")
            .unwrap_or_default();
        if committed == "ERROR" {
            failures.push(format!("COMMIT of {schema} after the refused create raised"));
        }
        match v112_pl_call_on(session, &v112_pl_deferred_create(&imv, &source)) {
            Ok(_) if schema == "tb" => {
                tb_imvs.push((imv, V112_PL_DEFERRED_CREATE_QUERY.replace("{s}", &source)));
            }
            Ok(_) => {}
            Err(error) => failures.push(format!("DEFERRED create of {imv} after COMMIT: {error}")),
        }
    }
    let _ = Spi::get_one::<String>("SELECT dblink_disconnect('reflex_lock_holder')");
    check_tb(&mut failures, "after both schemas committed", &tb_imvs);
    let mut ta_imvs: Vec<(String, String)> = V112_DP_TRUNCATE_IMVS
        .iter()
        .map(|(suffix, query, _, _)| (format!("ta.sim_{suffix}"), query.replace("{s}", "ta.sim")))
        .collect();
    for created in ["ta.sim_open", "ta.sim_after_own_write"] {
        if rbc_select(&format!("to_regclass({}) IS NOT NULL", sql_lit(created))) == "true" {
            ta_imvs.push((created.to_string(), V112_PL_DEFERRED_CREATE_QUERY.replace("{s}", "ta.sim")));
        } else {
            failures.push(format!("{created} was not created"));
        }
    }
    check_tb(&mut failures, "the other schema after both committed", &ta_imvs);
    probe_db_close(DBNAME);
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

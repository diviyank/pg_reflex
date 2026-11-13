// DROP TABLE of a partition of a partitioned source. The partition flush runs at
// COMMIT, so the matrix steps are committed transactions on a `dblink` worker in
// a private database (helpers of `pg_test_v112_partition_dml.rs`).

const DSP_DEPENDENT: (&str, &str) = ("dep", "SELECT SUM(c) AS c, SUM(sv) AS sv FROM {s}_agg");

const DSP_FILLED_PARTITION: &str = "CREATE TABLE {s}_pe PARTITION OF {s} FOR VALUES IN ('e'); \
     INSERT INTO {s}_pe VALUES (201,'e',1),(202,'e',2),(203,'e',5)";

fn dsp_failures(s: &str, mode: &str, steps: &[(&str, &str)]) -> Vec<String> {
    let mut failures = v112_pd_failures(s, mode, steps);
    let (suffix, query) = DSP_DEPENDENT;
    let imv = format!("{s}_{suffix}");
    let fresh = query.replace("{s}", s);
    let mismatches = rbc_mismatch(&imv, &fresh);
    if mismatches != 0 {
        failures.push(format!(
            "{mode}: dependent {mismatches} mismatches {}",
            v112_pd_difference(&imv, &fresh)
        ));
    }
    failures
}

fn dsp_assert_matrix(dbname: &str, mode: &str, steps: &[(&str, &str)]) {
    probe_db_open(dbname);
    v112_pd_fixture("pd", mode);
    let (suffix, query) = DSP_DEPENDENT;
    v112_dp_create(&format!("pd_{suffix}"), &query.replace("{s}", "pd"), None, mode, None);
    let failures = dsp_failures("pd", mode, steps);
    probe_db_close(dbname);
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[pg_test]
fn pg_drop_partition_with_rows_immediate() {
    dsp_assert_matrix(
        "reflex_dsp_imm",
        "IMMEDIATE",
        &[
            ("create and fill a partition", DSP_FILLED_PARTITION),
            ("drop the filled partition", "DROP TABLE {s}_pe"),
            ("write through the root afterwards", "INSERT INTO {s} VALUES (9,'a',3),(10,'e',4)"),
        ],
    );
}

#[pg_test]
fn pg_drop_partition_with_rows_deferred() {
    dsp_assert_matrix(
        "reflex_dsp_def",
        "DEFERRED",
        &[
            ("create and fill a partition", DSP_FILLED_PARTITION),
            ("drop the filled partition", "DROP TABLE {s}_pe"),
            ("write through the root afterwards", "INSERT INTO {s} VALUES (9,'a',3),(10,'e',4)"),
        ],
    );
}

#[pg_test]
fn pg_drop_subpartitioned_branch() {
    dsp_assert_matrix(
        "reflex_dsp_branch",
        "IMMEDIATE",
        &[
            ("drop the sub-partitioned branch with its leaves", "DROP TABLE {s}_pc"),
            ("write through the root afterwards", "INSERT INTO {s} VALUES (11,'a',3)"),
        ],
    );
}

#[pg_test]
fn pg_detach_then_drop_unchanged() {
    dsp_assert_matrix(
        "reflex_dsp_detach",
        "IMMEDIATE",
        &[
            ("create and fill a partition", DSP_FILLED_PARTITION),
            (
                "detach and drop it in one transaction",
                "ALTER TABLE {s} DETACH PARTITION {s}_pe; DROP TABLE {s}_pe",
            ),
        ],
    );
}

/// pg_reflex's own swap drops the partitions it replaces. Those drops must not
/// queue the upstream IMV as a changed source root: the flush would race the
/// code-driven cascade. A committed fixture, and the upstream IMV's triggers off
/// (as pg_reflex's own reconcile has them): a populated leaf is otherwise rebuilt
/// as a row diff, not swapped.
#[pg_test]
fn pg_internal_swap_queues_no_flush() {
    const DBNAME: &str = "reflex_dsp_swap";
    probe_db_open(DBNAME);
    v112_pd_fixture("pd", "IMMEDIATE");
    v112_dp_create(
        "pd_upstream_dep",
        "SELECT g, SUM(c) AS c FROM pd_agg_part GROUP BY g",
        None,
        "IMMEDIATE",
        None,
    );
    let outcome = v112_dp_try(
        "DO $$ DECLARE leaf_before OID := to_regclass('pd_agg_part_pd_pa')::oid; \
         BEGIN \
           ALTER TABLE pd_agg_part DISABLE TRIGGER USER; \
           DELETE FROM public.__reflex_partition_pending; \
           PERFORM tests.crate_test_partition_swap_for_child('pd_agg_part', 'pd_pa'); \
           IF EXISTS (SELECT 1 FROM pg_class WHERE oid = leaf_before) THEN \
             RAISE EXCEPTION 'fixture: the leaf was not swapped'; \
           END IF; \
           IF EXISTS (SELECT 1 FROM public.__reflex_partition_pending \
                      WHERE source_root LIKE '%pd_agg_part') THEN \
             RAISE EXCEPTION 'the swap queued its upstream IMV for a flush'; \
           END IF; \
           ALTER TABLE pd_agg_part ENABLE TRIGGER USER; \
         END $$",
    );
    let upstream = rbc_mismatch("pd_agg_part", V112_PD_IMVS[7].1.replace("{s}", "pd").as_str());
    let dependent = rbc_mismatch(
        "pd_upstream_dep",
        "SELECT g, COUNT(*) AS c FROM pd GROUP BY g",
    );
    probe_db_close(DBNAME);
    assert_eq!(outcome, Ok(()));
    assert_eq!(upstream, 0, "upstream IMV wrong after the swap");
    assert_eq!(dependent, 0, "dependent wrong after the swap");
}

fn dsp_pending_rows_like(pattern: &str) -> i64 {
    Spi::get_one::<i64>(&format!(
        "SELECT count(*)::int8 FROM public.__reflex_partition_pending WHERE source_root LIKE '{pattern}'"
    ))
    .expect("pending count")
    .expect("pending count NULL")
}

/// A partitioned source `name` (partitions `_a`, `_b`, both filled) read by an
/// IMV `name_v`.
fn dsp_tracked_source(name: &str) {
    Spi::run(&format!(
        "CREATE TABLE {name} (k TEXT NOT NULL, v INT) PARTITION BY LIST (k); \
         CREATE TABLE {name}_a PARTITION OF {name} FOR VALUES IN ('a'); \
         CREATE TABLE {name}_b PARTITION OF {name} FOR VALUES IN ('b'); \
         INSERT INTO {name} VALUES ('a', 1), ('a', 2), ('b', 5)"
    ))
    .expect("tracked source");
    let created = Spi::get_one::<String>(&format!(
        "SELECT create_reflex_ivm('{name}_v', 'SELECT k, SUM(v) AS s FROM {name} GROUP BY k')"
    ))
    .expect("create")
    .unwrap_or_default();
    assert_eq!(created, "CREATE REFLEX INCREMENTAL VIEW", "creating {name}_v");
    Spi::run("DELETE FROM public.__reflex_partition_pending").expect("clear queue");
}

fn dsp_role_reading_the_registry(role: &str) {
    Spi::run(&format!("CREATE ROLE {role} NOSUPERUSER")).expect("role");
    Spi::run(&format!("GRANT CREATE ON SCHEMA public TO {role}")).expect("schema grant");
    Spi::run(&format!(
        "GRANT SELECT ON public.__reflex_ivm_reference, public.__reflex_source_partition_snapshot TO {role}"
    ))
    .expect("registry grants");
}

/// A role holding only INSERT on the queue cannot run the upsert; an unrelated
/// DROP TABLE of that role must neither raise nor warn.
#[pg_test]
fn pg_drop_unrelated_table_by_role_with_partial_queue_privilege() {
    dsp_role_reading_the_registry("dsp_partial_role");
    Spi::run("GRANT INSERT ON public.__reflex_partition_pending TO dsp_partial_role")
        .expect("insert-only grant");
    Spi::run("SELECT tests.gec_keep_warnings()").expect("keep warnings");
    Spi::run("SET ROLE dsp_partial_role").expect("assume role");
    Spi::run("CREATE TABLE dsp_unrelated (id INT)").expect("create as role");
    Spi::run("DROP TABLE dsp_unrelated").expect("an unrelated drop must not raise");
    Spi::run("RESET ROLE").expect("reset role");
    let warnings = Spi::get_one::<Vec<String>>("SELECT tests.gec_kept_warnings()")
        .expect("kept warnings")
        .unwrap_or_default();
    assert_eq!(warnings, Vec::<String>::new(), "an unrelated drop must stay silent");
}

/// A role that cannot run the queue upsert drops a tracked filled partition: the
/// drop succeeds, the role is told which primitive repairs it, and that remedy
/// converges.
fn dsp_assert_drop_warns_and_flush_repairs(source: &str, role: &str, queue_grant: Option<&str>) {
    dsp_tracked_source(source);
    dsp_role_reading_the_registry(role);
    if let Some(privileges) = queue_grant {
        Spi::run(&format!("GRANT {privileges} ON public.__reflex_partition_pending TO {role}"))
            .expect("queue grant");
    }
    Spi::run(&format!("ALTER TABLE {source}_a OWNER TO {role}")).expect("partition owner");
    Spi::run("DELETE FROM public.__reflex_partition_pending").expect("clear the owner change's row");
    Spi::run("SELECT tests.gec_keep_warnings()").expect("keep warnings");
    Spi::run(&format!("SET ROLE {role}")).expect("assume role");
    Spi::run(&format!("DROP TABLE {source}_a")).expect("the drop itself must succeed");
    Spi::run("RESET ROLE").expect("reset role");
    let warnings = Spi::get_one::<Vec<String>>("SELECT tests.gec_kept_warnings()")
        .expect("kept warnings")
        .unwrap_or_default();
    assert!(
        warnings
            .iter()
            .any(|w| w.contains(&format!("reflex_flush_partition_source('public.{source}')"))),
        "no warning naming the remedy: {warnings:?}"
    );
    assert_eq!(dsp_pending_rows_like(&format!("%{source}")), 0, "the role cannot have queued the root");

    let flushed = Spi::get_one::<String>(&format!(
        "SELECT reflex_flush_partition_source('public.{source}')"
    ))
    .expect("flush")
    .unwrap_or_default();
    assert!(!flushed.starts_with("ERROR"), "the remedy failed: {flushed}");
    assert_imv_correct(
        &format!("{source}_v"),
        &format!("SELECT k, SUM(v) AS s FROM {source} GROUP BY k"),
    );
}

#[pg_test]
fn pg_drop_tracked_partition_without_queue_privilege_warns() {
    dsp_assert_drop_warns_and_flush_repairs("dsp_priv", "dsp_noqueue_role", None);
}

#[pg_test]
fn pg_drop_tracked_partition_with_queue_privileges_lacking_select_warns() {
    dsp_assert_drop_warns_and_flush_repairs("dsp_priv_nosel", "dsp_nosel_role", Some("INSERT, UPDATE"));
}

#[pg_test]
fn pg_drop_tracked_partition_with_queue_privileges_lacking_insert_warns() {
    dsp_assert_drop_warns_and_flush_repairs("dsp_priv_noins", "dsp_noins_role", Some("SELECT, UPDATE"));
}

#[pg_test]
fn pg_drop_tracked_partition_with_queue_privileges_lacking_update_warns() {
    dsp_assert_drop_warns_and_flush_repairs("dsp_priv_noupd", "dsp_noupd_role", Some("SELECT, INSERT"));
}

/// A partition of a root whose name marks it pg_reflex-owned is never queued.
#[pg_test]
fn pg_drop_partition_of_pg_reflex_named_root_queues_nothing() {
    dsp_tracked_source("dsp__reflex_named");
    Spi::run("DROP TABLE dsp__reflex_named_a").expect("drop partition");
    assert_eq!(dsp_pending_rows_like("%dsp__reflex_named"), 0);
}

/// Dropping the root together with its partitions leaves no queue row behind:
/// the source branch tears the IMV down.
#[pg_test]
fn pg_drop_root_with_partitions_queues_nothing() {
    dsp_tracked_source("dsp_rootdrop");
    Spi::run("DROP TABLE dsp_rootdrop CASCADE").expect("drop root");
    assert_eq!(dsp_pending_rows_like("%dsp_rootdrop"), 0);
}

/// The source and its partitions live in a schema dropped with CASCADE; the IMV
/// lives outside it.
#[pg_test]
fn pg_drop_schema_holding_source_and_partitions_queues_nothing() {
    Spi::run(
        "CREATE SCHEMA dsp_sch; \
         CREATE TABLE dsp_sch.src (k TEXT NOT NULL, v INT) PARTITION BY LIST (k); \
         CREATE TABLE dsp_sch.src_a PARTITION OF dsp_sch.src FOR VALUES IN ('a'); \
         CREATE TABLE dsp_sch.src_b PARTITION OF dsp_sch.src FOR VALUES IN ('b'); \
         INSERT INTO dsp_sch.src VALUES ('a', 1), ('b', 5)",
    )
    .expect("schema source");
    let created = Spi::get_one::<String>(
        "SELECT create_reflex_ivm('dsp_sch_v', 'SELECT k, SUM(v) AS s FROM dsp_sch.src GROUP BY k')",
    )
    .expect("create")
    .unwrap_or_default();
    assert_eq!(created, "CREATE REFLEX INCREMENTAL VIEW");
    Spi::run("DELETE FROM public.__reflex_partition_pending").expect("clear queue");
    Spi::run("DROP SCHEMA dsp_sch CASCADE").expect("drop schema");
    assert_eq!(dsp_pending_rows_like("%dsp_sch.src"), 0);
}

/// The partitions named before their root in one statement.
#[pg_test]
fn pg_drop_partitions_then_root_in_one_statement_queues_nothing() {
    dsp_tracked_source("dsp_rootlast");
    Spi::run("DROP TABLE dsp_rootlast_a, dsp_rootlast_b, dsp_rootlast").expect("drop all");
    assert_eq!(dsp_pending_rows_like("%dsp_rootlast"), 0);
}

/// A disabled IMV reads nothing: dropping a partition of its source queues no
/// root for it.
#[pg_test]
fn pg_drop_partition_read_only_by_disabled_imv_queues_nothing() {
    dsp_tracked_source("dsp_off");
    Spi::run("UPDATE public.__reflex_ivm_reference SET enabled = FALSE WHERE name = 'dsp_off_v'")
        .expect("disable");
    Spi::run("DROP TABLE dsp_off_a").expect("drop partition");
    assert_eq!(dsp_pending_rows_like("%dsp_off"), 0);
}

/// Control for the three guards above: the same shape with an enabled IMV does
/// queue the root.
#[pg_test]
fn pg_drop_partition_of_enabled_imv_source_queues_root() {
    dsp_tracked_source("dsp_on");
    Spi::run("DROP TABLE dsp_on_a").expect("drop partition");
    assert_eq!(dsp_pending_rows_like("%dsp_on"), 1);
}

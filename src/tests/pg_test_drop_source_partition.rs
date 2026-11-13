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

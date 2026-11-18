// A partitioned source's writes are counted over its leaves as they are now. A
// leaf written earlier in the transaction and then detached or dropped takes its
// writes out of that count while the rows of the deltas already applied keep
// them, so later writes can bring the count back to the applied rows while the
// source holds unapplied ones. A statement writing both sources then applies
// the joint term twice unless the changed partition set makes the count
// unknown. Each test checks the IMMEDIATE IMV inside the transaction and after
// COMMIT.

const TSP_SQL: &str = "SELECT tsp_d.label, COUNT(*) AS c, SUM(tsp_s.v) AS sv \
     FROM tsp_s JOIN tsp_d ON tsp_d.g = tsp_s.g GROUP BY tsp_d.label";

/// The other source's trigger fires first: it reads tsp_s's writes before tsp_s's
/// own delta is applied.
const TSP_CTE_S_FIRST: &str = "WITH s AS (INSERT INTO tsp_s VALUES (30, 'e', 7), (31, 'e', 7), \
     (32, 'e', 7), (33, 'e', 7) RETURNING 1) INSERT INTO tsp_d VALUES ('e', 'ee')";

/// tsp_s has the leaves tsp_s_a, tsp_s_b and tsp_s_e, and the branch tsp_s_c
/// holding the leaf tsp_s_c1.
fn tsp_open(dbname: &str) {
    probe_db_open(dbname);
    for ddl in [
        "CREATE TABLE tsp_s (id INT NOT NULL, g TEXT NOT NULL, v INT) PARTITION BY LIST (g)",
        "CREATE TABLE tsp_s_a PARTITION OF tsp_s FOR VALUES IN ('a')",
        "CREATE TABLE tsp_s_b PARTITION OF tsp_s FOR VALUES IN ('b')",
        "CREATE TABLE tsp_s_c PARTITION OF tsp_s FOR VALUES IN ('c') PARTITION BY RANGE (id)",
        "CREATE TABLE tsp_s_c1 PARTITION OF tsp_s_c FOR VALUES FROM (MINVALUE) TO (MAXVALUE)",
        "CREATE TABLE tsp_s_e PARTITION OF tsp_s FOR VALUES IN ('e')",
        "CREATE TABLE tsp_d (g TEXT PRIMARY KEY, label TEXT)",
        "INSERT INTO tsp_s SELECT g, 'a', g FROM generate_series(1, 5) g",
        "INSERT INTO tsp_d VALUES ('a', 'aa'), ('b', 'bb'), ('c', 'cc')",
    ] {
        worker_exec(ddl);
    }
    ts_create("tsp_v", TSP_SQL, None, "IMMEDIATE");
}

/// Writes then empties the partition holding `g`, runs `partition_ddl`, then a
/// statement writing both sources.
fn tsp_assert_correct_after(dbname: &str, g: &str, partition_ddl: &[&str]) {
    tsp_open(dbname);
    worker_exec("BEGIN");
    worker_exec(&format!("INSERT INTO tsp_s VALUES (20, '{g}', 1), (21, '{g}', 1)"));
    worker_exec(&format!("DELETE FROM tsp_s WHERE g = '{g}'"));
    for ddl in partition_ddl {
        worker_exec(ddl);
    }
    worker_exec(TSP_CTE_S_FIRST);
    let inside = rbc_mismatch("tsp_v", TSP_SQL);
    worker_exec("COMMIT");
    let after = rbc_mismatch("tsp_v", TSP_SQL);
    probe_db_close(dbname);
    assert_eq!(
        (inside, after),
        (0, 0),
        "tsp_v differs from its query (inside, after COMMIT) after {partition_ddl:?}"
    );
}

#[pg_test]
fn pg_two_source_partition_set_detached_written_leaf() {
    tsp_assert_correct_after(
        "reflex_tsp_detach",
        "b",
        &["ALTER TABLE tsp_s DETACH PARTITION tsp_s_b"],
    );
}

#[pg_test]
fn pg_two_source_partition_set_dropped_written_leaf() {
    tsp_assert_correct_after("reflex_tsp_drop", "b", &["DROP TABLE tsp_s_b"]);
}

#[pg_test]
fn pg_two_source_partition_set_detached_branch_of_written_leaf() {
    tsp_assert_correct_after(
        "reflex_tsp_detach_branch",
        "c",
        &["ALTER TABLE tsp_s DETACH PARTITION tsp_s_c"],
    );
}

#[pg_test]
fn pg_two_source_partition_set_dropped_leaf_of_branch() {
    tsp_assert_correct_after("reflex_tsp_drop_sub", "c", &["DROP TABLE tsp_s_c1"]);
}

/// A leaf written and emptied, detached, and a table written and emptied in this
/// transaction attached in its place: the count is back to the applied rows.
#[pg_test]
fn pg_two_source_partition_set_written_leaf_swapped_for_written_table() {
    tsp_assert_correct_after(
        "reflex_tsp_swap",
        "b",
        &[
            "ALTER TABLE tsp_s DETACH PARTITION tsp_s_b",
            "CREATE TABLE tsp_s_x (id INT NOT NULL, g TEXT NOT NULL, v INT)",
            "INSERT INTO tsp_s_x VALUES (40, 'x', 1), (41, 'x', 1)",
            "DELETE FROM tsp_s_x",
            "ALTER TABLE tsp_s ATTACH PARTITION tsp_s_x FOR VALUES IN ('x')",
        ],
    );
}

/// An attached table written and emptied in this transaction adds its writes to
/// the count.
#[pg_test]
fn pg_two_source_partition_set_attached_written_table() {
    tsp_assert_correct_after(
        "reflex_tsp_attach",
        "b",
        &[
            "CREATE TABLE tsp_s_x (id INT NOT NULL, g TEXT NOT NULL, v INT)",
            "INSERT INTO tsp_s_x VALUES (40, 'x', 1), (41, 'x', 1)",
            "DELETE FROM tsp_s_x",
            "ALTER TABLE tsp_s ATTACH PARTITION tsp_s_x FOR VALUES IN ('x')",
        ],
    );
}

/// A table attached, written through the source, emptied and removed again: the
/// leaves are back to those first read, but the rows counted as applied while
/// the table was attached are no longer in the sum.
fn tsp_assert_correct_after_attached_table_removed(dbname: &str, remove: &str) {
    tsp_open(dbname);
    worker_exec("CREATE TABLE tsp_t (LIKE tsp_s INCLUDING ALL)");
    worker_exec("BEGIN");
    worker_exec("INSERT INTO tsp_s VALUES (40, 'a', 1)");
    worker_exec("ALTER TABLE tsp_s ATTACH PARTITION tsp_t FOR VALUES IN ('z')");
    worker_exec("INSERT INTO tsp_s VALUES (41, 'z', 1), (42, 'z', 1), (43, 'z', 1), (44, 'z', 1)");
    worker_exec("DELETE FROM tsp_s WHERE g = 'z'");
    worker_exec(remove);
    worker_exec(
        "WITH s AS (INSERT INTO tsp_s SELECT 100 + g, 'e', 7 FROM generate_series(1, 8) g \
         RETURNING 1) INSERT INTO tsp_d VALUES ('e', 'ee')",
    );
    let inside = rbc_mismatch("tsp_v", TSP_SQL);
    worker_exec("COMMIT");
    let after = rbc_mismatch("tsp_v", TSP_SQL);
    probe_db_close(dbname);
    assert_eq!(
        (inside, after),
        (0, 0),
        "tsp_v differs from its query (inside, after COMMIT) after <{remove}>"
    );
}

#[pg_test]
fn pg_two_source_partition_set_attached_written_table_detached() {
    tsp_assert_correct_after_attached_table_removed(
        "reflex_tsp_att_det",
        "ALTER TABLE tsp_s DETACH PARTITION tsp_t",
    );
}

#[pg_test]
fn pg_two_source_partition_set_attached_written_table_dropped() {
    tsp_assert_correct_after_attached_table_removed("reflex_tsp_att_drop", "DROP TABLE tsp_t");
}

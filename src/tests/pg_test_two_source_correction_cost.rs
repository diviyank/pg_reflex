// The correction −(ΔX ⋈ ΔY) of a statement writing both sources of an IMMEDIATE
// join IMV merges up to four signed pieces into the intermediate table. Its
// statistics are refreshed once, before the statements that read them, not after
// every piece: ANALYZE of the intermediate table is the dominant cost of a
// corrected statement and grows with the IMV.

const JO_DDL: [&str; 4] = [
    "CREATE TABLE jo_x (k INT PRIMARY KEY, g INT NOT NULL, a INT NOT NULL)",
    "CREATE TABLE jo_y (k INT PRIMARY KEY, b INT NOT NULL)",
    "INSERT INTO jo_x SELECT i, i % 40, i % 7 FROM generate_series(1, 400) i",
    "INSERT INTO jo_y SELECT i, i % 11 FROM generate_series(1, 400) i",
];
const JO_SUM: &str = "SELECT jo_x.g, COUNT(*) AS n, SUM(jo_y.b) AS s, SUM(jo_x.a) AS sa \
     FROM jo_x JOIN jo_y ON jo_y.k = jo_x.k GROUP BY jo_x.g";

fn jo_analyze_count(table: &str) -> i64 {
    worker_scalar_i64(&format!(
        "SELECT pg_stat_get_analyze_count(to_regclass('{table}'))"
    ))
}

/// Each statement writes both sources: the stasher's delta and the corrected
/// delta each analyze the intermediate and affected tables once, and the
/// correction once more, whatever the number of its pieces. The IMV stays exact.
#[pg_test]
fn pg_two_source_correction_analyzes_once_per_correction() {
    const DBNAME: &str = "reflex_tsc_cost";
    probe_db_open(DBNAME);
    for statement in JO_DDL {
        worker_exec(statement);
    }
    ts_create("jo_sum", JO_SUM, None, "IMMEDIATE");
    let intermediate = rbc_select("public.reflex_intermediate_table_name('jo_sum')");
    let affected = "__reflex_affected_jo_sum";
    assert!(
        jo_analyze_count(&intermediate) >= 0 && jo_analyze_count(affected) >= 0,
        "the intermediate and affected tables of jo_sum must exist"
    );
    for statement in [
        "WITH x AS (UPDATE jo_x SET a = a + 1 WHERE k = 11 RETURNING 1) \
         UPDATE jo_y SET b = b + 1 WHERE k = 11",
        "WITH x AS (INSERT INTO jo_x VALUES (1001, 3, 5) RETURNING 1) \
         INSERT INTO jo_y VALUES (1001, 9)",
        "WITH x AS (DELETE FROM jo_x WHERE k = 21 RETURNING 1) DELETE FROM jo_y WHERE k = 21",
        "WITH x AS (UPDATE jo_x SET g = g + 1 WHERE k = 31 RETURNING 1) \
         UPDATE jo_y SET b = b + 2 WHERE k = 31",
    ] {
        let intermediate_before = jo_analyze_count(&intermediate);
        let affected_before = jo_analyze_count(affected);
        worker_exec("BEGIN");
        worker_exec(statement);
        let corrected = tsx_corrected("jo_sum");
        worker_exec("COMMIT");
        assert_eq!(corrected, 1, "<{statement}> must be corrected, not recomputed");
        assert_eq!(rbc_mismatch("jo_sum", JO_SUM), 0, "jo_sum differs after <{statement}>");
        assert!(!rbc_known_stale("jo_sum"), "jo_sum flagged stale after <{statement}>");
        assert_eq!(
            jo_analyze_count(&intermediate) - intermediate_before,
            3,
            "<{statement}>: the intermediate table is analyzed once per delta and once per correction"
        );
        assert_eq!(
            jo_analyze_count(affected) - affected_before,
            3,
            "<{statement}>: the affected table is analyzed once per delta and once per correction"
        );
    }
    probe_db_close(DBNAME);
}

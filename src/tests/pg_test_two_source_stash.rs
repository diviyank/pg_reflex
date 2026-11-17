// The rows a delta keeps for the other source's delta (the stash) must come
// back as the values written, whatever the session's output settings are and
// however they change in between; a source whose rows cannot be kept exactly
// recomputes. A delta the trigger would skip is still applied when another
// source's delta read its rows half written. An IMV created after its sources
// were written in the same transaction owes those writes nothing.

const TSS_KEYED: &str = "SELECT xk.g, COUNT(*) AS n, SUM(xk.a) AS sa, SUM(yk.b) AS sb \
     FROM xk JOIN yk ON yk.k = xk.k GROUP BY xk.g";

/// `xk ⋈ yk` grouped by `xk.g` of `key_type`, with one row per seeded key, in a
/// worker session running `session_settings`. `extra_column` is appended to
/// xk's columns.
fn tss_keyed_open(
    dbname: &str,
    key_type: &str,
    extra_column: &str,
    seeded_keys: &[&str],
    session_settings: &[&str],
) {
    probe_db_open(dbname);
    worker_exec(&format!(
        "CREATE TABLE xk (k INT PRIMARY KEY, g {key_type} NOT NULL, a INT NOT NULL {extra_column})"
    ));
    worker_exec("CREATE TABLE yk (k INT PRIMARY KEY, b INT NOT NULL)");
    for (i, key) in seeded_keys.iter().enumerate() {
        worker_exec(&format!("INSERT INTO xk (k, g, a) VALUES ({i}, {key}, 1)"));
        worker_exec(&format!("INSERT INTO yk VALUES ({i}, 10)"));
    }
    ts_create("tss_keyed", TSS_KEYED, None, "IMMEDIATE");
    for setting in session_settings {
        worker_exec(setting);
    }
}

/// One transaction inserting, updating and deleting the row of `key` in both
/// sources, a statement each: the IMV is exact after each and after COMMIT.
/// Returns the deltas corrected (not recomputed) after the first statement and
/// after the last.
fn tss_keyed_joint_writes(key: &str) -> (i64, i64) {
    worker_exec("BEGIN");
    let mut corrected = Vec::new();
    for statement in [
        format!(
            "WITH y AS (INSERT INTO yk VALUES (600, 7) RETURNING 1) \
             INSERT INTO xk (k, g, a) VALUES (600, {key}, 1)"
        ),
        "WITH y AS (UPDATE yk SET b = b + 1 WHERE k = 600 RETURNING 1) \
         UPDATE xk SET a = a + 1 WHERE k = 600"
            .to_string(),
        "WITH y AS (DELETE FROM yk WHERE k = 600 RETURNING 1) DELETE FROM xk WHERE k = 600"
            .to_string(),
    ] {
        worker_exec(&statement);
        corrected.push(tsx_corrected("tss_keyed"));
        tsx_assert_correct(
            &[("tss_keyed", TSS_KEYED)],
            &format!("after <{statement}> (corrected so far: {corrected:?})"),
        );
    }
    worker_exec("COMMIT");
    tsx_assert_correct(&[("tss_keyed", TSS_KEYED)], "after COMMIT");
    (corrected[0], corrected[2])
}

/// A float8 key printed with `extra_float_digits = 0` loses its last digits:
/// 0.1 + 0.2 prints as 0.3, another group.
#[pg_test]
fn pg_two_source_stash_float8_key_under_extra_float_digits_0() {
    const DBNAME: &str = "reflex_tss_float";
    const KEY: &str = "(0.1::float8 + 0.2::float8)";
    tss_keyed_open(
        DBNAME,
        "FLOAT8",
        "",
        &["0.3::float8", KEY],
        &["SET extra_float_digits = 0"],
    );
    let (first, _) = tss_keyed_joint_writes(KEY);
    assert!(first > 0, "a float8 key is kept exactly: the delta is corrected");
    probe_db_close(DBNAME);
}

/// A timestamptz key printed as 'SQL, DMY' in Asia/Kolkata ends in 'IST', read
/// back as Israel's time zone: another instant.
#[pg_test]
fn pg_two_source_stash_timestamptz_key_under_sql_dmy_kolkata() {
    const DBNAME: &str = "reflex_tss_tstz";
    const KEY: &str = "'2024-03-05 10:11:12.123456+05:30'::timestamptz";
    tss_keyed_open(
        DBNAME,
        "TIMESTAMPTZ",
        "",
        &["'2024-03-05 10:11:12.123456+02'::timestamptz", KEY],
        &["SET DateStyle = 'SQL, DMY'", "SET TimeZone = 'Asia/Kolkata'"],
    );
    let (first, _) = tss_keyed_joint_writes(KEY);
    assert!(first > 0, "a timestamptz key is kept exactly: the delta is corrected");
    probe_db_close(DBNAME);
}

/// An interval key printed as sql_standard ('-1 2:00:00' for -1 day -2 hours)
/// reads as -1 day +2 hours once a user trigger, fired between xk's delta and
/// yk's, has set IntervalStyle back to postgres.
#[pg_test]
fn pg_two_source_stash_interval_key_with_interval_style_changed_before_the_read() {
    const DBNAME: &str = "reflex_tss_interval";
    const KEY: &str = "'-1 days -2 hours'::interval";
    tss_keyed_open(
        DBNAME,
        "INTERVAL",
        "",
        &["'-1 days +2 hours'::interval", KEY],
        &["SET IntervalStyle = sql_standard"],
    );
    worker_exec(
        "CREATE FUNCTION tss_restyle() RETURNS trigger LANGUAGE plpgsql AS \
         $$ BEGIN PERFORM set_config('IntervalStyle', 'postgres', true); RETURN NULL; END $$",
    );
    worker_exec(
        "CREATE TRIGGER zz_tss_restyle AFTER INSERT OR UPDATE OR DELETE ON xk \
         FOR EACH STATEMENT EXECUTE FUNCTION tss_restyle()",
    );
    let (first, _) = tss_keyed_joint_writes(KEY);
    assert!(first > 0, "an interval key is kept exactly: the delta is corrected");
    probe_db_close(DBNAME);
}

/// A bytea key under `bytea_output = escape`, changed to hex before the read.
#[pg_test]
fn pg_two_source_stash_bytea_key_under_bytea_output_escape() {
    const DBNAME: &str = "reflex_tss_bytea";
    const KEY: &str = "'\\x00ff5c2722'::bytea";
    tss_keyed_open(
        DBNAME,
        "BYTEA",
        "",
        &["'\\x00ff5c27'::bytea", KEY],
        &["SET bytea_output = escape"],
    );
    worker_exec(
        "CREATE FUNCTION tss_hex() RETURNS trigger LANGUAGE plpgsql AS \
         $$ BEGIN PERFORM set_config('bytea_output', 'hex', true); RETURN NULL; END $$",
    );
    worker_exec(
        "CREATE TRIGGER zz_tss_hex AFTER INSERT OR UPDATE OR DELETE ON xk \
         FOR EACH STATEMENT EXECUTE FUNCTION tss_hex()",
    );
    let (first, _) = tss_keyed_joint_writes(KEY);
    assert!(first > 0, "a bytea key is kept exactly: the delta is corrected");
    probe_db_close(DBNAME);
}

/// A source holding a money column, whose text depends on `lc_monetary`, keeps
/// no rows: every delta owed a correction recomputes.
#[pg_test]
fn pg_two_source_stash_money_column_recomputes() {
    const DBNAME: &str = "reflex_tss_money";
    tss_keyed_open(DBNAME, "INT", ", price MONEY DEFAULT 12.34", &["1", "2"], &[]);
    let (_, last) = tss_keyed_joint_writes("2");
    assert_eq!(
        last, 0,
        "rows holding a money column must not be kept as text: no delta may be corrected"
    );
    probe_db_close(DBNAME);
}

const TSS_SWAP_DDL: [&str; 4] = [
    "CREATE TABLE xa (k INT PRIMARY KEY, g INT NOT NULL, a INT NOT NULL)",
    "CREATE TABLE sb (id INT PRIMARY KEY, k INT NOT NULL, b INT NOT NULL)",
    "INSERT INTO xa SELECT g, g, g * 10 FROM generate_series(1, 20) g",
    "INSERT INTO sb SELECT g, g, g * 100 FROM generate_series(1, 60) g",
];
const TSS_SWAP_SUM: &str = "SELECT xa.g, COUNT(*) AS n, SUM(xa.a) AS sa, SUM(sb.b) AS sb \
     FROM xa JOIN sb ON sb.k = xa.k GROUP BY xa.g";
/// Rows 1 and 2 of sb exchange their (k, b): the rows before and after hold
/// the same values, which the trigger skips as an unchanged statement.
const TSS_SWAP: &str = "UPDATE sb SET k = CASE id WHEN 1 THEN 2 ELSE 1 END, \
     b = CASE id WHEN 1 THEN 200 ELSE 100 END WHERE id IN (1, 2)";

fn tss_swap_open(dbname: &str, before_row_body: &str) {
    probe_db_open(dbname);
    for statement in TSS_SWAP_DDL {
        worker_exec(statement);
    }
    worker_exec(&format!(
        "CREATE FUNCTION tss_bump() RETURNS trigger LANGUAGE plpgsql AS \
         $$ BEGIN {before_row_body} RETURN NEW; END $$"
    ));
    worker_exec(
        "CREATE TRIGGER tss_bump BEFORE UPDATE ON sb FOR EACH ROW EXECUTE FUNCTION tss_bump()",
    );
    ts_create("tss_swap_sum", TSS_SWAP_SUM, None, "IMMEDIATE");
}

fn tss_swap_stays_exact() {
    worker_exec("BEGIN");
    worker_exec(TSS_SWAP);
    tsx_assert_correct(&[("tss_swap_sum", TSS_SWAP_SUM)], "after the exchanging UPDATE");
    worker_exec("COMMIT");
    tsx_assert_correct(&[("tss_swap_sum", TSS_SWAP_SUM)], "after COMMIT");
}

/// A BEFORE ROW trigger on sb updates xa: the delta of xa fired for row 2 joins
/// sb while row 1 holds its new k and row 2 its old one. sb's own delta, whose
/// rows exchange their values, must not be skipped as unchanged.
#[pg_test]
fn pg_two_source_stash_skipped_delta_after_a_half_written_read() {
    const DBNAME: &str = "reflex_tss_swap";
    tss_swap_open(DBNAME, "UPDATE xa SET a = a + 1 WHERE k = NEW.k;");
    tss_swap_stays_exact();
    probe_db_close(DBNAME);
}

/// As above, with the trigger also updating another row of sb: that nested
/// statement's delta finds xa's stash unusable and recomputes the IMV while
/// row 2 still holds its old values, which drops the stash. sb's outer delta
/// must still not be skipped.
#[pg_test]
fn pg_two_source_stash_skipped_delta_after_a_recompute_of_half_written_rows() {
    const DBNAME: &str = "reflex_tss_swap_recompute";
    tss_swap_open(
        DBNAME,
        "IF pg_trigger_depth() > 1 THEN RETURN NEW; END IF; \
         UPDATE xa SET a = a + 1 WHERE k = NEW.k; \
         UPDATE sb SET b = b + 1 WHERE id = 50;",
    );
    tss_swap_stays_exact();
    probe_db_close(DBNAME);
}

/// Sources written, then the IMV created, in one transaction: the IMV holds
/// those rows, so a later statement writing both sources is corrected, not
/// recomputed.
#[pg_test]
fn pg_two_source_stash_imv_created_after_its_sources_were_written() {
    const DBNAME: &str = "reflex_tss_created";
    probe_db_open(DBNAME);
    worker_exec("CREATE TABLE xk (k INT PRIMARY KEY, g INT NOT NULL, a INT NOT NULL)");
    worker_exec("CREATE TABLE yk (k INT PRIMARY KEY, b INT NOT NULL)");
    worker_exec("BEGIN");
    worker_exec("INSERT INTO xk SELECT g, g % 5, g FROM generate_series(1, 50) g");
    worker_exec("INSERT INTO yk SELECT g, g FROM generate_series(1, 40) g");
    ts_create("tss_keyed", TSS_KEYED, None, "IMMEDIATE");
    worker_exec("UPDATE yk SET b = b + 1 WHERE k = 3");
    tsx_assert_correct(&[("tss_keyed", TSS_KEYED)], "after a write of yk alone");
    worker_exec(
        "WITH y AS (INSERT INTO yk VALUES (600, 7) RETURNING 1) INSERT INTO xk VALUES (600, 1, 1)",
    );
    tsx_assert_correct(&[("tss_keyed", TSS_KEYED)], "after a statement writing both sources");
    let corrected = tsx_corrected("tss_keyed");
    worker_exec("COMMIT");
    tsx_assert_correct(&[("tss_keyed", TSS_KEYED)], "after COMMIT");
    assert!(
        corrected > 0,
        "the writes made before the IMV was created are not owed a delta: \
         the statement writing both sources is corrected, not recomputed"
    );
    probe_db_close(DBNAME);
}

/// The library can run before ALTER EXTENSION UPDATE has re-rendered the
/// trigger bodies and created the functions that keep rows exactly. Such a body
/// calls `reflex_build_delta_sql`, which never resolves a stash: its deltas
/// keep no rows, call none of those functions, and recompute.
#[pg_test]
fn pg_two_source_stash_trigger_body_older_than_the_library_keeps_no_rows() {
    const DBNAME: &str = "reflex_tss_older_body";
    tss_keyed_open(DBNAME, "INT", "", &["1", "2"], &[]);
    worker_exec(
        "DO $$ DECLARE body TEXT; BEGIN \
           FOR body IN SELECT pg_get_functiondef(p.oid) FROM pg_proc p \
                       WHERE p.prosrc LIKE '%reflex_build_delta_sql_resolving(%' LOOP \
             EXECUTE replace(body, 'reflex_build_delta_sql_resolving(', 'reflex_build_delta_sql('); \
           END LOOP; END $$",
    );
    assert_eq!(
        worker_scalar_i64(
            "SELECT count(*)::int8 FROM pg_proc WHERE prosrc LIKE '%reflex_build_delta_sql_resolving(%'"
        ),
        0,
        "every trigger body calls the older entry point"
    );
    worker_exec("ALTER EXTENSION pg_reflex DROP FUNCTION public.reflex_joint_stash_text(anyarray)");
    worker_exec(
        "ALTER EXTENSION pg_reflex DROP FUNCTION public.reflex_joint_stashed_rows(text, anyarray)",
    );
    worker_exec("DROP FUNCTION public.reflex_joint_stash_text(anyarray)");
    worker_exec("DROP FUNCTION public.reflex_joint_stashed_rows(text, anyarray)");
    let (_, last) = tss_keyed_joint_writes("2");
    assert_eq!(last, 0, "a body that resolves no stash corrects no delta");
    probe_db_close(DBNAME);
}

/// A source with a column named `t`, the alias the stash once gave its
/// transition table: the rows kept are the rows, not that column.
#[pg_test]
fn pg_two_source_stash_source_with_a_column_named_t() {
    const DBNAME: &str = "reflex_tss_column_t";
    tss_keyed_open(
        DBNAME,
        "INT",
        ", t TIMETZ DEFAULT '10:00:00+01', c INT DEFAULT 3, n INT DEFAULT 4",
        &["1", "2"],
        &[],
    );
    let (first, _) = tss_keyed_joint_writes("2");
    assert!(first > 0, "the rows of a source with a column named t are kept: corrected");
    probe_db_close(DBNAME);
}

// A partition swap committed while another session holds a lock the COMMIT-time
// flush needs. A lock timeout or a deadlock is transient: it is recorded, the
// root stays queued and its IMVs flagged stale, but it never counts toward the
// suspension of the root. One COMMIT flushes a root once, however many partition
// DDL commands queued it. A worker session commits each step in a private
// database (helpers of `pg_test_partition_attach_locks.rs`,
// `pg_test_rebuild_commit.rs` and `pg_test_v112_deferred_partition.rs`); a
// holder session keeps the obstacle in an open transaction.

const V112_TS_LEAVES: [&str; 3] = ["a", "b", "c"];
const V112_TS_ROOT: &str = "public.sim";
const V112_TS_HELD_IMV: &str = "sim_total";
const V112_TS_HOLDER: &str = "reflex_ts_holder";

/// The source `sim`, LIST-partitioned on `g` with one leaf per value of
/// `V112_TS_LEAVES`, read by every IMV of `V112_DP_TRUNCATE_IMVS` (IMMEDIATE).
/// Each swap replaces a leaf by a copy whose values moved, so every IMV changes.
struct V112TsSource {
    generation: std::collections::HashMap<&'static str, usize>,
}

impl V112TsSource {
    fn open(dbname: &str) -> Self {
        probe_db_open(dbname);
        worker_exec("CREATE TABLE sim(id INT NOT NULL, g TEXT NOT NULL, v INT) PARTITION BY LIST (g)");
        for g in V112_TS_LEAVES {
            worker_exec(&format!("CREATE TABLE sim_{g}0 PARTITION OF sim FOR VALUES IN ('{g}')"));
        }
        worker_exec("INSERT INTO sim SELECT i, (ARRAY['a','b','c'])[1 + i % 3], i FROM generate_series(1, 30) i");
        for (suffix, query, key, partition_by) in V112_DP_TRUNCATE_IMVS {
            v112_dp_create(&format!("sim_{suffix}"), &query.replace("{s}", "sim"), *key, "IMMEDIATE", *partition_by);
        }
        worker_exec(
            "CREATE TABLE public.v112_ts_flush_attempts(at timestamptz); \
             CREATE FUNCTION public.v112_ts_count_flush_attempt() RETURNS trigger LANGUAGE plpgsql AS $f$ \
             BEGIN INSERT INTO public.v112_ts_flush_attempts VALUES (NEW.last_attempt_at); RETURN NULL; END $f$; \
             CREATE TRIGGER v112_ts_count_flush_attempt AFTER UPDATE OF last_attempt_at \
               ON public.__reflex_partition_pending FOR EACH ROW \
               EXECUTE FUNCTION public.v112_ts_count_flush_attempt()",
        );
        V112TsSource { generation: V112_TS_LEAVES.iter().map(|g| (*g, 0)).collect() }
    }

    /// One statement (one transaction) that DETACHes and ATTACHes each of `leaves`:
    /// two partition DDL commands per leaf.
    fn swap(&mut self, leaves: &[&'static str]) -> String {
        let mut body = String::new();
        for g in leaves {
            let generation = self.generation.get_mut(g).expect("known leaf");
            let old = format!("sim_{g}{generation}");
            *generation += 1;
            let new = format!("sim_{g}{generation}");
            body.push_str(&format!(
                "CREATE TABLE {new} (LIKE sim INCLUDING ALL); \
                 INSERT INTO {new} SELECT id, g, v + 1 FROM {old}; \
                 ALTER TABLE sim DETACH PARTITION {old}; \
                 ALTER TABLE sim ATTACH PARTITION {new} FOR VALUES IN ('{g}'); \
                 DROP TABLE {old}; "
            ));
        }
        format!("DO $swap$ BEGIN {body} END $swap$")
    }
}

fn v112_ts_holder_exec(sql: &str) {
    Spi::get_one::<String>(&format!("SELECT dblink_exec('{V112_TS_HOLDER}', {})", sql_lit(sql)))
        .unwrap_or_else(|e| panic!("holder failed for <{sql}>: {e}"));
}

/// Opens the holder session and locks the target of `V112_TS_HELD_IMV` against
/// writes in an open transaction: the flush of `sim` reconciles it, the oracle
/// still reads it.
fn v112_ts_hold(dbname: &str) {
    Spi::get_one::<String>(&format!(
        "SELECT dblink_connect('{V112_TS_HOLDER}', {})",
        sql_lit(&conninfo_for(dbname))
    ))
    .expect("holder connect");
    v112_ts_holder_exec("BEGIN");
    v112_ts_holder_exec("SET LOCAL deadlock_timeout = '60s'");
    v112_ts_holder_exec(&format!("LOCK TABLE {V112_TS_HELD_IMV} IN SHARE MODE"));
}

fn v112_ts_release() {
    v112_ts_holder_exec("ROLLBACK");
    let _ = Spi::get_one::<String>(&format!("SELECT dblink_disconnect('{V112_TS_HOLDER}')"));
}

/// The pending row of the root: (failures, last_error), `None` when drained.
fn v112_ts_pending() -> Option<(i32, String)> {
    let row = rbc_select(&format!(
        "COALESCE((SELECT failures || '|' || COALESCE(last_error, '') \
                   FROM public.__reflex_partition_pending WHERE source_root = '{V112_TS_ROOT}'), 'none')"
    ));
    if row == "none" {
        return None;
    }
    let (failures, last_error) = row.split_once('|').expect("failures|last_error");
    Some((failures.parse().expect("failures"), last_error.to_string()))
}

fn v112_ts_flush_attempts() -> i64 {
    worker_scalar_i64("SELECT count(*)::int8 FROM public.v112_ts_flush_attempts")
}

/// Runs `step` as one committed transaction of the worker; the number of
/// flushes of the root it started.
fn v112_ts_commit(step: &str) -> Result<i64, String> {
    let before = v112_ts_flush_attempts();
    v112_dp_try(step)?;
    Ok(v112_ts_flush_attempts() - before)
}

/// The IMVs that differ from their query, and among them those not flagged stale.
fn v112_ts_wrong_and_silently_stale() -> (Vec<String>, Vec<String>) {
    let mut wrong = Vec::new();
    let mut silent = Vec::new();
    for (suffix, query, _, _) in V112_DP_TRUNCATE_IMVS {
        let imv = format!("sim_{suffix}");
        let mismatches = rbc_mismatch(&imv, &query.replace("{s}", "sim"));
        if mismatches != 0 {
            wrong.push(format!("{imv} {mismatches} mismatches"));
            if !rbc_known_stale(&imv) {
                silent.push(format!("{imv} {mismatches} mismatches, not flagged stale"));
            }
        }
    }
    (wrong, silent)
}

fn v112_ts_stale_imvs() -> String {
    rbc_select(
        "SELECT COALESCE(string_agg(name, ',' ORDER BY name), '') FROM public.__reflex_ivm_reference \
         WHERE known_stale",
    )
}

/// After the obstacle is gone: a write, then the next flush of the root brings
/// every IMV back to its query and drains the root.
fn v112_ts_recovery_failures() -> Vec<String> {
    let mut failures = Vec::new();
    if let Err(error) = v112_dp_try("INSERT INTO sim VALUES (101,'a',4),(102,'c',5); DELETE FROM sim WHERE id = 3") {
        failures.push(format!("write after release: {error}"));
    }
    let flushed = rbc_select(&format!("reflex_flush_partition_source('{V112_TS_ROOT}')"));
    if flushed.starts_with("ERROR") {
        failures.push(format!("flush after release: {flushed}"));
    }
    if let Some(pending) = v112_ts_pending() {
        failures.push(format!("root still pending after the flush that followed the release: {pending:?}"));
    }
    let (wrong, _) = v112_ts_wrong_and_silently_stale();
    failures.extend(wrong.into_iter().map(|w| format!("after recovery: {w}")));
    failures
}

/// What a COMMIT met by a transient obstacle must leave: a commit that
/// succeeded, one flush of the root, the error recorded but not counted, and no
/// IMV that is wrong without being flagged stale.
fn v112_ts_transient_commit_failures(
    label: &str,
    committed: Result<i64, String>,
    expected_error: &str,
) -> Vec<String> {
    let mut failures = Vec::new();
    match committed {
        Err(error) => failures.push(format!("{label}: the COMMIT raised {error}")),
        Ok(1) => {}
        Ok(flushes) => failures.push(format!("{label}: the COMMIT flushed the root {flushes} times")),
    }
    match v112_ts_pending() {
        None => failures.push(format!("{label}: the root left the queue although its flush failed")),
        Some((failures_counted, last_error)) => {
            if failures_counted != 0 {
                failures.push(format!(
                    "{label}: failures = {failures_counted} after a transient error ({last_error})"
                ));
            }
            if !last_error.contains(expected_error) {
                failures.push(format!("{label}: last_error = {last_error:?}, expected {expected_error:?}"));
            }
        }
    }
    let (wrong, silent) = v112_ts_wrong_and_silently_stale();
    if wrong.is_empty() {
        failures.push(format!("{label}: fixture: no IMV was left behind by the failed flush"));
    }
    failures.extend(silent.into_iter().map(|s| format!("{label}: {s}")));
    failures
}

#[pg_test]
fn v112_ts_lock_timeout_at_the_commit_of_a_three_leaf_swap_is_retried_not_counted() {
    let dbname = "reflex_ts_timeout";
    let mut source = V112TsSource::open(dbname);
    v112_ts_hold(dbname);
    worker_exec("SET lock_timeout = '300ms'");
    let committed = v112_ts_commit(&source.swap(&V112_TS_LEAVES));
    let mut failures = v112_ts_transient_commit_failures("three-leaf swap", committed, "lock timeout");
    worker_exec("RESET lock_timeout");
    v112_ts_release();
    failures.extend(v112_ts_recovery_failures());
    let stale_after_recovery = v112_ts_stale_imvs();
    probe_db_close(dbname);
    assert!(failures.is_empty(), "{}\nstale after recovery: {stale_after_recovery}", failures.join("\n"));
}

#[pg_test]
fn v112_ts_deadlock_at_the_commit_of_a_three_leaf_swap_is_retried_not_counted() {
    let dbname = "reflex_ts_deadlock";
    let mut source = V112TsSource::open(dbname);
    v112_ts_hold(dbname);
    // The worker is not read while it waits: messages it sends then would fill
    // the connection and stall it before its COMMIT.
    worker_exec("SET deadlock_timeout = '1s'; SET lock_timeout = '30s'; SET client_min_messages = error");
    let before = v112_ts_flush_attempts();
    let swap = source.swap(&V112_TS_LEAVES);
    Spi::run(&format!("SELECT dblink_send_query('reflex_lock_worker', {})", sql_lit(&swap)))
        .expect("send the swap");
    Spi::run(
        "CREATE FUNCTION pg_temp.v112_ts_await_lock_wait(q text) RETURNS bool LANGUAGE plpgsql AS $fn$ \
         BEGIN \
           FOR i IN 1..200 LOOP \
             PERFORM pg_stat_clear_snapshot(); \
             IF EXISTS (SELECT 1 FROM pg_stat_activity \
                        WHERE query = q AND wait_event_type = 'Lock') THEN RETURN TRUE; END IF; \
             PERFORM pg_sleep(0.05); \
           END LOOP; \
           RETURN FALSE; \
         END $fn$",
    )
    .expect("poll fn");
    let worker_waiting = Spi::get_one::<bool>(&format!("SELECT pg_temp.v112_ts_await_lock_wait({})", sql_lit(&swap)))
        .expect("poll")
        .unwrap_or(false);
    // The swap's COMMIT waits on the held IMV; the holder now reads the source
    // the swap detached from, which closes the cycle the worker detects first.
    v112_ts_holder_exec("DO $r$ BEGIN PERFORM count(*) FROM sim; END $r$");
    let _ = Spi::get_one::<String>("SELECT r FROM dblink_get_result('reflex_lock_worker', false) AS t(r text)");
    let worker_error = Spi::get_one::<String>("SELECT dblink_error_message('reflex_lock_worker')")
        .expect("dblink_error_message")
        .map(|m| if m == "OK" { String::new() } else { m })
        .unwrap_or_default();
    let _ = Spi::get_one::<String>("SELECT r FROM dblink_get_result('reflex_lock_worker', false) AS t(r text)");
    let committed = if worker_error.is_empty() {
        Ok(v112_ts_flush_attempts() - before)
    } else {
        Err(worker_error)
    };
    let mut failures = Vec::new();
    if !worker_waiting {
        failures.push("fixture: the swap's COMMIT was never seen waiting on a lock".to_string());
    }
    failures.extend(v112_ts_transient_commit_failures("three-leaf swap", committed, "deadlock detected"));
    worker_exec("RESET deadlock_timeout; RESET lock_timeout; RESET client_min_messages");
    v112_ts_release();
    failures.extend(v112_ts_recovery_failures());
    let stale_after_recovery = v112_ts_stale_imvs();
    probe_db_close(dbname);
    assert!(failures.is_empty(), "{}\nstale after recovery: {stale_after_recovery}", failures.join("\n"));
}

#[pg_test]
fn v112_ts_transient_failures_over_more_commits_than_the_cap_never_suspend() {
    let dbname = "reflex_ts_repeated";
    let mut source = V112TsSource::open(dbname);
    v112_ts_hold(dbname);
    worker_exec("SET lock_timeout = '200ms'");
    let mut failures = Vec::new();
    for commit in 1..=crate::partition::PARTITION_FLUSH_FAILURE_CAP + 2 {
        let committed = v112_ts_commit(&source.swap(&["a"]));
        failures.extend(v112_ts_transient_commit_failures(&format!("commit {commit}"), committed, "lock timeout"));
    }
    worker_exec("RESET lock_timeout");
    v112_ts_release();
    match v112_ts_commit(&source.swap(&["b"])) {
        Err(error) => failures.push(format!("first commit after the release raised {error}")),
        Ok(1) => {}
        Ok(flushes) => failures.push(format!("first commit after the release flushed the root {flushes} times")),
    }
    if let Some(pending) = v112_ts_pending() {
        failures.push(format!("root still pending after the first commit that met no lock: {pending:?}"));
    }
    let (wrong, _) = v112_ts_wrong_and_silently_stale();
    failures.extend(wrong.into_iter().map(|w| format!("after the first commit that met no lock: {w}")));
    let stale_after_recovery = v112_ts_stale_imvs();
    probe_db_close(dbname);
    assert!(failures.is_empty(), "{}\nstale after recovery: {stale_after_recovery}", failures.join("\n"));
}

#[pg_test]
fn v112_ts_logic_failure_counts_once_per_commit_and_suspends_at_the_cap() {
    let dbname = "reflex_ts_logic";
    let mut source = V112TsSource::open(dbname);
    worker_exec(&format!("ALTER TABLE {V112_TS_HELD_IMV} ADD CONSTRAINT v112_ts_refuse CHECK (sv < 0) NOT VALID"));
    let cap = crate::partition::PARTITION_FLUSH_FAILURE_CAP;
    let mut failures = Vec::new();
    for commit in 1..=cap + 1 {
        let leaves: &[&'static str] = if commit == 1 { &V112_TS_LEAVES } else { &["a"] };
        let label = format!("commit {commit} ({} partition DDL commands)", 2 * leaves.len());
        let expected_flushes = if commit <= cap { 1 } else { 0 };
        match v112_ts_commit(&source.swap(leaves)) {
            Err(error) => failures.push(format!("{label}: the COMMIT raised {error}")),
            Ok(flushes) if flushes != expected_flushes => {
                failures.push(format!("{label}: the COMMIT flushed the root {flushes} times, expected {expected_flushes}"))
            }
            Ok(_) => {}
        }
        match v112_ts_pending() {
            None => failures.push(format!("{label}: the root left the queue although its flush failed")),
            Some((counted, last_error)) => {
                if counted != commit.min(cap) {
                    failures.push(format!("{label}: failures = {counted}, expected {}", commit.min(cap)));
                }
                if !last_error.contains("v112_ts_refuse") {
                    failures.push(format!("{label}: last_error = {last_error:?}"));
                }
            }
        }
        let (_, silent) = v112_ts_wrong_and_silently_stale();
        failures.extend(silent.into_iter().map(|s| format!("{label}: {s}")));
    }
    worker_exec(&format!("ALTER TABLE {V112_TS_HELD_IMV} DROP CONSTRAINT v112_ts_refuse"));
    rbc_select(&format!("reflex_reset_partition_failures('{V112_TS_ROOT}')"));
    failures.extend(v112_ts_recovery_failures());
    let stale_after_recovery = v112_ts_stale_imvs();
    probe_db_close(dbname);
    assert!(failures.is_empty(), "{}\nstale after recovery: {stale_after_recovery}", failures.join("\n"));
}

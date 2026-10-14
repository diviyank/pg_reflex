// A custom setting once `SET LOCAL`, or `SET` then `RESET`, reads back as ''
// for the rest of the session. Every read of a pg_reflex setting must take
// that, like an unset one, as "use the default" — and an unparsable value too,
// with a WARNING — instead of raising: a raise inside a DEFERRED flush discards
// the IMV's staged delta and leaves its readers silently wrong.
//
// Each test poisons its own session deliberately (`gec_poison_*`) and checks
// the poisoned state before writing, so none depends on what ran before it on
// the worker.

const GEC_SET_LOCAL_POISON: &str = "BEGIN; SET LOCAL reflex.wipe_threshold = 1000000000; COMMIT";

thread_local! {
    static GEC_WARNINGS: std::cell::RefCell<Vec<String>> = const { std::cell::RefCell::new(Vec::new()) };
}

unsafe extern "C-unwind" fn gec_keep_reflex_warnings(edata: *mut pgrx::pg_sys::ErrorData) {
    const WARNING: i32 = 19;
    if edata.is_null() {
        return;
    }
    let (level, message) = unsafe { ((*edata).elevel, (*edata).message) };
    if level != WARNING || message.is_null() {
        return;
    }
    let text = unsafe { std::ffi::CStr::from_ptr(message) }.to_string_lossy().to_string();
    if text.starts_with("pg_reflex") {
        GEC_WARNINGS.with(|kept| kept.borrow_mut().push(text));
    }
}

/// Starts keeping the `pg_reflex` WARNINGs of the calling backend.
#[pg_extern]
fn gec_keep_warnings() -> bool {
    GEC_WARNINGS.with(|kept| kept.borrow_mut().clear());
    unsafe {
        pgrx::pg_sys::emit_log_hook = Some(gec_keep_reflex_warnings);
    }
    true
}

/// The WARNINGs kept since the last call, one per row.
#[pg_extern]
fn gec_kept_warnings() -> Vec<String> {
    GEC_WARNINGS.with(|kept| kept.borrow_mut().drain(..).collect())
}

fn gec_worker_setting(name: &str) -> String {
    rbc_select(&format!("COALESCE(current_setting('{name}', true), '<unset>')"))
}

/// Commits a `SET LOCAL` of the setting: the worker session reads it as ''
/// from then on.
fn gec_poison_worker_with_set_local() {
    worker_exec(GEC_SET_LOCAL_POISON);
    assert_eq!(gec_worker_setting("reflex.wipe_threshold"), "", "poisoned by SET LOCAL");
}

fn gec_poison_worker_with_set_reset() {
    worker_exec("SET reflex.wipe_threshold = 0.3");
    worker_exec("RESET reflex.wipe_threshold");
    assert_eq!(gec_worker_setting("reflex.wipe_threshold"), "", "poisoned by SET + RESET");
}

fn gec_poison_in_process(name: &str) {
    Spi::run(&format!("SET {name} = 'on'")).expect("set");
    Spi::run(&format!("RESET {name}")).expect("reset");
    assert_eq!(
        Spi::get_one::<String>(&format!("SELECT current_setting('{name}', true)"))
            .expect("current_setting"),
        Some(String::new()),
        "poisoned by SET + RESET"
    );
}

/// No IMV of the worker's database is flagged stale and no flush failed.
fn gec_assert_no_flush_failure() {
    assert_eq!(
        sc_text_rows(
            "SELECT name || ': ' || COALESCE(stale_reason, '') FROM public.__reflex_ivm_reference \
              WHERE COALESCE(known_stale, FALSE) ORDER BY name"
        ),
        Vec::<String>::new(),
        "no IMV known_stale"
    );
    assert_eq!(
        sc_text_rows(
            "SELECT imv_name || ': ' || COALESCE(detail, '') FROM public.__reflex_event_log \
              WHERE event = 'error' ORDER BY id"
        ),
        Vec::<String>::new(),
        "no flush failed"
    );
}

fn gec_assert_chain_correct(chain: &ScChain) {
    let wrong: Vec<String> = chain
        .nodes()
        .into_iter()
        .filter_map(|(node, fresh)| {
            let mismatches = rbc_mismatch(&node, &fresh);
            (mismatches != 0).then(|| format!("{node}={mismatches}"))
        })
        .collect();
    assert_eq!(wrong, Vec::<String>::new(), "every node matches its recompute");
}

const GEC_CHAIN: ScChain = ScChain { field_names: false, cross_plan: false };

/// T1: a `SET LOCAL` committed earlier in the session must not cost a later
/// DEFERRED flush its delta.
#[pg_test]
fn gec_set_local_then_deferred_write_keeps_chain_correct() {
    const DBNAME: &str = "reflex_gec_t1";
    sc_open(DBNAME, "DEFERRED", GEC_CHAIN);
    gec_poison_worker_with_set_local();
    assert_eq!(GEC_CHAIN.commit("INSERT INTO sc_cur VALUES (3)"), Ok(()));
    gec_assert_chain_correct(&GEC_CHAIN);
    gec_assert_no_flush_failure();
    probe_db_close(DBNAME);
}

/// T2: the same after a session-level `SET` then `RESET`.
#[pg_test]
fn gec_set_reset_then_deferred_write_keeps_chain_correct() {
    const DBNAME: &str = "reflex_gec_t2";
    sc_open(DBNAME, "DEFERRED", GEC_CHAIN);
    gec_poison_worker_with_set_reset();
    assert_eq!(GEC_CHAIN.commit("INSERT INTO sc_cur VALUES (3)"), Ok(()));
    gec_assert_chain_correct(&GEC_CHAIN);
    gec_assert_no_flush_failure();
    probe_db_close(DBNAME);
}

/// T3, the report's shape: a leaf swap extending plan 3's last month, in a
/// transaction that `SET LOCAL`s the setting, then the heal of the ignoring
/// readers in the poisoned session.
#[pg_test]
fn gec_poisoned_heal_after_extending_push_keeps_chain_correct() {
    const DBNAME: &str = "reflex_gec_t3";
    sc_open(DBNAME, "DEFERRED", GEC_CHAIN);
    let extend = sc_swap(3, 4, &sc_leaf_rows(3, 4, 6, 5));
    assert_eq!(
        GEC_CHAIN.commit(&format!("SET LOCAL reflex.wipe_threshold = 1000000000; {extend}")),
        Ok(())
    );
    assert_eq!(gec_worker_setting("reflex.wipe_threshold"), "", "poisoned by the push");
    assert_eq!(GEC_CHAIN.commit("SELECT reflex_heal_ignored_sources()"), Ok(()));
    gec_assert_chain_correct(&GEC_CHAIN);
    assert_eq!(hor_queued_keys(), Vec::<String>::new(), "heal queue drained");
    gec_assert_no_flush_failure();
    probe_db_close(DBNAME);
}

const GEC_REGION_SRC_DDL: &[&str] = &[
    "CREATE TABLE gec_src (id INT NOT NULL, region TEXT NOT NULL, amount INT NOT NULL, \
       PRIMARY KEY (id, region)) PARTITION BY LIST (region)",
    "CREATE TABLE gec_src_a PARTITION OF gec_src FOR VALUES IN ('A')",
    "CREATE TABLE gec_src_b PARTITION OF gec_src FOR VALUES IN ('B')",
    "CREATE TABLE gec_src_c PARTITION OF gec_src FOR VALUES IN ('C')",
    "INSERT INTO gec_src SELECT g, r, g % 17 FROM generate_series(1, 300) g, \
       unnest(ARRAY['A', 'B', 'C']) r",
];

/// One DEFERRED IMV over `gec_src` in a poisoned worker session; a committed
/// UPDATE must land, through the dispatch builder the IMV's shape selects.
fn gec_deferred_dispatch_case(dbname: &str, sql: &str, keys: &str, partition_by: &str) {
    probe_db_open(dbname);
    for ddl in GEC_REGION_SRC_DDL {
        worker_exec(ddl);
    }
    assert_eq!(
        rbc_select(&format!(
            "create_reflex_ivm('gec_v', {}, {}, NULL, 'DEFERRED', NULL, {partition_by})",
            sql_lit(sql),
            sql_lit(keys)
        )),
        "CREATE REFLEX INCREMENTAL VIEW"
    );
    gec_poison_worker_with_set_local();
    worker_exec("UPDATE gec_src SET amount = amount + 5 WHERE id % 10 = 0 AND region IN ('A', 'C')");
    assert_eq!(rbc_mismatch("gec_v", sql), 0, "gec_v matches its recompute");
    gec_assert_no_flush_failure();
    probe_db_close(dbname);
}

/// T4, `build_high_selectivity_dispatch_sql`: an aggregate kept unpartitioned
/// (its group key is the source's partition key, which would otherwise
/// partition it and route it to the partition-aware builder).
#[pg_test]
fn gec_poisoned_aggregate_dispatch_applies_delta() {
    gec_deferred_dispatch_case(
        "reflex_gec_t4_agg",
        "SELECT region, SUM(amount) AS total, COUNT(*) AS n FROM gec_src GROUP BY region",
        "region",
        "ARRAY[]::text[]",
    );
}

/// T4, `build_partition_aware_dispatch_sql_strategy`: a partitioned aggregate.
#[pg_test]
fn gec_poisoned_partition_aware_dispatch_applies_delta() {
    gec_deferred_dispatch_case(
        "reflex_gec_t4_part",
        "SELECT region, id % 7 AS bucket, SUM(amount) AS total, COUNT(*) AS n \
         FROM gec_src GROUP BY region, id % 7",
        "region, bucket",
        "ARRAY['region']",
    );
}

/// T4, `build_passthrough_partition_dispatch_sql`: a keyed partitioned passthrough.
#[pg_test]
fn gec_poisoned_passthrough_partition_dispatch_applies_delta() {
    gec_deferred_dispatch_case(
        "reflex_gec_t4_pt",
        "SELECT id, region, amount FROM gec_src",
        "id, region",
        "ARRAY['region']",
    );
}

/// T4, the Path C block of a trigger body: a dimension row flipping into the
/// filter must not trip it in a poisoned session.
#[pg_test]
fn gec_poisoned_path_c_does_not_fail() {
    Spi::run("CREATE TABLE gec_pc_d (kind TEXT PRIMARY KEY, active BOOL NOT NULL)").expect("dim");
    Spi::run("CREATE TABLE gec_pc_f (id SERIAL PRIMARY KEY, kind TEXT NOT NULL, v INT NOT NULL)")
        .expect("fact");
    Spi::run("INSERT INTO gec_pc_d VALUES ('a', TRUE), ('b', FALSE), ('c', FALSE), ('d', FALSE)")
        .expect("seed dim");
    Spi::run(
        "INSERT INTO gec_pc_f (kind, v) SELECT k, g FROM generate_series(1, 50) g, \
         unnest(ARRAY['a', 'b', 'c', 'd']) k",
    )
    .expect("seed fact");
    let sql = "SELECT f.kind, SUM(f.v) AS s, COUNT(*) AS n FROM gec_pc_f f \
               JOIN gec_pc_d d ON d.kind = f.kind WHERE d.active GROUP BY f.kind";
    assert_eq!(
        crate::create_reflex_ivm("gec_pc_v", sql, None, None, None, None),
        "CREATE REFLEX INCREMENTAL VIEW"
    );
    gec_poison_in_process("reflex.wipe_threshold");
    Spi::run("SELECT tests.gec_keep_warnings()").expect("keep warnings");
    Spi::run("UPDATE gec_pc_d SET active = TRUE WHERE kind IN ('b', 'c', 'd')").expect("flip in");
    let warnings = Spi::get_one::<Vec<String>>("SELECT tests.gec_kept_warnings()")
        .expect("warnings")
        .unwrap_or_default();
    assert_eq!(warnings, Vec::<String>::new(), "no Path C failure");
    assert_imv_correct("gec_pc_v", sql);
}

/// An attach of `attach_rows` rows onto 2000 analysed ones, to an
/// unpartitioned IMV, after `poison` has set up the worker session. Returns
/// the WARNINGs kept during the attach and whether
/// `reflex_apply_partition_delta` applied it by its Path B reconcile (taken
/// when `attach_rows / 2000` reaches the threshold).
fn gec_attach_case(dbname: &str, attach_rows: i32, poison: fn()) -> (Vec<String>, bool) {
    let sql = "SELECT k, SUM(v) AS s, COUNT(*) AS n FROM gec_ap GROUP BY k";
    probe_db_open(dbname);
    for ddl in [
        "CREATE TABLE gec_ap (k INT NOT NULL, v INT NOT NULL) PARTITION BY LIST (k)",
        "CREATE TABLE gec_ap_1 PARTITION OF gec_ap FOR VALUES IN (1)",
        "INSERT INTO gec_ap SELECT 1, g FROM generate_series(1, 2000) g",
        "ANALYZE gec_ap",
        "CREATE TABLE gec_ap_2 (LIKE gec_ap)",
        &format!("INSERT INTO gec_ap_2 SELECT 2, g FROM generate_series(1, {attach_rows}) g"),
        "CREATE TABLE gec_trace (context TEXT NOT NULL)",
        "CREATE FUNCTION gec_note_refresh() RETURNS TRIGGER LANGUAGE plpgsql AS $$ \
           DECLARE stack TEXT; \
           BEGIN GET DIAGNOSTICS stack = PG_CONTEXT; \
             INSERT INTO gec_trace VALUES (stack); RETURN NULL; END $$",
    ] {
        worker_exec(ddl);
    }
    assert_eq!(
        rbc_select(&format!(
            "create_reflex_ivm('gec_ap_v', {}, 'k', NULL, NULL, NULL, ARRAY[]::text[])",
            sql_lit(sql)
        )),
        "CREATE REFLEX INCREMENTAL VIEW"
    );
    worker_exec(
        "CREATE TRIGGER gec_note_refresh AFTER UPDATE OF last_update_date \
           ON public.__reflex_ivm_reference FOR EACH ROW EXECUTE FUNCTION gec_note_refresh()",
    );
    poison();
    assert_eq!(rbc_select("tests.gec_keep_warnings()"), "true");
    worker_exec("ALTER TABLE gec_ap ATTACH PARTITION gec_ap_2 FOR VALUES IN (2)");
    let warnings = sc_text_rows("SELECT unnest(tests.gec_kept_warnings())");
    assert_eq!(rbc_mismatch("gec_ap_v", sql), 0, "gec_ap_v matches its recompute");
    let path_b = worker_scalar_i64(
        "SELECT count(*)::int8 FROM gec_trace \
          WHERE context LIKE '%reflex_apply_partition_delta%' \
            AND context LIKE '%reflex_reconcile%'",
    ) > 0;
    gec_assert_no_flush_failure();
    probe_db_close(dbname);
    (warnings, path_b)
}

/// T4, `reflex_apply_partition_delta` in a poisoned session.
#[pg_test]
fn gec_poisoned_partition_attach_takes_path_b() {
    let (_, path_b) =
        gec_attach_case("reflex_gec_t4_attach", 6000, gec_poison_worker_with_set_local);
    assert!(path_b, "the attach is applied by Path B's reconcile");
}

/// `reflex_apply_partition_delta` in a poisoned session uses exactly the
/// default threshold 0.5: an attach of 0.6 x the source takes Path B, one of
/// 0.4 x does not.
#[pg_test]
fn gec_poisoned_partition_delta_uses_the_default_threshold() {
    let (_, above) =
        gec_attach_case("reflex_gec_pd_above", 1200, gec_poison_worker_with_set_local);
    assert!(above, "ratio 0.6 reaches the default 0.5: Path B");
    let (_, below) =
        gec_attach_case("reflex_gec_pd_below", 800, gec_poison_worker_with_set_local);
    assert!(!below, "ratio 0.4 stays under the default 0.5: delta path");
}

/// The partition-anchor debug switch is read on every partitioned create and
/// flush; a poisoned session must not abort them.
#[pg_test]
fn gec_poisoned_debug_anchor_switch_does_not_abort_create() {
    Spi::run(
        "CREATE TABLE gec_da (id INT NOT NULL, region TEXT NOT NULL, amount INT NOT NULL) \
         PARTITION BY LIST (region)",
    )
    .expect("source");
    Spi::run("CREATE TABLE gec_da_a PARTITION OF gec_da FOR VALUES IN ('A')").expect("leaf");
    Spi::run("INSERT INTO gec_da SELECT g, 'A', g FROM generate_series(1, 20) g").expect("seed");
    gec_poison_in_process("pg_reflex.debug_resolve_anchor");
    let sql = "SELECT region, SUM(amount) AS total FROM gec_da GROUP BY region";
    assert_eq!(
        Spi::get_one::<String>(&format!(
            "SELECT create_reflex_ivm('gec_da_v', {}, NULL, NULL, NULL, NULL, ARRAY['region'])",
            sql_lit(sql)
        ))
        .expect("create")
        .unwrap_or_default(),
        "CREATE REFLEX INCREMENTAL VIEW"
    );
    Spi::run("INSERT INTO gec_da VALUES (21, 'A', 21)").expect("write");
    assert_imv_correct("gec_da_v", sql);
}

/// T5, SQL the extension installs (`reflex_apply_partition_delta`): an
/// unparsable `reflex.wipe_threshold` falls back to the default with a WARNING
/// naming the setting.
#[pg_test]
fn gec_unparsable_threshold_in_partition_delta_warns_and_defaults() {
    let (warnings, path_b) = gec_attach_case("reflex_gec_t5_attach", 6000, || {
        worker_exec("SET reflex.wipe_threshold = 'abc'")
    });
    assert!(path_b, "the default threshold applies: Path B");
    assert!(
        warnings.iter().any(|w| w.contains("reflex.wipe_threshold")),
        "a WARNING names reflex.wipe_threshold: {warnings:?}"
    );
}

/// T5, SQL the library generates at run time (the dispatch blocks), which
/// must not depend on a function the extension update creates: an unparsable
/// wipe setting is an operator error that fails the flush loudly — the IMV is
/// flagged stale with the cast error and an event row — never silently, and
/// the printed remedy repairs it once the setting is fixed.
#[pg_test]
fn gec_unparsable_wipe_settings_in_generated_dispatch_fail_loudly() {
    const DBNAME: &str = "reflex_gec_t5_dispatch";
    let sql = "SELECT region, id % 7 AS bucket, SUM(amount) AS total, COUNT(*) AS n \
               FROM gec_src GROUP BY region, id % 7";
    probe_db_open(DBNAME);
    for ddl in GEC_REGION_SRC_DDL {
        worker_exec(ddl);
    }
    assert_eq!(
        rbc_select(&format!(
            "create_reflex_ivm('gec_v', {}, 'region, bucket', NULL, 'DEFERRED', NULL, \
               ARRAY['region'])",
            sql_lit(sql)
        )),
        "CREATE REFLEX INCREMENTAL VIEW"
    );
    for setting in ["reflex.wipe_threshold", "reflex.wipe_floor_rows"] {
        worker_exec(&format!("SET {setting} = 'abc'"));
        worker_exec(
            "UPDATE gec_src SET amount = amount + 5 WHERE id % 10 = 0 AND region IN ('A', 'C')",
        );
        assert_eq!(
            sc_text_rows(
                "SELECT stale_reason FROM public.__reflex_ivm_reference \
                  WHERE name = 'gec_v' AND known_stale \
                    AND stale_reason LIKE '%invalid input syntax%reflex_reconcile%'"
            )
            .len(),
            1,
            "{setting}: the failed flush is flagged stale with its cause and remedy"
        );
        assert!(
            worker_scalar_i64(
                "SELECT count(*)::int8 FROM public.__reflex_event_log \
                  WHERE imv_name = 'gec_v' AND event = 'error' AND sqlstate = '22P02'"
            ) > 0,
            "{setting}: the failed flush is logged"
        );
        worker_exec(&format!("RESET {setting}"));
        let reconciled = rbc_select("reflex_reconcile('gec_v')");
        assert!(!reconciled.starts_with("ERROR"), "{setting}: {reconciled}");
        assert_eq!(rbc_mismatch("gec_v", sql), 0, "{setting}: the remedy repairs gec_v");
        worker_exec("DELETE FROM public.__reflex_event_log");
        gec_assert_no_flush_failure();
    }
    probe_db_close(DBNAME);
}

/// The upgrade window: the new library runs against an install whose
/// extension SQL predates `__reflex_setting`. Creating IMVs and flushing every
/// generated dispatch and the Path C block in a poisoned session must not need
/// it.
#[pg_test]
fn gec_library_does_not_need_the_setting_helper() {
    const DBNAME: &str = "reflex_gec_no_helper";
    probe_db_open(DBNAME);
    worker_exec("ALTER EXTENSION pg_reflex DROP FUNCTION public.__reflex_setting(text, anyelement)");
    worker_exec("DROP FUNCTION public.__reflex_setting(text, anyelement)");
    for ddl in GEC_REGION_SRC_DDL {
        worker_exec(ddl);
    }
    let deferred = [
        (
            "gec_agg",
            "SELECT region, SUM(amount) AS total, COUNT(*) AS n FROM gec_src GROUP BY region",
            "region",
            "ARRAY[]::text[]",
        ),
        (
            "gec_part",
            "SELECT region, id % 7 AS bucket, SUM(amount) AS total, COUNT(*) AS n \
             FROM gec_src GROUP BY region, id % 7",
            "region, bucket",
            "ARRAY['region']",
        ),
        ("gec_pt", "SELECT id, region, amount FROM gec_src", "id, region", "ARRAY['region']"),
    ];
    for (name, sql, keys, partition_by) in deferred {
        assert_eq!(
            rbc_select(&format!(
                "create_reflex_ivm('{name}', {}, {}, NULL, 'DEFERRED', NULL, {partition_by})",
                sql_lit(sql),
                sql_lit(keys)
            )),
            "CREATE REFLEX INCREMENTAL VIEW",
            "{name}"
        );
    }
    for ddl in [
        "CREATE TABLE gec_pc_d (kind TEXT PRIMARY KEY, active BOOL NOT NULL)",
        "CREATE TABLE gec_pc_f (id SERIAL PRIMARY KEY, kind TEXT NOT NULL, v INT NOT NULL)",
        "INSERT INTO gec_pc_d VALUES ('a', TRUE), ('b', FALSE), ('c', FALSE), ('d', FALSE)",
        "INSERT INTO gec_pc_f (kind, v) SELECT k, g FROM generate_series(1, 50) g, \
           unnest(ARRAY['a', 'b', 'c', 'd']) k",
    ] {
        worker_exec(ddl);
    }
    let path_c_sql = "SELECT f.kind, SUM(f.v) AS s, COUNT(*) AS n FROM gec_pc_f f \
                      JOIN gec_pc_d d ON d.kind = f.kind WHERE d.active GROUP BY f.kind";
    assert_eq!(
        rbc_select(&format!("create_reflex_ivm('gec_pc_v', {})", sql_lit(path_c_sql))),
        "CREATE REFLEX INCREMENTAL VIEW"
    );
    gec_poison_worker_with_set_local();
    assert_eq!(rbc_select("tests.gec_keep_warnings()"), "true");
    worker_exec("UPDATE gec_src SET amount = amount + 5 WHERE id % 10 = 0 AND region IN ('A', 'C')");
    worker_exec("UPDATE gec_pc_d SET active = TRUE WHERE kind IN ('b', 'c', 'd')");
    let warnings = sc_text_rows("SELECT unnest(tests.gec_kept_warnings())");
    assert_eq!(warnings, Vec::<String>::new(), "no flush or Path C failure");
    for (name, sql, _, _) in deferred {
        assert_eq!(rbc_mismatch(name, sql), 0, "{name} matches its recompute");
    }
    assert_eq!(rbc_mismatch("gec_pc_v", path_c_sql), 0, "gec_pc_v matches its recompute");
    gec_assert_no_flush_failure();
    probe_db_close(DBNAME);
}

/// The helper reads a valid value, and takes unset, '' and an unparsable value
/// as the default.
#[pg_test]
fn gec_setting_helper_reads_values_and_defaults() {
    let read = |default: &str| {
        Spi::get_one::<String>(&format!(
            "SELECT public.__reflex_setting('reflex.gec_probe', {default})::text"
        ))
        .expect("read")
        .unwrap_or_default()
    };
    assert_eq!(read("0.5::NUMERIC"), "0.5", "unset");
    Spi::run("SET reflex.gec_probe = '0.3'").expect("set");
    assert_eq!(read("0.5::NUMERIC"), "0.3", "numeric");
    Spi::run("SET reflex.gec_probe = '2000'").expect("set");
    assert_eq!(read("1000::BIGINT"), "2000", "bigint");
    Spi::run("SET reflex.gec_probe = 'on'").expect("set");
    assert_eq!(read("FALSE"), "true", "bool");
    Spi::run("SET reflex.gec_probe = 'abc'").expect("set");
    assert_eq!(read("0.5::NUMERIC"), "0.5", "unparsable");
    Spi::run("SET reflex.gec_probe = '99999999999999999999'").expect("set");
    assert_eq!(read("1000::BIGINT"), "1000", "out of range (22003)");
    Spi::run("RESET reflex.gec_probe").expect("reset");
    assert_eq!(read("0.5::NUMERIC"), "0.5", "empty");
}

thread_local! {
    static GEC_TRACE: std::cell::RefCell<Vec<String>> = const { std::cell::RefCell::new(Vec::new()) };
}

const GEC_TRACED_PREFIXES: &[&str] = &[
    "pg_reflex wipe:",
    "pg_reflex Path C smart bulk-INSERT:",
    "REFLEX-DBG resolve_anchor",
];

unsafe extern "C-unwind" fn gec_keep_reflex_trace(edata: *mut pgrx::pg_sys::ErrorData) {
    if edata.is_null() {
        return;
    }
    let message = unsafe { (*edata).message };
    if message.is_null() {
        return;
    }
    let text = unsafe { std::ffi::CStr::from_ptr(message) }.to_string_lossy().to_string();
    if GEC_TRACED_PREFIXES.iter().any(|prefix| text.starts_with(prefix)) {
        GEC_TRACE.with(|kept| kept.borrow_mut().push(text));
    }
}

/// Starts keeping the dispatch, Path C and anchor trace messages of the
/// calling backend; the caller sets `log_min_messages` low enough for them to
/// reach the server log, which is what calls the hook.
#[pg_extern]
fn gec_keep_trace() -> bool {
    GEC_TRACE.with(|kept| kept.borrow_mut().clear());
    unsafe {
        pgrx::pg_sys::emit_log_hook = Some(gec_keep_reflex_trace);
    }
    true
}

/// The trace messages kept since the last call, one per row.
#[pg_extern]
fn gec_kept_trace() -> Vec<String> {
    GEC_TRACE.with(|kept| kept.borrow_mut().drain(..).collect())
}

fn gec_kept_trace_in_process() -> Vec<String> {
    Spi::get_one::<Vec<String>>("SELECT tests.gec_kept_trace()")
        .expect("trace")
        .unwrap_or_default()
}

/// The threshold the unpartitioned aggregate dispatch (dispatch.rs:128)
/// actually uses, read from its `RAISE DEBUG 'pg_reflex wipe: ratio=% thr=%'`,
/// after `setup` in a fresh worker session.
fn gec_dispatch_threshold(dbname: &str, setup: &[&str]) -> Vec<String> {
    let sql = "SELECT region, SUM(amount) AS total, COUNT(*) AS n FROM gec_src GROUP BY region";
    probe_db_open(dbname);
    for ddl in GEC_REGION_SRC_DDL {
        worker_exec(ddl);
    }
    assert_eq!(
        rbc_select(&format!(
            "create_reflex_ivm('gec_v', {}, 'region', NULL, 'DEFERRED', NULL, ARRAY[]::text[])",
            sql_lit(sql)
        )),
        "CREATE REFLEX INCREMENTAL VIEW"
    );
    for step in setup {
        worker_exec(step);
    }
    worker_exec("SET log_min_messages = debug1");
    assert_eq!(rbc_select("tests.gec_keep_trace()"), "true");
    worker_exec("UPDATE gec_src SET amount = amount + 5 WHERE id % 10 = 0 AND region IN ('A', 'C')");
    let thresholds: Vec<String> = sc_text_rows("SELECT m FROM unnest(tests.gec_kept_trace()) m")
        .iter()
        .filter(|m| m.starts_with("pg_reflex wipe:"))
        .map(|m| m.split("thr=").nth(1).unwrap_or("").split(' ').next().unwrap_or("").to_string())
        .collect();
    worker_exec("RESET log_min_messages");
    assert_eq!(rbc_mismatch("gec_v", sql), 0, "gec_v matches its recompute");
    gec_assert_no_flush_failure();
    probe_db_close(dbname);
    thresholds
}

/// '' must give exactly the never-set default at the generated dispatch (0.5
/// for an IMV with no dependents), and a valid value must still be honoured.
#[pg_test]
fn gec_poisoned_dispatch_uses_the_never_set_default() {
    let never_set = gec_dispatch_threshold("reflex_gec_thr_unset", &[]);
    assert_eq!(never_set, vec!["0.5".to_string()], "never set: compiled default");
    let set_local = gec_dispatch_threshold("reflex_gec_thr_local", &[GEC_SET_LOCAL_POISON]);
    assert_eq!(set_local, never_set, "SET LOCAL-poisoned == never set");
    let set_reset = gec_dispatch_threshold(
        "reflex_gec_thr_reset",
        &["SET reflex.wipe_threshold = 0.3", "RESET reflex.wipe_threshold"],
    );
    assert_eq!(set_reset, never_set, "SET+RESET-poisoned == never set");
    let valid = gec_dispatch_threshold("reflex_gec_thr_valid", &["SET reflex.wipe_threshold = 0.3"]);
    assert_eq!(valid, vec!["0.3".to_string()], "a valid value is honoured");
}

/// A dimension flip of `flipped` kinds into an IMV already holding `active`
/// kinds (`facts` fact rows each), statistics fresh, in a session poisoned on
/// `reflex.wipe_threshold`. Returns whether Path C took the flip.
fn gec_path_c_taken(prefix: &str, active: i32, flipped: i32, facts: i32) -> bool {
    let (dim, fact, imv) = (format!("{prefix}_d"), format!("{prefix}_f"), format!("{prefix}_v"));
    Spi::run(&format!("CREATE TABLE {dim} (kind INT PRIMARY KEY, active BOOL NOT NULL)"))
        .expect("dim");
    Spi::run(&format!(
        "CREATE TABLE {fact} (id SERIAL PRIMARY KEY, kind INT NOT NULL, v INT NOT NULL)"
    ))
    .expect("fact");
    Spi::run(&format!(
        "INSERT INTO {dim} SELECT k, k <= {active} FROM generate_series(1, {}) k",
        active + flipped
    ))
    .expect("seed dim");
    Spi::run(&format!(
        "INSERT INTO {fact} (kind, v) SELECT k, g FROM generate_series(1, {}) k, \
         generate_series(1, {facts}) g",
        active + flipped
    ))
    .expect("seed fact");
    let sql = format!(
        "SELECT f.kind, SUM(f.v) AS s, COUNT(*) AS n FROM {fact} f \
         JOIN {dim} d ON d.kind = f.kind WHERE d.active GROUP BY f.kind"
    );
    assert_eq!(
        crate::create_reflex_ivm(&imv, &sql, None, None, None, None),
        "CREATE REFLEX INCREMENTAL VIEW"
    );
    Spi::run(&format!(
        "DO $a$ BEGIN \
           EXECUTE 'ANALYZE ' || public.reflex_intermediate_table_name('{imv}'); \
           EXECUTE 'ANALYZE {imv}'; EXECUTE 'ANALYZE {dim}'; EXECUTE 'ANALYZE {fact}'; END $a$"
    ))
    .expect("analyze");
    gec_poison_in_process("reflex.wipe_threshold");
    Spi::run("SET LOCAL log_min_messages = debug1").expect("log level");
    Spi::run("SELECT tests.gec_keep_trace()").expect("keep trace");
    Spi::run(&format!("UPDATE {dim} SET active = TRUE WHERE kind > {active}")).expect("flip in");
    let taken = gec_kept_trace_in_process()
        .iter()
        .any(|m| m.starts_with("pg_reflex Path C smart bulk-INSERT:"));
    Spi::run("SET LOCAL log_min_messages = warning").expect("log level");
    assert_imv_correct(&imv, &sql);
    taken
}

/// Path C in a poisoned session uses the default threshold 0.5: a flip far
/// under it stays on the standard path, one far over it takes Path C.
#[pg_test]
fn gec_poisoned_path_c_uses_the_default_threshold() {
    assert!(
        !gec_path_c_taken("gec_pcl", 400, 1, 3),
        "1 new kind against 400: under the default, standard path"
    );
    assert!(
        gec_path_c_taken("gec_pch", 1, 3, 50),
        "3 new kinds against 1: over the default, Path C"
    );
}

/// The anchor debug switch reads FALSE in a poisoned session: no debug output.
/// Set on, the same create does print it (the observable works).
#[pg_test]
fn gec_poisoned_debug_anchor_switch_stays_off() {
    Spi::run(
        "CREATE TABLE gec_dz (id INT NOT NULL, region TEXT NOT NULL, amount INT NOT NULL) \
         PARTITION BY LIST (region)",
    )
    .expect("source");
    Spi::run("CREATE TABLE gec_dz_a PARTITION OF gec_dz FOR VALUES IN ('A')").expect("leaf");
    Spi::run("INSERT INTO gec_dz SELECT g, 'A', g FROM generate_series(1, 20) g").expect("seed");
    let create = |name: &str| {
        Spi::get_one::<String>(&format!(
            "SELECT create_reflex_ivm('{name}', \
               'SELECT region, SUM(amount) AS total FROM gec_dz GROUP BY region', \
               NULL, NULL, NULL, NULL, ARRAY['region'])"
        ))
        .expect("create")
        .unwrap_or_default()
    };
    gec_poison_in_process("pg_reflex.debug_resolve_anchor");
    Spi::run("SET LOCAL log_min_messages = debug1").expect("log level");
    Spi::run("SELECT tests.gec_keep_trace()").expect("keep trace");
    assert_eq!(create("gec_dz_off"), "CREATE REFLEX INCREMENTAL VIEW");
    let poisoned = gec_kept_trace_in_process();
    Spi::run("SET pg_reflex.debug_resolve_anchor = on").expect("on");
    assert_eq!(create("gec_dz_on"), "CREATE REFLEX INCREMENTAL VIEW");
    let switched_on = gec_kept_trace_in_process();
    Spi::run("SET LOCAL log_min_messages = warning").expect("log level");
    assert!(
        !poisoned.iter().any(|m| m.starts_with("REFLEX-DBG")),
        "poisoned switch reads FALSE: {poisoned:?}"
    );
    assert!(
        switched_on.iter().any(|m| m.starts_with("REFLEX-DBG")),
        "switched on, the anchor resolution prints: {switched_on:?}"
    );
}

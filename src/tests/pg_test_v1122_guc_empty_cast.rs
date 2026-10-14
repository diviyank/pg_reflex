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
fn gec_deferred_dispatch_case(dbname: &str, sql: &str, keys: &str, partitioned: bool) {
    probe_db_open(dbname);
    for ddl in GEC_REGION_SRC_DDL {
        worker_exec(ddl);
    }
    let partition_by = if partitioned { ", NULL, ARRAY['region']" } else { "" };
    assert_eq!(
        rbc_select(&format!(
            "create_reflex_ivm('gec_v', {}, {}, NULL, 'DEFERRED'{partition_by})",
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

/// T4, `build_high_selectivity_dispatch_sql`: an unpartitioned aggregate.
#[pg_test]
fn gec_poisoned_aggregate_dispatch_applies_delta() {
    gec_deferred_dispatch_case(
        "reflex_gec_t4_agg",
        "SELECT region, SUM(amount) AS total, COUNT(*) AS n FROM gec_src GROUP BY region",
        "region",
        false,
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
        true,
    );
}

/// T4, `build_passthrough_partition_dispatch_sql`: a keyed partitioned passthrough.
#[pg_test]
fn gec_poisoned_passthrough_partition_dispatch_applies_delta() {
    gec_deferred_dispatch_case(
        "reflex_gec_t4_pt",
        "SELECT id, region, amount FROM gec_src",
        "id, region",
        true,
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

/// T4, `reflex_apply_partition_delta`: an attached partition three times the
/// size of the source is applied by Path B's reconcile at the default
/// threshold, which a poisoned session must still read.
#[pg_test]
fn gec_poisoned_partition_attach_takes_path_b() {
    const DBNAME: &str = "reflex_gec_t4_attach";
    let sql = "SELECT k, SUM(v) AS s, COUNT(*) AS n FROM gec_ap GROUP BY k";
    probe_db_open(DBNAME);
    for ddl in [
        "CREATE TABLE gec_ap (k INT NOT NULL, v INT NOT NULL) PARTITION BY LIST (k)",
        "CREATE TABLE gec_ap_1 PARTITION OF gec_ap FOR VALUES IN (1)",
        "INSERT INTO gec_ap SELECT 1, g FROM generate_series(1, 2000) g",
        "ANALYZE gec_ap",
        "CREATE TABLE gec_ap_2 (LIKE gec_ap)",
        "INSERT INTO gec_ap_2 SELECT 2, g FROM generate_series(1, 6000) g",
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
    gec_poison_worker_with_set_local();
    worker_exec("ALTER TABLE gec_ap ATTACH PARTITION gec_ap_2 FOR VALUES IN (2)");
    assert_eq!(rbc_mismatch("gec_ap_v", sql), 0, "gec_ap_v matches its recompute");
    assert!(
        worker_scalar_i64(
            "SELECT count(*)::int8 FROM gec_trace \
              WHERE context LIKE '%reflex_apply_partition_delta%' \
                AND context LIKE '%reflex_reconcile%'"
        ) > 0,
        "the attach is applied by Path B's reconcile: {:?}",
        sc_text_rows("SELECT left(context, 400) FROM gec_trace")
    );
    gec_assert_no_flush_failure();
    probe_db_close(DBNAME);
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

/// T5: an unparsable value of either wipe setting falls back to the default
/// with a WARNING naming the setting; the delta is applied, nothing is stale.
#[pg_test]
fn gec_unparsable_wipe_settings_warn_and_apply_delta() {
    const DBNAME: &str = "reflex_gec_t5";
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
    worker_exec("SET reflex.wipe_threshold = 'abc'");
    worker_exec("SET reflex.wipe_floor_rows = 'many'");
    assert_eq!(rbc_select("tests.gec_keep_warnings()"), "true");
    worker_exec("UPDATE gec_src SET amount = amount + 5 WHERE id % 10 = 0 AND region IN ('A', 'C')");
    let warnings = sc_text_rows("SELECT unnest(tests.gec_kept_warnings())");
    assert_eq!(rbc_mismatch("gec_v", sql), 0, "gec_v matches its recompute");
    gec_assert_no_flush_failure();
    for setting in ["reflex.wipe_threshold", "reflex.wipe_floor_rows"] {
        assert!(
            warnings.iter().any(|w| w.contains(setting)),
            "a WARNING names {setting}: {warnings:?}"
        );
    }
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
    Spi::run("SET reflex.gec_probe = '1e30'").expect("set");
    assert_eq!(read("1000::BIGINT"), "1000", "out of range");
    Spi::run("RESET reflex.gec_probe").expect("reset");
    assert_eq!(read("0.5::NUMERIC"), "0.5", "empty");
}

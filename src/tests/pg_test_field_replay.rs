// Field-replay regression suite (audit Phase 3). Each test distils a real base-db
// view that caused a production incident into self-contained synthetic tables at
// the view's real imv_options + shape, then replays the exact mutation that bit.
// Oracle = assert_imv_correct (EXCEPT ALL). Source views:
//   R1 current_assortment_activity_view (1.10.2 scalar-subquery filter)
//   R2 sop_incoming_stock_baseline_view (1.10.1 aggregate + LEFT-JOIN secondary)

// PS-6 helpers (unique names — all field-replay tests share one `mod tests`).
fn ps6_regclass_exists(name: &str) -> bool {
    Spi::get_one::<i64>(&format!(
        "SELECT COUNT(*) FROM pg_class WHERE relname = '{}'",
        name.replace('\'', "''")
    ))
    .unwrap()
    .unwrap_or(0)
        > 0
}

fn ps6_flush_count(imv: &str) -> i64 {
    Spi::get_one::<i64>(&format!(
        "SELECT COALESCE(flush_count, 0) FROM public.__reflex_ivm_reference WHERE name = '{}'",
        imv
    ))
    .unwrap()
    .unwrap_or(0)
}

/// PS-6 wedge + heal — the reproducible current-code form of the field 42P01
/// (`__reflex_pt_new_… does not exist`). A passthrough IMV whose per-source
/// scratch tables have gone missing (older create loop that didn't cover the
/// source, a partial create, or a manual drop) silently goes stale: every
/// DEFERRED flush fails fast inside the per-IMV subtransaction, is swallowed as
/// a WARNING, records the 42P01 in `last_error`, and never maintains the IMV.
///
/// The heal is `reflex_rebuild_triggers(<SOURCE>)` — the source-scoped entry
/// point (NOT the IMV name) which must recreate the missing pt scratch pair
/// idempotently so the IMV maintains again without a full drop+recreate.
#[pg_test]
fn ps6_missing_passthrough_scratch_wedges_then_heals() {
    Spi::run("CREATE TABLE ps6w_src (id INT PRIMARY KEY, grp TEXT, note TEXT)").unwrap();
    Spi::run("INSERT INTO ps6w_src VALUES (1,'a','x'),(2,'b','y')").unwrap();
    let sql = "SELECT id, grp, note FROM ps6w_src";
    crate::create_reflex_ivm("ps6w_v", sql, Some("id"), None, Some("DEFERRED"), None);

    let pt_new = "__reflex_pt_new_ps6w_v_ps6w_src";
    let pt_old = "__reflex_pt_old_ps6w_v_ps6w_src";
    assert!(
        ps6_regclass_exists(pt_new) && ps6_regclass_exists(pt_old),
        "a fresh passthrough create must have its pt scratch pair"
    );
    let stmts = flush_statements_for("ps6w_v", "ps6w_src", "INSERT");
    assert!(
        stmts.iter().any(|s| s.contains(pt_new)),
        "the passthrough flush must reference the pt scratch table: {stmts:#?}"
    );

    // --- wedge: the pt scratch tables vanish out from under the live IMV ---
    Spi::run(&format!("DROP TABLE \"{pt_new}\"")).unwrap();
    Spi::run(&format!("DROP TABLE \"{pt_old}\"")).unwrap();

    Spi::run("INSERT INTO ps6w_src VALUES (3,'c','z')").unwrap();
    Spi::run("SELECT reflex_flush_deferred('ps6w_src')")
        .expect("flush call itself returns (the 42P01 is swallowed as a WARNING)");
    let err_wedged = Spi::get_one::<String>(
        "SELECT last_error FROM public.__reflex_ivm_reference WHERE name='ps6w_v'",
    )
    .unwrap();
    assert!(
        err_wedged.as_deref().unwrap_or("").contains("does not exist"),
        "wedge must record the 42P01 in last_error, got: {err_wedged:?}"
    );
    let stale_missing =
        Spi::get_one::<i64>("SELECT COUNT(*) FROM ps6w_v WHERE id = 3").unwrap().unwrap();
    assert_eq!(stale_missing, 0, "IMV must be stale (row 3 never applied) while wedged");

    // --- heal via the SOURCE-scoped entry point ---
    let status = Spi::get_one::<String>("SELECT reflex_rebuild_triggers('ps6w_src')")
        .unwrap()
        .unwrap();
    assert!(!status.starts_with("ERROR"), "rebuild_triggers errored: {status}");
    assert!(
        ps6_regclass_exists(pt_new) && ps6_regclass_exists(pt_old),
        "reflex_rebuild_triggers must recreate the missing pt scratch pair"
    );

    // absorb whatever the wedge lost, then prove LIVE maintenance is restored.
    Spi::run("SELECT reflex_reconcile('ps6w_v')").expect("reconcile after heal");
    let fc_before = ps6_flush_count("ps6w_v");
    Spi::run("INSERT INTO ps6w_src VALUES (4,'d','w')").unwrap();
    Spi::run("SELECT reflex_flush_deferred('ps6w_src')").expect("healed flush");
    let err_healed = Spi::get_one::<String>(
        "SELECT last_error FROM public.__reflex_ivm_reference WHERE name='ps6w_v'",
    )
    .unwrap();
    assert!(
        err_healed.is_none(),
        "last_error must clear on a successful post-heal flush, got: {err_healed:?}"
    );
    assert!(
        ps6_flush_count("ps6w_v") > fc_before,
        "flush_count must advance on the healed flush"
    );
    assert_imv_correct("ps6w_v", sql);
}

/// R1a — 1.10.2 silent-wrong-delete: a passthrough IMV filtered by
/// `assortment_id = (SELECT … FROM sop_current)`, keyed on (product_id,
/// location_id). An UPDATE to a NON-current row that collides on (product_id,
/// location_id) with a current row must not delete the current IMV row.
#[pg_test]
fn replay_caav_noncurrent_keycollision_preserves_current_row() {
    Spi::run("CREATE TABLE aar (assortment_id INT, product_id INT, location_id INT, is_active BOOL)").unwrap();
    Spi::run("CREATE TABLE sop_current (assortment_id INT)").unwrap();
    Spi::run("INSERT INTO sop_current VALUES (1)").unwrap();
    // current (1,100,200) and non-current (14,100,200) collide on (product_id, location_id).
    Spi::run("INSERT INTO aar VALUES (1,100,200,true), (14,100,200,false)").unwrap();
    let sql = "SELECT product_id, location_id, is_active FROM aar \
               WHERE assortment_id = (SELECT assortment_id FROM sop_current)";
    crate::create_reflex_ivm("caav_v", sql, Some("product_id,location_id"), None, Some("DEFERRED"), None);
    // Update the OUT-of-filter (non-current) row. Must not disturb the current row.
    Spi::run("UPDATE aar SET is_active = true WHERE assortment_id = 14").unwrap();
    Spi::run("SELECT reflex_flush_deferred('aar')").expect("flush");
    assert_imv_correct("caav_v", sql);
}

/// R1b — 1.10.2 relevance-skip: a batch of mutations confined to NON-current
/// assortments must leave the IMV equal to a fresh recompute (the filter holds).
#[pg_test]
fn replay_caav_noncurrent_batch_relevance_skip_correct() {
    Spi::run("CREATE TABLE aar2 (assortment_id INT, product_id INT, location_id INT, is_active BOOL)").unwrap();
    Spi::run("CREATE TABLE sop_current2 (assortment_id INT)").unwrap();
    Spi::run("INSERT INTO sop_current2 VALUES (1)").unwrap();
    Spi::run("INSERT INTO aar2 VALUES (1,100,200,true),(1,101,200,false)").unwrap();
    let sql = "SELECT product_id, location_id, is_active FROM aar2 \
               WHERE assortment_id = (SELECT assortment_id FROM sop_current2)";
    crate::create_reflex_ivm("caav2_v", sql, Some("product_id,location_id"), None, Some("DEFERRED"), None);
    // Insert + update + delete, all on non-current assortment 14.
    Spi::run("INSERT INTO aar2 VALUES (14,100,200,false),(14,300,400,true)").unwrap();
    Spi::run("UPDATE aar2 SET is_active = true WHERE assortment_id = 14 AND product_id = 100").unwrap();
    Spi::run("DELETE FROM aar2 WHERE assortment_id = 14 AND product_id = 300").unwrap();
    Spi::run("SELECT reflex_flush_deferred('aar2')").expect("flush");
    assert_imv_correct("caav2_v", sql);
}

/// R2a — 1.10.1 shape, distilled: aggregate over a UNION ALL of two dim-joined
/// subqueries (each with a scalar-subquery date filter), LEFT JOIN to a secondary
/// (unit_pricing), GROUP BY. Correctness after a multi-source deferred mutation
/// (INSERT primary + UPDATE secondary in one batch). Oracle = EXCEPT ALL.
#[pg_test]
fn replay_sop_baseline_multisource_mutation_matches_recompute() {
    Spi::run("CREATE TABLE supply_plan (id INT PRIMARY KEY)").unwrap();
    Spi::run("CREATE TABLE max_order_date (order_date DATE)").unwrap();
    Spi::run("CREATE TABLE purchase_baseline (id INT PRIMARY KEY, product_id INT, location_id INT, supply_plan_id INT, delivery_date DATE, qty NUMERIC)").unwrap();
    Spi::run("CREATE TABLE stock_transfer_baseline (id INT PRIMARY KEY, product_id INT, location_id INT, supply_plan_id INT, delivery_date DATE, qty NUMERIC)").unwrap();
    Spi::run("CREATE TABLE unit_pricing (product_id INT PRIMARY KEY, unit_price NUMERIC)").unwrap();
    Spi::run("INSERT INTO supply_plan VALUES (1),(2)").unwrap();
    Spi::run("INSERT INTO max_order_date VALUES (date '2024-01-01')").unwrap();
    Spi::run("INSERT INTO unit_pricing VALUES (10,5.0),(11,7.0)").unwrap();
    Spi::run("INSERT INTO purchase_baseline VALUES \
        (1,10,100,1,date '2024-02-01',3),(2,11,100,1,date '2024-03-01',4)").unwrap();
    Spi::run("INSERT INTO stock_transfer_baseline VALUES (1,10,100,1,date '2024-02-15',2)").unwrap();
    let sql = "SELECT st.product_id, st.location_id, st.delivery_date, \
               SUM(st.purch) AS purchase_qty, \
               SUM(st.purch * COALESCE(up.unit_price,0)) AS purchase_value \
               FROM ( \
                 SELECT pb.product_id, pb.location_id, pb.delivery_date, pb.qty AS purch \
                 FROM purchase_baseline pb JOIN supply_plan sp ON pb.supply_plan_id = sp.id \
                 WHERE pb.delivery_date >= (SELECT order_date FROM max_order_date) \
                 UNION ALL \
                 SELECT stb.product_id, stb.location_id, stb.delivery_date, 0 AS purch \
                 FROM stock_transfer_baseline stb JOIN supply_plan sp ON stb.supply_plan_id = sp.id \
                 WHERE stb.delivery_date >= (SELECT order_date FROM max_order_date) \
               ) st \
               LEFT JOIN unit_pricing up ON up.product_id = st.product_id \
               GROUP BY st.product_id, st.location_id, st.delivery_date";
    crate::create_reflex_ivm("sopisb_v", sql, None, None, Some("DEFERRED"), None);
    // Multi-source deferred batch: new purchase row + secondary price update.
    Spi::run("INSERT INTO purchase_baseline VALUES (3,10,100,2,date '2024-04-01',5)").unwrap();
    Spi::run("UPDATE unit_pricing SET unit_price = 9.0 WHERE product_id = 10").unwrap();
    Spi::run("SELECT reflex_flush_deferred('purchase_baseline')").expect("flush pb");
    Spi::run("SELECT reflex_flush_deferred('unit_pricing')").expect("flush up");
    assert_imv_correct("sopisb_v", sql);
}

/// R2b — 1.10.1 plan-quality: a 1-row primary delta must stay O(delta), not
/// re-aggregate the whole union. The literal regression that took 18 minutes.
#[pg_test]
fn replay_sop_baseline_secondary_is_sublinear() {
    for (suf, n) in [("s", 20000_i32), ("b", 500000_i32)] {
        Spi::run(&format!("CREATE TABLE sp_{s} (id INT PRIMARY KEY)", s = suf)).unwrap();
        Spi::run(&format!("CREATE TABLE mod_{s} (order_date DATE)", s = suf)).unwrap();
        Spi::run(&format!("CREATE TABLE pb_{s} (id INT PRIMARY KEY, product_id INT, location_id INT, supply_plan_id INT, delivery_date DATE, qty NUMERIC)", s = suf)).unwrap();
        Spi::run(&format!("CREATE TABLE stb_{s} (id INT PRIMARY KEY, product_id INT, location_id INT, supply_plan_id INT, delivery_date DATE, qty NUMERIC)", s = suf)).unwrap();
        Spi::run(&format!("CREATE TABLE up_{s} (product_id INT PRIMARY KEY, unit_price NUMERIC)", s = suf)).unwrap();
        Spi::run(&format!("INSERT INTO sp_{s} SELECT g FROM generate_series(1,50) g", s = suf)).unwrap();
        Spi::run(&format!("INSERT INTO mod_{s} VALUES (date '2024-01-01')", s = suf)).unwrap();
        Spi::run(&format!("INSERT INTO up_{s} SELECT g, (g % 9) + 1 FROM generate_series(1,500) g", s = suf)).unwrap();
        Spi::run(&format!("INSERT INTO pb_{s} SELECT i, (i % 500)+1, i % 50, (i % 50)+1, date '2024-02-01' + (i % 300), i FROM generate_series(1,{n}) i", s = suf, n = n)).unwrap();
        Spi::run(&format!("INSERT INTO stb_{s} SELECT i, (i % 500)+1, i % 50, (i % 50)+1, date '2024-02-01' + (i % 300), i FROM generate_series(1,{n}) i", s = suf, n = n)).unwrap();
        let sql = format!(
            "SELECT st.product_id, st.location_id, st.delivery_date, \
             SUM(st.purch) AS purchase_qty, \
             SUM(st.purch * COALESCE(up.unit_price,0)) AS purchase_value \
             FROM ( \
               SELECT pb.product_id, pb.location_id, pb.delivery_date, pb.qty AS purch \
               FROM pb_{s} pb JOIN sp_{s} sp ON pb.supply_plan_id = sp.id \
               WHERE pb.delivery_date >= (SELECT order_date FROM mod_{s}) \
               UNION ALL \
               SELECT stb.product_id, stb.location_id, stb.delivery_date, 0 AS purch \
               FROM stb_{s} stb JOIN sp_{s} sp ON stb.supply_plan_id = sp.id \
               WHERE stb.delivery_date >= (SELECT order_date FROM mod_{s}) \
             ) st \
             LEFT JOIN up_{s} up ON up.product_id = st.product_id \
             GROUP BY st.product_id, st.location_id, st.delivery_date", s = suf);
        crate::create_reflex_ivm(&format!("sopisb_qy_{s}", s = suf), &sql, None, None, Some("DEFERRED"), None);
    }
    // If the main name has no flush recorded (decomposed sub-IMV), discover it:
    //   SELECT name FROM reflex_ivm_status() WHERE name LIKE 'sopisb_qy_s%';
    let small = min_flush_ms_sampled("pb_s", "sopisb_qy_s",
        |k| format!("INSERT INTO pb_s VALUES ({}, 7, 1, 1, date '2024-05-01', 5)", 900001 + k),
        PLAN_PROBE_SAMPLES);
    let big = min_flush_ms_sampled("pb_b", "sopisb_qy_b",
        |k| format!("INSERT INTO pb_b VALUES ({}, 7, 1, 1, date '2024-05-01', 5)", 900001 + k),
        PLAN_PROBE_SAMPLES);
    eprintln!("FIELD_R2B sop-baseline small={}ms big={}ms", small, big);
    assert_sublinear("sop-baseline-secondary", small, big, 25);
}

/// PS-6 D1 guard — the `reliability_snapshot_kinds` field shape: an aggregate
/// IMV depending on another aggregate IMV + a tracked base dim + a
/// `<subquery:k>`. The field 42P01 named a `__reflex_pt_*` scratch table for
/// this shape, but on current code an AGGREGATE IMV never routes through the
/// passthrough op path: its generated flush references no pt table, so the
/// missing-scratch failure cannot arise on a fresh create. This pins that
/// invariant — if a future change makes the aggregate path emit a pt reference
/// without creating the table, this test breaks.
#[pg_test]
fn ps6_aggregate_reliability_shape_never_references_pt_scratch() {
    Spi::run("CREATE TABLE ps6_kind_dim (id INT PRIMARY KEY, label TEXT)").unwrap();
    Spi::run("CREATE TABLE ps6_dp_fact (id INT PRIMARY KEY, sku TEXT, qty NUMERIC)").unwrap();
    Spi::run("CREATE TABLE ps6_demand_planning (id INT PRIMARY KEY, kind_id INT, sku TEXT)").unwrap();
    Spi::run("INSERT INTO ps6_kind_dim VALUES (1,'a'),(2,'b')").unwrap();
    Spi::run("INSERT INTO ps6_dp_fact VALUES (1,'x',3),(2,'y',4)").unwrap();
    Spi::run("INSERT INTO ps6_demand_planning VALUES (1,1,'x'),(2,2,'y')").unwrap();

    // inner aggregate IMV
    crate::create_reflex_ivm(
        "ps6_agg",
        "SELECT sku, SUM(qty) AS tot FROM ps6_dp_fact GROUP BY sku",
        None, None, None, None,
    );

    // reliability shape: aggregate, joins tracked base dim demand_planning,
    // references the agg IMV, and cross-joins a FROM-subquery (<subquery:k>).
    let sql = "SELECT dp.kind_id, SUM(ab.tot) AS s, COUNT(*) AS c \
               FROM ps6_demand_planning dp \
               JOIN ps6_agg ab ON ab.sku = dp.sku \
               , (SELECT id FROM ps6_kind_dim) k \
               WHERE dp.kind_id = k.id \
               GROUP BY dp.kind_id";
    crate::create_reflex_ivm("ps6_snap", sql, None, None, None, None);

    // The shape is aggregate, not passthrough.
    let is_pt = Spi::get_one::<bool>(
        "SELECT (aggregations->>'is_passthrough')::bool \
         FROM public.__reflex_ivm_reference WHERE name='ps6_snap'",
    ).unwrap().unwrap_or(false);
    assert!(!is_pt, "reliability shape must be aggregate (is_passthrough=false)");

    // Its generated flush for the tracked base dim references NO pt scratch.
    for op in ["INSERT", "UPDATE", "DELETE"] {
        let stmts = flush_statements_for("ps6_snap", "ps6_demand_planning", op);
        assert!(
            !stmts.iter().any(|s| s.contains("__reflex_pt_")),
            "aggregate flush ({op}) must not reference a passthrough scratch table: {stmts:#?}"
        );
    }

    // And it maintains correctly across a base-dim mutation (IMMEDIATE).
    Spi::run("INSERT INTO ps6_demand_planning VALUES (3,1,'x')").unwrap();
    assert_imv_correct("ps6_snap", sql);
    // audit is green — no missing-internal-table finding for an aggregate IMV.
    let report: String = Spi::get_one("SELECT reflex_audit('ps6_snap')")
        .unwrap().unwrap();
    assert!(
        !report.contains("internal-tables-exist"),
        "aggregate reliability shape should have no internal-tables finding:\n{report}"
    );
}

/// R2c — PS-5 Bug-1 closer, deterministic. The `sop_incoming_stock_baseline_view`
/// shape (UNION ALL in FROM, `BOOL_OR`, correlated scalar-subquery filter) keyed
/// on columns reached through the UNION — hence structurally NULLABLE, the
/// permanent-slow-path case. Before PS-5 the intermediate MERGE and the target
/// DELETE/INSERT matched this nullable key with `IS NOT DISTINCT FROM`, forcing a
/// nested loop over the whole intermediate/target and defeating both index use and
/// (in the field) partition pruning — the 9-minute flush. This pins that:
///   (1) maintenance stays CORRECT across INSERT/UPDATE/DELETE on both operands
///       (EXCEPT ALL oracle), and
///   (2) the generated flush for the shape actually carries the PS-5 gate — i.e.
///       the sargable path is taken, not the seq-scan one.
#[pg_test]
fn replay_sop_baseline_bool_or_nullable_key_is_gated_and_correct() {
    Spi::run("CREATE TABLE r2c_pb (id INT PRIMARY KEY, region TEXT, sku TEXT, qty NUMERIC, active BOOL)").unwrap();
    Spi::run("CREATE TABLE r2c_stb (id INT PRIMARY KEY, region TEXT, sku TEXT, qty NUMERIC, active BOOL)").unwrap();
    Spi::run("CREATE TABLE r2c_mod (order_date DATE)").unwrap();
    Spi::run("CREATE TABLE r2c_ship (id INT PRIMARY KEY, region TEXT, ship_date DATE)").unwrap();
    Spi::run("INSERT INTO r2c_mod VALUES (date '2024-01-01')").unwrap();
    // region deliberately includes a genuine NULL — the nullable-key case.
    Spi::run("INSERT INTO r2c_pb VALUES \
        (1,'north','a',3,true),(2,'north','b',4,false),(3,NULL,'a',5,true)").unwrap();
    Spi::run("INSERT INTO r2c_stb VALUES (1,'north','a',2,true),(2,NULL,'c',6,false)").unwrap();
    Spi::run("INSERT INTO r2c_ship VALUES (1,'north',date '2024-02-01'),(2,'south',date '2024-03-01')").unwrap();

    // UNION ALL in FROM + BOOL_OR + correlated scalar subquery filter, GROUP BY a
    // union-derived (structurally nullable) key.
    let sql = "SELECT s.region, \
               SUM(s.qty) AS total_qty, \
               BOOL_OR(s.active) AS any_active \
               FROM ( \
                 SELECT pb.region, pb.sku, pb.qty, pb.active FROM r2c_pb pb \
                 WHERE EXISTS (SELECT 1 FROM r2c_ship sh WHERE sh.ship_date >= (SELECT order_date FROM r2c_mod)) \
                 UNION ALL \
                 SELECT stb.region, stb.sku, stb.qty, stb.active FROM r2c_stb stb \
                 WHERE stb.qty >= (SELECT count(*) FROM r2c_mod) \
               ) s \
               GROUP BY s.region";
    crate::create_reflex_ivm("r2c_v", sql, None, None, None, None);
    assert_imv_correct("r2c_v", sql);

    // (2) The gate must be present in the generated flush for BOTH operands — i.e.
    // the codegen took the nullable-key sargable path, not the seq-scan one. Use
    // the same SPI-through-installed-.so provenance technique as the audit gate
    // (reaches real codegen, fails if a sibling clobbered the install).
    for src in ["r2c_pb", "r2c_stb"] {
        let stmts = flush_statements_for("r2c_v", src, "INSERT");
        assert!(
            stmts.iter().any(|s| s.contains("AS __ng WHERE")),
            "sop-baseline nullable-key flush for source {src} must carry the PS-5 gate \
             (else it is on the non-sargable seq-scan path that caused the 9-min flush): {stmts:#?}"
        );
        // The gated MERGE's fast variant must use `=` (sargable), reachable by index.
        assert!(
            stmts.iter().any(|s| s.contains("MERGE INTO") && s.contains("ON t.\"region\" = d.\"region\"")),
            "the gated MERGE fast variant must match the group key with `=`: {stmts:#?}"
        );
    }

    // (1 cont.) Correctness across mutations on both operands, incl. the NULL group.
    Spi::run("INSERT INTO r2c_pb VALUES (4,'north','d',10,true)").unwrap();      // non-NULL group
    assert_imv_correct("r2c_v", sql);
    Spi::run("INSERT INTO r2c_stb VALUES (3,NULL,'e',1,true)").unwrap();          // NULL group
    assert_imv_correct("r2c_v", sql);
    Spi::run("UPDATE r2c_pb SET active = false WHERE id = 1").unwrap();           // BOOL_OR recompute
    assert_imv_correct("r2c_v", sql);
    Spi::run("DELETE FROM r2c_pb WHERE region IS NULL").unwrap();                 // shrink NULL group
    assert_imv_correct("r2c_v", sql);
}

/// PS-6 F1 — exercise the ACTUAL shipped 1.10.11→1.11.0 migration `DO $ps6$`
/// block (extracted from the migration file at compile time, so this cannot
/// drift from what ships) against a live wedged passthrough IMV. The pgrx test
/// harness installs the extension fresh and never runs the migration chain, so
/// without this the migration SQL is untested — and it carries real logic (a
/// data-modifying CTE with array_agg, the known_stale marking).
///
/// The block must (1) recreate the missing pt scratch pair via
/// reflex_rebuild_triggers and (2) mark the wedged IMV known_stale with a PS-6
/// stale_reason — NOT silently declare full recovery — so the existing F3/F4
/// reflex_doctor path surfaces it and prescribes reflex_reconcile, which then
/// clears known_stale once the lost deltas are backfilled.
#[pg_test]
fn ps6_migration_do_block_recreates_scratch_and_marks_known_stale() {
    let migration = include_str!("../../sql/pg_reflex--1.10.11--1.11.0.sql");
    let start = migration.find("DO $ps6$").expect("migration must contain the PS-6 DO block");
    let end = migration[start..].find("$ps6$;").expect("PS-6 block must terminate") + start
        + "$ps6$;".len();
    let do_block = &migration[start..end];

    Spi::run("CREATE TABLE ps6m_src (id INT PRIMARY KEY, grp TEXT)").unwrap();
    Spi::run("INSERT INTO ps6m_src VALUES (1,'a')").unwrap();
    let sql = "SELECT id, grp FROM ps6m_src";
    crate::create_reflex_ivm("ps6m_v", sql, Some("id"), None, Some("DEFERRED"), None);

    let pt_new = "__reflex_pt_new_ps6m_v_ps6m_src";
    let pt_old = "__reflex_pt_old_ps6m_v_ps6m_src";
    Spi::run(&format!("DROP TABLE \"{pt_new}\"")).unwrap();
    Spi::run(&format!("DROP TABLE \"{pt_old}\"")).unwrap();

    // Wedge it: a deferred flush records the 42P01 in last_error and (the bug
    // this whole fix is about) loses the staged delta.
    Spi::run("INSERT INTO ps6m_src VALUES (2,'b')").unwrap();
    Spi::run("SELECT reflex_flush_deferred('ps6m_src')").expect("flush swallows the 42P01");
    let wedged_err = Spi::get_one::<String>(
        "SELECT last_error FROM public.__reflex_ivm_reference WHERE name='ps6m_v'",
    )
    .unwrap();
    assert!(
        wedged_err.as_deref().unwrap_or("").contains("does not exist"),
        "precondition: IMV must carry the 42P01 last_error, got {wedged_err:?}"
    );

    // Run the real migration recovery block.
    Spi::run(do_block).expect("the shipped PS-6 DO block must execute");

    assert!(
        ps6_regclass_exists(pt_new) && ps6_regclass_exists(pt_old),
        "migration must recreate the pt scratch pair"
    );
    let (stale, reason) = Spi::connect(|c| {
        let row = c
            .select(
                "SELECT known_stale, stale_reason FROM public.__reflex_ivm_reference WHERE name='ps6m_v'",
                None,
                &[],
            )
            .unwrap()
            .first();
        (
            row.get_by_name::<bool, _>("known_stale").unwrap().unwrap_or(false),
            row.get_by_name::<String, _>("stale_reason").unwrap().unwrap_or_default(),
        )
    });
    assert!(stale, "migration must mark the wedged IMV known_stale (deltas were lost)");
    assert!(
        reason.contains("PS-6"),
        "known_stale must carry the PS-6 recovery reason, got: {reason:?}"
    );

    // reflex_reconcile is the prescribed backfill and must clear known_stale.
    Spi::run("SELECT reflex_reconcile('ps6m_v')").expect("reconcile backfills");
    let still_stale = Spi::get_one::<bool>(
        "SELECT known_stale FROM public.__reflex_ivm_reference WHERE name='ps6m_v'",
    )
    .unwrap()
    .unwrap_or(true);
    assert!(!still_stale, "reflex_reconcile must clear known_stale");
    assert_imv_correct("ps6m_v", sql);
}

/// PS-6 heal-loop resilience — the shipped `DO $ps6$` loop calls
/// `reflex_rebuild_triggers(_src)` once per enabled passthrough source. Contrary
/// to the PS-6 comment ("returns an ERROR string rather than raising"), that
/// function RAISES on a source it cannot resolve: for a DEFERRED passthrough it
/// resolves the qualified `depends_on` name and then fetches the source's columns
/// via `'<schema>.<rel>'::regclass`, which ereports ERROR when the schema/relation
/// no longer exists — the recurring multi-tenant / dropped-schema footgun (a stale
/// IMV whose source lives in, or once lived in, a non-`public` tenant schema).
///
/// Because ALTER EXTENSION UPDATE is ONE atomic transaction, an unguarded raise in
/// this loop rolls back the ENTIRE 1.11.0 upgrade (PS-1/PS-4/PS-2/PS-3/PS-7
/// included) — the operator gets none of 1.11.0. The loop must survive one
/// unresolvable source (downgrade the raise to a WARNING) and keep healing the
/// others. This exercises the ACTUAL shipped `DO $ps6$` block via `include_str!`.
#[pg_test]
fn ps6_migration_do_block_survives_unresolvable_source() {
    let migration = include_str!("../../sql/pg_reflex--1.10.11--1.11.0.sql");
    let start = migration.find("DO $ps6$").expect("migration must contain the PS-6 DO block");
    let end = migration[start..].find("$ps6$;").expect("PS-6 block must terminate") + start
        + "$ps6$;".len();
    let do_block = &migration[start..end];

    // (1) A healthy, resolvable passthrough source. Drop its scratch pair so the
    //     heal loop has something to (re)create — proof that the loop kept going.
    Spi::run("CREATE TABLE ps6g_ok_src (id INT PRIMARY KEY, grp TEXT)").unwrap();
    Spi::run("INSERT INTO ps6g_ok_src VALUES (1,'a')").unwrap();
    crate::create_reflex_ivm(
        "ps6g_ok_v",
        "SELECT id, grp FROM ps6g_ok_src",
        Some("id"),
        None,
        Some("DEFERRED"),
        None,
    );
    let ok_new = "__reflex_pt_new_ps6g_ok_v_ps6g_ok_src";
    let ok_old = "__reflex_pt_old_ps6g_ok_v_ps6g_ok_src";
    Spi::run(&format!("DROP TABLE \"{ok_new}\"")).unwrap();
    Spi::run(&format!("DROP TABLE \"{ok_old}\"")).unwrap();
    assert!(
        !ps6_regclass_exists(ok_new) && !ps6_regclass_exists(ok_old),
        "precondition: the resolvable source's scratch pair must be dropped"
    );

    // (2) A stale passthrough IMV whose depends_on names a qualified relation in a
    //     schema that does not exist. reflex_rebuild_triggers takes the qualified
    //     branch, sees a DEFERRED dependent, and RAISES at `::regclass` fetching
    //     the missing source's columns — exactly the field abort.
    Spi::run("CREATE TABLE ps6g_bad_src (id INT PRIMARY KEY, grp TEXT)").unwrap();
    crate::create_reflex_ivm(
        "ps6g_bad_v",
        "SELECT id, grp FROM ps6g_bad_src",
        Some("id"),
        None,
        Some("DEFERRED"),
        None,
    );
    Spi::run(
        "UPDATE public.__reflex_ivm_reference \
            SET depends_on = ARRAY['ps6g_gone_schema.ps6g_gone_src'] \
          WHERE name = 'ps6g_bad_v'",
    )
    .unwrap();

    // (a) The shipped heal loop must NOT propagate the raise: a WARNING keeps the
    //     enclosing atomic ALTER EXTENSION UPDATE alive, an ERROR rolls it all back.
    //     On the current unguarded loop this returns Err (the upgrade aborts).
    let res = Spi::run(do_block);
    assert!(
        res.is_ok(),
        "PS-6 heal loop must not abort the upgrade when a passthrough source is \
         unresolvable — it must WARN and continue. Got: {res:?}"
    );

    // (b) …and it must have kept healing the OTHER, resolvable source.
    assert!(
        ps6_regclass_exists(ok_new) && ps6_regclass_exists(ok_old),
        "PS-6 heal loop must recreate the resolvable source's scratch pair despite \
         another source raising"
    );
}

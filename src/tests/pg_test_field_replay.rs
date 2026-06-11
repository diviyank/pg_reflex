// Field-replay regression suite (audit Phase 3). Each test distils a real base-db
// view that caused a production incident into self-contained synthetic tables at
// the view's real imv_options + shape, then replays the exact mutation that bit.
// Oracle = assert_imv_correct (EXCEPT ALL). Source views:
//   R1 current_assortment_activity_view (1.10.2 scalar-subquery filter)
//   R2 sop_incoming_stock_baseline_view (1.10.1 aggregate + LEFT-JOIN secondary)

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
        Spi::run(&format!("INSERT INTO pb_{s} VALUES (900001, 7, 1, 1, date '2024-05-01', 5)", s = suf)).unwrap();
        Spi::run(&format!("SELECT reflex_flush_deferred('pb_{s}')", s = suf)).unwrap();
    }
    // If the main name has no flush recorded (decomposed sub-IMV), discover it:
    //   SELECT name FROM reflex_ivm_status() WHERE name LIKE 'sopisb_qy_s%';
    let small = last_flush_ms_of("sopisb_qy_s");
    let big = last_flush_ms_of("sopisb_qy_b");
    eprintln!("FIELD_R2B sop-baseline small={}ms big={}ms", small, big);
    assert_sublinear("sop-baseline-secondary", small, big, 25);
}

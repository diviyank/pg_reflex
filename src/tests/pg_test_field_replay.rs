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

// A TRUNCATE of a source that sits on the NULLABLE side of an outer join must
// not empty the dependent IMV.
//
// Field incident 2026-09-30 (alp.sop_forecast_view, 1.11.4): a bulk upsert on
// the table under `current_assortment_activity_view` tripped the wipe
// threshold, which rebuilt that IMV with TRUNCATE + INSERT. Its AFTER TRUNCATE
// trigger ran `DELETE FROM <dependent>` on `sop_forecast_view`, which only
// LEFT JOINs it — every row of every plan was deleted, although a LEFT JOIN
// keeps every anchor row whatever the nullable side holds. The refill INSERT
// only staged a delta for the keys present on the nullable side, so even a
// successful flush could not restore the unmatched rows.
//
// Fixtures are real IMVs over real tables; correctness is the bidirectional
// EXCEPT ALL oracle against the base query.

const TOJ_SQL_IMMEDIATE: &str = "SELECT a.id, a.product_id, a.location_id, a.qty, \
     COALESCE(c.is_active, FALSE) AS active \
     FROM toj_anchor a LEFT JOIN toj_act c \
     ON c.product_id = a.product_id AND c.location_id = a.location_id";

fn build_toj_tables(anchor: &str, act: &str) {
    Spi::run(&format!(
        "CREATE TABLE {anchor} (id INT PRIMARY KEY, product_id INT NOT NULL, \
         location_id INT NOT NULL, qty INT)"
    ))
    .expect("anchor");
    Spi::run(&format!(
        "CREATE TABLE {act} (product_id INT NOT NULL, location_id INT NOT NULL, \
         is_active BOOL, PRIMARY KEY (product_id, location_id))"
    ))
    .expect("secondary");
    Spi::run(&format!(
        "INSERT INTO {anchor} SELECT g, g % 7, g % 5, g FROM generate_series(1, 200) g"
    ))
    .expect("seed anchor");
    Spi::run(&format!(
        "INSERT INTO {act} SELECT p, l, (p + l) % 2 = 0 \
         FROM generate_series(0, 3) p CROSS JOIN generate_series(0, 4) l"
    ))
    .expect("seed secondary");
}

fn toj_row_count(view: &str) -> i64 {
    Spi::get_one::<i64>(&format!("SELECT count(*)::int8 FROM {view}"))
        .expect("count")
        .expect("count NULL")
}

/// T1 — IMMEDIATE: truncating the LEFT JOIN's nullable side keeps every anchor
/// row (now null-extended), it does not empty the IMV.
#[pg_test]
fn pg_toj_truncate_left_joined_source_keeps_anchor_rows_immediate() {
    build_toj_tables("toj_anchor", "toj_act");
    let res = crate::create_reflex_ivm("toj_v1", TOJ_SQL_IMMEDIATE, Some("id"), None, None, None);
    assert_eq!(res, "CREATE REFLEX INCREMENTAL VIEW");
    assert_imv_correct("toj_v1", TOJ_SQL_IMMEDIATE);

    Spi::run("TRUNCATE toj_act").expect("truncate secondary");

    assert_eq!(toj_row_count("toj_v1"), 200, "LEFT JOIN dependent lost anchor rows");
    assert_imv_correct("toj_v1", TOJ_SQL_IMMEDIATE);
}

/// T2 — DEFERRED, the field sequence on a plain table: TRUNCATE + re-INSERT of
/// the nullable side in one transaction, then the COMMIT-time flush.
#[pg_test]
fn pg_toj_truncate_refill_left_joined_source_deferred() {
    build_toj_tables("tojd_anchor", "tojd_act");
    let sql = TOJ_SQL_IMMEDIATE
        .replace("toj_anchor", "tojd_anchor")
        .replace("toj_act", "tojd_act");
    let res = crate::create_reflex_ivm("toj_v2", &sql, Some("id"), None, Some("DEFERRED"), None);
    assert_eq!(res, "CREATE REFLEX INCREMENTAL VIEW");
    assert_imv_correct("toj_v2", &sql);

    Spi::run("CREATE TEMP TABLE tojd_keep AS SELECT * FROM tojd_act WHERE product_id < 2")
        .expect("keep a subset");
    Spi::run("TRUNCATE tojd_act").expect("truncate secondary");
    assert_eq!(
        toj_row_count("toj_v2"),
        200,
        "the TRUNCATE deleted rows before the flush — a failed flush makes the loss permanent"
    );
    Spi::run("INSERT INTO tojd_act SELECT * FROM tojd_keep").expect("refill subset");
    Spi::run("SELECT reflex_flush_deferred('tojd_act')").expect("flush");

    assert_eq!(toj_row_count("toj_v2"), 200, "LEFT JOIN dependent lost anchor rows");
    assert_imv_correct("toj_v2", &sql);
}

/// T3 — the production chain: the nullable side is itself a DEFERRED
/// passthrough IMV whose rebuild (`reflex_reconcile`, which the wipe-threshold
/// dispatch calls) is TRUNCATE + INSERT.
#[pg_test]
fn pg_toj_reconcile_of_left_joined_imv_keeps_dependent() {
    build_toj_tables("tojc_anchor", "tojc_rel");
    let caav = crate::create_reflex_ivm(
        "tojc_caav",
        "SELECT product_id, location_id, is_active FROM tojc_rel",
        Some("product_id, location_id"),
        None,
        Some("DEFERRED"),
        None,
    );
    assert_eq!(caav, "CREATE REFLEX INCREMENTAL VIEW");
    let sql = "SELECT a.id, a.product_id, a.location_id, a.qty, \
               COALESCE(c.is_active, FALSE) AS active \
               FROM tojc_anchor a LEFT JOIN tojc_caav c \
               ON c.product_id = a.product_id AND c.location_id = a.location_id";
    let fresh = "SELECT a.id, a.product_id, a.location_id, a.qty, \
                 COALESCE(c.is_active, FALSE) AS active \
                 FROM tojc_anchor a LEFT JOIN tojc_rel c \
                 ON c.product_id = a.product_id AND c.location_id = a.location_id";
    let dep = crate::create_reflex_ivm("tojc_sfv", sql, Some("id"), None, Some("DEFERRED"), None);
    assert_eq!(dep, "CREATE REFLEX INCREMENTAL VIEW");
    assert_imv_correct("tojc_sfv", fresh);

    let res = Spi::get_one::<&str>("SELECT reflex_reconcile('tojc_caav')")
        .expect("reconcile")
        .expect("reconcile result");
    assert_eq!(res, "RECONCILED");
    assert_eq!(
        toj_row_count("tojc_sfv"),
        200,
        "the rebuild's TRUNCATE deleted rows before the flush — a failed flush makes the loss permanent"
    );
    Spi::run("SELECT reflex_flush_deferred('tojc_caav')").expect("flush");

    assert_eq!(toj_row_count("tojc_sfv"), 200, "LEFT JOIN dependent lost anchor rows");
    assert_imv_correct("tojc_sfv", fresh);
}

/// T4 — control: an INNER JOIN source truncated really does empty the result,
/// and the dependent must follow.
#[pg_test]
fn pg_toj_truncate_inner_joined_source_still_empties_dependent() {
    build_toj_tables("toji_anchor", "toji_act");
    let sql = "SELECT a.id, a.product_id, a.location_id, a.qty, c.is_active AS active \
               FROM toji_anchor a JOIN toji_act c \
               ON c.product_id = a.product_id AND c.location_id = a.location_id";
    let res = crate::create_reflex_ivm("toj_v4", sql, Some("id"), None, None, None);
    assert_eq!(res, "CREATE REFLEX INCREMENTAL VIEW");
    assert!(toj_row_count("toj_v4") > 0, "fixture produced an empty inner join");

    Spi::run("TRUNCATE toji_act").expect("truncate secondary");

    assert_eq!(toj_row_count("toj_v4"), 0);
    assert_imv_correct("toj_v4", sql);
}

/// T5 — the full field sequence in one transaction: an incremental change to
/// the nullable-side IMV stages a delta for the dependent, then a rebuild of
/// that IMV (TRUNCATE + INSERT) stages the same key again. The TRUNCATE leaves
/// the earlier staged row in place, the flush sees the key twice, and the
/// dependent must still end up complete.
#[pg_test]
fn pg_toj_incremental_then_rebuild_of_left_joined_imv_keeps_dependent() {
    build_toj_tables("toj5_anchor", "toj5_rel");
    let caav = crate::create_reflex_ivm(
        "toj5_caav",
        "SELECT product_id, location_id, is_active FROM toj5_rel",
        Some("product_id, location_id"),
        None,
        Some("DEFERRED"),
        None,
    );
    assert_eq!(caav, "CREATE REFLEX INCREMENTAL VIEW");
    let sql = "SELECT a.id, a.product_id, a.location_id, a.qty, \
               COALESCE(c.is_active, FALSE) AS active \
               FROM toj5_anchor a LEFT JOIN toj5_caav c \
               ON c.product_id = a.product_id AND c.location_id = a.location_id";
    let fresh = "SELECT a.id, a.product_id, a.location_id, a.qty, \
                 COALESCE(c.is_active, FALSE) AS active \
                 FROM toj5_anchor a LEFT JOIN toj5_rel c \
                 ON c.product_id = a.product_id AND c.location_id = a.location_id";
    let dep = crate::create_reflex_ivm("toj5_sfv", sql, Some("id"), None, Some("DEFERRED"), None);
    assert_eq!(dep, "CREATE REFLEX INCREMENTAL VIEW");
    assert_imv_correct("toj5_sfv", fresh);

    Spi::run("INSERT INTO toj5_rel VALUES (5, 1, TRUE)").expect("activate a new key");
    Spi::run("SELECT reflex_flush_deferred('toj5_rel')").expect("flush rel into caav");
    let res = Spi::get_one::<&str>("SELECT reflex_reconcile('toj5_caav')")
        .expect("reconcile")
        .expect("reconcile result");
    assert_eq!(res, "RECONCILED");
    Spi::run("SELECT reflex_flush_deferred('toj5_caav')").expect("flush caav into sfv");

    assert_eq!(toj_row_count("toj5_sfv"), 200, "LEFT JOIN dependent lost anchor rows");
    assert_imv_correct("toj5_sfv", fresh);
    let stale = Spi::get_one::<bool>(
        "SELECT known_stale FROM public.__reflex_ivm_reference WHERE name = 'toj5_sfv'",
    )
    .expect("stale q")
    .unwrap_or(false);
    assert!(!stale, "the dependent's flush failed and was discarded");
}

/// T6 — the production shape: the dependent is partitioned by plan, mirroring
/// a LIST-partitioned anchor, and LEFT JOINs a DEFERRED passthrough IMV that is
/// rebuilt.
#[pg_test]
fn pg_toj_reconcile_of_left_joined_imv_keeps_partitioned_dependent() {
    Spi::run(
        "CREATE TABLE toj6_anchor (plan INT NOT NULL, id INT NOT NULL, product_id INT NOT NULL, \
         location_id INT NOT NULL, qty INT) PARTITION BY LIST (plan)",
    )
    .expect("anchor");
    for plan in [1, 2] {
        Spi::run(&format!(
            "CREATE TABLE toj6_anchor_p{plan} PARTITION OF toj6_anchor FOR VALUES IN ({plan})"
        ))
        .expect("anchor partition");
    }
    Spi::run(
        "INSERT INTO toj6_anchor SELECT p, g, g % 7, g % 5, g \
         FROM generate_series(1, 100) g CROSS JOIN (VALUES (1), (2)) v(p)",
    )
    .expect("seed anchor");
    Spi::run(
        "CREATE TABLE toj6_rel (product_id INT NOT NULL, location_id INT NOT NULL, is_active BOOL, \
         PRIMARY KEY (product_id, location_id))",
    )
    .expect("rel");
    Spi::run(
        "INSERT INTO toj6_rel SELECT p, l, (p + l) % 2 = 0 \
         FROM generate_series(0, 3) p CROSS JOIN generate_series(0, 4) l",
    )
    .expect("seed rel");
    create_imv(
        "toj6_caav",
        "SELECT create_reflex_ivm('toj6_caav', \
         'SELECT product_id, location_id, is_active FROM toj6_rel', \
         'product_id, location_id', NULL, 'DEFERRED')",
    );
    create_imv(
        "toj6_sfv",
        "SELECT create_reflex_ivm('toj6_sfv', \
         'SELECT a.plan, a.id, a.product_id, a.location_id, a.qty, \
                 COALESCE(c.is_active, FALSE) AS active \
          FROM toj6_anchor a LEFT JOIN toj6_caav c \
          ON c.product_id = a.product_id AND c.location_id = a.location_id', \
         'plan, id', NULL, 'DEFERRED', NULL, ARRAY['plan'])",
    );
    let fresh = "SELECT a.plan, a.id, a.product_id, a.location_id, a.qty, \
                 COALESCE(c.is_active, FALSE) AS active \
                 FROM toj6_anchor a LEFT JOIN toj6_rel c \
                 ON c.product_id = a.product_id AND c.location_id = a.location_id";
    assert_imv_correct("toj6_sfv", fresh);

    let res = Spi::get_one::<&str>("SELECT reflex_reconcile('toj6_caav')")
        .expect("reconcile")
        .expect("reconcile result");
    assert_eq!(res, "RECONCILED");
    assert_eq!(
        toj_row_count("toj6_sfv"),
        200,
        "the rebuild's TRUNCATE deleted partitioned dependent rows before the flush"
    );
    Spi::run("SELECT reflex_flush_deferred('toj6_caav')").expect("flush");

    assert_eq!(toj_row_count("toj6_sfv"), 200);
    assert_imv_correct("toj6_sfv", fresh);
}

/// T7 — aggregate dependent: a LEFT JOIN secondary truncated keeps every group
/// (counts unchanged, the secondary-derived sum drops to zero).
#[pg_test]
fn pg_toj_truncate_left_joined_source_keeps_aggregate_groups() {
    build_toj_tables("toj7_anchor", "toj7_act");
    let sql = "SELECT a.product_id, COUNT(*) AS n, \
               SUM(CASE WHEN c.is_active THEN a.qty ELSE 0 END) AS active_qty \
               FROM toj7_anchor a LEFT JOIN toj7_act c \
               ON c.product_id = a.product_id AND c.location_id = a.location_id \
               GROUP BY a.product_id";
    let res = crate::create_reflex_ivm("toj_v7", sql, None, None, None, None);
    assert_eq!(res, "CREATE REFLEX INCREMENTAL VIEW");
    assert_imv_correct("toj_v7", sql);

    Spi::run("TRUNCATE toj7_act").expect("truncate secondary");

    assert_eq!(toj_row_count("toj_v7"), 7, "aggregate groups were deleted");
    assert_imv_correct("toj_v7", sql);
}

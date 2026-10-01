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

    assert_eq!(
        toj_row_count("toj_v1"),
        200,
        "LEFT JOIN dependent lost anchor rows"
    );
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

    assert_eq!(
        toj_row_count("toj_v2"),
        200,
        "LEFT JOIN dependent lost anchor rows"
    );
    assert_imv_correct("toj_v2", &sql);
}

/// T3 — the production chain: the nullable side is itself a DEFERRED
/// passthrough IMV that drifted and is repaired with `reflex_reconcile` (what
/// the wipe-threshold dispatch calls). Since the reconcile of an IMV with
/// dependents diffs instead of TRUNCATE + INSERT, the dependent only sees the
/// drifted key.
#[pg_test]
fn pg_toj_reconcile_chain_keeps_dependent() {
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
    // A consumer of the dependent, as in production: without one the dependent's
    // own refresh is a plain DELETE + INSERT, which is not what is measured here.
    let top_sql = "SELECT product_id, COUNT(*) AS n, \
                   SUM(CASE WHEN active THEN 1 ELSE 0 END) AS active_n \
                   FROM tojc_sfv GROUP BY product_id";
    assert_eq!(
        crate::create_reflex_ivm("tojc_top", top_sql, None, None, Some("DEFERRED"), None),
        "CREATE REFLEX INCREMENTAL VIEW"
    );

    Spi::run("SET LOCAL session_replication_role = replica").expect("bypass triggers");
    Spi::run(
        "UPDATE tojc_rel SET is_active = NOT is_active WHERE product_id = 1 AND location_id = 1",
    )
    .expect("drift the caav");
    Spi::run("SET LOCAL session_replication_role = origin").expect("restore triggers");
    let drifted_rows = Spi::get_one::<i64>(
        "SELECT count(*)::int8 FROM tojc_anchor WHERE product_id = 1 AND location_id = 1",
    )
    .expect("drifted rows")
    .expect("count");
    let changes_before = tree_xact_changes("tojc_sfv");
    let boundary = cmin_boundary("tojc_sfv");

    let res = Spi::get_one::<&str>("SELECT reflex_reconcile('tojc_caav')")
        .expect("reconcile")
        .expect("reconcile result");
    assert_eq!(res, "RECONCILED");
    assert_eq!(
        toj_row_count("tojc_sfv"),
        200,
        "the rebuild deleted rows before the flush — a failed flush makes the loss permanent"
    );
    Spi::run("SELECT reflex_flush_deferred('tojc_caav')").expect("flush");
    Spi::run("SELECT reflex_flush_deferred('tojc_sfv')").expect("flush sfv into top");
    assert_imv_correct("tojc_top", top_sql);

    assert_eq!(
        toj_row_count("tojc_sfv"),
        200,
        "LEFT JOIN dependent lost anchor rows"
    );
    assert_imv_correct("tojc_sfv", fresh);
    let growth = tree_xact_changes("tojc_sfv") - changes_before;
    assert!(
        growth <= 2 * drifted_rows,
        "dependent rewritten beyond the drifted key: {growth} changes for {drifted_rows} rows"
    );
    let rewritten = rows_rewritten_since("tojc_sfv", boundary);
    assert!(
        rewritten <= drifted_rows,
        "dependent rows rewritten beyond the drifted key: {rewritten} for {drifted_rows} rows"
    );
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
    assert!(
        toj_row_count("toj_v4") > 0,
        "fixture produced an empty inner join"
    );

    Spi::run("TRUNCATE toji_act").expect("truncate secondary");

    assert_eq!(toj_row_count("toj_v4"), 0);
    assert_imv_correct("toj_v4", sql);
}

/// T5 — the full field sequence in one transaction: an incremental change to
/// the nullable-side IMV stages a delta for the dependent, then a rebuild of
/// that IMV stages the same key again. The flush sees the key twice, and the
/// dependent must still end up complete, touching only that key's rows.
#[pg_test]
fn pg_toj_incremental_then_reconcile_chain_keeps_dependent() {
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
    // A consumer of the dependent, as in production: without one the dependent's
    // own refresh is a plain DELETE + INSERT, which is not what is measured here.
    let top_sql = "SELECT product_id, COUNT(*) AS n, \
                   SUM(CASE WHEN active THEN 1 ELSE 0 END) AS active_n \
                   FROM toj5_sfv GROUP BY product_id";
    assert_eq!(
        crate::create_reflex_ivm("toj5_top", top_sql, None, None, Some("DEFERRED"), None),
        "CREATE REFLEX INCREMENTAL VIEW"
    );
    let key_rows = Spi::get_one::<i64>(
        "SELECT count(*)::int8 FROM toj5_anchor WHERE product_id = 5 AND location_id = 1",
    )
    .expect("key rows")
    .expect("count");
    let changes_before = tree_xact_changes("toj5_sfv");
    let boundary = cmin_boundary("toj5_sfv");

    Spi::run("INSERT INTO toj5_rel VALUES (5, 1, TRUE)").expect("activate a new key");
    Spi::run("SELECT reflex_flush_deferred('toj5_rel')").expect("flush rel into caav");
    let res = Spi::get_one::<&str>("SELECT reflex_reconcile('toj5_caav')")
        .expect("reconcile")
        .expect("reconcile result");
    assert_eq!(res, "RECONCILED");
    Spi::run("SELECT reflex_flush_deferred('toj5_caav')").expect("flush caav into sfv");
    Spi::run("SELECT reflex_flush_deferred('toj5_sfv')").expect("flush sfv into top");
    assert_imv_correct("toj5_top", top_sql);

    assert_eq!(
        toj_row_count("toj5_sfv"),
        200,
        "LEFT JOIN dependent lost anchor rows"
    );
    assert_imv_correct("toj5_sfv", fresh);
    let stale = Spi::get_one::<bool>(
        "SELECT known_stale FROM public.__reflex_ivm_reference WHERE name = 'toj5_sfv'",
    )
    .expect("stale q")
    .unwrap_or(false);
    assert!(!stale, "the dependent's flush failed and was discarded");
    let growth = tree_xact_changes("toj5_sfv") - changes_before;
    assert!(
        growth <= 2 * key_rows,
        "dependent rewritten beyond key (5,1): {growth} changes for {key_rows} rows"
    );
    let rewritten = rows_rewritten_since("toj5_sfv", boundary);
    assert!(
        rewritten <= key_rows,
        "dependent rows rewritten beyond key (5,1): {rewritten} for {key_rows} rows"
    );
}

/// T6 — the production shape: the dependent is partitioned by plan, mirroring
/// a LIST-partitioned anchor, and LEFT JOINs a plain table that is truncated.
/// DEFERRED: untouched at TRUNCATE time, rebuilt by the COMMIT-time flush.
#[pg_test]
fn pg_toj_truncate_left_joined_source_keeps_partitioned_dependent() {
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
        "CREATE TABLE toj6_rel_plain (product_id INT NOT NULL, location_id INT NOT NULL, \
         is_active BOOL, PRIMARY KEY (product_id, location_id))",
    )
    .expect("rel");
    Spi::run(
        "INSERT INTO toj6_rel_plain SELECT p, l, (p + l) % 2 = 0 \
         FROM generate_series(0, 3) p CROSS JOIN generate_series(0, 4) l",
    )
    .expect("seed rel");
    let fresh = "SELECT a.plan, a.id, a.product_id, a.location_id, a.qty, \
                 COALESCE(c.is_active, FALSE) AS active \
                 FROM toj6_anchor a LEFT JOIN toj6_rel_plain c \
                 ON c.product_id = a.product_id AND c.location_id = a.location_id";
    create_imv(
        "toj6_sfv",
        &format!(
            "SELECT create_reflex_ivm('toj6_sfv', '{}', 'plan, id', NULL, 'DEFERRED', NULL, \
             ARRAY['plan'])",
            fresh.replace('\'', "''")
        ),
    );
    assert_imv_correct("toj6_sfv", fresh);
    let boundary = cmin_boundary("toj6_sfv");

    Spi::run("TRUNCATE toj6_rel_plain").expect("truncate secondary");
    assert_eq!(
        toj_row_count("toj6_sfv"),
        200,
        "the TRUNCATE deleted partitioned dependent rows before the flush"
    );
    assert_eq!(
        rows_rewritten_since("toj6_sfv", boundary),
        0,
        "DEFERRED partitioned dependent rewritten at TRUNCATE time"
    );
    Spi::run("SET CONSTRAINTS ALL IMMEDIATE").expect("commit-time flush");

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

/// DEFERRED: the dependent is untouched by the TRUNCATE itself and rebuilt
/// once at flush; a delta another source staged earlier is not applied twice.
#[pg_test]
fn pg_toj_deferred_truncate_rebuilds_once_without_double_counting() {
    build_toj_tables("tjd_anchor", "tjd_act");
    let sql = "SELECT a.product_id, COUNT(*) AS n, SUM(a.qty) AS q, \
               SUM(CASE WHEN c.is_active THEN 1 ELSE 0 END) AS active_n \
               FROM tjd_anchor a LEFT JOIN tjd_act c \
               ON c.product_id = a.product_id AND c.location_id = a.location_id \
               GROUP BY a.product_id";
    assert_eq!(
        crate::create_reflex_ivm("tjd_v", sql, None, None, Some("DEFERRED"), None),
        "CREATE REFLEX INCREMENTAL VIEW"
    );
    Spi::run("INSERT INTO tjd_anchor VALUES (1000, 1, 1, 50)").expect("anchor delta staged");
    let before = tree_xact_changes("tjd_v");
    let boundary = cmin_boundary("tjd_v");
    Spi::run("TRUNCATE tjd_act").expect("truncate secondary");
    assert_eq!(
        tree_xact_changes("tjd_v"),
        before,
        "DEFERRED dependent rewritten at TRUNCATE time"
    );
    assert_eq!(
        rows_rewritten_since("tjd_v", boundary),
        0,
        "DEFERRED dependent rewritten at TRUNCATE time"
    );
    Spi::run("SELECT reflex_flush_deferred('tjd_anchor')").expect("flush anchor");
    let after_rebuild = tree_xact_changes("tjd_v");
    let rebuilt_boundary = cmin_boundary("tjd_v");
    Spi::run("SELECT reflex_flush_deferred('tjd_act')").expect("flush act");
    assert_imv_correct("tjd_v", sql);
    assert_eq!(
        tree_xact_changes("tjd_v"),
        after_rebuild,
        "a later flush in the same transaction touched the rebuilt IMV"
    );
    assert_eq!(
        rows_rewritten_since("tjd_v", rebuilt_boundary),
        0,
        "a later flush in the same transaction rewrote the rebuilt IMV"
    );
    // One rebuild of a target without dependents deletes and re-inserts each row
    // once; a second rebuild, or the staged delta applied on top, exceeds that.
    let rebuild_writes = tree_xact_changes("tjd_v") - before;
    assert!(
        rebuild_writes <= 2 * toj_row_count("tjd_v"),
        "rebuilt more than once: {rebuild_writes} row changes"
    );
}

/// Same for a keyless passthrough dependent (duplicate rows would appear).
#[pg_test]
fn pg_toj_deferred_truncate_keyless_passthrough_no_duplicates() {
    build_toj_tables("tjk_anchor", "tjk_act");
    let sql = TOJ_SQL_IMMEDIATE
        .replace("toj_anchor", "tjk_anchor")
        .replace("toj_act", "tjk_act");
    assert_eq!(
        crate::create_reflex_ivm("tjk_v", &sql, None, None, Some("DEFERRED"), None),
        "CREATE REFLEX INCREMENTAL VIEW"
    );
    Spi::run("INSERT INTO tjk_anchor VALUES (1000, 1, 1, 50)").expect("anchor delta staged");
    let before = tree_xact_changes("tjk_v");
    let boundary = cmin_boundary("tjk_v");
    Spi::run("TRUNCATE tjk_act").expect("truncate");
    assert_eq!(
        tree_xact_changes("tjk_v"),
        before,
        "DEFERRED dependent rewritten at TRUNCATE time"
    );
    assert_eq!(
        rows_rewritten_since("tjk_v", boundary),
        0,
        "DEFERRED dependent rewritten at TRUNCATE time"
    );
    Spi::run("SELECT reflex_flush_deferred('tjk_anchor')").expect("flush anchor");
    Spi::run("SELECT reflex_flush_deferred('tjk_act')").expect("flush act");
    assert_imv_correct("tjk_v", &sql);
    let rebuild_writes = tree_xact_changes("tjk_v") - before;
    assert!(
        rebuild_writes <= 2 * toj_row_count("tjk_v"),
        "rebuilt more than once: {rebuild_writes} row changes"
    );
}

/// DEFERRED, a TRUNCATE alone in the transaction, on the IMV's only source: the
/// truncate trigger empties that source's staging delta and pending rows, yet the
/// COMMIT-time flush must still rebuild the dependent.
#[pg_test]
fn pg_toj_deferred_truncate_only_source_rebuilds_at_commit() {
    build_toj_tables("tjo_anchor", "tjo_act");
    let sql = "SELECT product_id, COUNT(*) AS n, SUM(qty) AS q FROM tjo_anchor GROUP BY product_id";
    assert_eq!(
        crate::create_reflex_ivm("tjo_v", sql, None, None, Some("DEFERRED"), None),
        "CREATE REFLEX INCREMENTAL VIEW"
    );
    Spi::run("TRUNCATE tjo_anchor").expect("truncate only source");
    assert_eq!(
        toj_row_count("tjo_v"),
        7,
        "DEFERRED dependent rewritten at TRUNCATE time"
    );
    Spi::run("SET CONSTRAINTS ALL IMMEDIATE").expect("commit-time flush");
    assert_eq!(
        toj_row_count("tjo_v"),
        0,
        "the truncate was never applied at COMMIT"
    );
    assert_imv_correct("tjo_v", sql);
}

/// DEFERRED, the IMV ignores its first source: the COMMIT-time rebuild must be
/// reached through a source the IMV does not ignore.
#[pg_test]
fn pg_toj_deferred_truncate_rebuilds_when_first_source_ignored() {
    build_toj_tables("tjg_anchor", "tjg_act");
    let sql = TOJ_SQL_IMMEDIATE
        .replace("toj_anchor", "tjg_anchor")
        .replace("toj_act", "tjg_act");
    assert_eq!(
        crate::create_reflex_ivm(
            "tjg_v",
            &sql,
            Some("id"),
            None,
            Some("DEFERRED"),
            Some("!tjg_anchor")
        ),
        "CREATE REFLEX INCREMENTAL VIEW"
    );
    let first_source = Spi::get_one::<&str>(
        "SELECT depends_on[1] FROM public.__reflex_ivm_reference WHERE name = 'tjg_v'",
    )
    .expect("depends_on")
    .expect("first source");
    assert!(
        first_source.ends_with("tjg_anchor"),
        "fixture must ignore the first source, got {first_source}"
    );
    Spi::run("TRUNCATE tjg_act").expect("truncate");
    Spi::run("SET CONSTRAINTS ALL IMMEDIATE").expect("commit-time flush");
    assert_imv_correct("tjg_v", &sql);
}

fn build_upstream_rel(rel: &str) {
    Spi::run(&format!(
        "CREATE TABLE {rel} (product_id INT NOT NULL, location_id INT NOT NULL, \
         is_active BOOL, PRIMARY KEY (product_id, location_id))"
    ))
    .expect("rel");
    Spi::run(&format!(
        "INSERT INTO {rel} SELECT p, l, (p + l) % 3 = 0 \
         FROM generate_series(0, 3) p CROSS JOIN generate_series(0, 4) l"
    ))
    .expect("seed rel");
}

fn create_deferred_passthrough(name: &str, from: &str) {
    assert_eq!(
        crate::create_reflex_ivm(
            name,
            &format!("SELECT product_id, location_id, is_active FROM {from}"),
            Some("product_id, location_id"),
            None,
            Some("DEFERRED"),
            None
        ),
        "CREATE REFLEX INCREMENTAL VIEW"
    );
}

/// `<prefix>_anchor LEFT JOIN <prefix>_act LEFT JOIN <upstream>`, and the same
/// query reading the base table `<prefix>_rel` in place of the upstream IMV.
fn upstream_join_sql(prefix: &str, upstream: &str) -> (String, String) {
    let sql = format!(
        "SELECT a.id, a.product_id, a.location_id, a.qty, \
         COALESCE(c.is_active, FALSE) AS active, COALESCE(d.is_active, FALSE) AS d_active \
         FROM {prefix}_anchor a LEFT JOIN {prefix}_act c \
         ON c.product_id = a.product_id AND c.location_id = a.location_id \
         LEFT JOIN {upstream} d ON d.product_id = a.product_id AND d.location_id = a.location_id"
    );
    let fresh = sql.replace(&format!("{upstream} d"), &format!("{prefix}_rel d"));
    (sql, fresh)
}

/// The commit-time TRUNCATE rebuild of `imv` ran (it is in the batch marker) and
/// did not fall back to rebuild-and-flag.
fn assert_truncate_rebuilt(imv: &str) {
    let rebuilt = Spi::get_one::<bool>(&format!(
        "SELECT EXISTS (SELECT 1 FROM pg_temp.__reflex_deferred_reconciled_batch WHERE name = '{imv}')"
    ))
    .expect("batch marker")
    .unwrap_or(false);
    assert!(rebuilt, "the commit-time rebuild of {imv} never ran");
    let stale = Spi::get_one::<bool>(&format!(
        "SELECT known_stale FROM public.__reflex_ivm_reference WHERE name = '{imv}'"
    ))
    .expect("stale")
    .unwrap_or(false);
    assert!(!stale, "{imv} was flagged stale");
}

/// DEFERRED: the commit-time rebuild of a truncated source's dependent must see
/// the final state of an upstream DEFERRED IMV that changes in the same
/// transaction (production: sop_forecast_view LEFT JOINs the DEFERRED caav).
#[pg_test]
fn pg_toj_deferred_truncate_then_upstream_imv_change() {
    build_toj_tables("rvw_anchor", "rvw_act");
    build_upstream_rel("rvw_rel");
    create_deferred_passthrough("rvw_d", "rvw_rel");
    let (sql, fresh) = upstream_join_sql("rvw", "rvw_d");
    assert_eq!(
        crate::create_reflex_ivm("rvw_p", &sql, Some("id"), None, Some("DEFERRED"), None),
        "CREATE REFLEX INCREMENTAL VIEW"
    );
    assert_imv_correct("rvw_p", &fresh);
    Spi::run("TRUNCATE rvw_act").expect("truncate");
    Spi::run("UPDATE rvw_rel SET is_active = NOT is_active").expect("upstream change");
    Spi::run("SET CONSTRAINTS ALL IMMEDIATE").expect("commit-time flush");
    assert_imv_correct(
        "rvw_d",
        "SELECT product_id, location_id, is_active FROM rvw_rel",
    );
    assert_imv_correct("rvw_p", &fresh);
    assert_truncate_rebuilt("rvw_p");
}

/// Control: the same sequence with DELETE instead of TRUNCATE.
#[pg_test]
fn pg_toj_control_delete_then_upstream_imv_change() {
    build_toj_tables("rvc_anchor", "rvc_act");
    build_upstream_rel("rvc_rel");
    create_deferred_passthrough("rvc_d", "rvc_rel");
    let (sql, fresh) = upstream_join_sql("rvc", "rvc_d");
    assert_eq!(
        crate::create_reflex_ivm("rvc_p", &sql, Some("id"), None, Some("DEFERRED"), None),
        "CREATE REFLEX INCREMENTAL VIEW"
    );
    assert_imv_correct("rvc_p", &fresh);
    Spi::run("DELETE FROM rvc_act").expect("delete");
    Spi::run("UPDATE rvc_rel SET is_active = NOT is_active").expect("upstream change");
    Spi::run("SET CONSTRAINTS ALL IMMEDIATE").expect("commit-time flush");
    assert_imv_correct(
        "rvc_d",
        "SELECT product_id, location_id, is_active FROM rvc_rel",
    );
    assert_imv_correct("rvc_p", &fresh);
}

/// Two upstream levels (rel → D1 → D2 → X): only D1's source is pending when X
/// would first be rebuilt, so X's direct sources alone do not show the wait.
#[pg_test]
fn pg_toj_deferred_truncate_waits_for_two_level_upstream() {
    build_toj_tables("rv2_anchor", "rv2_act");
    build_upstream_rel("rv2_rel");
    create_deferred_passthrough("rv2_d1", "rv2_rel");
    create_deferred_passthrough("rv2_d2", "rv2_d1");
    let (sql, fresh) = upstream_join_sql("rv2", "rv2_d2");
    assert_eq!(
        crate::create_reflex_ivm("rv2_p", &sql, Some("id"), None, Some("DEFERRED"), None),
        "CREATE REFLEX INCREMENTAL VIEW"
    );
    assert_imv_correct("rv2_p", &fresh);
    Spi::run("TRUNCATE rv2_act").expect("truncate");
    Spi::run("UPDATE rv2_rel SET is_active = NOT is_active").expect("upstream change");
    Spi::run("SET CONSTRAINTS ALL IMMEDIATE").expect("commit-time flush");
    assert_imv_correct(
        "rv2_d2",
        "SELECT product_id, location_id, is_active FROM rv2_rel",
    );
    assert_imv_correct("rv2_p", &fresh);
    assert_truncate_rebuilt("rv2_p");
}

/// The upstream IMV is itself waiting for a TRUNCATE rebuild: the dependent
/// must be rebuilt after it, not before.
#[pg_test]
fn pg_toj_deferred_truncate_upstream_and_dependent_both_truncated() {
    build_toj_tables("rvb_anchor", "rvb_act");
    build_upstream_rel("rvb_rel");
    create_deferred_passthrough("rvb_d", "rvb_rel");
    let (sql, fresh) = upstream_join_sql("rvb", "rvb_d");
    assert_eq!(
        crate::create_reflex_ivm("rvb_p", &sql, Some("id"), None, Some("DEFERRED"), None),
        "CREATE REFLEX INCREMENTAL VIEW"
    );
    Spi::run("TRUNCATE rvb_act").expect("truncate dependent's source");
    Spi::run("TRUNCATE rvb_rel").expect("truncate upstream's source");
    Spi::run("SET CONSTRAINTS ALL IMMEDIATE").expect("commit-time flush");
    assert_eq!(toj_row_count("rvb_d"), 0);
    assert_imv_correct("rvb_p", &fresh);
    assert_truncate_rebuilt("rvb_p");
}

/// Deferrals are bounded: once exhausted the IMV is rebuilt anyway and marked
/// stale, never left silently wrong.
#[pg_test]
fn pg_toj_deferred_truncate_rebuild_deferrals_exhausted_marks_stale() {
    build_toj_tables("rvx_anchor", "rvx_act");
    build_upstream_rel("rvx_rel");
    create_deferred_passthrough("rvx_d", "rvx_rel");
    let (sql, _) = upstream_join_sql("rvx", "rvx_d");
    assert_eq!(
        crate::create_reflex_ivm("rvx_p", &sql, Some("id"), None, Some("DEFERRED"), None),
        "CREATE REFLEX INCREMENTAL VIEW"
    );
    Spi::run("TRUNCATE rvx_act").expect("truncate");
    Spi::run("UPDATE rvx_rel SET is_active = NOT is_active").expect("upstream change");
    Spi::run("UPDATE pg_temp.__reflex_deferred_rebuild SET attempts = 64 WHERE name = 'rvx_p'")
        .expect("exhaust the deferrals");
    Spi::run("SELECT reflex_flush_deferred('rvx_anchor')").expect("flush while upstream pending");
    let (stale, reason) = Spi::get_two::<bool, String>(
        "SELECT known_stale, stale_reason FROM public.__reflex_ivm_reference WHERE name = 'rvx_p'",
    )
    .expect("registry");
    assert_eq!(
        stale,
        Some(true),
        "exhausted deferrals left the IMV unflagged"
    );
    assert!(
        reason.unwrap_or_default().contains("upstream"),
        "stale_reason must name the cause"
    );
    assert_eq!(
        toj_row_count("rvx_p"),
        200,
        "the IMV was not rebuilt on exhaustion"
    );
}

const DEFERRED_EVENT_PROBE_DDL: &str = "\
    CREATE TABLE tje_q (i INT); \
    CREATE TABLE tje_log (i INT); \
    CREATE FUNCTION tje_fire() RETURNS TRIGGER AS $f$ BEGIN \
      INSERT INTO tje_log VALUES (NEW.i); \
      IF NEW.i < 3 THEN INSERT INTO tje_q VALUES (NEW.i + 1); END IF; \
      RETURN NULL; END $f$ LANGUAGE plpgsql; \
    CREATE CONSTRAINT TRIGGER tje_t AFTER INSERT ON tje_q DEFERRABLE INITIALLY DEFERRED \
      FOR EACH ROW EXECUTE FUNCTION tje_fire()";

/// PG fires deferred events queued while deferred events fire (the rebuild's
/// re-enqueue relies on it), and fires a queued event whose row was deleted
/// (the TRUNCATE body deletes the pending row it enqueued): SET CONSTRAINTS.
#[pg_test]
fn pg_toj_pg_fires_deferred_events_queued_during_set_constraints() {
    Spi::run(DEFERRED_EVENT_PROBE_DDL).expect("probe");
    Spi::run("INSERT INTO tje_q VALUES (1), (10)").expect("queue");
    Spi::run("DELETE FROM tje_q WHERE i = 10").expect("delete a queued row");
    Spi::run("SET CONSTRAINTS ALL IMMEDIATE").expect("fire");
    let fired = Spi::get_one::<String>("SELECT string_agg(i::text, ',' ORDER BY i) FROM tje_log")
        .expect("log")
        .expect("log NULL");
    assert_eq!(fired, "1,2,3,10");
}

/// Same at a real COMMIT, and the DEFERRED truncate + upstream change scenario
/// committed for real (a remote session: a pg_test body never commits).
#[pg_test]
fn pg_toj_pg_fires_deferred_events_queued_during_commit() {
    const DBNAME: &str = "reflex_toj_commit_probe";
    probe_db_open(DBNAME);
    worker_exec(DEFERRED_EVENT_PROBE_DDL);
    worker_exec(
        "BEGIN; INSERT INTO tje_q VALUES (1), (10); DELETE FROM tje_q WHERE i = 10; COMMIT",
    );
    let fired = worker_scalar_i64(
        "SELECT (string_agg(i::text, ',' ORDER BY i) = '1,2,3,10')::int::int8 FROM tje_log",
    );

    let (sql, fresh) = upstream_join_sql("rvm", "rvm_d");
    worker_exec(
        "CREATE TABLE rvm_anchor (id INT PRIMARY KEY, product_id INT NOT NULL, \
           location_id INT NOT NULL, qty INT); \
         CREATE TABLE rvm_act (product_id INT NOT NULL, location_id INT NOT NULL, \
           is_active BOOL, PRIMARY KEY (product_id, location_id)); \
         CREATE TABLE rvm_rel (product_id INT NOT NULL, location_id INT NOT NULL, \
           is_active BOOL, PRIMARY KEY (product_id, location_id)); \
         INSERT INTO rvm_anchor SELECT g, g % 7, g % 5, g FROM generate_series(1, 200) g; \
         INSERT INTO rvm_act SELECT p, l, (p + l) % 2 = 0 \
           FROM generate_series(0, 3) p CROSS JOIN generate_series(0, 4) l; \
         INSERT INTO rvm_rel SELECT p, l, (p + l) % 3 = 0 \
           FROM generate_series(0, 3) p CROSS JOIN generate_series(0, 4) l",
    );
    worker_exec(
        "DO $mk$ BEGIN PERFORM create_reflex_ivm('rvm_d', \
           'SELECT product_id, location_id, is_active FROM rvm_rel', \
           'product_id, location_id', 'UNLOGGED', 'DEFERRED'); END $mk$",
    );
    worker_exec(&format!(
        "DO $mk$ BEGIN PERFORM create_reflex_ivm('rvm_p', {}, 'id', 'UNLOGGED', 'DEFERRED'); END $mk$",
        sql_lit(&sql)
    ));
    worker_exec("BEGIN; TRUNCATE rvm_act; UPDATE rvm_rel SET is_active = NOT is_active; COMMIT");
    let mismatches = worker_scalar_i64(&format!(
        "SELECT count(*)::int8 FROM ((SELECT * FROM rvm_p EXCEPT ALL SELECT * FROM ({fresh}) f1) \
         UNION ALL (SELECT * FROM ({fresh}) f2 EXCEPT ALL SELECT * FROM rvm_p)) o"
    ));
    probe_db_close(DBNAME);

    assert_eq!(
        fired, 1,
        "deferred events queued during COMMIT were not all fired"
    );
    assert_eq!(
        mismatches, 0,
        "committed DEFERRED truncate + upstream change left rvm_p wrong"
    );
}

/// A committed pending row nobody will flush (no queued event: inserted with
/// triggers disabled) keeps the upstream looking busy: the re-enqueued flushes
/// run out of deferrals and the IMV is rebuilt and flagged, not left wrong.
#[pg_test]
fn pg_toj_deferred_truncate_orphan_pending_row_rebuilds_and_flags() {
    const DBNAME: &str = "reflex_toj_orphan_probe";
    probe_db_open(DBNAME);
    let (sql, fresh) = upstream_join_sql("rvo", "rvo_d");
    worker_exec(
        "CREATE TABLE rvo_anchor (id INT PRIMARY KEY, product_id INT NOT NULL, \
           location_id INT NOT NULL, qty INT); \
         CREATE TABLE rvo_act (product_id INT NOT NULL, location_id INT NOT NULL, \
           is_active BOOL, PRIMARY KEY (product_id, location_id)); \
         CREATE TABLE rvo_rel (product_id INT NOT NULL, location_id INT NOT NULL, \
           is_active BOOL, PRIMARY KEY (product_id, location_id)); \
         INSERT INTO rvo_anchor SELECT g, g % 7, g % 5, g FROM generate_series(1, 200) g; \
         INSERT INTO rvo_act SELECT p, l, (p + l) % 2 = 0 \
           FROM generate_series(0, 3) p CROSS JOIN generate_series(0, 4) l; \
         INSERT INTO rvo_rel SELECT p, l, (p + l) % 3 = 0 \
           FROM generate_series(0, 3) p CROSS JOIN generate_series(0, 4) l",
    );
    worker_exec(
        "DO $mk$ BEGIN PERFORM create_reflex_ivm('rvo_d', \
           'SELECT product_id, location_id, is_active FROM rvo_rel', \
           'product_id, location_id', 'UNLOGGED', 'DEFERRED'); END $mk$",
    );
    worker_exec(&format!(
        "DO $mk$ BEGIN PERFORM create_reflex_ivm('rvo_p', {}, 'id', 'UNLOGGED', 'DEFERRED'); END $mk$",
        sql_lit(&sql)
    ));
    worker_exec(
        "BEGIN; SET LOCAL session_replication_role = replica; \
         INSERT INTO public.__reflex_deferred_pending (source_table, operation) \
           VALUES ('rvo_rel', 'UPDATE'); COMMIT",
    );
    worker_exec("BEGIN; TRUNCATE rvo_act; COMMIT");
    let mismatches = worker_scalar_i64(&format!(
        "SELECT count(*)::int8 FROM ((SELECT * FROM rvo_p EXCEPT ALL SELECT * FROM ({fresh}) f1) \
         UNION ALL (SELECT * FROM ({fresh}) f2 EXCEPT ALL SELECT * FROM rvo_p)) o"
    ));
    let flagged = worker_scalar_i64(
        "SELECT (known_stale AND stale_reason LIKE '%upstream%')::int::int8 \
         FROM public.__reflex_ivm_reference WHERE name = 'rvo_p'",
    );
    probe_db_close(DBNAME);

    assert_eq!(mismatches, 0, "exhausted deferrals did not rebuild rvo_p");
    assert_eq!(flagged, 1, "exhausted deferrals left rvo_p unflagged");
}

/// A decomposed wrapper is refused like `reconcile_one` refuses it: nothing to
/// run, and not flagged with advice (`reflex_reconcile(<wrapper>)`) that cannot work.
#[pg_test]
fn pg_toj_truncate_skips_decomposed_wrapper() {
    build_toj_tables("tjw_anchor", "tjw_act");
    assert_eq!(
        crate::create_reflex_ivm(
            "tjw_u",
            "SELECT product_id, location_id FROM tjw_anchor \
             UNION ALL SELECT product_id, location_id FROM tjw_act",
            None,
            None,
            None,
            None
        ),
        "CREATE REFLEX INCREMENTAL VIEW"
    );
    let is_wrapper = Spi::get_one::<bool>(
        "SELECT end_query = '' AND aggregations::text = '{}' \
         FROM public.__reflex_ivm_reference WHERE name = 'tjw_u'",
    )
    .expect("registry")
    .unwrap_or(false);
    assert!(is_wrapper, "fixture must register a decomposed wrapper");
    let stmts = Spi::get_one::<String>("SELECT reflex_build_truncate_sql('tjw_u')")
        .expect("build")
        .unwrap_or_default();
    assert_eq!(stmts, "");
    let stale = Spi::get_one::<bool>(
        "SELECT known_stale FROM public.__reflex_ivm_reference WHERE name = 'tjw_u'",
    )
    .expect("stale")
    .unwrap_or(false);
    assert!(!stale, "wrapper flagged stale");
    Spi::run("TRUNCATE tjw_act").expect("truncate an operand's source");
    assert_imv_correct(
        "tjw_u",
        "SELECT product_id, location_id FROM tjw_anchor \
         UNION ALL SELECT product_id, location_id FROM tjw_act",
    );
}

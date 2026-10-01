// `reflex_reconcile` of an IMV that other IMVs read must hand them the rows
// that actually changed, not a TRUNCATE.
//
// Field 2026-09-30: a bulk upsert tripped the wipe threshold on
// `current_assortment_activity_view` (178k rows); its rebuild was TRUNCATE +
// INSERT, so `sop_forecast_view` (123M rows), which LEFT JOINs it, was told
// that everything changed — first wiped, then (once the TRUNCATE handler was
// fixed) rebuilt in full inside the job's transaction. A rebuild that only
// corrected a handful of rows must cost the dependents a handful of rows.

/// The dependent's key carries the join columns, as `sop_forecast_view`'s
/// does: that is what lets pg_reflex scope a change of the joined IMV to the
/// changed keys. A key without them falls back to a full refresh per
/// statement, a separate cost tracked in untreated_bugs.
const RDA_DEPENDENT_KEY: &str = "product_id, location_id, id";

/// The diff reaches the dependent as two statements (the rows that left, the
/// rows that arrived), each rewriting at most the drifted key's rows.
const RDA_DIFF_STATEMENTS: i64 = 2;

fn rda_xact_changes(rel: &str) -> (i64, i64) {
    let row = Spi::get_two::<i64, i64>(&format!(
        "SELECT pg_stat_get_xact_tuples_inserted('{rel}'::regclass), \
                pg_stat_get_xact_tuples_deleted('{rel}'::regclass)"
    ))
    .expect("xact stats");
    (row.0.unwrap_or(0), row.1.unwrap_or(0))
}

fn rda_build(prefix: &str) {
    Spi::run(&format!(
        "CREATE TABLE {prefix}_anchor (id INT PRIMARY KEY, product_id INT NOT NULL, \
         location_id INT NOT NULL, qty INT)"
    ))
    .expect("anchor");
    Spi::run(&format!(
        "CREATE TABLE {prefix}_rel (product_id INT NOT NULL, location_id INT NOT NULL, is_active BOOL)"
    ))
    .expect("rel");
    Spi::run(&format!(
        "INSERT INTO {prefix}_anchor SELECT g, g % 7, g % 5, g FROM generate_series(1, 200) g"
    ))
    .expect("seed anchor");
    Spi::run(&format!(
        "INSERT INTO {prefix}_rel SELECT p, l, CASE WHEN p = 3 THEN NULL ELSE (p + l) % 2 = 0 END \
         FROM generate_series(0, 3) p CROSS JOIN generate_series(0, 4) l"
    ))
    .expect("seed rel");
}

fn rda_dependent_sql(prefix: &str, upstream: &str) -> String {
    format!(
        "SELECT a.id, a.product_id, a.location_id, a.qty, COALESCE(c.is_active, FALSE) AS active \
         FROM {prefix}_anchor a LEFT JOIN {upstream} c \
         ON c.product_id = a.product_id AND c.location_id = a.location_id"
    )
}

fn rda_reconcile(view: &str) {
    let res = Spi::get_one::<&str>(&format!("SELECT reflex_reconcile('{view}')"))
        .expect("reconcile")
        .expect("reconcile result");
    assert_eq!(res, "RECONCILED");
}

/// R1 — a drift of one upstream row reaches the dependent as the rows of that
/// one key, and both IMVs end correct.
fn rda_one_row_drift_case(prefix: &str, mode: &str) {
    rda_build(prefix);
    let up = format!("{prefix}_up");
    let dep = format!("{prefix}_dep");
    let res = crate::create_reflex_ivm(
        &up,
        &format!("SELECT product_id, location_id, is_active FROM {prefix}_rel"),
        Some("product_id, location_id"),
        None,
        Some(mode),
        None,
    );
    assert_eq!(res, "CREATE REFLEX INCREMENTAL VIEW");
    let res = crate::create_reflex_ivm(
        &dep,
        &rda_dependent_sql(prefix, &up),
        Some(RDA_DEPENDENT_KEY),
        None,
        Some(mode),
        None,
    );
    assert_eq!(res, "CREATE REFLEX INCREMENTAL VIEW");
    let dep_fresh = rda_dependent_sql(prefix, &format!("{prefix}_rel"));

    Spi::run(&format!(
        "UPDATE {up} SET is_active = NOT is_active WHERE product_id = 0 AND location_id = 0"
    ))
    .expect("drift one upstream row");
    if mode == "DEFERRED" {
        Spi::run(&format!("SELECT reflex_flush_deferred('{up}')")).expect("flush drift");
    }

    let (ins_before, del_before) = rda_xact_changes(&dep);
    rda_reconcile(&up);
    if mode == "DEFERRED" {
        Spi::run(&format!("SELECT reflex_flush_deferred('{up}')")).expect("flush reconcile");
    }
    let (ins_after, del_after) = rda_xact_changes(&dep);

    assert_imv_correct(&up, &format!("SELECT product_id, location_id, is_active FROM {prefix}_rel"));
    assert_imv_correct(&dep, &dep_fresh);
    let key_rows = Spi::get_one::<i64>(&format!(
        "SELECT count(*)::int8 FROM {prefix}_anchor WHERE product_id = 0 AND location_id = 0"
    ))
    .expect("q")
    .expect("v");
    assert!(
        del_after - del_before <= RDA_DIFF_STATEMENTS * key_rows
            && ins_after - ins_before <= RDA_DIFF_STATEMENTS * key_rows,
        "dependent rewritten beyond the drifted key: deleted {} / inserted {} (key has {} rows)",
        del_after - del_before,
        ins_after - ins_before,
        key_rows
    );
}

#[pg_test]
fn pg_rda_one_row_drift_reaches_dependent_as_one_key_immediate() {
    rda_one_row_drift_case("rda1", "IMMEDIATE");
}

#[pg_test]
fn pg_rda_one_row_drift_reaches_dependent_as_one_key_deferred() {
    rda_one_row_drift_case("rda2", "DEFERRED");
}

/// R2 — reconciling an IMV that is already correct (NULLs included) changes
/// nothing downstream.
#[pg_test]
fn pg_rda_reconcile_of_correct_imv_leaves_dependent_untouched() {
    rda_build("rda3");
    let res = crate::create_reflex_ivm(
        "rda3_up",
        "SELECT product_id, location_id, is_active FROM rda3_rel",
        Some("product_id, location_id"),
        None,
        None,
        None,
    );
    assert_eq!(res, "CREATE REFLEX INCREMENTAL VIEW");
    let res = crate::create_reflex_ivm(
        "rda3_dep",
        &rda_dependent_sql("rda3", "rda3_up"),
        Some("id"),
        None,
        None,
        None,
    );
    assert_eq!(res, "CREATE REFLEX INCREMENTAL VIEW");

    let before = rda_xact_changes("rda3_dep");
    rda_reconcile("rda3_up");
    assert_eq!(rda_xact_changes("rda3_dep"), before, "a no-op reconcile rewrote the dependent");
    assert_imv_correct("rda3_dep", &rda_dependent_sql("rda3", "rda3_rel"));
}

/// R3 — duplicate rows: the diff is a multiset diff, restoring exactly the
/// missing copy.
#[pg_test]
fn pg_rda_reconcile_restores_exact_duplicate_multiplicity() {
    Spi::run("CREATE TABLE rda4_rel (product_id INT, tag TEXT)").expect("rel");
    Spi::run("INSERT INTO rda4_rel VALUES (1,'a'),(1,'a'),(1,'a'),(2,NULL),(2,NULL),(3,'c')")
        .expect("seed");
    let res = crate::create_reflex_ivm(
        "rda4_up",
        "SELECT product_id, tag FROM rda4_rel",
        None,
        None,
        None,
        None,
    );
    assert_eq!(res, "CREATE REFLEX INCREMENTAL VIEW");
    let res = crate::create_reflex_ivm(
        "rda4_dep",
        "SELECT product_id, COUNT(*) AS n FROM rda4_up GROUP BY product_id",
        None,
        None,
        None,
        None,
    );
    assert_eq!(res, "CREATE REFLEX INCREMENTAL VIEW");

    Spi::run(
        "DELETE FROM rda4_up WHERE ctid IN \
         (SELECT ctid FROM rda4_up WHERE product_id = 1 LIMIT 1)",
    )
    .expect("drop one duplicate");
    Spi::run(
        "DELETE FROM rda4_up WHERE ctid IN \
         (SELECT ctid FROM rda4_up WHERE product_id = 2 LIMIT 1)",
    )
    .expect("drop one NULL duplicate");
    Spi::run("INSERT INTO rda4_up VALUES (3,'c')").expect("add a phantom duplicate");

    rda_reconcile("rda4_up");

    assert_imv_correct("rda4_up", "SELECT product_id, tag FROM rda4_rel");
    assert_imv_correct(
        "rda4_dep",
        "SELECT product_id, COUNT(*) AS n FROM rda4_rel GROUP BY product_id",
    );
}

/// R4 — an aggregate upstream with a dependent: the corrected group reaches
/// the dependent as that group only.
#[pg_test]
fn pg_rda_aggregate_upstream_reaches_dependent_as_changed_groups() {
    rda_build("rda5");
    let up_sql = "SELECT product_id, location_id, BOOL_OR(is_active) AS is_active \
                  FROM rda5_rel GROUP BY product_id, location_id";
    let res = crate::create_reflex_ivm("rda5_up", up_sql, None, None, None, None);
    assert_eq!(res, "CREATE REFLEX INCREMENTAL VIEW");
    let res = crate::create_reflex_ivm(
        "rda5_dep",
        &rda_dependent_sql("rda5", "rda5_up"),
        Some(RDA_DEPENDENT_KEY),
        None,
        None,
        None,
    );
    assert_eq!(res, "CREATE REFLEX INCREMENTAL VIEW");
    let dep_fresh = rda_dependent_sql("rda5", &format!("({up_sql})"));

    Spi::run("UPDATE rda5_up SET is_active = NOT is_active WHERE product_id = 0 AND location_id = 0")
        .expect("drift one group");
    let (ins_before, del_before) = rda_xact_changes("rda5_dep");
    rda_reconcile("rda5_up");
    let (ins_after, del_after) = rda_xact_changes("rda5_dep");

    assert_imv_correct("rda5_up", up_sql);
    assert_imv_correct("rda5_dep", &dep_fresh);
    let key_rows = Spi::get_one::<i64>(
        "SELECT count(*)::int8 FROM rda5_anchor WHERE product_id = 0 AND location_id = 0",
    )
    .expect("q")
    .expect("v");
    assert!(
        del_after - del_before <= RDA_DIFF_STATEMENTS * key_rows
            && ins_after - ins_before <= RDA_DIFF_STATEMENTS * key_rows,
        "dependent rewritten beyond the drifted group: deleted {} / inserted {}",
        del_after - del_before,
        ins_after - ins_before
    );
}

/// R5 — control: an IMV nobody reads still rebuilds correctly (fast path).
#[pg_test]
fn pg_rda_reconcile_without_dependents_still_correct() {
    rda_build("rda6");
    let sql = "SELECT product_id, location_id, is_active FROM rda6_rel";
    let res = crate::create_reflex_ivm("rda6_up", sql, Some("product_id, location_id"), None, None, None);
    assert_eq!(res, "CREATE REFLEX INCREMENTAL VIEW");
    Spi::run("DELETE FROM rda6_up WHERE product_id = 1").expect("drift");
    rda_reconcile("rda6_up");
    assert_imv_correct("rda6_up", sql);
}

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

/// An aggregate IMV has no unique index, so its rebuild diff is whole-row: a
/// changed group reaches the dependent as DELETE + INSERT, and the dependent's
/// own maintenance rewrites the key's rows once per statement (delete + insert
/// each), i.e. twice the keyed-UPDATE cost. Still O(key rows), not O(IMV).
const AGG_WHOLE_ROW_DIFF_FACTOR: i64 = 2;

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

    let boundary = cmin_boundary(&dep);
    rda_reconcile(&up);
    if mode == "DEFERRED" {
        Spi::run(&format!("SELECT reflex_flush_deferred('{up}')")).expect("flush reconcile");
    }
    let rewritten = rows_rewritten_since(&dep, boundary);

    assert_imv_correct(
        &up,
        &format!("SELECT product_id, location_id, is_active FROM {prefix}_rel"),
    );
    assert_imv_correct(&dep, &dep_fresh);
    let key_rows = Spi::get_one::<i64>(&format!(
        "SELECT count(*)::int8 FROM {prefix}_anchor WHERE product_id = 0 AND location_id = 0"
    ))
    .expect("q")
    .expect("v");
    assert!(
        (1..=RDA_DIFF_STATEMENTS * key_rows).contains(&rewritten),
        "dependent must receive the drifted key's rows and no more: rewritten {} (key has {} rows)",
        rewritten,
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

    let boundary = cmin_boundary("rda3_dep");
    rda_reconcile("rda3_up");
    assert_eq!(
        rows_rewritten_since("rda3_dep", boundary),
        0,
        "a no-op reconcile rewrote the dependent"
    );
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

    Spi::run(
        "UPDATE rda5_up SET is_active = NOT is_active WHERE product_id = 0 AND location_id = 0",
    )
    .expect("drift one group");
    let boundary = cmin_boundary("rda5_dep");
    rda_reconcile("rda5_up");
    let rewritten = rows_rewritten_since("rda5_dep", boundary);

    assert_imv_correct("rda5_up", up_sql);
    assert_imv_correct("rda5_dep", &dep_fresh);
    let key_rows = Spi::get_one::<i64>(
        "SELECT count(*)::int8 FROM rda5_anchor WHERE product_id = 0 AND location_id = 0",
    )
    .expect("q")
    .expect("v");
    assert!(
        (1..=AGG_WHOLE_ROW_DIFF_FACTOR * RDA_DIFF_STATEMENTS * key_rows).contains(&rewritten),
        "dependent must receive the drifted group's rows and no more: rewritten {}",
        rewritten
    );
}

/// R5 — control: an IMV nobody reads still rebuilds correctly (fast path).
#[pg_test]
fn pg_rda_reconcile_without_dependents_still_correct() {
    rda_build("rda6");
    let sql = "SELECT product_id, location_id, is_active FROM rda6_rel";
    let res = crate::create_reflex_ivm(
        "rda6_up",
        sql,
        Some("product_id, location_id"),
        None,
        None,
        None,
    );
    assert_eq!(res, "CREATE REFLEX INCREMENTAL VIEW");
    Spi::run("DELETE FROM rda6_up WHERE product_id = 1").expect("drift");
    rda_reconcile("rda6_up");
    // TRUNCATE resets the transaction's tuple counters, so after the fast path
    // the counter holds exactly the refill; a diff would add to the seed's.
    let refilled = tree_xact_changes("rda6_up");
    let total = Spi::get_one::<i64>("SELECT count(*)::int8 FROM rda6_rel")
        .unwrap()
        .unwrap();
    assert_eq!(
        refilled, total,
        "an IMV with no dependents must keep the TRUNCATE + refill fast path (a diff would touch only the drifted rows)"
    );
    assert_imv_correct("rda6_up", sql);
}

/// A LIST(plan)-partitioned source and a partitioned passthrough IMV over it.
fn rda_build_partitioned(prefix: &str) {
    Spi::run(&format!(
        "CREATE TABLE {prefix}_src (plan INT NOT NULL, id INT NOT NULL, product_id INT NOT NULL, \
         qty INT) PARTITION BY LIST (plan)"
    ))
    .expect("src");
    for plan in [1, 2, 3] {
        Spi::run(&format!(
            "CREATE TABLE {prefix}_src_p{plan} PARTITION OF {prefix}_src FOR VALUES IN ({plan})"
        ))
        .expect("src partition");
    }
    Spi::run(&format!(
        "INSERT INTO {prefix}_src SELECT p, g, g % 10, g \
         FROM generate_series(1, 100) g CROSS JOIN (VALUES (1), (2), (3)) v(p)"
    ))
    .expect("seed src");
    create_imv(
        &format!("{prefix}_up"),
        &format!(
            "SELECT create_reflex_ivm('{prefix}_up', \
             'SELECT plan, id, product_id, qty FROM {prefix}_src', \
             'plan, id', NULL, 'IMMEDIATE', NULL, ARRAY['plan'])"
        ),
    );
}

/// P1 — whole-IMV reconcile of a partitioned IMV: an aggregate dependent sees
/// only the drifted group change, not a rebuild.
#[pg_test]
fn pg_rda_partitioned_reconcile_reaches_aggregate_dependent_as_delta() {
    rda_build_partitioned("rdp1");
    let dep_sql =
        "SELECT product_id, SUM(qty) AS q, COUNT(*) AS n FROM rdp1_up GROUP BY product_id";
    let res = crate::create_reflex_ivm("rdp1_dep", dep_sql, None, None, None, None);
    assert_eq!(res, "CREATE REFLEX INCREMENTAL VIEW");
    let fresh = "SELECT product_id, SUM(qty) AS q, COUNT(*) AS n FROM rdp1_src GROUP BY product_id";

    Spi::run("UPDATE rdp1_up SET qty = qty + 1000 WHERE plan = 2 AND id = 7").expect("drift");
    let boundary = cmin_boundary("rdp1_dep");
    rda_reconcile("rdp1_up");
    let rewritten = rows_rewritten_since("rdp1_dep", boundary);

    assert_imv_correct("rdp1_up", "SELECT plan, id, product_id, qty FROM rdp1_src");
    assert_imv_correct("rdp1_dep", fresh);
    assert!(
        (1..=RDA_DIFF_STATEMENTS).contains(&rewritten),
        "aggregate dependent must receive the one-group delta, no rebuild: rewritten {}",
        rewritten
    );
}

/// P2 — partition-scoped reconcile: a dependent partitioned on the same column
/// receives the drifted row, not a refill of the partition.
#[pg_test]
fn pg_rda_partition_reconcile_reaches_partitioned_dependent_as_delta() {
    rda_build_partitioned("rdp2");
    create_imv(
        "rdp2_dep",
        "SELECT create_reflex_ivm('rdp2_dep', \
         'SELECT plan, id, qty * 2 AS q2 FROM rdp2_up', \
         'plan, id', NULL, 'IMMEDIATE', NULL, ARRAY['plan'])",
    );
    let fresh = "SELECT plan, id, qty * 2 AS q2 FROM rdp2_src";

    Spi::run("UPDATE rdp2_up SET qty = qty + 1000 WHERE plan = 2 AND id = 7").expect("drift");
    let boundary = cmin_boundary("rdp2_dep");
    let res = Spi::get_one::<String>("SELECT reflex_reconcile_partition('rdp2_up', '2')")
        .expect("reconcile_partition")
        .expect("result");
    assert!(res.starts_with("RECONCILED"), "{res}");
    let rewritten = rows_rewritten_since("rdp2_dep", boundary);

    assert_imv_correct("rdp2_up", "SELECT plan, id, product_id, qty FROM rdp2_src");
    assert_imv_correct("rdp2_dep", fresh);
    assert!(
        (1..=RDA_DIFF_STATEMENTS).contains(&rewritten),
        "partitioned dependent must receive the one-row delta, no refill: rewritten {}",
        rewritten
    );
}

/// P3 — a dependent that IGNORES the reconciled IMV sees none of its writes,
/// so it must still be refreshed by the cascade (forecast_analysis_view
/// ignores sop_forecast_view in production).
#[pg_test]
fn pg_rda_partitioned_reconcile_still_refreshes_ignoring_dependent() {
    rda_build_partitioned("rdp3");
    // Drift BEFORE the dependents exist so they materialise the drifted rows;
    // only the reconcile's cascade can then correct the ignoring one.
    Spi::run("UPDATE rdp3_up SET qty = qty + 1000 WHERE plan = 2 AND id = 7").expect("drift");
    let dep_sql = "SELECT plan, SUM(qty) AS q FROM rdp3_up GROUP BY plan";
    create_imv(
        "rdp3_dep",
        &format!(
            "SELECT create_reflex_ivm('rdp3_dep', '{dep_sql}', NULL, NULL, 'IMMEDIATE', '!rdp3_up', ARRAY['plan'])"
        ),
    );
    let fresh = "SELECT plan, SUM(qty) AS q FROM rdp3_src GROUP BY plan";

    rda_reconcile("rdp3_up");

    assert_imv_correct("rdp3_up", "SELECT plan, id, product_id, qty FROM rdp3_src");
    assert_imv_correct("rdp3_dep", fresh);
}

/// An observing dependent gets the diff only; an ignoring dependent is refreshed by cascade.
#[pg_test]
fn pg_rda_ignoring_and_observing_dependents_on_one_rebuild() {
    rbd_build_rel("rdi1");
    assert_eq!(
        crate::create_reflex_ivm(
            "rdi1_up",
            "SELECT product_id, location_id, is_active FROM rdi1_rel",
            Some("product_id, location_id"),
            None,
            None,
            None
        ),
        "CREATE REFLEX INCREMENTAL VIEW"
    );
    // Drift BEFORE the dependents exist so both materialise the drifted rows;
    // only the reconcile can then correct the ignoring one.
    Spi::run(
        "UPDATE rdi1_up SET is_active = NOT is_active WHERE product_id = 0 AND location_id = 0",
    )
    .expect("drift");
    assert_eq!(
        crate::create_reflex_ivm(
            "rdi1_obs",
            &rbd_dep_sql("rdi1", "rdi1_up"),
            Some("product_id, location_id, id"),
            None,
            None,
            None
        ),
        "CREATE REFLEX INCREMENTAL VIEW"
    );
    create_imv("rdi1_ign", &format!(
        "SELECT create_reflex_ivm('rdi1_ign', $q${}$q$, 'product_id, location_id, id', NULL, 'IMMEDIATE', '!rdi1_up')",
        rbd_dep_sql("rdi1", "rdi1_up")));
    let boundary = cmin_boundary("rdi1_obs");
    rda_reconcile("rdi1_up");
    assert_imv_correct("rdi1_obs", &rbd_dep_sql("rdi1", "rdi1_rel"));
    assert_imv_correct("rdi1_ign", &rbd_dep_sql("rdi1", "rdi1_rel"));
    let key_rows = Spi::get_one::<i64>(
        "SELECT count(*)::int8 FROM rdi1_anchor WHERE product_id = 0 AND location_id = 0",
    )
    .unwrap()
    .unwrap();
    assert!(
        (1..=RDA_DIFF_STATEMENTS * key_rows).contains(&rows_rewritten_since("rdi1_obs", boundary)),
        "observing dependent must get the diff only, not a cascade rebuild"
    );
}

/// Review focus #2: rebuild rows outside the leaf are never written into it.
#[pg_test]
fn pg_rda_partition_leaf_rebuild_stays_in_leaf() {
    rda_build_partitioned("rdp4");
    create_imv("rdp4_dep", "SELECT create_reflex_ivm('rdp4_dep', 'SELECT plan, id, qty FROM rdp4_up', 'plan, id', NULL, 'IMMEDIATE', NULL, ARRAY['plan'])");
    Spi::run("UPDATE rdp4_up SET qty = qty + 1 WHERE plan = 2 AND id = 3").expect("drift");
    let res = Spi::get_one::<String>("SELECT reflex_reconcile_partition('rdp4_up', '2')")
        .unwrap()
        .unwrap();
    assert!(res.starts_with("RECONCILED"), "{res}");
    for p in [1, 3] {
        let n = Spi::get_one::<i64>(&format!(
            "SELECT count(*)::int8 FROM rdp4_up WHERE plan = {p}"
        ))
        .unwrap()
        .unwrap();
        assert_eq!(n, 100, "plan {p} touched by a plan-2 rebuild");
    }
    assert_imv_correct("rdp4_up", "SELECT plan, id, product_id, qty FROM rdp4_src");
    assert_imv_correct("rdp4_dep", "SELECT plan, id, qty FROM rdp4_src");
}

/// ATTACH of a brand-new source partition keeps today's behaviour: dependents correct.
#[pg_test]
fn pg_rda_partition_attach_new_plan_unchanged() {
    rda_build_partitioned("rdp5");
    create_imv("rdp5_dep", "SELECT create_reflex_ivm('rdp5_dep', 'SELECT plan, SUM(qty) AS q FROM rdp5_up GROUP BY plan', NULL, NULL, 'IMMEDIATE', NULL, ARRAY['plan'])");
    Spi::run("CREATE TABLE rdp5_src_p4 (LIKE rdp5_src)").expect("new");
    Spi::run("INSERT INTO rdp5_src_p4 SELECT 4, g, g % 10, g FROM generate_series(1, 100) g")
        .expect("fill");
    Spi::run("ALTER TABLE rdp5_src ATTACH PARTITION rdp5_src_p4 FOR VALUES IN (4)")
        .expect("attach");
    Spi::run("SELECT reflex_flush_partitions()").expect("flush partitions");
    assert_imv_correct("rdp5_up", "SELECT plan, id, product_id, qty FROM rdp5_src");
    assert_imv_correct(
        "rdp5_dep",
        "SELECT plan, SUM(qty) AS q FROM rdp5_src GROUP BY plan",
    );
}

/// Review focus #3 (partitioned): on one partitioned rebuild the observing
/// dependent gets the diff only, the ignoring one is refreshed by the cascade.
#[pg_test]
fn pg_rda_partitioned_reconcile_observing_and_ignoring_dependents() {
    rda_build_partitioned("rdp6");
    // Drift BEFORE the dependents exist so they materialise the drifted rows;
    // only the reconcile's cascade can then correct the ignoring one.
    Spi::run("UPDATE rdp6_up SET qty = qty + 1000 WHERE plan = 2 AND id = 7").expect("drift");
    let dep_sql = "SELECT plan, SUM(qty) AS q FROM rdp6_up GROUP BY plan";
    create_imv(
        "rdp6_dep",
        &format!(
            "SELECT create_reflex_ivm('rdp6_dep', '{dep_sql}', NULL, NULL, 'IMMEDIATE', '!rdp6_up', ARRAY['plan'])"
        ),
    );
    create_imv(
        "rdp6_obs",
        &format!(
            "SELECT create_reflex_ivm('rdp6_obs', '{dep_sql}', NULL, NULL, 'IMMEDIATE', NULL, ARRAY['plan'])"
        ),
    );
    let fresh = "SELECT plan, SUM(qty) AS q FROM rdp6_src GROUP BY plan";

    let boundary = cmin_boundary("rdp6_obs");
    rda_reconcile("rdp6_up");
    let rewritten = rows_rewritten_since("rdp6_obs", boundary);

    assert_imv_correct("rdp6_up", "SELECT plan, id, product_id, qty FROM rdp6_src");
    assert_imv_correct("rdp6_dep", fresh);
    assert_imv_correct("rdp6_obs", fresh);
    assert!(
        (1..=RDA_DIFF_STATEMENTS).contains(&rewritten),
        "observing dependent must get the diff only, not a cascade rebuild: rewritten {}",
        rewritten
    );
}

/// A whole-IMV reconcile over a tree with an EMPTY leaf that stays empty
/// changed nothing in that leaf, so it must not demote the reconcile to a full
/// cascade: the observing dependent still sees only the drifted group.
#[pg_test]
fn pg_rda_partitioned_reconcile_with_empty_leaf_reaches_dependent_as_delta() {
    Spi::run(
        "CREATE TABLE rdp7_src (plan INT NOT NULL, id INT NOT NULL, product_id INT NOT NULL, \
         qty INT) PARTITION BY LIST (plan)",
    )
    .expect("src");
    for plan in [1, 2, 3, 4] {
        Spi::run(&format!(
            "CREATE TABLE rdp7_src_p{plan} PARTITION OF rdp7_src FOR VALUES IN ({plan})"
        ))
        .expect("src partition");
    }
    Spi::run(
        "INSERT INTO rdp7_src SELECT p, g, g % 10, g \
         FROM generate_series(1, 100) g CROSS JOIN (VALUES (1), (2), (3)) v(p)",
    )
    .expect("seed src, plan 4 left empty");
    create_imv(
        "rdp7_up",
        "SELECT create_reflex_ivm('rdp7_up', 'SELECT plan, id, product_id, qty FROM rdp7_src', \
         'plan, id', NULL, 'IMMEDIATE', NULL, ARRAY['plan'])",
    );
    let dep_sql =
        "SELECT product_id, SUM(qty) AS q, COUNT(*) AS n FROM rdp7_up GROUP BY product_id";
    let res = crate::create_reflex_ivm("rdp7_dep", dep_sql, None, None, None, None);
    assert_eq!(res, "CREATE REFLEX INCREMENTAL VIEW");
    let fresh = "SELECT product_id, SUM(qty) AS q, COUNT(*) AS n FROM rdp7_src GROUP BY product_id";

    Spi::run("UPDATE rdp7_up SET qty = qty + 1000 WHERE plan = 2 AND id = 7").expect("drift");
    let boundary = cmin_boundary("rdp7_dep");
    rda_reconcile("rdp7_up");
    let rewritten = rows_rewritten_since("rdp7_dep", boundary);

    assert_imv_correct("rdp7_up", "SELECT plan, id, product_id, qty FROM rdp7_src");
    assert_imv_correct("rdp7_dep", fresh);
    assert!(
        (1..=RDA_DIFF_STATEMENTS).contains(&rewritten),
        "an empty leaf must not force a full cascade: rewritten {}",
        rewritten
    );
}

/// A partitioned AGGREGATE IMV (intermediate + end_query) rebuilds its
/// populated leaf through the diff too: the dependent sees the drifted group.
#[pg_test]
fn pg_rda_partitioned_aggregate_reconcile_reaches_dependent_as_delta() {
    rda_build_partitioned("rdp8");
    create_imv(
        "rdp8_agg",
        "SELECT create_reflex_ivm('rdp8_agg', \
         'SELECT plan, product_id, SUM(qty) AS q FROM rdp8_src GROUP BY plan, product_id', \
         NULL, NULL, 'IMMEDIATE', NULL, ARRAY['plan'])",
    );
    let res = crate::create_reflex_ivm(
        "rdp8_dep",
        "SELECT product_id, SUM(q) AS q FROM rdp8_agg GROUP BY product_id",
        None,
        None,
        None,
        None,
    );
    assert_eq!(res, "CREATE REFLEX INCREMENTAL VIEW");

    Spi::run("UPDATE rdp8_agg SET q = q + 1000 WHERE plan = 2 AND product_id = 7").expect("drift");
    let boundary = cmin_boundary("rdp8_dep");
    rda_reconcile("rdp8_agg");
    let rewritten = rows_rewritten_since("rdp8_dep", boundary);

    assert_imv_correct(
        "rdp8_agg",
        "SELECT plan, product_id, SUM(qty) AS q FROM rdp8_src GROUP BY plan, product_id",
    );
    assert_imv_correct(
        "rdp8_dep",
        "SELECT product_id, SUM(qty) AS q FROM rdp8_src GROUP BY product_id",
    );
    assert!(
        (1..=RDA_DIFF_STATEMENTS).contains(&rewritten),
        "aggregate dependent must receive the one-group delta: rewritten {}",
        rewritten
    );
}

/// Two-level mirror: the leaf diff is scoped by the leaf's FULL constraint
/// (both levels), so sibling leaves stay untouched and the dependent sees the
/// drifted row only.
#[pg_test]
fn pg_rda_two_level_leaf_rebuild_reaches_dependent_as_delta() {
    Spi::run(
        "CREATE TABLE rdp9_src (plan INT NOT NULL, region INT NOT NULL, id INT NOT NULL, \
         qty INT) PARTITION BY LIST (plan)",
    )
    .expect("src");
    for plan in [1, 2] {
        Spi::run(&format!(
            "CREATE TABLE rdp9_src_p{plan} PARTITION OF rdp9_src FOR VALUES IN ({plan}) \
             PARTITION BY LIST (region)"
        ))
        .expect("branch");
        for region in [1, 2] {
            Spi::run(&format!(
                "CREATE TABLE rdp9_src_p{plan}_r{region} PARTITION OF rdp9_src_p{plan} \
                 FOR VALUES IN ({region})"
            ))
            .expect("leaf");
        }
    }
    Spi::run(
        "INSERT INTO rdp9_src SELECT p, r, g, g FROM generate_series(1, 50) g \
         CROSS JOIN (VALUES (1), (2)) v(p) CROSS JOIN (VALUES (1), (2)) w(r)",
    )
    .expect("seed");
    create_imv(
        "rdp9_up",
        "SELECT create_reflex_ivm('rdp9_up', 'SELECT plan, region, id, qty FROM rdp9_src', \
         'plan, region, id', NULL, 'IMMEDIATE', NULL, ARRAY['plan', 'region'])",
    );
    let res = crate::create_reflex_ivm(
        "rdp9_dep",
        "SELECT region, SUM(qty) AS q, COUNT(*) AS n FROM rdp9_up GROUP BY region",
        None,
        None,
        None,
        None,
    );
    assert_eq!(res, "CREATE REFLEX INCREMENTAL VIEW");

    Spi::run("UPDATE rdp9_up SET qty = qty + 1000 WHERE plan = 2 AND region = 1 AND id = 7")
        .expect("drift");
    let up_boundary = cmin_boundary("rdp9_up");
    let boundary = cmin_boundary("rdp9_dep");
    rda_reconcile("rdp9_up");

    assert_imv_correct("rdp9_up", "SELECT plan, region, id, qty FROM rdp9_src");
    assert_imv_correct(
        "rdp9_dep",
        "SELECT region, SUM(qty) AS q, COUNT(*) AS n FROM rdp9_src GROUP BY region",
    );
    assert_eq!(
        rows_rewritten_since("rdp9_up", up_boundary),
        1,
        "only the drifted row of leaf (2, 1) is rewritten"
    );
    let rewritten = rows_rewritten_since("rdp9_dep", boundary);
    assert!(
        (1..=RDA_DIFF_STATEMENTS).contains(&rewritten),
        "dependent must receive the one-row delta: rewritten {}",
        rewritten
    );
}

/// A DEFERRED observer of a partitioned rebuild must end exact. With
/// `empty_plan_3` one leaf is emptied behind the IMV's back, so the rebuild
/// refills it in place (invisible to the dependent) while plan 2 drifted: a
/// mix of diffed and refilled leaves would cascade a full rebuild into the
/// observer while the diff is still staged for it, applying it twice.
fn rda_partitioned_deferred_observer_case(prefix: &str, empty_plan_3: bool) {
    rda_build_partitioned(prefix);
    let dep_sql = format!(
        "SELECT product_id, SUM(qty) AS q, COUNT(*) AS n FROM {prefix}_up GROUP BY product_id"
    );
    let res = crate::create_reflex_ivm(
        &format!("{prefix}_dep"),
        &dep_sql,
        None,
        None,
        Some("DEFERRED"),
        None,
    );
    assert_eq!(res, "CREATE REFLEX INCREMENTAL VIEW");

    Spi::run(&format!(
        "UPDATE {prefix}_up SET qty = qty + 1000 WHERE plan = 2 AND id = 7"
    ))
    .expect("drift");
    if empty_plan_3 {
        Spi::run(&format!("ALTER TABLE {prefix}_up DISABLE TRIGGER USER")).expect("disable");
        Spi::run(&format!("DELETE FROM {prefix}_up WHERE plan = 3")).expect("empty plan 3");
        Spi::run(&format!("ALTER TABLE {prefix}_up ENABLE TRIGGER USER")).expect("enable");
    }
    Spi::run(&format!("SELECT reflex_flush_deferred('{prefix}_up')")).expect("flush drift");
    rda_reconcile(&format!("{prefix}_up"));
    Spi::run(&format!("SELECT reflex_flush_deferred('{prefix}_up')")).expect("flush reconcile");

    assert_imv_correct(
        &format!("{prefix}_up"),
        &format!("SELECT plan, id, product_id, qty FROM {prefix}_src"),
    );
    assert_imv_correct(
        &format!("{prefix}_dep"),
        &format!(
            "SELECT product_id, SUM(qty) AS q, COUNT(*) AS n FROM {prefix}_src GROUP BY product_id"
        ),
    );
}

#[pg_test]
fn pg_rda_partitioned_reconcile_deferred_observer_exact() {
    rda_partitioned_deferred_observer_case("rdpa", false);
}

#[pg_test]
fn pg_rda_partitioned_mixed_rebuild_deferred_observer_exact() {
    rda_partitioned_deferred_observer_case("rdpb", true);
}

/// P3 through `reflex_reconcile_partition`: the partition-scoped cascade
/// still refreshes a dependent that IGNORES the reconciled IMV.
#[pg_test]
fn pg_rda_partition_reconcile_still_refreshes_ignoring_dependent() {
    rda_build_partitioned("rdpc");
    // Drift BEFORE the dependents exist so they materialise the drifted rows;
    // only the reconcile's cascade can then correct the ignoring one.
    Spi::run("UPDATE rdpc_up SET qty = qty + 1000 WHERE plan = 2 AND id = 7").expect("drift");
    let dep_sql = "SELECT plan, SUM(qty) AS q FROM rdpc_up GROUP BY plan";
    create_imv(
        "rdpc_dep",
        &format!(
            "SELECT create_reflex_ivm('rdpc_dep', '{dep_sql}', NULL, NULL, 'IMMEDIATE', '!rdpc_up', ARRAY['plan'])"
        ),
    );

    let res = Spi::get_one::<String>("SELECT reflex_reconcile_partition('rdpc_up', '2')")
        .expect("reconcile_partition")
        .expect("result");
    assert!(res.starts_with("RECONCILED"), "{res}");

    assert_imv_correct("rdpc_up", "SELECT plan, id, product_id, qty FROM rdpc_src");
    assert_imv_correct(
        "rdpc_dep",
        "SELECT plan, SUM(qty) AS q FROM rdpc_src GROUP BY plan",
    );
}

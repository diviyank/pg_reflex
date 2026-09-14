// A partition source root at `PARTITION_FLUSH_FAILURE_CAP` is skipped by every
// flush, so no change to it reaches the IMVs built on it. On db_dev that stayed
// invisible for weeks: a full reconcile cleared `known_stale` while the root
// stayed capped, and `reflex_ivm_status` reported every dependent healthy.
//
// The IMVs here are real; only the queue counter is set directly, because driving
// five genuine drain failures is not what these tests are about.

fn cps_build() {
    Spi::run(
        "CREATE TABLE cps_src (id BIGINT, region TEXT NOT NULL, amount NUMERIC) \
         PARTITION BY LIST (region)",
    )
    .expect("source");
    Spi::run("CREATE TABLE cps_src_n PARTITION OF cps_src FOR VALUES IN ('N')").expect("leaf");
    Spi::run("INSERT INTO cps_src VALUES (1, 'N', 10)").expect("seed");

    let parent = Spi::get_one::<&str>(
        "SELECT create_reflex_ivm( \
            'cps_p', \
            'SELECT region, SUM(amount) AS total FROM cps_src GROUP BY region', \
            NULL, NULL, NULL, NULL, ARRAY['region'])",
    )
    .expect("parent imv")
    .expect("parent result");
    assert!(!parent.starts_with("ERROR"), "parent imv: {parent}");

    let child = Spi::get_one::<&str>(
        "SELECT create_reflex_ivm( \
            'cps_c', \
            'SELECT region, SUM(total) AS s FROM cps_p GROUP BY region', \
            NULL, NULL, NULL, NULL, ARRAY[]::text[])",
    )
    .expect("child imv")
    .expect("child result");
    assert!(!child.starts_with("ERROR"), "child imv: {child}");

    Spi::run("ANALYZE cps_p").expect("analyze parent");
    Spi::run("ANALYZE cps_c").expect("analyze child");
}

fn cps_queue_root(failures: i32) {
    Spi::run(&format!(
        "INSERT INTO public.__reflex_partition_pending (source_root, failures, last_error) \
         VALUES ('public.cps_src', {failures}, 'duplicate key value violates unique constraint') \
         ON CONFLICT (source_root) DO UPDATE SET failures = EXCLUDED.failures, \
                                                 last_error = EXCLUDED.last_error"
    ))
    .expect("queue the root");
}

fn cps_status(imv: &str) -> (bool, Option<String>, bool) {
    let (stale, estimate) = Spi::get_two::<bool, bool>(&format!(
        "SELECT known_stale, is_estimate FROM reflex_ivm_status() WHERE name = '{imv}'"
    ))
    .expect("status query");
    let reason = Spi::get_one::<String>(&format!(
        "SELECT stale_reason FROM reflex_ivm_status() WHERE name = '{imv}'"
    ))
    .expect("status reason query");
    (
        stale.expect("status row exists"),
        reason,
        estimate.expect("is_estimate"),
    )
}

#[pg_test]
fn cps_capped_root_marks_its_direct_dependent_stale() {
    cps_build();
    cps_queue_root(5);

    let (stale, reason, estimate) = cps_status("cps_p");
    assert!(stale, "an IMV whose source root is capped receives no changes and must report stale");
    let reason = reason.expect("a capped root must explain itself in stale_reason");
    assert!(reason.contains("public.cps_src"), "reason must name the capped root: {reason}");
    assert!(
        reason.contains("reflex_reset_partition_failures('public.cps_src')")
            && reason.contains("reflex_flush_partition_source('public.cps_src')"),
        "reason must carry the converging remedy: {reason}"
    );
    assert!(!estimate, "an anomalous IMV must report the exact row count");
}

#[pg_test]
fn cps_capped_root_marks_transitive_dependents_stale() {
    cps_build();
    cps_queue_root(5);

    let (stale, reason, _) = cps_status("cps_c");
    assert!(
        stale,
        "an IMV built on an IMV whose source is capped is just as unmaintained"
    );
    assert!(
        reason.as_deref().unwrap_or("").contains("public.cps_src"),
        "the downstream reason must still name the capped root: {reason:?}"
    );
}

#[pg_test]
fn cps_root_below_the_cap_is_not_reported() {
    cps_build();
    cps_queue_root(4);

    let (stale, _, _) = cps_status("cps_p");
    assert!(
        !stale,
        "a root below the cap is still retried at every flush; only a capped one is given up on"
    );
}

#[pg_test]
fn cps_prescribed_remedy_clears_the_report() {
    cps_build();
    cps_queue_root(5);
    assert!(cps_status("cps_p").0, "precondition: capped root reported");

    Spi::run("SELECT reflex_reset_partition_failures('public.cps_src')").expect("reset");
    Spi::run("SELECT reflex_flush_partition_source('public.cps_src')").expect("flush");

    let drained = Spi::get_one::<bool>(
        "SELECT NOT EXISTS(SELECT 1 FROM public.__reflex_partition_pending \
                            WHERE source_root = 'public.cps_src')",
    )
    .expect("pending query")
    .unwrap_or(false);
    assert!(drained, "a healthy root must drain once re-armed");

    let (p_stale, _, _) = cps_status("cps_p");
    let (c_stale, _, _) = cps_status("cps_c");
    assert!(
        !p_stale && !c_stale,
        "running the printed remedy must retire the report it printed"
    );
}

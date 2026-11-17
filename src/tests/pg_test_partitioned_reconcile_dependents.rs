// Re-verification of the 2026-07-28 report "partitioned reconcile destroys
// dependent IMVs" against every shape it measured, in one scenario: reconcile
// twice, default and 'error' alter-source policy, doctor clean afterwards.
// Reuses the fixtures/helpers of pg_test_reconcile_dependent_dataloss.rs.

fn doctor_finding_count(imv: &str) -> i64 {
    Spi::get_one::<i64>(&format!(
        "SELECT count(*)::int8 FROM reflex_doctor('{imv}')"
    ))
    .expect("doctor query")
    .expect("doctor count NULL")
}

#[pg_test]
fn pg_partitioned_reconcile_keeps_dependents_correct() {
    build_rdd_source("prd_s");
    create_imv(
        "prd_p",
        "SELECT create_reflex_ivm('prd_p', \
         'SELECT k, bucket, SUM(amt) AS total FROM prd_s GROUP BY k, bucket', \
         NULL, NULL, NULL, NULL, ARRAY['k'])",
    );
    create_imv(
        "prd_part_dep",
        "SELECT create_reflex_ivm('prd_part_dep', 'SELECT k, SUM(total) AS t FROM prd_p GROUP BY k')",
    );
    create_imv(
        "prd_plain_dep",
        "SELECT create_reflex_ivm('prd_plain_dep', \
         'SELECT bucket, SUM(total) AS t FROM prd_p GROUP BY bucket')",
    );

    Spi::run("CREATE TABLE prd_u (k TEXT NOT NULL, bucket INT NOT NULL, amt NUMERIC)")
        .expect("control source");
    Spi::run(
        "INSERT INTO prd_u SELECT v.k, (g % 5), (g % 97)::numeric \
         FROM generate_series(1, 300) g CROSS JOIN (VALUES ('A'),('B'),('C')) v(k)",
    )
    .expect("seed control");
    create_imv(
        "prd_u_p",
        "SELECT create_reflex_ivm('prd_u_p', \
         'SELECT k, bucket, SUM(amt) AS total FROM prd_u GROUP BY k, bucket')",
    );
    create_imv(
        "prd_u_dep",
        "SELECT create_reflex_ivm('prd_u_dep', 'SELECT k, SUM(total) AS t FROM prd_u_p GROUP BY k')",
    );

    let parent_agg = "SELECT k, bucket, SUM(amt) AS total FROM prd_s GROUP BY k, bucket";
    let part_dep_fresh = format!("SELECT k, SUM(total) AS t FROM ({parent_agg}) o GROUP BY k");
    let plain_dep_fresh =
        format!("SELECT bucket, SUM(total) AS t FROM ({parent_agg}) o GROUP BY bucket");
    let control_fresh = "SELECT k, SUM(total) AS t FROM \
         (SELECT k, bucket, SUM(amt) AS total FROM prd_u GROUP BY k, bucket) o GROUP BY k";

    let mirror_before = partition_child_names("prd_part_dep");
    let intermediate_mirror_before = partition_child_names("__reflex_intermediate_prd_part_dep");
    assert_eq!(mirror_before.len(), 3, "fixture: {mirror_before:?}");
    let part_dep_rows = dep_row_count("prd_part_dep");
    assert!(part_dep_rows > 0, "fixture produced an empty dependent");

    for policy in ["warn", "error", "warn"] {
        Spi::run(&format!("SET pg_reflex.alter_source_policy = '{policy}'")).expect("policy");
        Spi::run("UPDATE prd_p SET total = total + 1").expect("drift partitioned parent");
        Spi::run("UPDATE prd_u_p SET total = total + 1").expect("drift control parent");

        for parent in ["prd_p", "prd_u_p"] {
            let res = Spi::get_one::<&str>(&format!("SELECT reflex_reconcile('{parent}')"))
                .unwrap_or_else(|e| panic!("reconcile of {parent} under '{policy}': {e:?}"))
                .expect("reconcile result");
            assert_eq!(res, "RECONCILED", "{parent} under '{policy}'");
        }

        assert_imv_correct("prd_p", parent_agg);
        assert_imv_correct("prd_part_dep", &part_dep_fresh);
        assert_imv_correct("prd_plain_dep", &plain_dep_fresh);
        assert_imv_correct("prd_u_dep", control_fresh);
        assert_eq!(dep_row_count("prd_part_dep"), part_dep_rows, "policy '{policy}'");

        assert_eq!(
            partition_child_names("prd_part_dep"),
            mirror_before,
            "partition mirror changed under '{policy}'"
        );
        assert_eq!(
            partition_child_names("__reflex_intermediate_prd_part_dep"),
            intermediate_mirror_before,
            "intermediate mirror changed under '{policy}'"
        );
        assert_no_swap_residue_children("prd_part_dep");
        assert_no_swap_residue_children("__reflex_intermediate_prd_part_dep");
        assert_eq!(partition_child_names("prd_plain_dep").len(), 0);
    }

    for imv in ["prd_p", "prd_part_dep", "prd_plain_dep", "prd_u_p", "prd_u_dep"] {
        assert_eq!(doctor_finding_count(imv), 0, "doctor reports findings on {imv}");
    }
}

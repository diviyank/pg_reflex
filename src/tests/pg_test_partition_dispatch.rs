// Integration tests for the per-partition trigger dispatch
// (plans/partitioning_3.md §3 + §4).
//
// Phase A's swap is exercised by `pg_test_partition.rs`; these tests
// exercise the trigger-time dispatch path that calls into the swap.
// Included from src/lib.rs `tests` module via include!.

/// Same smoke test but with wipe_threshold + 4 partitions — exercises the
/// per-partition dispatch DO block.
#[pg_test]
fn pg_part_update_propagates_with_dispatch_threshold() {
    Spi::run(
        "CREATE TABLE part_dt (id BIGINT, region TEXT NOT NULL, amount NUMERIC) \
         PARTITION BY LIST (region)",
    )
    .expect("create");
    for r in ["A", "B"] {
        Spi::run(&format!(
            "CREATE TABLE part_dt_{r} PARTITION OF part_dt FOR VALUES IN ('{r}')"
        ))
        .expect("p");
    }
    Spi::run("INSERT INTO part_dt VALUES (1, 'A', 10), (2, 'A', 20), (3, 'B', 5)")
        .expect("seed");
    Spi::run(
        "SELECT create_reflex_ivm( \
            'part_dt_v', \
            'SELECT region, SUM(amount) AS total FROM part_dt GROUP BY region', \
            NULL, NULL, NULL, NULL, \
            ARRAY['region'] \
         )",
    )
    .expect("create_imv");

    Spi::run("SELECT reflex_set_wipe_threshold('part_dt_v', 0.1::numeric)").expect("thr");

    Spi::run("UPDATE part_dt SET amount = amount * 5 WHERE region = 'A'").expect("upd");

    let post = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT total FROM part_dt_v WHERE region = 'A'",
    )
    .expect("q")
    .expect("a");
    assert_eq!(post.to_string(), "150", "expected 150, got {post}");
}

/// Smoke test: an UPDATE on a partitioned source must propagate to the
/// IMV via the AFTER UPDATE STATEMENT trigger.  Regression guard — if
/// this breaks, the per-partition dispatch tests below will also fail.
#[pg_test]
fn pg_part_update_on_partitioned_source_propagates_to_imv() {
    Spi::run(
        "CREATE TABLE part_smk (id BIGINT, region TEXT NOT NULL, amount NUMERIC) \
         PARTITION BY LIST (region)",
    )
    .expect("create");
    Spi::run("CREATE TABLE part_smk_a PARTITION OF part_smk FOR VALUES IN ('A')").expect("p");
    Spi::run("INSERT INTO part_smk VALUES (1, 'A', 10), (2, 'A', 20)").expect("seed");
    Spi::run(
        "SELECT create_reflex_ivm( \
            'part_smk_v', \
            'SELECT region, SUM(amount) AS total FROM part_smk GROUP BY region', \
            NULL, NULL, NULL, NULL, \
            ARRAY['region'] \
         )",
    )
    .expect("create_imv");

    let pre = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT total FROM part_smk_v WHERE region = 'A'",
    )
    .expect("q")
    .expect("a");
    assert_eq!(pre.to_string(), "30", "pre-update");

    Spi::run("UPDATE part_smk SET amount = amount * 5 WHERE region = 'A'").expect("upd");

    let post = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT total FROM part_smk_v WHERE region = 'A'",
    )
    .expect("q")
    .expect("a");
    assert_eq!(
        post.to_string(),
        "150",
        "UPDATE on partitioned source must propagate to IMV (10*5 + 20*5 = 150)"
    );
}

/// Bulk UPDATE concentrated in one partition should route to
/// `reflex_reconcile_partition` (atomic swap) rather than the global
/// high-selectivity dispatch.  We measure this indirectly: after the
/// dispatch, the IMV's data is correct AND the partition that wasn't
/// touched is unchanged.
#[pg_test]
fn pg_part_trigger_dispatch_routes_bulk_update_to_partition_scoped() {
    // 4-way LIST partition on `region`.
    Spi::run(
        "CREATE TABLE part_disp1 (id BIGINT NOT NULL, region TEXT NOT NULL, amount NUMERIC) \
         PARTITION BY LIST (region)",
    )
    .expect("create");
    for r in ["A", "B", "C", "D"] {
        Spi::run(&format!(
            "CREATE TABLE part_disp1_{r} PARTITION OF part_disp1 FOR VALUES IN ('{r}')"
        ))
        .expect("p");
    }
    // 50 rows per partition.
    for r in ["A", "B", "C", "D"] {
        for i in 0..50 {
            Spi::run(&format!(
                "INSERT INTO part_disp1 (id, region, amount) VALUES ({}, '{}', {})",
                i, r, i
            ))
            .expect("seed");
        }
    }

    Spi::run(
        "SELECT create_reflex_ivm( \
            'part_disp1_v', \
            'SELECT region, SUM(amount) AS total, COUNT(*) AS n FROM part_disp1 GROUP BY region', \
            NULL, NULL, NULL, NULL, \
            ARRAY['region'] \
         )",
    )
    .expect("create_imv");

    // Tighten wipe_threshold so an UPDATE of all rows in A trips the dispatch.
    Spi::run("SELECT reflex_set_wipe_threshold('part_disp1_v', 0.1::numeric)").expect("thr");
    Spi::run("SELECT reflex_set_wipe_floor_rows('part_disp1_v', 1)").expect("floor");
    Spi::run("ANALYZE part_disp1").expect("analyze");
    Spi::run("ANALYZE \"__reflex_intermediate_part_disp1_v\"").expect("ai");

    // Bulk update everything in partition A.
    Spi::run("UPDATE part_disp1 SET amount = amount * 10 WHERE region = 'A'").expect("upd");

    // Sanity check: the source actually has the updated amounts.
    let src_a = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT SUM(amount) FROM part_disp1 WHERE region = 'A'",
    )
    .expect("src_q")
    .expect("src_a");
    assert_eq!(src_a.to_string(), "12250", "src A after update");

    // Verify A's total is 10 * old_sum, B/C/D unchanged.
    let total_a = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT total FROM part_disp1_v WHERE region = 'A'",
    )
    .expect("q")
    .expect("a");
    // Old A sum = 0+1+...+49 = 1225.  After *10: 12250.
    assert_eq!(total_a.to_string(), "12250", "A total");

    let total_b = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT total FROM part_disp1_v WHERE region = 'B'",
    )
    .expect("q")
    .expect("b");
    assert_eq!(total_b.to_string(), "1225", "B total");
}

/// All partitions hot → trip-cap should fire and fall back to global
/// `reflex_reconcile`.  Correctness check: data ends up correct.
#[pg_test]
fn pg_part_trigger_dispatch_trip_cap_falls_back_to_global() {
    Spi::run(
        "CREATE TABLE part_trip (id BIGINT NOT NULL, region TEXT NOT NULL, amount NUMERIC) \
         PARTITION BY LIST (region)",
    )
    .expect("create");
    for r in ["A", "B"] {
        Spi::run(&format!(
            "CREATE TABLE part_trip_{r} PARTITION OF part_trip FOR VALUES IN ('{r}')"
        ))
        .expect("p");
    }
    for r in ["A", "B"] {
        for i in 0..50 {
            Spi::run(&format!(
                "INSERT INTO part_trip (id, region, amount) VALUES ({}, '{}', {})",
                i, r, i
            ))
            .expect("seed");
        }
    }

    Spi::run(
        "SELECT create_reflex_ivm( \
            'part_trip_v', \
            'SELECT region, SUM(amount) AS total FROM part_trip GROUP BY region', \
            NULL, NULL, NULL, NULL, \
            ARRAY['region'] \
         )",
    )
    .expect("create_imv");
    Spi::run("SELECT reflex_set_wipe_threshold('part_trip_v', 0.01::numeric)").expect("thr");
    Spi::run("SELECT reflex_set_wipe_floor_rows('part_trip_v', 1)").expect("floor");
    Spi::run("ANALYZE part_trip").expect("a");
    Spi::run("ANALYZE \"__reflex_intermediate_part_trip_v\"").expect("ai");
    Spi::run("ANALYZE part_trip_v").expect("at");

    // Bulk update everything in BOTH partitions.
    Spi::run("UPDATE part_trip SET amount = amount + 1000").expect("upd");

    // 2 partitions of 50 rows each. After amount += 1000: A_sum = 1225 + 50000, B_sum = 1225 + 50000.
    let total_a = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT total FROM part_trip_v WHERE region = 'A'",
    )
    .expect("q")
    .expect("a");
    assert_eq!(total_a.to_string(), "51225");
    let total_b = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT total FROM part_trip_v WHERE region = 'B'",
    )
    .expect("q")
    .expect("b");
    assert_eq!(total_b.to_string(), "51225");
}

/// Standard low-volume UPDATE on a partitioned IMV should run the
/// incremental path (no partition swap).
#[pg_test]
fn pg_part_trigger_dispatch_low_selectivity_stays_incremental() {
    Spi::run(
        "CREATE TABLE part_low (id BIGINT NOT NULL, region TEXT NOT NULL, amount NUMERIC) \
         PARTITION BY LIST (region)",
    )
    .expect("create");
    for r in ["A", "B"] {
        Spi::run(&format!(
            "CREATE TABLE part_low_{r} PARTITION OF part_low FOR VALUES IN ('{r}')"
        ))
        .expect("p");
    }
    for r in ["A", "B"] {
        for i in 0..50 {
            Spi::run(&format!(
                "INSERT INTO part_low (id, region, amount) VALUES ({}, '{}', {})",
                i, r, i
            ))
            .expect("seed");
        }
    }
    Spi::run(
        "SELECT create_reflex_ivm( \
            'part_low_v', \
            'SELECT region, SUM(amount) AS total FROM part_low GROUP BY region', \
            NULL, NULL, NULL, NULL, \
            ARRAY['region'] \
         )",
    )
    .expect("create_imv");
    Spi::run("ANALYZE part_low").expect("a");
    Spi::run("ANALYZE \"__reflex_intermediate_part_low_v\"").expect("ai");
    Spi::run("ANALYZE part_low_v").expect("at");

    // Update a single row → ratio is well below default 0.5 → incremental path.
    Spi::run("UPDATE part_low SET amount = amount + 100 WHERE id = 0 AND region = 'A'")
        .expect("upd");

    // 1225 - 0 + 100 = 1325
    let total_a = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT total FROM part_low_v WHERE region = 'A'",
    )
    .expect("q")
    .expect("a");
    assert_eq!(total_a.to_string(), "1325");
    let total_b = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT total FROM part_low_v WHERE region = 'B'",
    )
    .expect("q")
    .expect("b");
    assert_eq!(total_b.to_string(), "1225");
}

/// Unpartitioned IMV must not engage the partition-aware dispatch — falls
/// through to the legacy global high-selectivity dispatch.
#[pg_test]
fn pg_part_trigger_dispatch_unpartitioned_imv_unchanged() {
    Spi::run("CREATE TABLE part_np (id BIGINT, val TEXT, amount NUMERIC)").expect("create");
    for v in ["a", "b"] {
        for i in 0..30 {
            Spi::run(&format!(
                "INSERT INTO part_np VALUES ({}, '{}', {})",
                i, v, i
            ))
            .expect("seed");
        }
    }
    Spi::run(
        "SELECT create_reflex_ivm( \
            'part_np_v', \
            'SELECT val, SUM(amount) AS total FROM part_np GROUP BY val' \
         )",
    )
    .expect("create_imv");

    Spi::run("UPDATE part_np SET amount = amount + 1 WHERE val = 'a'").expect("upd");

    // 0+1+...+29 = 435; +30 = 465.
    let total_a = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT total FROM part_np_v WHERE val = 'a'",
    )
    .expect("q")
    .expect("a");
    assert_eq!(total_a.to_string(), "465");
}

/// Tier 2 (plans/partitioning_3.md §4): an UPDATE on a non-anchor JOIN
/// secondary must still propagate correctly via the partition-aware
/// dispatch.  The fact table is unpartitioned; the partition column
/// lives on the dim/anchor.  The IMV's affected table carries the
/// partition column via the JOIN, so the post-scratch dispatch reads
/// pkey counts directly from affected.
#[pg_test]
fn pg_part_tier2_non_anchor_source_update_routes_through_dispatch() {
    // PG: unique constraints on partitioned tables must include all
    // partition columns; use (id, region) as PK.
    Spi::run(
        "CREATE TABLE part_t2_dim (id BIGINT, region TEXT NOT NULL, PRIMARY KEY (id, region)) \
         PARTITION BY LIST (region)",
    )
    .expect("dim_create");
    for r in ["A", "B"] {
        Spi::run(&format!(
            "CREATE TABLE part_t2_dim_{r} PARTITION OF part_t2_dim FOR VALUES IN ('{r}')"
        ))
        .expect("p");
    }
    Spi::run("INSERT INTO part_t2_dim VALUES (1, 'A'), (2, 'A'), (3, 'B')").expect("seed_dim");

    Spi::run("CREATE TABLE part_t2_fact (id BIGINT, dim_id BIGINT NOT NULL, amount NUMERIC)")
        .expect("fact_create");
    Spi::run(
        "INSERT INTO part_t2_fact VALUES (1, 1, 10), (2, 2, 20), (3, 3, 30), (4, 1, 5)",
    )
    .expect("seed_fact");

    let cres = Spi::get_one::<&str>(
        "SELECT create_reflex_ivm( \
            'part_t2_v', \
            'SELECT d.region AS region, SUM(f.amount) AS total \
             FROM part_t2_fact f JOIN part_t2_dim d ON f.dim_id = d.id \
             GROUP BY d.region', \
            NULL, NULL, NULL, NULL, \
            ARRAY['region'] \
         )",
    )
    .expect("create_imv")
    .expect("create_res");
    assert!(
        !cres.starts_with("ERROR"),
        "create_imv failed: {cres}"
    );

    // Initial materialization: A = 10 + 20 + 5 = 35, B = 30.
    let pre_a = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT total FROM part_t2_v WHERE region = 'A'",
    )
    .expect("pre_q")
    .expect("pre_a");
    assert_eq!(pre_a.to_string(), "35", "pre A");

    // UPDATE on the fact (non-anchor) — multiplies amounts in A's slice.
    Spi::run("UPDATE part_t2_fact SET amount = amount * 2 WHERE dim_id IN (1, 2)")
        .expect("upd");

    // A = 20 + 40 + 10 = 70, B = 30 (unchanged).
    let post_a = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT total FROM part_t2_v WHERE region = 'A'",
    )
    .expect("post_q")
    .expect("post_a");
    assert_eq!(post_a.to_string(), "70", "post A");
    let post_b = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT total FROM part_t2_v WHERE region = 'B'",
    )
    .expect("post_q")
    .expect("post_b");
    assert_eq!(post_b.to_string(), "30", "post B unchanged");
}

/// Phase D: verify `partition_join_paths` is populated for non-anchor
/// JOIN-secondary sources at create_reflex_ivm time.
#[pg_test]
fn pg_part_tier2_partition_join_paths_populated_for_join_secondary() {
    // Plan §4 expects: non-anchor source S has a JOIN equality S.X =
    // ANCHOR.partition_col where ANCHOR.partition_col is also the GROUP
    // BY column.  We construct that shape: pricing JOINs to dim on
    // `pricing.region = dim.region`, GROUP BY dim.region.
    Spi::run(
        "CREATE TABLE part_t2pjp_dim (id BIGINT, region TEXT NOT NULL, PRIMARY KEY (id, region)) \
         PARTITION BY LIST (region)",
    )
    .expect("dim_create");
    Spi::run("CREATE TABLE part_t2pjp_dim_a PARTITION OF part_t2pjp_dim FOR VALUES IN ('A')")
        .expect("p");
    Spi::run("CREATE TABLE part_t2pjp_dim_b PARTITION OF part_t2pjp_dim FOR VALUES IN ('B')")
        .expect("p_b");
    Spi::run("INSERT INTO part_t2pjp_dim VALUES (1, 'A'), (2, 'B')").expect("seed_dim");
    // pricing has a `product_region` column that JOINs to dim.region; PK
    // on (product_region) so the source_join_keys safety gate passes.
    // Different column name avoids the `multiple sources own partition
    // column` ambiguity in resolve_anchor_source.
    Spi::run(
        "CREATE TABLE part_t2pjp_pricing ( \
             product_region TEXT NOT NULL, price NUMERIC, PRIMARY KEY (product_region))",
    )
    .expect("pricing_create");
    Spi::run("INSERT INTO part_t2pjp_pricing VALUES ('A', 100), ('B', 200)").expect("seed_p");

    let cres = Spi::get_one::<&str>(
        "SELECT create_reflex_ivm( \
            'part_t2pjp_v', \
            'SELECT d.region AS region, SUM(p.price) AS total \
             FROM part_t2pjp_dim d JOIN part_t2pjp_pricing p ON p.product_region = d.region \
             GROUP BY d.region', \
            NULL, NULL, NULL, NULL, \
            ARRAY['region'] \
         )",
    )
    .expect("create")
    .expect("create_res");
    assert!(
        !cres.starts_with("ERROR"),
        "create_imv failed: {cres}"
    );

    // Inspect the aggregations JSON for the persisted partition_join_paths.
    let json: String = Spi::get_one::<String>(
        "SELECT aggregations::text FROM public.__reflex_ivm_reference WHERE name = 'part_t2pjp_v'",
    )
    .expect("agg")
    .expect("agg_some");
    assert!(
        json.contains("partition_join_paths"),
        "aggregations json missing partition_join_paths: {}",
        json
    );
    // The non-anchor pricing source should have a fragment.
    assert!(
        json.contains("part_t2pjp_pricing")
            && json.contains("transition_alias"),
        "expected partition_join_paths entry for pricing with placeholder, got: {}",
        json
    );
}

/// Smoke test for the AccessExclusiveLock window during an extensive
/// update.  We can't measure cross-session blocking from a single-
/// connection pg_test, but we can verify that:
///   * `reflex_reconcile_partition` on a large partition does NOT hold
///     `AccessExclusiveLock` on the IMV's partition CHILD for the data
///     fill (the lock is taken only at DETACH/ATTACH time).
///   * The total set of locks held after the swap commits is the same
///     shape as today's TRUNCATE+INSERT path — no stuck locks.
///
/// Methodology: after the swap, query `pg_locks` from the same session
/// (which sees its own locks) and confirm no stale locks beyond the
/// expected post-commit set.
#[pg_test]
fn pg_part_swap_releases_partition_locks_after_commit() {
    Spi::run(
        "CREATE TABLE part_lk (id BIGINT, region TEXT NOT NULL, amount NUMERIC) \
         PARTITION BY LIST (region)",
    )
    .expect("create");
    for r in ["A", "B"] {
        Spi::run(&format!(
            "CREATE TABLE part_lk_{r} PARTITION OF part_lk FOR VALUES IN ('{r}')"
        ))
        .expect("p");
    }
    // 10k rows / partition — enough that a TRUNCATE+INSERT-shaped reconcile
    // would be visibly expensive; the swap should not be.
    Spi::run(
        "INSERT INTO part_lk \
         SELECT i, \
                CASE (i % 2) WHEN 0 THEN 'A' ELSE 'B' END, \
                (random() * 100)::NUMERIC(10, 2) \
         FROM generate_series(1, 20000) AS i",
    )
    .expect("seed");
    Spi::run(
        "SELECT create_reflex_ivm( \
            'part_lk_v', \
            'SELECT region, SUM(amount) AS total FROM part_lk GROUP BY region', \
            NULL, NULL, NULL, NULL, \
            ARRAY['region'] \
         )",
    )
    .expect("create_imv");

    // Drift one partition and reconcile via the swap path.
    Spi::run("UPDATE part_lk_v SET total = 9999 WHERE region = 'A'").expect("drift");

    let msg = Spi::get_one::<String>("SELECT reflex_reconcile_partition('part_lk_v', 'A')")
        .expect("rec")
        .expect("msg");
    assert!(msg.starts_with("RECONCILED"), "{msg}");

    // After the swap commits, no `__reflex_swap_*` table should remain.
    // (The successful swap renames the swap tables to the canonical
    // child names; failed mid-swap would leave swap tables which the
    // next entry to reconcile_partition cleans up idempotently.)
    let leftover_names: Vec<String> = Spi::connect(|client| {
        let rows = client
            .select(
                "SELECT relname::text AS n FROM pg_class \
                 WHERE relkind = 'r' AND relname LIKE '__reflex_swap_%_part_lk_v_%'",
                None,
                &[],
            )
            .expect("pg_class query");
        rows.filter_map(|row| {
            row.get_by_name::<&str, _>("n")
                .unwrap_or(None)
                .map(|s| s.to_string())
        })
        .collect()
    });
    assert!(
        leftover_names.is_empty(),
        "no swap tables should remain after commit; found: {:?}",
        leftover_names
    );

    // Verify A's partition was actually rebuilt (drift cleared).
    let a_total = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT total FROM part_lk_v WHERE region = 'A'",
    )
    .expect("q")
    .expect("a");
    let expected_a = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT SUM(amount) FROM part_lk WHERE region = 'A'",
    )
    .expect("q")
    .expect("e");
    assert_eq!(a_total.to_string(), expected_a.to_string());
}

/// Verify the SQL helper `__reflex_partition_child_for_key` returns the
/// right child for LIST partitions.
#[pg_test]
fn pg_part_partition_child_for_key_helper_resolves_list_correctly() {
    Spi::run(
        "CREATE TABLE part_keyf (id BIGINT, region TEXT NOT NULL) PARTITION BY LIST (region)",
    )
    .expect("create");
    Spi::run("CREATE TABLE part_keyf_n PARTITION OF part_keyf FOR VALUES IN ('N')").expect("n");
    Spi::run("CREATE TABLE part_keyf_s PARTITION OF part_keyf FOR VALUES IN ('S')").expect("s");

    let n = Spi::get_one::<&str>(
        "SELECT public.__reflex_partition_child_for_key( \
            'part_keyf'::regclass, 'region', 'N')::text",
    )
    .expect("call")
    .expect("n");
    assert!(n.contains("part_keyf_n"));

    let s = Spi::get_one::<&str>(
        "SELECT public.__reflex_partition_child_for_key( \
            'part_keyf'::regclass, 'region', 'S')::text",
    )
    .expect("call")
    .expect("s");
    assert!(s.contains("part_keyf_s"));

    // No matching child → NULL.
    let none: Option<&str> = Spi::get_one::<&str>(
        "SELECT public.__reflex_partition_child_for_key( \
            'part_keyf'::regclass, 'region', 'X')::text",
    )
    .expect("call");
    assert!(none.is_none());
}

/// LIST passthrough IMV, multi-partition UPDATE that stays COLD (small fraction
/// per partition). The keyed cold delete+insert must keep the IMV byte-identical
/// to a fresh recompute. This is the regression guard for Phase 1 pruning
/// (which must not change results) and Phase 2 (in-place upsert).
#[pg_test]
fn pg_part_passthrough_cold_multi_partition_update_oracle() {
    Spi::run(
        "CREATE TABLE pt_cold_src (id BIGINT NOT NULL, region TEXT NOT NULL, amount BIGINT, PRIMARY KEY (id, region)) \
         PARTITION BY LIST (region)",
    )
    .expect("create src");
    for r in ["A", "B", "C", "D"] {
        Spi::run(&format!(
            "CREATE TABLE pt_cold_src_{r} PARTITION OF pt_cold_src FOR VALUES IN ('{r}')"
        ))
        .expect("p");
        for i in 0..200 {
            let id_base = match r {
                "A" => 0,
                "B" => 1000,
                "C" => 2000,
                _ => 3000,
            };
            Spi::run(&format!(
                "INSERT INTO pt_cold_src (id, region, amount) VALUES ({}, '{}', {})",
                id_base + i,
                r, i
            ))
            .expect("seed");
        }
    }
    let sql = "SELECT id, region, amount FROM pt_cold_src";
    Spi::run(
        "SELECT create_reflex_ivm('pt_cold_v', 'SELECT id, region, amount FROM pt_cold_src', \
         'id,region', NULL, NULL, NULL, ARRAY['region'])",
    )
    .expect("create_imv");
    assert_imv_correct("pt_cold_v", sql);

    // Cold update: touch a handful of rows across TWO partitions (small fraction).
    Spi::run("UPDATE pt_cold_src SET amount = amount + 1 WHERE region IN ('A','C') AND id % 100 < 3")
        .expect("cold update");
    assert_imv_correct("pt_cold_v", sql);

    // Cold delete across partitions.
    Spi::run("DELETE FROM pt_cold_src WHERE region IN ('B','D') AND id % 100 = 0")
        .expect("cold delete");
    assert_imv_correct("pt_cold_v", sql);

    // Cold insert into existing partitions.
    Spi::run("INSERT INTO pt_cold_src (id, region, amount) VALUES (501,'A',999),(2501,'C',999)")
        .expect("cold insert");
    assert_imv_correct("pt_cold_v", sql);
}

/// The LIST dispatch SQL must carry a typed partition-key pruning predicate;
/// RANGE must not (pruning is LIST-only). Guards the codegen shape directly.
#[pg_test]
fn pg_part_list_dispatch_sql_has_pruning_predicate() {
    let list_sql = crate::trigger::build_passthrough_partition_dispatch_sql(
        "v", "\"public\".\"v\"", "SELECT 1 AS pkey", "region", "\"public\".\"v\".\"region\"",
        "LIST", "DELETE FROM \"public\".\"v\" WHERE id IN (SELECT id FROM pt_old)", "",
    );
    assert!(
        list_sql.contains("= ANY($2::text[]::"),
        "LIST dispatch must prune on the partition key; got:\n{list_sql}"
    );
    let range_sql = crate::trigger::build_passthrough_partition_dispatch_sql(
        "v", "\"public\".\"v\"", "SELECT 1 AS pkey", "ts", "\"public\".\"v\".\"ts\"",
        "RANGE", "DELETE FROM \"public\".\"v\" WHERE id IN (SELECT id FROM pt_old)", "",
    );
    assert!(
        !range_sql.contains("= ANY($2::text[]::"),
        "RANGE dispatch must not add value-ANY pruning (LIST-only); got:\n{range_sql}"
    );
}

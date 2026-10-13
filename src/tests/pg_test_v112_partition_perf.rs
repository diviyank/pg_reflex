// Commit-time work narrowed for partition swaps and drops (1.12.1), each next
// to the oracle that shows the narrower work stays correct.

/// FULL JOIN passthrough IMVs over `{p}_f` and `{p}_h`, each followed by the
/// query that recomputes it: `(name, query, key, scoped)`, `scoped` telling
/// whether a write to `{p}_f` may be applied to the rows of its values only.
fn pp_full_join_cells(p: &str) -> Vec<(String, String, Option<&'static str>, bool)> {
    let base = |select: &str, on: &str| {
        format!(
            "SELECT {select} FROM {p}_f AS f FULL JOIN {p}_h AS h ON {on} \
             LEFT JOIN {p}_a AS a ON a.k = COALESCE(f.k, h.k)"
        )
    };
    let measures = "COALESCE(f.q, 0) AS q, COALESCE(h.s, 0) AS s, (a.k IS NOT NULL) AS active";
    vec![
        (
            format!("{p}_keyed"),
            base(
                &format!("COALESCE(f.plan, h.plan) AS plan, COALESCE(f.k, h.k) AS k, {measures}"),
                "f.plan = h.plan AND f.k = h.k",
            ),
            Some("plan, k"),
            true,
        ),
        (
            format!("{p}_reversed"),
            base(
                &format!("COALESCE(h.plan, f.plan) AS plan, COALESCE(h.k, f.k) AS k, {measures}"),
                "h.k = f.k AND h.plan = f.plan AND f.q <> 7",
            ),
            None,
            true,
        ),
        (
            format!("{p}_one_column"),
            base(
                &format!("COALESCE(f.plan, h.plan) AS plan, COALESCE(f.k, h.k) AS k, {measures}"),
                "f.plan = h.plan AND f.k + 0 = h.k",
            ),
            Some("plan, k"),
            true,
        ),
        (
            format!("{p}_plan_only"),
            base(
                &format!("COALESCE(f.plan, h.plan) AS plan, COALESCE(f.k, h.k) AS k, {measures}"),
                "f.plan = h.plan",
            ),
            None,
            true,
        ),
        (
            format!("{p}_unequated"),
            base(
                &format!("COALESCE(f.plan, h.plan) AS plan, COALESCE(f.k, h.k) AS k, {measures}"),
                "f.plan = h.plan + 0 AND f.k + 0 = h.k",
            ),
            Some("plan, k"),
            false,
        ),
        (
            format!("{p}_filtered"),
            format!(
                "{} WHERE COALESCE(f.q, 0) + COALESCE(h.s, 0) <> 13",
                base(
                    &format!(
                        "COALESCE(f.plan, h.plan) AS plan, COALESCE(f.k, h.k) AS k, {measures}"
                    ),
                    "f.plan = h.plan AND f.k = h.k",
                )
            ),
            Some("plan, k"),
            true,
        ),
    ]
}

fn pp_full_join_steps(p: &str) -> Vec<String> {
    [
        "INSERT INTO {p}_f VALUES (1, 6, 5)",
        "INSERT INTO {p}_f VALUES (1, 40, 6), (3, 41, 7)",
        "DELETE FROM {p}_f WHERE plan = 1 AND k = 1",
        "UPDATE {p}_f SET k = k + 100 WHERE plan = 2 AND k <= 2",
        "UPDATE {p}_h SET plan = 3 WHERE plan = 1 AND k = 4",
        "UPDATE {p}_f SET q = 7 WHERE plan = 2 AND k = 5",
        "UPDATE {p}_f SET q = q + 1 WHERE plan = 1",
        "DELETE FROM {p}_h WHERE plan = 2",
        "DELETE FROM {p}_f WHERE plan = 3",
        "INSERT INTO {p}_a VALUES (2), (40)",
        "UPDATE {p}_f SET plan = 3 WHERE plan = 1 AND k = 2",
    ]
    .iter()
    .map(|step| step.replace("{p}", p))
    .collect()
}

fn pp_full_join_run(p: &str, mode: &str) {
    for ddl in [
        "CREATE TABLE {p}_f (plan INT NOT NULL, k INT NOT NULL, q INT NOT NULL, PRIMARY KEY (plan, k))",
        "CREATE TABLE {p}_h (plan INT NOT NULL, k INT NOT NULL, s INT NOT NULL, PRIMARY KEY (plan, k))",
        "CREATE TABLE {p}_a (k INT PRIMARY KEY)",
        "INSERT INTO {p}_f SELECT p, k, p + k FROM generate_series(1, 2) p, generate_series(1, 5) k",
        "INSERT INTO {p}_h SELECT p, k, p * k FROM generate_series(1, 2) p, generate_series(4, 8) k",
        "INSERT INTO {p}_a VALUES (1), (4), (6)",
    ] {
        Spi::run(&ddl.replace("{p}", p)).expect("fixture");
    }
    let cells = pp_full_join_cells(p);
    for (name, query, key, scoped) in &cells {
        let key = key.map_or("NULL".to_string(), sql_lit);
        let created = Spi::get_one::<String>(&format!(
            "SELECT create_reflex_ivm('{name}', {}, {key}, NULL, '{mode}')",
            sql_lit(query)
        ))
        .expect("create")
        .unwrap_or_default();
        assert_eq!(created, "CREATE REFLEX INCREMENTAL VIEW", "{name}");
        let delta = Spi::get_one::<String>(&format!(
            "SELECT reflex_build_delta_sql(r.name, '{p}_f', 'DELETE', r.base_query, \
               COALESCE(r.end_query, ''), r.aggregations::text, r.base_query) \
             FROM public.__reflex_ivm_reference r WHERE r.name = '{name}'"
        ))
        .expect("delta sql")
        .unwrap_or_default();
        assert_eq!(
            !delta.contains("reflex_rebuild_target_rows"),
            *scoped,
            "{name}: a write to {p}_f should {}be applied to the rows of its values only",
            if *scoped { "" } else { "not " }
        );
    }
    for step in pp_full_join_steps(p) {
        Spi::run(&step).expect("step");
        if mode == "DEFERRED" {
            for source in ["f", "h", "a"] {
                Spi::run(&format!("SELECT reflex_flush_deferred('{p}_{source}')")).expect("flush");
            }
        }
        for (name, query, _, _) in &cells {
            let mismatches = Spi::get_one::<i64>(&format!(
                "SELECT count(*) FROM ((SELECT * FROM {name} EXCEPT ALL ({query})) \
                 UNION ALL (({query}) EXCEPT ALL SELECT * FROM {name})) x"
            ))
            .expect("oracle")
            .unwrap_or(-1);
            assert_eq!(mismatches, 0, "{name} after <{step}>");
        }
    }
}

#[pg_test]
fn v112_pp_full_join_passthrough_is_refreshed_by_the_values_it_follows_immediate() {
    pp_full_join_run("pp_fji", "IMMEDIATE");
}

#[pg_test]
fn v112_pp_full_join_passthrough_is_refreshed_by_the_values_it_follows_deferred() {
    pp_full_join_run("pp_fjd", "DEFERRED");
}

/// An aggregate IMV partitioned by plan, with NULL groups, read by a
/// passthrough, each statement committed on a private database: a leaf swap
/// of its source rebuilds its mirror leaf as a diff keyed by its GROUP BY
/// columns.
fn pp_group_keyed_run(db: &str, mode: &str) {
    probe_db_open(db);
    for ddl in [
        "CREATE TABLE pp_src (plan INT NOT NULL, d INT NOT NULL, g INT, v INT NOT NULL) PARTITION BY LIST (plan)",
        "CREATE TABLE pp_src_1 PARTITION OF pp_src FOR VALUES IN (1) PARTITION BY RANGE (d)",
        "CREATE TABLE pp_src_1_a PARTITION OF pp_src_1 FOR VALUES FROM (1) TO (7)",
        "CREATE TABLE pp_src_1_b PARTITION OF pp_src_1 FOR VALUES FROM (7) TO (21)",
        "CREATE TABLE pp_src_2 PARTITION OF pp_src FOR VALUES IN (2) PARTITION BY RANGE (d)",
        "CREATE TABLE pp_src_2_a PARTITION OF pp_src_2 FOR VALUES FROM (1) TO (21)",
        "INSERT INTO pp_src SELECT p, d, NULLIF(d % 4, 0), p * d FROM generate_series(1, 2) p, generate_series(1, 12) d",
    ] {
        worker_exec(ddl);
    }
    let agg = "SELECT plan, g, SUM(v) AS s, COUNT(*) AS c FROM pp_src GROUP BY plan, g";
    for (name, query, key) in [
        ("pp_agg", agg, "NULL"),
        ("pp_reader", "SELECT plan, g, s, c FROM pp_agg", "'plan, g'"),
    ] {
        let created = worker_scalar_i64(&format!(
            "SELECT (create_reflex_ivm('{name}', {}, {key}, NULL, '{mode}', NULL, ARRAY['plan']) \
               = 'CREATE REFLEX INCREMENTAL VIEW')::int::int8",
            sql_lit(query)
        ));
        assert_eq!(created, 1, "{name}");
    }
    assert_eq!(
        worker_scalar_i64(
            "SELECT count(*)::int8 FROM pg_index WHERE indrelid = 'pp_agg'::regclass AND indisunique"
        ),
        0,
        "the aggregate's target must have no unique index for its groups to be the diff's key"
    );
    for rows in [
        "SELECT 1, d, NULLIF(d % 4, 0), d + 1 FROM generate_series(7, 12) d",
        "SELECT 1, d, NULLIF(d % 5, 0), d FROM generate_series(7, 20) d",
        "SELECT 1, d, NULL, d FROM generate_series(7, 9) d",
        "SELECT 1, d, d % 3, d FROM generate_series(7, 15) d",
    ] {
        worker_exec(&format!(
            "CREATE TABLE pp_new (LIKE pp_src INCLUDING DEFAULTS); \
             INSERT INTO pp_new {rows}; \
             ALTER TABLE pp_src_1 DETACH PARTITION pp_src_1_b; \
             ALTER TABLE pp_src_1 ATTACH PARTITION pp_new FOR VALUES FROM (7) TO (21); \
             DROP TABLE pp_src_1_b; ALTER TABLE pp_new RENAME TO pp_src_1_b"
        ));
        for (name, fresh) in [
            ("pp_agg", agg.to_string()),
            ("pp_reader", format!("SELECT plan, g, s, c FROM ({agg}) x")),
        ] {
            let mismatches = worker_scalar_i64(&format!(
                "SELECT count(*)::int8 FROM ((SELECT plan, g, s, c FROM {name} EXCEPT ALL ({fresh})) \
                 UNION ALL (({fresh}) EXCEPT ALL SELECT plan, g, s, c FROM {name})) x"
            ));
            assert_eq!(mismatches, 0, "{mode} {name} after swapping in <{rows}>");
        }
    }
    probe_db_close(db);
}

#[pg_test]
fn v112_pp_aggregate_leaf_rebuilt_as_a_diff_keyed_by_its_groups_immediate() {
    pp_group_keyed_run("reflex_pp_gki", "IMMEDIATE");
}

#[pg_test]
fn v112_pp_aggregate_leaf_rebuilt_as_a_diff_keyed_by_its_groups_deferred() {
    pp_group_keyed_run("reflex_pp_gkd", "DEFERRED");
}

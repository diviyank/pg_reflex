// Partition changes combined with a TRUNCATE, partitions whose triggers are
// missing or disabled, and the audit's view of the triggers of a source. The
// committed scenarios run on a `dblink` worker in a private database (helpers of
// `pg_test_partition_attach_locks.rs`, `pg_test_rebuild_commit.rs`,
// `pg_test_v112_deferred_partition.rs` and `pg_test_v112_partition_dml.rs`).

/// (IMV, what differs) for every IMV of `imvs` that differs from its query or is
/// flagged stale on the worker.
fn v112_pr_wrong_imvs(imvs: &[(String, String)]) -> Vec<String> {
    imvs.iter()
        .filter_map(|(imv, query)| {
            if rbc_mismatch(imv, query) != 0 {
                return Some(format!("{imv} {}", v112_pd_difference(imv, query)));
            }
            rbc_known_stale(imv).then(|| format!("{imv} is flagged stale"))
        })
        .collect()
}

fn v112_pr_worker_reconcile(imvs: &[(String, String)]) {
    for (imv, _) in imvs {
        let _ = rbc_select(&format!("reflex_reconcile({})", sql_lit(imv)));
    }
}

// ---------------------------------------------------------------------------
// A partition change and a TRUNCATE in one transaction
// ---------------------------------------------------------------------------

/// IMVs over `x` that do not mirror its partitions, one grouped by the partition
/// key, a keyed passthrough, and an IMV reading one of them; each with the query
/// over `x` it must equal.
fn v112_pr_change_fixture(mode: &str) -> Vec<(String, String)> {
    for ddl in [
        "CREATE TABLE x(id INT NOT NULL, g TEXT NOT NULL, v INT) PARTITION BY LIST (g)",
        "CREATE TABLE x_a PARTITION OF x FOR VALUES IN ('a')",
        "CREATE TABLE x_b PARTITION OF x FOR VALUES IN ('b')",
        "INSERT INTO x VALUES (1,'a',1),(2,'b',2),(3,'a',5)",
    ] {
        worker_exec(ddl);
    }
    const BY_VALUE: &str = "SELECT v, COUNT(*) AS c FROM x GROUP BY v";
    let over_by_value = |relation: &str| format!("SELECT COUNT(*) AS n, SUM(c) AS t FROM {relation}");
    let imvs: Vec<(&str, String, Option<&str>, String)> = vec![
        ("x_total", "SELECT COUNT(*) AS c, SUM(v) AS sv FROM x".into(), None, String::new()),
        ("x_byv", BY_VALUE.into(), None, String::new()),
        ("x_byg", "SELECT g, COUNT(*) AS c, SUM(v) AS sv FROM x GROUP BY g".into(), None, String::new()),
        ("x_keyed", "SELECT id, g, v FROM x".into(), Some("id, g"), String::new()),
        ("x_dep", over_by_value("x_byv"), None, over_by_value(&format!("({BY_VALUE}) q"))),
    ];
    for (imv, query, key, _) in &imvs {
        v112_dp_create(imv, query, *key, mode, None);
    }
    imvs.into_iter()
        .map(|(imv, query, _, over_x)| (imv.to_string(), if over_x.is_empty() { query } else { over_x }))
        .collect()
}

struct V112PrPartitionChange {
    kind: &'static str,
    /// Committed before the transaction under test, one transaction each.
    setup: Vec<String>,
    change: String,
}

/// The three partition changes, on a partition named after `n`: created and
/// loaded directly, attached already filled, and detached while it holds rows.
fn v112_pr_partition_changes(n: usize) -> Vec<V112PrPartitionChange> {
    let id = 1000 + 20 * n;
    let rows = |name: &str, value: &str| {
        format!("INSERT INTO {name} VALUES ({id},'{value}',5),({},'{value}',6)", id + 1)
    };
    let (created, attached, detached) = (format!("x_c{n}"), format!("x_t{n}"), format!("x_d{n}"));
    vec![
        V112PrPartitionChange {
            kind: "create and load",
            setup: vec![],
            change: format!(
                "CREATE TABLE {created} PARTITION OF x FOR VALUES IN ('c{n}'); {}",
                rows(&created, &format!("c{n}"))
            ),
        },
        V112PrPartitionChange {
            kind: "attach filled",
            setup: vec![],
            change: format!(
                "CREATE TABLE {attached} (LIKE x INCLUDING ALL); {}; \
                 ALTER TABLE x ATTACH PARTITION {attached} FOR VALUES IN ('t{n}')",
                rows(&attached, &format!("t{n}"))
            ),
        },
        V112PrPartitionChange {
            kind: "detach filled",
            setup: vec![
                format!("CREATE TABLE {detached} PARTITION OF x FOR VALUES IN ('d{n}')"),
                rows("x", &format!("d{n}")),
            ],
            change: format!("ALTER TABLE x DETACH PARTITION {detached}"),
        },
    ]
}

/// Orders of a partition change (P), a TRUNCATE (T) and a write (W) in one
/// transaction; the TRUNCATE names a partition, or the source (R).
const V112_PR_ORDERS: &[&str] =
    &["PT", "TP", "PTW", "PWT", "TPW", "TWP", "WPT", "WTP", "PR", "RP", "PRW", "WRP"];

fn v112_pr_change_failures(dbname: &str, mode: &str) -> Vec<String> {
    let imvs = v112_pr_change_fixture(mode);
    let mut failures = Vec::new();
    let mut judge = |label: &str, outcome: Result<(), String>| {
        if let Err(error) = outcome {
            failures.push(format!("{mode} {label}: raised {error}"));
        }
        let wrong = v112_pr_wrong_imvs(&imvs);
        if !wrong.is_empty() {
            failures.push(format!("{mode} {label}: {}", wrong.join("; ")));
            v112_pr_worker_reconcile(&imvs);
        }
    };
    let mut n = 0;
    for order in V112_PR_ORDERS {
        for position in 0..3 {
            n += 1;
            let change = v112_pr_partition_changes(n).swap_remove(position);
            let id = 1000 + 20 * n;
            v112_dp_reconnect(dbname);
            judge(
                "refill",
                v112_dp_try(&format!(
                    "INSERT INTO x VALUES ({},'a',1),({},'b',2),({},'a',4)",
                    id + 10,
                    id + 11,
                    id + 12
                )),
            );
            for (step, setup) in change.setup.iter().enumerate() {
                judge(&format!("{} setup {step}", change.kind), v112_dp_try(setup));
            }
            let statements: Vec<String> = order
                .chars()
                .map(|step| match step {
                    'P' => change.change.clone(),
                    'T' => "TRUNCATE x_a".to_string(),
                    'R' => "TRUNCATE x".to_string(),
                    _ => format!(
                        "INSERT INTO x VALUES ({},'b',3),({},'a',7); UPDATE x_b SET v = v + 1",
                        id + 15,
                        id + 16
                    ),
                })
                .collect();
            v112_dp_reconnect(dbname);
            judge(&format!("{} in order {order}", change.kind), v112_dp_try(&statements.join("; ")));
            judge(
                &format!("write after {} in order {order}", change.kind),
                v112_dp_try(&format!("INSERT INTO x VALUES ({},'a',2),({},'b',9)", id + 17, id + 18)),
            );
        }
    }
    failures
}

fn v112_pr_assert_change_and_truncate_matrix(dbname: &str, mode: &str) {
    probe_db_open(dbname);
    let failures = v112_pr_change_failures(dbname, mode);
    probe_db_close(dbname);
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[pg_test]
fn v112_pr_partition_change_and_truncate_in_one_transaction_immediate() {
    v112_pr_assert_change_and_truncate_matrix("reflex_pr_change_i", "IMMEDIATE");
}

#[pg_test]
fn v112_pr_partition_change_and_truncate_in_one_transaction_deferred() {
    v112_pr_assert_change_and_truncate_matrix("reflex_pr_change_d", "DEFERRED");
}

// ---------------------------------------------------------------------------
// Partitions whose triggers are missing or disabled
// ---------------------------------------------------------------------------

const V112_PR_TREE_SEED: &str =
    "INSERT INTO s VALUES (1,'a',1),(2,'a',2),(3,'b',5),(4,'c',7),(104,'c',9)";

/// A LIST-partitioned source with a RANGE sub-partitioned branch, read by an
/// ungrouped aggregate, a grouped one and an IMV over the grouped one; each with
/// the query over `s` it must equal.
fn v112_pr_tree_fixture(mode: &str) -> Vec<(String, String)> {
    for ddl in [
        "CREATE TABLE s(id INT NOT NULL, g TEXT NOT NULL, v INT) PARTITION BY LIST (g)",
        "CREATE TABLE s_pa PARTITION OF s FOR VALUES IN ('a', 'b')",
        "CREATE TABLE s_pc PARTITION OF s FOR VALUES IN ('c') PARTITION BY RANGE (id)",
        "CREATE TABLE s_pc1 PARTITION OF s_pc FOR VALUES FROM (0) TO (100)",
        "CREATE TABLE s_pc2 PARTITION OF s_pc FOR VALUES FROM (100) TO (200)",
        V112_PR_TREE_SEED,
    ] {
        worker_exec(ddl);
    }
    const GROUPED: &str = "SELECT g, COUNT(*) AS c, SUM(v) AS sv FROM s GROUP BY g";
    let over_grouped = |relation: &str| format!("SELECT COUNT(*) AS n, SUM(sv) AS t FROM {relation}");
    let total = "SELECT COUNT(*) AS c, SUM(v) AS sv FROM s".to_string();
    for (imv, query) in [("s_total", &total), ("s_agg", &GROUPED.to_string()), ("s_dep", &over_grouped("s_agg"))] {
        v112_dp_create(imv, query, None, mode, None);
    }
    vec![
        ("s_total".to_string(), total),
        ("s_agg".to_string(), GROUPED.to_string()),
        ("s_dep".to_string(), over_grouped(&format!("({GROUPED}) q"))),
    ]
}

/// (triggers damaged, as (table, trigger); the transaction that truncates).
const V112_PR_TRUNCATES_OVER_A_DAMAGED_TREE: &[(&[(&str, &str)], &str)] = &[
    (&[("s_pa", "__reflex_trigger_trunc_on_s")], "TRUNCATE s_pa, s_pc1"),
    (&[("s_pa", "__reflex_trigger_trunc_on_s")], "TRUNCATE s_pc1, s_pa"),
    (&[("s_pa", "__reflex_trigger_trunc_on_s")], "TRUNCATE s"),
    (
        &[("s_pa", "__reflex_trigger_trunc_on_s")],
        "TRUNCATE s_pa, s_pc1; INSERT INTO s VALUES (150,'c',3),(50,'c',4); TRUNCATE s_pc2",
    ),
    (&[("s_pc", "__reflex_trigger_trunc_on_s")], "TRUNCATE s_pc; INSERT INTO s VALUES (51,'c',4)"),
    (&[("s_pa", "__reflex_trigger_btrunc_on_s")], "TRUNCATE s_pa, s_pc1"),
    (
        &[("s_pa", "__reflex_trigger_btrunc_on_s")],
        "TRUNCATE s_pc1; INSERT INTO s VALUES (52,'c',4); TRUNCATE s_pa, s_pc2",
    ),
    (
        &[("s_pa", "__reflex_trigger_trunc_on_s"), ("s_pc2", "__reflex_trigger_btrunc_on_s")],
        "TRUNCATE s_pa, s_pc1; INSERT INTO s VALUES (151,'c',3),(53,'c',4); TRUNCATE s_pc2",
    ),
    (
        &[("s_pc1", "__reflex_trigger_trunc_on_s"), ("s_pc2", "__reflex_trigger_btrunc_on_s")],
        "TRUNCATE s_pc1, s_pa; TRUNCATE s_pc2",
    ),
];

/// A TRUNCATE of several tables of the tree rebuilds the IMVs when one of them
/// lacks a truncate trigger, and so does the next TRUNCATE of the transaction.
fn v112_pr_truncate_over_a_damaged_tree_failures(mode: &str) -> Vec<String> {
    let imvs = v112_pr_tree_fixture(mode);
    let mut failures = Vec::new();
    for damage in ["dropped", "disabled"] {
        for (triggers, transaction) in V112_PR_TRUNCATES_OVER_A_DAMAGED_TREE {
            let case = format!("{mode} {triggers:?} {damage}, {transaction}");
            for (table, trigger) in *triggers {
                worker_exec(&match damage {
                    "dropped" => format!("DROP TRIGGER \"{trigger}\" ON {table}"),
                    _ => format!("ALTER TABLE {table} DISABLE TRIGGER \"{trigger}\""),
                });
            }
            if let Err(error) = v112_dp_try(transaction) {
                failures.push(format!("{case}: raised {error}"));
            }
            let wrong = v112_pr_wrong_imvs(&imvs);
            if !wrong.is_empty() {
                failures.push(format!("{case}: {}", wrong.join("; ")));
            }
            for (table, trigger) in *triggers {
                let _ = v112_dp_try(&format!("ALTER TABLE {table} ENABLE TRIGGER \"{trigger}\""));
            }
            let _ = rbc_select("reflex_rebuild_triggers('s')");
            v112_pr_worker_reconcile(&imvs);
            worker_exec(&format!("DELETE FROM s; {V112_PR_TREE_SEED}"));
            let wrong = v112_pr_wrong_imvs(&imvs);
            assert!(wrong.is_empty(), "fixture: reseed after {case}: {}", wrong.join("; "));
        }
    }
    failures
}

fn v112_pr_assert_truncate_over_a_damaged_tree(dbname: &str, mode: &str) {
    probe_db_open(dbname);
    let failures = v112_pr_truncate_over_a_damaged_tree_failures(mode);
    probe_db_close(dbname);
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[pg_test]
fn v112_pr_truncate_rebuilds_when_a_truncated_table_lacks_a_truncate_trigger_immediate() {
    v112_pr_assert_truncate_over_a_damaged_tree("reflex_pr_trunc_i", "IMMEDIATE");
}

#[pg_test]
fn v112_pr_truncate_rebuilds_when_a_truncated_table_lacks_a_truncate_trigger_deferred() {
    v112_pr_assert_truncate_over_a_damaged_tree("reflex_pr_trunc_d", "DEFERRED");
}

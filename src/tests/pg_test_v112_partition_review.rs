// Partition changes combined with a TRUNCATE, partitions whose triggers are
// missing or disabled, and the audit's view of the triggers of a source. The
// committed scenarios run on a `dblink` worker in a private database (helpers of
// `pg_test_partition_attach_locks.rs`, `pg_test_rebuild_commit.rs`,
// `pg_test_v112_deferred_partition.rs` and `pg_test_v112_partition_dml.rs`).

/// The statements an audit report prints under its "Suggested fix:" lines.
fn v112_pr_remedies(report: &str) -> Vec<String> {
    let mut remedies: Vec<String> = Vec::new();
    let mut inside_a_fix = false;
    for line in report.lines() {
        if line.trim() == "Suggested fix:" {
            inside_a_fix = true;
            continue;
        }
        if !inside_a_fix {
            continue;
        }
        match line.strip_prefix("    ").map(str::trim) {
            Some(statement) if !statement.is_empty() => {
                if !remedies.iter().any(|known| known == statement) {
                    remedies.push(statement.to_string());
                }
            }
            _ => inside_a_fix = false,
        }
    }
    remedies
}

/// The expression of a remedy printed as `SELECT <expression>;`.
fn v112_pr_remedy_expression(remedy: &str) -> &str {
    remedy
        .trim_end_matches(';')
        .strip_prefix("SELECT ")
        .unwrap_or_else(|| panic!("the remedy is not a SELECT statement: {remedy}"))
}

fn v112_pr_worker_audit() -> String {
    rbc_select("reflex_audit()")
}

/// Runs on the worker, one transaction each, every remedy `report` prints; the
/// remedies that answered with an error.
fn v112_pr_worker_run_remedies(report: &str) -> Vec<String> {
    v112_pr_remedies(report)
        .into_iter()
        .filter_map(|remedy| {
            let outcome = rbc_select(v112_pr_remedy_expression(&remedy));
            outcome.starts_with("ERROR").then(|| format!("{remedy} answered {outcome}"))
        })
        .collect()
}

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

/// (table of the tree, a partition-key value it accepts, one of its triggers).
const V112_PR_TREE_TRIGGERS: &[(&str, &str, &str)] = &[
    ("s_pa", "a", "reflex_partition_ins_on_s"),
    ("s_pa", "a", "reflex_partition_del_on_s"),
    ("s_pa", "a", "reflex_partition_upd_on_s"),
    ("s_pa", "a", "reflex_partition_guard_on_s"),
    ("s_pa", "a", "__reflex_trigger_btrunc_on_s"),
    ("s_pa", "a", "__reflex_trigger_trunc_on_s"),
    ("s_pc1", "c", "reflex_partition_ins_on_s"),
    ("s_pc1", "c", "__reflex_trigger_trunc_on_s"),
    ("s_pc", "c", "reflex_partition_upd_on_s"),
    ("s_pc", "c", "__reflex_trigger_trunc_on_s"),
    ("s", "a", "reflex_partitioned_source_s"),
    ("s", "a", "__reflex_trigger_btrunc_on_s"),
    ("s", "a", "__reflex_trigger_ins_on_s"),
    ("s", "a", "__reflex_trigger_trunc_on_s"),
];

fn v112_pr_trigger_state(table: &str, trigger: &str) -> String {
    rbc_select(&format!(
        "SELECT COALESCE((SELECT tgenabled::text FROM pg_trigger \
           WHERE tgrelid = '{table}'::regclass AND tgname = '{trigger}'), 'missing')"
    ))
}

/// Statements addressed to `table`, one transaction each: every kind of write,
/// and a TRUNCATE when `truncating`.
fn v112_pr_writes_addressed_to(table: &str, value: &str, id: usize, truncating: bool) -> Vec<String> {
    let mut writes = vec![
        format!("INSERT INTO {table} VALUES ({id},'{value}',5),({},'{value}',6)", id + 1),
        format!("UPDATE {table} SET v = v + 1"),
        format!("DELETE FROM {table} WHERE id = (SELECT min(id) FROM {table})"),
    ];
    if truncating {
        writes.push(format!("TRUNCATE {table}"));
        writes.push(format!("INSERT INTO {table} VALUES ({},'{value}',8)", id + 2));
    }
    writes
}

/// For every trigger of the tree, dropped and then disabled: the audit names the
/// table and the trigger, its printed remedy leaves a clean audit and correct
/// IMVs, and statements addressed to the table are maintained again.
fn v112_pr_damaged_trigger_failures(mode: &str) -> Vec<String> {
    let imvs = v112_pr_tree_fixture(mode);
    let mut failures = Vec::new();
    let clean = v112_pr_worker_audit();
    if !clean.contains("no findings") {
        failures.push(format!("{mode} audit after create:\n{clean}"));
    }
    let (id_while_damaged, id_once_restored) = (20, 30);
    for (damage, expected_state) in [("dropped", "missing"), ("disabled", "D")] {
        for (table, value, trigger) in V112_PR_TREE_TRIGGERS {
            let case = format!("{mode} {trigger} {damage} on {table}");
            let truncating = trigger.contains("trunc");
            worker_exec(&match damage {
                "dropped" => format!("DROP TRIGGER \"{trigger}\" ON {table}"),
                _ => format!("ALTER TABLE {table} DISABLE TRIGGER \"{trigger}\""),
            });
            assert_eq!(v112_pr_trigger_state(table, trigger), expected_state, "fixture: {case}");
            for write in v112_pr_writes_addressed_to(table, value, id_while_damaged, truncating) {
                let _ = v112_dp_try(&write);
            }

            let report = v112_pr_worker_audit();
            let named = report.contains("trigger-attached")
                && report.contains(trigger)
                && report.contains(&format!(" {table} "))
                && (damage == "dropped" || report.contains("disabled"));
            if !named {
                failures.push(format!("{case}: the audit does not name it:\n{report}"));
            }
            failures.extend(
                v112_pr_worker_run_remedies(&report).into_iter().map(|e| format!("{case}: {e}")),
            );
            let state = v112_pr_trigger_state(table, trigger);
            if state != "O" {
                failures.push(format!("{case}: after the remedy the trigger is {state}"));
            }
            let after_remedy = v112_pr_worker_audit();
            if !after_remedy.contains("no findings") {
                failures.push(format!("{case}: audit after the remedy:\n{after_remedy}"));
            }
            let wrong = v112_pr_wrong_imvs(&imvs);
            if !wrong.is_empty() {
                failures.push(format!("{case}: after the remedy {}", wrong.join("; ")));
            }

            // Whatever the remedy did, the next case starts from a sound state.
            let _ = v112_dp_try(&format!("ALTER TABLE {table} ENABLE TRIGGER \"{trigger}\""));
            let _ = rbc_select("reflex_rebuild_triggers('s')");
            v112_pr_worker_reconcile(&imvs);

            let mut writes =
                v112_pr_writes_addressed_to(table, value, id_once_restored, truncating);
            writes.push(format!("DELETE FROM s; {V112_PR_TREE_SEED}"));
            for write in writes {
                if let Err(error) = v112_dp_try(&write) {
                    failures.push(format!("{case}: {write} raised {error}"));
                }
                let wrong = v112_pr_wrong_imvs(&imvs);
                if !wrong.is_empty() {
                    failures.push(format!("{case}: after {write}: {}", wrong.join("; ")));
                    v112_pr_worker_reconcile(&imvs);
                }
            }
        }
    }
    failures.extend(v112_pr_partition_without_any_trigger_failures(mode, &imvs));
    failures
}

/// A partition that carries none of its triggers, as one created while event
/// triggers did not fire: one finding per IMV of the source names the partition
/// and its six triggers, and the remedy restores them and the rows it missed.
fn v112_pr_partition_without_any_trigger_failures(
    mode: &str,
    imvs: &[(String, String)],
) -> Vec<String> {
    let case = format!("{mode} every trigger dropped on s_pa");
    let mut failures = Vec::new();
    let triggers: Vec<String> = rbc_select(
        "SELECT string_agg(tgname::text, ',' ORDER BY tgname) FROM pg_trigger \
         WHERE tgrelid = 's_pa'::regclass AND NOT tgisinternal",
    )
    .split(',')
    .map(str::to_string)
    .collect();
    assert_eq!(triggers.len(), 6, "fixture: s_pa carries {triggers:?}");
    for trigger in &triggers {
        worker_exec(&format!("DROP TRIGGER \"{trigger}\" ON s_pa"));
    }
    for write in v112_pr_writes_addressed_to("s_pa", "a", 20, true) {
        let _ = v112_dp_try(&write);
    }
    let report = v112_pr_worker_audit();
    let findings_naming_the_partition = report
        .lines()
        .filter(|line| line.contains("Table s_pa of the partition tree of source s "))
        .filter(|line| triggers.iter().all(|trigger| line.contains(trigger.as_str())))
        .count();
    if findings_naming_the_partition != 2 {
        failures.push(format!(
            "{case}: {findings_naming_the_partition} findings name the partition and its \
             six triggers, one is expected for each of s_total and s_agg:\n{report}"
        ));
    }
    failures.extend(v112_pr_worker_run_remedies(&report).into_iter().map(|e| format!("{case}: {e}")));
    let after_remedy = v112_pr_worker_audit();
    if !after_remedy.contains("no findings") {
        failures.push(format!("{case}: audit after the remedy:\n{after_remedy}"));
    }
    let wrong = v112_pr_wrong_imvs(imvs);
    if !wrong.is_empty() {
        failures.push(format!("{case}: after the remedy {}", wrong.join("; ")));
        let _ = rbc_select("reflex_rebuild_triggers('s')");
        v112_pr_worker_reconcile(imvs);
    }
    for write in v112_pr_writes_addressed_to("s_pa", "a", 30, true) {
        if let Err(error) = v112_dp_try(&write) {
            failures.push(format!("{case}: {write} raised {error}"));
        }
        let wrong = v112_pr_wrong_imvs(imvs);
        if !wrong.is_empty() {
            failures.push(format!("{case}: after {write}: {}", wrong.join("; ")));
            v112_pr_worker_reconcile(imvs);
        }
    }
    failures
}

fn v112_pr_assert_damaged_triggers_are_reported_and_restored(dbname: &str, mode: &str) {
    probe_db_open(dbname);
    let failures = v112_pr_damaged_trigger_failures(mode);
    probe_db_close(dbname);
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[pg_test]
fn v112_pr_audit_names_a_missing_or_disabled_tree_trigger_and_its_remedy_restores_it_immediate() {
    v112_pr_assert_damaged_triggers_are_reported_and_restored("reflex_pr_damage_i", "IMMEDIATE");
}

#[pg_test]
fn v112_pr_audit_names_a_missing_or_disabled_tree_trigger_and_its_remedy_restores_it_deferred() {
    v112_pr_assert_damaged_triggers_are_reported_and_restored("reflex_pr_damage_d", "DEFERRED");
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

// ---------------------------------------------------------------------------
// The audit's view of the triggers of a source
// ---------------------------------------------------------------------------

fn v112_pr_audit_of(imv: &str) -> String {
    Spi::get_one::<String>(&format!("SELECT reflex_audit({})", sql_lit(imv)))
        .expect("audit")
        .expect("audit report")
}

fn v112_pr_run_remedies(report: &str) {
    for remedy in v112_pr_remedies(report) {
        let outcome = Spi::get_one::<String>(&format!(
            "SELECT ({})::text",
            v112_pr_remedy_expression(&remedy)
        ))
        .expect("remedy")
        .unwrap_or_default();
        assert!(!outcome.starts_with("ERROR"), "{remedy} answered {outcome}");
    }
}

fn v112_pr_assert_audit_clean(imv: &str, when: &str) {
    let report = v112_pr_audit_of(imv);
    assert!(report.contains("no findings"), "{when}: audit of {imv}:\n{report}");
}

/// Sources whose trigger names exceed 63 bytes and are stored shortened:
/// (statements creating the source, the name the query uses, a table to write).
fn v112_pr_long_named_sources() -> Vec<(Vec<String>, &'static str, &'static str)> {
    const PLAIN: &str = "v112_pr_a_plain_source_with_a_name_of_fifty_five_characters";
    const QUALIFIED: &str = "v112_pr_a_long_schema_name.a_source_table_with_a_long_name";
    const PARTITIONED: &str = "v112_pr_a_partitioned_source_with_a_name_of_fifty_chars";
    const PARTITION: &str = "v112_pr_a_partitioned_source_with_a_name_of_fifty_chars_p1";
    vec![
        (vec![format!("CREATE TABLE {PLAIN}(id INT NOT NULL, g TEXT NOT NULL, v INT)")], PLAIN, PLAIN),
        (
            vec![
                "CREATE SCHEMA v112_pr_a_long_schema_name".to_string(),
                format!("CREATE TABLE {QUALIFIED}(id INT NOT NULL, g TEXT NOT NULL, v INT)"),
            ],
            QUALIFIED,
            QUALIFIED,
        ),
        (
            vec![
                format!(
                    "CREATE TABLE {PARTITIONED}(id INT NOT NULL, g TEXT NOT NULL, v INT) \
                     PARTITION BY LIST (g)"
                ),
                format!(
                    "CREATE TABLE {PARTITION} PARTITION OF {PARTITIONED} FOR VALUES IN ('a', 'b', 'c')"
                ),
            ],
            PARTITIONED,
            PARTITION,
        ),
    ]
}

/// Right after create the audit is clean; a trigger then dropped is reported
/// under the name PostgreSQL stores, and the printed remedy clears the finding.
#[pg_test]
fn v112_pr_audit_knows_the_stored_trigger_names_of_a_long_named_source() {
    lock_shared_fixtures();
    for mode in ["IMMEDIATE", "DEFERRED"] {
        for (ddls, source, written) in v112_pr_long_named_sources() {
            for ddl in &ddls {
                Spi::run(ddl).expect("source");
            }
            Spi::run(&format!("INSERT INTO {source} VALUES (1,'a',1),(2,'b',2)")).expect("seed");
            let query = format!("SELECT g, SUM(v) AS sv FROM {source} GROUP BY g");
            assert_eq!(
                crate::create_reflex_ivm("v112_pr_long_v", &query, None, None, Some(mode), None),
                "CREATE REFLEX INCREMENTAL VIEW"
            );
            let case = format!("{mode} {source}");
            v112_pr_assert_audit_clean("v112_pr_long_v", &format!("{case} after create"));

            let stored: Vec<String> = Spi::connect(|client| {
                client
                    .select(
                        &format!(
                            "SELECT tgname::text AS n FROM pg_trigger \
                             WHERE tgrelid = '{source}'::regclass AND NOT tgisinternal \
                               AND tgname LIKE '\\_\\_reflex\\_trigger\\_%' ORDER BY 1"
                        ),
                        None,
                        &[],
                    )
                    .expect("triggers")
                    .filter_map(|row| row.get_by_name::<String, _>("n").ok().flatten())
                    .collect()
            });
            assert!(stored.len() >= 4, "fixture: {case} carries {stored:?}");
            for trigger in &stored {
                Spi::run(&format!("DROP TRIGGER \"{trigger}\" ON {source}")).expect("drop trigger");
                let report = v112_pr_audit_of("v112_pr_long_v");
                assert!(
                    report.contains("trigger-attached") && report.contains(trigger.as_str()),
                    "{case}: {trigger} dropped, audit:\n{report}"
                );
                v112_pr_run_remedies(&report);
                v112_pr_assert_audit_clean("v112_pr_long_v", &format!("{case} after the remedy"));
            }
            if mode == "IMMEDIATE" {
                Spi::run(&format!("INSERT INTO {written} VALUES (3,'a',4),(4,'c',5)")).expect("write");
                Spi::run(&format!("UPDATE {written} SET v = v + 1 WHERE id = 1")).expect("update");
                Spi::run(&format!("DELETE FROM {written} WHERE id = 2")).expect("delete");
                assert_imv_correct("v112_pr_long_v", &query);
            }
            assert_eq!(crate::drop_reflex_ivm("v112_pr_long_v"), "DROP REFLEX INCREMENTAL VIEW");
            Spi::run(&format!("DROP TABLE {source}")).expect("drop source");
            if ddls.len() > 1 && ddls[0].starts_with("CREATE SCHEMA") {
                Spi::run("DROP SCHEMA v112_pr_a_long_schema_name").expect("drop schema");
            }
        }
    }
}

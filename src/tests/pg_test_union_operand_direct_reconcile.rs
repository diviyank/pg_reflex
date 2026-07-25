// 2026-07-24 bug (S3 residual after PS-10 / PS-12) — directly naming a machine-
// generated UNION-ALL operand sub-IMV in `reflex_reconcile` / `reflex_rebuild_imv`
// doubles its MATERIALISED wrapper.
//
// `reconcile_one` rebuilds with `TRUNCATE` + `INSERT`. The operand carries an
// `__reflex_union_mirror_{ins,del,upd}_*` trigger that mirrors its rows into the
// wrapper's `__reflex_src_idx`-tagged slice; that mirror covers INSERT/UPDATE/DELETE
// but NOT TRUNCATE. So the TRUNCATE removes nothing from the wrapper and the INSERT
// re-appends the operand's whole row set: the wrapper's slice for that operand — and
// every consumer reading it — silently doubles.
//
// Reconciling the WRAPPER or a top-level ancestor was never affected: that descent
// goes through `reconcile_generated_child_without_propagating`, which disables the
// operand's triggers first. Only a DIRECT call on the operand's own name reaches
// `reconcile_one` with the mirror live — and `reflex_scheduled_reconcile` makes that
// call on its own, since an aggregate operand is `REBUILDABLE_NODE` and is
// deliberately NOT "covered" by the wrapper above it.
//
// Fix: `reflex_reconcile_with_orphans` routes an operand of a materialised wrapper
// through the same suppressed-rebuild + slice-resync path the DEFERRED cross-source
// guard already uses (`reconcile_generated_child_for_cross_source_guard`).

/// Wrapper-slice oracle: the materialised wrapper must hold exactly its operands'
/// current contents, tagged by position — no duplicates, nothing missing. Compares
/// bidirectionally with `EXCEPT ALL` against a fresh UNION ALL rebuilt from the
/// operand relations themselves.
fn assert_wrapper_mirrors_operands(wrapper: &str, operands: &[&str]) {
    let payload_cols: Vec<String> = Spi::connect(|client| {
        client
            .select(
                &format!(
                    "SELECT a.attname::text AS name FROM pg_attribute a \
                     WHERE a.attrelid = to_regclass('{wrapper}') AND a.attnum > 0 \
                       AND NOT a.attisdropped AND a.attname <> '__reflex_src_idx' \
                     ORDER BY a.attnum"
                ),
                None,
                &[],
            )
            .unwrap()
            .filter_map(|r| r.get_by_name::<&str, _>("name").ok().flatten().map(String::from))
            .collect()
    });
    assert!(
        !payload_cols.is_empty(),
        "fixture precondition: '{wrapper}' must be a materialised wrapper with payload columns"
    );
    let col_list = payload_cols.join(", ");
    let fresh = operands
        .iter()
        .enumerate()
        .map(|(i, op)| format!("SELECT {i}::SMALLINT AS __reflex_src_idx, {col_list} FROM {op}"))
        .collect::<Vec<_>>()
        .join(" UNION ALL ");
    let mismatches = Spi::get_one::<i64>(&format!(
        "SELECT COUNT(*) FROM ( \
           (SELECT __reflex_src_idx, {col_list} FROM {wrapper} \
              EXCEPT ALL SELECT * FROM ({fresh}) __f1) \
           UNION ALL \
           (SELECT * FROM ({fresh}) __f2 \
              EXCEPT ALL SELECT __reflex_src_idx, {col_list} FROM {wrapper}) \
         ) __oracle"
    ))
    .expect("wrapper oracle query failed")
    .expect("wrapper oracle returned NULL");
    assert_eq!(
        mismatches, 0,
        "wrapper '{wrapper}' must mirror its operands exactly ({mismatches} rows differ)"
    );
}

/// Main repro: reconcile an operand BY NAME, over REAL drift — a source mutated
/// with its triggers off, the physical shape of a missed delta and the exact reason
/// an operator reaches for `reflex_reconcile` at all. Both faces are pinned at once:
///
/// - the rebuild must not DOUBLE the wrapper's slice (pre-fix: 5 rows — 2 stale
///   plus 3 re-appended by the mirror the `TRUNCATE` never fired), and
/// - it must RESYNC the slice, so repairing the operand actually repairs the
///   wrapper and the parent (suppressing the mirror without the resync leaves the
///   2 stale rows and a silently wrong parent).
#[pg_test]
fn duo_direct_reconcile_of_operand_does_not_double_wrapper() {
    Spi::run("CREATE TABLE duo1_a (id INT PRIMARY KEY, g INT, v INT)").unwrap();
    Spi::run("CREATE TABLE duo1_b (id INT PRIMARY KEY, g INT, v INT)").unwrap();
    Spi::run("INSERT INTO duo1_a VALUES (1,10,7), (2,20,8)").unwrap();
    Spi::run("INSERT INTO duo1_b VALUES (200,10,3)").unwrap();

    let sql = "WITH u AS ( \
                 SELECT g, v FROM duo1_a \
                 UNION ALL \
                 SELECT g, v FROM duo1_b \
               ) \
               SELECT g, SUM(v) AS s FROM u GROUP BY g";
    assert_eq!(
        crate::create_reflex_ivm("duo1_v", sql, None, None, None, None),
        "CREATE REFLEX INCREMENTAL VIEW"
    );

    let wrapper = "duo1_v__cte_u";
    let operand0 = "duo1_v__cte_u__union_0";
    let operand1 = "duo1_v__cte_u__union_1";

    // Fixture precondition: a MATERIALISED wrapper (stored `__reflex_src_idx`
    // slices), not a VIEW — the shape that can double. A VIEW wrapper stores
    // nothing and is out of scope.
    let is_materialised = Spi::get_one::<bool>(&format!(
        "SELECT EXISTS (SELECT 1 FROM pg_attribute a \
           WHERE a.attrelid = to_regclass('{wrapper}') \
             AND a.attname = '__reflex_src_idx' AND NOT a.attisdropped)"
    ))
    .expect("q")
    .expect("v");
    assert!(
        is_materialised,
        "fixture precondition: '{wrapper}' must be a materialised UNION-ALL wrapper"
    );

    assert_wrapper_mirrors_operands(wrapper, &[operand0, operand1]);
    assert_imv_correct("duo1_v", sql);

    let before = Spi::get_one::<i64>(&format!(
        "SELECT count(*) FROM {wrapper} WHERE __reflex_src_idx = 0"
    ))
    .expect("q")
    .expect("v");
    assert_eq!(before, 2, "fixture precondition: union_0 seeds 2 wrapper rows");

    // Inject REAL drift: mutate the source with its triggers off, so operand 0 and
    // the wrapper both genuinely miss the row. This is what a reconcile is for, and
    // it is what makes the wrapper resync observable — without drift a
    // suppressed-but-unresynced rebuild would leave a coincidentally-correct
    // wrapper and the test would be a false green.
    Spi::run("ALTER TABLE duo1_a DISABLE TRIGGER USER").expect("disable source triggers");
    Spi::run("INSERT INTO duo1_a VALUES (3,30,9)").expect("drifted insert");
    Spi::run("ALTER TABLE duo1_a ENABLE TRIGGER USER").expect("re-enable source triggers");

    let drifted = Spi::get_one::<i64>(&format!(
        "SELECT count(*) FROM {wrapper} WHERE __reflex_src_idx = 0"
    ))
    .expect("q")
    .expect("v");
    assert_eq!(
        drifted, 2,
        "drift precondition: the wrapper must still be missing the row"
    );

    // The operator path under test: naming the internal operand directly.
    let result = Spi::get_one::<String>(&format!("SELECT reflex_reconcile('{operand0}')"))
        .expect("reflex_reconcile must return a value, not raise")
        .expect("reflex_reconcile returned NULL");
    assert_eq!(
        result, "RECONCILED",
        "reconciling an operand must still succeed: {result}"
    );

    let after = Spi::get_one::<i64>(&format!(
        "SELECT count(*) FROM {wrapper} WHERE __reflex_src_idx = 0"
    ))
    .expect("q")
    .expect("v");
    assert_eq!(
        after, 3,
        "the wrapper's union_0 slice must hold exactly the operand's 3 rebuilt rows \
         — not 5 (2 stale + 3 re-appended by the mirror the TRUNCATE never fired), \
         and not 2 (rebuilt with the mirror suppressed but never resynced)"
    );

    assert_wrapper_mirrors_operands(wrapper, &[operand0, operand1]);
    assert_imv_correct("duo1_v", sql);
}

/// `reflex_rebuild_imv` is a literal alias for `reflex_reconcile` (`src/lib.rs`), so
/// it must be equally safe — pinned separately because it is the name every audit
/// remedy string prints, and an alias is exactly the kind of thing a fix routed one
/// level too low would miss.
#[pg_test]
fn duo_direct_rebuild_imv_of_operand_does_not_double_wrapper() {
    Spi::run("CREATE TABLE duo2_a (id INT PRIMARY KEY, g INT, v INT)").unwrap();
    Spi::run("CREATE TABLE duo2_b (id INT PRIMARY KEY, g INT, v INT)").unwrap();
    Spi::run("INSERT INTO duo2_a VALUES (1,10,7), (2,20,8), (3,30,9)").unwrap();
    Spi::run("INSERT INTO duo2_b VALUES (200,10,3)").unwrap();

    let sql = "WITH u AS ( \
                 SELECT g, v FROM duo2_a \
                 UNION ALL \
                 SELECT g, v FROM duo2_b \
               ) \
               SELECT g, SUM(v) AS s FROM u GROUP BY g";
    crate::create_reflex_ivm("duo2_v", sql, None, None, None, None);

    let wrapper = "duo2_v__cte_u";
    Spi::run("SELECT reflex_rebuild_imv('duo2_v__cte_u__union_1')").expect("rebuild operand 1");

    let slice1 = Spi::get_one::<i64>(&format!(
        "SELECT count(*) FROM {wrapper} WHERE __reflex_src_idx = 1"
    ))
    .expect("q")
    .expect("v");
    assert_eq!(
        slice1, 1,
        "reflex_rebuild_imv on operand 1 must leave its wrapper slice at 1 row, not 2"
    );

    assert_wrapper_mirrors_operands(
        wrapper,
        &["duo2_v__cte_u__union_0", "duo2_v__cte_u__union_1"],
    );
    assert_imv_correct("duo2_v", sql);
}

/// `reflex_scheduled_reconcile` reaches the same primitive on its own: an aggregate
/// operand is `REBUILDABLE_NODE`, and the sweep's `covered` CTE deliberately stops
/// at the decomposed wrapper so operands are reconciled STANDALONE (pinned by
/// `pg_scheduled_reconcile_reconciles_operands_below_decomposed_wrapper`). That made
/// every drift sweep over a materialised UNION-ALL chain double the wrapper — the
/// unattended face of the same bug.
#[pg_test]
fn duo_scheduled_sweep_over_union_chain_does_not_double_wrapper() {
    Spi::run("CREATE TABLE duo3_a (id INT PRIMARY KEY, g INT, v INT)").unwrap();
    Spi::run("CREATE TABLE duo3_b (id INT PRIMARY KEY, g INT, v INT)").unwrap();
    Spi::run("INSERT INTO duo3_a VALUES (1,10,7), (2,20,8)").unwrap();
    Spi::run("INSERT INTO duo3_b VALUES (200,10,3), (201,40,4)").unwrap();

    let sql = "WITH u AS ( \
                 SELECT g, v FROM duo3_a \
                 UNION ALL \
                 SELECT g, v FROM duo3_b \
               ) \
               SELECT g, SUM(v) AS s FROM u GROUP BY g";
    crate::create_reflex_ivm("duo3_v", sql, None, None, None, None);

    // Age every node into candidacy — inside one transaction CURRENT_TIMESTAMP is
    // the transaction start, so a row touched here never looks stale otherwise.
    Spi::run(
        "UPDATE public.__reflex_ivm_reference \
            SET last_update_date = TIMESTAMP '2001-01-01 00:00:00' \
          WHERE name LIKE 'duo3_v%'",
    )
    .expect("backdate");

    Spi::run("SELECT * FROM reflex_scheduled_reconcile(1)").expect("scheduled reconcile");

    assert_wrapper_mirrors_operands(
        "duo3_v__cte_u",
        &["duo3_v__cte_u__union_0", "duo3_v__cte_u__union_1"],
    );
    assert_imv_correct("duo3_v", sql);
}

/// The routing's own `ALTER TABLE … DISABLE/ENABLE TRIGGER USER` must not turn a
/// plain `reflex_reconcile` into an aborted transaction under
/// `alter_source_policy = 'error'`. That DDL targets a relation the wrapper's
/// `depends_on` names, so the `reflex_on_ddl_command_end` alarm fires on it and
/// RAISEs under that policy — which is why the routing brackets itself with the
/// wrapper as internal reconcile root. Without the bracket this fix would trade a
/// silent doubling for a loud, unrelated failure.
#[pg_test]
fn duo_operand_reconcile_survives_alter_source_policy_error() {
    Spi::run("CREATE TABLE duo5_a (id INT PRIMARY KEY, g INT, v INT)").unwrap();
    Spi::run("CREATE TABLE duo5_b (id INT PRIMARY KEY, g INT, v INT)").unwrap();
    Spi::run("INSERT INTO duo5_a VALUES (1,10,7)").unwrap();
    Spi::run("INSERT INTO duo5_b VALUES (200,10,3)").unwrap();

    let sql = "WITH u AS ( \
                 SELECT g, v FROM duo5_a \
                 UNION ALL \
                 SELECT g, v FROM duo5_b \
               ) \
               SELECT g, SUM(v) AS s FROM u GROUP BY g";
    crate::create_reflex_ivm("duo5_v", sql, None, None, None, None);

    Spi::run("SET LOCAL pg_reflex.alter_source_policy = 'error'").expect("set policy");
    let result = Spi::get_one::<String>("SELECT reflex_reconcile('duo5_v__cte_u__union_0')")
        .expect("reconciling an operand under alter_source_policy='error' must not raise")
        .expect("v");
    assert_eq!(result, "RECONCILED", "unexpected result: {result}");

    assert_wrapper_mirrors_operands(
        "duo5_v__cte_u",
        &["duo5_v__cte_u__union_0", "duo5_v__cte_u__union_1"],
    );
    assert_imv_correct("duo5_v", sql);
}

/// The routing must not change anything for a NON-operand target: a plain aggregate
/// IMV that no wrapper depends on still takes the ordinary path and still
/// reconciles to a correct result.
#[pg_test]
fn duo_plain_imv_reconcile_unaffected_by_operand_routing() {
    Spi::run("CREATE TABLE duo4_s (id INT PRIMARY KEY, g INT, v INT)").unwrap();
    Spi::run("INSERT INTO duo4_s VALUES (1,10,7), (2,20,8), (3,10,1)").unwrap();

    let sql = "SELECT g, SUM(v) AS s FROM duo4_s GROUP BY g";
    crate::create_reflex_ivm("duo4_v", sql, None, None, None, None);

    let result = Spi::get_one::<String>("SELECT reflex_reconcile('duo4_v')")
        .expect("q")
        .expect("v");
    assert_eq!(result, "RECONCILED");
    assert_imv_correct("duo4_v", sql);

    let is_operand = Spi::get_one::<bool>(
        "SELECT EXISTS (SELECT 1 FROM public.__reflex_ivm_reference w \
           WHERE 'duo4_v' = ANY(COALESCE(w.depends_on_imv, ARRAY[]::TEXT[])))",
    )
    .expect("q")
    .expect("v");
    assert!(
        !is_operand,
        "fixture precondition: duo4_v must not be anybody's operand, so the \
         routing branch is not what made this pass"
    );
}

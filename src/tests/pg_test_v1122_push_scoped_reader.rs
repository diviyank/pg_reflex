// 1.12.2: a partition rebuild that diffed every leaf refreshes, by key, the
// DEFERRED aggregates reading the rebuilt IMV whose rows staged for them it can
// place exactly, instead of leaving them to apply the whole row diff at COMMIT.
// Chain and helpers: pg_test_v112_swap_cascade.rs.

const PSR_SUM_SQL: &str =
    "SELECT plan, yr, SUM(quantity) AS q, COUNT(*) AS n FROM {sfv} AS sf GROUP BY plan, yr";
const PSR_JOINED_SQL: &str = "SELECT sf.plan, SUM(sf.quantity * w.w) AS q, COUNT(*) AS n \
     FROM {sfv} AS sf JOIN sc_w AS w ON w.loc = sf.loc GROUP BY plan";
const PSR_IMMEDIATE_SQL: &str =
    "SELECT plan, yr, SUM(quantity) AS q, COUNT(*) AS n FROM {sfv} AS sf GROUP BY plan, yr";
const PSR_BY_YEAR_SQL: &str =
    "SELECT yr, SUM(quantity) AS q, COUNT(*) AS n FROM {sfv} AS sf GROUP BY yr";

/// The readers of `sc_sfv` this file adds to the DEFERRED chain, each with its
/// refresh mode and query over `{sfv}`.
const PSR_READERS: &[(&str, &str, &str)] = &[
    ("psr_sum", "DEFERRED", PSR_SUM_SQL),
    ("psr_joined", "DEFERRED", PSR_JOINED_SQL),
    ("psr_immediate", "IMMEDIATE", PSR_IMMEDIATE_SQL),
    ("psr_by_year", "DEFERRED", PSR_BY_YEAR_SQL),
];

fn psr_open(dbname: &str) -> ScChain {
    let chain = ScChain { field_names: false, cross_plan: false };
    sc_open(dbname, "DEFERRED", chain);
    worker_exec("CREATE TABLE sc_w (loc INT PRIMARY KEY, w INT NOT NULL)");
    worker_exec("INSERT INTO sc_w VALUES (1, 2), (2, 3), (3, 5)");
    for (name, mode, sql) in PSR_READERS {
        chain.create(&format!(
            "create_reflex_ivm('{name}', {}, NULL, NULL, '{mode}', NULL, ARRAY[]::text[])",
            sql_lit(&sql.replace("{sfv}", "sc_sfv"))
        ));
    }
    // The COMMIT flush of these small aggregates would otherwise rebuild them
    // whole (high selectivity): a reader's refresh is then the push's doing.
    for name in ["sc_fdya", "psr_sum", "psr_joined", "psr_immediate", "psr_by_year"] {
        rbc_select(&format!("reflex_set_wipe_threshold('{name}', 1000000000)"));
    }
    chain
}

/// The nodes of the chain and of this file's readers that do not match their
/// query, settled as [`ScChain::wrong_nodes`] does.
fn psr_wrong(chain: &ScChain) -> Vec<String> {
    sc_settled(|| {
        let mut wrong = chain.oracle_mismatches(&[]);
        for (name, _, sql) in PSR_READERS {
            let fresh = sql.replace("{sfv}", &sc_sfv_inline());
            let mismatches = rbc_mismatch(name, &fresh);
            if mismatches != 0 {
                wrong.push(format!("{name}: {mismatches} mismatches"));
            }
        }
        wrong
    })
}

fn psr_commit(chain: &ScChain, statements: &str) -> Result<(), String> {
    chain.commit(statements)
}

fn psr_flush_count(imv: &str) -> i64 {
    worker_scalar_i64(&format!(
        "SELECT COALESCE(flush_count, 0)::int8 FROM public.__reflex_ivm_reference \
         WHERE name = '{imv}'"
    ))
}

fn psr_refreshes_of(chain: &ScChain, imv: &str) -> Vec<String> {
    chain
        .refreshes()
        .into_iter()
        .filter(|(node, _)| node == imv)
        .map(|(_, kind)| kind)
        .collect()
}

/// Rows of plan `plan` changed by an ordinary write, applied to the passthrough
/// by an explicit flush, so that its readers' deltas are staged in the
/// transaction before whatever follows.
fn psr_staged_write(plan: i32, product: i32, by: i32) -> String {
    format!(
        "UPDATE sc_ss SET qty = qty + {by} WHERE plan = {plan} AND product = {product}; \
         SELECT reflex_flush_deferred('sc_ss')"
    )
}

/// Step 0.2: the existing contract a key refresh of a DEFERRED reader relies
/// on. A leaf filled from empty is not diffed, so the readers of the passthrough
/// are refreshed by key; deltas staged before that refresh for the refreshed
/// plan are skipped, those of another plan and those staged after it applied.
#[pg_test]
fn v1122_psr_key_refresh_skips_only_the_deltas_it_reflects() {
    let chain = psr_open("reflex_psr_s0");
    chain
        .commit(&sc_swap(3, 2, "SELECT * FROM sc_ss WHERE FALSE"))
        .expect("empty a leaf");
    let statements = format!(
        "{}; {}; {}; {SC_FLUSH}; {}",
        psr_staged_write(3, 1, 11),
        psr_staged_write(2, 1, 13),
        sc_swap(3, 2, &sc_leaf_rows(3, 2, 4, 2)),
        "UPDATE sc_ss SET qty = qty + 17 WHERE plan = 3 AND product = 2"
    );
    let outcome = psr_commit(&chain, &statements);
    let sum_refreshes = psr_refreshes_of(&chain, "psr_sum");
    let wrong = psr_wrong(&chain);
    probe_db_close("reflex_psr_s0");
    assert_eq!(outcome, Ok(()));
    assert_eq!(sum_refreshes, vec!["groups".to_string()], "fixture: psr_sum refreshed by key");
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

/// T1, T2: a push that diffs every leaf refreshes the DEFERRED aggregates of the
/// passthrough by plan, and their COMMIT flush has nothing left to apply.
fn psr_assert_push_refreshes_the_aggregates_by_key(dbname: &str, statements: &str) {
    let chain = psr_open(dbname);
    let flushes_before = [psr_flush_count("sc_fdya"), psr_flush_count("psr_sum")];
    let outcome = psr_commit(&chain, statements);
    let flushes_after = [psr_flush_count("sc_fdya"), psr_flush_count("psr_sum")];
    let refreshes = [psr_refreshes_of(&chain, "sc_fdya"), psr_refreshes_of(&chain, "psr_sum")];
    let wrong = psr_wrong(&chain);
    probe_db_close(dbname);
    assert_eq!(outcome, Ok(()));
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
    let groups = vec!["groups".to_string()];
    assert_eq!(refreshes, [groups.clone(), groups], "[sc_fdya, psr_sum] refreshes");
    assert_eq!(
        flushes_after, flushes_before,
        "[sc_fdya, psr_sum] flush_count: the COMMIT flush applied no delta"
    );
}

#[pg_test]
fn v1122_psr_push_of_one_leaf_refreshes_the_aggregates_by_key() {
    psr_assert_push_refreshes_the_aggregates_by_key(
        "reflex_psr_t1a",
        &sc_swap(2, 2, &sc_leaf_rows(2, 2, 4, 7)),
    );
}

#[pg_test]
fn v1122_psr_push_of_three_leaves_refreshes_the_aggregates_by_key() {
    psr_assert_push_refreshes_the_aggregates_by_key("reflex_psr_t1b", &sc_three_leaves(2, 5));
}

#[pg_test]
fn v1122_psr_explicit_push_of_two_plans_refreshes_the_aggregates_by_key() {
    psr_assert_push_refreshes_the_aggregates_by_key(
        "reflex_psr_t1c",
        &format!(
            "{}; {}; {SC_FLUSH}",
            sc_swap(1, 2, &sc_leaf_rows(1, 2, 4, 5)),
            sc_swap(3, 4, &sc_leaf_rows(3, 4, 6, 5))
        ),
    );
}

/// T3: deltas staged in the push's transaction before it, for the pushed plan
/// and for another, and a write after it, all reach the aggregates once.
#[pg_test]
fn v1122_psr_writes_around_the_push_reach_the_aggregates_once() {
    let chain = psr_open("reflex_psr_t3");
    let statements = format!(
        "{}; {}; {}; {SC_FLUSH}; {}",
        psr_staged_write(2, 1, 11),
        psr_staged_write(3, 1, 13),
        sc_swap(2, 2, &sc_leaf_rows(2, 2, 4, 7)),
        "UPDATE sc_ss SET qty = qty + 17 WHERE plan IN (2, 3) AND product = 2"
    );
    let outcome = psr_commit(&chain, &statements);
    let refreshes = psr_refreshes_of(&chain, "psr_sum");
    let wrong = psr_wrong(&chain);
    probe_db_close("reflex_psr_t3");
    assert_eq!(outcome, Ok(()));
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
    assert_eq!(refreshes, vec!["groups".to_string()], "psr_sum refreshed by key");
}

/// T4, T5, T6: a DEFERRED reader joining another table (its staged rows cannot
/// be placed by plan), an IMMEDIATE reader, and a reader not grouped by plan
/// keep receiving the row diff: none is refreshed, all stay correct.
#[pg_test]
fn v1122_psr_readers_the_push_cannot_scope_keep_the_row_diff() {
    let chain = psr_open("reflex_psr_t4");
    let mut failures = Vec::new();
    for (label, statements) in [
        ("one leaf", sc_swap(2, 2, &sc_leaf_rows(2, 2, 4, 7))),
        (
            "write then push",
            format!(
                "{}; {}; {SC_FLUSH}",
                psr_staged_write(2, 1, 11),
                sc_swap(2, 3, &sc_leaf_rows(2, 3, 4, 7))
            ),
        ),
    ] {
        if let Err(error) = psr_commit(&chain, &statements) {
            failures.push(format!("{label}: raised {error}"));
            continue;
        }
        for reader in ["psr_joined", "psr_immediate", "psr_by_year"] {
            let refreshes = psr_refreshes_of(&chain, reader);
            if !refreshes.is_empty() {
                failures.push(format!("{label}: {reader} refreshed {refreshes:?}"));
            }
        }
        for wrong in psr_wrong(&chain) {
            failures.push(format!("{label}: {wrong}"));
        }
    }
    probe_db_close("reflex_psr_t4");
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// T7: a key refresh the push starts that fails falls back to the full
/// reconcile of the reader, which ends correct and not stale.
#[pg_test]
fn v1122_psr_failed_key_refresh_falls_back_to_a_full_reconcile() {
    let chain = psr_open("reflex_psr_t7");
    for ddl in [
        "CREATE FUNCTION psr_refuse_key_refresh() RETURNS TRIGGER LANGUAGE plpgsql AS $$ \
           DECLARE stack TEXT; BEGIN GET DIAGNOSTICS stack = PG_CONTEXT; \
             IF stack LIKE '%reflex_scoped_cascade%' AND stack NOT LIKE '%SELECT public.reflex_reconcile(%' THEN \
               RAISE EXCEPTION 'psr: key refresh refused'; END IF; RETURN NULL; END $$",
        "CREATE TRIGGER psr_refuse_key_refresh AFTER INSERT OR DELETE ON psr_sum \
           FOR EACH STATEMENT EXECUTE FUNCTION psr_refuse_key_refresh()",
    ] {
        worker_exec(ddl);
    }
    let outcome = psr_commit(&chain, &sc_swap(2, 2, &sc_leaf_rows(2, 2, 4, 7)));
    let refreshes = psr_refreshes_of(&chain, "psr_sum");
    let stale = rbc_known_stale("psr_sum");
    let wrong = psr_wrong(&chain);
    probe_db_close("reflex_psr_t7");
    assert_eq!(outcome, Ok(()));
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
    assert!(!stale, "psr_sum reported stale");
    assert!(
        refreshes.iter().any(|kind| kind == "full"),
        "psr_sum's failed key refresh falls back to a full reconcile: {refreshes:?}"
    );
}



/// Step 0.4 guard: a push whose diff staged fewer rows than half the rows of
/// the plans a refresh would rescan leaves the aggregates to apply those rows
/// at COMMIT, as before; both stay correct.
#[pg_test]
fn v1122_psr_low_churn_push_leaves_the_aggregates_to_the_commit_flush() {
    let chain = psr_open("reflex_psr_t8");
    let one_product_changed = "SELECT plan, d, yr, product, loc, \
           qty + CASE WHEN product = 1 THEN 3 ELSE 0 END, base FROM sc_ss_p_2_2027_02";
    let flushes_before = psr_flush_count("psr_sum");
    let outcome = psr_commit(&chain, &sc_swap(2, 2, one_product_changed));
    let flushes_after = psr_flush_count("psr_sum");
    let refreshes = [psr_refreshes_of(&chain, "sc_fdya"), psr_refreshes_of(&chain, "psr_sum")];
    let wrong = psr_wrong(&chain);
    probe_db_close("reflex_psr_t8");
    assert_eq!(outcome, Ok(()));
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
    assert_eq!(refreshes, [Vec::<String>::new(), Vec::new()], "[sc_fdya, psr_sum] refreshes");
    assert_eq!(flushes_after, flushes_before + 1, "psr_sum applied the row diff at COMMIT");
}

/// A partition rebuild run inside a trigger, while the statement may still
/// stage deltas, leaves a DEFERRED aggregate of the rebuilt IMV to its COMMIT
/// flush: a refresh of its groups there would only list it for a full rebuild.
#[pg_test]
fn v1122_psr_rebuild_inside_a_trigger_leaves_the_aggregate_to_its_flush() {
    probe_db_open("reflex_psr_t9");
    let chain = ScChain { field_names: false, cross_plan: false };
    for ddl in [
        "CREATE TABLE hs (id INT NOT NULL, region TEXT NOT NULL, amount INT NOT NULL) \
           PARTITION BY LIST (region)",
        "CREATE TABLE hs_a PARTITION OF hs FOR VALUES IN ('A')",
        "CREATE TABLE hs_b PARTITION OF hs FOR VALUES IN ('B')",
        "INSERT INTO hs SELECT g, r, g FROM unnest(ARRAY['A', 'B']) r, generate_series(1, 20) g",
        "CREATE TABLE kick (n INT)",
        "CREATE TABLE sc_trace (seq SERIAL, name TEXT NOT NULL, context TEXT NOT NULL)",
        "CREATE TABLE sc_xact (id TEXT NOT NULL)",
    ] {
        worker_exec(ddl);
    }
    for call in [
        "create_reflex_ivm('hv', 'SELECT region, SUM(amount) AS total, COUNT(*) AS n FROM hs \
           GROUP BY region', NULL, NULL, 'IMMEDIATE', NULL, ARRAY['region'])",
        "create_reflex_ivm('hg', 'SELECT region, SUM(total) AS t, COUNT(*) AS n FROM hv \
           GROUP BY region', NULL, NULL, 'DEFERRED', NULL, ARRAY[]::text[])",
        "reflex_set_wipe_threshold('hg', 1000000000)",
    ] {
        assert!(!rbc_select(call).starts_with("ERROR"), "{call}");
    }
    for ddl in [
        "CREATE FUNCTION kick_reconcile() RETURNS TRIGGER LANGUAGE plpgsql AS $$ \
           BEGIN PERFORM public.reflex_reconcile_partition('hv', 'A'); RETURN NULL; END $$",
        "CREATE TRIGGER kick_reconcile AFTER INSERT ON kick \
           FOR EACH STATEMENT EXECUTE FUNCTION kick_reconcile()",
        "CREATE FUNCTION sc_note_refresh() RETURNS TRIGGER LANGUAGE plpgsql AS $$ \
           DECLARE stack TEXT; BEGIN GET DIAGNOSTICS stack = PG_CONTEXT; \
             INSERT INTO sc_trace (name, context) VALUES (NEW.name, stack); RETURN NULL; END $$",
        "CREATE TRIGGER sc_note_refresh AFTER UPDATE OF last_update_date \
           ON public.__reflex_ivm_reference FOR EACH ROW EXECUTE FUNCTION sc_note_refresh()",
    ] {
        worker_exec(ddl);
    }
    let outcome = chain
        .commit("UPDATE hs SET amount = amount + 7 WHERE region = 'A'; INSERT INTO kick VALUES (1)");
    let refreshes = psr_refreshes_of(&chain, "hg");
    let mismatches = rbc_mismatch(
        "hg",
        "SELECT region, SUM(total) AS t, COUNT(*) AS n FROM \
           (SELECT region, SUM(amount) AS total FROM hs GROUP BY region) AS v GROUP BY region",
    );
    probe_db_close("reflex_psr_t9");
    assert_eq!(outcome, Ok(()));
    assert_eq!(mismatches, 0, "hg");
    assert!(refreshes.is_empty(), "hg is left to its COMMIT flush: {refreshes:?}");
}

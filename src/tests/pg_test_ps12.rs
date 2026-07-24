// PS-12 — a decomposed WRAPPER node reaching `reconcile_one` corrupts or aborts.
//
// Two reports, one root cause. A wrapper (`end_query = ''`, `aggregations = '{}'`)
// is a set-op / DISTINCT ON / window VIEW, or a materialised UNION-ALL mirror
// TABLE, maintained through its operands — never rebuildable by
// `reconcile_one`'s passthrough branch (`TRUNCATE` + `INSERT <base_query>`):
//   * on a VIEW wrapper the `TRUNCATE` RAISES `"<v>" is not a table`, aborting the
//     caller's whole transaction (#7);
//   * on a materialised wrapper the extra leading `__reflex_src_idx` column makes
//     `INSERT INTO <wrapper> <base_query>` mis-shape the row — silent wrong data
//     that propagates to the parent (#8).
//
// The fix is a backstop in `reconcile_one` that refuses a wrapper with a clean
// error STRING (not a raise), before heal / sync / TRUNCATE.
//
// DISCRIMINATOR. The pre-spec proposed `is_generated_sub_imv = TRUE AND
// aggregations = '{}'`. That is WRONG for the shape #7 actually reproduces: a
// TOP-LEVEL UNION-ALL / set-op / DISTINCT-ON / window IMV registers its wrapper
// under the USER's own name (`ctx.view_name`), which `decompose.rs` never marks
// `is_generated_sub_imv` — only operand / CTE sub-nodes are marked. So the correct
// discriminator is the one `ImvRow::is_decomposed_wrapper` (`src/audit/mod.rs`)
// already uses: `end_query = '' AND aggregations = '{}'` (a plain equality, not
// `COALESCE(…, '{}')`, so a NULL-plan legacy row of UNKNOWN shape is NOT refused).
// The survey test below proves the discriminator over every wrapper shape and
// proves `is_generated_sub_imv` is FALSE for the top-level VIEW wrappers.

/// The registry discriminator, read straight from the row: `TRUE` iff the row is
/// a decomposed wrapper by the same test the backstop applies.
fn ps12_is_wrapper_row(name: &str) -> bool {
    Spi::get_one::<bool>(&format!(
        "SELECT end_query = '' AND aggregations::text = '{{}}' \
         FROM public.__reflex_ivm_reference WHERE name = '{name}'"
    ))
    .expect("registry query failed")
    .expect("no registry row")
}

fn ps12_is_generated(name: &str) -> bool {
    Spi::get_one::<bool>(&format!(
        "SELECT COALESCE(is_generated_sub_imv, FALSE) \
         FROM public.__reflex_ivm_reference WHERE name = '{name}'"
    ))
    .expect("registry query failed")
    .expect("no registry row")
}

// --- (A) The discriminator survey ----------------------------------------
//
// One test that builds every wrapper shape plus every non-wrapper shape and pins
// the discriminator's value on each — the empirical survey the pre-spec asks for.

/// (A) Every wrapper shape satisfies `end_query = '' AND aggregations = '{}'`, and
/// every legitimately-reconcilable shape does NOT. Also records, per shape,
/// whether `is_generated_sub_imv` holds — the column the pre-spec's discriminator
/// relied on and that this test shows is FALSE for the top-level VIEW wrappers,
/// which is why it cannot be the discriminator.
#[pg_test]
fn ps12_wrapper_discriminator_survey() {
    // Shape 1: top-level UNION ALL -> VIEW wrapper, user's name.
    Spi::run("CREATE TABLE ps12_ua_a (id BIGINT, v NUMERIC)").expect("ua a");
    Spi::run("CREATE TABLE ps12_ua_b (id BIGINT, v NUMERIC)").expect("ua b");
    Spi::run("INSERT INTO ps12_ua_a VALUES (1,10),(2,20)").expect("seed ua a");
    Spi::run("INSERT INTO ps12_ua_b VALUES (3,30)").expect("seed ua b");
    Spi::run(
        "SELECT create_reflex_ivm('ps12_ua', \
           'SELECT id, v FROM ps12_ua_a UNION ALL SELECT id, v FROM ps12_ua_b', 'id')",
    )
    .expect("create UNION ALL");

    // Shape 2: top-level UNION (dedup) -> VIEW wrapper.
    Spi::run(
        "SELECT create_reflex_ivm('ps12_un', \
           'SELECT id, v FROM ps12_ua_a UNION SELECT id, v FROM ps12_ua_b', 'id')",
    )
    .expect("create UNION");

    // Shape 3: top-level EXCEPT -> VIEW wrapper.
    Spi::run(
        "SELECT create_reflex_ivm('ps12_ex', \
           'SELECT id, v FROM ps12_ua_a EXCEPT SELECT id, v FROM ps12_ua_b', 'id')",
    )
    .expect("create EXCEPT");

    // Shape 4 + 5: DISTINCT ON and window -> VIEW wrappers.
    Spi::run("CREATE TABLE ps12_dw (grp BIGINT, id BIGINT, v NUMERIC)").expect("dw");
    Spi::run("INSERT INTO ps12_dw VALUES (1,1,10),(1,2,20),(2,3,30)").expect("seed dw");
    Spi::run(
        "SELECT create_reflex_ivm('ps12_don', \
           'SELECT DISTINCT ON (grp) grp, id, v FROM ps12_dw ORDER BY grp, v DESC', 'grp')",
    )
    .expect("create DISTINCT ON");
    Spi::run(
        "SELECT create_reflex_ivm('ps12_win', \
           'SELECT grp, id, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY id) AS rn \
            FROM ps12_dw', 'grp,id')",
    )
    .expect("create window");

    // Shape 6: materialised UNION-ALL wrapper (CTE body consumed by an aggregate).
    Spi::run("CREATE TABLE ps12_mw_a (id BIGINT, v NUMERIC)").expect("mw a");
    Spi::run("CREATE TABLE ps12_mw_b (id BIGINT, v NUMERIC)").expect("mw b");
    Spi::run("INSERT INTO ps12_mw_a VALUES (1,10)").expect("seed mw a");
    Spi::run("INSERT INTO ps12_mw_b VALUES (2,20)").expect("seed mw b");
    Spi::run(
        "SELECT create_reflex_ivm('ps12_mw', \
           'WITH u AS (SELECT id, v FROM ps12_mw_a UNION ALL SELECT id, v FROM ps12_mw_b) \
            SELECT id, SUM(v) AS total FROM u GROUP BY id', 'id')",
    )
    .expect("create CTE-over-UNION-ALL");

    // Non-wrapper A: ordinary passthrough (end_query='' but a REAL plan).
    Spi::run("CREATE TABLE ps12_pt (id BIGINT, v NUMERIC)").expect("pt");
    Spi::run("INSERT INTO ps12_pt VALUES (1,10)").expect("seed pt");
    Spi::run("SELECT create_reflex_ivm('ps12_pt_v', 'SELECT id, v FROM ps12_pt', 'id')")
        .expect("create passthrough");

    // Non-wrapper B: ordinary aggregate (non-empty end_query).
    Spi::run("CREATE TABLE ps12_ag (region TEXT, amount NUMERIC)").expect("ag");
    Spi::run("INSERT INTO ps12_ag VALUES ('us',100),('eu',200)").expect("seed ag");
    Spi::run(
        "SELECT create_reflex_ivm('ps12_ag_v', \
           'SELECT region, SUM(amount) AS total FROM ps12_ag GROUP BY region')",
    )
    .expect("create aggregate");

    // Non-wrapper C: a CTE sub-IMV that carries a REAL aggregations plan — the
    // shape the backstop must NOT refuse. A CTE aggregate produces a generated
    // sub-IMV `<view>__cte_agg` with a full plan.
    Spi::run(
        "SELECT create_reflex_ivm('ps12_cte_v', \
           'WITH agg AS (SELECT region, SUM(amount) AS total FROM ps12_ag GROUP BY region) \
            SELECT region, total FROM agg WHERE total > 0', 'region')",
    )
    .expect("create CTE-aggregate");

    // --- Wrappers: discriminator TRUE for all six. ---
    for w in [
        "ps12_ua",
        "ps12_un",
        "ps12_ex",
        "ps12_don",
        "ps12_win",
        "ps12_mw__cte_u",
    ] {
        assert!(
            ps12_is_wrapper_row(w),
            "{w} must satisfy the wrapper discriminator (end_query='' AND aggregations='{{}}')"
        );
    }

    // The top-level VIEW wrappers are the USER's IMV — NOT generated. This is the
    // pre-spec's error: `is_generated_sub_imv` is FALSE here, so the proposed
    // `is_generated AND aggregations='{}'` discriminator would MISS them.
    for w in ["ps12_ua", "ps12_un", "ps12_ex", "ps12_don", "ps12_win"] {
        assert!(
            !ps12_is_generated(w),
            "{w} is the user's top-level IMV — must NOT be is_generated_sub_imv, \
             which is why is_generated cannot be the discriminator"
        );
    }
    // Only the materialised wrapper (a CTE node) is generated.
    assert!(
        ps12_is_generated("ps12_mw__cte_u"),
        "the materialised CTE wrapper is a generated node"
    );

    // --- Non-wrappers: discriminator FALSE for all. ---
    for nw in ["ps12_pt_v", "ps12_ag_v", "ps12_cte_v", "ps12_cte_v__cte_agg"] {
        assert!(
            !ps12_is_wrapper_row(nw),
            "{nw} is legitimately reconcilable — must NOT match the wrapper discriminator"
        );
    }
    // The CTE aggregate sub-IMV carries a real plan (its aggregations is non-empty
    // AND its end_query is non-empty).
    assert!(
        ps12_is_generated("ps12_cte_v__cte_agg"),
        "fixture precondition: the CTE aggregate sub-IMV is a generated node"
    );
}

// --- (1) #7: VIEW wrapper — refuse without aborting the transaction -------

/// (1) `reflex_reconcile` on a top-level UNION-ALL VIEW wrapper returns a clean
/// error STRING and does NOT abort the transaction.
///
/// RED state (before the backstop): the call RAISES `"ps12_r1" is not a table`
/// from the passthrough branch's `TRUNCATE` on a VIEW, which aborts this test's
/// transaction — the test cannot even reach its assertions. GREEN: a returned
/// string, and a trivial statement after it proves the transaction is alive.
#[pg_test]
fn ps12_reconcile_refuses_view_wrapper_without_aborting_tx() {
    ps10_make_union_all_fixture("ps12_r1_a", "ps12_r1_b", "ps12_r1");

    let result = Spi::get_one::<String>("SELECT reflex_reconcile('ps12_r1')")
        .expect("reflex_reconcile must return a value, not raise")
        .expect("NULL reconcile result");
    assert!(
        result.starts_with("ERROR"),
        "a VIEW wrapper reconcile must return an ERROR string, got: {result}"
    );
    assert!(
        result.contains("wrapper") && result.contains("operand"),
        "the refusal must name why (a wrapper) and what to do (its operands): {result}"
    );

    // The transaction must still be alive — a raise would have aborted it and this
    // would never run.
    let alive = Spi::get_one::<i32>("SELECT 42")
        .expect("tx must be alive after a refused reconcile")
        .expect("NULL");
    assert_eq!(alive, 42, "the caller's transaction must survive the refusal");

    // And the wrapper is still correct — refusing changed nothing.
    assert_eq!(
        ps10_diff_count(
            "SELECT id, v FROM ps12_r1",
            "SELECT id, v FROM ps12_r1_a UNION ALL SELECT id, v FROM ps12_r1_b"
        ),
        0,
        "the refused wrapper must be unchanged and correct"
    );
}

/// (1b) The same refusal for `reflex_rebuild_imv`, the literal alias operators
/// reach for (`src/lib.rs`).
#[pg_test]
fn ps12_rebuild_imv_refuses_view_wrapper() {
    ps10_make_union_all_fixture("ps12_r1b_a", "ps12_r1b_b", "ps12_r1b");
    let result = Spi::get_one::<String>("SELECT reflex_rebuild_imv('ps12_r1b')")
        .expect("reflex_rebuild_imv must return a value, not raise")
        .expect("NULL");
    assert!(
        result.starts_with("ERROR") && result.contains("wrapper"),
        "reflex_rebuild_imv must refuse a VIEW wrapper the same way: {result}"
    );
}

// --- (2) #8: materialised wrapper — refuse, no column-shift, no doubling --

/// (2) A direct `reconcile_one` on a materialised UNION-ALL wrapper refuses and
/// leaves the wrapper byte-for-byte unchanged — no column-shift, no doubling.
///
/// Uses `crate_test_reconcile_one` to hit `reconcile_one` in isolation: the
/// operator entry point would first reconcile the operand sub-IMVs, whose INSERT
/// mirror triggers append to the wrapper (the pre-existing doubling hazard), which
/// would confound "did the backstop leave the wrapper alone?".
///
/// RED state (before the backstop): the passthrough branch TRUNCATEs the wrapper
/// then runs `INSERT INTO <wrapper> SELECT * FROM <operand> …`, feeding N payload
/// columns into the wrapper's N+1 columns (`__reflex_src_idx` + payload) — either a
/// raise or a shifted row. Either way the wrapper is wrong.
#[pg_test]
fn ps12_reconcile_one_refuses_materialized_wrapper_no_shift() {
    Spi::run("CREATE TABLE ps12_r2_a (id BIGINT, v NUMERIC)").expect("a");
    Spi::run("CREATE TABLE ps12_r2_b (id BIGINT, v NUMERIC)").expect("b");
    Spi::run("INSERT INTO ps12_r2_a VALUES (1,10),(3,30)").expect("seed a");
    Spi::run("INSERT INTO ps12_r2_b VALUES (2,20)").expect("seed b");
    Spi::run(
        "SELECT create_reflex_ivm('ps12_r2', \
           'WITH u AS (SELECT id, v FROM ps12_r2_a UNION ALL SELECT id, v FROM ps12_r2_b) \
            SELECT id, SUM(v) AS total FROM u GROUP BY id', 'id')",
    )
    .expect("create CTE-over-UNION-ALL");

    let wrapper = "ps12_r2__cte_u";
    let is_table = ps10_count(&format!(
        "SELECT count(*) FROM pg_class WHERE relname = '{wrapper}' AND relkind = 'r'"
    ));
    assert_eq!(is_table, 1, "fixture precondition: wrapper is a materialised TABLE");
    let has_src_idx = ps10_count(&format!(
        "SELECT count(*) FROM pg_attribute \
         WHERE attrelid = '{wrapper}'::regclass AND attname = '__reflex_src_idx'"
    ));
    assert_eq!(
        has_src_idx, 1,
        "fixture precondition: the wrapper carries the extra __reflex_src_idx column"
    );

    // The wrapper's correct payload BEFORE any reconcile attempt.
    let rows_before = ps10_count(&format!("SELECT count(*) FROM {wrapper}"));
    assert_eq!(rows_before, 3, "fixture precondition: 3 mirrored operand rows");

    let result =
        Spi::get_one::<String>(&format!("SELECT tests.crate_test_reconcile_one('{wrapper}'::text)"))
            .expect("reconcile_one must return a value, not raise")
            .expect("NULL");
    assert!(
        result.starts_with("ERROR") && result.contains("wrapper"),
        "reconcile_one must refuse a materialised wrapper: {result}"
    );

    // No doubling, no shift: same row count, and the payload still equals the union
    // of the operands. A column-shift would have moved id into __reflex_src_idx and
    // v into id, so comparing (id, v) against the source union catches it.
    assert_eq!(
        ps10_count(&format!("SELECT count(*) FROM {wrapper}")),
        rows_before,
        "the refused wrapper must not double or lose rows"
    );
    assert_eq!(
        ps10_diff_count(
            &format!("SELECT id, v FROM {wrapper}"),
            "SELECT id, v FROM ps12_r2_a UNION ALL SELECT id, v FROM ps12_r2_b"
        ),
        0,
        "the refused wrapper payload must be unchanged (no column-shift)"
    );
    // And the parent aggregate, fed from the wrapper, is still correct.
    assert_eq!(
        ps10_diff_count(
            "SELECT id, total FROM ps12_r2",
            "SELECT id, SUM(v) FROM (SELECT id, v FROM ps12_r2_a \
             UNION ALL SELECT id, v FROM ps12_r2_b) s GROUP BY id"
        ),
        0,
        "the parent aggregate must stay correct"
    );
}

// --- (3) Step 0: scheduled sweep leaves a stale wrapper untouched ----------

/// (3) A wrapper aged stale while its parent is FRESH is never a scheduled-sweep
/// candidate — the candidate CTE's `aggregations <> '{}'` filter (PS-10,
/// `src/reconcile.rs`) excludes it regardless of its parent's age. This is #8's
/// original path; the test proves PS-10 already closed it.
#[pg_test]
fn ps12_scheduled_sweep_skips_stale_wrapper_with_fresh_parent() {
    Spi::run("CREATE TABLE ps12_s3_a (id BIGINT, v NUMERIC)").expect("a");
    Spi::run("CREATE TABLE ps12_s3_b (id BIGINT, v NUMERIC)").expect("b");
    Spi::run("INSERT INTO ps12_s3_a VALUES (1,10),(3,30)").expect("seed a");
    Spi::run("INSERT INTO ps12_s3_b VALUES (2,20)").expect("seed b");
    Spi::run(
        "SELECT create_reflex_ivm('ps12_s3', \
           'WITH u AS (SELECT id, v FROM ps12_s3_a UNION ALL SELECT id, v FROM ps12_s3_b) \
            SELECT id, SUM(v) AS total FROM u GROUP BY id', 'id')",
    )
    .expect("create CTE-over-UNION-ALL");

    let wrapper = "ps12_s3__cte_u";

    // Age the WRAPPER stale (a year ago) while the parent stays fresh (now). The
    // parent must not be a candidate; the wrapper must be old enough to be one on
    // the age gate alone — so the ONLY thing that can keep it out of the batch is
    // the plan filter.
    Spi::run(&format!(
        "UPDATE public.__reflex_ivm_reference \
         SET last_update_date = CURRENT_TIMESTAMP - INTERVAL '365 days' \
         WHERE name = '{wrapper}'"
    ))
    .expect("age wrapper stale");
    Spi::run(
        "UPDATE public.__reflex_ivm_reference \
         SET last_update_date = CURRENT_TIMESTAMP WHERE name = 'ps12_s3'",
    )
    .expect("freshen parent");

    let payload_before = ps10_count(&format!("SELECT count(*) FROM {wrapper}"));

    // Sweep with a 60-minute gate: the fresh parent is NOT a candidate, the stale
    // wrapper WOULD be on age alone.
    Spi::run("CREATE TEMP TABLE ps12_s3_out AS SELECT * FROM reflex_scheduled_reconcile(60)")
        .expect("scheduled sweep must not raise");

    assert_eq!(
        ps10_count(&format!(
            "SELECT count(*) FROM ps12_s3_out WHERE name = '{wrapper}'"
        )),
        0,
        "the stale wrapper must not be swept even though its parent is fresh"
    );
    assert_eq!(
        ps10_count(&format!("SELECT count(*) FROM {wrapper}")),
        payload_before,
        "the wrapper's contents must be untouched (no column-shift)"
    );
    assert_eq!(
        ps10_diff_count(
            &format!("SELECT id, v FROM {wrapper}"),
            "SELECT id, v FROM ps12_s3_a UNION ALL SELECT id, v FROM ps12_s3_b"
        ),
        0,
        "the wrapper payload must stay correct"
    );
}

// --- (4) Anti-regression: legit reconciles still work ---------------------

/// (4) A passthrough IMV, an aggregate IMV, and a CTE-decomposed sub-IMV with a
/// REAL aggregations plan all still reconcile normally — the backstop refuses
/// wrappers WITHOUT refusing anything reconcilable. This is what pins the
/// `aggregations = '{}'` half of the discriminator: the self-mutation that drops
/// it (leaving `end_query = ''` alone) turns this test RED by refusing the
/// passthrough IMV.
#[pg_test]
fn ps12_legitimate_reconciles_still_succeed() {
    // Passthrough (end_query='' — the clause the mutation would over-refuse).
    Spi::run("CREATE TABLE ps12_l_pt (id BIGINT, v NUMERIC)").expect("pt");
    Spi::run("INSERT INTO ps12_l_pt VALUES (1,10),(2,20)").expect("seed pt");
    Spi::run("SELECT create_reflex_ivm('ps12_l_pt_v', 'SELECT id, v FROM ps12_l_pt', 'id')")
        .expect("create passthrough");
    let r = Spi::get_one::<String>("SELECT reflex_reconcile('ps12_l_pt_v')")
        .expect("reconcile")
        .expect("NULL");
    assert_eq!(r, "RECONCILED", "a passthrough IMV must still reconcile");

    // Aggregate.
    Spi::run("CREATE TABLE ps12_l_ag (region TEXT, amount NUMERIC)").expect("ag");
    Spi::run("INSERT INTO ps12_l_ag VALUES ('us',100),('eu',200)").expect("seed ag");
    Spi::run(
        "SELECT create_reflex_ivm('ps12_l_ag_v', \
           'SELECT region, SUM(amount) AS total FROM ps12_l_ag GROUP BY region')",
    )
    .expect("create aggregate");
    let r = Spi::get_one::<String>("SELECT reflex_reconcile('ps12_l_ag_v')")
        .expect("reconcile")
        .expect("NULL");
    assert_eq!(r, "RECONCILED", "an aggregate IMV must still reconcile");

    // CTE-decomposed sub-IMV with a real plan — reconciled DIRECTLY by name.
    Spi::run(
        "SELECT create_reflex_ivm('ps12_l_cte_v', \
           'WITH agg AS (SELECT region, SUM(amount) AS total FROM ps12_l_ag GROUP BY region) \
            SELECT region, total FROM agg WHERE total > 0', 'region')",
    )
    .expect("create CTE-aggregate");
    let sub = "ps12_l_cte_v__cte_agg";
    assert!(
        ps12_is_generated(sub) && !ps12_is_wrapper_row(sub),
        "fixture precondition: {sub} is a generated sub-IMV with a real plan"
    );
    let r = Spi::get_one::<String>(&format!("SELECT reflex_reconcile('{sub}')"))
        .expect("reconcile")
        .expect("NULL");
    assert_eq!(
        r, "RECONCILED",
        "a CTE-decomposed sub-IMV with a real plan must still reconcile — the \
         backstop must NOT refuse it"
    );
}

// --- (5) Schema-qualified wrapper -----------------------------------------

/// (5) The refusal holds for a wrapper in a non-`public` schema — the backstop
/// resolves the row by the same qualified `name` that `read_imv` uses.
#[pg_test]
fn ps12_reconcile_refuses_schema_qualified_wrapper() {
    Spi::run("CREATE SCHEMA ps12_sq").expect("schema");
    Spi::run("CREATE TABLE ps12_sq.a (id BIGINT, v NUMERIC)").expect("a");
    Spi::run("CREATE TABLE ps12_sq.b (id BIGINT, v NUMERIC)").expect("b");
    Spi::run("INSERT INTO ps12_sq.a VALUES (1,10)").expect("seed a");
    Spi::run("INSERT INTO ps12_sq.b VALUES (2,20)").expect("seed b");
    Spi::run(
        "SELECT create_reflex_ivm('ps12_sq.v', \
           'SELECT id, v FROM ps12_sq.a UNION ALL SELECT id, v FROM ps12_sq.b', 'id')",
    )
    .expect("create qualified UNION ALL");

    assert!(
        ps12_is_wrapper_row("ps12_sq.v"),
        "fixture precondition: the qualified IMV registers a wrapper row"
    );

    let result = Spi::get_one::<String>("SELECT reflex_reconcile('ps12_sq.v')")
        .expect("reflex_reconcile must return a value, not raise")
        .expect("NULL");
    assert!(
        result.starts_with("ERROR") && result.contains("wrapper"),
        "a schema-qualified VIEW wrapper must be refused too: {result}"
    );
    let alive = Spi::get_one::<i32>("SELECT 7").expect("tx alive").expect("NULL");
    assert_eq!(alive, 7, "the transaction must survive the qualified refusal");
    assert_eq!(
        ps10_diff_count(
            "SELECT id, v FROM ps12_sq.v",
            "SELECT id, v FROM ps12_sq.a UNION ALL SELECT id, v FROM ps12_sq.b"
        ),
        0,
        "the refused qualified wrapper must be unchanged and correct"
    );
}

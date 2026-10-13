// pg_reflex's own partition rebuild of an IMV that another IMV ignores.
//
// `sc_fav`'s sub-IMV `sc_fav__cte_forecast_sales` ignores the passthrough
// `sc_sfv` and maps its plan, so `sc_sfv` carries the ignored-source heal
// triggers. A partition flush of `sc_sfv` writes its rebuilt leaves through the
// root, and the same flush refreshes the ignoring sub-IMV on the plans of those
// leaves. Queuing a heal for that write is redundant work (two EXCEPTs over the
// whole transition tables per leaf) and leaves the sub-IMV reported stale after
// it was refreshed. Any other write to `sc_sfv` (its maintenance after a write to
// its source) still queues. Chain and helpers: pg_test_v112_swap_cascade.rs.

fn hor_queued_keys() -> Vec<String> {
    sc_text_rows(
        "SELECT imv_name || ':' || partition_key FROM public.__reflex_heal_pending \
         ORDER BY imv_name, partition_key",
    )
}

fn hor_stale_imvs() -> Vec<String> {
    sc_text_rows("SELECT name FROM reflex_ivm_status() WHERE known_stale ORDER BY name")
}

fn hor_heal_update_trigger_on(chain: &ScChain, relation: &str) -> i64 {
    worker_scalar_i64(&chain.named(&format!(
        "SELECT count(*)::int8 FROM pg_trigger \
         WHERE tgrelid = '{relation}'::regclass AND tgname = '__reflex_heal_upd'"
    )))
}

fn hor_forecast_sales(chain: &ScChain) -> String {
    chain.named("sc_fav__cte_forecast_sales")
}

/// Swaps of populated leaves, flushed explicitly and at COMMIT, leave every node
/// right and no heal behind; an ordinary write to the passthrough's source still
/// queues its plan, alone even when it follows a flush in the same transaction,
/// and the heal of it restores the chain.
fn hor_assert_own_rebuild_queues_no_heal(dbname: &str, mode: &str, field_names: bool) {
    let chain = ScChain { field_names, cross_plan: false };
    sc_open(dbname, mode, chain);
    let forecast_sales = hor_forecast_sales(&chain);
    let mut failures = Vec::new();
    if hor_heal_update_trigger_on(&chain, "sc_sfv") != 1 {
        failures.push("fixture: the passthrough carries no heal UPDATE trigger".to_string());
    }
    for (label, statements) in [
        ("three leaves, explicit flush", format!("{}; {SC_FLUSH}", sc_three_leaves(2, 4))),
        ("one leaf, flushed at COMMIT", sc_swap(3, 2, &sc_leaf_rows(3, 2, 4, 7))),
        ("whole plan replaced", sc_swap_plan(1, 3)),
    ] {
        if let Err(error) = chain.commit(&statements) {
            failures.push(format!("{label}: raised {error}"));
            continue;
        }
        for wrong in chain.wrong_nodes(&[]) {
            failures.push(format!("{label}: {wrong}"));
        }
        let queued = hor_queued_keys();
        if !queued.is_empty() {
            failures.push(format!("{label}: queued heals {queued:?}"));
        }
        let stale = hor_stale_imvs();
        if !stale.is_empty() {
            failures.push(format!("{label}: reported stale {stale:?}"));
        }
    }

    match chain.commit(&format!(
        "{}; {SC_FLUSH}; UPDATE sc_ss SET qty = qty + 1 WHERE plan = 4 AND product = 2",
        sc_swap(1, 2, &sc_leaf_rows(1, 2, 4, 11))
    )) {
        Err(error) => failures.push(format!("flush then write: raised {error}")),
        Ok(()) => {
            let queued = hor_queued_keys();
            if queued != vec![format!("{forecast_sales}:4")] {
                failures.push(format!(
                    "flush then write: queued {queued:?}, want only the written plan 4"
                ));
            }
            if !hor_stale_imvs().contains(&forecast_sales) {
                failures.push("flush then write: the ignoring sub-IMV is not stale".to_string());
            }
        }
    }
    match chain.commit("SELECT reflex_heal_ignored_sources()") {
        Err(error) => failures.push(format!("heal: raised {error}")),
        Ok(()) => {
            for wrong in chain.wrong_nodes(&[]) {
                failures.push(format!("heal: {wrong}"));
            }
            let queued = hor_queued_keys();
            if !queued.is_empty() {
                failures.push(format!("heal: still queued {queued:?}"));
            }
        }
    }
    probe_db_close(dbname);
    assert!(failures.is_empty(), "{mode}\n{}", failures.join("\n"));
}

#[pg_test]
fn v1121_hor_own_rebuild_queues_no_heal_immediate() {
    hor_assert_own_rebuild_queues_no_heal("reflex_hor_imm", "IMMEDIATE", false);
}

#[pg_test]
fn v1121_hor_own_rebuild_queues_no_heal_deferred_field_names() {
    hor_assert_own_rebuild_queues_no_heal("reflex_hor_def", "DEFERRED", true);
}

/// The heal's key derivation is the INSERT into `__reflex_heal_pending`; the
/// lock it takes on that table, held to the end of the transaction, shows
/// whether it ran. A partition flush of the passthrough never runs it, an
/// ordinary write to the passthrough's source does.
#[pg_test]
fn v1121_hor_own_rebuild_skips_the_heal_key_derivation() {
    let chain = ScChain { field_names: false, cross_plan: false };
    sc_open("reflex_hor_cost", "IMMEDIATE", chain);
    let heal_queue_locked = "SELECT count(*)::text FROM pg_locks \
         WHERE pid = pg_backend_pid() AND relation = 'public.__reflex_heal_pending'::regclass";
    let after_flush = sc_text_rows(&format!(
        "{}; {SC_FLUSH}; {heal_queue_locked}",
        sc_three_leaves(2, 4)
    ));
    let after_write = sc_text_rows(&format!(
        "UPDATE sc_ss SET qty = qty + 1 WHERE plan = 3 AND product = 1; {heal_queue_locked}"
    ));
    let wrong = chain.wrong_nodes(&["sc_fav", "sc_adya", "sc_fav__cte_forecast_sales"]);
    probe_db_close("reflex_hor_cost");

    assert_eq!(after_write, vec!["1".to_string()], "control: a write runs the derivation");
    assert_eq!(after_flush, vec!["0".to_string()], "the flush must not run it");
    assert!(wrong.is_empty(), "{}", wrong.join(", "));
}

const HOR_TWO_LEVEL_SQL: &str = "SELECT dp.id AS plan, sf.d, sf.product, sf.loc, sf.quantity \
     FROM sc_sfv AS sf JOIN sc_dp AS dp ON dp.id = sf.plan";

/// A reader partitioned on two levels is refreshed by the flush on the rebuilt
/// month partitions only, while its heal spans the whole plan: the flush does
/// not provably subsume that heal, so it stays queued.
#[pg_test]
fn v1121_hor_unproven_reader_keeps_its_heal() {
    let chain = ScChain { field_names: false, cross_plan: false };
    sc_open("reflex_hor_two", "IMMEDIATE", chain);
    chain.create(&format!(
        "create_reflex_ivm('hor_two_level', {}, 'plan, product, loc, d', NULL, 'IMMEDIATE', \
           '!sc_sfv', ARRAY['plan', 'd'])",
        sql_lit(HOR_TWO_LEVEL_SQL)
    ));
    let mapped = worker_scalar_i64(
        "SELECT count(*)::int8 FROM public.__reflex_ivm_reference \
         WHERE name = 'hor_two_level' AND aggregations->'ignore_heal_keys' ? 'sc_sfv'",
    );
    let flushed = chain.commit(&format!("{}; {SC_FLUSH}", sc_swap(2, 2, &sc_leaf_rows(2, 2, 4, 7))));
    let queued = hor_queued_keys();
    let mut wrong = chain.wrong_nodes(&[]);
    let fresh = HOR_TWO_LEVEL_SQL.replace("sc_sfv AS sf", &format!("({SC_SFV_SQL}) AS sf"));
    let reader_mismatches = rbc_mismatch("hor_two_level", &fresh);
    if reader_mismatches != 0 {
        wrong.push(format!("hor_two_level: {reader_mismatches} mismatches"));
    }
    probe_db_close("reflex_hor_two");

    assert_eq!(mapped, 1, "fixture: the reader maps its plan to the passthrough's");
    flushed.expect("swap and flush");
    assert!(wrong.is_empty(), "{}", wrong.join(", "));
    assert_eq!(queued, vec!["hor_two_level:2".to_string()]);
}

/// Readers of the passthrough the flush's coverage leaves out, with the query
/// that recomputes each from the base tables.
const HOR_UNCOVERED_READERS: [(&str, &str, &str, &str); 2] = [
    (
        "hor_by_product",
        "SELECT pr.product, sf.plan, sf.loc, sf.d, sf.quantity \
         FROM sc_sfv AS sf JOIN hor_prod AS pr ON pr.product = sf.product",
        "product, plan, loc, d",
        "'!sc_sfv', ARRAY['product']",
    ),
    (
        "hor_qualified",
        "SELECT dp.id AS plan, sf.product, sf.loc, sf.d, sf.quantity \
         FROM public.sc_sfv AS sf JOIN sc_dp AS dp ON dp.id = sf.plan",
        "plan, product, loc, d",
        "'!public.sc_sfv', ARRAY['plan']",
    ),
];

/// A reader mapping another column than the passthrough's partition column
/// keeps its heal; so does one the flush's cascade may not recognise as an
/// ignoring reader (its ignore names the passthrough schema-qualified). Neither
/// is ever left wrong without being reported stale.
#[pg_test]
fn v1121_hor_readers_outside_the_coverage_keep_their_heal() {
    let chain = ScChain { field_names: false, cross_plan: false };
    sc_open("reflex_hor_out", "IMMEDIATE", chain);
    chain.exec(
        "CREATE TABLE hor_prod (product INT NOT NULL, label TEXT NOT NULL, PRIMARY KEY (product)) \
         PARTITION BY LIST (product)",
    );
    for product in 1..=5 {
        chain.exec(&format!(
            "CREATE TABLE hor_prod_{product} PARTITION OF hor_prod FOR VALUES IN ({product})"
        ));
    }
    chain.exec("INSERT INTO hor_prod SELECT p, 'p' || p FROM generate_series(1, 5) p");
    let mut failures = Vec::new();
    for (name, query, key, tail) in HOR_UNCOVERED_READERS {
        chain.create(&format!(
            "create_reflex_ivm('{name}', {}, '{key}', NULL, 'IMMEDIATE', {tail})",
            sql_lit(query)
        ));
        if worker_scalar_i64(&format!(
            "SELECT count(*)::int8 FROM public.__reflex_ivm_reference \
             WHERE name = '{name}' AND aggregations->'ignore_heal_keys' <> '{{}}'::jsonb"
        )) != 1
        {
            failures.push(format!("fixture: {name} maps no ignored source"));
        }
    }
    if let Err(error) =
        chain.commit(&format!("{}; {SC_FLUSH}", sc_swap(2, 2, &sc_leaf_rows(2, 2, 4, 7))))
    {
        failures.push(format!("swap and flush: raised {error}"));
    }
    let queued = hor_queued_keys();
    let stale = hor_stale_imvs();
    failures.extend(chain.wrong_nodes(&[]));
    for (name, query, _, _) in HOR_UNCOVERED_READERS {
        if !queued.iter().any(|row| row.starts_with(&format!("{name}:"))) {
            failures.push(format!("{name}: no heal queued, {queued:?}"));
        }
        let fresh = query
            .replace("public.sc_sfv AS sf", "sc_sfv AS sf")
            .replace("sc_sfv AS sf", &format!("({SC_SFV_SQL}) AS sf"));
        let mismatches = rbc_mismatch(name, &fresh);
        if mismatches != 0 && !stale.iter().any(|imv| imv == name) {
            failures.push(format!("{name}: {mismatches} mismatches, not reported stale"));
        }
    }
    if queued.iter().any(|row| row.starts_with("sc_fav__cte_forecast_sales:")) {
        failures.push(format!("the covered sub-IMV queued a heal: {queued:?}"));
    }
    probe_db_close("reflex_hor_out");
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

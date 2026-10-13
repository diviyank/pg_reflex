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

// 1.12.1, honour ignore on the push path: a partition flush now leaves the
// ignoring sub-IMVs to their heal (tests at the end of this file). The tests
// below pin the explicit partition reconcile, which still refreshes them, and
// the heal trigger's skip around that refresh.

/// Plan `plan` of the source drifts behind its IMVs (written with their
/// triggers off), then the passthrough's partition of it is reconciled
/// explicitly.
fn hor_drift_and_reconcile(plan: i32) -> String {
    format!(
        "ALTER TABLE sc_ss DISABLE TRIGGER USER; \
         UPDATE sc_ss SET qty = qty + 3, base = base + 1 WHERE plan = {plan}; \
         ALTER TABLE sc_ss ENABLE TRIGGER USER; \
         SELECT reflex_reconcile_partition('sc_sfv', '{plan}')"
    )
}

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
        ("plan 2 reconciled", hor_drift_and_reconcile(2)),
        ("plan 3 reconciled", hor_drift_and_reconcile(3)),
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
        "{}; UPDATE sc_ss SET qty = qty + 1 WHERE plan = 4 AND product = 2",
        hor_drift_and_reconcile(1)
    )) {
        Err(error) => failures.push(format!("reconcile then write: raised {error}")),
        Ok(()) => {
            let queued = hor_queued_keys();
            if queued != vec![format!("{forecast_sales}:4")] {
                failures.push(format!(
                    "reconcile then write: queued {queued:?}, want only the written plan 4"
                ));
            }
            if !hor_stale_imvs().contains(&forecast_sales) {
                failures.push("reconcile then write: the ignoring sub-IMV is not stale".to_string());
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
    let after_reconcile = sc_text_rows(&format!(
        "{}; {heal_queue_locked}",
        hor_drift_and_reconcile(2)
    ));
    let after_write = sc_text_rows(&format!(
        "UPDATE sc_ss SET qty = qty + 1 WHERE plan = 3 AND product = 1; {heal_queue_locked}"
    ));
    let wrong = chain.wrong_nodes(&["sc_fav", "sc_adya", "sc_fav__cte_forecast_sales"]);
    probe_db_close("reflex_hor_cost");

    assert_eq!(after_write, vec!["1".to_string()], "control: a write runs the derivation");
    assert_eq!(after_reconcile, vec!["0".to_string()], "the explicit reconcile must not run it");
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
    let flushed = chain.commit(&hor_drift_and_reconcile(2));
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
        chain.commit(&hor_drift_and_reconcile(2))
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

fn hor_registry_flag(chain: &ScChain, imv: &str) -> (bool, String) {
    let row = sc_text_rows(&chain.named(&format!(
        "SELECT COALESCE(known_stale, FALSE)::text || '|' || COALESCE(stale_reason, '') \
         FROM public.__reflex_ivm_reference WHERE name = '{imv}'"
    )));
    let row = row.first().cloned().unwrap_or_default();
    let (stale, reason) = row.split_once('|').unwrap_or(("false", ""));
    (stale == "true", reason.to_string())
}

/// The flush's refresh of a covered reader returns an error (here: the reader
/// is disabled by another trigger once the cascade has queued it, so both its
/// partition refresh and its whole refresh refuse). The reader, whose heal the
/// flush skipped, is left behind: it must be flagged stale with a remedy that,
/// once the cause is cleared, brings it back and clears the flag.
#[pg_test]
fn v1121_hor_failed_refresh_of_a_covered_reader_flags_it_stale() {
    let chain = ScChain { field_names: false, cross_plan: false };
    sc_open("reflex_hor_fail", "IMMEDIATE", chain);
    let reader = hor_forecast_sales(&chain);
    chain.exec(&format!(
        "CREATE FUNCTION hor_disable_reader() RETURNS TRIGGER LANGUAGE plpgsql AS $$ \
           BEGIN UPDATE public.__reflex_ivm_reference SET enabled = FALSE \
                  WHERE name = '{reader}'; RETURN NULL; END $$"
    ));
    chain.exec(
        "CREATE TRIGGER hor_disable_reader AFTER UPDATE OF last_update_date \
           ON public.__reflex_ivm_reference FOR EACH ROW \
           WHEN (NEW.name = 'sc_fav__cte_forecast_bounds') \
           EXECUTE FUNCTION hor_disable_reader()",
    );
    let flushed = chain.commit(&hor_drift_and_reconcile(2));
    let behind = chain.wrong_nodes(&[]);
    let (flagged, reason) = hor_registry_flag(&chain, &reader);
    let queued = hor_queued_keys();

    chain.exec("DROP TRIGGER hor_disable_reader ON public.__reflex_ivm_reference");
    chain.exec(&format!(
        "UPDATE public.__reflex_ivm_reference SET enabled = TRUE WHERE name = '{reader}'"
    ));
    let remedy = format!("SELECT reflex_reconcile('{reader}')");
    let remedied = chain.commit(&remedy);
    let (flagged_after, _) = hor_registry_flag(&chain, &reader);
    let wrong_after = chain.wrong_nodes(&[]);
    probe_db_close("reflex_hor_fail");

    flushed.expect("swap and flush");
    assert!(
        behind.iter().any(|node| node.starts_with(&format!("{reader}:"))),
        "fixture: the failed refresh leaves the reader behind: {behind:?}"
    );
    assert!(
        flagged || queued.iter().any(|row| row.starts_with(&format!("{reader}:"))),
        "the reader is wrong and neither flagged stale nor queued: {behind:?}"
    );
    assert!(flagged, "the reader must be flagged stale, queued {queued:?}");
    assert!(reason.contains(&remedy), "the reason must print the remedy: {reason}");
    remedied.expect("remedy");
    assert!(!flagged_after, "the printed remedy must clear the flag");
    assert!(wrong_after.is_empty(), "{}", wrong_after.join(", "));
}

/// The whole reconcile of an IMV refreshes the readers that ignore it itself, a
/// partitioned one through its dependent cascade, an unpartitioned one through
/// its ignoring readers. A refresh there that returns an error flags the reader
/// stale too, and the printed remedy clears the flag.
#[pg_test]
fn v1121_hor_failed_refresh_after_a_whole_reconcile_flags_the_reader_stale() {
    let chain = ScChain { field_names: false, cross_plan: false };
    sc_open("reflex_hor_wfail", "IMMEDIATE", chain);
    for reader in ["hor_years", "hor_a_years"] {
        chain.create(&format!(
            "create_reflex_ivm('{reader}', {}, 'plan, yr', NULL, 'IMMEDIATE', '!sc_fdya', \
               ARRAY[]::text[])",
            sql_lit(
                "SELECT f.plan, f.yr, f.min_date FROM sc_fdya AS f JOIN sc_dp AS dp ON dp.id = f.plan"
            )
        ));
    }
    chain.exec(
        "CREATE FUNCTION hor_disable_after() RETURNS TRIGGER LANGUAGE plpgsql AS $$ \
           BEGIN IF NEW.name = TG_ARGV[0] THEN \
                   UPDATE public.__reflex_ivm_reference SET enabled = FALSE WHERE name = TG_ARGV[1]; \
                 END IF; RETURN NULL; END $$",
    );
    for (refreshed_first, disabled) in [
        ("sc_fav__cte_forecast_bounds", "sc_fav__cte_forecast_sales"),
        ("hor_a_years", "hor_years"),
    ] {
        chain.exec(&format!(
            "CREATE TRIGGER hor_disable_{disabled} AFTER UPDATE OF last_update_date \
               ON public.__reflex_ivm_reference FOR EACH ROW \
               EXECUTE FUNCTION hor_disable_after('{refreshed_first}', '{disabled}')"
        ));
    }
    let mut failures = Vec::new();
    for (reconciled, reader) in [("sc_sfv", "sc_fav__cte_forecast_sales"), ("sc_fdya", "hor_years")] {
        if let Err(error) = chain.commit(&format!("SELECT reflex_reconcile('{reconciled}')")) {
            failures.push(format!("{reconciled}: raised {error}"));
        }
        let (flagged, reason) = hor_registry_flag(&chain, reader);
        let remedy = format!("SELECT reflex_reconcile('{reader}')");
        if !flagged || !reason.contains(&remedy) {
            failures.push(format!("{reader} after reconcile of {reconciled}: flagged {flagged}, {reason}"));
        }
    }
    for (_, disabled) in [("", "sc_fav__cte_forecast_sales"), ("", "hor_years")] {
        chain.exec(&format!(
            "DROP TRIGGER hor_disable_{disabled} ON public.__reflex_ivm_reference"
        ));
        chain.exec(&format!(
            "UPDATE public.__reflex_ivm_reference SET enabled = TRUE WHERE name = '{disabled}'"
        ));
        if let Err(error) = chain.commit(&format!("SELECT reflex_reconcile('{disabled}')")) {
            failures.push(format!("remedy of {disabled}: raised {error}"));
        }
        if hor_registry_flag(&chain, disabled).0 {
            failures.push(format!("{disabled}: the printed remedy left the flag"));
        }
    }
    failures.extend(chain.wrong_nodes(&[]));
    probe_db_close("reflex_hor_wfail");
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

fn hor_rl_create(name: &str, sql: &str, key: &str, tail: &str) {
    let created = Spi::get_one::<String>(&format!(
        "SELECT create_reflex_ivm({}, {}, {}, NULL, 'IMMEDIATE'{tail})",
        sql_lit(name),
        sql_lit(sql),
        sql_lit(key)
    ))
    .expect("create")
    .unwrap_or_default();
    assert_eq!(created, "CREATE REFLEX INCREMENTAL VIEW", "{name}");
}

fn hor_rl_mismatch(fresh: &str) -> i64 {
    Spi::get_one::<i64>(&format!(
        "SELECT count(*) FROM ((SELECT x::text FROM hor_rl_r x EXCEPT ALL SELECT x::text FROM ({fresh}) x) \
         UNION ALL (SELECT x::text FROM ({fresh}) x EXCEPT ALL SELECT x::text FROM hor_rl_r x)) o"
    ))
    .expect("oracle")
    .unwrap_or(-1)
}

/// R ignores X and Y, an IMMEDIATE IMV observing X. The partition rebuild of X
/// writes Y while X's coverage is set; Y's changed rows reach plans of R other
/// than X's rebuilt one, so Y's write must still queue R's heal, and the heal
/// sweep must bring R back to its query.
#[pg_test]
fn v1121_hor_write_to_another_relation_inside_the_rebuild_still_queues() {
    for ddl in [
        "CREATE TABLE hor_rl_s (plan INT NOT NULL, k INT NOT NULL, v INT NOT NULL, \
           PRIMARY KEY (plan, k)) PARTITION BY LIST (plan)",
        "CREATE TABLE hor_rl_s_1 PARTITION OF hor_rl_s FOR VALUES IN (1)",
        "CREATE TABLE hor_rl_s_2 PARTITION OF hor_rl_s FOR VALUES IN (2)",
        "CREATE TABLE hor_rl_s_3 PARTITION OF hor_rl_s FOR VALUES IN (3)",
        "INSERT INTO hor_rl_s SELECT a, b, 10 * a + b FROM generate_series(1, 3) a, \
           generate_series(1, 3) b",
        "CREATE TABLE hor_rl_d (id INT PRIMARY KEY)",
        "INSERT INTO hor_rl_d VALUES (1), (2), (3)",
    ] {
        Spi::run(ddl).expect("fixture");
    }
    hor_rl_create("hor_rl_x", "SELECT plan, k, v FROM hor_rl_s", "plan, k", ", NULL, ARRAY['plan']");
    hor_rl_create(
        "hor_rl_y",
        "SELECT k AS plan, plan AS orig, v FROM hor_rl_x",
        "plan, orig",
        "",
    );
    hor_rl_create(
        "hor_rl_r",
        "SELECT d.id AS plan, x.k, x.v, y.v AS yv FROM hor_rl_d AS d \
         JOIN hor_rl_x AS x ON x.plan = d.id JOIN hor_rl_y AS y ON y.plan = d.id AND y.orig = x.k",
        "plan, k",
        ", '!hor_rl_x,!hor_rl_y', ARRAY['plan']",
    );
    let fresh = "SELECT d.id AS plan, x.k, x.v, y.v AS yv FROM hor_rl_d AS d \
         JOIN (SELECT plan, k, v FROM hor_rl_s) AS x ON x.plan = d.id \
         JOIN (SELECT k AS plan, plan AS orig, v FROM hor_rl_s) AS y \
           ON y.plan = d.id AND y.orig = x.k";
    let heal_maps_both: i64 = Spi::get_one(
        "SELECT count(*)::int8 FROM public.__reflex_ivm_reference, \
                jsonb_object_keys(aggregations->'ignore_heal_keys') k \
          WHERE name = 'hor_rl_r'",
    )
    .expect("heal keys")
    .unwrap_or(0);
    assert_eq!(heal_maps_both, 2, "fixture: R heals on both X and Y");
    assert_eq!(hor_rl_mismatch(fresh), 0, "fixture: R right before");

    Spi::run(
        "ALTER TABLE hor_rl_s DISABLE TRIGGER USER; \
         UPDATE hor_rl_s SET v = v + 100 WHERE plan = 2; \
         ALTER TABLE hor_rl_s ENABLE TRIGGER USER",
    )
    .expect("drift plan 2 of the source");
    let rebuilt = Spi::get_one::<String>("SELECT reflex_reconcile_partition('hor_rl_x', '2')")
        .expect("rebuild")
        .unwrap_or_default();
    assert!(rebuilt.starts_with("RECONCILED"), "{rebuilt}");
    let behind = hor_rl_mismatch(fresh);
    let queued: Vec<String> = Spi::connect(|client| {
        client
            .select(
                "SELECT partition_key FROM public.__reflex_heal_pending \
                  WHERE imv_name = 'hor_rl_r' ORDER BY 1",
                None,
                &[],
            )
            .expect("queue")
            .filter_map(|row| row.get_by_name::<String, _>("partition_key").ok().flatten())
            .collect()
    });
    assert!(
        behind == 0 || !queued.is_empty(),
        "R is {behind} rows wrong after X's rebuild and nothing is queued"
    );
    let healed = Spi::get_one::<String>("SELECT reflex_heal_ignored_sources()")
        .expect("heal")
        .unwrap_or_default();
    assert!(!healed.starts_with("ERROR"), "{healed}");
    assert_eq!(hor_rl_mismatch(fresh), 0, "R must match its query after the heal sweep");
}

// Honour `ignore_sources` on the push path. A change of the source's partitions
// (a leaf swapped, a leaf or a plan dropped) rebuilds the passthrough through the
// partition flush; the sub-IMVs of `sc_fav` that ignore the passthrough are not
// refreshed but queued for their heal: `forecast_sales` (mapped on plan) on the
// affected plans, `forecast_bounds` (no mapping) whole. They and their readers
// report stale until the application heals `sc_fav`, which brings every node of
// the chain back to its query.

fn hor_untouched_ignoring_chain(chain: &ScChain) -> Vec<String> {
    [
        "sc_fav__cte_forecast_bounds",
        "sc_fav__cte_date_limits",
        "sc_fav__cte_forecast_sales",
        "sc_fav",
    ]
    .iter()
    .filter_map(|node| {
        let node = chain.named(node);
        let written = worker_scalar_i64(&format!(
            "SELECT count(*)::int8 FROM {node} \
             WHERE xmin::text::int8 >= (SELECT id::int8 FROM sc_xact)"
        ));
        (written != 0).then(|| format!("{node}: {written} rows written"))
    })
    .collect()
}

fn hor_drop_leaf(plan: i32, month: i32) -> String {
    let leaf = sc_leaf(plan, month);
    format!("ALTER TABLE sc_ss_p_{plan} DETACH PARTITION {leaf}; DROP TABLE {leaf}")
}

fn hor_every_leaf(bump: i32) -> String {
    SC_PLANS
        .iter()
        .flat_map(|plan| {
            SC_MONTHS
                .iter()
                .map(move |month| sc_swap(*plan, *month, &sc_leaf_rows(*plan, *month, 4, bump)))
        })
        .collect::<Vec<_>>()
        .join("; ")
}

fn hor_assert_push_queues_the_ignoring_chain(dbname: &str, mode: &str, field_names: bool) {
    let chain = ScChain { field_names, cross_plan: false };
    sc_open(dbname, mode, chain);
    let sales = chain.named("sc_fav__cte_forecast_sales");
    let bounds = chain.named("sc_fav__cte_forecast_bounds");
    let fav = chain.named("sc_fav");
    let whole = format!("{bounds}:(whole IMV)");
    let mut failures = Vec::new();
    let adya = chain.named("sc_adya");
    for (label, statements, plans, healed_by) in [
        ("one leaf, at COMMIT", sc_swap(2, 2, &sc_leaf_rows(2, 2, 4, 7)), vec![2], &fav),
        (
            "one leaf, explicit flush",
            format!("{}; {SC_FLUSH}", sc_swap(1, 3, &sc_leaf_rows(1, 3, 4, 8))),
            vec![1],
            &adya,
        ),
        ("every leaf of every plan", hor_every_leaf(5), vec![1, 2, 3, 4], &fav),
        ("a new plan attached", sc_attach_plan(5, 5), vec![5], &adya),
        ("a month leaf dropped", hor_drop_leaf(3, 4), vec![3], &fav),
        (
            "a plan detached and dropped",
            "ALTER TABLE sc_ss DETACH PARTITION sc_ss_p_4; DROP TABLE sc_ss_p_4".to_string(),
            vec![4],
            &fav,
        ),
    ] {
        if let Err(error) = chain.commit(&statements) {
            failures.push(format!("{label}: raised {error}"));
            continue;
        }
        let refreshed: Vec<String> = chain
            .refreshes()
            .into_iter()
            .filter(|(node, _)| node.starts_with(&fav))
            .map(|(node, kind)| format!("{node}:{kind}"))
            .collect();
        if !refreshed.is_empty() {
            failures.push(format!("{label}: the push refreshed {refreshed:?}"));
        }
        for written in hor_untouched_ignoring_chain(&chain) {
            failures.push(format!("{label}: {written}"));
        }
        let mut expected: Vec<String> = vec![whole.clone()];
        expected.extend(plans.iter().map(|plan| format!("{sales}:{plan}")));
        expected.sort();
        let queued = hor_queued_keys();
        if queued != expected {
            failures.push(format!("{label}: queued {queued:?}, want {expected:?}"));
        }
        let stale = hor_stale_imvs();
        for node in [&sales, &bounds, &fav] {
            if !stale.contains(node) {
                failures.push(format!("{label}: {node} not reported stale, {stale:?}"));
            }
        }
        for wrong in chain.wrong_nodes(&[]) {
            let node = wrong.split(':').next().unwrap_or_default().to_string();
            if !stale.contains(&node) {
                failures.push(format!("{label}: {wrong}, not reported stale"));
            }
        }
        if let Err(error) =
            chain.commit(&format!("SELECT reflex_heal_ignored_sources('public.{healed_by}')"))
        {
            failures.push(format!("{label}: heal raised {error}"));
            continue;
        }
        for wrong in chain.wrong_nodes(&[]) {
            failures.push(format!("{label}, healed: {wrong}"));
        }
        let queued = hor_queued_keys();
        if !queued.is_empty() {
            failures.push(format!("{label}, healed: still queued {queued:?}"));
        }
        let stale = hor_stale_imvs();
        if !stale.is_empty() {
            failures.push(format!("{label}, healed: still stale {stale:?}"));
        }
    }
    probe_db_close(dbname);
    assert!(failures.is_empty(), "{mode}\n{}", failures.join("\n"));
}

#[pg_test]
fn v1121_push_queues_the_ignoring_chain_immediate() {
    hor_assert_push_queues_the_ignoring_chain("reflex_push_imm", "IMMEDIATE", false);
}

#[pg_test]
fn v1121_push_queues_the_ignoring_chain_deferred_field_names() {
    hor_assert_push_queues_the_ignoring_chain("reflex_push_def", "DEFERRED", true);
}

/// An explicit partition reconcile of the passthrough is not a push: it
/// refreshes the sub-IMVs ignoring it, and queues nothing.
#[pg_test]
fn v1121_push_explicit_reconcile_still_refreshes_the_ignoring_readers() {
    let chain = ScChain { field_names: false, cross_plan: false };
    sc_open("reflex_push_expl", "IMMEDIATE", chain);
    let committed = chain.commit(
        "ALTER TABLE sc_ss DISABLE TRIGGER USER; \
         UPDATE sc_ss SET qty = qty + 3 WHERE plan = 2; \
         ALTER TABLE sc_ss ENABLE TRIGGER USER; \
         SELECT reflex_reconcile_partition('sc_sfv', '2')",
    );
    let refreshed: Vec<String> = chain
        .refreshes()
        .into_iter()
        .map(|(node, kind)| format!("{node}:{kind}"))
        .collect();
    let wrong = chain.wrong_nodes(&[]);
    let queued = hor_queued_keys();
    probe_db_close("reflex_push_expl");

    committed.expect("drift and explicit reconcile");
    assert!(
        refreshed.iter().any(|r| r.starts_with("sc_fav__cte_forecast_sales:"))
            && refreshed.iter().any(|r| r.starts_with("sc_fav__cte_forecast_bounds:")),
        "the explicit reconcile must refresh the ignoring sub-IMVs: {refreshed:?}"
    );
    assert!(wrong.is_empty(), "{}", wrong.join(", "));
    assert!(queued.is_empty(), "queued {queued:?}");
}

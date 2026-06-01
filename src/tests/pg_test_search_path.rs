// 1.4.1: internal reflex artifacts (delta scratch, staging delta, passthrough
// scratch, affected-groups, shrunk-groups) are now co-located in the schema
// of their owning IMV or source table — so trigger bodies and generated SQL
// resolve them by qualified name and no longer depend on the session
// `search_path` containing `public`.

/// IMMEDIATE aggregate: source + IMV both in a non-public schema. DML under
/// a `search_path` that does NOT include `public` must still maintain the
/// IMV — the bug 1.4.1 fixes is exactly this regression on a real customer
/// workload (`alp.demand_planning` feeding `alp.ivm_sop_forecast_view`).
#[pg_test]
fn pg_test_immediate_imv_in_custom_schema_under_set_search_path() {
    Spi::run("CREATE SCHEMA alp").expect("schema");
    Spi::run("CREATE TABLE alp.demand_planning (id SERIAL, city TEXT, qty INT)")
        .expect("create source");
    Spi::run("INSERT INTO alp.demand_planning (city, qty) VALUES ('Paris', 10), ('London', 20)")
        .expect("seed");

    let r = crate::create_reflex_ivm(
        "alp.demand_view",
        "SELECT city, SUM(qty) AS total FROM alp.demand_planning GROUP BY city",
        None,
        None,
        Some("IMMEDIATE"),
        None,
    );
    assert_eq!(r, "CREATE REFLEX INCREMENTAL VIEW");

    // Sanity: intermediate / scratch / affected co-located in `alp`, not public.
    let int_schema = Spi::get_one::<String>(
        "SELECT n.nspname::TEXT FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid \
         WHERE c.relname = '__reflex_intermediate_demand_view'",
    )
    .expect("q")
    .expect("v");
    assert_eq!(int_schema, "alp");

    let scratch_schema = Spi::get_one::<String>(
        "SELECT n.nspname::TEXT FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid \
         WHERE c.relname = '__reflex_scratch_demand_view'",
    )
    .expect("q")
    .expect("v");
    assert_eq!(scratch_schema, "alp");

    let aff_schema = Spi::get_one::<String>(
        "SELECT n.nspname::TEXT FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid \
         WHERE c.relname = '__reflex_affected_demand_view'",
    )
    .expect("q")
    .expect("v");
    assert_eq!(aff_schema, "alp");

    // Restrict search_path to `alp` (excludes public). The reported bug fires
    // here: any unqualified reference to a delta/scratch/affected table inside
    // the trigger body looks up against this search_path and would fail.
    Spi::run("SET search_path = alp").expect("set search_path");

    // INSERT, UPDATE, DELETE must all maintain the IMV without errors.
    Spi::run("INSERT INTO alp.demand_planning (city, qty) VALUES ('Paris', 5)").expect("insert");
    Spi::run("UPDATE alp.demand_planning SET qty = qty + 100 WHERE city = 'London'")
        .expect("update");
    Spi::run("DELETE FROM alp.demand_planning WHERE city = 'Paris' AND qty = 5").expect("delete");

    // Reset search_path so the EXCEPT-ALL oracle (and pg_reflex's own
    // public-qualified registry reads) work without surprises.
    Spi::run("SET search_path = public, alp").expect("reset");

    let fresh = "SELECT city, SUM(qty) AS total FROM alp.demand_planning GROUP BY city";
    let mismatches = Spi::get_one::<i64>(&format!(
        "SELECT COUNT(*) FROM (\
            (SELECT * FROM alp.demand_view EXCEPT ALL SELECT * FROM ({fresh}) f1) \
            UNION ALL \
            (SELECT * FROM ({fresh}) f2 EXCEPT ALL SELECT * FROM alp.demand_view) \
         ) o"
    ))
    .expect("oracle")
    .expect("v");
    assert_eq!(
        mismatches, 0,
        "IMV diverged from fresh under non-public search_path",
    );
}

/// DEFERRED variant of the same regression. The staging-delta table is per
/// source (`alp.__reflex_delta_alp_demand_planning`); the deferred trigger
/// body must reference it qualified. The constraint trigger at COMMIT then
/// flushes — `reflex_flush_deferred` is itself called from a trigger body
/// under whatever search_path the writing session had.
#[pg_test]
fn pg_test_deferred_imv_in_custom_schema_under_set_search_path() {
    Spi::run("CREATE SCHEMA def_s").expect("schema");
    Spi::run("CREATE TABLE def_s.orders (id SERIAL, region TEXT, amt NUMERIC)")
        .expect("create source");
    Spi::run("INSERT INTO def_s.orders (region, amt) VALUES ('NA', 100), ('EU', 50)")
        .expect("seed");

    let r = crate::create_reflex_ivm(
        "def_s.orders_view",
        "SELECT region, SUM(amt) AS total FROM def_s.orders GROUP BY region",
        None,
        None,
        Some("DEFERRED"),
        None,
    );
    assert_eq!(r, "CREATE REFLEX INCREMENTAL VIEW");

    // Staging delta co-located in `def_s`, not public.
    let delta_schema = Spi::get_one::<String>(
        "SELECT n.nspname::TEXT FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid \
         WHERE c.relname = '__reflex_delta_def_s_orders'",
    )
    .expect("q")
    .expect("v");
    assert_eq!(delta_schema, "def_s");

    // Writer session has narrow search_path: this is the reported regression.
    Spi::run("SET search_path = def_s").expect("set search_path");

    Spi::run("INSERT INTO def_s.orders (region, amt) VALUES ('NA', 25)").expect("insert");
    Spi::run("UPDATE def_s.orders SET amt = amt + 1 WHERE region = 'EU'").expect("update");

    // Manual flush — exercises reflex_flush_deferred under the narrow search_path.
    Spi::run("SELECT public.reflex_flush_deferred('def_s.orders')").expect("flush");

    Spi::run("SET search_path = public, def_s").expect("reset");

    let fresh = "SELECT region, SUM(amt) AS total FROM def_s.orders GROUP BY region";
    let mismatches = Spi::get_one::<i64>(&format!(
        "SELECT COUNT(*) FROM (\
            (SELECT * FROM def_s.orders_view EXCEPT ALL SELECT * FROM ({fresh}) f1) \
            UNION ALL \
            (SELECT * FROM ({fresh}) f2 EXCEPT ALL SELECT * FROM def_s.orders_view) \
         ) o"
    ))
    .expect("oracle")
    .expect("v");
    assert_eq!(
        mismatches, 0,
        "deferred IMV diverged from fresh under non-public search_path",
    );
}

/// Passthrough IMV in a custom schema under narrow search_path. Exercises the
/// per-(IMV, source) passthrough scratch tables (`__reflex_pt_new_*`,
/// `__reflex_pt_old_*`) — these are now co-located in the IMV's schema.
#[pg_test]
fn pg_test_passthrough_imv_in_custom_schema_under_set_search_path() {
    Spi::run("CREATE SCHEMA pt_s").expect("schema");
    Spi::run(
        "CREATE TABLE pt_s.events (\
            id SERIAL PRIMARY KEY, \
            user_id INT NOT NULL, \
            kind TEXT NOT NULL\
         )",
    )
    .expect("create source");
    Spi::run("INSERT INTO pt_s.events (user_id, kind) VALUES (1, 'click'), (2, 'view')")
        .expect("seed");

    let r = crate::create_reflex_ivm(
        "pt_s.events_view",
        "SELECT id, user_id, kind FROM pt_s.events",
        Some("id"),
        None,
        Some("IMMEDIATE"),
        None,
    );
    assert_eq!(r, "CREATE REFLEX INCREMENTAL VIEW");

    // pt_new / pt_old in `pt_s`.
    let pt_new_schema = Spi::get_one::<String>(
        "SELECT n.nspname::TEXT FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid \
         WHERE c.relname = '__reflex_pt_new_events_view_pt_s_events'",
    )
    .expect("q")
    .expect("v");
    assert_eq!(pt_new_schema, "pt_s");

    Spi::run("SET search_path = pt_s").expect("narrow search_path");

    Spi::run("INSERT INTO pt_s.events (user_id, kind) VALUES (3, 'purchase')").expect("insert");
    Spi::run("UPDATE pt_s.events SET kind = 'CLICK' WHERE user_id = 1").expect("update");
    Spi::run("DELETE FROM pt_s.events WHERE user_id = 2").expect("delete");

    Spi::run("SET search_path = public, pt_s").expect("reset");

    let mismatches = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM (\
            (SELECT * FROM pt_s.events_view EXCEPT ALL SELECT id, user_id, kind FROM pt_s.events) \
            UNION ALL \
            (SELECT id, user_id, kind FROM pt_s.events EXCEPT ALL SELECT * FROM pt_s.events_view) \
         ) o",
    )
    .expect("oracle")
    .expect("v");
    assert_eq!(mismatches, 0, "passthrough IMV diverged under narrow search_path");
}

/// Two IMVs in *different* schemas feeding off the *same* source. Confirms
/// the per-source staging delta still lives in the source's schema (not
/// duplicated), and a shared source can fan out cross-schema without
/// search_path coupling.
#[pg_test]
fn pg_test_shared_source_two_imvs_in_distinct_schemas() {
    Spi::run("CREATE SCHEMA src_s").expect("schema");
    Spi::run("CREATE SCHEMA imv_a").expect("schema");
    Spi::run("CREATE SCHEMA imv_b").expect("schema");
    Spi::run("CREATE TABLE src_s.t (id SERIAL, grp TEXT, val INT)").expect("create source");
    Spi::run("INSERT INTO src_s.t (grp, val) VALUES ('x', 1), ('y', 2), ('x', 3)").expect("seed");

    crate::create_reflex_ivm(
        "imv_a.view_one",
        "SELECT grp, SUM(val) AS s FROM src_s.t GROUP BY grp",
        None,
        None,
        Some("DEFERRED"),
        None,
    );
    crate::create_reflex_ivm(
        "imv_b.view_two",
        "SELECT grp, COUNT(*) AS c FROM src_s.t GROUP BY grp",
        None,
        None,
        Some("DEFERRED"),
        None,
    );

    // The shared staging delta lives in `src_s` (per source), regardless of
    // which IMV's schema is involved.
    let delta_schema = Spi::get_one::<String>(
        "SELECT n.nspname::TEXT FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid \
         WHERE c.relname = '__reflex_delta_src_s_t'",
    )
    .expect("q")
    .expect("v");
    assert_eq!(delta_schema, "src_s");

    // Per-IMV artifacts live in each IMV's own schema.
    let int_a = Spi::get_one::<String>(
        "SELECT n.nspname::TEXT FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid \
         WHERE c.relname = '__reflex_intermediate_view_one'",
    )
    .expect("q")
    .expect("v");
    assert_eq!(int_a, "imv_a");

    let int_b = Spi::get_one::<String>(
        "SELECT n.nspname::TEXT FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid \
         WHERE c.relname = '__reflex_intermediate_view_two'",
    )
    .expect("q")
    .expect("v");
    assert_eq!(int_b, "imv_b");

    // DML under a search_path that includes ONLY the source schema (not public,
    // not either IMV schema): the trigger body must still resolve everything.
    Spi::run("SET search_path = src_s").expect("narrow search_path");
    Spi::run("INSERT INTO src_s.t (grp, val) VALUES ('x', 100), ('z', 50)").expect("insert");
    Spi::run("SELECT public.reflex_flush_deferred('src_s.t')").expect("flush");
    Spi::run("SET search_path = public, src_s, imv_a, imv_b").expect("reset");

    let m1 = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM (\
            (SELECT * FROM imv_a.view_one EXCEPT ALL SELECT grp, SUM(val) FROM src_s.t GROUP BY grp) \
            UNION ALL \
            (SELECT grp, SUM(val) FROM src_s.t GROUP BY grp EXCEPT ALL SELECT * FROM imv_a.view_one) \
         ) o",
    ).expect("oracle a").expect("v");
    assert_eq!(m1, 0);

    let m2 = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM (\
            (SELECT * FROM imv_b.view_two EXCEPT ALL SELECT grp, COUNT(*) FROM src_s.t GROUP BY grp) \
            UNION ALL \
            (SELECT grp, COUNT(*) FROM src_s.t GROUP BY grp EXCEPT ALL SELECT * FROM imv_b.view_two) \
         ) o",
    ).expect("oracle b").expect("v");
    assert_eq!(m2, 0);
}

/// DEFERRED IMV over a JOIN with schema-qualified sources whose base query
/// uses BARE-name column qualifiers (`demand_planning.id`, not
/// `alp.demand_planning.id`). Regression for the customer-reported bug where
/// `UPDATE alp.demand_planning …` produced
/// `WARNING: pg_reflex: IMV alp.ivm_sop_forecast_view flush failed at cascade:
///  missing FROM-clause entry for table "__reflex_new_alp_demand_planning"`.
///
/// Root cause: `replace_source_with_delta` rewrote only the schema-qualified
/// column form (`alp.demand_planning.col` → `__dt.col`) and left bare
/// `demand_planning.col` untouched. The subsequent `replace_source_with_transition`
/// pass inside `reflex_build_delta_sql` then wholesale-rewrote bare
/// `demand_planning.col` to `"__reflex_new_alp_demand_planning".col` — but
/// that transition table is not in the deferred flush's FROM clause.
#[pg_test]
fn pg_test_deferred_join_schema_qualified_with_bare_column_qualifiers() {
    Spi::run("CREATE SCHEMA alp").expect("schema");
    Spi::run(
        "CREATE TABLE alp.demand_planning (\
            id SERIAL PRIMARY KEY, \
            status TEXT NOT NULL, \
            qty INT NOT NULL\
         )",
    )
    .expect("create dp");
    Spi::run(
        "CREATE TABLE alp.sales_simulation (\
            id SERIAL PRIMARY KEY, \
            dem_plan_id INT NOT NULL, \
            product_id INT NOT NULL, \
            qty_sales INT NOT NULL\
         )",
    )
    .expect("create ss");
    Spi::run(
        "INSERT INTO alp.demand_planning (status, qty) VALUES \
            ('validated', 10), ('draft', 20), ('validated', 30)",
    )
    .expect("seed dp");
    Spi::run(
        "INSERT INTO alp.sales_simulation (dem_plan_id, product_id, qty_sales) VALUES \
            (1, 100, 5), (1, 101, 7), (2, 100, 3), (3, 102, 11)",
    )
    .expect("seed ss");

    // JOIN with bare-name column qualifiers — same shape as the customer IMV.
    let r = crate::create_reflex_ivm(
        "alp.ivm_sop_forecast_view",
        "SELECT dem_plan_id, sales_simulation.product_id, \
                SUM(qty_sales) AS quantity \
         FROM alp.sales_simulation \
         INNER JOIN alp.demand_planning \
           ON demand_planning.id = sales_simulation.dem_plan_id \
         WHERE demand_planning.status IN ('validated', 'current') \
         GROUP BY dem_plan_id, sales_simulation.product_id",
        None,
        None,
        Some("DEFERRED"),
        None,
    );
    assert_eq!(r, "CREATE REFLEX INCREMENTAL VIEW");

    // Customer-reported failing statement.
    Spi::run("UPDATE alp.demand_planning SET status = 'validated' WHERE id = 2")
        .expect("update dp");
    Spi::run("SELECT public.reflex_flush_deferred('alp.demand_planning')").expect("flush dp");

    // Also exercise the sister source (sales_simulation uses bare
    // `sales_simulation.product_id` qualifiers in the base_query) under all
    // three op shapes: INSERT, DELETE, UPDATE.
    Spi::run("INSERT INTO alp.sales_simulation (dem_plan_id, product_id, qty_sales) VALUES (2, 100, 9)")
        .expect("insert ss");
    Spi::run("SELECT public.reflex_flush_deferred('alp.sales_simulation')").expect("flush ss insert");

    Spi::run("DELETE FROM alp.sales_simulation WHERE dem_plan_id = 1 AND product_id = 101")
        .expect("delete ss");
    Spi::run("SELECT public.reflex_flush_deferred('alp.sales_simulation')").expect("flush ss delete");

    Spi::run("UPDATE alp.sales_simulation SET qty_sales = qty_sales + 100 WHERE dem_plan_id = 1")
        .expect("update ss");
    Spi::run("SELECT public.reflex_flush_deferred('alp.sales_simulation')").expect("flush ss update");

    // The flush must not have set last_error.
    let err: Option<String> = Spi::get_one::<&str>(
        "SELECT last_error FROM public.__reflex_ivm_reference \
         WHERE name = 'alp.ivm_sop_forecast_view'",
    )
    .expect("read last_error")
    .map(|s| s.to_string());
    assert!(
        err.is_none(),
        "flush should not record an error, got: {:?}",
        err
    );

    let fresh = "SELECT dem_plan_id, sales_simulation.product_id, \
                        SUM(qty_sales) AS quantity \
                 FROM alp.sales_simulation \
                 INNER JOIN alp.demand_planning \
                   ON demand_planning.id = sales_simulation.dem_plan_id \
                 WHERE demand_planning.status IN ('validated', 'current') \
                 GROUP BY dem_plan_id, sales_simulation.product_id";
    let mismatches = Spi::get_one::<i64>(&format!(
        "SELECT COUNT(*) FROM (\
            (SELECT * FROM alp.ivm_sop_forecast_view EXCEPT ALL SELECT * FROM ({fresh}) f1) \
            UNION ALL \
            (SELECT * FROM ({fresh}) f2 EXCEPT ALL SELECT * FROM alp.ivm_sop_forecast_view) \
         ) o"
    ))
    .expect("oracle")
    .expect("v");
    assert_eq!(
        mismatches, 0,
        "deferred IMV diverged from fresh after JOIN-source UPDATE/INSERT",
    );
}

/// Top-K MIN/MAX IMV under narrow search_path. Exercises the per-IMV
/// `__reflex_shrunk_*` capture table during UPDATE (the N1 path).
#[pg_test]
fn pg_test_topk_imv_in_custom_schema_under_set_search_path() {
    Spi::run("CREATE SCHEMA topk_s").expect("schema");
    Spi::run("CREATE TABLE topk_s.t (id SERIAL, grp TEXT, v INT)").expect("create");
    Spi::run("INSERT INTO topk_s.t (grp, v) SELECT 'g' || (i % 4), i FROM generate_series(1, 100) i")
        .expect("seed");

    let r = crate::create_reflex_ivm(
        "topk_s.minmax",
        "SELECT grp, MIN(v) AS mn, MAX(v) AS mx FROM topk_s.t GROUP BY grp",
        None,
        None,
        Some("IMMEDIATE"),
        None,
    );
    assert_eq!(r, "CREATE REFLEX INCREMENTAL VIEW");

    let shrunk_schema = Spi::get_one::<String>(
        "SELECT n.nspname::TEXT FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid \
         WHERE c.relname = '__reflex_shrunk_minmax'",
    )
    .expect("q")
    .expect("v");
    assert_eq!(shrunk_schema, "topk_s");

    Spi::run("SET search_path = topk_s").expect("narrow search_path");
    Spi::run("UPDATE topk_s.t SET v = v + 1000 WHERE id IN (1, 2, 3, 4, 5)").expect("update");
    Spi::run("DELETE FROM topk_s.t WHERE id BETWEEN 6 AND 10").expect("delete");
    Spi::run("SET search_path = public, topk_s").expect("reset");

    let fresh = "SELECT grp, MIN(v) AS mn, MAX(v) AS mx FROM topk_s.t GROUP BY grp";
    let mismatches = Spi::get_one::<i64>(&format!(
        "SELECT COUNT(*) FROM (\
            (SELECT * FROM topk_s.minmax EXCEPT ALL SELECT * FROM ({fresh}) f1) \
            UNION ALL \
            (SELECT * FROM ({fresh}) f2 EXCEPT ALL SELECT * FROM topk_s.minmax) \
         ) o"
    ))
    .expect("oracle")
    .expect("v");
    assert_eq!(mismatches, 0, "top-K MIN/MAX IMV diverged under narrow search_path");
}

/// DEFERRED IMV created from a search_path-relative bare source name, where a
/// homonym table exists in `public` with a DIFFERENT column shape. Reproduces
/// the customer-reported error
///   `column "company_id" does not exist`
/// that fires at COMMIT-time flush: `reflex_flush_deferred('foo')` split a
/// schemaless name, fell back to `nspname='public'`, projected the wrong
/// (public-homonym) column set, then applied it on the actual source schema's
/// staging delta.
///
/// The IMV must canonicalize bare sources to their search_path-resolved schema
/// at create time so depends_on, pending storage, and catalog lookups all key
/// off the same `schema.table`.
#[pg_test]
fn pg_test_deferred_imv_bare_source_with_public_homonym() {
    // Custom schema's `foo` — narrow column shape (id, value).
    Spi::run("CREATE SCHEMA hsch").expect("schema");
    Spi::run("CREATE TABLE hsch.foo (id INT PRIMARY KEY, value INT)").expect("create custom foo");
    Spi::run("INSERT INTO hsch.foo VALUES (1, 10), (2, 20)").expect("seed");

    // public.foo — homonym with an EXTRA column not present in hsch.foo.
    // Mirrors `public.location` carrying `company_id` in the customer DB.
    Spi::run(
        "CREATE TABLE public.foo (\
            extra_company_id INT NOT NULL DEFAULT 0, \
            id INT PRIMARY KEY, \
            value INT)",
    )
    .expect("create public foo");

    // Create the IMV with a BARE source reference under a search_path whose
    // first entry is the custom schema. PG resolves bare `foo` to `hsch.foo`
    // at IMV-build time.
    Spi::run("SET search_path = hsch, public").expect("set search_path");

    let r = crate::create_reflex_ivm(
        "hsch.foo_view",
        "SELECT id, SUM(value) AS s FROM foo GROUP BY id",
        None,
        None,
        Some("DEFERRED"),
        None,
    );
    assert_eq!(r, "CREATE REFLEX INCREMENTAL VIEW");

    // The trigger must be on hsch.foo (search_path-resolved at DDL time).
    // After canonicalization the trigger-name suffix is `schema_table`.
    let trig_schema = Spi::get_one::<String>(
        "SELECT n.nspname::TEXT FROM pg_trigger t \
         JOIN pg_class c ON c.oid = t.tgrelid \
         JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE t.tgname = '__reflex_trigger_upd_on_hsch_foo'",
    )
    .expect("trig lookup")
    .expect("trigger row");
    assert_eq!(trig_schema, "hsch", "trigger should be on hsch.foo");

    // The reported bug fires at COMMIT-time flush: `reflex_flush_deferred`
    // splits a schemaless `'foo'`, falls back to `nspname='public'`, projects
    // the wrong (public.foo) column set, then crashes when it can't find
    // `extra_company_id` on the actual staging delta. Drive the flush manually
    // using whatever the trigger stored in `__reflex_deferred_pending` — the
    // value that the COMMIT-time constraint trigger would itself pass.
    Spi::run("UPDATE hsch.foo SET value = 99 WHERE id = 1").expect("update under deferred");

    let stored_source: String = Spi::get_one::<String>(
        "SELECT source_table FROM public.__reflex_deferred_pending LIMIT 1",
    )
    .expect("pending")
    .expect("a row should be queued for the bare source");

    Spi::run(&format!(
        "SELECT public.reflex_flush_deferred('{}')",
        stored_source.replace('\'', "''")
    ))
    .expect("flush must not crash on homonym schema mis-resolution");

    // And the IMV must still equal a fresh recomputation.
    let fresh = "SELECT id, SUM(value) AS s FROM hsch.foo GROUP BY id";
    let mismatches = Spi::get_one::<i64>(&format!(
        "SELECT COUNT(*) FROM (\
            (SELECT * FROM hsch.foo_view EXCEPT ALL SELECT * FROM ({fresh}) f1) \
            UNION ALL \
            (SELECT * FROM ({fresh}) f2 EXCEPT ALL SELECT * FROM hsch.foo_view) \
         ) o"
    ))
    .expect("oracle")
    .expect("v");
    assert_eq!(
        mismatches, 0,
        "deferred IMV over bare source diverged from fresh (homonym in public)"
    );
}

/// Guardrail: creating a bare-named IMV under a non-public search_path still
/// succeeds and co-locates its maintenance tables in the head schema, but emits
/// a warning recommending a schema-qualified name (bare maintenance SQL is
/// search_path-fragile). This test pins the non-breaking behavior; the warning
/// itself is surfaced to the operator at creation time.
#[pg_test]
fn pg_test_bare_name_under_nonpublic_search_path_warns_and_creates() {
    Spi::run("CREATE SCHEMA gtnt").expect("schema");
    Spi::run("CREATE TABLE gtnt.s (id SERIAL, grp TEXT, val NUMERIC)").expect("source");
    Spi::run("INSERT INTO gtnt.s (grp, val) VALUES ('a', 1)").expect("seed");
    Spi::run("SET search_path = gtnt").expect("narrow search_path");

    let r = crate::create_reflex_ivm(
        "gv",
        "SELECT grp, SUM(val) AS total FROM s GROUP BY grp",
        None,
        None,
        None,
        None,
    );
    assert_eq!(r, "CREATE REFLEX INCREMENTAL VIEW");

    // Aux tables co-located in gtnt (the head schema), not public.
    let int_schema = Spi::get_one::<String>(
        "SELECT n.nspname::text FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE c.relname = '__reflex_intermediate_gv'",
    )
    .expect("q")
    .expect("v");
    assert_eq!(int_schema, "gtnt");
}

/// Drop must remove per-source trigger functions qualified with public. schema.
/// Tasks 1-3 made per-source trigger functions be created as
/// `public.__reflex_{ins,del,upd,trunc}_trigger_on_<safe_source>`. The DROP
/// path must use the same qualification so that under a non-public search_path,
/// it actually finds and drops the `public.` copies (not just unqualified
/// names that resolve elsewhere or not at all).
#[pg_test]
fn test_drop_removes_public_qualified_per_source_fns() {
    Spi::run("CREATE SCHEMA s_drop").unwrap();
    Spi::run("CREATE TABLE s_drop.orders (id INT PRIMARY KEY, amount INT)").unwrap();
    Spi::run("INSERT INTO s_drop.orders VALUES (1, 10)").unwrap();
    crate::create_reflex_ivm(
        "s_drop.orders_mv",
        "SELECT id, amount FROM s_drop.orders",
        None,
        None,
        None,
        None,
    );

    // Check that per-source trigger functions exist in public schema.
    let before: i64 = Spi::get_one(
        "SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace \
         WHERE n.nspname='public' AND p.proname LIKE '__reflex_%trigger_on_s_drop_orders'",
    ).unwrap().unwrap();
    assert!(before >= 1, "expected public per-source fns after create, got {before}");

    // Now restrict search_path to s_drop only (excludes public).
    // The DROP statements will run under this search_path.
    Spi::run("SET search_path = s_drop").unwrap();

    crate::drop_reflex_ivm("s_drop.orders_mv");

    // Reset to a path that includes public so the query works.
    Spi::run("SET search_path = public, s_drop").unwrap();

    // Verify all per-source trigger functions in public were removed.
    let after: i64 = Spi::get_one(
        "SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace \
         WHERE n.nspname='public' AND p.proname LIKE '__reflex_%trigger_on_s_drop_orders'",
    ).unwrap().unwrap();
    assert_eq!(after, 0, "drop must remove the public per-source fns, {after} left");
}

/// Tasks 1-3 commit: the deferred-flush function is created as `public.__reflex_deferred_flush_fn`,
/// the single constraint trigger `__reflex_deferred_flush_trigger` on `public.__reflex_deferred_pending`
/// binds the public copy, and the function self-registers as a `pg_reflex` extension member.
/// This test asserts that when multiple DEFERRED IMVs are created under different schemas,
/// only ONE public `__reflex_deferred_flush_fn` exists as an extension member, and the
/// shared constraint trigger binds the public copy.
#[pg_test]
fn test_deferred_imv_under_two_schemas_keeps_single_public_member_flush_fn() {
    for sch in ["s_one", "s_two"] {
        Spi::run(&format!("CREATE SCHEMA IF NOT EXISTS {sch}")).unwrap();
        Spi::run(&format!("SET search_path = {sch}, public")).unwrap();
        Spi::run(&format!("CREATE TABLE {sch}.t (id INT PRIMARY KEY, g TEXT, v INT)")).unwrap();
        Spi::run(&format!("INSERT INTO {sch}.t VALUES (1,'a',10)")).unwrap();
        crate::create_reflex_ivm(
            &format!("{sch}.mv"),
            &format!("SELECT g, sum(v) AS total FROM {sch}.t GROUP BY g"),
            None,
            None,
            Some("DEFERRED"),
            None,
        );
    }
    Spi::run("SET search_path = public").unwrap();

    let copies: i64 = Spi::get_one(
        "SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace \
         WHERE p.proname = '__reflex_deferred_flush_fn'",
    ).unwrap().unwrap();
    assert_eq!(copies, 1, "expected one flush fn copy, got {copies}");

    let schema: String = Spi::get_one(
        "SELECT n.nspname::text FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace \
         WHERE p.proname = '__reflex_deferred_flush_fn'",
    ).unwrap().unwrap();
    assert_eq!(schema, "public", "flush fn must be in public schema");

    let is_member: bool = Spi::get_one(
        "SELECT EXISTS(SELECT 1 FROM pg_depend d \
           JOIN pg_extension e ON e.oid=d.refobjid AND e.extname='pg_reflex' \
           JOIN pg_proc p ON p.oid=d.objid \
           JOIN pg_namespace n ON n.oid=p.pronamespace \
           WHERE d.deptype='e' AND p.proname='__reflex_deferred_flush_fn' AND n.nspname='public')",
    ).unwrap().unwrap();
    assert!(is_member, "flush fn must be a pg_reflex extension member");

    let trig_fn_schema: String = Spi::get_one(
        "SELECT pn.nspname::text FROM pg_trigger t \
           JOIN pg_proc p ON p.oid=t.tgfoid JOIN pg_namespace pn ON pn.oid=p.pronamespace \
           WHERE t.tgname='__reflex_deferred_flush_trigger' AND NOT t.tgisinternal",
    ).unwrap().unwrap();
    assert_eq!(trig_fn_schema, "public", "trigger must bind the public flush fn");
}

/// 1.8.0 self-heal migration: consolidates per-schema duplicate copies of
/// __reflex_deferred_flush_fn (created when IMVs were built under non-public
/// search_path) into a single canonical public copy. Test reproduces a split
/// state and validates the inline heal SQL logic.
#[pg_test]
fn test_self_heal_consolidates_split_flush_fn() {
    // Ensure baseline deferred infra exists (public flush fn + trigger + queue).
    for stmt in crate::schema_builder::build_deferred_flush_ddl() {
        Spi::run(&stmt).unwrap();
    }

    // Reproduce split state: a non-public copy bound to the live trigger, non-member.
    Spi::run("CREATE SCHEMA IF NOT EXISTS s_heal").unwrap();
    Spi::run(
        "CREATE OR REPLACE FUNCTION s_heal.__reflex_deferred_flush_fn() RETURNS TRIGGER AS $fn$ \
         BEGIN PERFORM public.reflex_flush_deferred(NEW.source_table); RETURN NULL; END; \
         $fn$ LANGUAGE plpgsql",
    ).unwrap();
    Spi::run("DROP TRIGGER IF EXISTS __reflex_deferred_flush_trigger ON public.__reflex_deferred_pending")
        .unwrap();
    Spi::run(
        "CREATE CONSTRAINT TRIGGER __reflex_deferred_flush_trigger \
         AFTER INSERT ON public.__reflex_deferred_pending DEFERRABLE INITIALLY DEFERRED \
         FOR EACH ROW EXECUTE FUNCTION s_heal.__reflex_deferred_flush_fn()",
    ).unwrap();

    // Verify split state: two copies exist, one non-public.
    let copies_before: i64 = Spi::get_one(
        "SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace \
         WHERE p.proname='__reflex_deferred_flush_fn'",
    ).unwrap().unwrap();
    assert_eq!(copies_before, 2, "expected two copies before heal");

    // --- Heal SQL (mirror of sql/pg_reflex--1.7.7--1.8.0.sql steps 1-3 + 5) ---

    // Step 1: Adopt-if-orphan the public copy so step 2's CREATE OR REPLACE is allowed.
    Spi::run(
        "DO $heal$ BEGIN \
           IF EXISTS (SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace \
                      WHERE p.proname='__reflex_deferred_flush_fn' AND n.nspname='public') \
              AND NOT EXISTS (SELECT 1 FROM pg_depend d \
                JOIN pg_extension e ON e.oid=d.refobjid AND e.extname='pg_reflex' \
                JOIN pg_proc p ON p.oid=d.objid JOIN pg_namespace n ON n.oid=p.pronamespace \
                WHERE d.deptype='e' AND p.proname='__reflex_deferred_flush_fn' AND n.nspname='public') \
           THEN ALTER EXTENSION pg_reflex ADD FUNCTION public.__reflex_deferred_flush_fn(); \
           END IF; END $heal$;",
    ).unwrap();

    // Step 2: Canonical public flush fn (auto-members via CREATE in extension).
    Spi::run(
        "CREATE OR REPLACE FUNCTION public.__reflex_deferred_flush_fn() RETURNS TRIGGER AS $fn$ \
         BEGIN PERFORM public.reflex_flush_deferred(NEW.source_table); RETURN NULL; END; \
         $fn$ LANGUAGE plpgsql",
    ).unwrap();

    // Step 3: Re-point the single live trigger at the public copy.
    Spi::run("DROP TRIGGER IF EXISTS __reflex_deferred_flush_trigger ON public.__reflex_deferred_pending")
        .unwrap();
    Spi::run(
        "CREATE CONSTRAINT TRIGGER __reflex_deferred_flush_trigger \
         AFTER INSERT ON public.__reflex_deferred_pending DEFERRABLE INITIALLY DEFERRED \
         FOR EACH ROW EXECUTE FUNCTION public.__reflex_deferred_flush_fn()",
    ).unwrap();

    // Step 5: Orphan sweep — drop every __reflex_* function in a non-public schema bound
    // to no live trigger (the now-unbound per-schema copies).
    Spi::run(
        "DO $sweep$ DECLARE r RECORD; BEGIN \
           FOR r IN SELECT n.nspname AS schema, p.proname AS fn FROM pg_proc p \
             JOIN pg_namespace n ON n.oid=p.pronamespace \
             WHERE p.proname LIKE '__reflex\\_%' AND n.nspname <> 'public' \
               AND NOT EXISTS (SELECT 1 FROM pg_trigger t WHERE t.tgfoid=p.oid AND NOT t.tgisinternal) \
           LOOP EXECUTE format('DROP FUNCTION IF EXISTS %I.%I()', r.schema, r.fn); END LOOP; \
         END $sweep$;",
    ).unwrap();

    // --- Assert clean state ---
    let copies_after: i64 = Spi::get_one(
        "SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace \
         WHERE p.proname='__reflex_deferred_flush_fn'",
    ).unwrap().unwrap();
    assert_eq!(copies_after, 1, "expected single flush fn after heal, got {copies_after}");

    let trig_schema: String = Spi::get_one(
        "SELECT pn.nspname::text FROM pg_trigger t JOIN pg_proc p ON p.oid=t.tgfoid \
         JOIN pg_namespace pn ON pn.oid=p.pronamespace \
         WHERE t.tgname='__reflex_deferred_flush_trigger' AND NOT t.tgisinternal",
    ).unwrap().unwrap();
    assert_eq!(trig_schema, "public", "trigger must bind the public flush fn after heal");

    let is_member: bool = Spi::get_one(
        "SELECT EXISTS(SELECT 1 FROM pg_depend d \
           JOIN pg_extension e ON e.oid=d.refobjid AND e.extname='pg_reflex' \
           JOIN pg_proc p ON p.oid=d.objid \
           JOIN pg_namespace n ON n.oid=p.pronamespace \
           WHERE d.deptype='e' AND p.proname='__reflex_deferred_flush_fn' AND n.nspname='public')",
    ).unwrap().unwrap();
    assert!(is_member, "flush fn must be extension member after heal");
}

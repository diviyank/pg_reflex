
#[pg_test]
fn test_deferred_basic_insert() {
    Spi::run("CREATE TABLE def_src (id SERIAL, city TEXT, amount NUMERIC)")
        .expect("create table");
    Spi::run("INSERT INTO def_src (city, amount) VALUES ('Paris', 100), ('London', 200)")
        .expect("insert seed");

    let result = crate::create_reflex_ivm(
        "def_view",
        "SELECT city, SUM(amount) AS total FROM def_src GROUP BY city",
        None,
        None,
        Some("DEFERRED"),
        None,
    );
    assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");

    // Verify refresh_mode in reference table
    let mode = Spi::get_one::<String>(
        "SELECT refresh_mode FROM public.__reflex_ivm_reference WHERE name = 'def_view'",
    ).expect("query").expect("value");
    assert_eq!(mode, "DEFERRED");

    // Verify staging table exists
    let staging_exists = Spi::get_one::<bool>(
        "SELECT EXISTS(SELECT 1 FROM pg_class WHERE relname = '__reflex_delta_def_src')",
    ).expect("query").expect("value");
    assert!(staging_exists, "Staging table should exist");

    // Verify deferred pending table exists
    let pending_exists = Spi::get_one::<bool>(
        "SELECT EXISTS(SELECT 1 FROM pg_class WHERE relname = '__reflex_deferred_pending')",
    ).expect("query").expect("value");
    assert!(pending_exists, "Deferred pending table should exist");

    // Verify initial data is correct (created during initial materialization)
    let paris_total = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT total FROM def_view WHERE city = 'Paris'",
    ).expect("query").expect("value");
    assert_eq!(paris_total.to_string(), "100");
}

#[pg_test]
fn test_immediate_mode_explicit() {
    Spi::run("CREATE TABLE imm_src (id SERIAL, city TEXT, amount NUMERIC)")
        .expect("create table");
    Spi::run("INSERT INTO imm_src (city, amount) VALUES ('Paris', 100)")
        .expect("insert");

    let result = crate::create_reflex_ivm(
        "imm_view",
        "SELECT city, SUM(amount) AS total FROM imm_src GROUP BY city",
        None,
        None,
        Some("IMMEDIATE"),
        None,
    );
    assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");

    // Verify it works like normal: INSERT should update immediately
    Spi::run("INSERT INTO imm_src (city, amount) VALUES ('Paris', 50)")
        .expect("insert");
    let total = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT total FROM imm_view WHERE city = 'Paris'",
    ).expect("query").expect("value");
    assert_eq!(total.to_string(), "150");
}

#[pg_test]
fn test_invalid_mode() {
    Spi::run("CREATE TABLE inv_mode (id SERIAL, val INT)").expect("create table");
    let result = crate::create_reflex_ivm(
        "inv_mode_view",
        "SELECT val, COUNT(*) AS cnt FROM inv_mode GROUP BY val",
        None,
        None,
        Some("INVALID"),
        None,
    );
    assert!(result.starts_with("ERROR:"), "Invalid mode should return error, got: {}", result);
}

/// Deferred: GROUP BY SUM/COUNT — INSERT + manual flush + oracle
#[pg_test]
fn test_deferred_groupby_insert_oracle() {
    Spi::run("CREATE TABLE dfi (id SERIAL, city TEXT NOT NULL, amount INT NOT NULL)").expect("create");
    Spi::run("INSERT INTO dfi (city, amount) VALUES ('Paris', 100), ('Paris', 200), ('London', 50)").expect("seed");

    crate::create_reflex_ivm("dfi_view",
        "SELECT city, SUM(amount) AS total, COUNT(*) AS cnt FROM dfi GROUP BY city",
        None, None, Some("DEFERRED"), None);

    let fresh = "SELECT city, SUM(amount) AS total, COUNT(*) AS cnt FROM dfi GROUP BY city";
    assert_imv_correct("dfi_view", fresh);

    // INSERT — delta staged, view NOT yet updated
    Spi::run("INSERT INTO dfi (city, amount) VALUES ('Paris', 50), ('Berlin', 300)").expect("insert");

    // Verify delta was staged
    let staged = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM __reflex_delta_dfi"
    ).expect("q").expect("v");
    assert!(staged > 0, "Delta should be staged: {} rows", staged);

    // Manual flush (simulates COMMIT constraint trigger)
    Spi::run("SELECT reflex_flush_deferred('dfi')").expect("flush");

    // Oracle check after flush
    assert_imv_correct("dfi_view", fresh);

    // Paris=350, London=50, Berlin=300
    let paris = Spi::get_one::<i64>(
        "SELECT total FROM dfi_view WHERE city = 'Paris'"
    ).expect("q").expect("v");
    assert_eq!(paris, 350i64);
}

/// Deferred: multiple INSERTs coalesced into single flush
#[pg_test]
fn test_deferred_batch_coalescing() {
    Spi::run("CREATE TABLE dbc (id SERIAL, grp TEXT NOT NULL, val INT NOT NULL)").expect("create");
    Spi::run("INSERT INTO dbc (grp, val) VALUES ('a', 10)").expect("seed");

    crate::create_reflex_ivm("dbc_view",
        "SELECT grp, SUM(val) AS total FROM dbc GROUP BY grp",
        None, None, Some("DEFERRED"), None);

    let fresh = "SELECT grp, SUM(val) AS total FROM dbc GROUP BY grp";

    // Multiple INSERTs — all staged, not flushed
    Spi::run("INSERT INTO dbc (grp, val) VALUES ('a', 20)").expect("insert 1");
    Spi::run("INSERT INTO dbc (grp, val) VALUES ('a', 30)").expect("insert 2");
    Spi::run("INSERT INTO dbc (grp, val) VALUES ('b', 100)").expect("insert 3");
    Spi::run("INSERT INTO dbc (grp, val) VALUES ('b', 200)").expect("insert 4");

    // All 4 coalesced in one flush
    Spi::run("SELECT reflex_flush_deferred('dbc')").expect("flush");
    assert_imv_correct("dbc_view", fresh);
    // a: 10+20+30=60, b: 100+200=300
}

/// Deferred: DELETE + flush + oracle
#[pg_test]
fn test_deferred_delete_oracle() {
    Spi::run("CREATE TABLE dfd (id SERIAL PRIMARY KEY, grp TEXT NOT NULL, val INT NOT NULL)").expect("create");
    Spi::run("INSERT INTO dfd (grp, val) VALUES ('a', 10), ('a', 20), ('b', 30)").expect("seed");

    crate::create_reflex_ivm("dfd_view",
        "SELECT grp, SUM(val) AS total FROM dfd GROUP BY grp",
        None, None, Some("DEFERRED"), None);

    let fresh = "SELECT grp, SUM(val) AS total FROM dfd GROUP BY grp";
    assert_imv_correct("dfd_view", fresh);

    // DELETE
    Spi::run("DELETE FROM dfd WHERE val = 10").expect("delete");
    Spi::run("SELECT reflex_flush_deferred('dfd')").expect("flush");
    assert_imv_correct("dfd_view", fresh);

    // Delete entire group
    Spi::run("DELETE FROM dfd WHERE grp = 'a'").expect("delete group");
    Spi::run("SELECT reflex_flush_deferred('dfd')").expect("flush");
    assert_imv_correct("dfd_view", fresh);
}

/// Deferred: UPDATE + flush + oracle
#[pg_test]
fn test_deferred_update_oracle() {
    Spi::run("CREATE TABLE dfu (id SERIAL PRIMARY KEY, grp TEXT NOT NULL, val INT NOT NULL)").expect("create");
    Spi::run("INSERT INTO dfu (grp, val) VALUES ('a', 10), ('a', 20), ('b', 30)").expect("seed");

    crate::create_reflex_ivm("dfu_view",
        "SELECT grp, SUM(val) AS total FROM dfu GROUP BY grp",
        None, None, Some("DEFERRED"), None);

    let fresh = "SELECT grp, SUM(val) AS total FROM dfu GROUP BY grp";
    assert_imv_correct("dfu_view", fresh);

    // UPDATE value
    Spi::run("UPDATE dfu SET val = 99 WHERE val = 10").expect("update");
    Spi::run("SELECT reflex_flush_deferred('dfu')").expect("flush");
    assert_imv_correct("dfu_view", fresh);

    // UPDATE group key (move row between groups)
    Spi::run("UPDATE dfu SET grp = 'b' WHERE val = 20").expect("move group");
    Spi::run("SELECT reflex_flush_deferred('dfu')").expect("flush");
    assert_imv_correct("dfu_view", fresh);
}

/// Deferred: mixed INSERT + DELETE + UPDATE, single flush
#[pg_test]
fn test_deferred_mixed_mutations() {
    Spi::run("CREATE TABLE dfm (id SERIAL PRIMARY KEY, grp TEXT NOT NULL, val INT NOT NULL)").expect("create");
    Spi::run("INSERT INTO dfm (grp, val) VALUES ('a', 10), ('a', 20), ('b', 30), ('c', 40)").expect("seed");

    crate::create_reflex_ivm("dfm_view",
        "SELECT grp, SUM(val) AS total, COUNT(*) AS cnt FROM dfm GROUP BY grp",
        None, None, Some("DEFERRED"), None);

    let fresh = "SELECT grp, SUM(val) AS total, COUNT(*) AS cnt FROM dfm GROUP BY grp";
    assert_imv_correct("dfm_view", fresh);

    // Multiple mixed mutations, all staged
    Spi::run("INSERT INTO dfm (grp, val) VALUES ('a', 100)").expect("insert");
    Spi::run("DELETE FROM dfm WHERE grp = 'c'").expect("delete");
    Spi::run("UPDATE dfm SET val = 999 WHERE grp = 'b'").expect("update");
    Spi::run("INSERT INTO dfm (grp, val) VALUES ('d', 50), ('d', 60)").expect("insert 2");

    // Single flush processes all accumulated deltas
    Spi::run("SELECT reflex_flush_deferred('dfm')").expect("flush");
    assert_imv_correct("dfm_view", fresh);
}

/// Deferred: DISTINCT with ref counting
#[pg_test]
fn test_deferred_distinct_oracle() {
    Spi::run("CREATE TABLE dfdst (id SERIAL PRIMARY KEY, val TEXT NOT NULL)").expect("create");
    Spi::run("INSERT INTO dfdst (val) VALUES ('x'), ('x'), ('y'), ('z')").expect("seed");

    crate::create_reflex_ivm("dfdst_view",
        "SELECT DISTINCT val FROM dfdst",
        None, None, Some("DEFERRED"), None);

    let fresh = "SELECT DISTINCT val FROM dfdst";
    assert_imv_correct("dfdst_view", fresh);

    // Insert duplicate
    Spi::run("INSERT INTO dfdst (val) VALUES ('x')").expect("insert dup");
    Spi::run("SELECT reflex_flush_deferred('dfdst')").expect("flush");
    assert_imv_correct("dfdst_view", fresh);

    // Delete one copy of 'x' — should still appear (refcount > 0)
    Spi::run("DELETE FROM dfdst WHERE id = 1").expect("delete one");
    Spi::run("SELECT reflex_flush_deferred('dfdst')").expect("flush");
    assert_imv_correct("dfdst_view", fresh);

    // Delete all 'z' — should disappear
    Spi::run("DELETE FROM dfdst WHERE val = 'z'").expect("delete z");
    Spi::run("SELECT reflex_flush_deferred('dfdst')").expect("flush");
    assert_imv_correct("dfdst_view", fresh);
}

/// Deferred: NULLs in aggregate columns — INSERT and DELETE with NULLs
#[pg_test]
fn test_deferred_null_values() {
    Spi::run("CREATE TABLE dfn (id SERIAL PRIMARY KEY, grp TEXT NOT NULL, val INT)").expect("create");
    Spi::run("INSERT INTO dfn (grp, val) VALUES ('a', 10), ('a', NULL), ('b', 30)").expect("seed");

    crate::create_reflex_ivm("dfn_view",
        "SELECT grp, SUM(val) AS total, COUNT(val) AS cv, COUNT(*) AS cs FROM dfn GROUP BY grp",
        None, None, Some("DEFERRED"), None);

    let fresh = "SELECT grp, SUM(val) AS total, COUNT(val) AS cv, COUNT(*) AS cs FROM dfn GROUP BY grp";
    assert_imv_correct("dfn_view", fresh);

    // INSERT NULL value
    Spi::run("INSERT INTO dfn (grp, val) VALUES ('a', NULL)").expect("insert null");
    Spi::run("SELECT reflex_flush_deferred('dfn')").expect("flush");
    assert_imv_correct("dfn_view", fresh);

    // INSERT non-NULL value
    Spi::run("INSERT INTO dfn (grp, val) VALUES ('a', 50)").expect("insert non-null");
    Spi::run("SELECT reflex_flush_deferred('dfn')").expect("flush");
    assert_imv_correct("dfn_view", fresh);

    // DELETE a NULL row
    Spi::run("DELETE FROM dfn WHERE val IS NULL AND id = (SELECT MIN(id) FROM dfn WHERE val IS NULL)").expect("delete null");
    Spi::run("SELECT reflex_flush_deferred('dfn')").expect("flush");
    assert_imv_correct("dfn_view", fresh);

    // DELETE a non-NULL row
    Spi::run("DELETE FROM dfn WHERE val = 30").expect("delete non-null");
    Spi::run("SELECT reflex_flush_deferred('dfn')").expect("flush");
    assert_imv_correct("dfn_view", fresh);
}

/// Deferred: fuzz — random mutations + flush + oracle
#[pg_test]
fn test_deferred_fuzz() {
    Spi::run("SELECT setseed(0.88)").expect("seed");
    Spi::run("CREATE TABLE df_fuzz (id SERIAL PRIMARY KEY, grp INT NOT NULL, val INT NOT NULL)").expect("create");
    Spi::run("INSERT INTO df_fuzz (grp, val) SELECT (random()*10)::int, (random()*500)::int FROM generate_series(1, 200)").expect("seed data");

    crate::create_reflex_ivm("df_fuzz_view",
        "SELECT grp, SUM(val) AS total, COUNT(*) AS cnt FROM df_fuzz GROUP BY grp",
        None, None, Some("DEFERRED"), None);

    let fresh = "SELECT grp, SUM(val) AS total, COUNT(*) AS cnt FROM df_fuzz GROUP BY grp";
    assert_imv_correct("df_fuzz_view", fresh);

    for _ in 0..5 {
        // Batch of 3-5 random mutations
        for _ in 0..3 {
            match Spi::get_one::<i32>("SELECT (random()*2)::int").expect("q").expect("v") {
                0 => Spi::run("INSERT INTO df_fuzz (grp, val) SELECT (random()*10)::int, (random()*500)::int FROM generate_series(1, (1+random()*20)::int)").expect("insert"),
                1 => Spi::run("DELETE FROM df_fuzz WHERE id IN (SELECT id FROM df_fuzz ORDER BY random() LIMIT (1+random()*5)::int)").expect("delete"),
                _ => Spi::run("UPDATE df_fuzz SET val = (random()*999)::int WHERE id = (SELECT id FROM df_fuzz ORDER BY random() LIMIT 1)").expect("update"),
            };
        }
        // Flush and verify
        Spi::run("SELECT reflex_flush_deferred('df_fuzz')").expect("flush");
        assert_imv_correct("df_fuzz_view", fresh);
    }
}

/// Deferred: UPDATE non-NULL to NULL — all values in group become NULL → SUM must be NULL
#[pg_test]
fn test_deferred_update_to_null_all_null_group() {
    Spi::run("CREATE TABLE dfun (id SERIAL PRIMARY KEY, grp TEXT NOT NULL, val INT)").expect("create");
    Spi::run("INSERT INTO dfun (grp, val) VALUES ('a', 10), ('a', NULL), ('b', 30)").expect("seed");
    // Group 'a': SUM=10, COUNT(val)=1, COUNT(*)=2

    crate::create_reflex_ivm("dfun_view",
        "SELECT grp, SUM(val) AS total, COUNT(val) AS cv, COUNT(*) AS cs FROM dfun GROUP BY grp",
        None, None, Some("DEFERRED"), None);

    let fresh = "SELECT grp, SUM(val) AS total, COUNT(val) AS cv, COUNT(*) AS cs FROM dfun GROUP BY grp";
    assert_imv_correct("dfun_view", fresh);

    // UPDATE the only non-NULL val in group 'a' to NULL
    // After: group 'a' has (NULL, NULL) → SUM=NULL, COUNT(val)=0, COUNT(*)=2
    Spi::run("UPDATE dfun SET val = NULL WHERE val = 10").expect("update to null");
    Spi::run("SELECT reflex_flush_deferred('dfun')").expect("flush");
    assert_imv_correct("dfun_view", fresh);
}

/// Same bug test for IMMEDIATE mode — verify the immediate path handles this correctly
#[pg_test]
fn test_immediate_update_to_null_all_null_group() {
    Spi::run("CREATE TABLE imun (id SERIAL PRIMARY KEY, grp TEXT NOT NULL, val INT)").expect("create");
    Spi::run("INSERT INTO imun (grp, val) VALUES ('a', 10), ('a', NULL), ('b', 30)").expect("seed");

    crate::create_reflex_ivm("imun_view",
        "SELECT grp, SUM(val) AS total, COUNT(val) AS cv, COUNT(*) AS cs FROM imun GROUP BY grp",
        None, None, Some("IMMEDIATE"), None);

    let fresh = "SELECT grp, SUM(val) AS total, COUNT(val) AS cv, COUNT(*) AS cs FROM imun GROUP BY grp";
    assert_imv_correct("imun_view", fresh);

    // UPDATE the only non-NULL val to NULL
    Spi::run("UPDATE imun SET val = NULL WHERE val = 10").expect("update to null");
    assert_imv_correct("imun_view", fresh);
}

/// Test A — zscore-style duplicate-key regression.
/// When a grouped aggregate IMV has a unique index on its group key, inserting
/// a row that maps into an existing group must not violate the index at flush.
/// Regression for: sibling-CTE DELETE+INSERT pattern where INSERT can't see DELETE.
#[pg_test]
fn test_deferred_groupby_unique_index_existing_group() {
    Spi::run("CREATE TABLE dfgk (id SERIAL PRIMARY KEY, grp TEXT NOT NULL, val INT NOT NULL)")
        .expect("create");
    Spi::run("INSERT INTO dfgk (grp, val) VALUES ('a', 10), ('b', 20)").expect("seed");

    crate::create_reflex_ivm(
        "dfgk_view",
        "SELECT grp, SUM(val) AS total FROM dfgk GROUP BY grp",
        None,
        None,
        Some("DEFERRED"),
        None,
    );

    // Unique index on group key — mirrors the real-world zscore_reflex setup.
    Spi::run("CREATE UNIQUE INDEX dfgk_view_unique ON dfgk_view (grp)").expect("unique idx");

    let fresh = "SELECT grp, SUM(val) AS total FROM dfgk GROUP BY grp";
    assert_imv_correct("dfgk_view", fresh);

    // INSERT a row mapping to an existing group — requires the flush to
    // refresh group 'a' without violating the unique index.
    Spi::run("INSERT INTO dfgk (grp, val) VALUES ('a', 100)").expect("insert existing");
    Spi::run("SELECT reflex_flush_deferred('dfgk')").expect("flush");
    assert_imv_correct("dfgk_view", fresh);

    // UPDATE an existing group's value — same risk.
    Spi::run("UPDATE dfgk SET val = 999 WHERE grp = 'b' AND val = 20").expect("update");
    Spi::run("SELECT reflex_flush_deferred('dfgk')").expect("flush");
    assert_imv_correct("dfgk_view", fresh);
}

/// Test B — qualified source-table refs in SELECT and GROUP BY.
/// Mirrors stock_transfer_baseline_reflex which uses `src.col AS alias` in
/// SELECT and mixed qualified/unqualified refs in GROUP BY.
/// Regression for: `replace_identifier` corrupting qualified refs by inlining
/// the `(SELECT ...) AS __dt` subquery before the dot.
#[pg_test]
fn test_deferred_qualified_source_refs() {
    Spi::run("CREATE TABLE dfqs (id SERIAL PRIMARY KEY, grp TEXT NOT NULL, raw INT NOT NULL, val INT NOT NULL)")
        .expect("create");
    Spi::run("INSERT INTO dfqs (grp, raw, val) VALUES ('a', 1, 10), ('b', 2, 20)").expect("seed");

    // Mix qualified and unqualified in SELECT, GROUP BY, and join predicates.
    // Also rename a grouped column via `AS` — the false-positive-warning case.
    crate::create_reflex_ivm(
        "dfqs_view",
        "SELECT dfqs.grp, dfqs.raw AS raw_renamed, SUM(dfqs.val) AS total \
         FROM dfqs GROUP BY dfqs.grp, raw",
        None,
        None,
        Some("DEFERRED"),
        None,
    );

    let fresh = "SELECT dfqs.grp, dfqs.raw AS raw_renamed, SUM(dfqs.val) AS total \
                 FROM dfqs GROUP BY dfqs.grp, raw";
    assert_imv_correct("dfqs_view", fresh);

    Spi::run("INSERT INTO dfqs (grp, raw, val) VALUES ('a', 1, 5), ('c', 3, 30)").expect("insert");
    Spi::run("SELECT reflex_flush_deferred('dfqs')").expect("flush");
    assert_imv_correct("dfqs_view", fresh);

    Spi::run("UPDATE dfqs SET val = 99 WHERE grp = 'b'").expect("update");
    Spi::run("SELECT reflex_flush_deferred('dfqs')").expect("flush");
    assert_imv_correct("dfqs_view", fresh);

    Spi::run("DELETE FROM dfqs WHERE grp = 'c'").expect("delete");
    Spi::run("SELECT reflex_flush_deferred('dfqs')").expect("flush");
    assert_imv_correct("dfqs_view", fresh);
}

/// Passthrough IMV + DEFERRED flush exercises every delta op without leaking
/// the IMMEDIATE-only `__reflex_old_<src>` transition-table reference.
/// Regression for: the passthrough DELETE path in reflex_build_delta_sql
/// literally names that table; the flush must stand it up as a temp view
/// over the delta or the unconditional DELETE/UPDATE calls fail to parse.
#[pg_test]
fn test_deferred_passthrough_all_ops() {
    Spi::run("CREATE TABLE dfpa (id SERIAL PRIMARY KEY, k TEXT NOT NULL, v INT NOT NULL)")
        .expect("create");
    Spi::run("INSERT INTO dfpa (k, v) VALUES ('a', 10), ('b', 20)").expect("seed");

    // Passthrough IMV: explicit unique key, no aggregate.
    crate::create_reflex_ivm(
        "dfpa_view",
        "SELECT id, k, v FROM dfpa",
        Some("id"),
        None,
        Some("DEFERRED"),
        None,
    );

    let fresh = "SELECT id, k, v FROM dfpa";
    assert_imv_correct("dfpa_view", fresh);

    // INSERT-only flush must not trip the DELETE-branch staging reference.
    Spi::run("INSERT INTO dfpa (k, v) VALUES ('c', 30)").expect("insert");
    Spi::run("SELECT reflex_flush_deferred('dfpa')").expect("flush");
    assert_imv_correct("dfpa_view", fresh);

    // DELETE flush.
    Spi::run("DELETE FROM dfpa WHERE k = 'a'").expect("delete");
    Spi::run("SELECT reflex_flush_deferred('dfpa')").expect("flush");
    assert_imv_correct("dfpa_view", fresh);

    // UPDATE flush.
    Spi::run("UPDATE dfpa SET v = 99 WHERE k = 'b'").expect("update");
    Spi::run("SELECT reflex_flush_deferred('dfpa')").expect("flush");
    assert_imv_correct("dfpa_view", fresh);

    // Mixed batch.
    Spi::run("INSERT INTO dfpa (k, v) VALUES ('d', 40)").expect("ins");
    Spi::run("DELETE FROM dfpa WHERE k = 'c'").expect("del");
    Spi::run("UPDATE dfpa SET v = 101 WHERE k = 'b'").expect("upd");
    Spi::run("SELECT reflex_flush_deferred('dfpa')").expect("flush");
    assert_imv_correct("dfpa_view", fresh);
}

/// Regression for journal/2026-04-21_min_max_recompute_bug.md:
/// DEFERRED DELETE on a source feeding a BOOL_OR aggregate whose argument
/// references a LEFT JOIN alias. The recompute step used to emit a scalar
/// subquery `SELECT BOOL_OR(alias.col ...) FROM source_table WHERE ...` —
/// the JOIN alias wasn't in the subquery's FROM, so the flush aborted with
/// `missing FROM-clause entry for table "alias"`.
#[pg_test]
fn test_deferred_bool_or_with_join_alias_recompute() {
    Spi::run("CREATE TABLE brja_src (g INT NOT NULL, p INT)").expect("create src");
    Spi::run("CREATE TABLE brja_dim (p INT PRIMARY KEY)").expect("create dim");
    Spi::run("INSERT INTO brja_src VALUES (1, 1), (1, 2), (2, 3)").expect("seed src");
    Spi::run("INSERT INTO brja_dim VALUES (1)").expect("seed dim");

    crate::create_reflex_ivm(
        "brja_view",
        "SELECT brja_src.g, BOOL_OR(d.p IS NOT NULL) AS has_match \
         FROM brja_src LEFT JOIN brja_dim d ON d.p = brja_src.p \
         GROUP BY brja_src.g",
        None,
        None,
        Some("DEFERRED"),
        None,
    );

    let fresh = "SELECT brja_src.g, BOOL_OR(d.p IS NOT NULL) AS has_match \
                 FROM brja_src LEFT JOIN brja_dim d ON d.p = brja_src.p \
                 GROUP BY brja_src.g";
    assert_imv_correct("brja_view", fresh);

    Spi::run("DELETE FROM brja_src WHERE p = 1").expect("delete");
    Spi::run("SELECT reflex_flush_deferred('brja_src')").expect("flush");
    assert_imv_correct("brja_view", fresh);

    Spi::run("INSERT INTO brja_src VALUES (1, 1)").expect("reinsert");
    Spi::run("SELECT reflex_flush_deferred('brja_src')").expect("flush");
    assert_imv_correct("brja_view", fresh);
}

/// Cross-source consistency: a join IMV over two DEFERRED sources where BOTH
/// sources are mutated in the same transaction.
///
/// In deferred mode the base-table writes land immediately; only IMV
/// maintenance is deferred to the COMMIT constraint trigger, which calls
/// `reflex_flush_deferred(src)` once per distinct mutated source. By the time
/// the first flush runs, BOTH base tables already hold their new rows — so
/// `flush('jbs_a')` computes ΔA ⋈ B_new and `flush('jbs_b')` computes
/// ΔB ⋈ A_new. Their net deltas each include the ΔA×ΔB cross product, so the
/// additive MERGE applies it TWICE; the correct view delta contains it once.
/// Running the two manual flushes after both inserts reproduces the
/// commit-time ordering exactly.
///
/// The 100 seed groups matter: with a large intermediate the per-flush
/// MERGE-vs-rebuild dispatch (1.4.5) takes the additive MERGE path for the
/// single affected group. A small (2-group) intermediate instead trips the
/// high-selectivity rebuild path, which recomputes the affected group from the
/// live base query and accidentally masks the double-count. Key 200 is
/// brand-new in both sources, so ΔA⋈ΔB is non-empty and the bug surfaces as
/// cnt=2/total=200 for k=200 instead of the correct cnt=1/total=100.
#[pg_test]
fn test_deferred_join_both_sources_mutated_oracle() {
    Spi::run("CREATE TABLE jbs_a (k INT NOT NULL, amt INT NOT NULL)").expect("create a");
    Spi::run("CREATE TABLE jbs_b (k INT NOT NULL, w INT NOT NULL)").expect("create b");
    Spi::run("INSERT INTO jbs_a (k, amt) SELECT g, 10 FROM generate_series(1, 100) g")
        .expect("seed a");
    Spi::run("INSERT INTO jbs_b (k, w) SELECT g, 5 FROM generate_series(1, 100) g")
        .expect("seed b");

    crate::create_reflex_ivm(
        "jbs_view",
        "SELECT jbs_a.k, COUNT(*) AS cnt, SUM(jbs_a.amt) AS total \
         FROM jbs_a JOIN jbs_b ON jbs_a.k = jbs_b.k \
         GROUP BY jbs_a.k",
        None,
        None,
        Some("DEFERRED"),
        None,
    );

    let fresh = "SELECT jbs_a.k, COUNT(*) AS cnt, SUM(jbs_a.amt) AS total \
                 FROM jbs_a JOIN jbs_b ON jbs_a.k = jbs_b.k \
                 GROUP BY jbs_a.k";
    assert_imv_correct("jbs_view", fresh);

    // Both sources mutated with a brand-new shared key — deltas staged, view
    // not yet updated.
    Spi::run("INSERT INTO jbs_a (k, amt) VALUES (200, 100)").expect("insert a");
    Spi::run("INSERT INTO jbs_b (k, w) VALUES (200, 7)").expect("insert b");

    // Mirror the COMMIT constraint trigger: flush every mutated source, both
    // base tables already populated.
    Spi::run("SELECT reflex_flush_deferred('jbs_a')").expect("flush a");
    Spi::run("SELECT reflex_flush_deferred('jbs_b')").expect("flush b");

    assert_imv_correct("jbs_view", fresh);
}

/// Regression for journal/2026-04-21_db_clone_benchmark.md bug 2/3:
/// DEFERRED flush on a source that is referenced with a user alias
/// (`FROM src AS s` or bare `FROM src s`) used to emit invalid SQL like
/// `FROM (SELECT … ) AS __dt AS s` from `replace_source_with_delta`.
#[pg_test]
fn test_deferred_flush_consumes_user_alias_in_from() {
    Spi::run("CREATE TABLE dfua (id SERIAL PRIMARY KEY, grp TEXT NOT NULL, val INT NOT NULL)")
        .expect("create");
    Spi::run("INSERT INTO dfua (grp, val) VALUES ('a', 10), ('a', 20), ('b', 30)")
        .expect("seed");

    crate::create_reflex_ivm(
        "dfua_view",
        "SELECT s.grp, SUM(s.val) AS total FROM dfua AS s GROUP BY s.grp",
        None,
        None,
        Some("DEFERRED"),
        None,
    );

    let fresh = "SELECT s.grp, SUM(s.val) AS total FROM dfua AS s GROUP BY s.grp";
    assert_imv_correct("dfua_view", fresh);

    Spi::run("INSERT INTO dfua (grp, val) VALUES ('a', 5), ('c', 100)").expect("insert");
    Spi::run("SELECT reflex_flush_deferred('dfua')").expect("flush");
    assert_imv_correct("dfua_view", fresh);

    Spi::run("DELETE FROM dfua WHERE grp = 'a' AND val = 10").expect("delete");
    Spi::run("SELECT reflex_flush_deferred('dfua')").expect("flush");
    assert_imv_correct("dfua_view", fresh);

    Spi::run("UPDATE dfua SET val = 999 WHERE grp = 'b'").expect("update");
    Spi::run("SELECT reflex_flush_deferred('dfua')").expect("flush");
    assert_imv_correct("dfua_view", fresh);
}

/// Test D — renamed grouped column should not cause creation to misbehave.
/// Regression for: the "not in GROUP BY" warning that fires on `src.col AS
/// other_name` even when `col` is in GROUP BY. Also verifies the renamed
/// column is populated correctly under IMMEDIATE mode.
#[pg_test]
fn test_immediate_renamed_grouped_column() {
    Spi::run("CREATE TABLE dfrn (id SERIAL PRIMARY KEY, src_col TEXT NOT NULL, val INT NOT NULL)")
        .expect("create");
    Spi::run("INSERT INTO dfrn (src_col, val) VALUES ('a', 10), ('b', 20)").expect("seed");

    crate::create_reflex_ivm(
        "dfrn_view",
        "SELECT dfrn.src_col AS renamed, SUM(val) AS total FROM dfrn GROUP BY src_col",
        None,
        None,
        Some("IMMEDIATE"),
        None,
    );

    let fresh = "SELECT dfrn.src_col AS renamed, SUM(val) AS total FROM dfrn GROUP BY src_col";
    assert_imv_correct("dfrn_view", fresh);

    Spi::run("INSERT INTO dfrn (src_col, val) VALUES ('a', 5)").expect("insert");
    assert_imv_correct("dfrn_view", fresh);

    // Verify renamed column is actually populated (not NULL).
    let row_count = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM dfrn_view WHERE renamed IS NOT NULL",
    )
    .expect("q")
    .expect("v");
    assert!(row_count > 0, "renamed column should be populated");
}

// ========================================================================
// #3 / #12b — DO-block gate + where_predicate flush correctness
// ========================================================================

#[pg_test]
fn test_flush_is_noop_when_affected_empty() {
    Spi::run("CREATE TABLE noop_src (id SERIAL PRIMARY KEY, grp TEXT NOT NULL, val INT NOT NULL, note TEXT)").expect("create");
    Spi::run("INSERT INTO noop_src (grp, val, note) VALUES ('a', 10, 'x'), ('b', 20, 'y')").expect("seed");
    crate::create_reflex_ivm(
        "noop_view",
        "SELECT grp, SUM(val) AS total FROM noop_src GROUP BY grp",
        None,
        None,
        Some("DEFERRED"),
        None,
    );
    let fresh = "SELECT grp, SUM(val) AS total FROM noop_src GROUP BY grp";
    assert_imv_correct("noop_view", fresh);
    Spi::run("UPDATE noop_src SET note = 'changed'").expect("update non-agg col");
    Spi::run("SELECT reflex_flush_deferred('noop_src')").expect("flush");
    assert_imv_correct("noop_view", fresh);
}

#[pg_test]
fn test_flush_correct_after_empty_delta_gate_sequence() {
    Spi::run("CREATE TABLE gate_src (id SERIAL PRIMARY KEY, grp TEXT NOT NULL, val INT NOT NULL)").expect("create");
    Spi::run("INSERT INTO gate_src (grp, val) VALUES ('a', 10)").expect("seed");
    crate::create_reflex_ivm(
        "gate_view",
        "SELECT grp, SUM(val) AS total FROM gate_src GROUP BY grp",
        None,
        None,
        Some("DEFERRED"),
        None,
    );
    let fresh = "SELECT grp, SUM(val) AS total FROM gate_src GROUP BY grp";
    assert_imv_correct("gate_view", fresh);
    Spi::run("INSERT INTO gate_src (grp, val) VALUES ('b', 20)").expect("insert");
    Spi::run("SELECT reflex_flush_deferred('gate_src')").expect("flush after insert");
    assert_imv_correct("gate_view", fresh);
    Spi::run("DELETE FROM gate_src WHERE grp = 'b'").expect("delete");
    Spi::run("SELECT reflex_flush_deferred('gate_src')").expect("flush after delete");
    assert_imv_correct("gate_view", fresh);
    Spi::run("INSERT INTO gate_src (grp, val) VALUES ('b', 30)").expect("re-insert");
    Spi::run("SELECT reflex_flush_deferred('gate_src')").expect("flush after re-insert");
    assert_imv_correct("gate_view", fresh);
}

#[pg_test]
fn test_deferred_upd_respects_where_predicate() {
    Spi::run("CREATE TABLE pred_src (id SERIAL PRIMARY KEY, grp TEXT NOT NULL, val INT NOT NULL, status TEXT NOT NULL)").expect("create");
    Spi::run("INSERT INTO pred_src (grp, val, status) VALUES ('a', 10, 'active'), ('b', 20, 'active'), ('c', 30, 'inactive')").expect("seed");
    crate::create_reflex_ivm(
        "pred_view",
        "SELECT grp, SUM(val) AS total FROM pred_src WHERE status = 'active' GROUP BY grp",
        None,
        None,
        Some("DEFERRED"),
        None,
    );
    let fresh = "SELECT grp, SUM(val) AS total FROM pred_src WHERE status = 'active' GROUP BY grp";
    assert_imv_correct("pred_view", fresh);
    Spi::run("UPDATE pred_src SET val = 999 WHERE status = 'inactive'").expect("update inactive");
    Spi::run("SELECT reflex_flush_deferred('pred_src')").expect("flush");
    assert_imv_correct("pred_view", fresh);
}

#[pg_test]
fn test_flush_deferred_skips_imv_on_predicate_miss() {
    Spi::run("CREATE TABLE two_src (id SERIAL PRIMARY KEY, grp TEXT NOT NULL, val INT NOT NULL, status TEXT NOT NULL)").expect("create");
    Spi::run("INSERT INTO two_src (grp, val, status) VALUES ('a', 10, 'active'), ('b', 20, 'active')").expect("seed");
    crate::create_reflex_ivm(
        "two_view_active",
        "SELECT grp, SUM(val) AS total FROM two_src WHERE status = 'active' GROUP BY grp",
        None,
        None,
        Some("DEFERRED"),
        None,
    );
    crate::create_reflex_ivm(
        "two_view_never",
        "SELECT grp, SUM(val) AS total FROM two_src WHERE status = 'never' GROUP BY grp",
        None,
        None,
        Some("DEFERRED"),
        None,
    );
    let fresh_active = "SELECT grp, SUM(val) AS total FROM two_src WHERE status = 'active' GROUP BY grp";
    let fresh_never = "SELECT grp, SUM(val) AS total FROM two_src WHERE status = 'never' GROUP BY grp";
    assert_imv_correct("two_view_active", fresh_active);
    assert_imv_correct("two_view_never", fresh_never);
    Spi::run("INSERT INTO two_src (grp, val, status) VALUES ('c', 30, 'active')").expect("insert");
    Spi::run("SELECT reflex_flush_deferred('two_src')").expect("flush");
    assert_imv_correct("two_view_active", fresh_active);
    assert_imv_correct("two_view_never", fresh_never);
    let never_int_count = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM __reflex_intermediate_two_view_never",
    )
    .expect("q")
    .expect("v");
    assert_eq!(never_int_count, 0, "intermediate for 'never' IMV must have 0 rows");
}

#[pg_test]
fn pg_test_flush_histogram_accumulates() {
    Spi::run("CREATE TABLE hist_src (id SERIAL, grp TEXT, val NUMERIC)").expect("create");
    Spi::run("INSERT INTO hist_src (grp, val) VALUES ('a', 1)").expect("seed");

    crate::create_reflex_ivm(
        "hist_view",
        "SELECT grp, SUM(val) AS total FROM hist_src GROUP BY grp",
        None, None, Some("DEFERRED"),
        None,
    );

    // Flush a handful of times so the ring buffer accumulates samples.
    for i in 0..5 {
        Spi::run(&format!("INSERT INTO hist_src (grp, val) VALUES ('a', {})", i + 2))
            .expect("insert");
        Spi::run("SELECT reflex_flush_deferred('hist_src')").expect("flush");
    }

    let samples: i64 = Spi::get_one("SELECT samples FROM reflex_ivm_histogram('hist_view')")
        .expect("q").expect("v");
    assert!(samples >= 5, "histogram should have at least 5 samples, got {}", samples);

    let p50: Option<f64> = Spi::get_one("SELECT p50_ms FROM reflex_ivm_histogram('hist_view')")
        .expect("q");
    assert!(p50.is_some(), "p50 should be populated when samples exist");
    assert!(p50.unwrap() >= 0.0, "p50 should be non-negative");

    let max: Option<i64> = Spi::get_one("SELECT max_ms FROM reflex_ivm_histogram('hist_view')")
        .expect("q");
    assert!(max.is_some());
}

#[pg_test]
fn pg_test_flush_histogram_empty_when_never_flushed() {
    Spi::run("CREATE TABLE hist_empty_src (id SERIAL, grp TEXT)").expect("create");
    crate::create_reflex_ivm(
        "hist_empty_view",
        "SELECT grp, COUNT(*) AS cnt FROM hist_empty_src GROUP BY grp",
        None, None, Some("DEFERRED"),
        None,
    );

    let samples: i64 = Spi::get_one("SELECT samples FROM reflex_ivm_histogram('hist_empty_view')")
        .expect("q").expect("v");
    assert_eq!(samples, 0, "no flushes yet, no samples");
}

/// 1.4.3 — Spurious-UPDATE short-circuit. When a statement-level UPDATE
/// stages U_OLD/U_NEW rows whose source-column projections are byte-identical
/// (the row was "updated" to the value it already had), no IMV can observe a
/// change. `reflex_flush_deferred` must skip every IMV body, leaving
/// flush_count unchanged for those IMVs, and still clean up the staging
/// delta + pending pointer. Customer reproducer: `UPDATE … SET status =
/// 'validated' WHERE status = 'validated'`.
#[pg_test]
fn pg_test_deferred_spurious_update_skips_imv_bodies() {
    Spi::run("CREATE TABLE sp_src (id SERIAL PRIMARY KEY, status TEXT, amount INT)")
        .expect("create");
    Spi::run("INSERT INTO sp_src (status, amount) VALUES ('validated', 10), ('draft', 20)")
        .expect("seed");

    crate::create_reflex_ivm(
        "sp_view",
        "SELECT status, SUM(amount) AS total FROM sp_src GROUP BY status",
        None,
        None,
        Some("DEFERRED"),
        None,
    );

    // Baseline: flush_count after initial materialization (might be 0 or 1
    // depending on path) — capture it.
    let count_before: i64 = Spi::get_one(
        "SELECT COALESCE(flush_count, 0) FROM public.__reflex_ivm_reference WHERE name = 'sp_view'",
    )
    .expect("q")
    .expect("v");

    // Snapshot the target so we can assert it didn't change.
    let total_before: i64 = Spi::get_one::<i64>(
        "SELECT SUM(total)::BIGINT FROM sp_view",
    )
    .expect("q")
    .expect("v");

    // Spurious UPDATE — set every column to the value it already has. PG
    // still fires the statement-level trigger and stages U_OLD/U_NEW rows;
    // pg_reflex must detect the multiset equality and skip.
    Spi::run("UPDATE sp_src SET status = status, amount = amount").expect("spurious update");
    Spi::run("SELECT reflex_flush_deferred('sp_src')").expect("flush");

    let count_after: i64 = Spi::get_one(
        "SELECT COALESCE(flush_count, 0) FROM public.__reflex_ivm_reference WHERE name = 'sp_view'",
    )
    .expect("q")
    .expect("v");
    assert_eq!(
        count_before, count_after,
        "spurious flush must not increment flush_count (counted: {} -> {})",
        count_before, count_after
    );

    // Staging delta must be empty after the flush (skip path still cleans up).
    let staged_after: i64 = Spi::get_one(
        "SELECT COUNT(*)::BIGINT FROM __reflex_delta_sp_src",
    )
    .expect("q")
    .expect("v");
    assert_eq!(staged_after, 0, "staging delta must be cleaned up even on skip");

    // Pending pointer must also be cleared.
    let pending_after: i64 = Spi::get_one(
        "SELECT COUNT(*)::BIGINT FROM public.__reflex_deferred_pending WHERE source_table = 'sp_src'",
    )
    .expect("q")
    .expect("v");
    assert_eq!(pending_after, 0, "deferred-pending pointer must be cleared");

    // Target unchanged.
    let total_after: i64 = Spi::get_one::<i64>(
        "SELECT SUM(total)::BIGINT FROM sp_view",
    )
    .expect("q")
    .expect("v");
    assert_eq!(total_before, total_after, "target must be unchanged");

    // Sanity: a REAL update afterwards still processes correctly.
    Spi::run("UPDATE sp_src SET amount = amount + 1 WHERE id = 1").expect("real update");
    Spi::run("SELECT reflex_flush_deferred('sp_src')").expect("flush");
    let count_after_real: i64 = Spi::get_one(
        "SELECT COALESCE(flush_count, 0) FROM public.__reflex_ivm_reference WHERE name = 'sp_view'",
    )
    .expect("q")
    .expect("v");
    assert!(
        count_after_real > count_after,
        "real update must increment flush_count (was {}, now {})",
        count_after,
        count_after_real
    );
    let fresh_total: i64 = Spi::get_one::<i64>(
        "SELECT SUM(amount)::BIGINT FROM sp_src",
    )
    .expect("q")
    .expect("v");
    let view_total: i64 = Spi::get_one::<i64>(
        "SELECT SUM(total)::BIGINT FROM sp_view",
    )
    .expect("q")
    .expect("v");
    assert_eq!(fresh_total, view_total, "view should match fresh aggregate");
}

/// 1.4.5 — DEFERRED-mode filter-aware spurious-skip.
///
/// An UPDATE that changes a *filter-only* column between two predicate-
/// matching values must be skipped by the per-IMV filter-aware check at
/// flush time. The 1.4.3 byte-identical multiset check above did NOT catch
/// this — the column bytes differ — but the filter-aware check sees that
/// the IMV-relevant projection is unchanged and skips the IMV body.
#[pg_test]
fn pg_test_deferred_filter_aware_skip_status_whitelist_flip() {
    Spi::run("CREATE TABLE fas_def_src (id SERIAL PRIMARY KEY, status TEXT NOT NULL, amount INT NOT NULL)")
        .expect("create");
    Spi::run(
        "INSERT INTO fas_def_src (status, amount) VALUES \
            ('validated', 10), ('current', 20), ('draft', 30)",
    )
    .expect("seed");

    // IMV: status is filter-only (not in SELECT/GROUP BY).
    crate::create_reflex_ivm(
        "fas_def_view",
        "SELECT SUM(amount) AS total FROM fas_def_src \
         WHERE status IN ('validated', 'current')",
        None,
        None,
        Some("DEFERRED"),
        None,
    );

    let total_before: i64 = Spi::get_one::<i64>(
        "SELECT total::BIGINT FROM fas_def_view",
    )
    .expect("q")
    .expect("v");
    let count_before: i64 = Spi::get_one(
        "SELECT COALESCE(flush_count, 0) FROM public.__reflex_ivm_reference WHERE name = 'fas_def_view'",
    )
    .expect("q")
    .expect("v");

    // Filter-equivalent flip: status='validated' → 'current' on id=1.
    // Both pass the whitelist, and `status` is filter-only. The
    // byte-identical 1.4.3 check does NOT fire (`status` differs); the
    // 1.4.5 filter-aware per-IMV check MUST fire.
    Spi::run("UPDATE fas_def_src SET status='current' WHERE id=1").expect("flip");
    Spi::run("SELECT reflex_flush_deferred('fas_def_src')").expect("flush");

    let count_after: i64 = Spi::get_one(
        "SELECT COALESCE(flush_count, 0) FROM public.__reflex_ivm_reference WHERE name = 'fas_def_view'",
    )
    .expect("q")
    .expect("v");
    assert_eq!(
        count_before, count_after,
        "filter-equivalent flip must NOT increment flush_count for this IMV"
    );

    let total_after: i64 = Spi::get_one::<i64>(
        "SELECT total::BIGINT FROM fas_def_view",
    )
    .expect("q")
    .expect("v");
    assert_eq!(total_before, total_after, "target must be unchanged");

    // Staging delta and pending pointer cleaned up.
    let staged_after: i64 = Spi::get_one(
        "SELECT COUNT(*)::BIGINT FROM __reflex_delta_fas_def_src",
    )
    .expect("q")
    .expect("v");
    assert_eq!(staged_after, 0, "delta must be cleaned up after skip");
}

/// 1.4.5 — DEFERRED-mode filter-aware skip: row entering the whitelist
/// from outside must NOT skip. The IMV must add the row's contribution.
/// Mirrors the customer-shaped filter-entry case in
/// `pg_test_deferred_join_schema_qualified_with_bare_column_qualifiers`,
/// but on a single source so the EXCEPT-ALL check is exercised directly
/// (no JOIN-source noise).
#[pg_test]
fn pg_test_deferred_filter_aware_skip_filter_entry_runs_full_path() {
    Spi::run("CREATE TABLE fas_def_entry (id SERIAL PRIMARY KEY, grp TEXT NOT NULL, status TEXT NOT NULL, amount INT NOT NULL)")
        .expect("create");
    // id=1 already in whitelist; id=2 is OUTSIDE the whitelist initially.
    Spi::run(
        "INSERT INTO fas_def_entry (grp, status, amount) VALUES \
            ('a','validated', 10), ('a','draft', 20)",
    )
    .expect("seed");

    crate::create_reflex_ivm(
        "fas_def_entry_view",
        "SELECT grp, SUM(amount) AS total FROM fas_def_entry \
         WHERE status IN ('validated', 'current') GROUP BY grp",
        None,
        None,
        Some("DEFERRED"),
        None,
    );

    let total_before: i64 = Spi::get_one::<i64>(
        "SELECT total::BIGINT FROM fas_def_entry_view WHERE grp='a'",
    )
    .expect("q")
    .expect("v");
    assert_eq!(total_before, 10, "initial: only id=1 passes");

    // Flip id=2 INTO the whitelist (draft → validated). Filter-aware skip
    // must NOT fire (the multiset projections differ on the entry side),
    // the IMV body runs, and grp='a' gets id=2's amount.
    Spi::run("UPDATE fas_def_entry SET status='validated' WHERE id=2").expect("entry");
    Spi::run("SELECT reflex_flush_deferred('fas_def_entry')").expect("flush");

    let total_after: i64 = Spi::get_one::<i64>(
        "SELECT total::BIGINT FROM fas_def_entry_view WHERE grp='a'",
    )
    .expect("q")
    .expect("v");
    assert_eq!(total_after, 30, "must add id=2's contribution");
}

/// 1.5.1 — Regression: DEFERRED flush over a source that has a `json`
/// column must not blow up the COMMIT.
///
/// The 1.4.3 byte-identical spurious-UPDATE short-circuit projects *every*
/// source column into an `EXCEPT ALL` multiset comparison. The PG `json`
/// type has no `=` operator (only `jsonb` does), so any UPDATE on a source
/// with a `json` column used to crash with
/// `could not identify an equality operator for type json` at COMMIT.
///
/// The fix casts `json` / `xml` columns to `text` in the comparison
/// projection only — the TEMP VIEW that downstream IMV codegen reads still
/// projects the raw column.
#[pg_test]
fn pg_test_deferred_json_column_does_not_break_spurious_check() {
    Spi::run(
        "CREATE TABLE jcol_src (id SERIAL PRIMARY KEY, city TEXT, amount INT, meta json)",
    )
    .expect("create");
    Spi::run(
        "INSERT INTO jcol_src (city, amount, meta) VALUES \
            ('Paris', 10, '{\"a\":1}'::json), \
            ('London', 20, '{\"a\":2}'::json)",
    )
    .expect("seed");

    crate::create_reflex_ivm(
        "jcol_view",
        "SELECT city, SUM(amount) AS total FROM jcol_src GROUP BY city",
        None,
        None,
        Some("DEFERRED"),
        None,
    );

    // Real UPDATE that touches an IMV-relevant column. Previously crashed
    // the spurious-check `EXCEPT ALL` on the json column. Must succeed.
    Spi::run("UPDATE jcol_src SET amount = amount + 5 WHERE city = 'Paris'")
        .expect("real update with json column on source");
    Spi::run("SELECT reflex_flush_deferred('jcol_src')")
        .expect("flush must succeed despite json column");

    let paris_total: i64 =
        Spi::get_one::<i64>("SELECT total::BIGINT FROM jcol_view WHERE city = 'Paris'")
            .expect("q")
            .expect("v");
    assert_eq!(paris_total, 15, "Paris should be 10+5 after update");

    // Spurious UPDATE — sets every column to its current value. Spurious
    // check must run (without crashing on json) AND must detect equality.
    let count_before: i64 = Spi::get_one(
        "SELECT COALESCE(flush_count, 0) FROM public.__reflex_ivm_reference WHERE name = 'jcol_view'",
    )
    .expect("q")
    .expect("v");
    Spi::run("UPDATE jcol_src SET city = city, amount = amount, meta = meta")
        .expect("spurious update");
    Spi::run("SELECT reflex_flush_deferred('jcol_src')").expect("spurious flush");
    let count_after: i64 = Spi::get_one(
        "SELECT COALESCE(flush_count, 0) FROM public.__reflex_ivm_reference WHERE name = 'jcol_view'",
    )
    .expect("q")
    .expect("v");
    assert_eq!(
        count_before, count_after,
        "spurious flush over json source must skip IMV body"
    );
}

/// 1.5.1 — Regression: DEFERRED per-IMV filter-aware skip over a source
/// that has a `json` column in the IMV's relevant-column set must not
/// blow up the COMMIT.
///
/// The filter-aware skip projects `imv_relevant_columns[source]` into
/// `EXCEPT ALL`. If the IMV selects/joins/groups on a `json` column,
/// the same equality-operator gap applies.
#[pg_test]
fn pg_test_deferred_json_column_in_relevant_set_does_not_break_filter_aware_skip() {
    Spi::run(
        "CREATE TABLE jcol_rel (id SERIAL PRIMARY KEY, city TEXT, amount INT, meta json)",
    )
    .expect("create");
    Spi::run(
        "INSERT INTO jcol_rel (city, amount, meta) VALUES \
            ('Paris', 10, '{\"a\":1}'::json), \
            ('London', 20, '{\"a\":2}'::json)",
    )
    .expect("seed");

    // IMV passthrough — projects `meta` so it lands in imv_relevant_columns.
    crate::create_reflex_ivm(
        "jcol_rel_view",
        "SELECT id, city, amount, meta FROM jcol_rel",
        Some("id"),
        None,
        Some("DEFERRED"),
        None,
    );

    // UPDATE touches the non-json column only; the filter-aware EXCEPT ALL
    // path projects ALL of {id, city, amount, meta} — including json.
    Spi::run("UPDATE jcol_rel SET amount = amount + 1 WHERE city = 'Paris'")
        .expect("update non-json column");
    Spi::run("SELECT reflex_flush_deferred('jcol_rel')")
        .expect("flush must succeed with json in relevant cols");

    let paris_amount: i64 =
        Spi::get_one::<i64>("SELECT amount::BIGINT FROM jcol_rel_view WHERE city = 'Paris'")
            .expect("q")
            .expect("v");
    assert_eq!(paris_amount, 11);
}

/// 1.5.1 — Regression: passthrough IMV with multi-source JOIN whose
/// SELECT list uses bare (unqualified) column refs must not wrongly
/// attribute those refs to every source.
///
/// Reproduction (matches the alp.sop_forecast_view shape the user hit):
///   SELECT dem_plan_id, week, sales_simulation.product_id, ...
///   FROM sales_simulation INNER JOIN demand_planning
///     ON demand_planning.id = sales_simulation.dem_plan_id
///
/// `dem_plan_id` is bare in the SELECT. Pre-1.5.1 the analyzer over-
/// attributed bare refs to every real source as a safe-correctness
/// move, expecting `create_ivm` to filter against the catalog. The
/// filter only ran for AGGREGATE IMVs; passthrough IMVs persisted
/// `dem_plan_id` as a "relevant column" of `demand_planning`. An
/// UPDATE on `demand_planning` then crashed at trigger fire with
/// `column "dem_plan_id" does not exist`.
#[pg_test]
fn pg_test_passthrough_join_bare_ref_not_wrongly_attributed() {
    Spi::run("CREATE TABLE br_dp (id SERIAL PRIMARY KEY, status TEXT, modified_date TIMESTAMP)")
        .expect("create dp");
    Spi::run(
        "CREATE TABLE br_ss (id SERIAL PRIMARY KEY, dem_plan_id INT, product_id INT, qty INT)",
    )
    .expect("create ss");
    Spi::run("INSERT INTO br_dp (status, modified_date) VALUES \
              ('current', now()), ('current', now())")
        .expect("seed dp");
    Spi::run("INSERT INTO br_ss (dem_plan_id, product_id, qty) VALUES \
              (1, 100, 10), (1, 101, 20), (2, 100, 30)")
        .expect("seed ss");

    // Bare `dem_plan_id` in SELECT — analyzer would over-attribute to
    // both `br_ss` and `br_dp` without the catalog filter.
    crate::create_reflex_ivm(
        "br_join_view",
        "SELECT dem_plan_id, br_ss.product_id, qty \
         FROM br_ss \
         INNER JOIN br_dp ON br_dp.id = br_ss.dem_plan_id \
         WHERE br_dp.status = 'current'",
        Some("dem_plan_id,product_id"),
        None,
        Some("IMMEDIATE"),
        None,
    );

    // Sanity: persisted JSON must NOT carry `dem_plan_id` as a relevant
    // column of `br_dp` (the demand_planning analog). It must remain in
    // `br_ss`'s set.
    let dp_cols: String = Spi::get_one(
        "SELECT COALESCE( \
            (aggregations::jsonb->'imv_relevant_columns'->'br_dp')::text, \
            '[]' \
         ) FROM public.__reflex_ivm_reference WHERE name = 'br_join_view'",
    )
    .expect("q")
    .expect("v");
    assert!(
        !dp_cols.contains("dem_plan_id"),
        "br_dp's imv_relevant_columns must not contain dem_plan_id, got: {}",
        dp_cols
    );

    // The actual crash repro: UPDATE the source that doesn't have
    // `dem_plan_id`. Pre-fix this errored with
    // `column "dem_plan_id" does not exist`.
    Spi::run("UPDATE br_dp SET modified_date = now() WHERE id = 1")
        .expect("update on dp must not crash on bad column attribution");

    // And the IMV is still correct after a real change.
    Spi::run("INSERT INTO br_ss (dem_plan_id, product_id, qty) VALUES (2, 200, 5)")
        .expect("insert into ss");
    let total: i64 = Spi::get_one::<i64>("SELECT SUM(qty)::BIGINT FROM br_join_view")
        .expect("q")
        .expect("v");
    assert_eq!(total, 10 + 20 + 30 + 5, "IMV must include the new row");
}

/// 1.5.1 — Regression: IMMEDIATE-mode filter_skip_block in the UPDATE
/// trigger body must not crash when the IMV's relevant columns include
/// a `json` column.
///
/// The IMMEDIATE trigger builds `_skip_cols` from
/// `imv_relevant_columns[source]` and runs `EXCEPT ALL` over the OLD/NEW
/// transition tables. Same equality-operator trap.
#[pg_test]
fn pg_test_immediate_json_column_does_not_break_filter_skip_block() {
    Spi::run(
        "CREATE TABLE jcol_imm (id SERIAL PRIMARY KEY, city TEXT, amount INT, meta json)",
    )
    .expect("create");
    Spi::run(
        "INSERT INTO jcol_imm (city, amount, meta) VALUES \
            ('Paris', 10, '{\"a\":1}'::json), \
            ('London', 20, '{\"a\":2}'::json)",
    )
    .expect("seed");

    crate::create_reflex_ivm(
        "jcol_imm_view",
        "SELECT id, city, amount, meta FROM jcol_imm",
        Some("id"),
        None,
        Some("IMMEDIATE"),
        None,
    );

    // IMMEDIATE: the UPDATE itself fires the trigger; previously the
    // filter_skip_block EXCEPT ALL would crash here on the json column.
    Spi::run("UPDATE jcol_imm SET amount = amount + 5 WHERE city = 'Paris'")
        .expect("immediate update must succeed with json column");

    let paris_amount: i64 =
        Spi::get_one::<i64>("SELECT amount::BIGINT FROM jcol_imm_view WHERE city = 'Paris'")
            .expect("q")
            .expect("v");
    assert_eq!(paris_amount, 15);
}

/// 1.6.2 — Regression: when a per-source staging delta table outlives the
/// source's column layout (e.g. user dropped the source and recreated it
/// with reordered columns to add partitioning), the deferred trigger's
/// `INSERT INTO staging SELECT 'U_OLD', * FROM transition` previously
/// failed with `column "X" is of type … but expression is of type …`
/// because the SELECT * positions no longer matched the staging's columns.
///
/// The fix is to emit named-column inserts in the deferred trigger DDL so
/// that column ORDER drift does not poison the trigger. Column NAMES are
/// looked up at IMV-create time from the current source shape.
///
/// This test recreates the exact failure shape on a tiny source.
#[pg_test]
fn pg_test_deferred_stale_staging_after_source_recreate() {
    // v1 source: creation_date at position 7 (last)
    Spi::run(
        "CREATE TABLE stale_src (\
            id BIGINT PRIMARY KEY, \
            a INT NOT NULL, \
            b INT NOT NULL, \
            c INT NOT NULL, \
            d INT NOT NULL, \
            e INT NOT NULL, \
            creation_date TIMESTAMPTZ)",
    )
    .expect("create v1");

    // Create + drop a DEFERRED IMV — this leaves the staging delta behind.
    crate::create_reflex_ivm(
        "stale_v1_view",
        "SELECT id, a, b, c, d, e, creation_date FROM stale_src",
        Some("id"),
        None,
        Some("DEFERRED"),
        None,
    );
    crate::drop_reflex_ivm("stale_v1_view");

    // Confirm staging persisted across IMV drop (the precondition for
    // the stale-shape failure mode).
    let staging_kept = Spi::get_one::<bool>(
        "SELECT EXISTS(SELECT 1 FROM pg_class WHERE relname = '__reflex_delta_stale_src')",
    )
    .expect("q")
    .expect("v");
    assert!(staging_kept, "staging delta must persist across IMV drop");

    // Drop + recreate source with reordered columns (creation_date now at
    // position 4). This mirrors the user's situation where the partitioned
    // source was built from scratch with a different column order.
    Spi::run("DROP TABLE stale_src").expect("drop source");
    Spi::run(
        "CREATE TABLE stale_src (\
            id BIGINT PRIMARY KEY, \
            a INT NOT NULL, \
            b INT NOT NULL, \
            creation_date TIMESTAMPTZ, \
            c INT NOT NULL, \
            d INT NOT NULL, \
            e INT NOT NULL)",
    )
    .expect("create v2");

    // Seed BEFORE creating the IMV so seed rows are picked up by the
    // initial materialization (not by the deferred trigger path). This
    // keeps the regression focused on the trigger's named-column INSERT
    // and avoids exercising orthogonal flush coalescing behaviour.
    Spi::run(
        "INSERT INTO stale_src (id, a, b, creation_date, c, d, e) VALUES \
            (1, 10, 20, now(), 30, 40, 50), \
            (2, 11, 21, now(), 31, 41, 51)",
    )
    .expect("seed insert");

    // Create a new DEFERRED IMV on the recreated source. The fix path
    // must ensure the trigger does NOT issue a positional INSERT that
    // would mismatch the stale staging's column order.
    crate::create_reflex_ivm(
        "stale_v2_view",
        "SELECT id, a, b, creation_date, c, d, e FROM stale_src",
        Some("id"),
        None,
        Some("DEFERRED"),
        None,
    );

    // UPDATE — the original user-reported failure path. Pre-fix, this
    // tripped the trigger with `column "X" is of type … but expression
    // is of type …` because `INSERT INTO staging SELECT 'U_OLD', * FROM
    // transition` mismatched the stale staging positionally.
    Spi::run("UPDATE stale_src SET a = 99 WHERE id = 1")
        .expect("update must succeed despite stale staging shape");

    // DELETE — symmetry check across all three trigger ops.
    Spi::run("DELETE FROM stale_src WHERE id = 2")
        .expect("delete must succeed despite stale staging shape");

    // Flush + correctness oracle.
    Spi::run("SELECT reflex_flush_deferred('stale_src')").expect("flush");
    let fresh = "SELECT id, a, b, creation_date, c, d, e FROM stale_src";
    assert_imv_correct("stale_v2_view", fresh);
}

/// 1.6.2 — Companion to the reorder-only regression above. When the
/// source's column SET (not just order) drifts between IMV incarnations,
/// the staging delta column names no longer match either. With an EMPTY
/// staging, `ensure_staging_matches_source` must drop+recreate it so the
/// new named-column INSERT path finds the columns it expects.
#[pg_test]
fn pg_test_deferred_empty_stale_staging_with_column_set_drift_recreated() {
    Spi::run("CREATE TABLE setdrift_src (id BIGINT PRIMARY KEY, a INT, extra TEXT)")
        .expect("v1");
    crate::create_reflex_ivm(
        "setdrift_v1",
        "SELECT id, a, extra FROM setdrift_src",
        Some("id"),
        None,
        Some("DEFERRED"),
        None,
    );
    crate::drop_reflex_ivm("setdrift_v1");

    // Source loses a column.
    Spi::run("DROP TABLE setdrift_src").expect("drop");
    Spi::run("CREATE TABLE setdrift_src (id BIGINT PRIMARY KEY, a INT)").expect("v2");

    // Empty staging + column-set drift: the guard should silently drop+
    // recreate the staging so create_reflex_ivm succeeds.
    crate::create_reflex_ivm(
        "setdrift_v2",
        "SELECT id, a FROM setdrift_src",
        Some("id"),
        None,
        Some("DEFERRED"),
        None,
    );

    // The staging must now have the v2 shape (no `extra` column).
    let extra_in_staging = Spi::get_one::<bool>(
        "SELECT EXISTS(SELECT 1 FROM pg_attribute \
         WHERE attrelid = '__reflex_delta_setdrift_src'::regclass \
           AND attname = 'extra' AND NOT attisdropped)",
    )
    .expect("q")
    .expect("v");
    assert!(
        !extra_in_staging,
        "stale staging must have been recreated without the dropped 'extra' column"
    );

    // Trigger must work end-to-end.
    Spi::run("INSERT INTO setdrift_src VALUES (1, 10), (2, 20)")
        .expect("insert seed via initial materialization is not what we want");
    // ^ note: this INSERT goes through the trigger (IMV already exists), so
    // the named-column INSERT in the deferred trigger body must agree with
    // the recreated staging's columns.
    Spi::run("SELECT reflex_flush_deferred('setdrift_src')").expect("flush");
    let fresh = "SELECT id, a FROM setdrift_src";
    assert_imv_correct("setdrift_v2", fresh);
}

/// Mirrors `__reflex_deferred_flush_fn` at COMMIT, including re-queued cascades:
/// loops over DISTINCT pending sources and flushes each, repeating until the
/// queue drains. `reconcile(C)` (fired by the cross-source guard) re-enqueues C
/// via its downstream's staging trigger, whose flush in turn feeds D. The
/// `SELECT DISTINCT` has no `ORDER BY` — matching the orchestrator's
/// nondeterministic source ordering. The 10_000 iteration cap converts a
/// non-converging queue into a loud failure instead of a hung test.
fn drain_deferred_pending() {
    use pgrx::pg_sys::panic::ErrorReportable;
    for _ in 0..10_000 {
        let srcs: Vec<String> = Spi::connect(|client| {
            client
                .select(
                    "SELECT DISTINCT source_table FROM public.__reflex_deferred_pending",
                    None,
                    &[],
                )
                .unwrap_or_report()
                .filter_map(|row| {
                    row.get_by_name::<&str, _>("source_table")
                        .ok()
                        .flatten()
                        .map(|s| s.to_string())
                })
                .collect()
        });
        if srcs.is_empty() {
            return;
        }
        for s in srcs {
            Spi::run(&format!(
                "SELECT reflex_flush_deferred('{}')",
                s.replace('\'', "''")
            ))
            .expect("flush");
        }
    }
    panic!("drain_deferred_pending did not converge after 10000 iterations");
}

/// Cascade correctness — stacked `a,b → casc_c → casc_d`. Mutating BOTH of
/// casc_c's sources in one transaction makes the cross-source guard reconcile
/// casc_c (`TRUNCATE casc_c; INSERT INTO casc_c …`). The TRUNCATE fires casc_c's
/// deferred AFTER TRUNCATE trigger, which propagates a truncate to its dependent
/// casc_d (ordered by graph_depth), emptying casc_d's intermediate BEFORE the
/// reconcile's insert-only casc_c delta is staged. casc_d's additive MERGE then
/// lands on a clean slate, so pre-existing keys are NOT double-counted.
///
/// Regression: an earlier analysis predicted casc_d would additively double-count
/// the insert-only reconcile delta; the AFTER TRUNCATE propagation prevents it.
/// Kept as proof that reconcile-driven cascades stay correct.
#[pg_test]
fn test_deferred_cascade_reconcile_insert_only_delta_oracle() {
    Spi::run("CREATE TABLE t1_a (k INT NOT NULL, amt INT NOT NULL)").expect("create a");
    Spi::run("CREATE TABLE t1_b (k INT NOT NULL, w INT NOT NULL)").expect("create b");
    Spi::run("INSERT INTO t1_a (k, amt) SELECT g, 10 FROM generate_series(1, 100) g")
        .expect("seed a");
    Spi::run("INSERT INTO t1_b (k, w) SELECT g, 5 FROM generate_series(1, 100) g")
        .expect("seed b");

    crate::create_reflex_ivm(
        "t1_casc_c",
        "SELECT t1_a.k, COUNT(*) AS cnt, SUM(t1_a.amt) AS total \
         FROM t1_a JOIN t1_b ON t1_a.k = t1_b.k \
         GROUP BY t1_a.k",
        None,
        None,
        Some("DEFERRED"),
        None,
    );
    crate::create_reflex_ivm(
        "t1_casc_d",
        "SELECT k, SUM(total) AS d_total, SUM(cnt) AS d_cnt \
         FROM t1_casc_c GROUP BY k",
        None,
        None,
        Some("DEFERRED"),
        None,
    );

    let fresh_d = "SELECT cc.k, SUM(cc.total) AS d_total, SUM(cc.cnt) AS d_cnt FROM ( \
                       SELECT t1_a.k AS k, COUNT(*) AS cnt, SUM(t1_a.amt) AS total \
                       FROM t1_a JOIN t1_b ON t1_a.k = t1_b.k GROUP BY t1_a.k \
                   ) cc GROUP BY cc.k";
    assert_imv_correct("t1_casc_d", fresh_d);

    // Mutate BOTH direct sources of casc_c with a brand-new shared key.
    Spi::run("INSERT INTO t1_a (k, amt) VALUES (200, 100)").expect("insert a");
    Spi::run("INSERT INTO t1_b (k, w) VALUES (200, 7)").expect("insert b");

    // Commit-time flush: casc_c reconciles (2 sources pending); its TRUNCATE
    // propagates a truncate to casc_d, then the re-queued insert-only casc_c
    // delta rebuilds casc_d on the now-empty intermediate.
    drain_deferred_pending();

    assert_imv_correct("t1_casc_d", fresh_d);
}

/// Cascade correctness under an adversarial flush order — `a,b → casc_c → casc_d ← e`.
///
/// casc_d's two sources become pending at different times: e from the start,
/// casc_c only after casc_c is reconciled and re-queued. Adversarial order
/// (e, a, b, drain):
///   1. flush e first — casc_c not yet pending, so the guard sees only 1 of
///      casc_d's 2 sources → casc_d goes incremental: Δe ⋈ STALE casc_c (no
///      shared key yet) → no-op for the new key.
///   2. flush a — reconciles casc_c (a,b both pending). casc_c's TRUNCATE fires
///      the AFTER TRUNCATE trigger, which propagates a truncate to casc_d,
///      EMPTYING its intermediate (discarding step 1), then stages the
///      insert-only casc_c delta. Sets the session reconcile marker.
///   3. flush b — casc_c already reconciled (marker) → skipped.
///   4. drain — the re-queued casc_c flush rebuilds casc_d on the now-empty
///      intermediate from casc_c ⋈ live e → correct.
///
/// Regression: predicted to double-count because casc_d goes incremental twice;
/// the AFTER TRUNCATE propagation wipes the first increment so the second lands
/// on an empty intermediate. Kept as proof the adversarial order stays correct.
#[pg_test]
fn test_deferred_cascade_cross_level_ordering_oracle() {
    Spi::run("CREATE TABLE t2_a (k INT NOT NULL, amt INT NOT NULL)").expect("create a");
    Spi::run("CREATE TABLE t2_b (k INT NOT NULL, w INT NOT NULL)").expect("create b");
    Spi::run("CREATE TABLE t2_e (k INT NOT NULL, bonus INT NOT NULL)").expect("create e");
    Spi::run("INSERT INTO t2_a (k, amt) SELECT g, 10 FROM generate_series(1, 100) g")
        .expect("seed a");
    Spi::run("INSERT INTO t2_b (k, w) SELECT g, 5 FROM generate_series(1, 100) g")
        .expect("seed b");
    Spi::run("INSERT INTO t2_e (k, bonus) SELECT g, 3 FROM generate_series(1, 100) g")
        .expect("seed e");

    crate::create_reflex_ivm(
        "t2_casc_c",
        "SELECT t2_a.k, COUNT(*) AS cnt, SUM(t2_a.amt) AS total \
         FROM t2_a JOIN t2_b ON t2_a.k = t2_b.k \
         GROUP BY t2_a.k",
        None,
        None,
        Some("DEFERRED"),
        None,
    );
    crate::create_reflex_ivm(
        "t2_casc_d",
        "SELECT t2_casc_c.k, SUM(t2_casc_c.total) AS d_total, SUM(t2_e.bonus) AS d_bonus \
         FROM t2_casc_c JOIN t2_e ON t2_casc_c.k = t2_e.k \
         GROUP BY t2_casc_c.k",
        None,
        None,
        Some("DEFERRED"),
        None,
    );

    let fresh_d = "SELECT cc.k, SUM(cc.total) AS d_total, SUM(t2_e.bonus) AS d_bonus FROM ( \
                       SELECT t2_a.k AS k, COUNT(*) AS cnt, SUM(t2_a.amt) AS total \
                       FROM t2_a JOIN t2_b ON t2_a.k = t2_b.k GROUP BY t2_a.k \
                   ) cc JOIN t2_e ON cc.k = t2_e.k GROUP BY cc.k";
    assert_imv_correct("t2_casc_d", fresh_d);

    // Mutate all three leaf sources with a brand-new shared key.
    Spi::run("INSERT INTO t2_a (k, amt) VALUES (200, 100)").expect("insert a");
    Spi::run("INSERT INTO t2_b (k, w) VALUES (200, 7)").expect("insert b");
    Spi::run("INSERT INTO t2_e (k, bonus) VALUES (200, 9)").expect("insert e");

    // Adversarial order: e BEFORE a/b so casc_d is evaluated while casc_c is
    // not yet pending. Bypasses the drain helper for the pinned prefix.
    Spi::run("SELECT reflex_flush_deferred('t2_e')").expect("flush e");
    Spi::run("SELECT reflex_flush_deferred('t2_a')").expect("flush a");
    Spi::run("SELECT reflex_flush_deferred('t2_b')").expect("flush b");
    // Drain the re-queued casc_c (→ casc_d incremental again).
    drain_deferred_pending();

    assert_imv_correct("t2_casc_d", fresh_d);
}

/// Cascade correctness under a favorable flush order — same cascade and
/// mutations as test_deferred_cascade_cross_level_ordering_oracle, order
/// (a, b, e, drain). Flushing a,b first reconciles casc_c and re-queues it; the
/// subsequent e flush then sees BOTH of casc_d's sources (casc_c + e) pending,
/// so the cross-source guard reconciles casc_d exactly once (reading live casc_c
/// and live e → correct) and records it in the session marker. The trailing
/// drained casc_c flush finds casc_d already in the marker and skips it.
///
/// Together with the adversarial-order test, shows both flush orders converge to
/// the correct result — guard reconcile here, AFTER TRUNCATE propagation there.
#[pg_test]
fn test_deferred_cascade_favorable_order_control() {
    Spi::run("CREATE TABLE t3_a (k INT NOT NULL, amt INT NOT NULL)").expect("create a");
    Spi::run("CREATE TABLE t3_b (k INT NOT NULL, w INT NOT NULL)").expect("create b");
    Spi::run("CREATE TABLE t3_e (k INT NOT NULL, bonus INT NOT NULL)").expect("create e");
    Spi::run("INSERT INTO t3_a (k, amt) SELECT g, 10 FROM generate_series(1, 100) g")
        .expect("seed a");
    Spi::run("INSERT INTO t3_b (k, w) SELECT g, 5 FROM generate_series(1, 100) g")
        .expect("seed b");
    Spi::run("INSERT INTO t3_e (k, bonus) SELECT g, 3 FROM generate_series(1, 100) g")
        .expect("seed e");

    crate::create_reflex_ivm(
        "t3_casc_c",
        "SELECT t3_a.k, COUNT(*) AS cnt, SUM(t3_a.amt) AS total \
         FROM t3_a JOIN t3_b ON t3_a.k = t3_b.k \
         GROUP BY t3_a.k",
        None,
        None,
        Some("DEFERRED"),
        None,
    );
    crate::create_reflex_ivm(
        "t3_casc_d",
        "SELECT t3_casc_c.k, SUM(t3_casc_c.total) AS d_total, SUM(t3_e.bonus) AS d_bonus \
         FROM t3_casc_c JOIN t3_e ON t3_casc_c.k = t3_e.k \
         GROUP BY t3_casc_c.k",
        None,
        None,
        Some("DEFERRED"),
        None,
    );

    let fresh_d = "SELECT cc.k, SUM(cc.total) AS d_total, SUM(t3_e.bonus) AS d_bonus FROM ( \
                       SELECT t3_a.k AS k, COUNT(*) AS cnt, SUM(t3_a.amt) AS total \
                       FROM t3_a JOIN t3_b ON t3_a.k = t3_b.k GROUP BY t3_a.k \
                   ) cc JOIN t3_e ON cc.k = t3_e.k GROUP BY cc.k";
    assert_imv_correct("t3_casc_d", fresh_d);

    Spi::run("INSERT INTO t3_a (k, amt) VALUES (200, 100)").expect("insert a");
    Spi::run("INSERT INTO t3_b (k, w) VALUES (200, 7)").expect("insert b");
    Spi::run("INSERT INTO t3_e (k, bonus) VALUES (200, 9)").expect("insert e");

    // Favorable order: a,b BEFORE e. casc_c reconciles + re-queues, so when e
    // is flushed casc_d sees both its sources pending → reconciles once.
    Spi::run("SELECT reflex_flush_deferred('t3_a')").expect("flush a");
    Spi::run("SELECT reflex_flush_deferred('t3_b')").expect("flush b");
    Spi::run("SELECT reflex_flush_deferred('t3_e')").expect("flush e");
    // Drain the re-queued casc_c; casc_d is already in the reconcile marker → skipped.
    drain_deferred_pending();

    assert_imv_correct("t3_casc_d", fresh_d);
}

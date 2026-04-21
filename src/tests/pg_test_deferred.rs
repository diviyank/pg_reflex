
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
        None, None, Some("DEFERRED"));

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
        None, None, Some("DEFERRED"));

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
        None, None, Some("DEFERRED"));

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
        None, None, Some("DEFERRED"));

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
        None, None, Some("DEFERRED"));

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
        None, None, Some("DEFERRED"));

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
        None, None, Some("DEFERRED"));

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
        None, None, Some("DEFERRED"));

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
        None, None, Some("DEFERRED"));

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
        None, None, Some("IMMEDIATE"));

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

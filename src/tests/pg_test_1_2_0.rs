// Tests for pg_reflex 1.2.0 Week 1: correctness fixes and operational safety.

/// Bug #10: Creating an IMV that would form a cycle in the dependency graph is rejected.
#[pg_test]
fn test_cycle_detection() {
    Spi::run("CREATE TABLE cyc_t (id INT, val INT)").expect("create table");
    let r = crate::create_reflex_ivm(
        "cyc_a",
        "SELECT id, SUM(val) AS s FROM cyc_t GROUP BY id",
        None,
        None,
        None,
    );
    assert_eq!(r, "CREATE REFLEX INCREMENTAL VIEW", "setup cyc_a: {}", r);
    // Inject a forward dependency edge: cyc_a.depends_on now contains the not-yet-created cyc_b.
    Spi::run(
        "UPDATE public.__reflex_ivm_reference \
         SET depends_on = array_append(depends_on, 'cyc_b') \
         WHERE name = 'cyc_a'",
    )
    .expect("inject cycle edge");
    // Creating cyc_b from cyc_a must detect the cycle and return ERROR.
    let r2 = crate::create_reflex_ivm(
        "cyc_b",
        "SELECT id, SUM(s) AS total FROM cyc_a GROUP BY id",
        None,
        None,
        None,
    );
    assert!(r2.starts_with("ERROR"), "Cycle must be rejected: {}", r2);
    assert!(
        r2.to_lowercase().contains("cycle") || r2.to_lowercase().contains("circular"),
        "Error must mention cycle or circular: {}",
        r2
    );
}

/// Bug #11: Advisory lock uses 2-arg (int4, int4) form — deferred flush must still work correctly.
#[pg_test]
fn test_advisory_lock_two_arg_flush() {
    Spi::run("CREATE TABLE adv_t (id INT, val INT)").expect("create table");
    Spi::run("INSERT INTO adv_t VALUES (1, 10), (2, 20)").expect("seed");
    let r = crate::create_reflex_ivm(
        "adv_v",
        "SELECT id, SUM(val) AS s FROM adv_t GROUP BY id",
        None,
        None,
        Some("DEFERRED"),
    );
    assert_eq!(r, "CREATE REFLEX INCREMENTAL VIEW", "setup: {}", r);
    Spi::run("INSERT INTO adv_t VALUES (1, 5)").expect("insert");
    let flush = Spi::get_one::<String>("SELECT reflex_flush_deferred('adv_t')")
        .expect("flush query")
        .expect("flush result");
    assert!(flush.starts_with("FLUSHED"), "Flush failed: {}", flush);
    assert_imv_correct("adv_v", "SELECT id, SUM(val) AS s FROM adv_t GROUP BY id");
}

/// Bug #4: CTE aliases that collide with pg_reflex reserved prefixes are rejected at create time.
#[pg_test]
fn test_cte_alias_collision_rejected() {
    Spi::run("CREATE TABLE cte_col_t (id INT, val INT)").expect("create table");

    let r1 = crate::create_reflex_ivm(
        "cte_col_v1",
        "WITH __reflex_new_foo AS (SELECT id, val FROM cte_col_t) \
         SELECT id, SUM(val) AS s FROM __reflex_new_foo GROUP BY id",
        None,
        None,
        None,
    );
    assert!(r1.starts_with("ERROR"), "__reflex_new_ alias must be rejected: {}", r1);

    let r2 = crate::create_reflex_ivm(
        "cte_col_v2",
        "WITH __reflex_old_bar AS (SELECT id, val FROM cte_col_t) \
         SELECT id, SUM(val) AS s FROM __reflex_old_bar GROUP BY id",
        None,
        None,
        None,
    );
    assert!(r2.starts_with("ERROR"), "__reflex_old_ alias must be rejected: {}", r2);

    let r3 = crate::create_reflex_ivm(
        "cte_col_v3",
        "WITH __reflex_delta_baz AS (SELECT id, val FROM cte_col_t) \
         SELECT id, SUM(val) AS s FROM __reflex_delta_baz GROUP BY id",
        None,
        None,
        None,
    );
    assert!(r3.starts_with("ERROR"), "__reflex_delta_ alias must be rejected: {}", r3);
}

/// Bug #13: reflex_build_delta_sql must not be STRICT.
/// A passthrough IMV with no aggregations must update correctly on INSERT.
#[pg_test]
fn test_non_strict_delta_sql_passthrough_update() {
    Spi::run("CREATE TABLE nstrict_t (id INT, name TEXT, val NUMERIC)").expect("create table");
    Spi::run("INSERT INTO nstrict_t VALUES (1, 'a', 10), (2, 'b', 20)").expect("seed");
    let r = crate::create_reflex_ivm(
        "nstrict_v",
        "SELECT id, name, val FROM nstrict_t",
        Some("id"),
        None,
        None,
    );
    assert_eq!(r, "CREATE REFLEX INCREMENTAL VIEW", "setup: {}", r);
    Spi::run("INSERT INTO nstrict_t VALUES (3, 'c', 30)").expect("insert");
    let count = Spi::get_one::<i64>("SELECT COUNT(*) FROM nstrict_v")
        .expect("count query")
        .expect("count value");
    assert_eq!(count, 3, "Passthrough IMV must have 3 rows after insert: {}", count);
}

/// Theme 3.1: reflex_rebuild_imv restores correctness after IMV drift.
#[pg_test]
fn test_rebuild_imv() {
    Spi::run("CREATE TABLE rb_t (id INT, grp TEXT, val NUMERIC)").expect("create table");
    Spi::run("INSERT INTO rb_t VALUES (1, 'a', 10), (2, 'a', 20), (3, 'b', 30)").expect("seed");
    let r = crate::create_reflex_ivm(
        "rb_v",
        "SELECT grp, SUM(val) AS total FROM rb_t GROUP BY grp",
        None,
        None,
        None,
    );
    assert_eq!(r, "CREATE REFLEX INCREMENTAL VIEW", "setup: {}", r);
    // Corrupt the IMV to simulate drift.
    Spi::run("TRUNCATE rb_v").expect("truncate to simulate drift");
    let before = Spi::get_one::<i64>("SELECT COUNT(*) FROM rb_v")
        .expect("q")
        .expect("v");
    assert_eq!(before, 0, "should be empty after truncate");
    let result = crate::reflex_rebuild_imv("rb_v");
    assert!(!result.starts_with("ERROR"), "rebuild must succeed: {}", result);
    assert_imv_correct("rb_v", "SELECT grp, SUM(val) AS total FROM rb_t GROUP BY grp");
}

/// Theme 3.2: Dropping a source table removes the IMV from the registry via event trigger.
#[pg_test]
fn test_source_drop_cleans_registry() {
    Spi::run("CREATE TABLE evtdrop_t (id INT, val INT)").expect("create table");
    Spi::run("INSERT INTO evtdrop_t VALUES (1, 10)").expect("seed");
    let r = crate::create_reflex_ivm(
        "evtdrop_v",
        "SELECT id, SUM(val) AS s FROM evtdrop_t GROUP BY id",
        None,
        None,
        None,
    );
    assert_eq!(r, "CREATE REFLEX INCREMENTAL VIEW", "setup: {}", r);
    Spi::run("DROP TABLE evtdrop_t CASCADE").expect("drop source");
    let count = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM public.__reflex_ivm_reference WHERE name = 'evtdrop_v'",
    )
    .expect("q")
    .expect("v");
    assert_eq!(count, 0, "IMV must be removed from registry after source drop");
}

/// Bug #6: GROUP BY on a CAST expression — INSERT+flush must produce correct aggregates
/// (the intermediate column type must match the expression's runtime type so
/// MERGE's `IS NOT DISTINCT FROM` join finds the row).
#[pg_test]
fn test_group_by_cast_expression() {
    Spi::run("CREATE TABLE gbc_t (id INT, order_id INT, amount NUMERIC)").expect("create table");
    Spi::run("INSERT INTO gbc_t VALUES (1, 100, 10), (2, 100, 20), (3, 200, 30)").expect("seed");
    let r = crate::create_reflex_ivm(
        "gbc_v",
        "SELECT CAST(order_id AS TEXT) AS oid, SUM(amount) AS total \
         FROM gbc_t GROUP BY CAST(order_id AS TEXT)",
        None,
        None,
        None,
    );
    assert_eq!(r, "CREATE REFLEX INCREMENTAL VIEW", "setup: {}", r);
    // Insert one more row: existing group '100' must aggregate, not insert-new.
    Spi::run("INSERT INTO gbc_t VALUES (4, 100, 5)").expect("insert");
    let count_100 = Spi::get_one::<i64>("SELECT COUNT(*) FROM gbc_v WHERE oid = '100'")
        .expect("q")
        .expect("v");
    assert_eq!(count_100, 1, "group '100' must have exactly one row after insert");
    assert_imv_correct(
        "gbc_v",
        "SELECT CAST(order_id AS TEXT) AS oid, SUM(amount) AS total \
         FROM gbc_t GROUP BY CAST(order_id AS TEXT)",
    );
}

/// Bug #5: MERGE INSERT on intermediate respects user-added DEFAULT expressions
/// (e.g., DEFAULT now()) rather than emitting a literal.
#[pg_test]
fn test_merge_default_expression_respected() {
    Spi::run("CREATE TABLE md_t (id INT, val NUMERIC)").expect("create table");
    Spi::run("INSERT INTO md_t VALUES (1, 10)").expect("seed");
    let r = crate::create_reflex_ivm(
        "md_v",
        "SELECT id, SUM(val) AS total FROM md_t GROUP BY id",
        None,
        None,
        None,
    );
    assert_eq!(r, "CREATE REFLEX INCREMENTAL VIEW", "setup: {}", r);
    // Add a user column with a DEFAULT expression to the intermediate.
    Spi::run(
        "ALTER TABLE public.__reflex_intermediate_md_v \
         ADD COLUMN created_at TIMESTAMP DEFAULT now()",
    )
    .expect("add col");
    // Trigger a MERGE insert for a brand-new group.
    Spi::run("INSERT INTO md_t VALUES (2, 20)").expect("insert");
    // The new row must have a non-NULL created_at (default resolved, not literal NULL or epoch).
    let ok = Spi::get_one::<bool>(
        "SELECT created_at IS NOT NULL AND created_at > '2000-01-01' \
         FROM public.__reflex_intermediate_md_v WHERE id = 2",
    )
    .expect("q")
    .expect("v");
    assert!(ok, "MERGE INSERT must resolve the DEFAULT expression on user-added columns");
}

/// Theme 3.4: One broken IMV in the deferred cascade must not abort the flush
/// for healthy IMVs. Each IMV body runs inside a PL/pgSQL DO block with
/// `EXCEPTION WHEN OTHERS`, giving us per-IMV subtransaction semantics.
#[pg_test]
fn test_per_imv_savepoint_isolates_failures() {
    Spi::run("CREATE TABLE sp_t (id INT, val INT)").expect("create table");
    Spi::run("INSERT INTO sp_t VALUES (1, 10)").expect("seed");

    let good = crate::create_reflex_ivm(
        "sp_good",
        "SELECT id, SUM(val) AS total FROM sp_t GROUP BY id",
        None,
        None,
        Some("DEFERRED"),
    );
    assert_eq!(good, "CREATE REFLEX INCREMENTAL VIEW", "setup good: {}", good);

    let bad = crate::create_reflex_ivm(
        "sp_bad",
        "SELECT id, SUM(val) AS total FROM sp_t GROUP BY id",
        None,
        None,
        Some("DEFERRED"),
    );
    assert_eq!(bad, "CREATE REFLEX INCREMENTAL VIEW", "setup bad: {}", bad);

    // Break sp_bad by dropping its intermediate table. Any attempt to flush it will error.
    Spi::run("DROP TABLE public.__reflex_intermediate_sp_bad CASCADE").expect("drop bad int");

    // Queue INSERTs for both IMVs (sp_bad has later graph_depth — same depth as sp_good here,
    // but both depend on sp_t, so both are candidates for the deferred flush).
    Spi::run("INSERT INTO sp_t VALUES (2, 20)").expect("queue insert");

    // Flush should not propagate the error — sp_good flushes; sp_bad logs a WARNING.
    let flush = Spi::get_one::<String>("SELECT reflex_flush_deferred('sp_t')")
        .expect("flush query")
        .expect("flush result");
    assert!(
        flush.starts_with("FLUSHED"),
        "Flush must complete despite broken IMV: {}",
        flush
    );
    // sp_good must be correct.
    assert_imv_correct(
        "sp_good",
        "SELECT id, SUM(val) AS total FROM sp_t GROUP BY id",
    );
}

/// Theme 4.2: reflex_ivm_status() returns registry rows with live row_count.
#[pg_test]
fn test_ivm_status_reports_registered_imv() {
    Spi::run("CREATE TABLE ivmst_t (id INT, val INT)").expect("create table");
    Spi::run("INSERT INTO ivmst_t VALUES (1, 10), (2, 20)").expect("seed");
    let r = crate::create_reflex_ivm(
        "ivmst_v",
        "SELECT id, SUM(val) AS total FROM ivmst_t GROUP BY id",
        None,
        None,
        None,
    );
    assert_eq!(r, "CREATE REFLEX INCREMENTAL VIEW", "setup: {}", r);
    let found = Spi::get_one::<i64>(
        "SELECT row_count FROM reflex_ivm_status() WHERE name = 'ivmst_v'",
    )
    .expect("q")
    .expect("v");
    assert_eq!(found, 2, "ivmst_v must report 2 rows from reflex_ivm_status");
}

/// Theme 4.1: DEFERRED flush updates registry timing columns.
#[pg_test]
fn test_flush_records_timing_and_row_count() {
    Spi::run("CREATE TABLE ftim_t (id INT, val INT)").expect("create table");
    Spi::run("INSERT INTO ftim_t VALUES (1, 10)").expect("seed");
    let r = crate::create_reflex_ivm(
        "ftim_v",
        "SELECT id, SUM(val) AS total FROM ftim_t GROUP BY id",
        None,
        None,
        Some("DEFERRED"),
    );
    assert_eq!(r, "CREATE REFLEX INCREMENTAL VIEW", "setup: {}", r);
    Spi::run("INSERT INTO ftim_t VALUES (2, 20)").expect("insert");
    let flush = Spi::get_one::<String>("SELECT reflex_flush_deferred('ftim_t')")
        .expect("flush query")
        .expect("flush result");
    assert!(flush.starts_with("FLUSHED"), "{}", flush);

    let fcount = Spi::get_one::<i64>(
        "SELECT flush_count FROM public.__reflex_ivm_reference WHERE name = 'ftim_v'",
    )
    .expect("q")
    .expect("v");
    assert!(fcount >= 1, "flush_count must be at least 1, got {}", fcount);

    let has_ms = Spi::get_one::<bool>(
        "SELECT last_flush_ms IS NOT NULL \
         FROM public.__reflex_ivm_reference WHERE name = 'ftim_v'",
    )
    .expect("q")
    .expect("v");
    assert!(has_ms, "last_flush_ms must be recorded after flush");
}

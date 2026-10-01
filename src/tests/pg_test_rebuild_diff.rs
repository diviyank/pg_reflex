// Shared helpers + step-0 probes for the rebuild-propagation safeguard.

/// `pg_partition_tree` yields no row for a plain table, so the relation itself
/// is added (a partitioned root holds no tuples and contributes 0).
fn tree_xact_changes(rel: &str) -> i64 {
    Spi::get_one::<i64>(&format!(
        "SELECT COALESCE(sum(pg_stat_get_xact_tuples_inserted(relid) \
                           + pg_stat_get_xact_tuples_updated(relid) \
                           + pg_stat_get_xact_tuples_deleted(relid)), 0)::int8 \
         FROM (SELECT relid FROM pg_partition_tree('{rel}'::regclass) WHERE isleaf \
               UNION SELECT '{rel}'::regclass) leaves"
    ))
    .expect("tree xact stats")
    .unwrap_or(0)
}

/// Highest command id among rows currently in `rel` (take it AFTER the drift).
fn cmin_boundary(rel: &str) -> i64 {
    Spi::get_one::<i64>(&format!(
        "SELECT COALESCE(max(cmin::text::int8), -1) FROM {rel}"
    ))
    .expect("cmin")
    .unwrap_or(-1)
}

/// Rows of `rel` written (inserted / updated / refilled / swapped in) after `boundary`.
fn rows_rewritten_since(rel: &str, boundary: i64) -> i64 {
    Spi::get_one::<i64>(&format!(
        "SELECT count(*)::int8 FROM {rel} WHERE cmin::text::int8 > {boundary}"
    ))
    .expect("rewritten")
    .unwrap_or(0)
}

fn rbd_build_rel(prefix: &str) {
    Spi::run(&format!(
        "CREATE TABLE {prefix}_anchor (id INT PRIMARY KEY, product_id INT NOT NULL, \
         location_id INT NOT NULL, qty INT)"
    ))
    .expect("anchor");
    Spi::run(&format!(
        "CREATE TABLE {prefix}_rel (product_id INT NOT NULL, location_id INT NOT NULL, is_active BOOL)"
    ))
    .expect("rel");
    Spi::run(&format!(
        "INSERT INTO {prefix}_anchor SELECT g, g % 7, g % 5, g FROM generate_series(1, 200) g"
    ))
    .expect("seed anchor");
    Spi::run(&format!(
        "INSERT INTO {prefix}_rel SELECT p, l, CASE WHEN p = 3 THEN NULL ELSE (p + l) % 2 = 0 END \
         FROM generate_series(0, 3) p CROSS JOIN generate_series(0, 4) l"
    ))
    .expect("seed rel");
}

fn rbd_dep_sql(prefix: &str, upstream: &str) -> String {
    format!(
        "SELECT a.id, a.product_id, a.location_id, a.qty, COALESCE(c.is_active, FALSE) AS active \
         FROM {prefix}_anchor a LEFT JOIN {upstream} c \
         ON c.product_id = a.product_id AND c.location_id = a.location_id"
    )
}

/// Step 0 (spec 3.1 #2): DELETE / UPDATE / INSERT statements written through a
/// partitioned IMV root reach a dependent's transition-table triggers with
/// exactly the changed rows (the primitive relies on nothing else).
#[pg_test]
fn pg_rbd_step0_dml_through_partitioned_root_maintains_dependent() {
    Spi::run(
        "CREATE TABLE rbd0_src (plan INT NOT NULL, id INT NOT NULL, qty INT) PARTITION BY LIST (plan)",
    )
    .expect("src");
    for p in [1, 2] {
        Spi::run(&format!(
            "CREATE TABLE rbd0_src_p{p} PARTITION OF rbd0_src FOR VALUES IN ({p})"
        ))
        .expect("part");
    }
    Spi::run(
        "INSERT INTO rbd0_src SELECT p, g, g FROM generate_series(1, 50) g, (VALUES (1),(2)) v(p)",
    )
    .expect("seed");
    create_imv(
        "rbd0_up",
        "SELECT create_reflex_ivm('rbd0_up', 'SELECT plan, id, qty FROM rbd0_src', \
         'plan, id', NULL, 'IMMEDIATE', NULL, ARRAY['plan'])",
    );
    let dep = crate::create_reflex_ivm(
        "rbd0_dep",
        "SELECT plan, SUM(qty) AS q, COUNT(*) AS n FROM rbd0_up GROUP BY plan",
        None,
        None,
        None,
        None,
    );
    assert_eq!(dep, "CREATE REFLEX INCREMENTAL VIEW");

    Spi::run("DELETE FROM rbd0_up WHERE plan = 2 AND id = 1").expect("delete via root");
    Spi::run("UPDATE rbd0_up SET qty = qty + 1 WHERE plan = 2 AND id = 2")
        .expect("update via root");
    Spi::run("INSERT INTO rbd0_up VALUES (2, 1000, 7)").expect("insert via root");

    assert_imv_correct(
        "rbd0_dep",
        "SELECT plan, SUM(qty) AS q, COUNT(*) AS n FROM rbd0_up GROUP BY plan",
    );
}

fn rbd_up_and_dep(prefix: &str, key: Option<&str>, mode: &str) {
    rbd_build_rel(prefix);
    let up = crate::create_reflex_ivm(
        &format!("{prefix}_up"),
        &format!("SELECT product_id, location_id, is_active FROM {prefix}_rel"),
        key,
        None,
        Some(mode),
        None,
    );
    assert_eq!(up, "CREATE REFLEX INCREMENTAL VIEW");
    let dep = crate::create_reflex_ivm(
        &format!("{prefix}_dep"),
        &rbd_dep_sql(prefix, &format!("{prefix}_up")),
        Some("product_id, location_id, id"),
        None,
        Some(mode),
        None,
    );
    assert_eq!(dep, "CREATE REFLEX INCREMENTAL VIEW");
}

fn rbd_rebuild(view: &str, sql: &str) -> String {
    Spi::get_one::<String>(&format!(
        "SELECT reflex_rebuild_target_rows('{view}', $q${sql}$q$)"
    ))
    .expect("rebuild")
    .expect("rebuild result")
}

/// Keyed diff: one drifted upstream row reaches the dependent as that key's rows only.
#[pg_test]
fn pg_rbd_keyed_one_row_drift_is_one_key_downstream() {
    rbd_up_and_dep("rbk1", Some("product_id, location_id"), "IMMEDIATE");
    Spi::run(
        "UPDATE rbk1_up SET is_active = NOT is_active WHERE product_id = 0 AND location_id = 0",
    )
    .expect("drift");
    let before = tree_xact_changes("rbk1_dep");
    assert_eq!(
        rbd_rebuild(
            "rbk1_up",
            "SELECT product_id, location_id, is_active FROM rbk1_rel"
        ),
        "DIFFED"
    );
    let touched = tree_xact_changes("rbk1_dep") - before;
    assert_imv_correct(
        "rbk1_up",
        "SELECT product_id, location_id, is_active FROM rbk1_rel",
    );
    assert_imv_correct("rbk1_dep", &rbd_dep_sql("rbk1", "rbk1_rel"));
    let key_rows = Spi::get_one::<i64>(
        "SELECT count(*)::int8 FROM rbk1_anchor WHERE product_id = 0 AND location_id = 0",
    )
    .unwrap()
    .unwrap();
    assert!(
        touched > 0 && touched <= 2 * key_rows,
        "dependent touched {touched} rows, key has {key_rows}"
    );
}

/// No-op rebuild touches nothing downstream (NULL values included).
#[pg_test]
fn pg_rbd_keyed_noop_rebuild_touches_nothing() {
    rbd_up_and_dep("rbk2", Some("product_id, location_id"), "IMMEDIATE");
    let before = (tree_xact_changes("rbk2_up"), tree_xact_changes("rbk2_dep"));
    assert_eq!(
        rbd_rebuild(
            "rbk2_up",
            "SELECT product_id, location_id, is_active FROM rbk2_rel"
        ),
        "DIFFED"
    );
    assert_eq!(
        (tree_xact_changes("rbk2_up"), tree_xact_changes("rbk2_dep")),
        before
    );
}

/// Vanished key deleted, new key inserted, changed row updated in place.
#[pg_test]
fn pg_rbd_keyed_delete_insert_update() {
    rbd_up_and_dep("rbk3", Some("product_id, location_id"), "IMMEDIATE");
    Spi::run("DELETE FROM rbk3_rel WHERE product_id = 1 AND location_id = 1").expect("vanish");
    Spi::run("INSERT INTO rbk3_rel VALUES (6, 4, TRUE)").expect("new key");
    Spi::run("UPDATE rbk3_rel SET is_active = NOT COALESCE(is_active, FALSE) WHERE product_id = 2 AND location_id = 2")
        .expect("change");
    // rel triggers already maintained rbk3_up; drift it back to the old state to give the rebuild work.
    Spi::run("INSERT INTO rbk3_up VALUES (1, 1, TRUE)").expect("stale row");
    Spi::run("DELETE FROM rbk3_up WHERE product_id = 6").expect("missing row");
    assert_eq!(
        rbd_rebuild(
            "rbk3_up",
            "SELECT product_id, location_id, is_active FROM rbk3_rel"
        ),
        "DIFFED"
    );
    assert_imv_correct(
        "rbk3_up",
        "SELECT product_id, location_id, is_active FROM rbk3_rel",
    );
    assert_imv_correct("rbk3_dep", &rbd_dep_sql("rbk3", "rbk3_rel"));
}

/// Whole-row diff (no key): duplicates incl. NULL matched copy for copy.
#[pg_test]
fn pg_rbd_wholerow_duplicates_exact() {
    Spi::run("CREATE TABLE rbw1_rel (product_id INT, tag TEXT)").expect("rel");
    Spi::run("INSERT INTO rbw1_rel VALUES (1,'a'),(1,'a'),(1,'a'),(2,NULL),(2,NULL),(3,'c')")
        .expect("seed");
    assert_eq!(
        crate::create_reflex_ivm(
            "rbw1_up",
            "SELECT product_id, tag FROM rbw1_rel",
            None,
            None,
            None,
            None
        ),
        "CREATE REFLEX INCREMENTAL VIEW"
    );
    assert_eq!(
        crate::create_reflex_ivm(
            "rbw1_dep",
            "SELECT product_id, COUNT(*) AS n FROM rbw1_up GROUP BY product_id",
            None,
            None,
            None,
            None
        ),
        "CREATE REFLEX INCREMENTAL VIEW"
    );
    Spi::run(
        "DELETE FROM rbw1_up WHERE ctid IN (SELECT ctid FROM rbw1_up WHERE product_id = 1 LIMIT 1)",
    )
    .expect("d1");
    Spi::run(
        "DELETE FROM rbw1_up WHERE ctid IN (SELECT ctid FROM rbw1_up WHERE product_id = 2 LIMIT 1)",
    )
    .expect("d2");
    Spi::run("INSERT INTO rbw1_up VALUES (3,'c')").expect("phantom");
    assert_eq!(
        rbd_rebuild("rbw1_up", "SELECT product_id, tag FROM rbw1_rel"),
        "DIFFED"
    );
    assert_imv_correct("rbw1_up", "SELECT product_id, tag FROM rbw1_rel");
    assert_imv_correct(
        "rbw1_dep",
        "SELECT product_id, COUNT(*) AS n FROM rbw1_rel GROUP BY product_id",
    );
}

/// Whole-row diff detects values to_jsonb would equate: float last digit,
/// json whitespace, array lower bound — even with extra_float_digits = 0.
#[pg_test]
fn pg_rbd_wholerow_detects_subtle_value_changes() {
    let rel_sql = "SELECT id, f, j, a FROM rbw2_rel";
    let rows_text = "string_agg(id || ':' || f::text || j::text || a::text, '|' ORDER BY id)";
    Spi::run("CREATE TABLE rbw2_rel (id INT, f FLOAT8, j JSON, a INT[])").expect("rel");
    Spi::run(
        "INSERT INTO rbw2_rel SELECT id, 0.1::float8 + 0.2::float8, '{\"a\": 1}', '[0:1]={1,2}' FROM generate_series(1, 3) id",
    )
    .expect("seed");
    assert_eq!(
        crate::create_reflex_ivm("rbw2_up", rel_sql, None, None, None, None),
        "CREATE REFLEX INCREMENTAL VIEW"
    );
    assert_eq!(
        crate::create_reflex_ivm(
            "rbw2_dep",
            "SELECT id, f, j::text AS jt, array_lower(a, 1) AS lo FROM rbw2_up",
            None,
            None,
            None,
            None
        ),
        "CREATE REFLEX INCREMENTAL VIEW"
    );
    // Each look-alike drift in its own row, so each must be detected on its own.
    Spi::run("UPDATE rbw2_up SET f = 0.3 WHERE id = 1").expect("float look-alike");
    Spi::run("UPDATE rbw2_up SET j = '{\"a\":1}' WHERE id = 2").expect("json look-alike");
    Spi::run("UPDATE rbw2_up SET a = '{1,2}' WHERE id = 3").expect("array look-alike");
    Spi::run("SET LOCAL extra_float_digits = 0").expect("caller precision");
    assert_eq!(rbd_rebuild("rbw2_up", rel_sql), "DIFFED");
    assert_eq!(
        Spi::get_one::<String>("SHOW extra_float_digits")
            .unwrap()
            .unwrap(),
        "0",
        "caller's extra_float_digits not restored"
    );
    Spi::run("SET LOCAL extra_float_digits = 3").expect("oracle precision");
    let exact = Spi::get_one::<bool>(&format!(
        "SELECT (SELECT {rows_text} FROM rbw2_up) = (SELECT {rows_text} FROM rbw2_rel)"
    ))
    .unwrap()
    .unwrap();
    assert!(exact, "a look-alike value survived the rebuild");
    assert_imv_correct(
        "rbw2_dep",
        "SELECT id, f, j::text AS jt, array_lower(a, 1) AS lo FROM rbw2_rel",
    );
}

/// Fast path: no dependents → 'REPLACED' (DELETE + INSERT), still correct.
#[pg_test]
fn pg_rbd_no_dependents_takes_replace_path() {
    rbd_build_rel("rbf1");
    let up = crate::create_reflex_ivm(
        "rbf1_up",
        "SELECT product_id, location_id, is_active FROM rbf1_rel",
        Some("product_id, location_id"),
        None,
        None,
        None,
    );
    assert_eq!(up, "CREATE REFLEX INCREMENTAL VIEW");
    Spi::run("DELETE FROM rbf1_up WHERE product_id = 1").expect("drift");
    assert_eq!(
        rbd_rebuild(
            "rbf1_up",
            "SELECT product_id, location_id, is_active FROM rbf1_rel"
        ),
        "REPLACED"
    );
    assert_imv_correct(
        "rbf1_up",
        "SELECT product_id, location_id, is_active FROM rbf1_rel",
    );
}

/// Review focus #1: schema-qualified mixed-case IMV name. IMV names are
/// registry names (`schema.Name`, case kept, quoted per part when used in SQL).
/// Pre-existing and out of scope: `create_reflex_ivm` fails for a mixed-case
/// name with a unique key (explicit or PK-inferred) or an IMV source, so the
/// upstream is a grouped aggregate given a unique index on its group columns
/// (to reach the keyed diff) and the dependent is a plain row trigger
/// recording what reaches it.
#[pg_test]
fn pg_rbd_qualified_mixed_case_name() {
    let up_sql = "SELECT product_id, location_id, COUNT(*) AS n FROM rbq.rel GROUP BY product_id, location_id";
    Spi::run("CREATE SCHEMA rbq").expect("schema");
    Spi::run(
        "CREATE TABLE rbq.rel (product_id INT NOT NULL, location_id INT NOT NULL, is_active BOOL)",
    )
    .expect("rel");
    Spi::run("INSERT INTO rbq.rel VALUES (1,1,TRUE),(1,1,NULL),(2,2,FALSE),(3,3,TRUE)")
        .expect("seed");
    assert_eq!(
        crate::create_reflex_ivm("rbq.Up_V", up_sql, None, None, None, None),
        "CREATE REFLEX INCREMENTAL VIEW"
    );
    Spi::run("CREATE UNIQUE INDEX up_v_key ON rbq.\"Up_V\" (product_id, location_id)")
        .expect("key");
    let keyed = Spi::get_one::<bool>(
        "SELECT EXISTS (SELECT 1 FROM pg_index WHERE indrelid = 'rbq.\"Up_V\"'::regclass \
         AND indisunique AND indpred IS NULL AND indexprs IS NULL)",
    )
    .unwrap()
    .unwrap();
    assert!(keyed, "fixture must exercise the keyed diff");
    Spi::run("CREATE TABLE rbq.seen (op TEXT, product_id INT)").expect("seen");
    Spi::run(
        "CREATE FUNCTION rbq.record_seen() RETURNS trigger LANGUAGE plpgsql AS $f$ BEGIN \
           INSERT INTO rbq.seen VALUES (TG_OP, COALESCE(NEW.product_id, OLD.product_id)); RETURN NULL; END $f$",
    ).expect("fn");
    Spi::run(
        "CREATE TRIGGER record_seen AFTER INSERT OR UPDATE OR DELETE ON rbq.\"Up_V\" \
              FOR EACH ROW EXECUTE FUNCTION rbq.record_seen()",
    )
    .expect("trigger");
    Spi::run("DELETE FROM rbq.\"Up_V\" WHERE product_id = 1").expect("drift");
    Spi::run("UPDATE rbq.\"Up_V\" SET n = n + 5 WHERE product_id = 2").expect("drift");
    Spi::run("TRUNCATE rbq.seen").expect("reset");
    assert_eq!(rbd_rebuild("rbq.Up_V", up_sql), "DIFFED");
    assert_imv_correct("rbq.\"Up_V\"", up_sql);
    let seen = Spi::get_one::<String>(
        "SELECT string_agg(op || ':' || product_id, ',' ORDER BY op, product_id) FROM rbq.seen",
    )
    .unwrap();
    assert_eq!(
        seen.as_deref(),
        Some("INSERT:1,UPDATE:2"),
        "dependent must see exactly the changed rows"
    );
}

/// A rebuild query yielding a key twice (IMV already inconsistent) must fail
/// loudly, naming the IMV, instead of applying an arbitrary copy.
#[pg_test]
fn pg_rbd_keyed_duplicate_key_rebuild_raises() {
    rbd_up_and_dep("rbk4", Some("product_id, location_id"), "IMMEDIATE");
    let duplicated = "SELECT product_id, location_id, is_active FROM rbk4_rel \
                      UNION ALL SELECT product_id, location_id, NOT COALESCE(is_active, FALSE) \
                      FROM rbk4_rel WHERE product_id = 0 AND location_id = 0";
    let outcome = Spi::get_one::<String>(&format!(
        "DO $d$ BEGIN PERFORM reflex_rebuild_target_rows('rbk4_up', $q${duplicated}$q$); \
           PERFORM set_config('rbk4.outcome', 'NO ERROR', true); \
         EXCEPTION WHEN OTHERS THEN PERFORM set_config('rbk4.outcome', SQLERRM, true); END $d$; \
         SELECT current_setting('rbk4.outcome')"
    ))
    .expect("outcome")
    .expect("outcome value");
    assert!(
        outcome.contains("'rbk4_up' yields duplicate keys"),
        "duplicate-key rebuild must raise naming the IMV, got: {outcome}"
    );
}

/// An invalid unique index (e.g. a failed CREATE UNIQUE INDEX CONCURRENTLY)
/// guarantees nothing, so it must not be used as the diff key.
#[pg_test]
fn pg_rbd_invalid_unique_index_is_not_a_key() {
    let up_sql = "SELECT product_id, location_id, COUNT(*) AS n FROM rbi_rel GROUP BY product_id, location_id";
    Spi::run("CREATE TABLE rbi_rel (product_id INT NOT NULL, location_id INT NOT NULL)")
        .expect("rel");
    Spi::run("INSERT INTO rbi_rel VALUES (1,1),(1,1),(2,2)").expect("seed");
    assert_eq!(
        crate::create_reflex_ivm("rbi_up", up_sql, None, None, None, None),
        "CREATE REFLEX INCREMENTAL VIEW"
    );
    Spi::run("CREATE UNIQUE INDEX rbi_up_key ON rbi_up (product_id, location_id)").expect("key");
    Spi::run("UPDATE pg_index SET indisvalid = FALSE WHERE indexrelid = 'rbi_up_key'::regclass")
        .expect("invalidate");
    Spi::run("CREATE TABLE rbi_seen (op TEXT, product_id INT)").expect("seen");
    Spi::run(
        "CREATE FUNCTION rbi_record_seen() RETURNS trigger LANGUAGE plpgsql AS $f$ BEGIN \
           INSERT INTO rbi_seen VALUES (TG_OP, COALESCE(NEW.product_id, OLD.product_id)); RETURN NULL; END $f$",
    )
    .expect("fn");
    Spi::run(
        "CREATE TRIGGER rbi_record_seen AFTER INSERT OR UPDATE OR DELETE ON rbi_up \
         FOR EACH ROW EXECUTE FUNCTION rbi_record_seen()",
    )
    .expect("trigger");
    Spi::run("UPDATE rbi_up SET n = n + 5 WHERE product_id = 2").expect("drift");
    Spi::run("TRUNCATE rbi_seen").expect("reset");
    assert_eq!(rbd_rebuild("rbi_up", up_sql), "DIFFED");
    assert_imv_correct("rbi_up", up_sql);
    let seen = Spi::get_one::<String>(
        "SELECT string_agg(op || ':' || product_id, ',' ORDER BY op, product_id) FROM rbi_seen",
    )
    .unwrap();
    assert_eq!(
        seen.as_deref(),
        Some("DELETE:2,INSERT:2"),
        "an invalid unique index was used as the diff key"
    );
}

/// A FULL JOIN IMV with a dependent: a source change takes the full-refresh
/// fallback, which must reach the dependent as a diff, not a rebuild.
#[pg_test]
fn pg_rbd_full_join_fallback_hands_dependent_a_diff() {
    Spi::run("CREATE TABLE rfj_a (k INT PRIMARY KEY, v INT)").expect("a");
    Spi::run("CREATE TABLE rfj_b (k INT PRIMARY KEY, w INT)").expect("b");
    Spi::run("INSERT INTO rfj_a SELECT g, g FROM generate_series(1, 100) g").expect("seed a");
    Spi::run("INSERT INTO rfj_b SELECT g, g FROM generate_series(50, 150) g").expect("seed b");
    let up_sql =
        "SELECT COALESCE(a.k, b.k) AS k, a.v, b.w FROM rfj_a a FULL JOIN rfj_b b ON a.k = b.k";
    assert_eq!(
        crate::create_reflex_ivm("rfj_up", up_sql, Some("k"), None, None, None),
        "CREATE REFLEX INCREMENTAL VIEW"
    );
    assert_eq!(
        crate::create_reflex_ivm(
            "rfj_dep",
            "SELECT k, v, w FROM rfj_up",
            Some("k"),
            None,
            None,
            None
        ),
        "CREATE REFLEX INCREMENTAL VIEW"
    );
    let before = tree_xact_changes("rfj_dep");
    Spi::run("UPDATE rfj_a SET v = v + 1 WHERE k = 10").expect("one change");
    assert_imv_correct("rfj_up", up_sql);
    assert_imv_correct("rfj_dep", up_sql);
    assert!(
        tree_xact_changes("rfj_dep") - before <= 2,
        "dependent of a FULL JOIN IMV rebuilt in full"
    );
}

/// Ungrouped aggregate with a dependent: every statement rebuilds the
/// one-row target; the dependent must see one row change, not a rebuild.
#[pg_test]
fn pg_rbd_ungrouped_aggregate_epilogue_hands_dependent_a_diff() {
    Spi::run("CREATE TABLE rug_src (v INT)").expect("src");
    Spi::run("INSERT INTO rug_src SELECT g FROM generate_series(1, 10) g").expect("seed");
    assert_eq!(
        crate::create_reflex_ivm(
            "rug_up",
            "SELECT SUM(v) AS s, COUNT(*) AS n FROM rug_src",
            None,
            None,
            None,
            None
        ),
        "CREATE REFLEX INCREMENTAL VIEW"
    );
    assert_eq!(
        crate::create_reflex_ivm(
            "rug_dep",
            "SELECT s * 2 AS s2 FROM rug_up",
            None,
            None,
            None,
            None
        ),
        "CREATE REFLEX INCREMENTAL VIEW"
    );
    let before = tree_xact_changes("rug_dep");
    Spi::run("INSERT INTO rug_src VALUES (5)").expect("change");
    assert_imv_correct("rug_up", "SELECT SUM(v) AS s, COUNT(*) AS n FROM rug_src");
    assert_imv_correct("rug_dep", "SELECT SUM(v) * 2 AS s2 FROM rug_src");
    assert!(
        tree_xact_changes("rug_dep") - before <= 2,
        "dependent of an ungrouped aggregate IMV rebuilt in full"
    );
}

/// Deferred mode runs the delta inside a raw PL/pgSQL body (`PERFORM` form of
/// the rebuild call); the FULL JOIN fallback there must still hand the
/// dependent a diff.
#[pg_test]
fn pg_rbd_deferred_full_join_fallback_hands_dependent_a_diff() {
    Spi::run("CREATE TABLE rfd_a (k INT PRIMARY KEY, v INT)").expect("a");
    Spi::run("CREATE TABLE rfd_b (k INT PRIMARY KEY, w INT)").expect("b");
    Spi::run("INSERT INTO rfd_a SELECT g, g FROM generate_series(1, 100) g").expect("seed a");
    Spi::run("INSERT INTO rfd_b SELECT g, g FROM generate_series(50, 150) g").expect("seed b");
    let up_sql =
        "SELECT COALESCE(a.k, b.k) AS k, a.v, b.w FROM rfd_a a FULL JOIN rfd_b b ON a.k = b.k";
    assert_eq!(
        crate::create_reflex_ivm("rfd_up", up_sql, Some("k"), None, Some("DEFERRED"), None),
        "CREATE REFLEX INCREMENTAL VIEW"
    );
    assert_eq!(
        crate::create_reflex_ivm(
            "rfd_dep",
            "SELECT k, v, w FROM rfd_up",
            Some("k"),
            None,
            None,
            None
        ),
        "CREATE REFLEX INCREMENTAL VIEW"
    );
    let before = tree_xact_changes("rfd_dep");
    Spi::run("UPDATE rfd_a SET v = v + 1 WHERE k = 10").expect("one change");
    Spi::run("SELECT reflex_flush_deferred('rfd_a')").expect("flush");
    assert_imv_correct("rfd_up", up_sql);
    assert_imv_correct("rfd_dep", up_sql);
    assert!(
        tree_xact_changes("rfd_dep") - before <= 2,
        "dependent of a deferred FULL JOIN IMV rebuilt in full"
    );
}

// 2026-07-25 — LEFT JOIN aggregate grouped by the secondary's join column,
// where the ON clause equates two columns sharing the SAME bare name (e.g.
// `fa LEFT JOIN fb ON fa.k = fb.k GROUP BY fb.k`).
//
// `parse_join_condition_mappings` (soundness.rs) matches JOIN-equality sides
// by bare column name only, so it conflates `fa.k` (the primary's copy) with
// the GROUP BY column `fb.k` (the secondary's copy) purely because both are
// named `k`. `build_source_join_keys` then records this as a safe
// join-key -> group-column mapping, and `outer_join_secondary_stmts`'s fast
// path (ops.rs) trusts it to scope recompute to the changed secondary keys.
// But NULL-extended primary rows (failed join) live in group NULL, which is
// never in that changed-keys set — so a secondary INSERT that migrates rows
// OUT of the NULL group double-counts them (NULL group never purged), and a
// secondary DELETE that migrates rows INTO the NULL group loses them (NULL
// group never populated).
//
// Fix: `outer_join_secondary_stmts` now checks, before taking the fast path,
// whether any join-key-mapped GROUP BY column is itself secondary-derived
// (via `secondary_ref_identifiers`, already used by the STABLE-column
// fallback a few lines below). If so, it falls through to that fallback,
// which correctly rebuilds migrating groups.

/// Bug repro 1 (INSERT into secondary): silent double-count in the NULL
/// group. Must match a fresh recompute via the bidirectional EXCEPT ALL
/// oracle.
#[pg_test]
fn ljsg_insert_into_secondary_no_double_count() {
    Spi::run("CREATE TABLE ljsg_fa (id INT PRIMARY KEY, k INT)").unwrap();
    Spi::run("CREATE TABLE ljsg_fb (k INT PRIMARY KEY, w INT)").unwrap();
    Spi::run("INSERT INTO ljsg_fa VALUES (1,5), (2,5), (3,7)").unwrap();
    Spi::run("INSERT INTO ljsg_fb VALUES (7,70)").unwrap();
    let sql = "SELECT ljsg_fb.k AS k, COUNT(*) AS n \
               FROM ljsg_fa LEFT JOIN ljsg_fb ON ljsg_fa.k = ljsg_fb.k \
               GROUP BY ljsg_fb.k";
    let res = crate::create_reflex_ivm("ljsg_dup_v", sql, None, None, None, None);
    assert_eq!(res, "CREATE REFLEX INCREMENTAL VIEW");

    // Initial state must already be correct.
    assert_imv_correct("ljsg_dup_v", sql);

    // Migrates fa rows 1,2 from group NULL to group 5. Group NULL must lose
    // them, not just gain group 5.
    Spi::run("INSERT INTO ljsg_fb VALUES (5,50)").unwrap();
    assert_imv_correct("ljsg_dup_v", sql);
}

/// Bug repro 2 (DELETE from secondary): silent row loss — the NULL group
/// that should absorb the orphaned primary rows is never populated.
#[pg_test]
fn ljsg_delete_from_secondary_no_row_loss() {
    Spi::run("CREATE TABLE ljsg_fa2 (id INT PRIMARY KEY, k INT)").unwrap();
    Spi::run("CREATE TABLE ljsg_fb2 (k INT PRIMARY KEY, w INT)").unwrap();
    Spi::run("INSERT INTO ljsg_fa2 VALUES (1,5), (2,5), (3,7)").unwrap();
    Spi::run("INSERT INTO ljsg_fb2 VALUES (7,70)").unwrap();
    let sql = "SELECT ljsg_fb2.k AS k, COUNT(*) AS n \
               FROM ljsg_fa2 LEFT JOIN ljsg_fb2 ON ljsg_fa2.k = ljsg_fb2.k \
               GROUP BY ljsg_fb2.k";
    let res = crate::create_reflex_ivm("ljsg_del_v", sql, None, None, None, None);
    assert_eq!(res, "CREATE REFLEX INCREMENTAL VIEW");
    assert_imv_correct("ljsg_del_v", sql);

    // Migrates fa row 3 from group 7 to group NULL. Group NULL must gain it.
    Spi::run("DELETE FROM ljsg_fb2 WHERE k = 7").unwrap();
    assert_imv_correct("ljsg_del_v", sql);
}

/// Control (verified-safe boundary from the field report): identical shape
/// but the join columns have DIFFERENT bare names, so
/// `parse_join_condition_mappings` never conflates them and
/// `source_join_keys` stays empty for this secondary — the buggy fast path
/// never arms. Must stay correct both before and after the fix.
#[pg_test]
fn ljsg_control_different_bare_names_stays_correct() {
    Spi::run("CREATE TABLE ljsg_ga (id INT PRIMARY KEY, gk INT)").unwrap();
    Spi::run("CREATE TABLE ljsg_gb (other_k INT PRIMARY KEY, w INT)").unwrap();
    Spi::run("INSERT INTO ljsg_ga VALUES (1,5), (2,5), (3,7)").unwrap();
    Spi::run("INSERT INTO ljsg_gb VALUES (7,70)").unwrap();
    let sql = "SELECT ljsg_gb.other_k AS k, COUNT(*) AS n \
               FROM ljsg_ga LEFT JOIN ljsg_gb ON ljsg_ga.gk = ljsg_gb.other_k \
               GROUP BY ljsg_gb.other_k";
    let res = crate::create_reflex_ivm("ljsg_ctrl_v", sql, None, None, None, None);
    assert_eq!(res, "CREATE REFLEX INCREMENTAL VIEW");
    assert_imv_correct("ljsg_ctrl_v", sql);

    Spi::run("INSERT INTO ljsg_gb VALUES (5,50)").unwrap();
    assert_imv_correct("ljsg_ctrl_v", sql);

    Spi::run("DELETE FROM ljsg_gb WHERE other_k = 5").unwrap();
    assert_imv_correct("ljsg_ctrl_v", sql);
}

/// Sound shape (must keep the 1.4.6 performance win): GROUP BY is on the
/// PRIMARY's copy of the join column (`fa.k`), not the secondary's. This
/// column can never migrate (it belongs to the row that's always present),
/// so `outer_join_secondary_stmts` must still take the join-key-scoped fast
/// path — asserted by the ABSENCE of a full TRUNCATE/rebuild and PRESENCE of
/// the join-key membership predicate in the generated delta SQL for a
/// secondary mutation.
#[pg_test]
fn ljsg_primary_side_group_key_keeps_fast_path() {
    Spi::run("CREATE TABLE ljsg_fa3 (id INT PRIMARY KEY, k INT)").unwrap();
    Spi::run("CREATE TABLE ljsg_fb3 (k INT PRIMARY KEY, w INT)").unwrap();
    Spi::run("INSERT INTO ljsg_fa3 VALUES (1,5), (2,5), (3,7)").unwrap();
    Spi::run("INSERT INTO ljsg_fb3 VALUES (7,70)").unwrap();
    let sql = "SELECT ljsg_fa3.k AS k, COUNT(*) AS n \
               FROM ljsg_fa3 LEFT JOIN ljsg_fb3 ON ljsg_fa3.k = ljsg_fb3.k \
               GROUP BY ljsg_fa3.k";
    let res = crate::create_reflex_ivm("ljsg_prim_v", sql, None, None, None, None);
    assert_eq!(res, "CREATE REFLEX INCREMENTAL VIEW");
    assert_imv_correct("ljsg_prim_v", sql);

    Spi::run("INSERT INTO ljsg_fb3 VALUES (5,50)").unwrap();
    assert_imv_correct("ljsg_prim_v", sql);

    // Inspect the delta SQL the codegen produces for a secondary INSERT.
    let gen_sql: String = Spi::get_one(
        "SELECT public.reflex_build_delta_sql( \
             'ljsg_prim_v', 'ljsg_fb3', 'INSERT', base_query, end_query, \
             aggregations::TEXT, base_query) \
         FROM public.__reflex_ivm_reference WHERE name = 'ljsg_prim_v'",
    )
    .expect("build sql")
    .expect("non-empty");

    assert!(
        gen_sql.contains("(\"k\") IN (SELECT") || gen_sql.contains("\"k\" IN (SELECT"),
        "primary-side GROUP BY must keep the join-key-scoped fast path: {}",
        gen_sql
    );
    assert!(
        !gen_sql.to_uppercase().contains("TRUNCATE"),
        "primary-side GROUP BY must not fall back to a full rebuild: {}",
        gen_sql
    );
}

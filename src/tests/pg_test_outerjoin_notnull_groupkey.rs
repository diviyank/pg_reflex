// 2026-07-25 — Step-0 repro: aggregate `plan.not_null_columns` is built from
// `pg_attribute.attnotnull` across ALL sources keyed on the BARE column name
// (`query_column_types_from_catalog_with_per_source`, soundness.rs), with no
// awareness of which source sits on the NULLABLE side of an OUTER JOIN.
// `optimize_not_null_sums` then overwrites `plan.not_null_columns` with that
// catalog-only set, and the outer-join-aware prover `infer_not_null_columns`
// only ever ADDS to it.
//
// Consequence under test: a LEFT JOIN aggregate grouped by a column that is
// catalog-NOT-NULL in its own table but reached through the nullable side of
// the join is wrongly marked NOT NULL, so `affected_null_key_gate` returns
// None and the target sync matches groups with a bare `=` — which never
// matches the NULL group that the LEFT JOIN genuinely produces.

/// Repro A — INSERT an unmatched PRIMARY row: the NULL group's count must grow.
#[pg_test]
fn ojnn_primary_insert_unmatched_updates_null_group() {
    Spi::run("CREATE TABLE ojnn_fa (id INT PRIMARY KEY, k INT)").unwrap();
    Spi::run("CREATE TABLE ojnn_fb (k INT PRIMARY KEY, w INT)").unwrap();
    Spi::run("INSERT INTO ojnn_fa VALUES (1,5), (2,5), (3,7)").unwrap();
    Spi::run("INSERT INTO ojnn_fb VALUES (7,70)").unwrap();
    let sql = "SELECT ojnn_fb.k AS k, COUNT(*) AS n \
               FROM ojnn_fa LEFT JOIN ojnn_fb ON ojnn_fa.k = ojnn_fb.k \
               GROUP BY ojnn_fb.k";
    let res = crate::create_reflex_ivm("ojnn_ins_v", sql, None, None, None, None);
    assert_eq!(res, "CREATE REFLEX INCREMENTAL VIEW");
    let before = Spi::get_one::<String>(
        "SELECT string_agg(COALESCE(k::text,'NULL')||'='||n::text, ',' ORDER BY k NULLS FIRST) FROM ojnn_ins_v",
    ).unwrap().unwrap_or_default();
    assert_eq!(before, "NULL=2,7=1", "initial materialization wrong");

    Spi::run("INSERT INTO ojnn_fa VALUES (4,9)").unwrap();
    let after = Spi::get_one::<String>(
        "SELECT string_agg(COALESCE(k::text,'NULL')||'='||n::text, ',' ORDER BY k NULLS FIRST) FROM ojnn_ins_v",
    ).unwrap().unwrap_or_default();
    assert_eq!(after, "NULL=3,7=1", "after unmatched-primary INSERT");
    assert_imv_correct("ojnn_ins_v", sql);
}

/// Repro B — DELETE an unmatched PRIMARY row: the NULL group's count must shrink.
#[pg_test]
fn ojnn_primary_delete_unmatched_updates_null_group() {
    Spi::run("CREATE TABLE ojnn_fa2 (id INT PRIMARY KEY, k INT)").unwrap();
    Spi::run("CREATE TABLE ojnn_fb2 (k INT PRIMARY KEY, w INT)").unwrap();
    Spi::run("INSERT INTO ojnn_fa2 VALUES (1,5), (2,5), (3,7)").unwrap();
    Spi::run("INSERT INTO ojnn_fb2 VALUES (7,70)").unwrap();
    let sql = "SELECT ojnn_fb2.k AS k, COUNT(*) AS n \
               FROM ojnn_fa2 LEFT JOIN ojnn_fb2 ON ojnn_fa2.k = ojnn_fb2.k \
               GROUP BY ojnn_fb2.k";
    let res = crate::create_reflex_ivm("ojnn_del_v", sql, None, None, None, None);
    assert_eq!(res, "CREATE REFLEX INCREMENTAL VIEW");
    assert_imv_correct("ojnn_del_v", sql);

    Spi::run("DELETE FROM ojnn_fa2 WHERE id = 1").unwrap();
    assert_imv_correct("ojnn_del_v", sql);
}

/// Repro C — same shape, but the PRIMARY table's same-named column is itself
/// catalog-NOT-NULL. Excluding only the outer-join nullable side is NOT enough
/// here: the bare-name-keyed set still gets `k` from the primary.
#[pg_test]
fn ojnn_bare_name_collision_primary_not_null() {
    Spi::run("CREATE TABLE ojnn_fa4 (id INT PRIMARY KEY, k INT NOT NULL)").unwrap();
    Spi::run("CREATE TABLE ojnn_fb4 (k INT PRIMARY KEY, w INT)").unwrap();
    Spi::run("INSERT INTO ojnn_fa4 VALUES (1,5), (2,5), (3,7)").unwrap();
    Spi::run("INSERT INTO ojnn_fb4 VALUES (7,70)").unwrap();
    let sql = "SELECT ojnn_fb4.k AS k, COUNT(*) AS n \
               FROM ojnn_fa4 LEFT JOIN ojnn_fb4 ON ojnn_fa4.k = ojnn_fb4.k \
               GROUP BY ojnn_fb4.k";
    let res = crate::create_reflex_ivm("ojnn_col_v", sql, None, None, None, None);
    assert_eq!(res, "CREATE REFLEX INCREMENTAL VIEW");
    assert_imv_correct("ojnn_col_v", sql);

    Spi::run("INSERT INTO ojnn_fa4 VALUES (4,9)").unwrap();
    assert_imv_correct("ojnn_col_v", sql);
}

/// Repro D — RIGHT JOIN, the mirror image: the nullable side is the *primary*.
#[pg_test]
fn ojnn_right_join_nullable_primary() {
    Spi::run("CREATE TABLE ojnn_ra (rk INT PRIMARY KEY, w INT)").unwrap();
    Spi::run("CREATE TABLE ojnn_rb (id INT PRIMARY KEY, rk INT)").unwrap();
    Spi::run("INSERT INTO ojnn_ra VALUES (7,70)").unwrap();
    Spi::run("INSERT INTO ojnn_rb VALUES (1,5), (2,5), (3,7)").unwrap();
    let sql = "SELECT ojnn_ra.rk AS rk, COUNT(*) AS n \
               FROM ojnn_ra RIGHT JOIN ojnn_rb ON ojnn_ra.rk = ojnn_rb.rk \
               GROUP BY ojnn_ra.rk";
    let res = crate::create_reflex_ivm("ojnn_rj_v", sql, None, None, None, None);
    assert_eq!(res, "CREATE REFLEX INCREMENTAL VIEW");
    assert_imv_correct("ojnn_rj_v", sql);

    Spi::run("INSERT INTO ojnn_rb VALUES (4,9)").unwrap();
    assert_imv_correct("ojnn_rj_v", sql);
}

/// Repro F — RIGHT JOIN where BOTH sides' join columns are catalog NOT NULL,
/// so the per-source unanimity rule cannot help. Only the RIGHT/FULL bail-out
/// (the flat `analysis.joins` list cannot say which side is nullable) stops
/// `ojnn_ga.gk` from being promoted, though the RIGHT JOIN's unmatched
/// secondary rows put it squarely in a NULL group.
#[pg_test]
fn ojnn_right_join_both_sides_not_null() {
    Spi::run("CREATE TABLE ojnn_ga (gk INT PRIMARY KEY, w INT)").unwrap();
    Spi::run("CREATE TABLE ojnn_gb (id INT PRIMARY KEY, gk INT NOT NULL)").unwrap();
    Spi::run("INSERT INTO ojnn_ga VALUES (7,70)").unwrap();
    Spi::run("INSERT INTO ojnn_gb VALUES (1,5), (2,5), (3,7)").unwrap();
    let sql = "SELECT ojnn_ga.gk AS gk, COUNT(*) AS n \
               FROM ojnn_ga RIGHT JOIN ojnn_gb ON ojnn_ga.gk = ojnn_gb.gk \
               GROUP BY ojnn_ga.gk";
    let res = crate::create_reflex_ivm("ojnn_rj2_v", sql, None, None, None, None);
    assert_eq!(res, "CREATE REFLEX INCREMENTAL VIEW");
    assert_imv_correct("ojnn_rj2_v", sql);

    Spi::run("INSERT INTO ojnn_gb VALUES (4,9)").unwrap();
    assert_imv_correct("ojnn_rj2_v", sql);
}

/// Repro E — no outer join at all: the bare-name union alone over-promotes.
/// `ojnn_ea.k` is NOT NULL, `ojnn_eb.k` is nullable, and the IMV groups by
/// `ojnn_eb.k` — which really does produce a NULL group.
#[pg_test]
fn ojnn_inner_join_bare_name_union_over_promotes() {
    Spi::run("CREATE TABLE ojnn_ea (id INT PRIMARY KEY, j INT, k INT NOT NULL)").unwrap();
    Spi::run("CREATE TABLE ojnn_eb (id INT PRIMARY KEY, j INT, k INT)").unwrap();
    Spi::run("INSERT INTO ojnn_ea VALUES (1,10,1), (2,20,2)").unwrap();
    Spi::run("INSERT INTO ojnn_eb VALUES (1,10,NULL), (2,20,7)").unwrap();
    let sql = "SELECT ojnn_eb.k AS k, COUNT(*) AS n \
               FROM ojnn_ea JOIN ojnn_eb ON ojnn_ea.j = ojnn_eb.j \
               GROUP BY ojnn_eb.k";
    let res = crate::create_reflex_ivm("ojnn_inner_v", sql, None, None, None, None);
    assert_eq!(res, "CREATE REFLEX INCREMENTAL VIEW");
    assert_imv_correct("ojnn_inner_v", sql);

    // Another row joining the NULL-k secondary row: the NULL group must grow.
    Spi::run("INSERT INTO ojnn_ea VALUES (3,10,3)").unwrap();
    assert_imv_correct("ojnn_inner_v", sql);
}

/// Mechanism (not just symptom): the outer-join nullable side's catalog
/// NOT NULL column must not reach `plan.not_null_columns`, and the target-sync
/// codegen must therefore still emit `affected_null_key_gate`'s NULL-safe
/// alternative for that key.
#[pg_test]
fn ojnn_outer_join_key_absent_from_not_null_columns() {
    Spi::run("CREATE TABLE ojnn_fa3 (id INT PRIMARY KEY, k INT)").unwrap();
    Spi::run("CREATE TABLE ojnn_fb3 (k INT PRIMARY KEY, w INT)").unwrap();
    Spi::run("INSERT INTO ojnn_fa3 VALUES (1,5), (2,5), (3,7)").unwrap();
    Spi::run("INSERT INTO ojnn_fb3 VALUES (7,70)").unwrap();
    let sql = "SELECT ojnn_fb3.k AS k, COUNT(*) AS n \
               FROM ojnn_fa3 LEFT JOIN ojnn_fb3 ON ojnn_fa3.k = ojnn_fb3.k \
               GROUP BY ojnn_fb3.k";
    let res = crate::create_reflex_ivm("ojnn_diag_v", sql, None, None, None, None);
    assert_eq!(res, "CREATE REFLEX INCREMENTAL VIEW");

    let nn: String = Spi::get_one(
        "SELECT COALESCE((aggregations::jsonb)->>'not_null_columns', '<none>') \
         FROM public.__reflex_ivm_reference WHERE name = 'ojnn_diag_v'",
    )
    .expect("registry read")
    .expect("non-null");

    let gen_sql: String = Spi::get_one(
        "SELECT public.reflex_build_delta_sql( \
             'ojnn_diag_v', 'ojnn_fa3', 'INSERT', base_query, end_query, \
             aggregations::TEXT, base_query) \
         FROM public.__reflex_ivm_reference WHERE name = 'ojnn_diag_v'",
    )
    .expect("build sql")
    .expect("non-empty");

    assert!(
        !nn.contains("\"k\""),
        "`k` is NULL for every unmatched primary row, so it must not be recorded \
         NOT NULL just because ojnn_fb3.k is a PRIMARY KEY: {}",
        nn
    );
    // `id` is NOT NULL on the primary (non-nullable) side and must survive — the
    // reduction has to be surgical, not a blanket wipe.
    assert!(
        nn.contains("\"id\""),
        "non-nullable-side NOT NULL columns must be kept: {}",
        nn
    );
    assert!(
        gen_sql.contains("__ng.\"k\" IS NULL"),
        "target sync must keep the NULL-safe gated alternative for `k`: {}",
        gen_sql
    );
    assert!(
        gen_sql.contains("\"ojnn_diag_v\".\"k\" IS NOT DISTINCT FROM __a.\"k\""),
        "target sync must have a NULL-safe branch matching the NULL group: {}",
        gen_sql
    );
}

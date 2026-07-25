// 2026-07-25 — a GROUP BY column written as a 3-part schema-qualified
// reference (`public.fb.k`) on the SECONDARY side of an outer join was
// classified STABLE, arming a scoped recompute that never rebuilds the groups
// a secondary mutation migrates.
//
// `join_key_scope_is_sound` and the STABLE-column fallback in
// `outer_join_secondary_stmts` both took the qualifier via `split_once('.')`,
// which yields the SCHEMA (`public`) for a 3-part reference. No real table is
// ever named like the schema, so the column never matched
// `secondary_ref_identifiers` and was always treated as stable — the exact
// premise the LEFT-JOIN-groupby fix relies on to refuse the fast path.
//
// The qualifier is now the segment immediately preceding the column, taken
// over every identifier chain in the expression (so an expression group key
// over the secondary is recognised too), and any expression with no
// identifier-looking qualifier stays non-stable as before.

/// Bug repro 1 (INSERT into secondary, join-key fast path): the delta scopes
/// to the changed join key and never purges the NULL group, double-counting
/// the primary rows that migrate out of it.
#[pg_test]
fn qgb_three_part_group_key_insert_no_double_count() {
    Spi::run("CREATE TABLE qgb_fa (id INT PRIMARY KEY, k INT)").unwrap();
    Spi::run("CREATE TABLE qgb_fb (k INT PRIMARY KEY, w INT)").unwrap();
    Spi::run("INSERT INTO qgb_fa VALUES (1,5), (2,5), (3,7)").unwrap();
    Spi::run("INSERT INTO qgb_fb VALUES (7,70)").unwrap();
    let sql = "SELECT public.qgb_fb.k AS k, COUNT(*) AS n \
               FROM qgb_fa LEFT JOIN qgb_fb ON qgb_fa.k = qgb_fb.k \
               GROUP BY public.qgb_fb.k";
    let res = crate::create_reflex_ivm("qgb_ins_v", sql, None, None, None, None);
    assert_eq!(res, "CREATE REFLEX INCREMENTAL VIEW");
    assert_imv_correct("qgb_ins_v", sql);

    // The 3-part form must actually reach the plan — otherwise this test
    // pins nothing.
    let aggs: String = Spi::get_one(
        "SELECT aggregations::TEXT FROM public.__reflex_ivm_reference WHERE name = 'qgb_ins_v'",
    )
    .expect("aggs")
    .expect("non-empty");
    assert!(
        aggs.contains("public.qgb_fb.k"),
        "3-part GROUP BY reference must survive into the plan: {}",
        aggs
    );

    // Migrates fa rows 1,2 from group NULL to group 5.
    Spi::run("INSERT INTO qgb_fb VALUES (5,50)").unwrap();
    assert_imv_correct("qgb_ins_v", sql);
}

/// Bug repro 2 (DELETE from secondary, join-key fast path): the NULL group
/// that must absorb the orphaned primary rows is never populated.
#[pg_test]
fn qgb_three_part_group_key_delete_no_row_loss() {
    Spi::run("CREATE TABLE qgb_ga (id INT PRIMARY KEY, k INT)").unwrap();
    Spi::run("CREATE TABLE qgb_gb (k INT PRIMARY KEY, w INT)").unwrap();
    Spi::run("INSERT INTO qgb_ga VALUES (1,5), (2,5), (3,7)").unwrap();
    Spi::run("INSERT INTO qgb_gb VALUES (5,50), (7,70)").unwrap();
    let sql = "SELECT public.qgb_gb.k AS k, COUNT(*) AS n \
               FROM qgb_ga LEFT JOIN qgb_gb ON qgb_ga.k = qgb_gb.k \
               GROUP BY public.qgb_gb.k";
    let res = crate::create_reflex_ivm("qgb_del_v", sql, None, None, None, None);
    assert_eq!(res, "CREATE REFLEX INCREMENTAL VIEW");
    assert_imv_correct("qgb_del_v", sql);

    // Migrates fa row 3 from group 7 to group NULL.
    Spi::run("DELETE FROM qgb_gb WHERE k = 7").unwrap();
    assert_imv_correct("qgb_del_v", sql);
}

/// Bug repro 3 (STABLE-column fallback, expression group key): the same
/// first-dot-split defect through the other call site — a group key that is an
/// EXPRESSION over the secondary (`DATE_TRUNC('month', fb.ts)`) yielded the
/// junk qualifier `date_trunc('month', fb`, never matched the secondary, and
/// was scoped on even though its value migrates NULL<->value.
#[pg_test]
fn qgb_expression_group_key_over_secondary_not_scoped() {
    Spi::run("CREATE TABLE qgb_ea (id INT PRIMARY KEY, g INT, k INT)").unwrap();
    Spi::run("CREATE TABLE qgb_eb (id INT PRIMARY KEY, k INT, ts TIMESTAMP)").unwrap();
    // Every primary row shares the same join key, so the secondary INSERT
    // below migrates ALL of them out of the NULL month group at once — the
    // affected-group set computed from the transition table then contains only
    // the NEW month, and a scope that includes the (migrating) month key never
    // purges the stale NULL group.
    Spi::run("INSERT INTO qgb_ea VALUES (1,1,5), (2,1,5), (3,1,5)").unwrap();
    Spi::run("INSERT INTO qgb_eb VALUES (1,9,'2024-03-02')").unwrap();
    let sql = "SELECT qgb_ea.g AS g, DATE_TRUNC('month', qgb_eb.ts) AS m, COUNT(*) AS n \
               FROM qgb_ea LEFT JOIN qgb_eb ON qgb_ea.k = qgb_eb.k \
               GROUP BY qgb_ea.g, DATE_TRUNC('month', qgb_eb.ts)";
    let res = crate::create_reflex_ivm("qgb_expr_v", sql, None, None, None, None);
    assert_eq!(res, "CREATE REFLEX INCREMENTAL VIEW");
    assert_imv_correct("qgb_expr_v", sql);

    // Migrates ea rows 1,2 out of the NULL month group.
    Spi::run("INSERT INTO qgb_eb VALUES (2,5,'2024-04-11')").unwrap();
    assert_imv_correct("qgb_expr_v", sql);

    // ...and back into it.
    Spi::run("DELETE FROM qgb_eb WHERE k = 5").unwrap();
    assert_imv_correct("qgb_expr_v", sql);
}

/// Control (must keep the 1.4.6 performance win): a 3-part qualified GROUP BY
/// on the PRIMARY side is genuinely stable — it can never migrate — so the
/// join-key-scoped fast path must still arm. Pins that the qualifier fix
/// narrows classification only for the mutated table, not for every qualified
/// reference.
#[pg_test]
fn qgb_three_part_primary_group_key_keeps_fast_path() {
    Spi::run("CREATE TABLE qgb_pa (id INT PRIMARY KEY, k INT)").unwrap();
    Spi::run("CREATE TABLE qgb_pb (k INT PRIMARY KEY, w INT)").unwrap();
    Spi::run("INSERT INTO qgb_pa VALUES (1,5), (2,5), (3,7)").unwrap();
    Spi::run("INSERT INTO qgb_pb VALUES (7,70)").unwrap();
    let sql = "SELECT public.qgb_pa.k AS k, COUNT(*) AS n \
               FROM qgb_pa LEFT JOIN qgb_pb ON qgb_pa.k = qgb_pb.k \
               GROUP BY public.qgb_pa.k";
    let res = crate::create_reflex_ivm("qgb_prim_v", sql, None, None, None, None);
    assert_eq!(res, "CREATE REFLEX INCREMENTAL VIEW");
    assert_imv_correct("qgb_prim_v", sql);

    Spi::run("INSERT INTO qgb_pb VALUES (5,50)").unwrap();
    assert_imv_correct("qgb_prim_v", sql);

    let gen_sql: String = Spi::get_one(
        "SELECT public.reflex_build_delta_sql( \
             'qgb_prim_v', 'qgb_pb', 'INSERT', base_query, end_query, \
             aggregations::TEXT, base_query) \
         FROM public.__reflex_ivm_reference WHERE name = 'qgb_prim_v'",
    )
    .expect("build sql")
    .expect("non-empty");

    assert!(
        gen_sql.contains("(\"k\") IN (SELECT") || gen_sql.contains("\"k\" IN (SELECT"),
        "primary-side 3-part GROUP BY must keep the join-key-scoped fast path: {}",
        gen_sql
    );
    assert!(
        !gen_sql.to_uppercase().contains("TRUNCATE"),
        "primary-side 3-part GROUP BY must not fall back to a full rebuild: {}",
        gen_sql
    );
}

// 2026-11-06 — field bug (db_dev + db_prod, every tenant's
// stock_transfer_baseline_view): LEFT JOIN aggregate whose join-key GROUP BY
// column is ALIASED in the projection (`stb.to_location_id AS location_id`).
//
// `outer_join_secondary_stmts`'s join-key-scoped fast path (ops.rs) builds one
// membership predicate from the INTERMEDIATE column names and reuses it for the
// `DELETE FROM <target>` too. The target stores the alias, so a write to the
// secondary raised `column "to_location_id" does not exist` — under DEFERRED the
// flush was swallowed, the IMV marked known_stale and the delta discarded
// (silent `in_current_assortment` drift on prod).

const AGS_SQL: &str = "SELECT ags_stb.sp, ags_stb.prod, ags_stb.to_loc AS loc, \
     SUM(ags_stb.qty) AS qty, \
     COALESCE(BOOL_OR(ags_caav.active), FALSE) AS in_cur \
     FROM ags_stb \
     LEFT JOIN ags_caav ON ags_caav.prod = ags_stb.prod AND ags_caav.loc = ags_stb.to_loc \
     GROUP BY ags_stb.sp, ags_stb.prod, ags_stb.to_loc";

fn ags_fixture(suffix: &str) -> String {
    Spi::run(&format!(
        "CREATE TABLE ags_stb{s} (id INT PRIMARY KEY, sp INT NOT NULL, prod INT NOT NULL, \
         to_loc INT NOT NULL, qty INT)",
        s = suffix
    ))
    .unwrap();
    Spi::run(&format!(
        "CREATE TABLE ags_caav{s} (prod INT, loc INT, active BOOL, PRIMARY KEY (prod, loc))",
        s = suffix
    ))
    .unwrap();
    Spi::run(&format!(
        "INSERT INTO ags_stb{s} VALUES (1,1,10,100,5),(2,1,10,100,3),(3,1,11,100,7),(4,2,10,101,2)",
        s = suffix
    ))
    .unwrap();
    Spi::run(&format!(
        "INSERT INTO ags_caav{s} VALUES (10,100,TRUE),(11,100,FALSE)",
        s = suffix
    ))
    .unwrap();
    AGS_SQL
        .replace("ags_stb", &format!("ags_stb{}", suffix))
        .replace("ags_caav", &format!("ags_caav{}", suffix))
}

fn ags_assert_fast_path(imv: &str, secondary: &str) {
    let gen_sql: String = Spi::get_one(&format!(
        "SELECT public.reflex_build_delta_sql('{imv}', '{secondary}', 'UPDATE', \
             base_query, end_query, aggregations::TEXT, base_query) \
         FROM public.__reflex_ivm_reference WHERE name = '{imv}'"
    ))
    .expect("build sql")
    .expect("non-empty");
    assert!(
        gen_sql.contains(") IN (SELECT"),
        "secondary write must take the join-key-scoped path: {gen_sql}"
    );
    assert!(
        !gen_sql.to_uppercase().contains("TRUNCATE"),
        "secondary write must not fall back to a full rebuild: {gen_sql}"
    );
}

fn ags_mutate_secondary(suffix: &str, after_each: &dyn Fn()) {
    Spi::run(&format!(
        "UPDATE ags_caav{s} SET active = NOT active WHERE prod = 10 AND loc = 100",
        s = suffix
    ))
    .unwrap();
    after_each();
    Spi::run(&format!(
        "INSERT INTO ags_caav{s} VALUES (10,101,TRUE)",
        s = suffix
    ))
    .unwrap();
    after_each();
    Spi::run(&format!(
        "DELETE FROM ags_caav{s} WHERE prod = 11 AND loc = 100",
        s = suffix
    ))
    .unwrap();
    after_each();
}

/// IMMEDIATE: the target DELETE must address the aliased column, so every
/// secondary write keeps the IMV equal to a fresh recompute.
#[pg_test]
fn ags_immediate_aliased_join_key_group_col() {
    let sql = ags_fixture("_i");
    let res = crate::create_reflex_ivm("ags_i_v", &sql, None, None, None, None);
    assert_eq!(res, "CREATE REFLEX INCREMENTAL VIEW");
    assert_imv_correct("ags_i_v", &sql);
    ags_assert_fast_path("ags_i_v", "ags_caav_i");

    ags_mutate_secondary("_i", &|| assert_imv_correct("ags_i_v", &sql));
}

/// DEFERRED (the field mode): the flush must succeed — not be swallowed into
/// known_stale with the delta discarded.
#[pg_test]
fn ags_deferred_aliased_join_key_group_col() {
    let sql = ags_fixture("_d");
    crate::create_reflex_ivm("ags_d_v", &sql, None, None, Some("DEFERRED"), None);
    assert_imv_correct("ags_d_v", &sql);
    ags_assert_fast_path("ags_d_v", "ags_caav_d");

    ags_mutate_secondary("_d", &|| {
        Spi::run("SELECT reflex_flush_deferred('ags_caav_d')").expect("flush caav");
        let stale: bool = Spi::get_one(
            "SELECT known_stale FROM public.__reflex_ivm_reference WHERE name = 'ags_d_v'",
        )
        .expect("registry")
        .unwrap_or(false);
        assert!(!stale, "secondary flush must not mark the IMV stale");
        assert_imv_correct("ags_d_v", &sql);
    });
}

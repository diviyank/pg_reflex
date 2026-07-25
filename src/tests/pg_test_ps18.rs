// PS-18 — long wrapper names collapse the three union-mirror trigger
// FUNCTIONS into one, breaking a materialised UNION-ALL wrapper from the
// first base-table write. Found adversarially while reviewing PS-17 (the
// union-mirror repair primitive + trigger-attached check); pre-existing,
// unrelated to that diff — `install_union_mirror_triggers` itself is
// unchanged there, only its caller was new. See
// untreated_bugs/2026-07-25_union_mirror_function_name_collision.md.
//
// Root cause: the three mirror trigger FUNCTION names were built by
// appending the DML-kind tag (`_ins`/`_del`/`_upd`) AFTER the unbounded
// wrapper-derived component (`__reflex_union_mirror_<wrapper>_<i>_<tag>`).
// PostgreSQL silently truncates any identifier over NAMEDATALEN-1 (63 bytes)
// at a char boundary, so once the wrapper-derived prefix alone reaches 62
// bytes, the one character that distinguishes `ins`/`del`/`upd` never
// survives truncation and all three `CREATE OR REPLACE FUNCTION`s collapse
// onto the same `proname` — the last one issued (`upd`) silently overwrites
// the other two.
//
// Moving the tag before the unbounded wrapper component (matching what
// TRIGGER names already did) fixes that dimension alone, but function names
// are global per schema (unlike per-relation trigger names), so a second,
// longer wrapper name can still truncate away the trailing `_<operand_idx>`
// and collapse operand 0 and operand 1's same-kind function onto one
// `proname` instead — the actual failure mode this file's second test
// caught. Fixed by running the full raw name (tag, wrapper, and operand
// index together) through `safe_identifier`, which hashes the complete
// string into its truncated form whenever it exceeds 63 bytes, so two names
// differing only in a byte past the naive cutoff still end up distinct
// regardless of wrapper length. `drop_ivm.rs`'s independent reconstruction
// of these names for cleanup was updated to match, or `DROP FUNCTION IF
// EXISTS` silently no-ops on the mismatch and leaks the function.

fn ps18_count(sql: &str) -> i64 {
    Spi::get_one::<i64>(sql)
        .expect("count query failed")
        .expect("NULL count")
}

/// A materialised UNION-ALL wrapper whose name is 39 bytes — safely inside
/// the verified collision zone (wrapper len >= 38) for a 1-digit operand
/// index, comfortably past the boundary rather than sitting exactly on it.
fn ps18_make_long_wrapper_fixture() {
    Spi::run("CREATE TABLE ps18_len39_wrapper_name_abcdefgh_a (id BIGINT, v NUMERIC)")
        .expect("a");
    Spi::run("CREATE TABLE ps18_len39_wrapper_name_abcdefgh_b (id BIGINT, v NUMERIC)")
        .expect("b");
    Spi::run("INSERT INTO ps18_len39_wrapper_name_abcdefgh_a VALUES (1, 10)").expect("seed a");
    Spi::run("INSERT INTO ps18_len39_wrapper_name_abcdefgh_b VALUES (2, 20)").expect("seed b");
    Spi::run(
        "SELECT create_reflex_ivm('ps18_len39_wrapper_name_abcdefgh', \
           'WITH u AS (SELECT id, v FROM ps18_len39_wrapper_name_abcdefgh_a \
                       UNION ALL SELECT id, v FROM ps18_len39_wrapper_name_abcdefgh_b) \
            SELECT id, SUM(v) AS total FROM u GROUP BY id', 'id')",
    )
    .expect("create CTE-over-UNION-ALL IMV");
}

const PS18_WRAPPER: &str = "ps18_len39_wrapper_name_abcdefgh__cte_u";

/// (1) The three mirror trigger functions must be genuinely distinct
/// (different `pg_proc` oids), not collapsed onto one by truncation.
#[pg_test]
fn ps18_long_wrapper_mirror_functions_stay_distinct() {
    ps18_make_long_wrapper_fixture();
    assert_eq!(
        PS18_WRAPPER.len(),
        39,
        "fixture precondition: wrapper name is 39 bytes, inside the collision zone"
    );
    let is_table = ps18_count(&format!(
        "SELECT count(*) FROM pg_class WHERE relname = '{PS18_WRAPPER}' AND relkind = 'r'"
    ));
    assert_eq!(is_table, 1, "fixture precondition: materialised wrapper");

    for operand in [
        "ps18_len39_wrapper_name_abcdefgh__cte_u__union_0",
        "ps18_len39_wrapper_name_abcdefgh__cte_u__union_1",
    ] {
        let distinct_functions = ps18_count(&format!(
            "SELECT count(DISTINCT tgfoid) FROM pg_trigger \
             WHERE tgrelid = '{operand}'::regclass AND NOT tgisinternal"
        ));
        assert_eq!(
            distinct_functions, 3,
            "operand {operand}: the 3 mirror triggers must point at 3 DISTINCT \
             functions, not have collapsed onto one via name truncation"
        );
    }
}

/// Rows present in `imv_select` but not in `oracle`, plus the reverse. Zero
/// means the wrapper matches a fresh derivation exactly, multiset-wise —
/// robust to exactly which mirror trigger(s) actually fired for a given
/// base-table operation (the operand sub-IMV's own incremental maintenance
/// may resync via DELETE+INSERT rather than a raw UPDATE, so asserting on
/// which specific trigger ran would encode an assumption this test doesn't
/// need to make).
fn ps18_diff_count(imv_select: &str, oracle: &str) -> i64 {
    ps18_count(&format!(
        "SELECT count(*) FROM ( \
           (({imv_select}) EXCEPT ALL ({oracle})) \
           UNION ALL \
           (({oracle}) EXCEPT ALL ({imv_select})) \
         ) AS d"
    ))
}

const PS18_ORACLE: &str = "SELECT 0::SMALLINT AS __reflex_src_idx, id, v \
      FROM ps18_len39_wrapper_name_abcdefgh_a \
    UNION ALL \
    SELECT 1::SMALLINT AS __reflex_src_idx, id, v \
      FROM ps18_len39_wrapper_name_abcdefgh_b";

fn ps18_assert_wrapper_matches_oracle(step: &str) {
    let diff = ps18_diff_count(
        &format!("SELECT __reflex_src_idx, id, v FROM {PS18_WRAPPER}"),
        PS18_ORACLE,
    );
    assert_eq!(diff, 0, "wrapper diverged from a fresh derivation after {step}");
}

/// (2) Functional: each DML kind must run its OWN body, not another kind's
/// (the actual failure mode — a collapsed `proname` means whichever
/// `CREATE OR REPLACE` ran last wins for every trigger sharing that name,
/// and firing an INSERT/DELETE-shaped trigger through the UPDATE body errors
/// immediately since `__reflex_old`/`__reflex_new` transition tables differ
/// by trigger kind). Proven two ways: the INSERT must not error at all (the
/// literal reported crash), and after a full INSERT/UPDATE/DELETE sequence
/// the wrapper's content must still match a fresh derivation exactly.
#[pg_test]
fn ps18_long_wrapper_mirror_functions_run_correct_body() {
    ps18_make_long_wrapper_fixture();
    ps18_assert_wrapper_matches_oracle("create");

    Spi::run("INSERT INTO ps18_len39_wrapper_name_abcdefgh_a VALUES (3, 30)")
        .expect("insert into operand a must not error");
    ps18_assert_wrapper_matches_oracle("insert");

    Spi::run("UPDATE ps18_len39_wrapper_name_abcdefgh_a SET v = 999 WHERE id = 3")
        .expect("update on operand a must not error");
    ps18_assert_wrapper_matches_oracle("update");

    Spi::run("DELETE FROM ps18_len39_wrapper_name_abcdefgh_a WHERE id = 3")
        .expect("delete on operand a must not error");
    ps18_assert_wrapper_matches_oracle("delete");

    Spi::run("UPDATE ps18_len39_wrapper_name_abcdefgh_b SET v = 777 WHERE id = 2")
        .expect("update on operand b must not error");
    ps18_assert_wrapper_matches_oracle("update on the OTHER operand (index 1)");
}

/// (3) `drop_reflex_ivm` reconstructs these same three-per-operand function
/// names independently (`drop_ivm.rs`) to clean them up, since dropping the
/// mirror TRIGGER by relation cascade does not drop the FUNCTION it points
/// at. That reconstruction must apply the identical `safe_identifier`
/// hashing or `DROP FUNCTION IF EXISTS` silently no-ops on the mismatch and
/// leaks every mirror function this wrapper ever created.
#[pg_test]
fn ps18_drop_reflex_ivm_leaves_no_orphan_mirror_functions() {
    ps18_make_long_wrapper_fixture();
    let before = ps18_count(
        "SELECT count(*) FROM pg_proc WHERE proname LIKE '__reflex_union_mirror_%'",
    );
    assert_eq!(before, 6, "fixture precondition: 3 functions per operand x 2 operands");

    Spi::run("SELECT drop_reflex_ivm('ps18_len39_wrapper_name_abcdefgh', TRUE)")
        .expect("drop must not error");

    let after = ps18_count(
        "SELECT count(*) FROM pg_proc WHERE proname LIKE '__reflex_union_mirror_%'",
    );
    assert_eq!(after, 0, "drop_reflex_ivm must not leak any union-mirror function");
}

/// (4) A wrapper created by pg_reflex 1.11.0 or earlier has its mirror
/// functions named with the LEGACY `<wrapper>_<i>_<op>` order (tag last, no
/// `safe_identifier` hash) — this fix only changes what NEW creates produce,
/// so `drop_reflex_ivm` must still recognise and remove the legacy form or
/// every already-deployed union-mirror wrapper leaks its functions on drop
/// the moment the module is upgraded. Simulated by manually installing one
/// legacy-named no-op function alongside a real (short-name, unhashed by
/// either scheme) wrapper and asserting drop removes both forms.
#[pg_test]
fn ps18_drop_reflex_ivm_cleans_up_legacy_named_mirror_functions() {
    Spi::run(
        "CREATE TABLE ps18_legacy_a (id BIGINT, v NUMERIC); \
         CREATE TABLE ps18_legacy_b (id BIGINT, v NUMERIC); \
         SELECT create_reflex_ivm('ps18_legacy_wrap', \
           'WITH u AS (SELECT id, v FROM ps18_legacy_a \
                       UNION ALL SELECT id, v FROM ps18_legacy_b) \
            SELECT id, SUM(v) AS total FROM u GROUP BY id', 'id')",
    )
    .expect("create CTE-over-UNION-ALL IMV");

    // The wrapper is short, so the current (fixed) scheme produces unhashed,
    // tag-first names. Install one further function per operand under the
    // OLD tag-last name to stand in for a pre-1.11.1 leftover.
    for i in 0..2 {
        Spi::run(&format!(
            "CREATE FUNCTION public.__reflex_union_mirror_ps18_legacy_wrap__cte_u_{i}_ins() \
             RETURNS TRIGGER LANGUAGE plpgsql AS $body$ BEGIN RETURN NULL; END; $body$"
        ))
        .expect("install legacy-named stand-in function");
    }

    let legacy_before = ps18_count(
        "SELECT count(*) FROM pg_proc \
         WHERE proname LIKE '__reflex_union_mirror_ps18_legacy_wrap__cte_u_%_ins'",
    );
    assert_eq!(legacy_before, 2, "fixture precondition: 2 legacy-named stand-ins installed");

    Spi::run("SELECT drop_reflex_ivm('ps18_legacy_wrap', TRUE)").expect("drop must not error");

    let legacy_after = ps18_count(
        "SELECT count(*) FROM pg_proc \
         WHERE proname LIKE '__reflex_union_mirror_ps18_legacy_wrap%'",
    );
    assert_eq!(
        legacy_after, 0,
        "drop_reflex_ivm must clean up legacy-named (pre-1.11.1) mirror functions too"
    );
}

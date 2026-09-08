// A1: an ignored source whose columns determine the IMV's contents is refused
// at create time unless explicitly acknowledged with a '!' prefix.
//
// The 2026-09 silent-wipe incident: an IMV declared ignore_sources on a table
// whose `status` column gated its WHERE. The status changed, nothing refreshed
// the IMV, and a later partition-scoped rebuild from the base query wrote the
// slice to zero rows — successfully, silently, permanently.

fn isx_fixture() {
    Spi::run("CREATE TABLE isx_dp (id BIGINT PRIMARY KEY, status TEXT NOT NULL)").expect("dp");
    Spi::run("INSERT INTO isx_dp VALUES (1, 'validated')").expect("seed dp");
    Spi::run("CREATE TABLE isx_ss (dem_plan_id BIGINT, qty INT)").expect("ss");
    Spi::run("INSERT INTO isx_ss VALUES (1, 10)").expect("seed ss");
}

/// The incident's shape: the ignored source appears in the WHERE.
#[pg_test]
fn isx_refuses_ignored_source_referenced_in_where() {
    isx_fixture();
    let r = Spi::get_one::<String>(
        "SELECT create_reflex_ivm('isx_imv', \
           'SELECT ss.dem_plan_id, ss.qty FROM isx_ss ss \
              JOIN isx_dp dp ON dp.id = ss.dem_plan_id \
             WHERE dp.status = ''validated''', \
           'dem_plan_id', 'UNLOGGED', 'DEFERRED', 'isx_dp')",
    )
    .expect("create call")
    .expect("create result");
    assert!(r.starts_with("ERROR"), "must refuse, got: {r}");
    assert!(r.contains("isx_dp"), "the error must name the source: {r}");
    assert!(r.contains("status"), "the error must name the column: {r}");
}

/// The '!' ack permits it deliberately.
#[pg_test]
fn isx_ack_marker_permits_creation() {
    isx_fixture();
    let r = Spi::get_one::<String>(
        "SELECT create_reflex_ivm('isx_imv2', \
           'SELECT ss.dem_plan_id, ss.qty FROM isx_ss ss \
              JOIN isx_dp dp ON dp.id = ss.dem_plan_id \
             WHERE dp.status = ''validated''', \
           'dem_plan_id', 'UNLOGGED', 'DEFERRED', '!isx_dp')",
    )
    .expect("create call")
    .expect("create result");
    assert!(!r.starts_with("ERROR"), "ack must permit creation, got: {r}");
}

/// TRAP 1: the marker must never reach the runtime array, or the ignore itself
/// silently stops working — turning the safety feature into the outage.
#[pg_test]
fn isx_runtime_ignored_sources_is_free_of_the_marker() {
    isx_fixture();
    let _ = Spi::get_one::<String>(
        "SELECT create_reflex_ivm('isx_imv3', \
           'SELECT ss.dem_plan_id, ss.qty FROM isx_ss ss \
              JOIN isx_dp dp ON dp.id = ss.dem_plan_id \
             WHERE dp.status = ''validated''', \
           'dem_plan_id', 'UNLOGGED', 'DEFERRED', '!isx_dp')",
    );
    let clean = Spi::get_one::<bool>(
        "SELECT ignored_sources @> ARRAY['isx_dp'] AND NOT (ignored_sources @> ARRAY['!isx_dp']) \
         FROM public.__reflex_ivm_reference WHERE name = 'isx_imv3'",
    )
    .unwrap()
    .unwrap();
    assert!(clean, "ignored_sources must hold the clean name only");

    let acked = Spi::get_one::<bool>(
        "SELECT ignore_ack @> ARRAY['isx_dp'] \
         FROM public.__reflex_ivm_reference WHERE name = 'isx_imv3'",
    )
    .unwrap()
    .unwrap();
    assert!(acked, "ignore_ack must record the acknowledgement");
}

/// TRAP 1, second face: the marker must not reach trigger installation either.
/// The in-memory list gates `install_source_triggers`; a marker-bearing entry
/// stops matching there, the trigger gets installed, and the declared ignore
/// silently stops being an ignore.
#[pg_test]
fn isx_ack_marker_still_suppresses_the_trigger() {
    isx_fixture();
    let r = Spi::get_one::<String>(
        "SELECT create_reflex_ivm('isx_imv7', \
           'SELECT ss.dem_plan_id, ss.qty FROM isx_ss ss \
              JOIN isx_dp dp ON dp.id = ss.dem_plan_id \
             WHERE dp.status = ''validated''', \
           'dem_plan_id', 'UNLOGGED', 'IMMEDIATE', '!isx_dp')",
    )
    .expect("create call")
    .expect("create result");
    assert!(!r.starts_with("ERROR"), "ack must permit creation, got: {r}");

    let triggers_on_ignored = Spi::get_one::<i64>(
        "SELECT count(*) FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid \
         WHERE c.relname = 'isx_dp' AND NOT t.tgisinternal AND t.tgname ~ '^__reflex_trigger_'",
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        triggers_on_ignored, 0,
        "an acknowledged ignore must still suppress the trigger on the ignored source"
    );

    let triggers_on_kept = Spi::get_one::<i64>(
        "SELECT count(*) FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid \
         WHERE c.relname = 'isx_ss' AND NOT t.tgisinternal AND t.tgname ~ '^__reflex_trigger_'",
    )
    .unwrap()
    .unwrap();
    assert!(
        triggers_on_kept > 0,
        "the non-ignored source must still get its trigger"
    );
}

/// TRAP 2: the marker must survive into create_args, or rebuild_reflex_ivm
/// replays without it, A1 refuses the replay, and the IMV is unrebuildable.
#[pg_test]
fn isx_ack_survives_rebuild() {
    isx_fixture();
    let _ = Spi::get_one::<String>(
        "SELECT create_reflex_ivm('isx_imv4', \
           'SELECT ss.dem_plan_id, ss.qty FROM isx_ss ss \
              JOIN isx_dp dp ON dp.id = ss.dem_plan_id \
             WHERE dp.status = ''validated''', \
           'dem_plan_id', 'UNLOGGED', 'DEFERRED', '!isx_dp')",
    );
    let raw = Spi::get_one::<bool>(
        "SELECT create_args::jsonb -> 'ignore_sources' ? '!isx_dp' \
         FROM public.__reflex_ivm_reference WHERE name = 'isx_imv4'",
    )
    .unwrap()
    .unwrap();
    assert!(
        raw,
        "create_args must retain the raw marker for faithful replay"
    );

    // reflex_rebuild_imv is an alias for reflex_reconcile: it never touches
    // create_args and never re-enters the create path, so asserting on it would
    // stay green with A1 refusing every replay. reflex_rebuild_chain is the only
    // path that drops and re-creates from create_args, so it is the one that
    // proves the ack survives — and that A1 has not made the IMV unrebuildable.
    let r = Spi::get_one::<String>("SELECT reflex_rebuild_chain('isx_imv4')")
        .expect("rebuild call")
        .expect("rebuild result");
    assert!(
        !r.starts_with("ERROR"),
        "the replay must not be refused by the check that made it necessary: {r}"
    );
    let still_acked = Spi::get_one::<bool>(
        "SELECT ignore_ack @> ARRAY['isx_dp'] \
         FROM public.__reflex_ivm_reference WHERE name = 'isx_imv4'",
    )
    .unwrap()
    .unwrap();
    assert!(still_acked, "the rebuilt IMV must carry the acknowledgement");
}

/// I1: the escape hatch for the rebuild path. A legacy IMV created before A1
/// carries no '!' in create_args, so reflex_rebuild_chain replays a create the
/// check refuses — and reflex_rebuild_chain takes no ignore_sources argument,
/// so the '!' remedy is unreachable from there. reflex_ack_ignore_source must
/// make that state clearable, and the fix must converge in one call.
#[pg_test]
fn isx_ack_function_makes_a_legacy_imv_rebuildable() {
    isx_fixture();
    let r = Spi::get_one::<String>(
        "SELECT create_reflex_ivm('isx_legacy', \
           'SELECT ss.dem_plan_id, ss.qty FROM isx_ss ss \
              JOIN isx_dp dp ON dp.id = ss.dem_plan_id \
             WHERE dp.status = ''validated''', \
           'dem_plan_id', 'UNLOGGED', 'DEFERRED', '!isx_dp')",
    )
    .expect("create call")
    .expect("create result");
    assert!(!r.starts_with("ERROR"), "create returned: {r}");

    // Reproduce the legacy shape: strip the ack from both places, exactly as a
    // pre-A1 install would have it.
    Spi::run(
        "UPDATE public.__reflex_ivm_reference \
            SET ignore_ack = ARRAY[]::TEXT[], \
                create_args = jsonb_set(create_args::jsonb, '{ignore_sources}', \
                                        '[\"isx_dp\"]'::jsonb)::text \
          WHERE name = 'isx_legacy'",
    )
    .expect("de-ack");

    // The refusal reaches the caller as a RAISE (reflex_rebuild_chain uses
    // pgrx::error! so the drop rolls back), which would abort this test's
    // transaction. __reflex_doctor_try_repair wraps it in a savepoint and hands
    // back the message — the same shape an operator sees.
    let refused = Spi::get_one::<String>(
        "SELECT public.__reflex_doctor_try_repair( \
           'SELECT reflex_rebuild_chain(''isx_legacy'')')",
    )
    .expect("rebuild call")
    .expect("rebuild result");
    assert!(
        refused.starts_with("failed:"),
        "the legacy replay must be refused, got: {refused}"
    );
    assert!(
        refused.contains("unsound") && refused.contains("reflex_ack_ignore_source"),
        "the refusal must name the remedy that clears it, got: {refused}"
    );

    let ack = Spi::get_one::<String>("SELECT reflex_ack_ignore_source('isx_legacy', 'isx_dp')")
        .expect("ack call")
        .expect("ack result");
    assert_eq!(ack, "ACKNOWLEDGED", "ack returned: {ack}");

    let raw = Spi::get_one::<bool>(
        "SELECT create_args::jsonb -> 'ignore_sources' ? '!isx_dp' \
           AND NOT (create_args::jsonb -> 'ignore_sources' ? 'isx_dp') \
         FROM public.__reflex_ivm_reference WHERE name = 'isx_legacy'",
    )
    .unwrap()
    .unwrap();
    assert!(
        raw,
        "the ack must REPLACE the bare entry in create_args, not sit beside it"
    );

    let after = Spi::get_one::<String>("SELECT reflex_rebuild_chain('isx_legacy')")
        .expect("rebuild call")
        .expect("rebuild result");
    assert!(
        !after.starts_with("ERROR"),
        "the prescribed remedy must converge in one call, got: {after}"
    );
}

/// Same defect as isx_ack_function_makes_a_legacy_imv_rebuildable, but sized
/// like the production incident that exposed it: a ~32-char IMV name and a
/// schema-qualified ~20-char source, refused through a long
/// qualifier-attribution reason. __reflex_doctor_try_repair caps its output at
/// `left(SQLERRM, 400)`; before the remedy-first reordering the working
/// remedy (reflex_ack_ignore_source) sat behind two copies of both names plus
/// the reason and was truncated away entirely at this size, leaving an
/// operator with a finding they could not clear.
#[pg_test]
fn isx_ack_remedy_survives_truncation_for_production_sized_names() {
    isx_fixture();
    Spi::run("CREATE SCHEMA isxq").expect("schema");
    Spi::run("CREATE TABLE isxq.demand_planning (id BIGINT PRIMARY KEY, status TEXT NOT NULL)")
        .expect("qualified source");
    Spi::run("INSERT INTO isxq.demand_planning VALUES (1, 'validated')").expect("seed");

    let view_name = "isx_current_assortment_activity";
    let source = "isxq.demand_planning";

    // `t` is a derived-table alias: valid SQL Postgres can execute, but
    // invisible to pg_reflex's top-level alias map (which only tracks real
    // FROM/JOIN tables). Referencing it in the JOIN ON — rather than WHERE,
    // which the walk visits first and would otherwise claim the reason first
    // — is exactly the shape that produces the long "qualifier ... could not
    // be attributed to a top-level source" reason for the JOIN ON clause.
    let ivm_query = format!(
        "SELECT ss.dem_plan_id, ss.qty FROM isx_ss ss \
           JOIN (SELECT id FROM {source} WHERE status = ''validated'') t \
             ON t.id = ss.dem_plan_id"
    );

    // Confirm the shape hits the intended long reason, straight from
    // create_reflex_ivm's own (untruncated) return — not the 400-capped
    // doctor-repair path this test is really about.
    let bare_refusal = Spi::get_one::<String>(&format!(
        "SELECT create_reflex_ivm('{view_name}', '{ivm_query}', \
           'dem_plan_id', 'UNLOGGED', 'DEFERRED', '{source}')"
    ))
    .expect("bare create call")
    .expect("bare create result");
    assert!(
        bare_refusal.contains("qualifier t in JOIN ON could not be attributed"),
        "sanity: must hit the long qualifier-attribution reason, got: {bare_refusal}"
    );

    let create_sql = format!(
        "SELECT create_reflex_ivm('{view_name}', '{ivm_query}', \
           'dem_plan_id', 'UNLOGGED', 'DEFERRED', '!{source}')"
    );
    let r = Spi::get_one::<String>(&create_sql)
        .expect("create call")
        .expect("create result");
    assert!(!r.starts_with("ERROR"), "create returned: {r}");

    // Reproduce the legacy pre-A1 shape: strip the ack from both places.
    Spi::run(&format!(
        "UPDATE public.__reflex_ivm_reference \
            SET ignore_ack = ARRAY[]::TEXT[], \
                create_args = jsonb_set(create_args::jsonb, '{{ignore_sources}}', \
                                        '[\"{source}\"]'::jsonb)::text \
          WHERE name = '{view_name}'"
    ))
    .expect("de-ack");

    let refused = Spi::get_one::<String>(&format!(
        "SELECT public.__reflex_doctor_try_repair( \
           'SELECT reflex_rebuild_chain(''{view_name}'')')"
    ))
    .expect("rebuild call")
    .expect("rebuild result");
    assert!(
        refused.starts_with("failed:"),
        "the legacy replay must be refused, got: {refused}"
    );
    assert!(
        refused.len() <= "failed:".len() + 400,
        "sanity: the repair path is still capped at 400 chars, got len {}: {refused}",
        refused.len()
    );
    assert!(
        refused.contains("reflex_ack_ignore_source"),
        "the remedy must survive the 400-char truncation for production-sized names, got: {refused}"
    );
}

/// The ack function refuses loudly rather than silently no-opping.
#[pg_test]
fn isx_ack_function_refuses_unknown_imv_and_unignored_source() {
    isx_fixture();
    let r = Spi::get_one::<String>("SELECT reflex_ack_ignore_source('isx_nope', 'isx_dp')")
        .unwrap()
        .unwrap();
    assert!(r.starts_with("ERROR"), "unknown IMV must be refused: {r}");

    let _ = Spi::get_one::<String>(
        "SELECT create_reflex_ivm('isx_plain', 'SELECT dem_plan_id, qty FROM isx_ss', \
         'dem_plan_id', 'UNLOGGED', 'DEFERRED', NULL)",
    );
    let r = Spi::get_one::<String>("SELECT reflex_ack_ignore_source('isx_plain', 'isx_dp')")
        .unwrap()
        .unwrap();
    assert!(
        r.starts_with("ERROR"),
        "acking a source that is not ignored must be refused: {r}"
    );
}

/// M3: an entry that is a bare marker names no source. Refuse it rather than
/// writing an empty string into ignored_sources and ignore_ack.
#[pg_test]
fn isx_bare_marker_entry_is_refused() {
    isx_fixture();
    let r = Spi::get_one::<String>(
        "SELECT create_reflex_ivm('isx_bang', 'SELECT dem_plan_id, qty FROM isx_ss', \
         'dem_plan_id', 'UNLOGGED', 'DEFERRED', '!')",
    )
    .expect("create call")
    .expect("create result");
    assert!(
        r.starts_with("ERROR") && r.contains("names no source"),
        "a bare '!' must be refused, got: {r}"
    );
}

/// C1(a) — the ignored source is reachable only through a WHERE subquery. No
/// CTE, no set operation, no wildcard, no unqualified column: before the C1 fix
/// the qualifier `dp` resolved to itself and this was accepted.
#[pg_test]
fn isx_subquery_scoped_reference_is_refused() {
    isx_fixture();
    let r = Spi::get_one::<String>(
        "SELECT create_reflex_ivm('isx_sub', \
           'SELECT ss.dem_plan_id, ss.qty FROM isx_ss ss \
             WHERE ss.dem_plan_id IN \
               (SELECT dp.id FROM isx_dp dp WHERE dp.status = ''validated'')', \
           'dem_plan_id', 'UNLOGGED', 'DEFERRED', 'isx_dp')",
    )
    .expect("create call")
    .expect("create result");
    assert!(
        r.contains("is unsound for IMV") && r.contains("isx_dp"),
        "a subquery-scoped reference to the ignored source must be refused, got: {r}"
    );
}

/// C1(b) — the ignored source is wrapped in a derived table, whose alias never
/// enters the top-level alias map.
#[pg_test]
fn isx_derived_table_reference_is_refused() {
    isx_fixture();
    let r = Spi::get_one::<String>(
        "SELECT create_reflex_ivm('isx_der', \
           'SELECT ss.dem_plan_id, t.status FROM isx_ss ss \
              JOIN (SELECT id, status FROM isx_dp) t ON t.id = ss.dem_plan_id \
             WHERE t.status = ''validated''', \
           'dem_plan_id', 'UNLOGGED', 'DEFERRED', 'isx_dp')",
    )
    .expect("create call")
    .expect("create result");
    assert!(
        r.contains("is unsound for IMV") && r.contains("isx_dp"),
        "a derived-table reference to the ignored source must be refused, got: {r}"
    );
}

/// C1(c) — a quoted alias. Ident::to_string() re-adds the quotes for the alias
/// map key while Ident.value does not for the collected qualifier, so the
/// lookup misses and the fallback must catch it.
#[pg_test]
fn isx_quoted_alias_reference_is_refused() {
    isx_fixture();
    let r = Spi::get_one::<String>(
        "SELECT create_reflex_ivm('isx_quoted', \
           'SELECT \"DP\".id AS dem_plan_id, ss.qty FROM isx_dp \"DP\", isx_ss ss \
             WHERE \"DP\".status = ''validated'' AND \"DP\".id = ss.dem_plan_id', \
           'dem_plan_id', 'UNLOGGED', 'DEFERRED', 'isx_dp')",
    )
    .expect("create call")
    .expect("create result");
    assert!(
        r.contains("is unsound for IMV") && r.contains("isx_dp"),
        "a quoted-alias reference to the ignored source must be refused, got: {r}"
    );
}

/// Fail toward "unsound": a query the resolver cannot attribute is refused, not
/// waved through. collect_imv_relevant_columns returns an empty map for CTE
/// queries — building on that would have silently exempted this shape.
#[pg_test]
fn isx_unattributable_cte_query_is_refused() {
    isx_fixture();
    let r = Spi::get_one::<String>(
        "SELECT create_reflex_ivm('isx_imv5', \
           'WITH v AS (SELECT id FROM isx_dp WHERE status = ''validated'') \
            SELECT ss.dem_plan_id, ss.qty FROM isx_ss ss JOIN v ON v.id = ss.dem_plan_id', \
           'dem_plan_id', 'UNLOGGED', 'DEFERRED', 'isx_dp')",
    )
    .expect("create call")
    .expect("create result");
    assert!(
        r.starts_with("ERROR"),
        "unattributable query must be refused, got: {r}"
    );
    // The refusal must come from the OUTER query being unattributable, not from
    // a sub-IMV of the CTE decomposition happening to be refused for its own
    // reason — otherwise this test stays green with the CTE verdict removed.
    assert!(
        r.contains("(CTE)") && r.contains("'isx_imv5'"),
        "the outer CTE query itself must be what is refused, got: {r}"
    );
}

/// Second unattributable shape, and the one no sub-IMV re-check can rescue: a
/// set operation. Each operand is a plain single-source SELECT that the
/// resolver would pass, so if the set-op verdict is removed the create
/// succeeds — which is the false-green this test exists to block.
#[pg_test]
fn isx_unattributable_set_operation_is_refused() {
    isx_fixture();
    let r = Spi::get_one::<String>(
        "SELECT create_reflex_ivm('isx_imv9', \
           'SELECT ss.dem_plan_id AS dem_plan_id, ss.qty AS qty FROM isx_ss ss \
            UNION ALL \
            SELECT dp.id AS dem_plan_id, 0 AS qty FROM isx_dp dp', \
           'dem_plan_id', 'UNLOGGED', 'DEFERRED', 'isx_dp')",
    )
    .expect("create call")
    .expect("create result");
    assert!(
        r.starts_with("ERROR") && r.contains("set operation"),
        "an unattributable set operation must be refused, got: {r}"
    );
}

/// A genuinely sound ignore is still allowed: the source is not referenced at all.
#[pg_test]
fn isx_allows_sound_ignore() {
    isx_fixture();
    Spi::run("CREATE TABLE isx_unrelated (id BIGINT)").expect("unrelated");
    let r = Spi::get_one::<String>(
        "SELECT create_reflex_ivm('isx_imv6', 'SELECT dem_plan_id, qty FROM isx_ss', \
         'dem_plan_id', 'UNLOGGED', 'DEFERRED', 'isx_unrelated')",
    )
    .expect("create call")
    .expect("create result");
    assert!(!r.starts_with("ERROR"), "a sound ignore must be allowed, got: {r}");
}

/// No ignore_sources at all must not be perturbed by the check — the resolver
/// must never refuse a query that declares no ignores, however unparseable the
/// shape is to it.
#[pg_test]
fn isx_no_ignore_sources_is_never_refused() {
    isx_fixture();
    let r = Spi::get_one::<String>(
        "SELECT create_reflex_ivm('isx_imv8', \
           'SELECT ss.dem_plan_id, ss.qty FROM isx_ss ss \
              JOIN isx_dp dp ON dp.id = ss.dem_plan_id \
             WHERE dp.status = ''validated''', \
           'dem_plan_id', 'UNLOGGED', 'DEFERRED', NULL)",
    )
    .expect("create call")
    .expect("create result");
    assert!(
        !r.starts_with("ERROR"),
        "an IMV with no ignore_sources must be unaffected, got: {r}"
    );
}

/// `reflex_audit` returns a formatted REPORT STRING, not a table
/// (`src/audit/mod.rs:590-603`) — assert on its text, as the existing audit
/// tests do (`src/tests/pg_test_audit.rs:41-44`).
fn isx_audit_flags(imv: &str) -> bool {
    let report: String = Spi::get_one(&format!("SELECT reflex_audit('{imv}')"))
        .expect("audit query ok")
        .expect("non-null report");
    report.contains("ignore-soundness")
}

/// An IMV installed before A1 existed must be surfaced by the audit.
#[pg_test]
fn isx_audit_flags_installed_unsound_imv() {
    isx_fixture();
    // Create it soundly, then make it unsound the way the field did: by adding
    // the ignore afterwards. A registry row hand-built to fake the shape would
    // be a false-green fixture; this is a real IMV over real sources.
    let r = Spi::get_one::<String>(
        "SELECT create_reflex_ivm('isx_aud', \
           'SELECT ss.dem_plan_id, ss.qty FROM isx_ss ss \
              JOIN isx_dp dp ON dp.id = ss.dem_plan_id \
             WHERE dp.status = ''validated''', \
           'dem_plan_id', 'UNLOGGED', 'DEFERRED', '!isx_dp')",
    )
    .expect("create")
    .expect("create result");
    assert!(!r.starts_with("ERROR"), "setup create failed: {r}");

    // Strip the acknowledgement to simulate a pre-A1 installation.
    Spi::run(
        "UPDATE public.__reflex_ivm_reference SET ignore_ack = ARRAY[]::TEXT[] \
         WHERE name = 'isx_aud'",
    )
    .expect("strip ack");

    assert!(isx_audit_flags("isx_aud"), "audit must flag the unsound ignore");
}

/// The remedy must converge: running it clears the finding it printed.
#[pg_test]
fn isx_ack_function_clears_the_finding() {
    isx_fixture();
    let _ = Spi::get_one::<String>(
        "SELECT create_reflex_ivm('isx_aud2', \
           'SELECT ss.dem_plan_id, ss.qty FROM isx_ss ss \
              JOIN isx_dp dp ON dp.id = ss.dem_plan_id \
             WHERE dp.status = ''validated''', \
           'dem_plan_id', 'UNLOGGED', 'DEFERRED', '!isx_dp')",
    );
    Spi::run(
        "UPDATE public.__reflex_ivm_reference SET ignore_ack = ARRAY[]::TEXT[] \
         WHERE name = 'isx_aud2'",
    )
    .expect("strip ack");
    assert!(isx_audit_flags("isx_aud2"), "precondition: finding present");

    let r = Spi::get_one::<String>("SELECT reflex_ack_ignore_source('isx_aud2', 'isx_dp')")
        .expect("ack call")
        .expect("ack result");
    assert!(!r.starts_with("ERROR"), "ack returned: {r}");

    assert!(!isx_audit_flags("isx_aud2"), "the remedy must clear its own finding");

    // And it must survive a rebuild, or the finding comes back.
    let raw = Spi::get_one::<bool>(
        "SELECT create_args::jsonb -> 'ignore_sources' ? '!isx_dp' \
         FROM public.__reflex_ivm_reference WHERE name = 'isx_aud2'",
    )
    .unwrap()
    .unwrap();
    assert!(raw, "the ack must be patched into create_args, not only the column");
}

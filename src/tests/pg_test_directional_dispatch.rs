// Item α (2026-05-15) — Directional UPDATE dispatch correctness tests.
//
// Each test creates an IMV with a filtered WHERE clause, mutates the source
// in a way that exercises a specific directional path (OUT→IN, IN→OUT,
// mixed, pure-data, no-relevant-columns), and asserts EXCEPT-ALL = 0
// against a fresh recomputation.
//
// The IMVs here all have a non-empty `imv_relevant_columns[source]` so the
// directional probe gate fires; the no-gate fallback path is exercised
// indirectly by all existing tests over CTE-using / SELECT* IMVs.

#[pg_test]
fn test_directional_out_to_in_single_row() {
    Spi::run("CREATE TABLE dd_out_in (id INT PRIMARY KEY, city TEXT, amount BIGINT, status TEXT)")
        .expect("create");
    Spi::run(
        "INSERT INTO dd_out_in VALUES \
         (1, 'A', 10, 'archived'), \
         (2, 'A', 20, 'archived'), \
         (3, 'B', 30, 'validated')",
    )
    .expect("seed");

    let result = crate::create_reflex_ivm(
        "dd_out_in_v",
        "SELECT city, SUM(amount) AS s FROM dd_out_in WHERE status = 'validated' GROUP BY city",
        None,
        None,
        None,
        None,
    );
    assert!(!result.starts_with("ERROR"), "create: {}", result);

    // Pre: only city='B' is in the IMV.
    let pre_b = Spi::get_one::<i64>("SELECT s::BIGINT FROM dd_out_in_v WHERE city = 'B'")
        .expect("q")
        .expect("v");
    assert_eq!(pre_b, 30);

    // OUT→IN: flip id=1 from archived → validated. delta_old is empty
    // post-filter; delta_new has 1 row. Directional probe → 'INSERT' shape.
    Spi::run("UPDATE dd_out_in SET status = 'validated' WHERE id = 1").expect("flip");

    assert_imv_correct(
        "dd_out_in_v",
        "SELECT city, SUM(amount) AS s FROM dd_out_in WHERE status = 'validated' GROUP BY city",
    );

    crate::drop_reflex_ivm("dd_out_in_v");
}

#[pg_test]
fn test_directional_out_to_in_multi_row() {
    Spi::run("CREATE TABLE dd_out_in_m (id INT PRIMARY KEY, city TEXT, amount BIGINT, status TEXT)")
        .expect("create");
    // 200 rows of 'archived' in city 'A', 50 'validated' in city 'B'.
    Spi::run(
        "INSERT INTO dd_out_in_m \
         SELECT g, 'A', g, 'archived' FROM generate_series(1, 200) g",
    )
    .expect("seed-a");
    Spi::run(
        "INSERT INTO dd_out_in_m \
         SELECT 200 + g, 'B', g, 'validated' FROM generate_series(1, 50) g",
    )
    .expect("seed-b");

    let result = crate::create_reflex_ivm(
        "dd_out_in_m_v",
        "SELECT city, SUM(amount) AS s, COUNT(*) AS c \
         FROM dd_out_in_m WHERE status = 'validated' GROUP BY city",
        None,
        None,
        None,
        None,
    );
    assert!(!result.starts_with("ERROR"), "create: {}", result);

    // Multi-row OUT→IN: 100 of the 200 archived rows flip to validated.
    Spi::run("UPDATE dd_out_in_m SET status = 'validated' WHERE id <= 100").expect("flip");

    assert_imv_correct(
        "dd_out_in_m_v",
        "SELECT city, SUM(amount) AS s, COUNT(*) AS c \
         FROM dd_out_in_m WHERE status = 'validated' GROUP BY city",
    );

    crate::drop_reflex_ivm("dd_out_in_m_v");
}

#[pg_test]
fn test_directional_in_to_out_single_row() {
    Spi::run("CREATE TABLE dd_in_out (id INT PRIMARY KEY, city TEXT, amount BIGINT, status TEXT)")
        .expect("create");
    Spi::run(
        "INSERT INTO dd_in_out VALUES \
         (1, 'A', 10, 'validated'), \
         (2, 'A', 20, 'validated'), \
         (3, 'B', 30, 'validated')",
    )
    .expect("seed");

    let result = crate::create_reflex_ivm(
        "dd_in_out_v",
        "SELECT city, SUM(amount) AS s FROM dd_in_out WHERE status = 'validated' GROUP BY city",
        None,
        None,
        None,
        None,
    );
    assert!(!result.starts_with("ERROR"), "create: {}", result);

    // Pre: city='A' total = 30.
    let pre_a = Spi::get_one::<i64>("SELECT s::BIGINT FROM dd_in_out_v WHERE city = 'A'")
        .expect("q")
        .expect("v");
    assert_eq!(pre_a, 30);

    // IN→OUT: flip id=1 from validated → archived. delta_old has 1 row,
    // delta_new is empty post-filter. Directional probe → 'DELETE' shape.
    Spi::run("UPDATE dd_in_out SET status = 'archived' WHERE id = 1").expect("flip");

    assert_imv_correct(
        "dd_in_out_v",
        "SELECT city, SUM(amount) AS s FROM dd_in_out WHERE status = 'validated' GROUP BY city",
    );

    crate::drop_reflex_ivm("dd_in_out_v");
}

#[pg_test]
fn test_directional_in_to_out_multi_row() {
    Spi::run("CREATE TABLE dd_in_out_m (id INT PRIMARY KEY, city TEXT, amount BIGINT, status TEXT)")
        .expect("create");
    Spi::run(
        "INSERT INTO dd_in_out_m \
         SELECT g, 'A', g, 'validated' FROM generate_series(1, 100) g",
    )
    .expect("seed");

    let result = crate::create_reflex_ivm(
        "dd_in_out_m_v",
        "SELECT city, SUM(amount) AS s, COUNT(*) AS c \
         FROM dd_in_out_m WHERE status = 'validated' GROUP BY city",
        None,
        None,
        None,
        None,
    );
    assert!(!result.starts_with("ERROR"), "create: {}", result);

    // 50 of the 100 rows flip out.
    Spi::run("UPDATE dd_in_out_m SET status = 'archived' WHERE id <= 50").expect("flip");

    assert_imv_correct(
        "dd_in_out_m_v",
        "SELECT city, SUM(amount) AS s, COUNT(*) AS c \
         FROM dd_in_out_m WHERE status = 'validated' GROUP BY city",
    );

    crate::drop_reflex_ivm("dd_in_out_m_v");
}

#[pg_test]
fn test_directional_mixed_in_and_out_in_one_statement() {
    // A single UPDATE statement that flips some rows IN (archived→validated)
    // AND some OUT (validated→archived). Both transition multisets have rows
    // post-filter → directional probe → 'UPDATE' (today's UNION ALL path).
    Spi::run("CREATE TABLE dd_mix (id INT PRIMARY KEY, city TEXT, amount BIGINT, status TEXT)")
        .expect("create");
    Spi::run(
        "INSERT INTO dd_mix VALUES \
         (1, 'A', 10, 'validated'), \
         (2, 'A', 20, 'archived'), \
         (3, 'B', 30, 'validated'), \
         (4, 'B', 40, 'archived')",
    )
    .expect("seed");

    let result = crate::create_reflex_ivm(
        "dd_mix_v",
        "SELECT city, SUM(amount) AS s FROM dd_mix WHERE status = 'validated' GROUP BY city",
        None,
        None,
        None,
        None,
    );
    assert!(!result.starts_with("ERROR"), "create: {}", result);

    // CASE flips: id=1 OUT (10 was validated → archived);
    //             id=2 IN  (20 was archived → validated).
    // City A: net 0 contributions but the rows DID move across filter.
    Spi::run(
        "UPDATE dd_mix SET status = CASE WHEN status = 'validated' THEN 'archived' \
         ELSE 'validated' END WHERE id IN (1, 2)",
    )
    .expect("mix");

    assert_imv_correct(
        "dd_mix_v",
        "SELECT city, SUM(amount) AS s FROM dd_mix WHERE status = 'validated' GROUP BY city",
    );

    crate::drop_reflex_ivm("dd_mix_v");
}

#[pg_test]
fn test_directional_pure_data_update_no_filter_change() {
    // UPDATE on a SUM-driving column (amount) without changing status.
    // Both OLD and NEW pass the filter → directional probe → 'UPDATE'.
    Spi::run("CREATE TABLE dd_data (id INT PRIMARY KEY, city TEXT, amount BIGINT, status TEXT)")
        .expect("create");
    Spi::run(
        "INSERT INTO dd_data VALUES \
         (1, 'A', 100, 'validated'), \
         (2, 'A', 200, 'validated'), \
         (3, 'B', 300, 'validated')",
    )
    .expect("seed");

    let result = crate::create_reflex_ivm(
        "dd_data_v",
        "SELECT city, SUM(amount) AS s FROM dd_data WHERE status = 'validated' GROUP BY city",
        None,
        None,
        None,
        None,
    );
    assert!(!result.starts_with("ERROR"), "create: {}", result);

    // Pure data change. No filter movement.
    Spi::run("UPDATE dd_data SET amount = amount * 2 WHERE id = 1").expect("upd");

    assert_imv_correct(
        "dd_data_v",
        "SELECT city, SUM(amount) AS s FROM dd_data WHERE status = 'validated' GROUP BY city",
    );

    crate::drop_reflex_ivm("dd_data_v");
}

#[pg_test]
fn test_directional_with_filter_flip_and_data_change_same_row() {
    // UPDATE that BOTH flips status AND changes a SUM-driving column on the
    // SAME row. OLD post-filter = empty (status was 'archived'), NEW
    // post-filter has the row → INSERT shape. The new amount is correctly
    // captured in the contribution.
    Spi::run("CREATE TABLE dd_combo (id INT PRIMARY KEY, city TEXT, amount BIGINT, status TEXT)")
        .expect("create");
    Spi::run(
        "INSERT INTO dd_combo VALUES \
         (1, 'A', 10, 'archived'), \
         (2, 'A', 20, 'validated')",
    )
    .expect("seed");

    let result = crate::create_reflex_ivm(
        "dd_combo_v",
        "SELECT city, SUM(amount) AS s FROM dd_combo WHERE status = 'validated' GROUP BY city",
        None,
        None,
        None,
        None,
    );
    assert!(!result.starts_with("ERROR"), "create: {}", result);

    Spi::run("UPDATE dd_combo SET status = 'validated', amount = 999 WHERE id = 1")
        .expect("combo");

    assert_imv_correct(
        "dd_combo_v",
        "SELECT city, SUM(amount) AS s FROM dd_combo WHERE status = 'validated' GROUP BY city",
    );

    crate::drop_reflex_ivm("dd_combo_v");
}

#[pg_test]
fn test_directional_bool_or_through_insert_shape() {
    // BOOL_OR algebraic representation must produce correct results when the
    // UPDATE is promoted to INSERT shape.
    Spi::run("CREATE TABLE dd_bool (id INT PRIMARY KEY, grp TEXT, flag BOOLEAN, status TEXT)")
        .expect("create");
    Spi::run(
        "INSERT INTO dd_bool VALUES \
         (1, 'A', true,  'archived'), \
         (2, 'A', false, 'archived'), \
         (3, 'B', true,  'validated')",
    )
    .expect("seed");

    let result = crate::create_reflex_ivm(
        "dd_bool_v",
        "SELECT grp, BOOL_OR(flag) AS any_flag FROM dd_bool WHERE status = 'validated' GROUP BY grp",
        None,
        None,
        None,
        None,
    );
    assert!(!result.starts_with("ERROR"), "create: {}", result);

    // OUT→IN: id=1 (flag=true) flips to validated. Group 'A' enters IMV.
    Spi::run("UPDATE dd_bool SET status = 'validated' WHERE id = 1").expect("flip");

    assert_imv_correct(
        "dd_bool_v",
        "SELECT grp, BOOL_OR(flag) AS any_flag FROM dd_bool WHERE status = 'validated' GROUP BY grp",
    );

    crate::drop_reflex_ivm("dd_bool_v");
}

#[pg_test]
fn test_directional_min_max_through_insert_shape() {
    // MIN/MAX aggregate must produce correct results when promoted to
    // INSERT shape. Top-K refresh + recompute logic lives in the INSERT
    // codegen path; this verifies it's exercised correctly from an UPDATE
    // trigger context.
    Spi::run("CREATE TABLE dd_mm (id INT PRIMARY KEY, grp TEXT, val BIGINT, status TEXT)")
        .expect("create");
    Spi::run(
        "INSERT INTO dd_mm VALUES \
         (1, 'A', 10, 'archived'), \
         (2, 'A', 20, 'archived'), \
         (3, 'B', 30, 'validated'), \
         (4, 'B', 40, 'validated')",
    )
    .expect("seed");

    let result = crate::create_reflex_ivm(
        "dd_mm_v",
        "SELECT grp, MIN(val) AS mn, MAX(val) AS mx FROM dd_mm WHERE status = 'validated' GROUP BY grp",
        None,
        None,
        None,
        None,
    );
    assert!(!result.starts_with("ERROR"), "create: {}", result);

    // OUT→IN: id=1 (val=10) flips. Group 'A' enters with MIN=10, MAX=10.
    Spi::run("UPDATE dd_mm SET status = 'validated' WHERE id = 1").expect("flip");

    assert_imv_correct(
        "dd_mm_v",
        "SELECT grp, MIN(val) AS mn, MAX(val) AS mx FROM dd_mm WHERE status = 'validated' GROUP BY grp",
    );

    // IN→OUT: remove the only row for the now-promoted group via filter-flip.
    Spi::run("UPDATE dd_mm SET status = 'archived' WHERE id = 1").expect("flip-back");
    assert_imv_correct(
        "dd_mm_v",
        "SELECT grp, MIN(val) AS mn, MAX(val) AS mx FROM dd_mm WHERE status = 'validated' GROUP BY grp",
    );

    crate::drop_reflex_ivm("dd_mm_v");
}

#[pg_test]
fn test_directional_unfiltered_imv_falls_through() {
    // An IMV without a WHERE clause has no `imv_relevant_where`; the
    // directional probe is gated off. UPDATEs go through today's UNION ALL
    // path. Correctness must hold regardless.
    Spi::run("CREATE TABLE dd_nofilter (id INT PRIMARY KEY, city TEXT, amount BIGINT)")
        .expect("create");
    Spi::run(
        "INSERT INTO dd_nofilter VALUES (1, 'A', 10), (2, 'A', 20), (3, 'B', 30)",
    )
    .expect("seed");

    let result = crate::create_reflex_ivm(
        "dd_nofilter_v",
        "SELECT city, SUM(amount) AS s FROM dd_nofilter GROUP BY city",
        None,
        None,
        None,
        None,
    );
    assert!(!result.starts_with("ERROR"), "create: {}", result);

    Spi::run("UPDATE dd_nofilter SET amount = 100 WHERE id = 1").expect("upd");

    assert_imv_correct(
        "dd_nofilter_v",
        "SELECT city, SUM(amount) AS s FROM dd_nofilter GROUP BY city",
    );

    crate::drop_reflex_ivm("dd_nofilter_v");
}

#[pg_test]
fn test_directional_multi_fire_sequence() {
    // Mutate the source through OUT→IN, IN→OUT, mixed, and pure-data in
    // sequence on the same IMV. Each fire must converge to the fresh value.
    Spi::run("CREATE TABLE dd_seq (id INT PRIMARY KEY, grp TEXT, qty BIGINT, status TEXT)")
        .expect("create");
    Spi::run(
        "INSERT INTO dd_seq \
         SELECT g, 'g' || ((g - 1) % 5)::TEXT, g, \
         CASE WHEN (g % 2) = 0 THEN 'archived' ELSE 'validated' END \
         FROM generate_series(1, 100) g",
    )
    .expect("seed");

    let result = crate::create_reflex_ivm(
        "dd_seq_v",
        "SELECT grp, SUM(qty) AS s, COUNT(*) AS c \
         FROM dd_seq WHERE status = 'validated' GROUP BY grp",
        None,
        None,
        None,
        None,
    );
    assert!(!result.starts_with("ERROR"), "create: {}", result);

    let fresh = "SELECT grp, SUM(qty) AS s, COUNT(*) AS c \
                 FROM dd_seq WHERE status = 'validated' GROUP BY grp";

    // Phase 1: OUT→IN — flip 10 archived rows to validated.
    Spi::run("UPDATE dd_seq SET status = 'validated' WHERE id IN (2, 4, 6, 8, 10, 12, 14, 16, 18, 20)")
        .expect("p1");
    assert_imv_correct("dd_seq_v", fresh);

    // Phase 2: IN→OUT — flip 10 validated to archived.
    Spi::run("UPDATE dd_seq SET status = 'archived' WHERE id IN (1, 3, 5, 7, 9, 11, 13, 15, 17, 19)")
        .expect("p2");
    assert_imv_correct("dd_seq_v", fresh);

    // Phase 3: pure data UPDATE on rows that remain validated.
    Spi::run("UPDATE dd_seq SET qty = qty * 10 WHERE status = 'validated' AND id <= 30")
        .expect("p3");
    assert_imv_correct("dd_seq_v", fresh);

    // Phase 4: mixed — flip some IN and some OUT in one statement.
    Spi::run(
        "UPDATE dd_seq SET status = CASE WHEN status = 'validated' THEN 'archived' \
         ELSE 'validated' END WHERE id BETWEEN 21 AND 40",
    )
    .expect("p4");
    assert_imv_correct("dd_seq_v", fresh);

    crate::drop_reflex_ivm("dd_seq_v");
}

// =============================================================================
// 1.4.6 — source_join_keys metadata: the AggregationPlan must record
// (intermediate_col, source_col) mappings ONLY for sources that pass both
// safety gates (every JOIN equality maps to a GROUP BY column AND the
// source-side mapping cols cover a UNIQUE key on the source table). Wiring
// for bulk-INSERT/DELETE and Path B dispatch reads this metadata directly.
// =============================================================================

fn read_source_join_keys_from_ref(view_name: &str) -> serde_json::Value {
    let q = format!(
        "SELECT COALESCE(((aggregations::json)->'source_join_keys')::text, 'null') \
         FROM public.__reflex_ivm_reference WHERE name = '{}'",
        view_name.replace('\'', "''")
    );
    let s = Spi::get_one::<String>(&q)
        .expect("read source_join_keys")
        .unwrap_or_else(|| "null".to_string());
    serde_json::from_str(&s).expect("parse source_join_keys json")
}

#[pg_test]
fn test_source_join_keys_populated_for_dim_with_pk_in_group_by() {
    // alp-style shape: fact JOIN dim ON fact.dim_id = dim.id (dim.id is PK,
    // group_by includes dim_id projected from fact). Trigger source = dim →
    // should appear in source_join_keys with mapping (dim_id, id).
    Spi::run(
        "CREATE TABLE sjk_fact (id INT PRIMARY KEY, dim_id INT, val BIGINT); \
         CREATE TABLE sjk_dim (id INT PRIMARY KEY, status TEXT);",
    )
    .expect("create");
    Spi::run(
        "INSERT INTO sjk_fact VALUES (1, 10, 100), (2, 10, 200), (3, 20, 300); \
         INSERT INTO sjk_dim VALUES (10, 'on'), (20, 'off');",
    )
    .expect("seed");
    let result = crate::create_reflex_ivm(
        "sjk_v",
        "SELECT dim_id, SUM(val) AS s FROM sjk_fact \
         INNER JOIN sjk_dim ON sjk_dim.id = sjk_fact.dim_id \
         WHERE sjk_dim.status = 'on' GROUP BY dim_id",
        None,
        None,
        None,
        None,
    );
    assert!(!result.starts_with("ERROR"), "create: {}", result);

    let sjk = read_source_join_keys_from_ref("sjk_v");
    let map = sjk
        .as_object()
        .expect("source_join_keys should be a JSON object");
    assert!(
        map.contains_key("sjk_dim") || map.contains_key("public.sjk_dim"),
        "sjk_dim should be in source_join_keys (PK-in-GROUP-BY shape). Got keys: {:?}",
        map.keys().collect::<Vec<_>>()
    );
    let entries = map
        .get("sjk_dim")
        .or_else(|| map.get("public.sjk_dim"))
        .and_then(|v| v.as_array())
        .expect("sjk_dim mapping should be an array");
    assert!(
        !entries.is_empty(),
        "sjk_dim mapping should not be empty: {:?}",
        entries
    );
    // sjk_fact is the cardinality-driving primary FROM. Its JOIN equality
    // maps to "id" which is NOT a group_by col — so it must NOT appear.
    assert!(
        !map.contains_key("sjk_fact") && !map.contains_key("public.sjk_fact"),
        "sjk_fact (primary FROM) must NOT appear in source_join_keys. Got: {:?}",
        map.keys().collect::<Vec<_>>()
    );

    crate::drop_reflex_ivm("sjk_v");
    Spi::run("DROP TABLE sjk_fact; DROP TABLE sjk_dim;").expect("drop");
}

#[pg_test]
fn test_source_join_keys_empty_for_single_source_imv() {
    // No JOIN ⇒ no source_join_keys entries. Bulk path must stay disabled
    // for these shapes (this is exactly the dd_combo precondition gap that
    // forced the previous revert).
    Spi::run(
        "CREATE TABLE sjk_single (id INT PRIMARY KEY, city TEXT, amount BIGINT, status TEXT); \
         INSERT INTO sjk_single VALUES (1, 'A', 10, 'on'), (2, 'A', 20, 'on');",
    )
    .expect("create+seed");
    let result = crate::create_reflex_ivm(
        "sjk_single_v",
        "SELECT city, SUM(amount) AS s FROM sjk_single WHERE status = 'on' GROUP BY city",
        None,
        None,
        None,
        None,
    );
    assert!(!result.starts_with("ERROR"), "create: {}", result);

    let sjk = read_source_join_keys_from_ref("sjk_single_v");
    if let Some(map) = sjk.as_object() {
        assert!(
            map.is_empty(),
            "single-source IMV must have empty source_join_keys, got: {:?}",
            map
        );
    } // null or empty object are both acceptable.

    crate::drop_reflex_ivm("sjk_single_v");
    Spi::run("DROP TABLE sjk_single").expect("drop");
}

#[pg_test]
fn test_source_join_keys_skipped_when_composite_join_partial_map() {
    // pricing-style shape: composite JOIN on (assortment_id, product_id)
    // but only product_id is in group_by. The PK on the secondary
    // (sjk_partial_dim) is composite (a, b) — only `b` would map. Result:
    // safety gate refuses the mapping → no entry.
    Spi::run(
        "CREATE TABLE sjk_partial_fact (id INT PRIMARY KEY, a INT, b INT, val BIGINT); \
         CREATE TABLE sjk_partial_dim (a INT, b INT, info TEXT, status TEXT, PRIMARY KEY (a, b));",
    )
    .expect("create");
    Spi::run(
        "INSERT INTO sjk_partial_fact VALUES (1, 1, 100, 50), (2, 1, 100, 60); \
         INSERT INTO sjk_partial_dim VALUES (1, 100, 'x', 'on');",
    )
    .expect("seed");

    // Only `b` is projected/group_by'd; `a` is NOT in the SELECT.
    let result = crate::create_reflex_ivm(
        "sjk_partial_v",
        "SELECT sjk_partial_fact.b AS b, SUM(val) AS s FROM sjk_partial_fact \
         INNER JOIN sjk_partial_dim d ON d.a = sjk_partial_fact.a AND d.b = sjk_partial_fact.b \
         WHERE d.status = 'on' GROUP BY sjk_partial_fact.b",
        None,
        None,
        None,
        None,
    );
    assert!(!result.starts_with("ERROR"), "create: {}", result);

    let sjk = read_source_join_keys_from_ref("sjk_partial_v");
    if let Some(map) = sjk.as_object() {
        assert!(
            !map.contains_key("sjk_partial_dim") && !map.contains_key("public.sjk_partial_dim"),
            "partial composite-mapping must NOT register the dim source. Got: {:?}",
            map.keys().collect::<Vec<_>>()
        );
    }

    crate::drop_reflex_ivm("sjk_partial_v");
    Spi::run("DROP TABLE sjk_partial_fact; DROP TABLE sjk_partial_dim").expect("drop");
}

#[pg_test]
fn test_source_join_keys_skipped_when_secondary_lacks_unique_key() {
    // Secondary table has no PK / unique index → multiple secondary rows
    // can share the JOIN col value → bulk path unsafe → no entry recorded.
    Spi::run(
        "CREATE TABLE sjk_nounique_fact (id INT PRIMARY KEY, dim_id INT, val BIGINT); \
         CREATE TABLE sjk_nounique_dim (id INT, status TEXT);", // no PK
    )
    .expect("create");
    Spi::run(
        "INSERT INTO sjk_nounique_fact VALUES (1, 10, 100); \
         INSERT INTO sjk_nounique_dim VALUES (10, 'on'), (10, 'on');", // duplicate id
    )
    .expect("seed");

    let result = crate::create_reflex_ivm(
        "sjk_nounique_v",
        "SELECT dim_id, SUM(val) AS s FROM sjk_nounique_fact \
         INNER JOIN sjk_nounique_dim ON sjk_nounique_dim.id = sjk_nounique_fact.dim_id \
         WHERE sjk_nounique_dim.status = 'on' GROUP BY dim_id",
        None,
        None,
        None,
        None,
    );
    assert!(!result.starts_with("ERROR"), "create: {}", result);

    let sjk = read_source_join_keys_from_ref("sjk_nounique_v");
    if let Some(map) = sjk.as_object() {
        assert!(
            !map.contains_key("sjk_nounique_dim")
                && !map.contains_key("public.sjk_nounique_dim"),
            "dim with no UNIQUE key must NOT appear in source_join_keys. Got: {:?}",
            map.keys().collect::<Vec<_>>()
        );
    }

    crate::drop_reflex_ivm("sjk_nounique_v");
    Spi::run("DROP TABLE sjk_nounique_fact; DROP TABLE sjk_nounique_dim").expect("drop");
}

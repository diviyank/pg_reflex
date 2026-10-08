fn v112_cl_probe(body: &str) -> String {
    Spi::run(&format!(
        "DO $v112cl$ DECLARE r text; BEGIN {body}; \
         PERFORM set_config('v112_cl.out', COALESCE(r, 'OK'), true); \
         EXCEPTION WHEN OTHERS THEN \
         PERFORM set_config('v112_cl.out', 'RAISED: ' || SQLERRM, true); END $v112cl$"
    ))
    .expect("probe");
    Spi::get_one::<String>("SELECT current_setting('v112_cl.out')")
        .expect("probe result")
        .unwrap_or_default()
}

/// The text a one-value SELECT returns, or `RAISED: <message>`.
fn v112_cl_try(select: &str) -> String {
    v112_cl_probe(&format!("EXECUTE $v112q${select}$v112q$ INTO r"))
}

/// `OK`, or `RAISED: <message>`.
fn v112_cl_try_dml(dml: &str) -> String {
    v112_cl_probe(&format!("EXECUTE $v112q${dml}$v112q$"))
}

fn v112_cl_create(imv: &str, sql: &str, key: &str, mode: &str) -> String {
    v112_cl_try(&format!(
        "SELECT create_reflex_ivm('{imv}', $v112sel${sql}$v112sel$, '{key}', 'UNLOGGED', '{mode}')"
    ))
}

fn v112_cl_oracle(imv: &str, fresh_sql: &str) -> String {
    v112_cl_try(&format!(
        "SELECT COUNT(*)::text FROM (\
            (SELECT * FROM {imv} EXCEPT ALL SELECT * FROM ({fresh_sql}) AS __fresh1) \
            UNION ALL \
            (SELECT * FROM ({fresh_sql}) AS __fresh2 EXCEPT ALL SELECT * FROM {imv}) \
         ) __oracle"
    ))
}

/// Creates the IMV, then runs every DML step, comparing the IMV with its query
/// after create and after each step. `"ok"` when every comparison matched.
fn v112_cl_cell(
    imv: &str,
    sql: &str,
    key: &str,
    mode: &str,
    sources: &[&str],
    steps: &[String],
) -> String {
    let created = v112_cl_create(imv, sql, key, mode);
    if created != "CREATE REFLEX INCREMENTAL VIEW" {
        return created;
    }
    let mut outcomes = vec![format!("create={}", v112_cl_oracle(imv, sql))];
    for (position, dml) in steps.iter().enumerate() {
        let ran = v112_cl_try_dml(dml);
        if ran != "OK" {
            outcomes.push(format!("step{position}={ran}"));
            break;
        }
        let flush_failure = sources
            .iter()
            .filter(|_| mode == "DEFERRED")
            .map(|source| v112_cl_try(&format!("SELECT reflex_flush_deferred('{source}')")))
            .find(|flushed| flushed.starts_with("RAISED") || flushed.starts_with("ERROR"));
        if let Some(failure) = flush_failure {
            outcomes.push(format!("step{position}={failure}"));
            break;
        }
        outcomes.push(format!("step{position}={}", v112_cl_oracle(imv, sql)));
    }
    if outcomes.iter().all(|o| o.ends_with("=0")) {
        "ok".to_string()
    } else {
        outcomes.join(" ")
    }
}

const V112_CL_GV_STEPS: [&str; 5] = [
    "INSERT INTO {src}(g, v) VALUES ('b',1),('d',5),('a',NULL)",
    "UPDATE {src} SET v = v + 1 WHERE g = 'a'",
    "DELETE FROM {src} WHERE g = 'b'",
    "DELETE FROM {src}",
    "INSERT INTO {src}(g, v) VALUES ('z',9),('z',1)",
];

fn v112_cl_steps(steps: &[&str], src: &str) -> Vec<String> {
    steps.iter().map(|step| step.replace("{src}", src)).collect()
}

/// Outcome of `select` (`{src}` names the source) as IMV `v112_cl_<tag>` over a
/// fresh `(id SERIAL PRIMARY KEY, g TEXT, v INT)` source, through the g/v steps.
fn v112_cl_gv(tag: &str, select: &str, key: &str, mode: &str) -> String {
    let src = format!("v112_cl_{tag}_src");
    Spi::run(&format!(
        "CREATE TABLE {src}(id SERIAL PRIMARY KEY, g TEXT, v INT); \
         INSERT INTO {src}(g, v) VALUES ('a',1),('a',1),('b',2),('c',NULL)"
    ))
    .expect("source");
    v112_cl_cell(
        &format!("v112_cl_{tag}"),
        &select.replace("{src}", &src),
        key,
        mode,
        &[&src],
        &v112_cl_steps(&V112_CL_GV_STEPS, &src),
    )
}

fn v112_cl_registry(tag: &str, expression: &str) -> String {
    v112_cl_try(&format!(
        "SELECT ({expression})::text FROM public.__reflex_ivm_reference WHERE name = 'v112_cl_{tag}'"
    ))
}

fn v112_cl_unique_key(tag: &str) -> String {
    v112_cl_registry(tag, "array_to_string(unique_columns, ',')")
}

fn v112_cl_assert_rejected(tag: &str, outcome: &str, fragment: &str) {
    assert!(
        outcome.starts_with("ERROR: [reflex-unsupported]") && outcome.contains(fragment),
        "{tag}: expected a tagged rejection holding '{fragment}', got: {outcome}"
    );
    let left_behind = v112_cl_try(&format!(
        "SELECT (to_regclass('v112_cl_{tag}') IS NOT NULL OR EXISTS (SELECT 1 FROM \
         public.__reflex_ivm_reference WHERE name LIKE 'v112_cl_{tag}%'))::text"
    ));
    assert_eq!(left_behind, "false", "{tag}: the rejected create left objects");
}

// ---- passthrough key detection ----

#[pg_test]
fn v112_cl_alias_named_like_the_pk_is_not_the_key() {
    for (position, (select, mode)) in [
        ("SELECT v AS id, g FROM {src} WHERE v IS NOT NULL", "IMMEDIATE"),
        ("SELECT v AS id, g FROM {src} WHERE v IS NOT NULL", "DEFERRED"),
        ("SELECT v + 1 AS id FROM {src}", "IMMEDIATE"),
        ("SELECT g AS id, id AS v FROM {src}", "IMMEDIATE"),
    ]
    .iter()
    .enumerate()
    {
        let tag = format!("alias_key{position}");
        assert_eq!(v112_cl_gv(&tag, select, "", mode), "ok", "{select}");
        assert_eq!(v112_cl_unique_key(&tag), "", "{select}");
    }
}

#[pg_test]
fn v112_cl_bare_pk_reference_is_still_the_key() {
    for (position, select) in [
        "SELECT id, g, v FROM {src}",
        "SELECT {src}.id, g FROM {src}",
        "SELECT id AS id, v FROM {src}",
        "SELECT t.id, t.v FROM {src} t",
        "SELECT g, ID FROM {src}",
    ]
    .iter()
    .enumerate()
    {
        let tag = format!("bare_key{position}");
        assert_eq!(v112_cl_gv(&tag, select, "", "IMMEDIATE"), "ok", "{select}");
        assert_eq!(v112_cl_unique_key(&tag), "id", "{select}");
    }
}

// ---- an aggregate is never a keyed row-level passthrough ----

#[pg_test]
fn v112_cl_ungrouped_count_distinct_is_refused_a_key() {
    let keyed = v112_cl_gv("cd_keyed", "SELECT COUNT(DISTINCT v) AS m FROM {src}", "m", "IMMEDIATE");
    v112_cl_assert_rejected("cd_keyed", &keyed, "COUNT(DISTINCT");
    for (tag, mode) in [("cd_keyless_i", "IMMEDIATE"), ("cd_keyless_d", "DEFERRED")] {
        assert_eq!(
            v112_cl_gv(tag, "SELECT COUNT(DISTINCT v) AS m FROM {src}", "", mode),
            "ok"
        );
    }
}

#[pg_test]
fn v112_cl_ungrouped_aggregate_under_a_predicate_is_refused_a_key() {
    for (position, expression) in ["SUM(v) IS NULL", "COUNT(DISTINCT v) IS NULL", "MAX(v) IN (1, 2)"]
        .iter()
        .enumerate()
    {
        let select = format!("SELECT {expression} AS e FROM {{src}}");
        let keyed_tag = format!("wrap_keyed{position}");
        let keyed = v112_cl_gv(&keyed_tag, &select, "e", "IMMEDIATE");
        v112_cl_assert_rejected(&keyed_tag, &keyed, "cannot be maintained by a unique key");
        for (mode_tag, mode) in [("i", "IMMEDIATE"), ("d", "DEFERRED")] {
            let tag = format!("wrap_keyless{position}{mode_tag}");
            assert_eq!(v112_cl_gv(&tag, &select, "", mode), "ok", "{select}");
        }
    }
}

// ---- an aggregate anywhere in a select item is seen as one ----

const V112_CL_WRAPPERS: [&str; 14] = [
    "SUM(v) IS NULL",
    "SUM(v) IS NOT NULL",
    "SUM(v) BETWEEN 1 AND 2",
    "SUM(v) IN (1, 2)",
    "ARRAY[SUM(v), COUNT(*)]",
    "SUM(v)::text LIKE '1%'",
    "SUM(v) IS DISTINCT FROM 2",
    "CEIL(AVG(v))",
    "COALESCE(SUM(v), 0) = ANY(ARRAY[1, 2])",
    "MAX(v) NOT IN (2, 5) AND MIN(v) IS NOT NULL",
    "SUM(v) + (COUNT(*) IS NULL)::int",
    "COUNT(DISTINCT v) IS NULL",
    "COUNT(DISTINCT v) BETWEEN 1 AND 2",
    "COUNT(DISTINCT v) IN (1, 2)",
];

#[pg_test]
fn v112_cl_aggregate_under_a_predicate_beside_group_by_is_rejected() {
    for (position, expression) in V112_CL_WRAPPERS.iter().enumerate() {
        let tag = format!("wrap_g{position}");
        let select = format!("SELECT g, {expression} AS c FROM {{src}} GROUP BY g");
        let outcome = v112_cl_gv(&tag, &select, "", "IMMEDIATE");
        v112_cl_assert_rejected(&tag, &outcome, "'c' holds an aggregate inside an expression");
    }
}

#[pg_test]
fn v112_cl_aggregate_under_a_predicate_without_group_by_is_recomputed() {
    for (position, expression) in V112_CL_WRAPPERS.iter().enumerate() {
        let tag = format!("wrap_u{position}");
        let select = format!("SELECT {expression} AS c FROM {{src}}");
        assert_eq!(v112_cl_gv(&tag, &select, "", "IMMEDIATE"), "ok", "{select}");
        assert_eq!(v112_cl_unique_key(&tag), "", "{select}");
    }
}

#[pg_test]
fn v112_cl_aggregate_under_a_predicate_cte_remedy_is_maintained() {
    for (tag, select) in [
        (
            "wrap_cte_sum",
            "WITH a AS (SELECT g, SUM(v) AS s FROM {src} GROUP BY g) SELECT g, s IS NULL AS c FROM a",
        ),
        (
            "wrap_cte_array",
            "WITH a AS (SELECT g, SUM(v) AS s, COUNT(*) AS n FROM {src} GROUP BY g) \
             SELECT g, ARRAY[s, n] AS c FROM a",
        ),
        (
            "wrap_cte_cd",
            "WITH d AS (SELECT g, COUNT(DISTINCT v) AS n FROM {src} GROUP BY g) \
             SELECT g, n BETWEEN 1 AND 2 AS c FROM d",
        ),
    ] {
        assert_eq!(v112_cl_gv(tag, select, "", "IMMEDIATE"), "ok", "{select}");
    }
}

#[pg_test]
fn v112_cl_decomposed_aggregate_expression_is_still_maintained() {
    v112_cl_assert_aggregate_cells(
        "derived",
        &[
            "SUM(v)",
            "COALESCE(SUM(v), 0) + 1",
            "CASE WHEN COUNT(*) > 1 THEN (SUM(v))::numeric / COUNT(*) ELSE -MAX(v) END",
            "NOT (COUNT(v) > 1 OR MIN(v) = 2)",
        ],
        "IMMEDIATE",
    );
}

fn v112_cl_assert_aggregate_cells(prefix: &str, expressions: &[&str], mode: &str) {
    for (position, expression) in expressions.iter().enumerate() {
        for (shape, select) in [
            ("g", format!("SELECT g, {expression} AS c FROM {{src}} GROUP BY g")),
            ("u", format!("SELECT {expression} AS c FROM {{src}}")),
        ] {
            let tag = format!("{prefix}{position}{shape}");
            assert_eq!(v112_cl_gv(&tag, &select, "", mode), "ok", "{select}");
            assert_eq!(
                v112_cl_registry(&tag, "aggregations->>'is_passthrough'"),
                "false",
                "{select} is not maintained as an aggregate"
            );
        }
    }
}

// ---- spellings of a built-in aggregate ----

#[pg_test]
fn v112_cl_qualified_or_quoted_builtin_aggregate_is_maintained() {
    v112_cl_assert_aggregate_cells(
        "spell",
        &[
            "pg_catalog.sum(v)",
            "\"sum\"(v)",
            "PG_CATALOG.SUM(v)",
            "\"pg_catalog\".\"max\"(v)",
            "pg_catalog.avg(v) + \"count\"(*)",
        ],
        "IMMEDIATE",
    );
    assert_eq!(
        v112_cl_gv(
            "spell_cd_g",
            "SELECT g, pg_catalog.count(DISTINCT v) AS c FROM {src} GROUP BY g",
            "",
            "IMMEDIATE"
        ),
        "ok"
    );
    assert_eq!(v112_cl_registry("spell_cd_g", "aggregations->>'is_passthrough'"), "false");
    assert_eq!(
        v112_cl_gv("spell_cd_u", "SELECT pg_catalog.count(DISTINCT v) AS c FROM {src}", "", "IMMEDIATE"),
        "ok"
    );
}

#[pg_test]
fn v112_cl_aggregate_that_is_not_the_builtin_is_rejected_or_keyless() {
    Spi::run(
        "CREATE SCHEMA v112_cl_other; \
         CREATE AGGREGATE v112_cl_other.max(int) (SFUNC = int4larger, STYPE = int); \
         CREATE AGGREGATE v112_cl_other.v112_cl_mymax(int) (SFUNC = int4larger, STYPE = int)",
    )
    .expect("aggregates");
    for (tag, select, key, fragment) in [
        (
            "other_max_g",
            "SELECT g, v112_cl_other.max(v) AS c FROM {src} GROUP BY g",
            "",
            "Unsupported aggregate(s): V112_CL_OTHER.MAX",
        ),
        (
            "other_max_keyed",
            "SELECT v112_cl_other.max(v) AS c FROM {src}",
            "c",
            "Unsupported aggregate(s): V112_CL_OTHER.MAX",
        ),
        (
            "other_mymax_g",
            "SELECT g, v112_cl_other.v112_cl_mymax(v) AS c FROM {src} GROUP BY g",
            "",
            "Unsupported aggregate(s): V112_CL_MYMAX",
        ),
        (
            "upper_sum_g",
            "SELECT g, \"SUM\"(v) AS c FROM {src} GROUP BY g",
            "",
            "Unsupported aggregate(s): \"SUM\"",
        ),
    ] {
        let outcome = v112_cl_gv(tag, select, key, "IMMEDIATE");
        v112_cl_assert_rejected(tag, &outcome, fragment);
    }
    assert_eq!(
        v112_cl_gv("other_max_u", "SELECT v112_cl_other.max(v) AS c FROM {src}", "", "IMMEDIATE"),
        "ok"
    );
    assert_eq!(v112_cl_unique_key("other_max_u"), "");
}

// ---- a constant select item beside GROUP BY or DISTINCT ----

#[pg_test]
fn v112_cl_bare_constant_beside_group_by_is_rejected() {
    for (position, constant) in ["'plain'", "NULL", "TRUE", "3.5", "-1", "('plain')"].iter().enumerate() {
        for (shape, select) in [
            ("g", format!("SELECT g, {constant} AS d, SUM(v) AS s FROM {{src}} GROUP BY g")),
            ("d", format!("SELECT DISTINCT g, {constant} AS d FROM {{src}}")),
        ] {
            let tag = format!("const{position}{shape}");
            let outcome = v112_cl_gv(&tag, &select, "", "IMMEDIATE");
            v112_cl_assert_rejected(&tag, &outcome, "cast it");
        }
    }
}

#[pg_test]
fn v112_cl_cast_constant_beside_group_by_is_maintained() {
    for (position, constant) in [
        "'plain'::text",
        "NULL::text",
        "TRUE::boolean",
        "3.5::numeric",
        "(-1)::int",
        "2",
        "1 + 1",
        "upper('x')",
    ]
    .iter()
    .enumerate()
    {
        for (shape, select) in [
            ("g", format!("SELECT g, {constant} AS d, SUM(v) AS s FROM {{src}} GROUP BY g")),
            ("d", format!("SELECT DISTINCT g, {constant} AS d FROM {{src}}")),
        ] {
            let tag = format!("cast_const{position}{shape}");
            assert_eq!(v112_cl_gv(&tag, &select, "", "IMMEDIATE"), "ok", "{select}");
        }
    }
    assert_eq!(
        v112_cl_gv("const_passthrough", "SELECT id, 'plain' AS d, NULL AS n, g FROM {src}", "", "IMMEDIATE"),
        "ok"
    );
    assert_eq!(v112_cl_unique_key("const_passthrough"), "id");
}

// ---- output and group-key names ----

const V112_CL_JOIN_STEPS: [&str; 6] = [
    "INSERT INTO {b}(a_id, label) VALUES (2,'q'),(1,'p')",
    "INSERT INTO {a}(label) VALUES ('x')",
    "UPDATE {a} SET label = 'z' WHERE id = 1",
    "UPDATE {b} SET a_id = 3 WHERE label = 'q'",
    "DELETE FROM {b} WHERE label = 'p'",
    "DELETE FROM {a}",
];

fn v112_cl_join(tag: &str, select: &str) -> String {
    let a = format!("v112_cl_{tag}_a");
    let b = format!("v112_cl_{tag}_b");
    Spi::run(&format!(
        "CREATE TABLE {a}(id SERIAL PRIMARY KEY, label TEXT); \
         CREATE TABLE {b}(id SERIAL PRIMARY KEY, a_id INT, label TEXT); \
         INSERT INTO {a}(label) VALUES ('x'),('y'); \
         INSERT INTO {b}(a_id, label) VALUES (1,'p'),(1,'q'),(2,'p'),(1,'p')"
    ))
    .expect("sources");
    let named = |text: &str| text.replace("{a}", &a).replace("{b}", &b);
    let steps: Vec<String> = V112_CL_JOIN_STEPS.iter().map(|step| named(step)).collect();
    v112_cl_cell(&format!("v112_cl_{tag}"), &named(select), "", "IMMEDIATE", &[&a, &b], &steps)
}

#[pg_test]
fn v112_cl_group_keys_sharing_a_column_name_are_rejected() {
    for (tag, select) in [
        (
            "dupkey_bare",
            "SELECT a.label, b.label, COUNT(*) AS c FROM {a} a JOIN {b} b ON b.a_id = a.id \
             GROUP BY a.label, b.label",
        ),
        (
            "dupkey_aliased",
            "SELECT a.label AS la, b.label AS lb, COUNT(*) AS c FROM {a} a JOIN {b} b ON b.a_id = a.id \
             GROUP BY a.label, b.label",
        ),
        (
            "dupkey_distinct",
            "SELECT DISTINCT a.label AS la, b.label AS lb FROM {a} a JOIN {b} b ON b.a_id = a.id",
        ),
    ] {
        let outcome = v112_cl_join(tag, select);
        v112_cl_assert_rejected(tag, &outcome, "share the column name 'label'");
    }
    let twice = v112_cl_gv(
        "dupkey_twice",
        "SELECT g, COUNT(*) AS c FROM {src} GROUP BY g, {src}.g",
        "",
        "IMMEDIATE",
    );
    v112_cl_assert_rejected("dupkey_twice", &twice, "share the column name 'g'");
}

#[pg_test]
fn v112_cl_group_keys_renamed_in_a_cte_are_maintained() {
    assert_eq!(
        v112_cl_join(
            "dupkey_cte",
            "WITH j AS (SELECT a.label AS la, b.label AS lb FROM {a} a JOIN {b} b ON b.a_id = a.id) \
             SELECT la, lb, COUNT(*) AS c FROM j GROUP BY la, lb",
        ),
        "ok"
    );
}

#[pg_test]
fn v112_cl_output_columns_sharing_a_name_are_rejected() {
    for (tag, select) in [
        ("dupout_bare", "SELECT a.label, b.label FROM {a} a JOIN {b} b ON b.a_id = a.id"),
        ("dupout_alias", "SELECT a.label AS x, b.label AS x FROM {a} a JOIN {b} b ON b.a_id = a.id"),
        ("dupout_mixed", "SELECT a.label, b.id AS \"label\" FROM {a} a JOIN {b} b ON b.a_id = a.id"),
    ] {
        let outcome = v112_cl_join(tag, select);
        v112_cl_assert_rejected(tag, &outcome, "distinct alias");
    }
    assert_eq!(
        v112_cl_join(
            "dupout_fixed",
            "SELECT a.label AS la, b.label AS lb FROM {a} a JOIN {b} b ON b.a_id = a.id",
        ),
        "ok"
    );
    assert_eq!(
        v112_cl_join(
            "dupout_case",
            "SELECT a.label AS \"Label\", b.label FROM {a} a JOIN {b} b ON b.a_id = a.id",
        ),
        "ok"
    );
}

// ---- set-returning functions ----

const V112_CL_ARR_STEPS: [&str; 5] = [
    "INSERT INTO {src}(g, arr) VALUES ('d','{7,8,9}'),('e',NULL)",
    "UPDATE {src} SET arr = arr || 5 WHERE g = 'a'",
    "DELETE FROM {src} WHERE g = 'b'",
    "DELETE FROM {src}",
    "INSERT INTO {src}(g, arr) VALUES ('z','{1,1}')",
];

fn v112_cl_arr(tag: &str, select: &str, key: &str, mode: &str) -> String {
    let src = format!("v112_cl_{tag}_src");
    Spi::run(&format!(
        "CREATE TABLE {src}(id SERIAL PRIMARY KEY, g TEXT, arr INT[]); \
         INSERT INTO {src}(g, arr) VALUES ('a','{{1,2}}'),('b','{{3}}'),('c','{{}}')"
    ))
    .expect("source");
    v112_cl_cell(
        &format!("v112_cl_{tag}"),
        &select.replace("{src}", &src),
        key,
        mode,
        &[&src],
        &v112_cl_steps(&V112_CL_ARR_STEPS, &src),
    )
}

#[pg_test]
fn v112_cl_function_in_from_is_rejected() {
    for (tag, select) in [
        ("srf_from0", "SELECT id, g, s.i FROM {src}, generate_series(1, 2) AS s(i)"),
        ("srf_from1", "SELECT g, COUNT(*) AS c FROM {src}, generate_series(1, 2) AS s(i) GROUP BY g"),
        ("srf_from2", "SELECT g, e FROM {src}, unnest(arr) AS e"),
        ("srf_from3", "SELECT g, e FROM {src} CROSS JOIN LATERAL unnest(arr) AS e"),
        ("srf_from4", "SELECT g, e FROM {src} JOIN LATERAL pg_catalog.unnest(arr) AS e ON true"),
        ("srf_from6", "SELECT id, g FROM {src} WHERE id IN (SELECT i FROM generate_series(1, 3) AS i)"),
    ] {
        let outcome = v112_cl_arr(tag, select, "", "IMMEDIATE");
        v112_cl_assert_rejected(tag, &outcome, "function in FROM");
    }
}

#[pg_test]
fn v112_cl_function_in_from_remedies_are_maintained() {
    assert_eq!(
        v112_cl_arr("srf_remedy_select", "SELECT g, unnest(arr) AS e FROM {src}", "", "IMMEDIATE"),
        "ok"
    );
    Spi::run(
        "CREATE TABLE v112_cl_series(i INT PRIMARY KEY); INSERT INTO v112_cl_series VALUES (1),(2)",
    )
    .expect("series");
    assert_eq!(
        v112_cl_arr(
            "srf_remedy_table",
            "SELECT t.id, t.g, s.i FROM {src} t CROSS JOIN v112_cl_series s",
            "",
            "IMMEDIATE"
        ),
        "ok"
    );
    assert_eq!(v112_cl_try_dml("INSERT INTO v112_cl_series VALUES (3)"), "OK");
    assert_eq!(v112_cl_try_dml("INSERT INTO v112_cl_srf_remedy_table_src(g, arr) VALUES ('k','{1}')"), "OK");
    assert_eq!(v112_cl_try_dml("DELETE FROM v112_cl_series WHERE i = 1"), "OK");
    assert_eq!(
        v112_cl_oracle(
            "v112_cl_srf_remedy_table",
            "SELECT t.id, t.g, s.i FROM v112_cl_srf_remedy_table_src t CROSS JOIN v112_cl_series s"
        ),
        "0"
    );
}

#[pg_test]
fn v112_cl_function_below_the_top_level_from_is_still_accepted() {
    for (tag, select) in [
        ("srf_subquery", "SELECT id, g FROM {src} WHERE id IN (SELECT e FROM unnest(ARRAY[1, 3, 4]) AS e)"),
        ("srf_derived", "SELECT d.g, d.e FROM (SELECT g, e FROM {src}, unnest(arr) AS e) d"),
    ] {
        assert_eq!(v112_cl_arr(tag, select, "", "IMMEDIATE"), "ok", "{select}");
    }
}

#[pg_test]
fn v112_cl_set_returning_select_item_is_maintained_without_a_key() {
    for (tag, select, mode) in [
        ("srf_sel0", "SELECT id, unnest(arr) FROM {src}", "IMMEDIATE"),
        ("srf_sel1", "SELECT id, unnest(arr) AS e FROM {src}", "DEFERRED"),
        ("srf_sel2", "SELECT id, generate_series(1, 2) AS i FROM {src}", "IMMEDIATE"),
        ("srf_sel3", "SELECT id, g, pg_catalog.unnest(arr) + 1 AS e FROM {src}", "IMMEDIATE"),
    ] {
        assert_eq!(v112_cl_arr(tag, select, "", mode), "ok", "{select}");
        assert_eq!(v112_cl_unique_key(tag), "", "{select}");
    }
}

#[pg_test]
fn v112_cl_set_returning_select_item_is_refused_a_key() {
    let outcome = v112_cl_arr("srf_keyed", "SELECT id, unnest(arr) AS e FROM {src}", "id,e", "IMMEDIATE");
    v112_cl_assert_rejected("srf_keyed", &outcome, "set-returning function");
}

#[pg_test]
fn v112_cl_set_returning_function_elsewhere_keeps_the_key() {
    for (tag, select) in [
        (
            "srf_scalar",
            "SELECT id, g, (SELECT max(e) FROM unnest(ARRAY[1, 2, 3]) AS e) AS m FROM {src}",
        ),
        ("srf_in_subquery", "SELECT id, g FROM {src} WHERE id IN (SELECT unnest(ARRAY[1, 3, 4]))"),
        ("srf_none", "SELECT id, g, array_length(arr, 1) AS n FROM {src}"),
    ] {
        assert_eq!(v112_cl_arr(tag, select, "", "IMMEDIATE"), "ok", "{select}");
        assert_eq!(v112_cl_unique_key(tag), "id", "{select}");
    }
    assert_eq!(
        v112_cl_arr(
            "srf_grouped",
            "SELECT unnest(arr) AS e, COUNT(*) AS c FROM {src} GROUP BY unnest(arr)",
            "",
            "IMMEDIATE"
        ),
        "ok"
    );
}


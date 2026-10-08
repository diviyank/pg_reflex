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


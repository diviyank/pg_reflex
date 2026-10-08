fn v112_ty_probe(body: &str) -> String {
    Spi::run(&format!(
        "DO $v112ty$ DECLARE r text; BEGIN {body}; \
         PERFORM set_config('v112_ty.out', COALESCE(r, 'OK'), true); \
         EXCEPTION WHEN OTHERS THEN \
         PERFORM set_config('v112_ty.out', 'RAISED: ' || SQLERRM, true); END $v112ty$"
    ))
    .expect("probe");
    Spi::get_one::<String>("SELECT current_setting('v112_ty.out')")
        .expect("probe result")
        .unwrap_or_default()
}

/// The text a one-value SELECT returns, or `RAISED: <message>`.
fn v112_ty_try(select: &str) -> String {
    v112_ty_probe(&format!("EXECUTE $v112q${select}$v112q$ INTO r"))
}

/// `OK`, or `RAISED: <message>`.
fn v112_ty_try_dml(dml: &str) -> String {
    v112_ty_probe(&format!("EXECUTE $v112q${dml}$v112q$"))
}

fn v112_ty_oracle(imv: &str, fresh_sql: &str) -> String {
    v112_ty_try(&format!(
        "SELECT COUNT(*)::text FROM (\
            (SELECT * FROM {imv} EXCEPT ALL SELECT * FROM ({fresh_sql}) AS __fresh1) \
            UNION ALL \
            (SELECT * FROM ({fresh_sql}) AS __fresh2 EXCEPT ALL SELECT * FROM {imv}) \
         ) __oracle"
    ))
}

fn v112_ty_column_types(relation: &str) -> String {
    v112_ty_try(&format!(
        "SELECT string_agg(format_type(atttypid, atttypmod), ', ' ORDER BY attnum) \
         FROM pg_attribute WHERE attrelid = '{relation}'::regclass AND attnum > 0 AND NOT attisdropped"
    ))
}

/// `same`, or the column types of the IMV and of the query when they differ.
fn v112_ty_types_against_query(imv: &str, fresh_sql: &str) -> String {
    let query_view = format!("{imv}_typed_q");
    Spi::run(&format!("CREATE TEMP VIEW {query_view} AS {fresh_sql}")).expect("typed view");
    let imv_types = v112_ty_column_types(imv);
    let query_types = v112_ty_column_types(&query_view);
    if imv_types == query_types {
        "same".to_string()
    } else {
        format!("imv ({imv_types}) query ({query_types})")
    }
}

struct V112TyFixture {
    columns: &'static str,
    insert_columns: &'static str,
    seed: &'static str,
    steps: &'static [(&'static str, &'static str)],
}

/// The REAL and DOUBLE PRECISION values of group `a` sum to a different
/// floating-point result in each row order: 2^24 absorbs a following 1 in
/// single precision, as 1e16 does in double precision.
const V112_TY_SEED: &str = "\
    ('a',1,10,1,16777216,1e16,1.25,'x','ab',true,'1 day','12:00'),\
    ('a',2,20,2,1,1,2.50,'y','cd',false,'2 days','09:30'),\
    ('a',3,30,3,1,1,3.75,'yy','ef',false,'10 days','23:15'),\
    ('a',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL),\
    ('b',5,50,5,0.1,0.1,6.25,'z','gh',false,'3 hours','01:00'),\
    ('n',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL),\
    ('z',0,0,0,0.3,0.3,0,'k','ij',NULL,'0 days','00:00'),\
    ('m',-4,-40,-4,-0.7,-0.7,-5.00,'q','kl',false,'-1 day','18:45')";

const V112_TY_FIXTURE: V112TyFixture = V112TyFixture {
    columns: "g TEXT, v INT, q BIGINT, s SMALLINT, rl REAL, f DOUBLE PRECISION, n NUMERIC, \
              t TEXT, vc VARCHAR(10), b BOOLEAN, iv INTERVAL, tm TIME",
    insert_columns: "g, v, q, s, rl, f, n, t, vc, b, iv, tm",
    seed: V112_TY_SEED,
    steps: &[
        (
            "ins",
            "INSERT INTO {src}(g, v, q, s, rl, f, n, t, vc, b, iv, tm) VALUES \
             ('a',7,70,7,-16777216,-1e16,8.75,'w','zz',true,'20 days','23:59'),\
             ('c',3,30,3,0.2,0.2,3.75,'c','mn',false,'5 days','05:00'),\
             ('b',-1,-10,-1,0.7,0.7,-1.25,'a','aa',NULL,'1 hour','00:30')",
        ),
        (
            "upd",
            "UPDATE {src} SET v = v + 10, q = q + 100, s = s + 1, rl = rl * 3, f = f * 3, n = n + 10, \
             t = t || 'u', vc = vc || 'u', b = NOT b, iv = iv * 2, tm = tm + interval '1 hour' \
             WHERE g = 'b'",
        ),
        ("move", "UPDATE {src} SET g = 'm' WHERE g = 'z'"),
        (
            "del",
            "DELETE FROM {src} WHERE g = 'c' OR (g = 'a' AND v IN (1, 7))",
        ),
        ("nullonly", "DELETE FROM {src} WHERE v IS NOT NULL"),
        ("empty", "DELETE FROM {src}"),
        (
            "refill",
            "INSERT INTO {src}(g, v, q, s, rl, f, n, t, vc, b, iv, tm) VALUES \
             ('a',4,40,4,0.5,0.5,1.5,'r','rr',true,'4 days','04:00'),\
             ('a',9,90,9,2.5,2.5,9.5,'s','ss',false,'9 days','09:00')",
        ),
    ],
};

/// Columns whose aggregate arguments share one sanitised spelling: `"V"` beside
/// `v`, a column named `star` beside `COUNT(*)`, `v_topk` beside the top-K
/// companion of `MIN(v)`, and a long multibyte name.
const V112_TY_NAMES_FIXTURE: V112TyFixture = V112TyFixture {
    columns: "g TEXT, v INT, \"V\" INT, star INT, v_topk INT, b BOOLEAN, \
              \"aééééééééééééééééééééééééé\" INT",
    insert_columns: "g, v, \"V\", star, v_topk, b, \"aééééééééééééééééééééééééé\"",
    seed: "('a',1,100,NULL,7,true,11),('a',2,200,5,3,false,12),('a',3,NULL,6,9,NULL,13),\
           ('b',5,500,NULL,1,false,NULL),('n',NULL,NULL,NULL,NULL,NULL,NULL),('z',0,-7,1,0,true,14),\
           ('k',8,NULL,2,2,true,3),('k',6,NULL,NULL,5,false,4)",
    steps: &[
        (
            "ins",
            "INSERT INTO {src}(g, v, \"V\", star, v_topk, b, \"aééééééééééééééééééééééééé\") VALUES \
             ('a',7,700,NULL,-2,true,15),('c',3,300,2,4,false,16),('b',-1,NULL,3,8,NULL,17)",
        ),
        (
            "upd",
            "UPDATE {src} SET v = v + 10, \"V\" = \"V\" - 3, star = COALESCE(star, 0) + 1, \
             v_topk = v_topk * 2, b = NOT b WHERE g IN ('a', 'b', 'k')",
        ),
        ("move", "UPDATE {src} SET g = 'b' WHERE g = 'z'"),
        (
            "del",
            "DELETE FROM {src} WHERE g = 'c' OR (g = 'a' AND v IN (11, 17))",
        ),
        ("empty", "DELETE FROM {src}"),
        (
            "refill",
            "INSERT INTO {src}(g, v, \"V\", star, v_topk, b, \"aééééééééééééééééééééééééé\") VALUES \
             ('a',4,40,4,1,true,1),('a',9,NULL,NULL,5,false,2)",
        ),
    ],
};

/// Creates the IMV over a fresh source, then runs every DML step of the
/// fixture, comparing the IMV with the query after each, and finally compares
/// the column types. Returns "ok" when everything matched, otherwise the
/// outcome class and the detail.
fn v112_ty_cell_on(
    fixture: &V112TyFixture,
    tag: &str,
    select_template: &str,
    deferred: bool,
) -> String {
    let src = format!("v112_ty_{tag}_src");
    let imv = format!("v112_ty_{tag}");
    Spi::run(&format!(
        "CREATE TABLE {src}(id SERIAL PRIMARY KEY, {})",
        fixture.columns
    ))
    .expect("create source");
    Spi::run(&format!(
        "INSERT INTO {src}({}) VALUES {}",
        fixture.insert_columns, fixture.seed
    ))
    .expect("seed");
    let fresh = select_template.replace("{src}", &src);
    let mode = if deferred { "DEFERRED" } else { "IMMEDIATE" };
    let created = v112_ty_try(&format!(
        "SELECT create_reflex_ivm('{imv}', $v112sel${fresh}$v112sel$, NULL, 'UNLOGGED', '{mode}')"
    ));
    if created.starts_with("ERROR: [reflex-unsupported]") {
        return format!("REJECTED {created}");
    }
    if created != "CREATE REFLEX INCREMENTAL VIEW" {
        return format!("RAW-ERROR-AT-CREATE {created}");
    }
    let mut steps = vec![("create".to_string(), v112_ty_oracle(&imv, &fresh))];
    for (label, dml) in fixture.steps {
        let ran = v112_ty_try_dml(&dml.replace("{src}", &src));
        if ran != "OK" {
            steps.push((label.to_string(), ran));
            break;
        }
        if deferred {
            let flushed = v112_ty_try(&format!("SELECT reflex_flush_deferred('{src}')"));
            if flushed.starts_with("RAISED") || flushed.starts_with("ERROR") {
                steps.push((label.to_string(), flushed));
                break;
            }
        }
        steps.push((label.to_string(), v112_ty_oracle(&imv, &fresh)));
    }
    let detail: Vec<String> = steps.iter().map(|(l, o)| format!("{l}={o}")).collect();
    if !steps.iter().all(|(_, outcome)| outcome.parse::<i64>().is_ok()) {
        return format!("RAW-ERROR {}", detail.join(" "));
    }
    if !steps.iter().all(|(_, outcome)| outcome == "0") {
        return format!("WRONG-DATA {}", detail.join(" "));
    }
    match v112_ty_types_against_query(&imv, &fresh).as_str() {
        "same" => "ok".to_string(),
        difference => format!("WRONG-TYPE {difference}"),
    }
}

macro_rules! v112_ty_cells {
    ($fixture:expr, $deferred:expr; $($name:ident => $select:expr,)+) => {
        $(
            #[pg_test]
            fn $name() {
                let tag = stringify!($name).trim_start_matches("v112_types_");
                assert_eq!(
                    v112_ty_cell_on(&$fixture, tag, $select, $deferred),
                    "ok",
                    "{}",
                    $select
                );
            }
        )+
    };
}

/// Names of the relations and registry rows a create of `imv` leaves behind.
fn v112_ty_leftovers(imv: &str) -> String {
    v112_ty_try(&format!(
        "SELECT COALESCE(string_agg(leftover, ', ' ORDER BY leftover), 'none') FROM (\
            SELECT relname::text AS leftover FROM pg_class \
            WHERE relname LIKE '%{imv}' AND relname NOT LIKE '%{imv}_src%' AND relkind IN ('r', 'v', 'p') \
            UNION ALL \
            SELECT 'registry:' || name FROM public.__reflex_ivm_reference WHERE name LIKE '%{imv}' \
            UNION ALL \
            SELECT 'trigger:' || tgname::text FROM pg_trigger \
            WHERE tgrelid = to_regclass('{imv}_src') AND NOT tgisinternal \
         ) leftovers"
    ))
}

// ---------------------------------------------------------------------------
// SUM / AVG over a REAL argument
// ---------------------------------------------------------------------------

/// The create outcome of `select` over a fresh copy of the typed fixture.
fn v112_ty_create_outcome(tag: &str, select_template: &str) -> (String, String) {
    let src = format!("v112_ty_{tag}_src");
    let imv = format!("v112_ty_{tag}");
    Spi::run(&format!(
        "CREATE TABLE {src}(id SERIAL PRIMARY KEY, {})",
        V112_TY_FIXTURE.columns
    ))
    .expect("create source");
    Spi::run(&format!(
        "INSERT INTO {src}({}) VALUES {}",
        V112_TY_FIXTURE.insert_columns, V112_TY_FIXTURE.seed
    ))
    .expect("seed");
    let fresh = select_template.replace("{src}", &src);
    let created = v112_ty_try(&format!(
        "SELECT create_reflex_ivm('{imv}', $v112sel${fresh}$v112sel$)"
    ));
    (created, v112_ty_leftovers(&imv))
}

macro_rules! v112_ty_real_sum_rejections {
    ($($name:ident => $select:expr,)+) => {
        $(
            #[pg_test]
            fn $name() {
                let tag = stringify!($name).trim_start_matches("v112_types_");
                let (created, leftovers) = v112_ty_create_outcome(tag, $select);
                assert!(
                    created.starts_with("ERROR: [reflex-unsupported]")
                        && created.contains("REAL")
                        && created.contains("::numeric"),
                    "{}: {created}",
                    $select
                );
                assert_eq!(leftovers, "none", "{}", $select);
            }
        )+
    };
}

v112_ty_real_sum_rejections! {
    v112_types_real_sum_g_rejected => "SELECT g, SUM(rl) AS r FROM {src} GROUP BY g",
    v112_types_real_sum_u_rejected => "SELECT SUM(rl) AS r FROM {src}",
    v112_types_real_avg_g_rejected => "SELECT g, AVG(rl) AS r FROM {src} GROUP BY g",
    v112_types_real_avg_u_rejected => "SELECT AVG(rl) AS r FROM {src}",
    v112_types_real_sum_expr_rejected => "SELECT g, SUM(rl + rl) AS r FROM {src} GROUP BY g",
    v112_types_real_sum_cast_rejected => "SELECT g, SUM(v::real) AS r FROM {src} GROUP BY g",
    v112_types_real_sum_filter_rejected => "SELECT g, SUM(rl) FILTER (WHERE v > 1) AS r FROM {src} GROUP BY g",
    v112_types_real_sum_nested_rejected => "SELECT g, SUM(rl) / 2 AS r FROM {src} GROUP BY g",
    v112_types_real_avg_nested_rejected => "SELECT g, COALESCE(AVG(rl), 0) + 1 AS r FROM {src} GROUP BY g",
    v112_types_real_sum_having_rejected => "SELECT g, COUNT(*) AS r FROM {src} GROUP BY g HAVING SUM(rl) > 0",
    v112_types_real_sum_beside_others_rejected => "SELECT g, SUM(v) AS sv, MAX(rl) AS mr, AVG(rl) AS r FROM {src} GROUP BY g",
    v112_types_real_cte_sum_rejected => "WITH c AS (SELECT g, rl FROM {src} WHERE v > 0) SELECT g, SUM(rl) AS r FROM c GROUP BY g",
    v112_types_real_sum_in_cte_rejected => "WITH c AS (SELECT g, SUM(rl) AS r FROM {src} GROUP BY g) SELECT g, r FROM c WHERE r > 0",
    v112_types_real_sum_spelling_rejected => "SELECT g, sum(rl) AS r, avg(  rl ) AS a FROM {src} GROUP BY g",
}

// The remedy the rejection prescribes, and the REAL shapes that stay supported
v112_ty_cells! {
    V112_TY_FIXTURE, false;
    v112_types_real_sum_numeric_g => "SELECT g, SUM(rl::numeric) AS r FROM {src} GROUP BY g",
    v112_types_real_sum_numeric_u => "SELECT SUM(rl::numeric) AS r FROM {src}",
    v112_types_real_avg_numeric_g => "SELECT g, AVG(rl::numeric) AS r FROM {src} GROUP BY g",
    v112_types_real_avg_numeric_u => "SELECT AVG(rl::numeric) AS r FROM {src}",
    v112_types_real_sum_numeric_nested_g => "SELECT g, SUM(rl::numeric) / 2 AS r FROM {src} GROUP BY g",
    v112_types_real_sum_numeric_as_real_g => "SELECT g, SUM(rl::numeric)::real AS r FROM {src} GROUP BY g",
    v112_types_real_count_g => "SELECT g, COUNT(rl) AS r FROM {src} GROUP BY g",
    v112_types_real_group_key => "SELECT rl, COUNT(*) AS r FROM {src} GROUP BY rl",
}

v112_ty_cells! {
    V112_TY_FIXTURE, true;
    v112_types_real_sum_numeric_g_deferred => "SELECT g, SUM(rl::numeric) AS r FROM {src} GROUP BY g",
    v112_types_real_avg_numeric_u_deferred => "SELECT AVG(rl::numeric) AS r FROM {src}",
}

// ---------------------------------------------------------------------------
// MIN / MAX: the stored type is the type of the argument expression
// ---------------------------------------------------------------------------

v112_ty_cells! {
    V112_TY_FIXTURE, false;
    v112_types_min_real_expr_g => "SELECT g, MIN(rl * 2) AS r FROM {src} GROUP BY g",
    v112_types_max_real_expr_g => "SELECT g, MAX(rl * 2) AS r FROM {src} GROUP BY g",
    v112_types_min_real_expr_u => "SELECT MIN(rl * 2) AS r FROM {src}",
    v112_types_max_real_sum_expr_g => "SELECT g, MAX(rl + rl) AS r FROM {src} GROUP BY g",
    v112_types_min_real_expr_nested_g => "SELECT g, MIN(rl * 2) / 3 AS r FROM {src} GROUP BY g",
    v112_types_max_real_expr_nested_u => "SELECT MAX(rl * 2) / 3 AS r FROM {src}",
    v112_types_min_real_filter_g => "SELECT g, MIN(rl) FILTER (WHERE v > 1) AS r FROM {src} GROUP BY g",
    v112_types_max_real_column_g => "SELECT g, MAX(rl) AS r FROM {src} GROUP BY g",
    v112_types_max_real_column_nested_g => "SELECT g, MAX(rl) / 3 AS r FROM {src} GROUP BY g",
    v112_types_max_real_having_g => "SELECT g, COUNT(*) AS r FROM {src} GROUP BY g HAVING MAX(rl * 2) > 1",
    v112_types_min_double_expr_g => "SELECT g, MIN(f / 3) AS r FROM {src} GROUP BY g",

    v112_types_max_text_function_g => "SELECT g, SUM(v) AS s, MAX(upper(g)) AS u FROM {src} GROUP BY g",
    v112_types_max_text_function_only_g => "SELECT g, MAX(upper(t)) AS u FROM {src} GROUP BY g",
    v112_types_min_text_function_u => "SELECT MIN(upper(t)) AS u FROM {src}",
    v112_types_max_text_concat_g => "SELECT g, MAX(t || '!') AS u FROM {src} GROUP BY g",
    v112_types_max_varchar_function_g => "SELECT g, MAX(upper(vc)) AS u FROM {src} GROUP BY g",
    v112_types_max_varchar_column_g => "SELECT g, MAX(vc) AS u FROM {src} GROUP BY g",
    v112_types_max_int_function_g => "SELECT g, MAX(length(t)) AS u FROM {src} GROUP BY g",
    v112_types_max_text_function_nested_g => "SELECT g, COALESCE(MAX(upper(t)), '-') AS u FROM {src} GROUP BY g",

    v112_types_max_text_filter_g => "SELECT g, MAX(t) FILTER (WHERE v > 1) AS r FROM {src} GROUP BY g",
    v112_types_min_text_filter_g => "SELECT g, MIN(t) FILTER (WHERE v > 1) AS r FROM {src} GROUP BY g",
    v112_types_max_text_filter_u => "SELECT MAX(t) FILTER (WHERE v > 1) AS r FROM {src}",
    v112_types_min_text_filter_u => "SELECT MIN(t) FILTER (WHERE v > 1) AS r FROM {src}",
    v112_types_max_varchar_filter_g => "SELECT g, MAX(vc) FILTER (WHERE v > 1) AS r FROM {src} GROUP BY g",

    v112_types_max_text_filter_nested_g => "SELECT g, COALESCE(MAX(t) FILTER (WHERE v > 1), '-') AS r FROM {src} GROUP BY g",
    v112_types_min_text_filter_nested_g => "SELECT g, COALESCE(MIN(t) FILTER (WHERE v > 1), '-') AS r FROM {src} GROUP BY g",
    v112_types_max_text_filter_nested_u => "SELECT COALESCE(MAX(t) FILTER (WHERE v > 1), '-') AS r FROM {src}",
    v112_types_max_text_filter_having_g => "SELECT g, COUNT(*) AS r FROM {src} GROUP BY g HAVING MAX(t) FILTER (WHERE v > 1) > 'a'",

    v112_types_max_int_expr_g => "SELECT g, MAX(v * 3) AS r FROM {src} GROUP BY g",
    v112_types_min_int_expr_g => "SELECT g, MIN(v * 3) AS r FROM {src} GROUP BY g",
    v112_types_max_int_expr_u => "SELECT MAX(v * 3) AS r FROM {src}",
    v112_types_min_int_expr_u => "SELECT MIN(v * 3) AS r FROM {src}",
    v112_types_max_int_filter_g => "SELECT g, MAX(v) FILTER (WHERE v > 1) AS r FROM {src} GROUP BY g",
    v112_types_min_int_filter_u => "SELECT MIN(v) FILTER (WHERE v > 1) AS r FROM {src}",
    v112_types_max_bigint_expr_g => "SELECT g, MAX(q * 3) AS r FROM {src} GROUP BY g",
    v112_types_min_bigint_expr_u => "SELECT MIN(q * 3) AS r FROM {src}",
    v112_types_max_bigint_filter_g => "SELECT g, MAX(q) FILTER (WHERE v > 1) AS r FROM {src} GROUP BY g",
    v112_types_min_bigint_filter_u => "SELECT MIN(q) FILTER (WHERE v > 1) AS r FROM {src}",
    v112_types_max_smallint_expr_g => "SELECT g, MAX(s + s) AS r FROM {src} GROUP BY g",
    v112_types_min_smallint_filter_g => "SELECT g, MIN(s) FILTER (WHERE v > 1) AS r FROM {src} GROUP BY g",
    v112_types_max_numeric_expr_g => "SELECT g, MAX(n * 3) AS r FROM {src} GROUP BY g",

    v112_types_max_interval_column_g => "SELECT g, MAX(iv) AS r FROM {src} GROUP BY g",
    v112_types_min_interval_expr_u => "SELECT MIN(iv * 2) AS r FROM {src}",
    v112_types_max_time_column_g => "SELECT g, MAX(tm) AS r FROM {src} GROUP BY g",
}

v112_ty_cells! {
    V112_TY_FIXTURE, true;
    v112_types_min_real_expr_g_deferred => "SELECT g, MIN(rl * 2) AS r FROM {src} GROUP BY g",
    v112_types_max_text_function_g_deferred => "SELECT g, SUM(v) AS s, MAX(upper(g)) AS u FROM {src} GROUP BY g",
    v112_types_max_text_filter_g_deferred => "SELECT g, MAX(t) FILTER (WHERE v > 1) AS r FROM {src} GROUP BY g",
    v112_types_max_text_filter_nested_g_deferred => "SELECT g, COALESCE(MAX(t) FILTER (WHERE v > 1), '-') AS r FROM {src} GROUP BY g",
    v112_types_max_int_expr_g_deferred => "SELECT g, MAX(v * 3) AS r FROM {src} GROUP BY g",
}

// ---------------------------------------------------------------------------
// Result column types are those of the query
// ---------------------------------------------------------------------------

v112_ty_cells! {
    V112_TY_FIXTURE, false;
    v112_types_alias_of_real_column_g => "SELECT g, SUM(v) / 2 AS rl FROM {src} GROUP BY g",
    v112_types_alias_of_double_column_g => "SELECT g, SUM(v) AS f FROM {src} GROUP BY g",
    v112_types_alias_of_text_column_g => "SELECT g, COUNT(*) AS t FROM {src} GROUP BY g",
    v112_types_alias_of_int_column_g => "SELECT g, MAX(t) AS v FROM {src} GROUP BY g",
    v112_types_alias_of_int_column_u => "SELECT AVG(n) AS v FROM {src}",
    v112_types_alias_of_boolean_column_g => "SELECT g, SUM(q) AS b FROM {src} GROUP BY g",
    v112_types_alias_of_numeric_column_g => "SELECT g, BOOL_OR(b) AS n FROM {src} GROUP BY g",
    v112_types_alias_of_bigint_column_g => "SELECT g, MIN(n) + 0.5 AS q FROM {src} GROUP BY g",

    v112_types_varchar_n_derived_g => "SELECT g, (COUNT(*) + 1)::varchar(10) AS r FROM {src} GROUP BY g",
    v112_types_varchar_n_derived_u => "SELECT (COUNT(*) + 1)::varchar(10) AS r FROM {src}",
    v112_types_varchar_derived_g => "SELECT g, (SUM(v) / 2)::varchar AS r FROM {src} GROUP BY g",
    v112_types_varchar_top_level_g => "SELECT g, MAX(t)::varchar AS r FROM {src} GROUP BY g",
    v112_types_varchar_n_top_level_g => "SELECT g, COUNT(*)::varchar(10) AS r FROM {src} GROUP BY g",
    v112_types_numeric_scale_derived_g => "SELECT g, (SUM(n) / 3)::numeric(12, 2) AS r FROM {src} GROUP BY g",

    v112_types_unaliased_sum_g => "SELECT g, SUM(v) FROM {src} GROUP BY g",
    v112_types_sum_int_g => "SELECT g, SUM(v) AS r FROM {src} GROUP BY g",
    v112_types_sum_bigint_g => "SELECT g, SUM(q) AS r FROM {src} GROUP BY g",
    v112_types_sum_smallint_u => "SELECT SUM(s) AS r FROM {src}",
    v112_types_avg_int_g => "SELECT g, AVG(v) AS r FROM {src} GROUP BY g",
    v112_types_count_g => "SELECT g, COUNT(*) AS r, COUNT(v) AS c FROM {src} GROUP BY g",
    v112_types_bool_or_g => "SELECT g, BOOL_OR(b) AS r FROM {src} GROUP BY g",
}

// ---------------------------------------------------------------------------
// Ungrouped BOOL_OR over a NOT NULL argument
// ---------------------------------------------------------------------------

/// Runs `select` over a source whose boolean column is NOT NULL, through an
/// emptied source and a refill.
fn v112_ty_bool_or_cell(tag: &str, select_template: &str, deferred: bool) -> String {
    let src = format!("v112_ty_{tag}_src");
    let imv = format!("v112_ty_{tag}");
    Spi::run(&format!(
        "CREATE TABLE {src}(id SERIAL PRIMARY KEY, g TEXT NOT NULL, b BOOLEAN NOT NULL, nb BOOLEAN)"
    ))
    .expect("create source");
    Spi::run(&format!(
        "INSERT INTO {src}(g, b, nb) VALUES ('a', true, NULL), ('a', false, false), ('b', false, NULL)"
    ))
    .expect("seed");
    let fresh = select_template.replace("{src}", &src);
    let mode = if deferred { "DEFERRED" } else { "IMMEDIATE" };
    let created = v112_ty_try(&format!(
        "SELECT create_reflex_ivm('{imv}', $v112sel${fresh}$v112sel$, NULL, 'UNLOGGED', '{mode}')"
    ));
    if created != "CREATE REFLEX INCREMENTAL VIEW" {
        return format!("NOT-CREATED {created}");
    }
    let steps = [
        ("only_false", format!("DELETE FROM {src} WHERE b")),
        ("empty", format!("DELETE FROM {src}")),
        (
            "refill_false",
            format!("INSERT INTO {src}(g, b, nb) VALUES ('a', false, NULL)"),
        ),
        (
            "refill_true",
            format!("INSERT INTO {src}(g, b, nb) VALUES ('c', true, true)"),
        ),
        ("flip", format!("UPDATE {src} SET b = NOT b")),
        ("empty_again", format!("DELETE FROM {src}")),
    ];
    let mut outcomes = vec![format!("create={}", v112_ty_oracle(&imv, &fresh))];
    for (label, dml) in steps {
        let ran = v112_ty_try_dml(&dml);
        if ran != "OK" {
            outcomes.push(format!("{label}={ran}"));
            break;
        }
        if deferred {
            v112_ty_try(&format!("SELECT reflex_flush_deferred('{src}')"));
        }
        outcomes.push(format!("{label}={}", v112_ty_oracle(&imv, &fresh)));
    }
    if outcomes.iter().all(|o| o.ends_with("=0")) {
        "ok".to_string()
    } else {
        outcomes.join(" ")
    }
}

macro_rules! v112_ty_bool_or_cells {
    ($deferred:expr; $($name:ident => $select:expr,)+) => {
        $(
            #[pg_test]
            fn $name() {
                let tag = stringify!($name).trim_start_matches("v112_types_");
                assert_eq!(v112_ty_bool_or_cell(tag, $select, $deferred), "ok", "{}", $select);
            }
        )+
    };
}

v112_ty_bool_or_cells! {
    false;
    v112_types_bool_or_not_null_u => "SELECT BOOL_OR(b) AS r FROM {src}",
    v112_types_bool_or_not_null_g => "SELECT g, BOOL_OR(b) AS r FROM {src} GROUP BY g",
    v112_types_bool_or_nullable_u => "SELECT BOOL_OR(nb) AS r FROM {src}",
    v112_types_bool_or_is_null_u => "SELECT BOOL_OR(nb IS NULL) AS r FROM {src}",
    v112_types_bool_or_not_null_beside_count_u => "SELECT COUNT(*) AS c, BOOL_OR(b) AS r, SUM(id) AS s FROM {src}",
    v112_types_bool_or_not_null_nested_u => "SELECT NOT BOOL_OR(b) AS r FROM {src}",
    v112_types_bool_or_not_null_filter_u => "SELECT BOOL_OR(b) FILTER (WHERE g = 'a') AS r FROM {src}",
    v112_types_bool_or_not_null_having_u => "SELECT COUNT(*) AS c FROM {src} HAVING BOOL_OR(b)",
}

v112_ty_bool_or_cells! {
    true;
    v112_types_bool_or_not_null_u_deferred => "SELECT BOOL_OR(b) AS r FROM {src}",
    v112_types_bool_or_is_null_u_deferred => "SELECT BOOL_OR(nb IS NULL) AS r FROM {src}",
}

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
// Intermediate column names
// ---------------------------------------------------------------------------

v112_ty_cells! {
    V112_TY_NAMES_FIXTURE, false;
    v112_types_names_sum_plus_minus_g => "SELECT g, SUM(v + 1) AS ra, SUM(v - 1) AS rb FROM {src} GROUP BY g",
    v112_types_names_sum_plus_minus_u => "SELECT SUM(v + 1) AS ra, SUM(v - 1) AS rb FROM {src}",
    v112_types_names_sum_times_over_g => "SELECT g, SUM(v * 2) AS ra, SUM(v / 2) AS rb, SUM(v % 2) AS rc FROM {src} GROUP BY g",
    v112_types_names_avg_plus_minus_g => "SELECT g, AVG(v + 1) AS ra, AVG(v - 1) AS rb FROM {src} GROUP BY g",
    v112_types_names_count_nullif_g => "SELECT g, COUNT(NULLIF(v, 1)) AS ra, COUNT(NULLIF(v, -1)) AS rb FROM {src} GROUP BY g",
    v112_types_names_min_max_plus_minus_g => "SELECT g, MIN(v + 1) AS ra, MIN(v - 1) AS rb, MAX(v + 1) AS rc, MAX(v - 1) AS rd FROM {src} GROUP BY g",
    v112_types_names_bool_or_comparisons_g => "SELECT g, BOOL_OR(v > 1) AS ra, BOOL_OR(v < 1) AS rb FROM {src} GROUP BY g",
    v112_types_names_sum_filters_g => "SELECT g, SUM(v) FILTER (WHERE v > 1) AS ra, SUM(v) FILTER (WHERE v < 1) AS rb FROM {src} GROUP BY g",
    v112_types_names_count_filters_g => "SELECT g, COUNT(*) FILTER (WHERE v > 1) AS ra, COUNT(*) FILTER (WHERE v < 1) AS rb FROM {src} GROUP BY g",
    v112_types_names_quoted_column_g => "SELECT g, SUM(v) AS ra, SUM(\"V\") AS rb FROM {src} GROUP BY g",
    v112_types_names_quoted_column_first_g => "SELECT g, MAX(\"V\") AS ra, MAX(v) AS rb FROM {src} GROUP BY g",
    v112_types_names_count_star_then_column_g => "SELECT g, COUNT(*) AS ra, COUNT(star) AS rb FROM {src} GROUP BY g",
    v112_types_names_count_column_then_star_g => "SELECT g, COUNT(star) AS ra, COUNT(*) AS rb FROM {src} GROUP BY g",
    v112_types_names_topk_companion_g => "SELECT g, MIN(v) AS ra, MIN(v_topk) AS rb FROM {src} GROUP BY g",
    v112_types_names_topk_companion_first_g => "SELECT g, MAX(v_topk) AS ra, MAX(v) AS rb FROM {src} GROUP BY g",
    v112_types_names_nested_plus_minus_g => "SELECT g, SUM(v + 1) / 2 AS ra, SUM(v - 1) / 2 AS rb FROM {src} GROUP BY g",
    v112_types_names_top_then_nested_g => "SELECT g, SUM(v + 1) AS ra, COALESCE(SUM(v - 1), 0) AS rb FROM {src} GROUP BY g",
    v112_types_names_having_g => "SELECT g, SUM(v - 1) AS ra FROM {src} GROUP BY g HAVING SUM(v + 1) > 4",
    v112_types_names_shared_sum_avg_g => "SELECT g, SUM(v + 1) AS ra, AVG(v + 1) AS rb, AVG(v - 1) AS rc, SUM(v - 1) AS rd FROM {src} GROUP BY g",
    v112_types_names_long_plus_minus_g => "SELECT g, \
        SUM(COALESCE(v, 0) + COALESCE(star, 0) + COALESCE(v_topk, 0) + 1) AS ra, \
        SUM(COALESCE(v, 0) + COALESCE(star, 0) + COALESCE(v_topk, 0) - 1) AS rb FROM {src} GROUP BY g",
    v112_types_names_long_bool_or_g => "SELECT g, \
        BOOL_OR(COALESCE(v, 0) + COALESCE(star, 0) + COALESCE(v_topk, 0) > 1) AS ra, \
        BOOL_OR(COALESCE(v, 0) + COALESCE(star, 0) + COALESCE(v_topk, 0) < 1) AS rb FROM {src} GROUP BY g",
    v112_types_names_long_min_max_g => "SELECT g, \
        MIN(COALESCE(v, 0) + COALESCE(star, 0) + COALESCE(v_topk, 0) + 1) AS ra, \
        MAX(COALESCE(v, 0) + COALESCE(star, 0) + COALESCE(v_topk, 0) - 1) AS rb FROM {src} GROUP BY g",
    v112_types_names_multiplier_counter_g => "SELECT g, SUM(v) AS ra, SUM(\"V\") AS rb, SUM(\"V\" * COALESCE(star, 1)) AS rc FROM {src} GROUP BY g",
    v112_types_names_multibyte_g => "SELECT g, SUM(\"aééééééééééééééééééééééééé\") AS ra, \
        MAX(\"aééééééééééééééééééééééééé\") AS rb FROM {src} GROUP BY g",
}

v112_ty_cells! {
    V112_TY_NAMES_FIXTURE, true;
    v112_types_names_sum_plus_minus_g_deferred => "SELECT g, SUM(v + 1) AS ra, SUM(v - 1) AS rb FROM {src} GROUP BY g",
    v112_types_names_min_max_plus_minus_g_deferred => "SELECT g, MIN(v + 1) AS ra, MIN(v - 1) AS rb, MAX(v + 1) AS rc, MAX(v - 1) AS rd FROM {src} GROUP BY g",
    v112_types_names_long_bool_or_g_deferred => "SELECT g, \
        BOOL_OR(COALESCE(v, 0) + COALESCE(star, 0) + COALESCE(v_topk, 0) > 1) AS ra, \
        BOOL_OR(COALESCE(v, 0) + COALESCE(star, 0) + COALESCE(v_topk, 0) < 1) AS rb FROM {src} GROUP BY g",
}

// The same collisions with one of the aggregates, or both, in HAVING
v112_ty_cells! {
    V112_TY_NAMES_FIXTURE, false;
    v112_types_names_having_max_negated_g => "SELECT g, MAX(v) AS ra FROM {src} GROUP BY g HAVING MAX(-v) > -3",
    v112_types_names_having_max_negated_u => "SELECT MAX(v) AS ra FROM {src} HAVING MAX(-v) > 0",
    v112_types_names_having_min_negated_g => "SELECT g, MIN(v) AS ra FROM {src} GROUP BY g HAVING MIN(-v) < -2",
    v112_types_names_having_sum_negated_g => "SELECT g, SUM(v) AS ra FROM {src} GROUP BY g HAVING SUM(-v) < -3",
    v112_types_names_having_sum_plus_minus_g => "SELECT g, SUM(v - 1) AS ra FROM {src} GROUP BY g HAVING SUM(v + 1) > 6",
    v112_types_names_having_avg_negated_g => "SELECT g, AVG(v) AS ra FROM {src} GROUP BY g HAVING AVG(-v) < -2",
    v112_types_names_having_count_nullif_g => "SELECT g, COUNT(NULLIF(v, 1)) AS ra FROM {src} GROUP BY g HAVING COUNT(NULLIF(v, -1)) > 2",
    v112_types_names_having_bool_or_g => "SELECT g, BOOL_OR(v > 1) AS ra FROM {src} GROUP BY g HAVING BOOL_OR(v < 1)",
    v112_types_names_having_sum_filter_g => "SELECT g, SUM(v) FILTER (WHERE v > 1) AS ra FROM {src} GROUP BY g HAVING COALESCE(SUM(v) FILTER (WHERE v < 1), 0) < 0",
    v112_types_names_having_max_filter_g => "SELECT g, MAX(v) FILTER (WHERE v > 1) AS ra FROM {src} GROUP BY g HAVING COALESCE(MAX(v) FILTER (WHERE v < 1), 5) < 5",
    v112_types_names_having_count_filter_g => "SELECT g, COUNT(*) FILTER (WHERE v > 1) AS ra FROM {src} GROUP BY g HAVING COUNT(*) FILTER (WHERE v < 1) > 0",
    v112_types_names_having_quoted_max_g => "SELECT g, MAX(v) AS ra FROM {src} GROUP BY g HAVING MAX(\"V\") > 100",
    v112_types_names_having_quoted_sum_g => "SELECT g, SUM(\"V\") AS ra FROM {src} GROUP BY g HAVING SUM(v) > 3",
    v112_types_names_having_count_star_column_g => "SELECT g, COUNT(star) AS ra FROM {src} GROUP BY g HAVING COUNT(*) > 1",
    v112_types_names_having_nested_select_g => "SELECT g, COALESCE(MAX(v), 0) + 1 AS ra FROM {src} GROUP BY g HAVING MAX(-v) > -3",
    v112_types_names_having_only_sum_g => "SELECT g, COUNT(*) AS ra FROM {src} GROUP BY g HAVING SUM(v + 1) > SUM(v - 1) + 4",
    v112_types_names_having_only_max_g => "SELECT g, COUNT(*) AS ra FROM {src} GROUP BY g HAVING MAX(-v) > -6 AND MAX(v) > 2",
    v112_types_names_having_only_min_g => "SELECT g, COUNT(*) AS ra FROM {src} GROUP BY g HAVING MIN(v + 1) > 1 OR MIN(v - 1) < -1",
    v112_types_names_having_only_avg_g => "SELECT g, COUNT(*) AS ra FROM {src} GROUP BY g HAVING AVG(v + 1) > 3 AND AVG(v - 1) < 12",
    v112_types_names_having_only_count_g => "SELECT g, COUNT(*) AS ra FROM {src} GROUP BY g HAVING COUNT(NULLIF(v, 1)) > COUNT(NULLIF(v, -1)) - 1",
    v112_types_names_having_only_bool_or_g => "SELECT g, COUNT(*) AS ra FROM {src} GROUP BY g HAVING BOOL_OR(v > 1) AND NOT BOOL_OR(v < 1)",
    v112_types_names_having_only_filter_g => "SELECT g, COUNT(*) AS ra FROM {src} GROUP BY g HAVING COALESCE(SUM(v) FILTER (WHERE v > 1), 0) > COALESCE(SUM(v) FILTER (WHERE v < 1), 0) + 5",
    v112_types_names_having_only_quoted_g => "SELECT g, COUNT(*) AS ra FROM {src} GROUP BY g HAVING MAX(v) < 20 AND MAX(\"V\") > 100",
    v112_types_names_distinct_keys_g => "SELECT g, COUNT(DISTINCT v) AS ra FROM {src} GROUP BY g HAVING COUNT(DISTINCT v) > 1",
    v112_types_names_distinct_quoted_keys_g => "SELECT g, COUNT(DISTINCT \"V\") AS ra FROM {src} GROUP BY g HAVING COUNT(DISTINCT \"V\") > 0",
}

v112_ty_cells! {
    V112_TY_NAMES_FIXTURE, true;
    v112_types_names_having_max_negated_g_deferred => "SELECT g, MAX(v) AS ra FROM {src} GROUP BY g HAVING MAX(-v) > -3",
    v112_types_names_having_sum_plus_minus_g_deferred => "SELECT g, SUM(v - 1) AS ra FROM {src} GROUP BY g HAVING SUM(v + 1) > 6",
    v112_types_names_having_only_bool_or_g_deferred => "SELECT g, COUNT(*) AS ra FROM {src} GROUP BY g HAVING BOOL_OR(v > 1) AND NOT BOOL_OR(v < 1)",
}

/// A `COUNT(DISTINCT ...)` in HAVING over another argument than the selected
/// one has no column to share a name on: the create is refused.
#[pg_test]
fn v112_types_names_having_distinct_of_another_argument_is_rejected() {
    for (tag, select) in [
        ("hd_neg", "SELECT g, COUNT(DISTINCT v) AS ra FROM {src} GROUP BY g HAVING COUNT(DISTINCT -v) > 1"),
        ("hd_quoted", "SELECT g, COUNT(DISTINCT v) AS ra FROM {src} GROUP BY g HAVING COUNT(DISTINCT \"V\") > 1"),
    ] {
        let outcome = v112_ty_cell_on(&V112_TY_NAMES_FIXTURE, tag, select, false);
        assert!(outcome.starts_with("REJECTED "), "{select}: {outcome}");
        assert_eq!(v112_ty_leftovers(&format!("v112_ty_{tag}")), "none", "{select}");
    }
}

/// Two sources joined on `g`, each with its own `v`: runs `select` through
/// writes to both and compares the IMV with the query after each.
fn v112_ty_join_cell(tag: &str, select_template: &str, deferred: bool) -> String {
    let left = format!("v112_ty_{tag}_l");
    let right = format!("v112_ty_{tag}_r");
    let imv = format!("v112_ty_{tag}");
    for source in [&left, &right] {
        Spi::run(&format!(
            "CREATE TABLE {source}(id SERIAL PRIMARY KEY, g TEXT, v INT)"
        ))
        .expect("create source");
    }
    Spi::run(&format!(
        "INSERT INTO {left}(g, v) VALUES ('a',1),('a',9),('b',5),('c',NULL),('d',2)"
    ))
    .expect("seed left");
    Spi::run(&format!(
        "INSERT INTO {right}(g, v) VALUES ('a',4),('b',-7),('b',30),('c',3),('e',8)"
    ))
    .expect("seed right");
    let fresh = select_template
        .replace("{left}", &left)
        .replace("{right}", &right);
    let mode = if deferred { "DEFERRED" } else { "IMMEDIATE" };
    let created = v112_ty_try(&format!(
        "SELECT create_reflex_ivm('{imv}', $v112sel${fresh}$v112sel$, NULL, 'UNLOGGED', '{mode}')"
    ));
    if created != "CREATE REFLEX INCREMENTAL VIEW" {
        return format!("NOT-CREATED {created}");
    }
    let steps = [
        ("ins_right", format!("INSERT INTO {right}(g, v) VALUES ('a',40),('d',-1),('c',NULL)")),
        ("ins_left", format!("INSERT INTO {left}(g, v) VALUES ('e',6),('b',50)")),
        ("upd_right", format!("UPDATE {right} SET v = v - 20 WHERE g IN ('a', 'b')")),
        ("upd_left", format!("UPDATE {left} SET v = v + 20 WHERE g IN ('a', 'd')")),
        ("move_right", format!("UPDATE {right} SET g = 'd' WHERE g = 'e'")),
        ("del_left", format!("DELETE FROM {left} WHERE v IN (29, 50)")),
        ("del_right", format!("DELETE FROM {right} WHERE g = 'b'")),
        ("empty_left", format!("DELETE FROM {left}")),
        ("refill_left", format!("INSERT INTO {left}(g, v) VALUES ('a',3),('d',7)")),
    ];
    let mut outcomes = vec![format!("create={}", v112_ty_oracle(&imv, &fresh))];
    for (label, dml) in steps {
        let ran = v112_ty_try_dml(&dml);
        if ran != "OK" {
            outcomes.push(format!("{label}={ran}"));
            break;
        }
        if deferred {
            for source in [&left, &right] {
                v112_ty_try(&format!("SELECT reflex_flush_deferred('{source}')"));
            }
        }
        outcomes.push(format!("{label}={}", v112_ty_oracle(&imv, &fresh)));
    }
    if outcomes.iter().all(|o| o.ends_with("=0")) {
        "ok".to_string()
    } else {
        outcomes.join(" ")
    }
}

macro_rules! v112_ty_join_cells {
    ($deferred:expr; $($name:ident => $select:expr,)+) => {
        $(
            #[pg_test]
            fn $name() {
                let tag = stringify!($name).trim_start_matches("v112_types_names_");
                assert_eq!(v112_ty_join_cell(tag, $select, $deferred), "ok", "{}", $select);
            }
        )+
    };
}

v112_ty_join_cells! {
    false;
    v112_types_names_join_having_max => "SELECT a.g, MAX(a.v) AS ra FROM {left} a JOIN {right} b ON b.g = a.g GROUP BY a.g HAVING MAX(b.v) > 2",
    v112_types_names_join_having_sum => "SELECT a.g, SUM(a.v) AS ra FROM {left} a JOIN {right} b ON b.g = a.g GROUP BY a.g HAVING SUM(b.v) > 2",
    v112_types_names_join_select_both => "SELECT a.g, SUM(a.v) AS ra, SUM(b.v) AS rb, MIN(a.v) AS rc, MIN(b.v) AS rd FROM {left} a JOIN {right} b ON b.g = a.g GROUP BY a.g",
    v112_types_names_join_having_only => "SELECT a.g, COUNT(*) AS ra FROM {left} a JOIN {right} b ON b.g = a.g GROUP BY a.g HAVING MAX(a.v) > 1 AND MAX(b.v) < 35",
    v112_types_names_join_having_negated => "SELECT a.g, MAX(a.v) AS ra FROM {left} a JOIN {right} b ON b.g = a.g GROUP BY a.g HAVING MAX(-a.v) > -4",
}

v112_ty_join_cells! {
    true;
    v112_types_names_join_having_max_deferred => "SELECT a.g, MAX(a.v) AS ra FROM {left} a JOIN {right} b ON b.g = a.g GROUP BY a.g HAVING MAX(b.v) > 2",
}

/// `v` is NOT NULL and `"V"` is not: the counter that tells an all-NULL `"V"`
/// group from a zero sum must survive the elision of the counter of `v`.
const V112_TY_NOT_NULL_NAMES_FIXTURE: V112TyFixture = V112TyFixture {
    columns: "g TEXT NOT NULL, v INT NOT NULL, \"V\" INT",
    insert_columns: "g, v, \"V\"",
    seed: "('a',1,100),('a',2,NULL),('b',5,NULL),('b',6,NULL),('c',0,-7)",
    steps: &[
        ("ins", "INSERT INTO {src}(g, v, \"V\") VALUES ('a',7,700),('d',3,NULL),('b',-1,NULL)"),
        ("upd", "UPDATE {src} SET v = v + 10, \"V\" = \"V\" - 3 WHERE g IN ('a', 'b')"),
        ("null", "UPDATE {src} SET \"V\" = NULL WHERE g = 'c'"),
        ("del", "DELETE FROM {src} WHERE g = 'd' OR (g = 'a' AND v = 11)"),
        ("empty", "DELETE FROM {src}"),
        ("refill", "INSERT INTO {src}(g, v, \"V\") VALUES ('a',4,NULL),('a',9,NULL),('e',1,1)"),
    ],
};

v112_ty_cells! {
    V112_TY_NOT_NULL_NAMES_FIXTURE, false;
    v112_types_names_not_null_counter_g => "SELECT g, SUM(\"V\") AS ra, SUM(v) AS rb FROM {src} GROUP BY g",
    v112_types_names_not_null_counter_u => "SELECT SUM(\"V\") AS ra, SUM(v) AS rb FROM {src}",
    v112_types_names_not_null_counter_reversed_g => "SELECT g, SUM(v) AS ra, SUM(\"V\") AS rb FROM {src} GROUP BY g",
}

/// Every intermediate column of the plan has its own name, the name PostgreSQL
/// stores (no truncation), over arguments that all sanitise alike.
#[pg_test]
fn v112_types_names_are_distinct_and_within_the_identifier_limit() {
    let select = "SELECT g, \
        SUM(COALESCE(v, 0) + COALESCE(star, 0) + COALESCE(v_topk, 0) + 1) AS ra, \
        SUM(COALESCE(v, 0) + COALESCE(star, 0) + COALESCE(v_topk, 0) - 1) AS rb, \
        BOOL_OR(COALESCE(v, 0) + COALESCE(star, 0) + COALESCE(v_topk, 0) > 1) AS rc, \
        BOOL_OR(COALESCE(v, 0) + COALESCE(star, 0) + COALESCE(v_topk, 0) < 1) AS rd, \
        MIN(COALESCE(v, 0) + COALESCE(star, 0) + COALESCE(v_topk, 0) + 1) AS re, \
        MIN(COALESCE(v, 0) + COALESCE(star, 0) + COALESCE(v_topk, 0) - 1) AS rf, \
        SUM(v + 1) AS rh, SUM(v - 1) AS ri, SUM(v * 1) AS rj, COUNT(*) AS rk, COUNT(star) AS rl, \
        MIN(v) AS rm, MIN(v_topk) AS rn \
        FROM {src} GROUP BY g";
    assert_eq!(
        v112_ty_cell_on(&V112_TY_NAMES_FIXTURE, "names_limit", select, false),
        "ok"
    );
    let planned = "SELECT c->>'name' AS name, c->>'source_aggregate' AS agg, c->>'source_arg' AS arg, \
                          (c->>'topk_k') IS NOT NULL AS has_topk \
                   FROM public.__reflex_ivm_reference r, \
                        jsonb_array_elements(r.aggregations->'intermediate_columns') c \
                   WHERE r.name = 'v112_ty_names_limit'";
    let names = format!(
        "SELECT name FROM ({planned}) p UNION ALL SELECT name || '_topk' FROM ({planned}) p WHERE has_topk"
    );
    assert_eq!(
        v112_ty_try(&format!(
            "SELECT (COUNT(*) - COUNT(DISTINCT name))::text FROM ({names}) n"
        )),
        "0",
        "two intermediate columns share a name"
    );
    assert_eq!(
        v112_ty_try(&format!(
            "SELECT COUNT(*)::text FROM ({names}) n WHERE octet_length(name) > 63"
        )),
        "0",
        "an intermediate column name exceeds 63 bytes"
    );
    assert_eq!(
        v112_ty_try(&format!(
            "SELECT (COUNT(DISTINCT (agg, arg)) FILTER (WHERE name LIKE '\\_\\_sum\\_%'))::text \
             FROM ({planned}) p"
        )),
        "5",
        "each distinct SUM argument keeps its own column"
    );
    assert_eq!(
        v112_ty_try(&format!(
            "SELECT COUNT(*)::text FROM ({names}) n WHERE NOT EXISTS (\
                SELECT 1 FROM pg_attribute a \
                WHERE a.attrelid = '__reflex_intermediate_v112_ty_names_limit'::regclass \
                  AND a.attname = n.name)"
        )),
        "0",
        "a planned column is not stored under its planned name"
    );
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

// ---------------------------------------------------------------------------
// Collation of a MIN / MAX argument and of a group key
// ---------------------------------------------------------------------------

fn v112_ty_column_collations(relation: &str) -> String {
    v112_ty_try(&format!(
        "SELECT string_agg(COALESCE(c.collname, '-'), ', ' ORDER BY a.attnum) \
         FROM pg_attribute a LEFT JOIN pg_collation c ON c.oid = a.attcollation \
         WHERE a.attrelid = '{relation}'::regclass AND a.attnum > 0 AND NOT a.attisdropped"
    ))
}

/// The letters order differently under the database default, under "C" and
/// under ICU English: upper case before lower case in the first two, mixed in
/// the third, where the punctuation also comes first.
const V112_TY_COLLATION_FIXTURE: V112TyFixture = V112TyFixture {
    columns: "g TEXT, v INT, tu TEXT COLLATE \"en-x-icu\", td TEXT, tc TEXT COLLATE \"C\", \
              tp TEXT COLLATE \"POSIX\"",
    insert_columns: "g, v, tu, td, tc, tp",
    seed: "('a',1,'b','b','b','b'),('a',2,'C','C','C','C'),('a',3,'c','c','c','c'),\
           ('z',4,'m','m','m','m'),('z',5,'N','N','N','N'),('n',6,NULL,NULL,NULL,NULL),\
           ('p',7,'_x','_x','_x','_x'),('p',8,'Zed','Zed','Zed','Zed'),('p',9,'été','été','été','été')",
    steps: &[
        (
            "ins_low",
            "INSERT INTO {src}(g, v, tu, td, tc, tp) VALUES ('a',10,'a','a','a','a'),('z',11,'B','B','B','B'),\
             ('n',12,'q','q','q','q')",
        ),
        (
            "ins_high",
            "INSERT INTO {src}(g, v, tu, td, tc, tp) VALUES ('a',13,'D','D','D','D'),('z',14,'o','o','o','o'),\
             ('p',15,'Été','Été','Été','Été'),('n',16,'Q','Q','Q','Q')",
        ),
        (
            "upd",
            "UPDATE {src} SET tu = upper(tu), td = upper(td), tc = upper(tc), tp = upper(tp) WHERE v IN (1, 4, 12)",
        ),
        (
            "upd_back",
            "UPDATE {src} SET tu = lower(tu), td = lower(td), tc = lower(tc), tp = lower(tp) WHERE v IN (2, 5, 8)",
        ),
        ("move", "UPDATE {src} SET g = 'a' WHERE g = 'p'"),
        ("del_low", "DELETE FROM {src} WHERE v IN (10, 7, 11)"),
        ("del_high", "DELETE FROM {src} WHERE v IN (13, 14, 9, 15)"),
        ("empty", "DELETE FROM {src}"),
        (
            "refill",
            "INSERT INTO {src}(g, v, tu, td, tc, tp) VALUES ('a',1,'b','b','b','b'),('a',2,'A','A','A','A'),\
             ('a',3,'C','C','C','C')",
        ),
        (
            "refill_more",
            "INSERT INTO {src}(g, v, tu, td, tc, tp) VALUES ('a',4,'a','a','a','a'),('a',5,'d','d','d','d'),\
             ('a',6,'B','B','B','B')",
        ),
    ],
};

/// `ok` when the cell is correct through every step and each IMV column also
/// has the collation the query gives it; `no-icu` when the server has no ICU
/// collation to test with.
fn v112_ty_collation_cell(tag: &str, select_template: &str, deferred: bool) -> String {
    let has_icu = v112_ty_try(
        "SELECT COUNT(*)::text FROM pg_collation WHERE collname = 'en-x-icu'",
    );
    if has_icu == "0" {
        return "no-icu".to_string();
    }
    let outcome = v112_ty_cell_on(&V112_TY_COLLATION_FIXTURE, tag, select_template, deferred);
    if outcome != "ok" {
        return outcome;
    }
    let imv = format!("v112_ty_{tag}");
    let imv_collations = v112_ty_column_collations(&imv);
    let query_collations = v112_ty_column_collations(&format!("{imv}_typed_q"));
    if imv_collations == query_collations {
        "ok".to_string()
    } else {
        format!("WRONG-COLLATION imv ({imv_collations}) query ({query_collations})")
    }
}

macro_rules! v112_ty_collation_cells {
    ($deferred:expr; $($name:ident => $select:expr,)+) => {
        $(
            #[pg_test]
            fn $name() {
                let tag = stringify!($name).trim_start_matches("v112_types_");
                let outcome = v112_ty_collation_cell(tag, $select, $deferred);
                assert!(outcome == "ok" || outcome == "no-icu", "{}: {outcome}", $select);
            }
        )+
    };
}

v112_ty_collation_cells! {
    false;
    v112_types_coll_icu_column_min_g => "SELECT g, MIN(tu) AS lo FROM {src} GROUP BY g",
    v112_types_coll_icu_column_max_g => "SELECT g, MAX(tu) AS hi FROM {src} GROUP BY g",
    v112_types_coll_icu_column_both_u => "SELECT MIN(tu) AS lo, MAX(tu) AS hi FROM {src}",
    v112_types_coll_icu_column_expr_g => "SELECT g, MIN(tu || '') AS lo, MAX(tu || '') AS hi FROM {src} GROUP BY g",
    v112_types_coll_icu_column_expr_u => "SELECT MIN(tu || '') AS lo, MAX(tu || '') AS hi FROM {src}",
    v112_types_coll_icu_column_filter_g => "SELECT g, MIN(tu) FILTER (WHERE v > 1) AS lo, MAX(tu) FILTER (WHERE v < 15) AS hi FROM {src} GROUP BY g",
    v112_types_coll_icu_column_nested_g => "SELECT g, COALESCE(MIN(tu) FILTER (WHERE v > 0), '-') AS lo, COALESCE(MAX(tu), '-') || '!' AS hi FROM {src} GROUP BY g",
    v112_types_coll_icu_column_nested_u => "SELECT COALESCE(MIN(tu), '-') AS lo, COALESCE(MAX(tu || ''), '-') AS hi FROM {src}",
    v112_types_coll_icu_column_having_max_g => "SELECT g, COUNT(*) AS c FROM {src} GROUP BY g HAVING MAX(tu || '') > 'b'",
    v112_types_coll_icu_column_having_min_g => "SELECT g, COUNT(*) AS c FROM {src} GROUP BY g HAVING MIN(tu) < 'B'",
    v112_types_coll_icu_column_having_both_g => "SELECT g, MIN(tu) AS lo FROM {src} GROUP BY g HAVING MAX(tu) >= 'D'",
    v112_types_coll_icu_column_having_u => "SELECT COUNT(*) AS c FROM {src} HAVING MIN(tu) < 'B'",
    v112_types_coll_icu_expr_min_g => "SELECT g, MIN(td COLLATE \"en-x-icu\") AS lo FROM {src} GROUP BY g",
    v112_types_coll_icu_expr_max_g => "SELECT g, MAX(td COLLATE \"en-x-icu\") AS hi FROM {src} GROUP BY g",
    v112_types_coll_icu_expr_both_u => "SELECT MIN(td COLLATE \"en-x-icu\") AS lo, MAX(td COLLATE \"en-x-icu\") AS hi FROM {src}",
    v112_types_coll_icu_expr_filter_g => "SELECT g, MAX(td COLLATE \"en-x-icu\") FILTER (WHERE v > 1) AS hi FROM {src} GROUP BY g",
    v112_types_coll_icu_expr_nested_g => "SELECT g, COALESCE(MIN(td COLLATE \"en-x-icu\"), '-') AS lo FROM {src} GROUP BY g",
    v112_types_coll_icu_expr_having_g => "SELECT g, COUNT(*) AS c FROM {src} GROUP BY g HAVING MAX(td COLLATE \"en-x-icu\") > 'b'",
    v112_types_coll_icu_and_default_g => "SELECT g, MIN(tu) AS a, MIN(td) AS b, MAX(td COLLATE \"en-x-icu\") AS c, MAX(td) AS d FROM {src} GROUP BY g",
    v112_types_coll_c_column_g => "SELECT g, MIN(tc) AS lo, MAX(tc) AS hi FROM {src} GROUP BY g",
    v112_types_coll_c_expr_g => "SELECT g, MIN(tu COLLATE \"C\") AS lo, MAX(tu COLLATE \"C\") AS hi FROM {src} GROUP BY g",
    v112_types_coll_c_expr_having_u => "SELECT COUNT(*) AS c FROM {src} HAVING MIN(tu COLLATE \"C\") < 'B'",
    v112_types_coll_posix_column_g => "SELECT g, MIN(tp) AS lo, MAX(tp || '') AS hi FROM {src} GROUP BY g",
    v112_types_coll_posix_expr_u => "SELECT MIN(tu COLLATE \"POSIX\") AS lo, MAX(td COLLATE \"POSIX\") AS hi FROM {src}",
    v112_types_coll_default_g => "SELECT g, MIN(td) AS lo, MAX(td || '') AS hi FROM {src} GROUP BY g HAVING MAX(td) > 'B'",
}

v112_ty_collation_cells! {
    true;
    v112_types_coll_icu_column_expr_g_deferred => "SELECT g, MIN(tu || '') AS lo, MAX(tu || '') AS hi FROM {src} GROUP BY g",
    v112_types_coll_icu_expr_both_u_deferred => "SELECT MIN(td COLLATE \"en-x-icu\") AS lo, MAX(td COLLATE \"en-x-icu\") AS hi FROM {src}",
    v112_types_coll_icu_column_having_max_g_deferred => "SELECT g, COUNT(*) AS c FROM {src} GROUP BY g HAVING MAX(tu || '') > 'b'",
}

/// The same with a top-K heap of two values: the heap order and its underflow
/// both depend on the collation.
#[pg_test]
fn v112_types_coll_icu_small_heap() {
    if v112_ty_try("SELECT COUNT(*)::text FROM pg_collation WHERE collname = 'en-x-icu'") == "0" {
        return;
    }
    for (tag, select) in [
        ("coll_heap_col", "SELECT g, MIN(tu) AS lo, MAX(tu) AS hi FROM {src} GROUP BY g"),
        ("coll_heap_expr", "SELECT g, MIN(td COLLATE \"en-x-icu\") AS lo, MAX(tu || '') AS hi FROM {src} GROUP BY g"),
        ("coll_heap_u", "SELECT MIN(tu || '') AS lo, MAX(td COLLATE \"en-x-icu\") AS hi FROM {src}"),
    ] {
        let fixture = &V112_TY_COLLATION_FIXTURE;
        let src = format!("v112_ty_{tag}_src");
        let imv = format!("v112_ty_{tag}");
        Spi::run(&format!("CREATE TABLE {src}(id SERIAL PRIMARY KEY, {})", fixture.columns)).expect("source");
        Spi::run(&format!("INSERT INTO {src}({}) VALUES {}", fixture.insert_columns, fixture.seed)).expect("seed");
        let fresh = select.replace("{src}", &src);
        assert_eq!(
            v112_ty_try(&format!(
                "SELECT create_reflex_ivm('{imv}', $v112sel${fresh}$v112sel$, NULL, 'UNLOGGED', 'IMMEDIATE', 2)"
            )),
            "CREATE REFLEX INCREMENTAL VIEW",
            "{select}"
        );
        let mut outcomes = vec![format!("create={}", v112_ty_oracle(&imv, &fresh))];
        for (label, dml) in fixture.steps {
            assert_eq!(v112_ty_try_dml(&dml.replace("{src}", &src)), "OK", "{select} {label}");
            outcomes.push(format!("{label}={}", v112_ty_oracle(&imv, &fresh)));
        }
        assert!(outcomes.iter().all(|o| o.ends_with("=0")), "{select}: {}", outcomes.join(" "));
    }
}

// ---------------------------------------------------------------------------
// Types outside pg_catalog are stored under their schema
// ---------------------------------------------------------------------------

/// Creates the IMV with the types' schema on the path, then writes, reconciles
/// and compares under `writer_path`. Returns "ok" or the first failure.
fn v112_ty_user_type_cell(tag: &str, select_template: &str, writer_path: &str) -> String {
    let schema = format!("v112_ty_{tag}_app");
    let other = format!("v112_ty_{tag}_other");
    let imv = format!("public.v112_ty_{tag}");
    let src = format!("{schema}.s");
    for ddl in [
        format!("CREATE SCHEMA {schema}"),
        format!("CREATE SCHEMA {other}"),
        format!("CREATE TYPE {schema}.mood AS ENUM ('zeta', 'mid', 'alpha', 'x10', 'x2')"),
        format!("CREATE TYPE {schema}.\"My Mood\" AS ENUM ('low', 'High', 'higher')"),
        format!("CREATE DOMAIN {schema}.pos_int AS INT CHECK (VALUE > 0)"),
        format!("CREATE DOMAIN {schema}.label AS VARCHAR(8)"),
        format!(
            "CREATE TABLE {src}(id SERIAL PRIMARY KEY, g TEXT, e {schema}.mood, \
             q {schema}.\"My Mood\", d {schema}.pos_int, l {schema}.label, v INT)"
        ),
        format!(
            "INSERT INTO {src}(g, e, q, d, l, v) VALUES ('a','mid','low',3,'bb',1),('a','x10','High',9,'aa',2),\
             ('b','alpha','higher',5,'zz',3),('n',NULL,NULL,NULL,NULL,4)"
        ),
    ] {
        Spi::run(&ddl).expect("fixture");
    }
    let creator_path = v112_ty_try("SELECT current_setting('search_path')");
    let fresh = select_template.replace("{src}", &src).replace("{schema}", &schema);
    let set_path = |path: &str| {
        Spi::run(&format!("SELECT set_config('search_path', '{path}', true)")).expect("path");
    };
    set_path(&format!("{schema}, public"));
    let created = v112_ty_try(&format!(
        "SELECT public.create_reflex_ivm('{imv}', $v112sel${fresh}$v112sel$)"
    ));
    let writer = writer_path.replace("{other}", &other).replace("{schema}", &schema);
    set_path(&writer);
    let outcome = (|| {
        if created != "CREATE REFLEX INCREMENTAL VIEW" {
            return format!("NOT-CREATED {created}");
        }
        let steps = [
            ("create", String::new()),
            ("ins", format!("INSERT INTO {src}(g, e, q, d, l, v) VALUES ('a','zeta','higher',1,'cc',5),('c','x2','low',7,'dd',6)")),
            ("upd", format!("UPDATE {src} SET e = 'x2', q = 'low', d = 2, l = 'mm' WHERE v IN (2, 3)")),
            ("move", format!("UPDATE {src} SET g = 'b' WHERE g = 'c'")),
            ("del", format!("DELETE FROM {src} WHERE v IN (1, 5)")),
            ("reconcile", format!("SELECT public.reflex_reconcile('{imv}')")),
            ("empty", format!("DELETE FROM {src}")),
            ("refill", format!("INSERT INTO {src}(g, e, q, d, l, v) VALUES ('a','alpha','High',4,'ee',7)")),
        ];
        for (label, dml) in steps {
            if !dml.is_empty() {
                let ran = if dml.starts_with("SELECT") { v112_ty_try(&dml) } else { v112_ty_try_dml(&dml) };
                if ran.starts_with("RAISED") || ran.starts_with("ERROR") {
                    return format!("{label}: {ran}");
                }
            }
            let mismatches = v112_ty_oracle(&imv, &fresh);
            if mismatches != "0" {
                return format!("{label}: oracle {mismatches}");
            }
        }
        "ok".to_string()
    })();
    set_path(&creator_path);
    if outcome != "ok" {
        return outcome;
    }
    match v112_ty_types_against_query(&format!("v112_ty_{tag}"), &fresh).as_str() {
        "same" => "ok".to_string(),
        difference => format!("WRONG-TYPE {difference}"),
    }
}

macro_rules! v112_ty_user_type_cells {
    ($($name:ident => ($select:expr, $path:expr),)+) => {
        $(
            #[pg_test]
            fn $name() {
                let tag = stringify!($name).trim_start_matches("v112_types_");
                assert_eq!(v112_ty_user_type_cell(tag, $select, $path), "ok", "{} under {}", $select, $path);
            }
        )+
    };
}

v112_ty_user_type_cells! {
    v112_types_ut_enum_catalog_path => ("SELECT g, MAX(e) AS hi, MIN(e) AS lo FROM {src} GROUP BY g", "pg_catalog"),
    v112_types_ut_enum_other_path => ("SELECT g, MAX(e) AS hi, MIN(e) AS lo FROM {src} GROUP BY g", "{other}, public"),
    v112_types_ut_enum_creator_path => ("SELECT g, MAX(e) AS hi, MIN(e) AS lo FROM {src} GROUP BY g", "{schema}, public"),
    v112_types_ut_enum_u_catalog_path => ("SELECT MAX(e) AS hi, MIN(e) AS lo FROM {src}", "pg_catalog"),
    v112_types_ut_quoted_enum_catalog_path => ("SELECT g, MAX(q) AS hi, MIN(q) AS lo FROM {src} GROUP BY g", "pg_catalog"),
    v112_types_ut_quoted_enum_other_path => ("SELECT g, MAX(q) AS hi FROM {src} GROUP BY g", "{other}"),
    v112_types_ut_domain_int_catalog_path => ("SELECT g, MAX(d) AS hi, MIN(d) AS lo, SUM(d) AS s FROM {src} GROUP BY g", "pg_catalog"),
    v112_types_ut_enum_having_other_path => ("SELECT g, COUNT(*) AS c FROM {src} GROUP BY g HAVING MAX(e) > 'mid'", "{other}, public"),
    v112_types_ut_enum_nested_catalog_path => ("SELECT g, COALESCE(MAX(e), 'zeta') AS hi FROM {src} GROUP BY g", "pg_catalog"),
    v112_types_ut_enum_cast_catalog_path => ("SELECT g, MAX(l)::{schema}.label AS hi, (MAX(v) + 1)::{schema}.pos_int AS p FROM {src} GROUP BY g", "pg_catalog"),
}

// ---------------------------------------------------------------------------
// A group key has the type the query gives it
// ---------------------------------------------------------------------------

v112_ty_cells! {
    V112_TY_FIXTURE, false;
    v112_types_key_alias_of_text_column => "SELECT v AS g, COUNT(*) AS c FROM {src} GROUP BY v",
    v112_types_key_alias_of_int_column => "SELECT g AS v, COUNT(*) AS c FROM {src} GROUP BY g",
    v112_types_key_alias_and_aggregate_swapped => "SELECT g AS v, COUNT(*) AS c, SUM(v) AS g FROM {src} GROUP BY g",
    v112_types_key_alias_of_real_column => "SELECT t AS rl, MAX(v) AS hi FROM {src} GROUP BY t",
    v112_types_key_two_aliases_crossed => "SELECT v AS t, t AS q, COUNT(*) AS c FROM {src} GROUP BY v, t",
    v112_types_key_varchar => "SELECT vc, COUNT(*) AS c FROM {src} GROUP BY vc",
    v112_types_key_varchar_alias => "SELECT vc AS n, SUM(v) AS s FROM {src} GROUP BY vc",
    v112_types_key_interval => "SELECT iv, COUNT(*) AS c FROM {src} GROUP BY iv",
    v112_types_key_time => "SELECT tm, COUNT(*) AS c, MAX(v) AS hi FROM {src} GROUP BY tm",
    v112_types_key_smallint_bigint => "SELECT s, q, COUNT(*) AS c FROM {src} GROUP BY s, q",
    v112_types_key_expression => "SELECT upper(t) AS v, COUNT(*) AS c FROM {src} GROUP BY upper(t)",
    v112_types_key_distinct => "SELECT DISTINCT vc, iv FROM {src}",
    v112_types_key_count_distinct => "SELECT g, COUNT(DISTINCT vc) AS d FROM {src} GROUP BY g",
    v112_types_key_count_distinct_interval => "SELECT vc, COUNT(DISTINCT iv) AS d FROM {src} GROUP BY vc",
}

v112_ty_cells! {
    V112_TY_FIXTURE, true;
    v112_types_key_alias_of_text_column_deferred => "SELECT v AS g, COUNT(*) AS c FROM {src} GROUP BY v",
    v112_types_key_interval_deferred => "SELECT iv, COUNT(*) AS c FROM {src} GROUP BY iv",
}

v112_ty_cells! {
    V112_TY_NAMES_FIXTURE, false;
    v112_types_key_alias_of_counter_name => "SELECT star AS \"__count_star\", COUNT(*) AS c FROM {src} GROUP BY star",
}

v112_ty_user_type_cells! {
    v112_types_ut_enum_key_catalog_path => ("SELECT e, COUNT(*) AS c, MAX(q) AS hi FROM {src} GROUP BY e", "pg_catalog"),
    v112_types_ut_domain_key_other_path => ("SELECT d, l, COUNT(*) AS c FROM {src} GROUP BY d, l", "{other}, public"),
    v112_types_ut_quoted_enum_key_other_path => ("SELECT q, COUNT(*) AS c FROM {src} GROUP BY q", "{other}"),
}

v112_ty_collation_cells! {
    false;
    v112_types_coll_icu_group_key => "SELECT tu, COUNT(*) AS c, MAX(v) AS hi FROM {src} GROUP BY tu",
    v112_types_coll_icu_group_key_alias => "SELECT tu AS td, COUNT(*) AS c FROM {src} GROUP BY tu",
    v112_types_coll_icu_distinct => "SELECT DISTINCT tu, tc FROM {src}",
}

/// Two intervals that are equal and spell differently are one group.
const V112_TY_EQUAL_INTERVALS_FIXTURE: V112TyFixture = V112TyFixture {
    columns: "iv INTERVAL, v INT",
    insert_columns: "iv, v",
    seed: "('1 day', 1), ('2 days', 2), ('48 hours', 3)",
    steps: &[
        ("ins", "INSERT INTO {src}(iv, v) VALUES ('24 hours', 4), ('1 day', 5), ('72 hours', 6)"),
        ("upd", "UPDATE {src} SET iv = '24 hours' WHERE v = 2"),
        ("del", "DELETE FROM {src} WHERE v IN (1, 3)"),
        ("empty", "DELETE FROM {src}"),
        ("refill", "INSERT INTO {src}(iv, v) VALUES ('3 days', 7), ('72 hours', 8)"),
    ],
};

v112_ty_cells! {
    V112_TY_EQUAL_INTERVALS_FIXTURE, false;
    v112_types_key_equal_intervals => "SELECT iv, COUNT(*) AS c, SUM(v) AS s FROM {src} GROUP BY iv",
}

// ---------------------------------------------------------------------------
// SUM / AVG over a type PostgreSQL does not sum as a number
// ---------------------------------------------------------------------------

const V112_TY_MONEY_FIXTURE: V112TyFixture = V112TyFixture {
    columns: "g TEXT, m MONEY, iv INTERVAL, tm TIME, v INT",
    insert_columns: "g, m, iv, tm, v",
    seed: "('a','1.50','1 day','01:00',1),('a','2.25','2 hours','02:30',2),('b','-4.00','3 days','00:10',3),\
           ('n',NULL,NULL,NULL,4)",
    steps: &[
        ("ins", "INSERT INTO {src}(g, m, iv, tm, v) VALUES ('a','10.10','5 minutes','03:00',5),('c','7.00','1 hour','04:00',6)"),
        ("upd", "UPDATE {src} SET m = m * 2, iv = iv * 2, v = v + 1 WHERE g = 'a'"),
        ("move", "UPDATE {src} SET g = 'b' WHERE g = 'c'"),
        ("del", "DELETE FROM {src} WHERE v IN (2, 6)"),
        ("empty", "DELETE FROM {src}"),
        ("refill", "INSERT INTO {src}(g, m, iv, tm, v) VALUES ('a','3.33','4 days','05:00',7)"),
    ],
};

macro_rules! v112_ty_unsummable_rejections {
    ($($name:ident => ($select:expr, $type_name:expr),)+) => {
        $(
            #[pg_test]
            fn $name() {
                let tag = stringify!($name).trim_start_matches("v112_types_");
                let outcome = v112_ty_cell_on(&V112_TY_MONEY_FIXTURE, tag, $select, false);
                assert!(
                    outcome.starts_with("REJECTED ") && outcome.contains($type_name) && outcome.contains("numeric"),
                    "{}: {outcome}",
                    $select
                );
                assert_eq!(v112_ty_leftovers(&format!("v112_ty_{tag}")), "none", "{}", $select);
            }
        )+
    };
}

v112_ty_unsummable_rejections! {
    v112_types_sum_money_g_rejected => ("SELECT g, SUM(m) AS r FROM {src} GROUP BY g", "money"),
    v112_types_sum_money_u_rejected => ("SELECT SUM(m) AS r FROM {src}", "money"),
    v112_types_sum_money_nested_rejected => ("SELECT g, COALESCE(SUM(m), '0'::money) AS r FROM {src} GROUP BY g", "money"),
    v112_types_sum_money_having_rejected => ("SELECT g, COUNT(*) AS r FROM {src} GROUP BY g HAVING SUM(m) > '1'::money", "money"),
    v112_types_sum_interval_g_rejected => ("SELECT g, SUM(iv) AS r FROM {src} GROUP BY g", "interval"),
    v112_types_avg_interval_u_rejected => ("SELECT AVG(iv) AS r FROM {src}", "interval"),
    v112_types_sum_time_g_rejected => ("SELECT g, SUM(tm) AS r FROM {src} GROUP BY g", "interval"),
    v112_types_sum_interval_filter_rejected => ("SELECT g, SUM(iv) FILTER (WHERE v > 1) AS r FROM {src} GROUP BY g", "interval"),
}

// The remedies the rejection names, and the aggregates that stay supported
v112_ty_cells! {
    V112_TY_MONEY_FIXTURE, false;
    v112_types_sum_money_as_numeric_g => "SELECT g, SUM(m::numeric) AS r FROM {src} GROUP BY g",
    v112_types_sum_money_as_numeric_back_g => "SELECT g, SUM(m::numeric)::money AS r FROM {src} GROUP BY g",
    v112_types_sum_money_as_numeric_back_u => "SELECT SUM(m::numeric)::money AS r FROM {src}",
    v112_types_sum_interval_seconds_g => "SELECT g, SUM(EXTRACT(EPOCH FROM iv)) AS r FROM {src} GROUP BY g",
    v112_types_avg_interval_seconds_u => "SELECT AVG(EXTRACT(EPOCH FROM iv)) AS r FROM {src}",
    v112_types_min_max_money_g => "SELECT g, MIN(m) AS lo, MAX(m) AS hi, COUNT(m) AS c FROM {src} GROUP BY g",
}

// ---------------------------------------------------------------------------
// Two output columns of one name
// ---------------------------------------------------------------------------

#[pg_test]
fn v112_types_duplicate_output_names_are_rejected() {
    for (tag, select) in [
        ("dup_max", "SELECT g, MAX(t), MAX(v) FROM {src} GROUP BY g"),
        ("dup_sum", "SELECT SUM(v), SUM(q) FROM {src}"),
        ("dup_alias", "SELECT g, SUM(v) AS r, COUNT(*) AS r FROM {src} GROUP BY g"),
        ("dup_key", "SELECT g, g, COUNT(*) AS c FROM {src} GROUP BY g"),
        ("dup_expr", "SELECT g, SUM(v) + 1, SUM(q) + 1 FROM {src} GROUP BY g"),
        ("dup_having", "SELECT g, MIN(v), MIN(q) FROM {src} GROUP BY g HAVING COUNT(*) > 0"),
        ("dup_passthrough", "SELECT id, v, v FROM {src}"),
    ] {
        let (created, leftovers) = v112_ty_create_outcome(tag, select);
        assert!(
            created.starts_with("ERROR: [reflex-unsupported]") && created.contains(" AS "),
            "{select}: {created}"
        );
        assert_eq!(leftovers, "none", "{select}");
    }
}

// The remedy: the same queries with their columns aliased
v112_ty_cells! {
    V112_TY_FIXTURE, false;
    v112_types_duplicate_names_aliased_max => "SELECT g, MAX(t) AS a, MAX(v) AS b FROM {src} GROUP BY g",
    v112_types_duplicate_names_aliased_expr => "SELECT g, SUM(v) + 1 AS a, SUM(q) + 1 AS b FROM {src} GROUP BY g",
    v112_types_same_function_one_unaliased => "SELECT g, MAX(t), MAX(v) AS b FROM {src} GROUP BY g",
}

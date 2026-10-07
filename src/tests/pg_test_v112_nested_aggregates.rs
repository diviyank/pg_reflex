fn v112_na_probe(body: &str) -> String {
    Spi::run(&format!(
        "DO $v112na$ DECLARE r text; BEGIN {body}; \
         PERFORM set_config('v112_na.out', COALESCE(r, 'OK'), true); \
         EXCEPTION WHEN OTHERS THEN \
         PERFORM set_config('v112_na.out', 'RAISED: ' || SQLERRM, true); END $v112na$"
    ))
    .expect("probe");
    Spi::get_one::<String>("SELECT current_setting('v112_na.out')")
        .expect("probe result")
        .unwrap_or_default()
}

/// The text a one-value SELECT returns, or `RAISED: <message>`.
fn v112_na_try(select: &str) -> String {
    v112_na_probe(&format!("EXECUTE $v112q${select}$v112q$ INTO r"))
}

/// `OK`, or `RAISED: <message>`.
fn v112_na_try_dml(dml: &str) -> String {
    v112_na_probe(&format!("EXECUTE $v112q${dml}$v112q$"))
}

fn v112_na_oracle(imv: &str, fresh_sql: &str) -> String {
    v112_na_try(&format!(
        "SELECT COUNT(*)::text FROM (\
            (SELECT * FROM {imv} EXCEPT ALL SELECT * FROM ({fresh_sql}) AS __fresh1) \
            UNION ALL \
            (SELECT * FROM ({fresh_sql}) AS __fresh2 EXCEPT ALL SELECT * FROM {imv}) \
         ) __oracle"
    ))
}

struct V112NaFixture {
    columns: &'static str,
    seed: &'static str,
    steps: [(&'static str, &'static str); 6],
}

const V112_NA_NULLABLE: V112NaFixture = V112NaFixture {
    columns: "g TEXT, v INT, b BOOLEAN, t TEXT, w INT, f DOUBLE PRECISION, n NUMERIC",
    seed: "('a',1,true,'x',2,1.5,1.25),('a',2,false,'y',NULL,3.0,2.50),('a',3,false,'yy',3,4.5,3.75),\
           ('a',NULL,NULL,NULL,NULL,NULL,NULL),('b',5,false,'z',NULL,7.5,6.25),\
           ('n',NULL,NULL,NULL,4,NULL,NULL),('n',NULL,NULL,NULL,NULL,NULL,NULL),\
           ('z',0,NULL,'k',5,0.0,0),('m',-4,false,'q',2,-6.0,-5.00)",
    steps: [
        (
            "ins",
            "INSERT INTO {src}(g, v, b, t, w, f, n) VALUES ('a',7,true,'w',NULL,10.5,8.75),\
             ('c',3,false,'c',2,4.5,3.75),('n',NULL,NULL,NULL,NULL,NULL,NULL),('b',-1,NULL,'a',3,-1.5,-1.25)",
        ),
        (
            "upd",
            "UPDATE {src} SET v = v + 10, b = NOT b, t = t || 'u', f = f + 10, n = n + 10 WHERE g = 'b'",
        ),
        ("move", "UPDATE {src} SET g = 'm' WHERE g = 'z'"),
        (
            "del",
            "DELETE FROM {src} WHERE g = 'c' OR (g = 'a' AND v IN (1, 7))",
        ),
        ("nullonly", "DELETE FROM {src} WHERE v IS NOT NULL"),
        ("empty", "DELETE FROM {src}"),
    ],
};

const V112_NA_NOT_NULL: V112NaFixture = V112NaFixture {
    columns: "g TEXT NOT NULL, v INT NOT NULL, b BOOLEAN NOT NULL, t TEXT NOT NULL, w INT, \
              f DOUBLE PRECISION NOT NULL, n NUMERIC NOT NULL",
    seed: "('a',1,true,'x',2,1.5,1.25),('a',2,false,'y',NULL,3.0,2.50),('a',3,false,'yy',3,4.5,3.75),\
           ('b',5,false,'z',NULL,7.5,6.25),('z',0,false,'k',5,0.0,0),('m',-4,false,'q',2,-6.0,-5.00)",
    steps: [
        (
            "ins",
            "INSERT INTO {src}(g, v, b, t, w, f, n) VALUES ('a',7,true,'w',NULL,10.5,8.75),\
             ('c',3,false,'c',2,4.5,3.75),('b',-1,true,'a',3,-1.5,-1.25)",
        ),
        (
            "upd",
            "UPDATE {src} SET v = v + 10, b = NOT b, t = t || 'u', f = f + 10, n = n + 10 WHERE g = 'b'",
        ),
        ("move", "UPDATE {src} SET g = 'm' WHERE g = 'z'"),
        (
            "del",
            "DELETE FROM {src} WHERE g = 'c' OR (g = 'a' AND v IN (1, 7))",
        ),
        ("shrink", "DELETE FROM {src} WHERE g <> 'a'"),
        ("empty", "DELETE FROM {src}"),
    ],
};

/// Creates the IMV over a fresh source, then runs every DML step of the
/// fixture, comparing the IMV with the query after each. Returns "ok" when every
/// comparison matched, otherwise the outcome class and the per-step mismatch
/// counts.
fn v112_na_cell_on(
    fixture: &V112NaFixture,
    tag: &str,
    select_template: &str,
    deferred: bool,
) -> String {
    let src = format!("v112_na_{tag}_src");
    let imv = format!("v112_na_{tag}");
    Spi::run(&format!(
        "CREATE TABLE {src}(id SERIAL PRIMARY KEY, {})",
        fixture.columns
    ))
    .expect("create source");
    Spi::run(&format!(
        "INSERT INTO {src}(g, v, b, t, w, f, n) VALUES {}",
        fixture.seed
    ))
    .expect("seed");
    let fresh = select_template.replace("{src}", &src);
    let mode = if deferred { "DEFERRED" } else { "IMMEDIATE" };
    let created = v112_na_try(&format!(
        "SELECT create_reflex_ivm('{imv}', $v112sel${fresh}$v112sel$, NULL, 'UNLOGGED', '{mode}')"
    ));
    if created.starts_with("ERROR: [reflex-unsupported]") {
        return format!("REJECTED {created}");
    }
    if created != "CREATE REFLEX INCREMENTAL VIEW" {
        return format!("RAW-ERROR-AT-CREATE {created}");
    }
    let mut steps = vec![("create".to_string(), v112_na_oracle(&imv, &fresh))];
    for (label, dml) in fixture.steps {
        let ran = v112_na_try_dml(&dml.replace("{src}", &src));
        if ran != "OK" {
            steps.push((label.to_string(), ran));
            break;
        }
        if deferred {
            let flushed = v112_na_try(&format!("SELECT reflex_flush_deferred('{src}')"));
            if flushed.starts_with("RAISED") || flushed.starts_with("ERROR") {
                steps.push((label.to_string(), flushed));
                break;
            }
        }
        steps.push((label.to_string(), v112_na_oracle(&imv, &fresh)));
    }
    if steps.iter().all(|(_, outcome)| outcome == "0") {
        return "ok".to_string();
    }
    let detail: Vec<String> = steps.iter().map(|(l, o)| format!("{l}={o}")).collect();
    if steps.iter().all(|(_, outcome)| outcome.parse::<i64>().is_ok()) {
        format!("WRONG-DATA {}", detail.join(" "))
    } else {
        format!("RAW-ERROR {}", detail.join(" "))
    }
}

macro_rules! v112_na_cells {
    ($fixture:expr, $deferred:expr; $($name:ident => $select:expr,)+) => {
        $(
            #[pg_test]
            fn $name() {
                let tag = stringify!($name).trim_start_matches("v112_na_");
                assert_eq!(
                    v112_na_cell_on(&$fixture, tag, $select, $deferred),
                    "ok",
                    "{}",
                    $select
                );
            }
        )+
    };
}

// SUM(v): at top level, then inside each wrapper, grouped and ungrouped
v112_na_cells! {
    V112_NA_NULLABLE, false;
    v112_na_sum_top_g => "SELECT g, SUM(v) AS r FROM {src} GROUP BY g",
    v112_na_sum_top_u => "SELECT SUM(v) AS r FROM {src}",
    v112_na_sum_round_g => "SELECT g, ROUND(SUM(v), 2) AS r FROM {src} GROUP BY g",
    v112_na_sum_round_u => "SELECT ROUND(SUM(v), 2) AS r FROM {src}",
    v112_na_sum_coalesce_g => "SELECT g, COALESCE(SUM(v), 0) AS r FROM {src} GROUP BY g",
    v112_na_sum_coalesce_u => "SELECT COALESCE(SUM(v), 0) AS r FROM {src}",
    v112_na_sum_plus_g => "SELECT g, SUM(v) + 1 AS r FROM {src} GROUP BY g",
    v112_na_sum_plus_u => "SELECT SUM(v) + 1 AS r FROM {src}",
    v112_na_sum_ratio_g => "SELECT g, SUM(v) / NULLIF(COUNT(*), 0) AS r FROM {src} GROUP BY g",
    v112_na_sum_ratio_u => "SELECT SUM(v) / NULLIF(COUNT(*), 0) AS r FROM {src}",
    v112_na_sum_case_g => "SELECT g, CASE WHEN SUM(v) > 0 THEN SUM(v) ELSE 0 END AS r FROM {src} GROUP BY g",
    v112_na_sum_case_u => "SELECT CASE WHEN SUM(v) > 0 THEN SUM(v) ELSE 0 END AS r FROM {src}",
    v112_na_sum_cast_g => "SELECT g, (SUM(v) + 1)::numeric AS r FROM {src} GROUP BY g",
    v112_na_sum_cast_u => "SELECT (SUM(v) + 1)::numeric AS r FROM {src}",
}

// COUNT(*): at top level, then inside each wrapper, grouped and ungrouped
v112_na_cells! {
    V112_NA_NULLABLE, false;
    v112_na_cnt_top_g => "SELECT g, COUNT(*) AS r FROM {src} GROUP BY g",
    v112_na_cnt_top_u => "SELECT COUNT(*) AS r FROM {src}",
    v112_na_cnt_round_g => "SELECT g, ROUND(COUNT(*), 2) AS r FROM {src} GROUP BY g",
    v112_na_cnt_round_u => "SELECT ROUND(COUNT(*), 2) AS r FROM {src}",
    v112_na_cnt_coalesce_g => "SELECT g, COALESCE(COUNT(*), 0) AS r FROM {src} GROUP BY g",
    v112_na_cnt_coalesce_u => "SELECT COALESCE(COUNT(*), 0) AS r FROM {src}",
    v112_na_cnt_plus_g => "SELECT g, COUNT(*) + 1 AS r FROM {src} GROUP BY g",
    v112_na_cnt_plus_u => "SELECT COUNT(*) + 1 AS r FROM {src}",
    v112_na_cnt_ratio_g => "SELECT g, COUNT(*) / NULLIF(COUNT(*), 0) AS r FROM {src} GROUP BY g",
    v112_na_cnt_ratio_u => "SELECT COUNT(*) / NULLIF(COUNT(*), 0) AS r FROM {src}",
    v112_na_cnt_case_g => "SELECT g, CASE WHEN COUNT(*) > 0 THEN COUNT(*) ELSE 0 END AS r FROM {src} GROUP BY g",
    v112_na_cnt_case_u => "SELECT CASE WHEN COUNT(*) > 0 THEN COUNT(*) ELSE 0 END AS r FROM {src}",
    v112_na_cnt_cast_g => "SELECT g, (COUNT(*) + 1)::numeric AS r FROM {src} GROUP BY g",
    v112_na_cnt_cast_u => "SELECT (COUNT(*) + 1)::numeric AS r FROM {src}",
}

// COUNT(v): at top level, then inside each wrapper, grouped and ungrouped
v112_na_cells! {
    V112_NA_NULLABLE, false;
    v112_na_cntv_top_g => "SELECT g, COUNT(v) AS r FROM {src} GROUP BY g",
    v112_na_cntv_top_u => "SELECT COUNT(v) AS r FROM {src}",
    v112_na_cntv_round_g => "SELECT g, ROUND(COUNT(v), 2) AS r FROM {src} GROUP BY g",
    v112_na_cntv_round_u => "SELECT ROUND(COUNT(v), 2) AS r FROM {src}",
    v112_na_cntv_coalesce_g => "SELECT g, COALESCE(COUNT(v), 0) AS r FROM {src} GROUP BY g",
    v112_na_cntv_coalesce_u => "SELECT COALESCE(COUNT(v), 0) AS r FROM {src}",
    v112_na_cntv_plus_g => "SELECT g, COUNT(v) + 1 AS r FROM {src} GROUP BY g",
    v112_na_cntv_plus_u => "SELECT COUNT(v) + 1 AS r FROM {src}",
    v112_na_cntv_ratio_g => "SELECT g, COUNT(v) / NULLIF(COUNT(*), 0) AS r FROM {src} GROUP BY g",
    v112_na_cntv_ratio_u => "SELECT COUNT(v) / NULLIF(COUNT(*), 0) AS r FROM {src}",
    v112_na_cntv_case_g => "SELECT g, CASE WHEN COUNT(v) > 0 THEN COUNT(v) ELSE 0 END AS r FROM {src} GROUP BY g",
    v112_na_cntv_case_u => "SELECT CASE WHEN COUNT(v) > 0 THEN COUNT(v) ELSE 0 END AS r FROM {src}",
    v112_na_cntv_cast_g => "SELECT g, (COUNT(v) + 1)::numeric AS r FROM {src} GROUP BY g",
    v112_na_cntv_cast_u => "SELECT (COUNT(v) + 1)::numeric AS r FROM {src}",
}

// AVG(v): at top level, then inside each wrapper, grouped and ungrouped
v112_na_cells! {
    V112_NA_NULLABLE, false;
    v112_na_avg_top_g => "SELECT g, AVG(v) AS r FROM {src} GROUP BY g",
    v112_na_avg_top_u => "SELECT AVG(v) AS r FROM {src}",
    v112_na_avg_round_g => "SELECT g, ROUND(AVG(v), 2) AS r FROM {src} GROUP BY g",
    v112_na_avg_round_u => "SELECT ROUND(AVG(v), 2) AS r FROM {src}",
    v112_na_avg_coalesce_g => "SELECT g, COALESCE(AVG(v), 0) AS r FROM {src} GROUP BY g",
    v112_na_avg_coalesce_u => "SELECT COALESCE(AVG(v), 0) AS r FROM {src}",
    v112_na_avg_plus_g => "SELECT g, AVG(v) + 1 AS r FROM {src} GROUP BY g",
    v112_na_avg_plus_u => "SELECT AVG(v) + 1 AS r FROM {src}",
    v112_na_avg_ratio_g => "SELECT g, AVG(v) / NULLIF(COUNT(*), 0) AS r FROM {src} GROUP BY g",
    v112_na_avg_ratio_u => "SELECT AVG(v) / NULLIF(COUNT(*), 0) AS r FROM {src}",
    v112_na_avg_case_g => "SELECT g, CASE WHEN AVG(v) > 0 THEN AVG(v) ELSE 0 END AS r FROM {src} GROUP BY g",
    v112_na_avg_case_u => "SELECT CASE WHEN AVG(v) > 0 THEN AVG(v) ELSE 0 END AS r FROM {src}",
    v112_na_avg_cast_g => "SELECT g, (AVG(v) + 1)::numeric AS r FROM {src} GROUP BY g",
    v112_na_avg_cast_u => "SELECT (AVG(v) + 1)::numeric AS r FROM {src}",
}

// MIN(v): at top level, then inside each wrapper, grouped and ungrouped
v112_na_cells! {
    V112_NA_NULLABLE, false;
    v112_na_min_top_g => "SELECT g, MIN(v) AS r FROM {src} GROUP BY g",
    v112_na_min_top_u => "SELECT MIN(v) AS r FROM {src}",
    v112_na_min_round_g => "SELECT g, ROUND(MIN(v), 2) AS r FROM {src} GROUP BY g",
    v112_na_min_round_u => "SELECT ROUND(MIN(v), 2) AS r FROM {src}",
    v112_na_min_coalesce_g => "SELECT g, COALESCE(MIN(v), 0) AS r FROM {src} GROUP BY g",
    v112_na_min_coalesce_u => "SELECT COALESCE(MIN(v), 0) AS r FROM {src}",
    v112_na_min_plus_g => "SELECT g, MIN(v) + 1 AS r FROM {src} GROUP BY g",
    v112_na_min_plus_u => "SELECT MIN(v) + 1 AS r FROM {src}",
    v112_na_min_ratio_g => "SELECT g, MIN(v) / NULLIF(COUNT(*), 0) AS r FROM {src} GROUP BY g",
    v112_na_min_ratio_u => "SELECT MIN(v) / NULLIF(COUNT(*), 0) AS r FROM {src}",
    v112_na_min_case_g => "SELECT g, CASE WHEN MIN(v) > 0 THEN MIN(v) ELSE 0 END AS r FROM {src} GROUP BY g",
    v112_na_min_case_u => "SELECT CASE WHEN MIN(v) > 0 THEN MIN(v) ELSE 0 END AS r FROM {src}",
    v112_na_min_cast_g => "SELECT g, (MIN(v) + 1)::numeric AS r FROM {src} GROUP BY g",
    v112_na_min_cast_u => "SELECT (MIN(v) + 1)::numeric AS r FROM {src}",
}

// MAX(v): at top level, then inside each wrapper, grouped and ungrouped
v112_na_cells! {
    V112_NA_NULLABLE, false;
    v112_na_max_top_g => "SELECT g, MAX(v) AS r FROM {src} GROUP BY g",
    v112_na_max_top_u => "SELECT MAX(v) AS r FROM {src}",
    v112_na_max_round_g => "SELECT g, ROUND(MAX(v), 2) AS r FROM {src} GROUP BY g",
    v112_na_max_round_u => "SELECT ROUND(MAX(v), 2) AS r FROM {src}",
    v112_na_max_coalesce_g => "SELECT g, COALESCE(MAX(v), 0) AS r FROM {src} GROUP BY g",
    v112_na_max_coalesce_u => "SELECT COALESCE(MAX(v), 0) AS r FROM {src}",
    v112_na_max_plus_g => "SELECT g, MAX(v) + 1 AS r FROM {src} GROUP BY g",
    v112_na_max_plus_u => "SELECT MAX(v) + 1 AS r FROM {src}",
    v112_na_max_ratio_g => "SELECT g, MAX(v) / NULLIF(COUNT(*), 0) AS r FROM {src} GROUP BY g",
    v112_na_max_ratio_u => "SELECT MAX(v) / NULLIF(COUNT(*), 0) AS r FROM {src}",
    v112_na_max_case_g => "SELECT g, CASE WHEN MAX(v) > 0 THEN MAX(v) ELSE 0 END AS r FROM {src} GROUP BY g",
    v112_na_max_case_u => "SELECT CASE WHEN MAX(v) > 0 THEN MAX(v) ELSE 0 END AS r FROM {src}",
    v112_na_max_cast_g => "SELECT g, (MAX(v) + 1)::numeric AS r FROM {src} GROUP BY g",
    v112_na_max_cast_u => "SELECT (MAX(v) + 1)::numeric AS r FROM {src}",
}

// BOOL_OR(b): at top level, then inside each wrapper, grouped and ungrouped
v112_na_cells! {
    V112_NA_NULLABLE, false;
    v112_na_bor_top_g => "SELECT g, BOOL_OR(b) AS r FROM {src} GROUP BY g",
    v112_na_bor_top_u => "SELECT BOOL_OR(b) AS r FROM {src}",
    v112_na_bor_coalesce_g => "SELECT g, COALESCE(BOOL_OR(b), false) AS r FROM {src} GROUP BY g",
    v112_na_bor_coalesce_u => "SELECT COALESCE(BOOL_OR(b), false) AS r FROM {src}",
    v112_na_bor_not_g => "SELECT g, NOT BOOL_OR(b) AS r FROM {src} GROUP BY g",
    v112_na_bor_not_u => "SELECT NOT BOOL_OR(b) AS r FROM {src}",
    v112_na_bor_case_g => "SELECT g, CASE WHEN BOOL_OR(b) THEN 1 ELSE 0 END AS r FROM {src} GROUP BY g",
    v112_na_bor_case_u => "SELECT CASE WHEN BOOL_OR(b) THEN 1 ELSE 0 END AS r FROM {src}",
    v112_na_bor_and_g => "SELECT g, BOOL_OR(b) AND COUNT(*) > 1 AS r FROM {src} GROUP BY g",
    v112_na_bor_and_u => "SELECT BOOL_OR(b) AND COUNT(*) > 1 AS r FROM {src}",
    v112_na_bor_cast_g => "SELECT g, (NOT BOOL_OR(b))::text AS r FROM {src} GROUP BY g",
    v112_na_bor_cast_u => "SELECT (NOT BOOL_OR(b))::text AS r FROM {src}",
}

// FILTER on the aggregate, at top level and nested
v112_na_cells! {
    V112_NA_NULLABLE, false;
    v112_na_flt_sum_top_g => "SELECT g, SUM(v) FILTER (WHERE v > 1) AS r FROM {src} GROUP BY g",
    v112_na_flt_sum_g => "SELECT g, ROUND(SUM(v) FILTER (WHERE v > 1), 2) AS r FROM {src} GROUP BY g",
    v112_na_flt_sum_u => "SELECT COALESCE(SUM(v) FILTER (WHERE v > 1), 0) AS r FROM {src}",
    v112_na_flt_cnt_top_g => "SELECT g, COUNT(*) FILTER (WHERE v > 1) AS r FROM {src} GROUP BY g",
    v112_na_flt_cnt_g => "SELECT g, COUNT(*) FILTER (WHERE v > 1) + 1 AS r FROM {src} GROUP BY g",
    v112_na_flt_cnt_u => "SELECT COUNT(*) FILTER (WHERE v > 1) + 1 AS r FROM {src}",
    v112_na_flt_cntv_g => "SELECT g, COUNT(v) FILTER (WHERE v > 1) + 1 AS r FROM {src} GROUP BY g",
    v112_na_flt_avg_top_g => "SELECT g, AVG(v) FILTER (WHERE v > 1) AS r FROM {src} GROUP BY g",
    v112_na_flt_avg_g => "SELECT g, ROUND(AVG(v) FILTER (WHERE v > 1), 2) AS r FROM {src} GROUP BY g",
    v112_na_flt_avg_u => "SELECT ROUND(AVG(v) FILTER (WHERE v > 1), 2) AS r FROM {src}",
    v112_na_flt_min_top_g => "SELECT g, MIN(v) FILTER (WHERE v > 1) AS r FROM {src} GROUP BY g",
    v112_na_flt_min_g => "SELECT g, COALESCE(MIN(v) FILTER (WHERE v > 1), 0) AS r FROM {src} GROUP BY g",
    v112_na_flt_max_g => "SELECT g, MAX(v) FILTER (WHERE v > 1) + 1 AS r FROM {src} GROUP BY g",
    v112_na_flt_max_u => "SELECT MAX(v) FILTER (WHERE v > 1) + 1 AS r FROM {src}",
    v112_na_flt_bor_top_g => "SELECT g, BOOL_OR(b) FILTER (WHERE v > 1) AS r FROM {src} GROUP BY g",
    v112_na_flt_bor_g => "SELECT g, COALESCE(BOOL_OR(b) FILTER (WHERE v > 1), false) AS r FROM {src} GROUP BY g",
    v112_na_flt_both_g => "SELECT g, SUM(v) AS s, SUM(v) FILTER (WHERE v > 1) + 1 AS f, COUNT(*) FILTER (WHERE b) * 2 AS c FROM {src} GROUP BY g",
}

// MIN / MAX over a text column
v112_na_cells! {
    V112_NA_NULLABLE, false;
    v112_na_txt_min_top_g => "SELECT g, MIN(t) AS r FROM {src} GROUP BY g",
    v112_na_txt_min_upper_g => "SELECT g, upper(MIN(t)) AS r FROM {src} GROUP BY g",
    v112_na_txt_min_upper_u => "SELECT upper(MIN(t)) AS r FROM {src}",
    v112_na_txt_max_coalesce_g => "SELECT g, COALESCE(MAX(t), '-') AS r FROM {src} GROUP BY g",
    v112_na_txt_max_coalesce_u => "SELECT COALESCE(MAX(t), '-') AS r FROM {src}",
    v112_na_txt_min_concat_g => "SELECT g, MIN(t) || 'x' AS r FROM {src} GROUP BY g",
    v112_na_txt_max_case_g => "SELECT g, CASE WHEN MAX(t) > 'm' THEN MAX(t) ELSE 'low' END AS r FROM {src} GROUP BY g",
    v112_na_txt_minmax_concat_g => "SELECT g, (MIN(t) || MAX(t))::text AS r FROM {src} GROUP BY g",
}

// The same aggregate at top level and nested, several aggregates in one expression
v112_na_cells! {
    V112_NA_NULLABLE, false;
    v112_na_mix_sum_top_then_nested => "SELECT g, SUM(v) AS s, SUM(v) * 2 AS d FROM {src} GROUP BY g",
    v112_na_mix_sum_nested_then_top => "SELECT g, SUM(v) * 2 AS d, SUM(v) AS s FROM {src} GROUP BY g",
    v112_na_mix_sum_top_then_nested_u => "SELECT SUM(v) AS s, SUM(v) * 2 AS d FROM {src}",
    v112_na_mix_avg_top_then_nested => "SELECT g, AVG(v) AS a, ROUND(AVG(v), 2) AS r FROM {src} GROUP BY g",
    v112_na_mix_avg_nested_then_top => "SELECT g, ROUND(AVG(v), 2) AS r, AVG(v) AS a FROM {src} GROUP BY g",
    v112_na_mix_avg_top_then_nested_u => "SELECT AVG(v) AS a, ROUND(AVG(v), 2) AS r FROM {src}",
    v112_na_mix_sum_top_avg_nested => "SELECT g, SUM(v) AS s, ROUND(AVG(v), 2) AS r FROM {src} GROUP BY g",
    v112_na_mix_count_top_avg_nested => "SELECT g, COUNT(v) AS c, ROUND(AVG(v), 2) AS r FROM {src} GROUP BY g",
    v112_na_mix_avg_top_sum_nested => "SELECT g, AVG(v) AS a, SUM(v) + 1 AS s FROM {src} GROUP BY g",
    v112_na_mix_sum_over_count => "SELECT g, SUM(v) / NULLIF(COUNT(v), 0) AS r FROM {src} GROUP BY g",
    v112_na_mix_max_minus_min => "SELECT g, MAX(v) - MIN(v) AS r FROM {src} GROUP BY g",
    v112_na_mix_greatest_min => "SELECT g, GREATEST(MIN(v), 0) AS r FROM {src} GROUP BY g",
    v112_na_mix_min_top_then_nested => "SELECT g, MIN(v) AS lo, MIN(v) + 1 AS lo1, MAX(v) AS hi, MAX(v) * 2 AS hi2 FROM {src} GROUP BY g",
    v112_na_mix_sum_expr_arg => "SELECT g, ROUND(SUM(v * 2), 2) AS r, COALESCE(AVG(v + 1), 0) AS a FROM {src} GROUP BY g",
    v112_na_mix_cnt_top_then_nested => "SELECT g, COUNT(*) AS c, COUNT(*) * 2 AS d FROM {src} GROUP BY g",
    v112_na_mix_avg_as_divisor => "SELECT g, 100 / NULLIF(AVG(n), 0) AS r FROM {src} GROUP BY g",
    v112_na_mix_avg_as_subtrahend => "SELECT g, 10 - AVG(n) AS r, 2 * AVG(n) AS d FROM {src} GROUP BY g",
    v112_na_mix_sum_as_subtrahend => "SELECT g, 10 - SUM(v) AS r, -SUM(v) AS m FROM {src} GROUP BY g",
    v112_na_mix_avg_divides => "SELECT g, 1000 / AVG(v + 100) AS r FROM {src} GROUP BY g",
    v112_na_mix_avg_cast_plus => "SELECT g, AVG(v)::int + 1 AS r FROM {src} GROUP BY g",
    v112_na_mix_avg_squared => "SELECT g, AVG(n) ^ 2 AS r FROM {src} GROUP BY g",
    v112_na_mix_deep => "SELECT g, ROUND(COALESCE(AVG(v), 0) + COALESCE(SUM(v), 0), 2) AS r FROM {src} GROUP BY g",
    v112_na_mix_deep_u => "SELECT CASE WHEN COUNT(*) > 2 THEN ROUND(AVG(v), 1) ELSE SUM(v) + 1 END AS r FROM {src}",
}

// Result type of the nested aggregate: integer, numeric and float arguments
v112_na_cells! {
    V112_NA_NULLABLE, false;
    v112_na_typ_sum_cast_ratio => "SELECT g, SUM(v)::numeric / NULLIF(COUNT(*), 0) AS r FROM {src} GROUP BY g",
    v112_na_typ_sum_times_ratio => "SELECT g, SUM(v) * 1.0 / NULLIF(COUNT(*), 0) AS r FROM {src} GROUP BY g",
    v112_na_typ_sum_int_halved => "SELECT g, SUM(v) / 2 AS r FROM {src} GROUP BY g",
    v112_na_typ_sum_int_halved_u => "SELECT SUM(v) / 2 AS r FROM {src}",
    v112_na_typ_sum_modulo => "SELECT g, SUM(v) % 2 AS r FROM {src} GROUP BY g",
    v112_na_typ_sum_numeric_ratio => "SELECT g, SUM(n) / NULLIF(COUNT(*), 0) AS r FROM {src} GROUP BY g",
    v112_na_typ_sum_numeric_plus => "SELECT g, SUM(n) + 1 AS r FROM {src} GROUP BY g",
    v112_na_typ_sum_float_half => "SELECT g, SUM(f) / 2 AS r FROM {src} GROUP BY g",
    v112_na_typ_sum_over_sum => "SELECT g, SUM(n) / NULLIF(SUM(f), 0) AS r FROM {src} GROUP BY g",
    v112_na_typ_avg_numeric_round => "SELECT g, ROUND(AVG(n), 2) AS r FROM {src} GROUP BY g",
    v112_na_typ_avg_float_plus => "SELECT g, AVG(f) + 1 AS r FROM {src} GROUP BY g",
    v112_na_typ_avg_int_halved => "SELECT g, AVG(v) / 2 AS r FROM {src} GROUP BY g",
    v112_na_typ_min_ratio => "SELECT g, MIN(v) / 2 AS r FROM {src} GROUP BY g",
    v112_na_typ_count_ratio => "SELECT g, COUNT(v) / 2 AS r FROM {src} GROUP BY g",
    v112_na_typ_sum_multiplier => "SELECT g, SUM(v * COALESCE(w, 1)) + 1 AS r FROM {src} GROUP BY g",
    v112_na_typ_sum_top_multiplier_nested => "SELECT g, SUM(v) AS s, SUM(v * COALESCE(w, 1)) + 1 AS r FROM {src} GROUP BY g",
}

// NOT NULL arguments, whose non-NULL companion counter is elided
v112_na_cells! {
    V112_NA_NOT_NULL, false;
    v112_na_nn_sum_top_g => "SELECT g, SUM(v) AS r FROM {src} GROUP BY g",
    v112_na_nn_sum_top_u => "SELECT SUM(v) AS r FROM {src}",
    v112_na_nn_sum_plus_g => "SELECT g, SUM(v) + 1 AS r FROM {src} GROUP BY g",
    v112_na_nn_sum_plus_u => "SELECT SUM(v) + 1 AS r FROM {src}",
    v112_na_nn_sum_round_g => "SELECT g, ROUND(SUM(v), 2) AS r FROM {src} GROUP BY g",
    v112_na_nn_sum_coalesce_u => "SELECT COALESCE(SUM(v), 0) AS r FROM {src}",
    v112_na_nn_sum_ratio_g => "SELECT g, SUM(v) / NULLIF(COUNT(*), 0) AS r FROM {src} GROUP BY g",
    v112_na_nn_sum_ratio_u => "SELECT SUM(v) / NULLIF(COUNT(*), 0) AS r FROM {src}",
    v112_na_nn_sum_numeric_ratio_g => "SELECT g, SUM(n) / NULLIF(COUNT(*), 0) AS r FROM {src} GROUP BY g",
    v112_na_nn_sum_over_sum_g => "SELECT g, SUM(n) / NULLIF(SUM(f), 0) AS r FROM {src} GROUP BY g",
    v112_na_nn_sum_top_then_nested_g => "SELECT g, SUM(v) AS s, SUM(v) * 2 AS d FROM {src} GROUP BY g",
    v112_na_nn_sum_nested_then_top_g => "SELECT g, SUM(v) * 2 AS d, SUM(v) AS s FROM {src} GROUP BY g",
    v112_na_nn_sum_filter_g => "SELECT g, ROUND(SUM(v) FILTER (WHERE v > 1), 2) AS r FROM {src} GROUP BY g",
    v112_na_nn_sum_multiplier_g => "SELECT g, SUM(v * COALESCE(w, 1)) + 1 AS r FROM {src} GROUP BY g",
    v112_na_nn_sum_multiplier_u => "SELECT SUM(v * COALESCE(w, 1)) + 1 AS r FROM {src}",
    v112_na_nn_sum_top_multiplier_nested_g => "SELECT g, SUM(v) AS s, SUM(v * COALESCE(w, 1)) + 1 AS r FROM {src} GROUP BY g",
    v112_na_nn_avg_round_g => "SELECT g, ROUND(AVG(v), 2) AS r FROM {src} GROUP BY g",
    v112_na_nn_avg_round_u => "SELECT ROUND(AVG(v), 2) AS r FROM {src}",
    v112_na_nn_cnt_plus_u => "SELECT COUNT(*) + 1 AS r FROM {src}",
    v112_na_nn_min_plus_u => "SELECT MIN(v) + 1 AS r FROM {src}",
    v112_na_nn_bor_top_g => "SELECT g, BOOL_OR(b) AS r FROM {src} GROUP BY g",
    v112_na_nn_bor_coalesce_g => "SELECT g, COALESCE(BOOL_OR(b), false) AS r FROM {src} GROUP BY g",
    v112_na_nn_bor_not_u => "SELECT NOT BOOL_OR(b) AS r FROM {src}",
    v112_na_nn_bor_and_g => "SELECT g, BOOL_OR(b) AND COUNT(*) > 1 AS r FROM {src} GROUP BY g",
}

// DEFERRED mode
v112_na_cells! {
    V112_NA_NULLABLE, true;
    v112_na_dfr_sum_g => "SELECT g, COALESCE(SUM(v), 0) AS r FROM {src} GROUP BY g",
    v112_na_dfr_sum_plus_g => "SELECT g, SUM(v) + 1 AS r FROM {src} GROUP BY g",
    v112_na_dfr_sum_u => "SELECT SUM(v) + 1 AS r FROM {src}",
    v112_na_dfr_sum_ratio_g => "SELECT g, SUM(v) / NULLIF(COUNT(*), 0) AS r FROM {src} GROUP BY g",
    v112_na_dfr_cnt_g => "SELECT g, COUNT(*) + 1 AS r FROM {src} GROUP BY g",
    v112_na_dfr_cntv_g => "SELECT g, COUNT(v) + 1 AS r FROM {src} GROUP BY g",
    v112_na_dfr_avg_g => "SELECT g, ROUND(AVG(v), 2) AS r FROM {src} GROUP BY g",
    v112_na_dfr_avg_u => "SELECT ROUND(AVG(v), 2) AS r FROM {src}",
    v112_na_dfr_avg_top_g => "SELECT g, AVG(v) AS r FROM {src} GROUP BY g",
    v112_na_dfr_min_g => "SELECT g, MIN(v) + 1 AS r FROM {src} GROUP BY g",
    v112_na_dfr_max_g => "SELECT g, COALESCE(MAX(v), 0) AS r FROM {src} GROUP BY g",
    v112_na_dfr_max_u => "SELECT COALESCE(MAX(v), 0) AS r FROM {src}",
    v112_na_dfr_bor_g => "SELECT g, COALESCE(BOOL_OR(b), false) AS r FROM {src} GROUP BY g",
    v112_na_dfr_sum_filter_g => "SELECT g, ROUND(SUM(v) FILTER (WHERE v > 1), 2) AS r FROM {src} GROUP BY g",
    v112_na_dfr_avg_filter_g => "SELECT g, ROUND(AVG(v) FILTER (WHERE v > 1), 2) AS r FROM {src} GROUP BY g",
    v112_na_dfr_cnt_filter_u => "SELECT COUNT(*) FILTER (WHERE v > 1) + 1 AS r FROM {src}",
    v112_na_dfr_sum_top_g => "SELECT g, SUM(v) AS r FROM {src} GROUP BY g",
}

// DEFERRED mode, NOT NULL arguments
v112_na_cells! {
    V112_NA_NOT_NULL, true;
    v112_na_dfr_nn_sum_plus_u => "SELECT SUM(v) + 1 AS r FROM {src}",
    v112_na_dfr_nn_bor_coalesce_g => "SELECT g, COALESCE(BOOL_OR(b), false) AS r FROM {src} GROUP BY g",
}

#[pg_test]
fn v112_na_rounded_average_is_the_average() {
    Spi::run("CREATE TABLE v112_na_repro_src(id SERIAL PRIMARY KEY, g TEXT, v INT)").expect("create");
    Spi::run("INSERT INTO v112_na_repro_src(g, v) VALUES ('a',1),('a',2),('b',5)").expect("seed");
    let q = "SELECT ROUND(AVG(v), 2) AS a FROM v112_na_repro_src";
    assert_eq!(
        crate::create_reflex_ivm("v112_na_repro", q, None, None, None, None),
        "CREATE REFLEX INCREMENTAL VIEW"
    );
    let stored = Spi::get_one::<String>("SELECT a::text FROM v112_na_repro")
        .expect("read")
        .expect("one row");
    assert_eq!(stored, "2.67");
    assert_imv_correct("v112_na_repro", q);
}

// Nested aggregates reached through a CTE, beside HAVING, and under a WHERE
v112_na_cells! {
    V112_NA_NULLABLE, false;
    v112_na_ctx_cte_avg => "WITH v112_na_ctx_body AS (SELECT g, ROUND(AVG(v), 2) AS m FROM {src} GROUP BY g) SELECT g, m FROM v112_na_ctx_body",
    v112_na_ctx_cte_outer_expr => "WITH v112_na_ctx_agg AS (SELECT g, AVG(v) AS m, SUM(v) AS s FROM {src} GROUP BY g) SELECT g, ROUND(m, 2) AS r, s + 1 AS s1 FROM v112_na_ctx_agg",
    v112_na_ctx_having_sum => "SELECT g, SUM(v) + 1 AS r FROM {src} GROUP BY g HAVING SUM(v) > 0",
    v112_na_ctx_having_count => "SELECT g, ROUND(AVG(v), 2) AS r FROM {src} GROUP BY g HAVING COUNT(*) > 1",
    v112_na_ctx_where_sum => "SELECT g, SUM(v) / 2 AS r, ROUND(AVG(n), 1) AS a FROM {src} WHERE v <> 2 OR v IS NULL GROUP BY g",
}

const V112_NA_EXTREMA: V112NaFixture = V112NaFixture {
    columns: "g TEXT, v INT, b BOOLEAN, t TEXT, w INT, f DOUBLE PRECISION, n NUMERIC, \
              q BIGINT GENERATED ALWAYS AS (v::bigint * 3) STORED",
    seed: V112_NA_NULLABLE.seed,
    steps: V112_NA_NULLABLE.steps,
};

/// `same <type>` when the IMV's `r` column has the type the query gives it,
/// otherwise both types.
fn v112_na_result_type(tag: &str, select_template: &str) -> String {
    let fresh = select_template.replace("{src}", &format!("v112_na_{tag}_src"));
    let imv_type = v112_na_try(&format!(
        "SELECT pg_typeof((SELECT r FROM v112_na_{tag} LIMIT 1))::text"
    ));
    let query_type = v112_na_try(&format!(
        "SELECT pg_typeof((SELECT r FROM ({fresh}) AS __typed LIMIT 1))::text"
    ));
    if imv_type == query_type {
        format!("same {imv_type}")
    } else {
        format!("imv {imv_type} query {query_type}")
    }
}

macro_rules! v112_na_typed_cells {
    ($fixture:expr; $($name:ident => $select:expr,)+) => {
        $(
            #[pg_test]
            fn $name() {
                let tag = stringify!($name).trim_start_matches("v112_na_");
                let outcome = v112_na_cell_on(&$fixture, tag, $select, false);
                let result_type = v112_na_result_type(tag, $select);
                assert!(
                    outcome == "ok" && result_type.starts_with("same "),
                    "{}: {} | {}",
                    $select,
                    outcome,
                    result_type
                );
            }
        )+
    };
}

// Nested MIN: plain, expression and filtered arguments of each numeric type, result type included
v112_na_typed_cells! {
    V112_NA_EXTREMA;
    v112_na_x_min_int_half_g => "SELECT g, MIN(v) / 2 AS r FROM {src} GROUP BY g",
    v112_na_x_min_int_half_u => "SELECT MIN(v) / 2 AS r FROM {src}",
    v112_na_x_min_int_plus_g => "SELECT g, MIN(v) + 1 AS r FROM {src} GROUP BY g",
    v112_na_x_min_int_plus_u => "SELECT MIN(v) + 1 AS r FROM {src}",
    v112_na_x_min_int_round_g => "SELECT g, ROUND(MIN(v), 2) AS r FROM {src} GROUP BY g",
    v112_na_x_min_int_round_u => "SELECT ROUND(MIN(v), 2) AS r FROM {src}",
    v112_na_x_min_int_coalesce_g => "SELECT g, COALESCE(MIN(v), 0) AS r FROM {src} GROUP BY g",
    v112_na_x_min_int_coalesce_u => "SELECT COALESCE(MIN(v), 0) AS r FROM {src}",
    v112_na_x_min_big_half_g => "SELECT g, MIN(q) / 2 AS r FROM {src} GROUP BY g",
    v112_na_x_min_big_half_u => "SELECT MIN(q) / 2 AS r FROM {src}",
    v112_na_x_min_big_plus_g => "SELECT g, MIN(q) + 1 AS r FROM {src} GROUP BY g",
    v112_na_x_min_big_plus_u => "SELECT MIN(q) + 1 AS r FROM {src}",
    v112_na_x_min_big_round_g => "SELECT g, ROUND(MIN(q), 2) AS r FROM {src} GROUP BY g",
    v112_na_x_min_big_round_u => "SELECT ROUND(MIN(q), 2) AS r FROM {src}",
    v112_na_x_min_big_coalesce_g => "SELECT g, COALESCE(MIN(q), 0) AS r FROM {src} GROUP BY g",
    v112_na_x_min_big_coalesce_u => "SELECT COALESCE(MIN(q), 0) AS r FROM {src}",
    v112_na_x_min_num_half_g => "SELECT g, MIN(n) / 2 AS r FROM {src} GROUP BY g",
    v112_na_x_min_num_half_u => "SELECT MIN(n) / 2 AS r FROM {src}",
    v112_na_x_min_num_plus_g => "SELECT g, MIN(n) + 1 AS r FROM {src} GROUP BY g",
    v112_na_x_min_num_plus_u => "SELECT MIN(n) + 1 AS r FROM {src}",
    v112_na_x_min_num_round_g => "SELECT g, ROUND(MIN(n), 2) AS r FROM {src} GROUP BY g",
    v112_na_x_min_num_round_u => "SELECT ROUND(MIN(n), 2) AS r FROM {src}",
    v112_na_x_min_num_coalesce_g => "SELECT g, COALESCE(MIN(n), 0) AS r FROM {src} GROUP BY g",
    v112_na_x_min_num_coalesce_u => "SELECT COALESCE(MIN(n), 0) AS r FROM {src}",
    v112_na_x_min_intx_half_g => "SELECT g, MIN(v * 3) / 2 AS r FROM {src} GROUP BY g",
    v112_na_x_min_intx_half_u => "SELECT MIN(v * 3) / 2 AS r FROM {src}",
    v112_na_x_min_intx_plus_g => "SELECT g, MIN(v * 3) + 1 AS r FROM {src} GROUP BY g",
    v112_na_x_min_intx_plus_u => "SELECT MIN(v * 3) + 1 AS r FROM {src}",
    v112_na_x_min_intx_round_g => "SELECT g, ROUND(MIN(v * 3), 2) AS r FROM {src} GROUP BY g",
    v112_na_x_min_intx_round_u => "SELECT ROUND(MIN(v * 3), 2) AS r FROM {src}",
    v112_na_x_min_intx_coalesce_g => "SELECT g, COALESCE(MIN(v * 3), 0) AS r FROM {src} GROUP BY g",
    v112_na_x_min_intx_coalesce_u => "SELECT COALESCE(MIN(v * 3), 0) AS r FROM {src}",
    v112_na_x_min_bigx_half_g => "SELECT g, MIN(q + 1) / 2 AS r FROM {src} GROUP BY g",
    v112_na_x_min_bigx_half_u => "SELECT MIN(q + 1) / 2 AS r FROM {src}",
    v112_na_x_min_bigx_plus_g => "SELECT g, MIN(q + 1) + 1 AS r FROM {src} GROUP BY g",
    v112_na_x_min_bigx_plus_u => "SELECT MIN(q + 1) + 1 AS r FROM {src}",
    v112_na_x_min_bigx_round_g => "SELECT g, ROUND(MIN(q + 1), 2) AS r FROM {src} GROUP BY g",
    v112_na_x_min_bigx_round_u => "SELECT ROUND(MIN(q + 1), 2) AS r FROM {src}",
    v112_na_x_min_bigx_coalesce_g => "SELECT g, COALESCE(MIN(q + 1), 0) AS r FROM {src} GROUP BY g",
    v112_na_x_min_bigx_coalesce_u => "SELECT COALESCE(MIN(q + 1), 0) AS r FROM {src}",
    v112_na_x_min_numx_half_g => "SELECT g, MIN(n * 2) / 2 AS r FROM {src} GROUP BY g",
    v112_na_x_min_numx_half_u => "SELECT MIN(n * 2) / 2 AS r FROM {src}",
    v112_na_x_min_numx_plus_g => "SELECT g, MIN(n * 2) + 1 AS r FROM {src} GROUP BY g",
    v112_na_x_min_numx_plus_u => "SELECT MIN(n * 2) + 1 AS r FROM {src}",
    v112_na_x_min_numx_round_g => "SELECT g, ROUND(MIN(n * 2), 2) AS r FROM {src} GROUP BY g",
    v112_na_x_min_numx_round_u => "SELECT ROUND(MIN(n * 2), 2) AS r FROM {src}",
    v112_na_x_min_numx_coalesce_g => "SELECT g, COALESCE(MIN(n * 2), 0) AS r FROM {src} GROUP BY g",
    v112_na_x_min_numx_coalesce_u => "SELECT COALESCE(MIN(n * 2), 0) AS r FROM {src}",
    v112_na_x_min_intf_half_g => "SELECT g, MIN(v) FILTER (WHERE v > 1) / 2 AS r FROM {src} GROUP BY g",
    v112_na_x_min_intf_half_u => "SELECT MIN(v) FILTER (WHERE v > 1) / 2 AS r FROM {src}",
    v112_na_x_min_intf_plus_g => "SELECT g, MIN(v) FILTER (WHERE v > 1) + 1 AS r FROM {src} GROUP BY g",
    v112_na_x_min_intf_plus_u => "SELECT MIN(v) FILTER (WHERE v > 1) + 1 AS r FROM {src}",
    v112_na_x_min_intf_round_g => "SELECT g, ROUND(MIN(v) FILTER (WHERE v > 1), 2) AS r FROM {src} GROUP BY g",
    v112_na_x_min_intf_round_u => "SELECT ROUND(MIN(v) FILTER (WHERE v > 1), 2) AS r FROM {src}",
    v112_na_x_min_intf_coalesce_g => "SELECT g, COALESCE(MIN(v) FILTER (WHERE v > 1), 0) AS r FROM {src} GROUP BY g",
    v112_na_x_min_intf_coalesce_u => "SELECT COALESCE(MIN(v) FILTER (WHERE v > 1), 0) AS r FROM {src}",
    v112_na_x_min_bigf_half_g => "SELECT g, MIN(q) FILTER (WHERE v > 1) / 2 AS r FROM {src} GROUP BY g",
    v112_na_x_min_bigf_half_u => "SELECT MIN(q) FILTER (WHERE v > 1) / 2 AS r FROM {src}",
    v112_na_x_min_bigf_plus_g => "SELECT g, MIN(q) FILTER (WHERE v > 1) + 1 AS r FROM {src} GROUP BY g",
    v112_na_x_min_bigf_plus_u => "SELECT MIN(q) FILTER (WHERE v > 1) + 1 AS r FROM {src}",
    v112_na_x_min_bigf_round_g => "SELECT g, ROUND(MIN(q) FILTER (WHERE v > 1), 2) AS r FROM {src} GROUP BY g",
    v112_na_x_min_bigf_round_u => "SELECT ROUND(MIN(q) FILTER (WHERE v > 1), 2) AS r FROM {src}",
    v112_na_x_min_bigf_coalesce_g => "SELECT g, COALESCE(MIN(q) FILTER (WHERE v > 1), 0) AS r FROM {src} GROUP BY g",
    v112_na_x_min_bigf_coalesce_u => "SELECT COALESCE(MIN(q) FILTER (WHERE v > 1), 0) AS r FROM {src}",
    v112_na_x_min_numf_half_g => "SELECT g, MIN(n) FILTER (WHERE v > 1) / 2 AS r FROM {src} GROUP BY g",
    v112_na_x_min_numf_half_u => "SELECT MIN(n) FILTER (WHERE v > 1) / 2 AS r FROM {src}",
    v112_na_x_min_numf_plus_g => "SELECT g, MIN(n) FILTER (WHERE v > 1) + 1 AS r FROM {src} GROUP BY g",
    v112_na_x_min_numf_plus_u => "SELECT MIN(n) FILTER (WHERE v > 1) + 1 AS r FROM {src}",
    v112_na_x_min_numf_round_g => "SELECT g, ROUND(MIN(n) FILTER (WHERE v > 1), 2) AS r FROM {src} GROUP BY g",
    v112_na_x_min_numf_round_u => "SELECT ROUND(MIN(n) FILTER (WHERE v > 1), 2) AS r FROM {src}",
    v112_na_x_min_numf_coalesce_g => "SELECT g, COALESCE(MIN(n) FILTER (WHERE v > 1), 0) AS r FROM {src} GROUP BY g",
    v112_na_x_min_numf_coalesce_u => "SELECT COALESCE(MIN(n) FILTER (WHERE v > 1), 0) AS r FROM {src}",
}

// Nested MAX: plain, expression and filtered arguments of each numeric type, result type included
v112_na_typed_cells! {
    V112_NA_EXTREMA;
    v112_na_x_max_int_half_g => "SELECT g, MAX(v) / 2 AS r FROM {src} GROUP BY g",
    v112_na_x_max_int_half_u => "SELECT MAX(v) / 2 AS r FROM {src}",
    v112_na_x_max_int_plus_g => "SELECT g, MAX(v) + 1 AS r FROM {src} GROUP BY g",
    v112_na_x_max_int_plus_u => "SELECT MAX(v) + 1 AS r FROM {src}",
    v112_na_x_max_int_round_g => "SELECT g, ROUND(MAX(v), 2) AS r FROM {src} GROUP BY g",
    v112_na_x_max_int_round_u => "SELECT ROUND(MAX(v), 2) AS r FROM {src}",
    v112_na_x_max_int_coalesce_g => "SELECT g, COALESCE(MAX(v), 0) AS r FROM {src} GROUP BY g",
    v112_na_x_max_int_coalesce_u => "SELECT COALESCE(MAX(v), 0) AS r FROM {src}",
    v112_na_x_max_big_half_g => "SELECT g, MAX(q) / 2 AS r FROM {src} GROUP BY g",
    v112_na_x_max_big_half_u => "SELECT MAX(q) / 2 AS r FROM {src}",
    v112_na_x_max_big_plus_g => "SELECT g, MAX(q) + 1 AS r FROM {src} GROUP BY g",
    v112_na_x_max_big_plus_u => "SELECT MAX(q) + 1 AS r FROM {src}",
    v112_na_x_max_big_round_g => "SELECT g, ROUND(MAX(q), 2) AS r FROM {src} GROUP BY g",
    v112_na_x_max_big_round_u => "SELECT ROUND(MAX(q), 2) AS r FROM {src}",
    v112_na_x_max_big_coalesce_g => "SELECT g, COALESCE(MAX(q), 0) AS r FROM {src} GROUP BY g",
    v112_na_x_max_big_coalesce_u => "SELECT COALESCE(MAX(q), 0) AS r FROM {src}",
    v112_na_x_max_num_half_g => "SELECT g, MAX(n) / 2 AS r FROM {src} GROUP BY g",
    v112_na_x_max_num_half_u => "SELECT MAX(n) / 2 AS r FROM {src}",
    v112_na_x_max_num_plus_g => "SELECT g, MAX(n) + 1 AS r FROM {src} GROUP BY g",
    v112_na_x_max_num_plus_u => "SELECT MAX(n) + 1 AS r FROM {src}",
    v112_na_x_max_num_round_g => "SELECT g, ROUND(MAX(n), 2) AS r FROM {src} GROUP BY g",
    v112_na_x_max_num_round_u => "SELECT ROUND(MAX(n), 2) AS r FROM {src}",
    v112_na_x_max_num_coalesce_g => "SELECT g, COALESCE(MAX(n), 0) AS r FROM {src} GROUP BY g",
    v112_na_x_max_num_coalesce_u => "SELECT COALESCE(MAX(n), 0) AS r FROM {src}",
    v112_na_x_max_intx_half_g => "SELECT g, MAX(v * 3) / 2 AS r FROM {src} GROUP BY g",
    v112_na_x_max_intx_half_u => "SELECT MAX(v * 3) / 2 AS r FROM {src}",
    v112_na_x_max_intx_plus_g => "SELECT g, MAX(v * 3) + 1 AS r FROM {src} GROUP BY g",
    v112_na_x_max_intx_plus_u => "SELECT MAX(v * 3) + 1 AS r FROM {src}",
    v112_na_x_max_intx_round_g => "SELECT g, ROUND(MAX(v * 3), 2) AS r FROM {src} GROUP BY g",
    v112_na_x_max_intx_round_u => "SELECT ROUND(MAX(v * 3), 2) AS r FROM {src}",
    v112_na_x_max_intx_coalesce_g => "SELECT g, COALESCE(MAX(v * 3), 0) AS r FROM {src} GROUP BY g",
    v112_na_x_max_intx_coalesce_u => "SELECT COALESCE(MAX(v * 3), 0) AS r FROM {src}",
    v112_na_x_max_bigx_half_g => "SELECT g, MAX(q + 1) / 2 AS r FROM {src} GROUP BY g",
    v112_na_x_max_bigx_half_u => "SELECT MAX(q + 1) / 2 AS r FROM {src}",
    v112_na_x_max_bigx_plus_g => "SELECT g, MAX(q + 1) + 1 AS r FROM {src} GROUP BY g",
    v112_na_x_max_bigx_plus_u => "SELECT MAX(q + 1) + 1 AS r FROM {src}",
    v112_na_x_max_bigx_round_g => "SELECT g, ROUND(MAX(q + 1), 2) AS r FROM {src} GROUP BY g",
    v112_na_x_max_bigx_round_u => "SELECT ROUND(MAX(q + 1), 2) AS r FROM {src}",
    v112_na_x_max_bigx_coalesce_g => "SELECT g, COALESCE(MAX(q + 1), 0) AS r FROM {src} GROUP BY g",
    v112_na_x_max_bigx_coalesce_u => "SELECT COALESCE(MAX(q + 1), 0) AS r FROM {src}",
    v112_na_x_max_numx_half_g => "SELECT g, MAX(n * 2) / 2 AS r FROM {src} GROUP BY g",
    v112_na_x_max_numx_half_u => "SELECT MAX(n * 2) / 2 AS r FROM {src}",
    v112_na_x_max_numx_plus_g => "SELECT g, MAX(n * 2) + 1 AS r FROM {src} GROUP BY g",
    v112_na_x_max_numx_plus_u => "SELECT MAX(n * 2) + 1 AS r FROM {src}",
    v112_na_x_max_numx_round_g => "SELECT g, ROUND(MAX(n * 2), 2) AS r FROM {src} GROUP BY g",
    v112_na_x_max_numx_round_u => "SELECT ROUND(MAX(n * 2), 2) AS r FROM {src}",
    v112_na_x_max_numx_coalesce_g => "SELECT g, COALESCE(MAX(n * 2), 0) AS r FROM {src} GROUP BY g",
    v112_na_x_max_numx_coalesce_u => "SELECT COALESCE(MAX(n * 2), 0) AS r FROM {src}",
    v112_na_x_max_intf_half_g => "SELECT g, MAX(v) FILTER (WHERE v > 1) / 2 AS r FROM {src} GROUP BY g",
    v112_na_x_max_intf_half_u => "SELECT MAX(v) FILTER (WHERE v > 1) / 2 AS r FROM {src}",
    v112_na_x_max_intf_plus_g => "SELECT g, MAX(v) FILTER (WHERE v > 1) + 1 AS r FROM {src} GROUP BY g",
    v112_na_x_max_intf_plus_u => "SELECT MAX(v) FILTER (WHERE v > 1) + 1 AS r FROM {src}",
    v112_na_x_max_intf_round_g => "SELECT g, ROUND(MAX(v) FILTER (WHERE v > 1), 2) AS r FROM {src} GROUP BY g",
    v112_na_x_max_intf_round_u => "SELECT ROUND(MAX(v) FILTER (WHERE v > 1), 2) AS r FROM {src}",
    v112_na_x_max_intf_coalesce_g => "SELECT g, COALESCE(MAX(v) FILTER (WHERE v > 1), 0) AS r FROM {src} GROUP BY g",
    v112_na_x_max_intf_coalesce_u => "SELECT COALESCE(MAX(v) FILTER (WHERE v > 1), 0) AS r FROM {src}",
    v112_na_x_max_bigf_half_g => "SELECT g, MAX(q) FILTER (WHERE v > 1) / 2 AS r FROM {src} GROUP BY g",
    v112_na_x_max_bigf_half_u => "SELECT MAX(q) FILTER (WHERE v > 1) / 2 AS r FROM {src}",
    v112_na_x_max_bigf_plus_g => "SELECT g, MAX(q) FILTER (WHERE v > 1) + 1 AS r FROM {src} GROUP BY g",
    v112_na_x_max_bigf_plus_u => "SELECT MAX(q) FILTER (WHERE v > 1) + 1 AS r FROM {src}",
    v112_na_x_max_bigf_round_g => "SELECT g, ROUND(MAX(q) FILTER (WHERE v > 1), 2) AS r FROM {src} GROUP BY g",
    v112_na_x_max_bigf_round_u => "SELECT ROUND(MAX(q) FILTER (WHERE v > 1), 2) AS r FROM {src}",
    v112_na_x_max_bigf_coalesce_g => "SELECT g, COALESCE(MAX(q) FILTER (WHERE v > 1), 0) AS r FROM {src} GROUP BY g",
    v112_na_x_max_bigf_coalesce_u => "SELECT COALESCE(MAX(q) FILTER (WHERE v > 1), 0) AS r FROM {src}",
    v112_na_x_max_numf_half_g => "SELECT g, MAX(n) FILTER (WHERE v > 1) / 2 AS r FROM {src} GROUP BY g",
    v112_na_x_max_numf_half_u => "SELECT MAX(n) FILTER (WHERE v > 1) / 2 AS r FROM {src}",
    v112_na_x_max_numf_plus_g => "SELECT g, MAX(n) FILTER (WHERE v > 1) + 1 AS r FROM {src} GROUP BY g",
    v112_na_x_max_numf_plus_u => "SELECT MAX(n) FILTER (WHERE v > 1) + 1 AS r FROM {src}",
    v112_na_x_max_numf_round_g => "SELECT g, ROUND(MAX(n) FILTER (WHERE v > 1), 2) AS r FROM {src} GROUP BY g",
    v112_na_x_max_numf_round_u => "SELECT ROUND(MAX(n) FILTER (WHERE v > 1), 2) AS r FROM {src}",
    v112_na_x_max_numf_coalesce_g => "SELECT g, COALESCE(MAX(n) FILTER (WHERE v > 1), 0) AS r FROM {src} GROUP BY g",
    v112_na_x_max_numf_coalesce_u => "SELECT COALESCE(MAX(n) FILTER (WHERE v > 1), 0) AS r FROM {src}",
}

// Nested MIN / MAX over an integer expression, combined
v112_na_typed_cells! {
    V112_NA_EXTREMA;
    v112_na_x_spread_halved_g => "SELECT g, (MAX(v * 3) - MIN(v * 3)) / 2 AS r FROM {src} GROUP BY g",
    v112_na_x_smallint_halved_g => "SELECT g, MAX(v::smallint) / 2 AS r FROM {src} GROUP BY g",
    v112_na_x_modulo_g => "SELECT g, MAX(v * 3) % 2 AS r FROM {src} GROUP BY g",
    v112_na_x_float_halved_g => "SELECT g, MAX(f * 2) / 2 AS r FROM {src} GROUP BY g",
    v112_na_x_top_and_nested_g => "SELECT g, MAX(v * 3) AS top, MAX(v * 3) / 2 AS r FROM {src} GROUP BY g",
    v112_na_x_filtered_top_and_nested_g => "SELECT g, MIN(v) FILTER (WHERE v > 1) AS top, MIN(v) FILTER (WHERE v > 1) / 2 AS r FROM {src} GROUP BY g",
    v112_na_x_sum_and_max_g => "SELECT g, (SUM(v) + MAX(v * 3)) / 2 AS r FROM {src} GROUP BY g",
}

// A string literal that spells an internal column read is left as written
v112_na_typed_cells! {
    V112_NA_EXTREMA;
    v112_na_lit_sum_case_g => "SELECT g, CASE WHEN SUM(v) > 0 THEN 'THEN \"__sum_v\" END' ELSE 'x' END AS r FROM {src} GROUP BY g",
    v112_na_lit_sum_case_u => "SELECT CASE WHEN SUM(v) > 0 THEN 'THEN \"__sum_v\" END' ELSE 'x' END AS r FROM {src}",
    v112_na_lit_sum_concat_g => "SELECT g, (SUM(v) / 2)::text || 'THEN \"__sum_v\" END' AS r FROM {src} GROUP BY g",
    v112_na_lit_sum_function_g => "SELECT g, COALESCE((SUM(v) / 2)::text, 'THEN \"__sum_v\" END') AS r FROM {src} GROUP BY g",
    v112_na_lit_sum_dollar_g => "SELECT g, COALESCE((SUM(v) / 2)::text, $lit$THEN \"__sum_v\" END$lit$) AS r FROM {src} GROUP BY g",
    v112_na_lit_sum_cast_text_g => "SELECT g, ('THEN \"__sum_v\" END' || (SUM(v) / 2)::text)::text AS r FROM {src} GROUP BY g",
    v112_na_lit_sum_escaped_quote_g => "SELECT g, COALESCE((SUM(v) / 2)::text, 'it''s THEN \"__sum_v\" END') AS r FROM {src} GROUP BY g",
    v112_na_lit_max_concat_g => "SELECT g, (MAX(v * 3) / 2)::text || '\"__max_v_3\"' AS r FROM {src} GROUP BY g",
    v112_na_lit_max_case_u => "SELECT CASE WHEN MAX(v * 3) / 2 > 0 THEN '\"__max_v_3\"' ELSE 'low' END AS r FROM {src}",
    v112_na_lit_min_function_g => "SELECT g, COALESCE((MIN(v) FILTER (WHERE v > 1) / 2)::text, '\"__min_case_when_v_1_then_v_end\"') AS r FROM {src} GROUP BY g",
}

v112_na_typed_cells! {
    V112_NA_NOT_NULL;
    v112_na_lit_counter_concat_g => "SELECT g, (SUM(v) + 1)::text || '\"__nonnull_count_v\"' AS r FROM {src} GROUP BY g",
    v112_na_lit_counter_case_u => "SELECT CASE WHEN SUM(v) > 0 THEN '\"__nonnull_count_v\"' ELSE 'x' END AS r FROM {src}",
    v112_na_lit_counter_function_g => "SELECT g, COALESCE(NULLIF((SUM(v) / 2)::text, '0'), '\"__nonnull_count_v\"') AS r FROM {src} GROUP BY g",
    v112_na_lit_bool_counter_case_g => "SELECT g, CASE WHEN BOOL_OR(b) THEN '\"__bool_or_b_nonnull_count\"' ELSE 'none' END AS r FROM {src} GROUP BY g",
    v112_na_lit_bool_counter_dollar_g => "SELECT g, CASE WHEN BOOL_OR(b) THEN $lit$\"__bool_or_b_nonnull_count\"$lit$ ELSE 'none' END AS r FROM {src} GROUP BY g",
}

#[pg_test]
fn v112_na_literal_naming_the_sum_read_is_stored_as_written() {
    Spi::run("CREATE TABLE v112_na_litrepro_src(id SERIAL PRIMARY KEY, g TEXT, v INT)").expect("create");
    Spi::run("INSERT INTO v112_na_litrepro_src(g, v) VALUES ('a',1),('a',2),('b',-7),('n',NULL)").expect("seed");
    let q = "SELECT g, CASE WHEN SUM(v) > 0 THEN 'THEN \"__sum_v\" END' ELSE 'x' END AS lit \
             FROM v112_na_litrepro_src GROUP BY g";
    assert_eq!(
        crate::create_reflex_ivm("v112_na_litrepro", q, None, None, None, None),
        "CREATE REFLEX INCREMENTAL VIEW"
    );
    let stored = Spi::get_one::<String>("SELECT lit FROM v112_na_litrepro WHERE g = 'a'")
        .expect("read")
        .expect("group a");
    assert_eq!(stored, "THEN \"__sum_v\" END");
    assert_imv_correct("v112_na_litrepro", q);
    Spi::run("INSERT INTO v112_na_litrepro_src(g, v) VALUES ('b',20),('n',3)").expect("insert");
    assert_imv_correct("v112_na_litrepro", q);
    Spi::run("DELETE FROM v112_na_litrepro_src WHERE g = 'a' AND v = 2").expect("delete");
    assert_imv_correct("v112_na_litrepro", q);
}

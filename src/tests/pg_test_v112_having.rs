struct V112HvFixture {
    columns: &'static str,
    seed: &'static str,
    steps: &'static [(&'static str, &'static str)],
}

/// Thresholds used by the cells sit between the per-group values these steps
/// produce, so groups enter and leave the result under INSERT, UPDATE and
/// DELETE. Group `n` holds only NULLs until `fill` and again after `unfill`.
const V112_HV_NULLABLE: V112HvFixture = V112HvFixture {
    columns: "g TEXT, v INT, b BOOLEAN, n NUMERIC",
    seed: "('a',1,true,1.25),('a',2,false,2.5),('a',3,false,3.75),('a',NULL,NULL,NULL),\
           ('b',5,false,6.25),('n',NULL,NULL,NULL),('n',NULL,NULL,NULL),('z',0,NULL,0),\
           ('m',-4,false,-5),('p',7,true,7),('p',8,NULL,8.5)",
    steps: &[
        (
            "ins",
            "INSERT INTO {src}(g, v, b, n) VALUES ('a',7,true,8.75),('c',3,false,3.75),\
             ('n',NULL,NULL,NULL),('b',-1,NULL,-1.25),('q',6,false,6)",
        ),
        (
            "upd",
            "UPDATE {src} SET v = v + 10, b = NOT b, n = n + 10 WHERE g = 'b'",
        ),
        ("fill", "UPDATE {src} SET v = 9, b = true, n = 9 WHERE g = 'n'"),
        (
            "unfill",
            "UPDATE {src} SET v = NULL, b = NULL, n = NULL WHERE g = 'n'",
        ),
        ("move", "UPDATE {src} SET g = 'm' WHERE g = 'z'"),
        (
            "del",
            "DELETE FROM {src} WHERE g = 'c' OR (g = 'a' AND v IN (1, 7)) OR (g = 'p' AND v = 8)",
        ),
        (
            "shrink",
            "UPDATE {src} SET v = v - 10, n = n - 10 WHERE g = 'b'",
        ),
        ("nullonly", "DELETE FROM {src} WHERE v IS NOT NULL"),
        (
            "refill",
            "INSERT INTO {src}(g, v, b, n) VALUES ('a',4,true,4),('a',5,false,5),('k',1,false,1)",
        ),
        ("empty", "DELETE FROM {src}"),
    ],
};

const V112_HV_NOT_NULL: V112HvFixture = V112HvFixture {
    columns: "g TEXT NOT NULL, v INT NOT NULL, b BOOLEAN NOT NULL, n NUMERIC NOT NULL",
    seed: "('a',1,true,1.25),('a',2,false,2.5),('a',3,false,3.75),('b',5,false,6.25),\
           ('z',0,false,0),('m',-4,false,-5),('p',7,true,7),('p',8,false,8.5)",
    steps: &[
        (
            "ins",
            "INSERT INTO {src}(g, v, b, n) VALUES ('a',7,true,8.75),('c',3,false,3.75),\
             ('b',-1,true,-1.25),('q',6,false,6)",
        ),
        (
            "upd",
            "UPDATE {src} SET v = v + 10, b = NOT b, n = n + 10 WHERE g = 'b'",
        ),
        ("move", "UPDATE {src} SET g = 'm' WHERE g = 'z'"),
        (
            "del",
            "DELETE FROM {src} WHERE g = 'c' OR (g = 'a' AND v IN (1, 7)) OR (g = 'p' AND v = 8)",
        ),
        (
            "shrink",
            "UPDATE {src} SET v = v - 10, n = n - 10 WHERE g = 'b'",
        ),
        ("only_a", "DELETE FROM {src} WHERE g <> 'a'"),
        (
            "refill",
            "INSERT INTO {src}(g, v, b, n) VALUES ('a',4,true,4),('a',5,false,5),('k',1,false,1)",
        ),
        ("empty", "DELETE FROM {src}"),
    ],
};

fn v112_hv_outcome(steps: Vec<(String, String)>) -> String {
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

/// Creates the IMV over a fresh source, then runs every DML step of the
/// fixture, comparing the IMV with the query after each. `topk` of `Some(0)`
/// keeps scalar MIN / MAX columns; `None` takes the default top-K size.
fn v112_hv_cell_on(
    fixture: &V112HvFixture,
    tag: &str,
    select_template: &str,
    deferred: bool,
    topk: Option<i32>,
) -> String {
    let src = format!("v112_hv_{tag}_src");
    let imv = format!("v112_hv_{tag}");
    Spi::run(&format!(
        "CREATE TABLE {src}(id SERIAL PRIMARY KEY, {})",
        fixture.columns
    ))
    .expect("create source");
    Spi::run(&format!(
        "INSERT INTO {src}(g, v, b, n) VALUES {}",
        fixture.seed
    ))
    .expect("seed");
    let fresh = select_template.replace("{src}", &src);
    let mode = if deferred { "DEFERRED" } else { "IMMEDIATE" };
    let topk_argument = topk.map(|k| format!(", {k}")).unwrap_or_default();
    let created = v112_na_try(&format!(
        "SELECT create_reflex_ivm('{imv}', $v112sel${fresh}$v112sel$, NULL, 'UNLOGGED', '{mode}'{topk_argument})"
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
    v112_hv_outcome(steps)
}

macro_rules! v112_hv_cells {
    ($fixture:expr, $deferred:expr, $topk:expr; $($name:ident => $select:expr,)+) => {
        $(
            #[pg_test]
            fn $name() {
                let tag = stringify!($name).trim_start_matches("v112_hv_");
                assert_eq!(
                    v112_hv_cell_on(&$fixture, tag, $select, $deferred, $topk),
                    "ok",
                    "{}",
                    $select
                );
            }
        )+
    };
}

// Each aggregate alone in HAVING, absent from the SELECT list
v112_hv_cells! {
    V112_HV_NULLABLE, false, None;
    v112_hv_sum_gt => "SELECT g, COUNT(*) AS c FROM {src} GROUP BY g HAVING SUM(v) > 5",
    v112_hv_sum_all_null => "SELECT g, COUNT(*) AS c FROM {src} GROUP BY g HAVING SUM(v) >= 0",
    v112_hv_sum_numeric_all_null => "SELECT g, COUNT(*) AS c FROM {src} GROUP BY g HAVING SUM(n) <= 6.25",
    v112_hv_count_star => "SELECT g, SUM(v) AS s FROM {src} GROUP BY g HAVING COUNT(*) > 1",
    v112_hv_count_col => "SELECT g, SUM(v) AS s FROM {src} GROUP BY g HAVING COUNT(v) > 1",
    v112_hv_count_col_zero => "SELECT g, COUNT(*) AS c FROM {src} GROUP BY g HAVING COUNT(v) = 0",
    v112_hv_avg => "SELECT g, COUNT(*) AS c FROM {src} GROUP BY g HAVING AVG(v) > 2",
    v112_hv_avg_numeric => "SELECT g, COUNT(*) AS c FROM {src} GROUP BY g HAVING AVG(n) >= 2.5",
    v112_hv_min => "SELECT g, COUNT(*) AS c FROM {src} GROUP BY g HAVING MIN(v) > 1",
    v112_hv_max => "SELECT g, COUNT(*) AS c FROM {src} GROUP BY g HAVING MAX(v) < 7",
    v112_hv_bool_or => "SELECT g, COUNT(*) AS c FROM {src} GROUP BY g HAVING BOOL_OR(b)",
    v112_hv_not_bool_or => "SELECT g, COUNT(*) AS c FROM {src} GROUP BY g HAVING NOT BOOL_OR(b)",
}

// FILTER inside a HAVING aggregate
v112_hv_cells! {
    V112_HV_NULLABLE, false, None;
    v112_hv_count_star_filter => "SELECT g, COUNT(*) AS c FROM {src} GROUP BY g HAVING COUNT(*) FILTER (WHERE v > 1) > 1",
    v112_hv_count_star_filter_zero => "SELECT g, COUNT(*) AS c FROM {src} GROUP BY g HAVING COUNT(*) FILTER (WHERE v > 5) = 0",
    v112_hv_count_col_filter => "SELECT g, COUNT(*) AS c FROM {src} GROUP BY g HAVING COUNT(v) FILTER (WHERE b) > 0",
    v112_hv_sum_filter => "SELECT g, COUNT(*) AS c FROM {src} GROUP BY g HAVING SUM(v) FILTER (WHERE v > 1) > 5",
    v112_hv_sum_filter_all_null => "SELECT g, COUNT(*) AS c FROM {src} GROUP BY g HAVING COALESCE(SUM(v) FILTER (WHERE v > 100), -1) < 0 AND SUM(v) FILTER (WHERE v > 2) >= 5",
    v112_hv_avg_filter => "SELECT g, COUNT(*) AS c FROM {src} GROUP BY g HAVING AVG(v) FILTER (WHERE v > 1) > 3",
    v112_hv_min_filter => "SELECT g, COUNT(*) AS c FROM {src} GROUP BY g HAVING MIN(v) FILTER (WHERE v > 1) > 2",
    v112_hv_max_filter => "SELECT g, COUNT(*) AS c FROM {src} GROUP BY g HAVING MAX(v) FILTER (WHERE v < 7) > 2",
    v112_hv_filter_beside_unfiltered => "SELECT g, COUNT(*) AS c, SUM(v) AS s FROM {src} GROUP BY g HAVING COUNT(*) FILTER (WHERE v > 1) < COUNT(*)",
}

// A HAVING aggregate nested in an expression
v112_hv_cells! {
    V112_HV_NULLABLE, false, None;
    v112_hv_round_avg => "SELECT g, COUNT(*) AS c FROM {src} GROUP BY g HAVING ROUND(AVG(v), 2) > 2",
    v112_hv_round_avg_selected => "SELECT g, ROUND(AVG(v), 2) AS r FROM {src} GROUP BY g HAVING ROUND(AVG(v), 2) > 1",
    v112_hv_coalesce_sum => "SELECT g, COUNT(*) AS c FROM {src} GROUP BY g HAVING COALESCE(SUM(v), 0) >= 0",
    v112_hv_coalesce_sum_negative => "SELECT g, COUNT(*) AS c FROM {src} GROUP BY g HAVING COALESCE(SUM(v), -1) < 0",
    v112_hv_sum_integer_division => "SELECT g, COUNT(*) AS c FROM {src} GROUP BY g HAVING SUM(v) / 2 > 2",
    v112_hv_sum_over_count => "SELECT g, COUNT(*) AS c FROM {src} GROUP BY g HAVING SUM(v) / COUNT(*) >= 2",
    v112_hv_sum_cast_division => "SELECT g, COUNT(*) AS c FROM {src} GROUP BY g HAVING SUM(v)::numeric / 2 > 2.4",
    v112_hv_max_integer_division => "SELECT g, COUNT(*) AS c FROM {src} GROUP BY g HAVING MAX(v) / 2 > 1",
    v112_hv_min_integer_division => "SELECT g, COUNT(*) AS c FROM {src} GROUP BY g HAVING MIN(v) / 2 < 1",
    v112_hv_case => "SELECT g, COUNT(*) AS c FROM {src} GROUP BY g HAVING CASE WHEN SUM(v) > 5 THEN COUNT(*) > 1 ELSE MIN(v) < 0 END",
    v112_hv_and => "SELECT g, COUNT(*) AS c FROM {src} GROUP BY g HAVING MIN(v) > 0 AND SUM(v) > 5",
    v112_hv_or => "SELECT g, COUNT(*) AS c FROM {src} GROUP BY g HAVING SUM(v) > 10 OR COUNT(*) > 2",
    v112_hv_not_nested => "SELECT g, COUNT(*) AS c FROM {src} GROUP BY g HAVING NOT (SUM(v) > 5)",
    v112_hv_arithmetic => "SELECT g, COUNT(*) AS c FROM {src} GROUP BY g HAVING SUM(v) - MIN(v) > COUNT(v) * 2",
    v112_hv_group_column => "SELECT g, SUM(v) AS s FROM {src} GROUP BY g HAVING g <> 'a' AND SUM(v) > 4",
}

// The same aggregate in the SELECT list and in HAVING
v112_hv_cells! {
    V112_HV_NULLABLE, false, None;
    v112_hv_sum_selected => "SELECT g, SUM(v) AS s FROM {src} GROUP BY g HAVING SUM(v) >= 0",
    v112_hv_avg_selected => "SELECT g, AVG(v) AS a FROM {src} GROUP BY g HAVING AVG(v) > 2",
    v112_hv_min_selected => "SELECT g, MIN(v) AS lo, MAX(v) AS hi FROM {src} GROUP BY g HAVING MIN(v) > 1 OR MAX(v) < 0",
    v112_hv_filter_selected => "SELECT g, COUNT(*) FILTER (WHERE v > 1) AS c FROM {src} GROUP BY g HAVING COUNT(*) FILTER (WHERE v > 1) > 1",
    v112_hv_sum_selected_nested => "SELECT g, SUM(v) + 1 AS s FROM {src} GROUP BY g HAVING SUM(v) + 1 > 6",
}

// MIN / MAX kept as scalar columns, recomputed on retraction
v112_hv_cells! {
    V112_HV_NULLABLE, false, Some(0);
    v112_hv_min_scalar => "SELECT g, COUNT(*) AS c FROM {src} GROUP BY g HAVING MIN(v) > 1",
    v112_hv_max_scalar => "SELECT g, COUNT(*) AS c FROM {src} GROUP BY g HAVING MAX(v) < 7",
    v112_hv_max_division_scalar => "SELECT g, COUNT(*) AS c FROM {src} GROUP BY g HAVING MAX(v) / 2 > 1",
}

// NOT NULL source columns
v112_hv_cells! {
    V112_HV_NOT_NULL, false, None;
    v112_hv_nn_sum_gt => "SELECT g, COUNT(*) AS c FROM {src} GROUP BY g HAVING SUM(v) > 5",
    v112_hv_nn_sum_selected => "SELECT g, SUM(v) AS s FROM {src} GROUP BY g HAVING SUM(v) > 5",
    v112_hv_nn_sum_integer_division => "SELECT g, COUNT(*) AS c FROM {src} GROUP BY g HAVING SUM(v) / 2 > 2",
    v112_hv_nn_sum_filter => "SELECT g, COUNT(*) AS c FROM {src} GROUP BY g HAVING SUM(v) FILTER (WHERE v > 1) > 5",
    v112_hv_nn_count_star_filter => "SELECT g, COUNT(*) AS c FROM {src} GROUP BY g HAVING COUNT(*) FILTER (WHERE v > 1) > 1",
    v112_hv_nn_round_avg => "SELECT g, COUNT(*) AS c FROM {src} GROUP BY g HAVING ROUND(AVG(v), 2) > 2",
    v112_hv_nn_bool_or => "SELECT g, COUNT(*) AS c FROM {src} GROUP BY g HAVING BOOL_OR(b)",
    v112_hv_nn_max_integer_division => "SELECT g, COUNT(*) AS c FROM {src} GROUP BY g HAVING MAX(v) / 2 > 1",
}

// DEFERRED refresh
v112_hv_cells! {
    V112_HV_NULLABLE, true, None;
    v112_hv_def_sum_all_null => "SELECT g, COUNT(*) AS c FROM {src} GROUP BY g HAVING SUM(v) >= 0",
    v112_hv_def_count_star_filter => "SELECT g, COUNT(*) AS c FROM {src} GROUP BY g HAVING COUNT(*) FILTER (WHERE v > 1) > 1",
    v112_hv_def_round_avg => "SELECT g, COUNT(*) AS c FROM {src} GROUP BY g HAVING ROUND(AVG(v), 2) > 2",
    v112_hv_def_sum_integer_division => "SELECT g, COUNT(*) AS c FROM {src} GROUP BY g HAVING SUM(v) / 2 > 2",
    v112_hv_def_max => "SELECT g, COUNT(*) AS c FROM {src} GROUP BY g HAVING MAX(v) < 7",
}


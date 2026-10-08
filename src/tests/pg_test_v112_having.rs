struct V112HvFixture {
    columns: &'static str,
    seed: &'static str,
    steps: &'static [(&'static str, &'static str)],
}

/// Thresholds used by the cells sit between the per-group values these steps
/// produce, so groups enter and leave the result under INSERT, UPDATE and
/// DELETE. Group `n` holds only NULLs until `fill` and again after `unfill`;
/// group `d` repeats one value, which a count of rows and a count of distinct
/// values tell apart. `regrow` brings back the row of an ungrouped aggregate
/// that `nullonly` (or `only_a`) and `refill` left filtered out.
const V112_HV_NULLABLE: V112HvFixture = V112HvFixture {
    columns: "g TEXT, v INT, b BOOLEAN, n NUMERIC",
    seed: "('a',1,true,1.25),('a',2,false,2.5),('a',3,false,3.75),('a',NULL,NULL,NULL),\
           ('b',5,false,6.25),('n',NULL,NULL,NULL),('n',NULL,NULL,NULL),('z',0,NULL,0),\
           ('m',-4,false,-5),('p',7,true,7),('p',8,NULL,8.5),('d',4,false,4),('d',4,true,4),\
           ('d',4,false,4)",
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
        (
            "regrow",
            "INSERT INTO {src}(g, v, b, n) VALUES ('a',30,true,30),('k',-2,false,-2),('r',6,true,6),\
             ('r',6,false,6)",
        ),
        ("empty", "DELETE FROM {src}"),
    ],
};

const V112_HV_NOT_NULL: V112HvFixture = V112HvFixture {
    columns: "g TEXT NOT NULL, v INT NOT NULL, b BOOLEAN NOT NULL, n NUMERIC NOT NULL",
    seed: "('a',1,true,1.25),('a',2,false,2.5),('a',3,false,3.75),('b',5,false,6.25),\
           ('z',0,false,0),('m',-4,false,-5),('p',7,true,7),('p',8,false,8.5),('d',4,false,4),\
           ('d',4,true,4),('d',4,false,4)",
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
        (
            "regrow",
            "INSERT INTO {src}(g, v, b, n) VALUES ('a',30,true,30),('k',-2,false,-2),('r',6,true,6),\
             ('r',6,false,6)",
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
            let flushed = v112_hv_flush(&imv, &src);
            if flushed.contains("RAISED") || flushed.contains("ERROR") {
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

// An aggregate with no GROUP BY: one row when HAVING holds, none otherwise
v112_hv_cells! {
    V112_HV_NULLABLE, false, None;
    v112_hv_ungrouped_sum => "SELECT SUM(v) AS s FROM {src} HAVING SUM(v) > 20",
    v112_hv_ungrouped_sum_all_null => "SELECT COUNT(*) AS c FROM {src} HAVING SUM(v) >= 0",
    v112_hv_ungrouped_count_star => "SELECT COUNT(*) AS c FROM {src} HAVING COUNT(*) >= 0",
    v112_hv_ungrouped_count_star_positive => "SELECT COUNT(*) AS c, SUM(v) AS s FROM {src} HAVING COUNT(*) > 4",
    v112_hv_ungrouped_avg => "SELECT AVG(v) AS a FROM {src} HAVING ROUND(AVG(v), 2) > 3",
    v112_hv_ungrouped_min => "SELECT MIN(v) AS lo FROM {src} HAVING MIN(v) < 0",
    v112_hv_ungrouped_filter => "SELECT COUNT(*) AS c FROM {src} HAVING COUNT(*) FILTER (WHERE v > 5) > 2",
    v112_hv_ungrouped_no_aggregate_in_having => "SELECT SUM(v) AS s FROM {src} HAVING 1 = 1",
}

v112_hv_cells! {
    V112_HV_NOT_NULL, false, None;
    v112_hv_nn_ungrouped_sum => "SELECT SUM(v) AS s FROM {src} HAVING SUM(v) > 20",
}

v112_hv_cells! {
    V112_HV_NULLABLE, true, None;
    v112_hv_def_ungrouped_sum => "SELECT SUM(v) AS s FROM {src} HAVING SUM(v) > 20",
}

// COUNT(DISTINCT) regroups the end query, where HAVING follows GROUP BY
v112_hv_cells! {
    V112_HV_NULLABLE, false, None;
    v112_hv_count_distinct => "SELECT g, COUNT(DISTINCT v) AS d FROM {src} GROUP BY g HAVING COUNT(DISTINCT v) > 1",
    v112_hv_count_distinct_positive => "SELECT g, COUNT(DISTINCT v) AS d FROM {src} GROUP BY g HAVING COUNT(DISTINCT v) > 0",
    v112_hv_count_distinct_zero => "SELECT g, COUNT(DISTINCT v) AS d FROM {src} GROUP BY g HAVING COUNT(DISTINCT v) = 0",
    v112_hv_count_distinct_nested => "SELECT g, COUNT(DISTINCT v) AS d FROM {src} GROUP BY g HAVING COUNT(DISTINCT v) * 2 > 3",
    v112_hv_count_distinct_group_column => "SELECT g, COUNT(DISTINCT v) AS d FROM {src} GROUP BY g HAVING g <> 'a'",
    v112_hv_count_distinct_ungrouped => "SELECT COUNT(DISTINCT v) AS d FROM {src} HAVING COUNT(DISTINCT v) > 4",
}

v112_hv_cells! {
    V112_HV_NULLABLE, true, None;
    v112_hv_def_count_distinct => "SELECT g, COUNT(DISTINCT v) AS d FROM {src} GROUP BY g HAVING COUNT(DISTINCT v) > 1",
}

fn v112_hv_source(tag: &str) -> String {
    let src = format!("v112_hv_{tag}_src");
    Spi::run(&format!(
        "CREATE TABLE {src}(id SERIAL PRIMARY KEY, {})",
        V112_HV_NULLABLE.columns
    ))
    .expect("create source");
    Spi::run(&format!(
        "INSERT INTO {src}(g, v, b, n) VALUES {}",
        V112_HV_NULLABLE.seed
    ))
    .expect("seed");
    src
}

fn v112_hv_create(imv: &str, select: &str) -> String {
    v112_na_try(&format!(
        "SELECT create_reflex_ivm('{imv}', $v112sel${select}$v112sel$)"
    ))
}

fn v112_hv_assert_nothing_created(imv: &str) {
    let relations = Spi::get_one::<i64>(&format!(
        "SELECT COUNT(*) FROM pg_class WHERE relname LIKE '%{imv}%' AND relname NOT LIKE '%{imv}_src%'"
    ))
    .expect("catalog")
    .unwrap_or(-1);
    let registered = Spi::get_one::<i64>(&format!(
        "SELECT COUNT(*) FROM public.__reflex_ivm_reference WHERE name LIKE '%{imv}%'"
    ))
    .expect("registry")
    .unwrap_or(-1);
    assert_eq!((relations, registered), (0, 0), "{imv} left objects behind");
}

/// The shape is refused with the tagged error before any DDL, and `remedy`,
/// the rewrite the message prescribes, is created and stays correct.
fn v112_hv_rejected_with_working_remedy(tag: &str, rejected: &str, remedy: &str) {
    let src = v112_hv_source(tag);
    let imv = format!("v112_hv_{tag}");
    let outcome = v112_hv_create(&imv, &rejected.replace("{src}", &src));
    assert!(
        outcome.starts_with("ERROR: [reflex-unsupported]")
            && outcome.contains("COUNT(DISTINCT")
            && outcome.contains("CTE"),
        "{outcome}"
    );
    v112_hv_assert_nothing_created(&imv);

    let remedy = remedy.replace("{src}", &src);
    assert_eq!(
        v112_hv_create(&imv, &remedy),
        "CREATE REFLEX INCREMENTAL VIEW"
    );
    assert_imv_correct(&imv, &remedy);
    for (label, dml) in V112_HV_NULLABLE.steps {
        Spi::run(&dml.replace("{src}", &src)).expect(label);
        assert_imv_correct(&imv, &remedy);
    }
}

#[pg_test]
fn v112_hv_count_distinct_with_other_aggregate_in_having_is_rejected() {
    v112_hv_rejected_with_working_remedy(
        "cd_having_other",
        "SELECT g, COUNT(DISTINCT v) AS d FROM {src} GROUP BY g HAVING SUM(v) > 5",
        "WITH cd AS (SELECT g, COUNT(DISTINCT v) AS d FROM {src} GROUP BY g), \
              other AS (SELECT g, SUM(v) AS s FROM {src} GROUP BY g) \
         SELECT cd.g, cd.d FROM cd JOIN other ON other.g = cd.g WHERE other.s > 5",
    );
}

#[pg_test]
fn v112_hv_count_distinct_only_in_having_is_rejected() {
    v112_hv_rejected_with_working_remedy(
        "cd_having_only",
        "SELECT g, SUM(v) AS s FROM {src} GROUP BY g HAVING COUNT(DISTINCT v) > 1",
        "WITH cd AS (SELECT g, COUNT(DISTINCT v) AS d FROM {src} GROUP BY g), \
              other AS (SELECT g, SUM(v) AS s FROM {src} GROUP BY g) \
         SELECT other.g, other.s FROM cd JOIN other ON other.g = cd.g WHERE cd.d > 1",
    );
}

#[pg_test]
fn v112_hv_count_distinct_of_another_column_in_having_is_rejected() {
    v112_hv_rejected_with_working_remedy(
        "cd_having_other_column",
        "SELECT g, COUNT(DISTINCT v) AS d FROM {src} GROUP BY g HAVING COUNT(DISTINCT n) > 1",
        "WITH cd AS (SELECT g, COUNT(DISTINCT v) AS d FROM {src} GROUP BY g), \
              other AS (SELECT g, COUNT(DISTINCT n) AS dn FROM {src} GROUP BY g) \
         SELECT cd.g, cd.d FROM cd JOIN other ON other.g = cd.g WHERE other.dn > 1",
    );
}

#[pg_test]
fn v112_hv_count_distinct_beside_derived_aggregate_is_rejected() {
    v112_hv_rejected_with_working_remedy(
        "cd_derived",
        "SELECT g, COUNT(DISTINCT v) AS d, SUM(v) + 1 AS s FROM {src} GROUP BY g",
        "WITH cd AS (SELECT g, COUNT(DISTINCT v) AS d FROM {src} GROUP BY g), \
              other AS (SELECT g, SUM(v) + 1 AS s FROM {src} GROUP BY g) \
         SELECT cd.g, cd.d, other.s FROM cd JOIN other ON other.g = cd.g",
    );
}

/// Flushes the staged writes of `table` for `imv` under the spelling the IMV
/// registered that source with.
fn v112_hv_flush(imv: &str, table: &str) -> String {
    v112_na_try(&format!(
        "SELECT COALESCE(string_agg(public.reflex_flush_deferred(src), ' | '), 'RAISED: {table} is not a source of {imv}') \
         FROM (SELECT DISTINCT src FROM public.__reflex_ivm_reference, unnest(depends_on) src \
               WHERE name = '{imv}' AND (src = '{table}' OR src LIKE '%.{table}')) registered"
    ))
}

/// Writes to the table read by the HAVING subquery move its value across the
/// per-group aggregates in both directions; writes to the outer source follow,
/// among them one to a column only a subquery reads.
fn v112_hv_subquery_roundtrip(tag: &str, select: &str, deferred: bool) -> String {
    let src = format!("v112_hv_{tag}_src");
    let lookup = format!("v112_hv_{tag}_o");
    let imv = format!("v112_hv_{tag}");
    Spi::run(&format!(
        "CREATE TABLE {src}(id SERIAL PRIMARY KEY, g TEXT, v INT, w INT DEFAULT 0)"
    ))
    .expect("create source");
    Spi::run(&format!(
        "INSERT INTO {src}(g, v) VALUES ('a',1),('a',2),('b',10),('c',4),('c',NULL),('d',NULL)"
    ))
    .expect("seed");
    Spi::run(&format!(
        "CREATE TABLE {lookup}(id SERIAL PRIMARY KEY, g TEXT, k INT)"
    ))
    .expect("create lookup");
    Spi::run(&format!(
        "INSERT INTO {lookup}(g, k) VALUES ('a',1),('b',2),('c',2),('d',3)"
    ))
    .expect("seed lookup");
    let fresh = select.replace("{src}", &src).replace("{o}", &lookup);
    let mode = if deferred { "DEFERRED" } else { "IMMEDIATE" };
    let created = v112_na_try(&format!(
        "SELECT create_reflex_ivm('{imv}', $v112sel${fresh}$v112sel$, NULL, 'UNLOGGED', '{mode}')"
    ));
    if created != "CREATE REFLEX INCREMENTAL VIEW" {
        return format!("AT-CREATE {created}");
    }
    let lookup_steps = [
        ("o_ins", format!("INSERT INTO {lookup}(g, k) VALUES ('z', 4)")),
        (
            "o_ins_many",
            format!("INSERT INTO {lookup}(g, k) VALUES ('z', 5), ('z', 6), ('z', 7), ('z', 11)"),
        ),
        ("o_upd", format!("UPDATE {lookup} SET k = k - 9 WHERE g = 'z'")),
        ("o_del", format!("DELETE FROM {lookup} WHERE g = 'z'")),
        ("o_upd_all", format!("UPDATE {lookup} SET k = k + 1")),
        ("o_del_joined", format!("DELETE FROM {lookup} WHERE g = 'c'")),
        ("src_ins", format!("INSERT INTO {src}(g, v) VALUES ('a', 5), ('e', 9), ('d', 1)")),
        ("src_upd", format!("UPDATE {src} SET v = v - 3 WHERE g IN ('a', 'b')")),
        ("src_del", format!("DELETE FROM {src} WHERE g = 'a' AND v < 2")),
        (
            "src_w_only",
            format!("UPDATE {src} SET w = w + 7 WHERE g IN ('a', 'b')"),
        ),
        (
            "src_lower",
            format!("UPDATE {src} SET v = 1 WHERE g = 'c' AND v IS NOT NULL"),
        ),
        ("src_w_reset", format!("UPDATE {src} SET w = 0")),
        (
            "src_raise",
            format!("INSERT INTO {src}(g, v) VALUES ('e', 40), ('b', 6), ('b', 5)"),
        ),
        ("o_empty", format!("DELETE FROM {lookup}")),
        ("o_refill", format!("INSERT INTO {lookup}(g, k) VALUES ('a', 1), ('b', 0), ('e', 2)")),
        ("src_empty", format!("DELETE FROM {src}")),
    ];
    let mut steps = vec![("create".to_string(), v112_na_oracle(&imv, &fresh))];
    for (label, dml) in &lookup_steps {
        let ran = v112_na_try_dml(dml);
        if ran != "OK" {
            steps.push((label.to_string(), ran));
            break;
        }
        let written = if label.starts_with("o_") { &lookup } else { &src };
        if deferred && fresh.contains(written.as_str()) {
            let flushed = v112_hv_flush(&imv, written);
            if flushed.contains("RAISED") || flushed.contains("ERROR") {
                steps.push((label.to_string(), flushed));
                break;
            }
        }
        steps.push((label.to_string(), v112_na_oracle(&imv, &fresh)));
    }
    v112_hv_outcome(steps)
}

const V112_HV_SUB_SUM: &str = "SELECT g, SUM(v) AS s FROM {src} GROUP BY g HAVING SUM(v) > (SELECT COUNT(*) FROM {o})";
const V112_HV_SUB_COUNT: &str = "SELECT g, COUNT(*) AS c, COUNT(v) AS cv FROM {src} GROUP BY g \
             HAVING COUNT(v) * 3 >= (SELECT COUNT(*) FROM {o})";
const V112_HV_SUB_MIN_MAX: &str = "SELECT g, MIN(v) AS lo, MAX(v) AS hi FROM {src} GROUP BY g \
             HAVING MAX(v) > (SELECT MAX(k) FROM {o}) OR MIN(v) < (SELECT MIN(k) FROM {o})";
const V112_HV_SUB_AVG_COLUMN: &str = "SELECT g, AVG(v) AS a FROM {src} GROUP BY g \
             HAVING AVG(v) >= (SELECT COALESCE(SUM(k), 0) FROM {o} WHERE k > 2)";
const V112_HV_SUB_JOINED: &str = "SELECT {src}.g, SUM({src}.v) AS s, COUNT(*) AS c FROM {src} JOIN {o} ON {o}.g = {src}.g \
             GROUP BY {src}.g HAVING SUM({src}.v) > (SELECT MAX(k) FROM {o})";

macro_rules! v112_hv_subquery_cells {
    ($($name:ident => $shape:expr, $deferred:expr;)+) => {
        $(
            #[pg_test]
            fn $name() {
                let tag = stringify!($name).trim_start_matches("v112_hv_");
                assert_eq!(
                    v112_hv_subquery_roundtrip(tag, $shape, $deferred),
                    "ok",
                    "{}",
                    $shape
                );
            }
        )+
    };
}

v112_hv_subquery_cells! {
    v112_hv_sub_sum => V112_HV_SUB_SUM, false;
    v112_hv_sub_count => V112_HV_SUB_COUNT, false;
    v112_hv_sub_min_max => V112_HV_SUB_MIN_MAX, false;
    v112_hv_sub_avg_column => V112_HV_SUB_AVG_COLUMN, false;
    v112_hv_sub_joined => V112_HV_SUB_JOINED, false;
    v112_hv_sub_sum_def => V112_HV_SUB_SUM, true;
    v112_hv_sub_count_def => V112_HV_SUB_COUNT, true;
    v112_hv_sub_min_max_def => V112_HV_SUB_MIN_MAX, true;
    v112_hv_sub_joined_def => V112_HV_SUB_JOINED, true;
}

const V112_HV_SUB_ALSO_IN_WHERE: &str = "SELECT g, SUM(v) AS s FROM {src} WHERE g IN (SELECT g FROM {o}) \
             GROUP BY g HAVING SUM(v) > (SELECT MAX(k) FROM {o})";
const V112_HV_SUB_JOINED_FILTERED: &str = "SELECT {src}.g, SUM({src}.v) AS sv FROM {src} JOIN {o} ON {o}.g = {src}.g \
             WHERE {o}.k > 1 GROUP BY {src}.g \
             HAVING SUM({src}.v) > (SELECT COUNT(*) FROM {o} WHERE k <= 1)";
const V112_HV_SUB_OWN_DISJOINT_ROWS: &str = "SELECT g, COUNT(*) AS c FROM {src} WHERE v > 3 GROUP BY g \
             HAVING COUNT(*) >= (SELECT COUNT(*) FROM {src} WHERE v <= 1)";
const V112_HV_SUB_OWN_OTHER_COLUMN: &str = "SELECT g, COUNT(*) AS c FROM {src} GROUP BY g \
             HAVING COUNT(*) > (SELECT COUNT(*) FROM {src} WHERE w > 0)";
const V112_HV_SUB_OWN_MAX: &str = "SELECT g, MAX(v) AS m FROM {src} GROUP BY g \
             HAVING MAX(v) >= (SELECT MAX(v) FROM {src})";

v112_hv_subquery_cells! {
    v112_hv_sub_also_in_where => V112_HV_SUB_ALSO_IN_WHERE, false;
    v112_hv_sub_joined_filtered => V112_HV_SUB_JOINED_FILTERED, false;
    v112_hv_sub_own_disjoint_rows => V112_HV_SUB_OWN_DISJOINT_ROWS, false;
    v112_hv_sub_own_other_column => V112_HV_SUB_OWN_OTHER_COLUMN, false;
    v112_hv_sub_own_max => V112_HV_SUB_OWN_MAX, false;
    v112_hv_sub_also_in_where_def => V112_HV_SUB_ALSO_IN_WHERE, true;
    v112_hv_sub_joined_filtered_def => V112_HV_SUB_JOINED_FILTERED, true;
    v112_hv_sub_own_disjoint_rows_def => V112_HV_SUB_OWN_DISJOINT_ROWS, true;
    v112_hv_sub_own_other_column_def => V112_HV_SUB_OWN_OTHER_COLUMN, true;
    v112_hv_sub_own_max_def => V112_HV_SUB_OWN_MAX, true;
}

/// The report's exact sequence: the subquery value stays below both sums, so
/// the result must not move at all.
#[pg_test]
fn v112_hv_sub_write_leaves_qualifying_sums_unchanged() {
    Spi::run("CREATE TABLE v112_hv_rep_s(id SERIAL PRIMARY KEY, g TEXT, v INT)").expect("s");
    Spi::run("INSERT INTO v112_hv_rep_s(g, v) VALUES ('a', 1), ('a', 2), ('b', 10)").expect("seed");
    Spi::run("CREATE TABLE v112_hv_rep_o(k INT)").expect("o");
    Spi::run("INSERT INTO v112_hv_rep_o VALUES (1)").expect("seed o");
    let q = "SELECT g, SUM(v) AS s FROM v112_hv_rep_s GROUP BY g \
             HAVING SUM(v) > (SELECT COUNT(*) FROM v112_hv_rep_o)";
    assert_eq!(
        v112_hv_create("v112_hv_rep", q),
        "CREATE REFLEX INCREMENTAL VIEW"
    );
    Spi::run("INSERT INTO v112_hv_rep_o VALUES (2)").expect("insert o");
    let sums = Spi::get_one::<String>(
        "SELECT string_agg(g || '=' || s, ',' ORDER BY g) FROM v112_hv_rep",
    )
    .expect("read")
    .unwrap_or_default();
    assert_eq!(sums, "a=3,b=10");
    assert_imv_correct("v112_hv_rep", q);
    Spi::run("DELETE FROM v112_hv_rep_o WHERE k = 2").expect("delete o");
    assert_imv_correct("v112_hv_rep", q);
}

/// Only a relation read by the end query counts: an output column or the
/// intermediate table carrying the source's name does not.
#[pg_test]
fn v112_hv_sub_end_query_reads_source_only_as_a_relation() {
    use crate::trigger::end_query_reads_source;
    let plain = "SELECT \"g\", \"__sum_v\" AS \"s\" FROM \"__reflex_intermediate_s\" \
                 WHERE __ivm_count > 0";
    assert!(!end_query_reads_source(plain, "public.s"));
    let with_subquery = format!("{plain} AND (\"__sum_v\" > (SELECT COUNT(*) FROM s))");
    assert!(end_query_reads_source(&with_subquery, "public.s"));
    assert!(end_query_reads_source(&with_subquery, "\"public\".\"s\""));
    assert!(!end_query_reads_source(&with_subquery, "public.o"));
    assert!(end_query_reads_source("SELECT FROM WHERE s (", "s"));
}

// The HAVING subquery reads the IMV's own source
v112_hv_cells! {
    V112_HV_NULLABLE, false, None;
    v112_hv_sub_own_source => "SELECT g, COUNT(*) AS c FROM {src} GROUP BY g HAVING COUNT(*) > (SELECT COUNT(*) FROM {src}) / 4",
}

v112_hv_cells! {
    V112_HV_NULLABLE, true, None;
    v112_hv_sub_own_source_def => "SELECT g, COUNT(*) AS c FROM {src} GROUP BY g HAVING COUNT(*) > (SELECT COUNT(*) FROM {src}) / 4",
}

/// The bulk-insert shortcut is offered for a dimension joined on its key when
/// a row of it starts passing the WHERE clause, and withheld once the HAVING
/// subquery reads that dimension.
fn v112_hv_shortcut_fixture(tag: &str, having: &str) -> (String, String, String) {
    let fact = format!("v112_hv_{tag}_f");
    let dimension = format!("v112_hv_{tag}_d");
    let imv = format!("v112_hv_{tag}");
    Spi::run(&format!(
        "CREATE TABLE {fact}(id SERIAL PRIMARY KEY, did INT, v INT)"
    ))
    .expect("fact");
    Spi::run(&format!(
        "CREATE TABLE {dimension}(id INT PRIMARY KEY, active BOOLEAN)"
    ))
    .expect("dimension");
    Spi::run(&format!(
        "INSERT INTO {dimension} VALUES (1, true), (2, false), (3, false), (4, false), (5, false)"
    ))
    .expect("seed dimension");
    Spi::run(&format!(
        "INSERT INTO {fact}(did, v) VALUES (1,10),(1,20),(2,5),(2,5),(2,5),(3,1),(4,50),(5,100)"
    ))
    .expect("seed fact");
    let fresh = format!(
        "SELECT f.did, SUM(f.v) AS s FROM {fact} f JOIN {dimension} d ON d.id = f.did \
         WHERE d.active GROUP BY f.did HAVING {}",
        having.replace("{d}", &dimension)
    );
    assert_eq!(v112_hv_create(&imv, &fresh), "CREATE REFLEX INCREMENTAL VIEW");
    (imv, dimension, fresh)
}

fn v112_hv_shortcut_sql(imv: &str, dimension: &str) -> String {
    v112_na_try(&format!(
        "SELECT COALESCE(string_agg(public.reflex_build_path_c_explain_sql(name, src), ' | '), 'RAISED: not a source') \
         FROM (SELECT DISTINCT name, src FROM public.__reflex_ivm_reference, unnest(depends_on) src \
               WHERE name = '{imv}' AND (src = '{dimension}' OR src LIKE '%.{dimension}')) registered"
    ))
}

#[pg_test]
fn v112_hv_sub_joined_dimension_gets_no_bulk_insert_shortcut() {
    let (plain, plain_dimension, _) = v112_hv_shortcut_fixture("pc_plain", "SUM(f.v) > 10");
    let offered = v112_hv_shortcut_sql(&plain, &plain_dimension);
    assert!(offered.starts_with("SELECT"), "{offered}");

    let (imv, dimension, fresh) = v112_hv_shortcut_fixture(
        "pc_sub",
        "SUM(f.v) > 10 * (SELECT COUNT(*) FROM {d} WHERE active)",
    );
    assert_eq!(v112_hv_shortcut_sql(&imv, &dimension), "");
    assert_imv_correct(&imv, &fresh);
    for promotion in [
        "SET active = true WHERE id IN (2, 4)",
        "SET active = true WHERE id = 5",
        "SET active = false WHERE id IN (1, 2)",
        "SET active = true WHERE id IN (1, 2, 3)",
    ] {
        Spi::run(&format!("UPDATE {dimension} {promotion}")).expect("promotion");
        assert_imv_correct(&imv, &fresh);
    }
}

// CAST of a compound operand, in HAVING and in the SELECT list
v112_hv_cells! {
    V112_HV_NULLABLE, false, None;
    v112_hv_cast_sum_division => "SELECT g, COUNT(*) AS c FROM {src} GROUP BY g HAVING CAST(SUM(v) / 2 AS numeric) > 2.4",
    v112_hv_cast_count_division => "SELECT g, SUM(v) AS s FROM {src} GROUP BY g HAVING CAST(COUNT(*) / 2 AS numeric) >= 1.5",
    v112_hv_cast_max_division => "SELECT g, COUNT(*) AS c FROM {src} GROUP BY g HAVING CAST(MAX(v) / 2 AS float8) > 3.4",
    v112_hv_cast_group_expression_typmod => "SELECT g, COUNT(*) AS c FROM {src} GROUP BY g HAVING CAST(g || 'x' AS varchar(1)) = 'a' AND COUNT(*) > 1",
    v112_hv_cast_group_function_division => "SELECT g, SUM(v) AS s FROM {src} GROUP BY g HAVING CAST(length(g) / 2 AS numeric) < 0.4 AND SUM(v) > 3",
    v112_hv_cast_mixed_addition_to_text => "SELECT g, SUM(v) AS s FROM {src} GROUP BY g HAVING CASE WHEN CAST(length(g) + COUNT(*) AS text) = '5' THEN TRUE ELSE SUM(v) > 10 END",
    v112_hv_cast_case => "SELECT g, COUNT(*) AS c FROM {src} GROUP BY g HAVING CAST(CASE WHEN SUM(v) > 5 THEN 1 ELSE 0 END AS boolean)",
    v112_hv_cast_unary_minus => "SELECT g, COUNT(*) AS c FROM {src} GROUP BY g HAVING CAST(- SUM(v) AS numeric) / 2 < -2.4 OR CAST(- COUNT(*) AS text) = '-2'",
    v112_hv_cast_nested_typmod => "SELECT g, COUNT(*) AS c FROM {src} GROUP BY g HAVING CAST(CAST(SUM(v) / 2 AS numeric(10, 1)) AS text) <> '2.0'",
    v112_hv_cast_comparison => "SELECT g, COUNT(*) AS c FROM {src} GROUP BY g HAVING CAST(SUM(v) > 5 AS int) = 1",
    v112_hv_cast_double_colon_parenthesised => "SELECT g, COUNT(*) AS c FROM {src} GROUP BY g HAVING (SUM(v) / 2)::numeric > 2.4 AND SUM(v)::numeric / 2 > 2.4",
    v112_hv_cast_selected_sum_division => "SELECT g, CAST(SUM(v) / 2 AS numeric) AS h FROM {src} GROUP BY g",
    v112_hv_cast_selected_sum_division_plus => "SELECT g, CAST(SUM(v) / 2 AS numeric) + 0 AS h FROM {src} GROUP BY g",
    v112_hv_cast_selected_count_division => "SELECT g, COALESCE(CAST(COUNT(*) / 2 AS numeric), 0) AS h FROM {src} GROUP BY g",
    v112_hv_cast_selected_and_having => "SELECT g, CAST(SUM(v) / 2 AS numeric) AS h FROM {src} GROUP BY g HAVING CAST(SUM(v) / 2 AS numeric) > 2.4",
    v112_hv_cast_selected_ungrouped => "SELECT CAST(SUM(v) / 3 AS numeric) AS h FROM {src} HAVING CAST(COUNT(*) / 2 AS numeric) > 2.4",
}

v112_hv_cells! {
    V112_HV_NULLABLE, true, None;
    v112_hv_def_cast_sum_division => "SELECT g, COUNT(*) AS c FROM {src} GROUP BY g HAVING CAST(SUM(v) / 2 AS numeric) > 2.4",
    v112_hv_def_cast_selected_sum_division => "SELECT g, CAST(SUM(v) / 2 AS numeric) AS h FROM {src} GROUP BY g",
}

v112_hv_cells! {
    V112_HV_NOT_NULL, false, None;
    v112_hv_nn_cast_sum_division => "SELECT g, COUNT(*) AS c FROM {src} GROUP BY g HAVING CAST(SUM(v) / 2 AS numeric) > 2.4",
    v112_hv_nn_cast_selected_sum_division => "SELECT g, CAST(SUM(v) / 2 AS numeric) AS h FROM {src} GROUP BY g",
}

// The only aggregate HAVING holds is the selected COUNT(DISTINCT), under an
// expression the end query evaluates as written
v112_hv_cells! {
    V112_HV_NULLABLE, false, None;
    v112_hv_count_distinct_between => "SELECT g, COUNT(DISTINCT v) AS d FROM {src} GROUP BY g HAVING COUNT(DISTINCT v) BETWEEN 2 AND 3",
    v112_hv_count_distinct_in_list => "SELECT g, COUNT(DISTINCT v) AS d FROM {src} GROUP BY g HAVING COUNT(DISTINCT v) IN (1, 3) AND g <> 'b'",
}

/// The regrouped end query holds one row per distinct value, so any other
/// aggregate it evaluated would see the distinct values instead of the rows:
/// group `d` holds the value 4 three times.
#[pg_test]
fn v112_hv_count_distinct_with_other_aggregate_under_any_expression_is_rejected() {
    let src = v112_hv_source("cd_hidden");
    let hidden_aggregates = [
        "SUM(v) BETWEEN 3 AND 13",
        "COUNT(*) IN (3, 4, 5)",
        "(COUNT(v) > 2) IS TRUE",
        "COUNT(DISTINCT v) > 1 AND COUNT(*) BETWEEN 3 AND 9",
        "SUM(v) > 5 IS TRUE",
        "SUM(v)::text LIKE '1%'",
        "COUNT(v) = ANY (ARRAY[3, 4, 5])",
        "SUM(v) IS NOT NULL",
        "MAX(v) IS NOT DISTINCT FROM 4",
        "AVG(v) NOT BETWEEN 1 AND 2",
        "COUNT(*) > ALL (ARRAY[1, 2])",
        "pg_catalog.count(*) > 2",
        "pg_catalog.sum(v) BETWEEN 3 AND 13",
        "\"count\"(*) > 2",
    ];
    for (position, having) in hidden_aggregates.iter().enumerate() {
        let imv = format!("v112_hv_cd_hidden_{position}");
        let outcome = v112_hv_create(
            &imv,
            &format!("SELECT g, COUNT(DISTINCT v) AS d FROM {src} GROUP BY g HAVING {having}"),
        );
        assert!(
            outcome.starts_with(
                "ERROR: [reflex-unsupported] COUNT(DISTINCT col) cannot be mixed with other aggregates"
            ) && outcome.contains("CTE"),
            "{having}: {outcome}"
        );
        v112_hv_assert_nothing_created(&imv);
    }
}

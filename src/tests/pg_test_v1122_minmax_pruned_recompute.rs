// The MIN/MAX recompute of the groups whose top-K heap shrank reads the source
// scoped to those groups. With literal group keys the planner prunes a source
// partitioned on a group column; with the correlated EXISTS over the
// shrunk-groups table it reads every partition. The literal form is taken only
// when it selects exactly what the EXISTS form selects: no NULL key, every key
// type in the round-trip allowlist, and at most
// `MIN_MAX_RECOMPUTE_LITERAL_KEY_CAP` groups.

/// Source partitioned LIST (plan) then RANGE (d), one leaf per (plan, year),
/// named `{p}_src_{plan}_{year}`. `key_column` is a third grouping column of the
/// declared type, filled by `key_value` (an SQL expression over `plan`, `k` and `d`).
fn mmp_source(p: &str, plans: &[&str], key_type: &str, key_value: &str, ks: i32) {
    Spi::run(&format!(
        "CREATE TABLE {p}_src (plan INT, d DATE NOT NULL, yr INT NOT NULL, k INT NOT NULL, \
           key_column {key_type}, v INT NOT NULL) PARTITION BY LIST (plan)"
    ))
    .expect("source");
    for plan in plans {
        let label = if *plan == "NULL" { "n".to_string() } else { plan.to_string() };
        Spi::run(&format!(
            "CREATE TABLE {p}_src_{label} PARTITION OF {p}_src FOR VALUES IN ({plan}) \
             PARTITION BY RANGE (d)"
        ))
        .expect("plan partition");
        for year in [2027, 2028] {
            Spi::run(&format!(
                "CREATE TABLE {p}_src_{label}_{year} PARTITION OF {p}_src_{label} \
                 FOR VALUES FROM ('{year}-01-01') TO ('{next}-01-01')",
                next = year + 1
            ))
            .expect("leaf");
        }
        Spi::run(&format!(
            "INSERT INTO {p}_src (plan, d, yr, k, key_column, v) \
             SELECT {plan}, d::date, EXTRACT(YEAR FROM d)::int, k, NULL, k * 7 \
             FROM generate_series(DATE '2027-11-01', DATE '2028-02-28', INTERVAL '1 day') d, \
                  generate_series(1, {ks}) k"
        ))
        .expect("rows");
    }
    Spi::run(&format!("UPDATE {p}_src SET key_column = {key_value}")).expect("keys");
    Spi::run(&format!("ANALYZE {p}_src")).expect("analyze");
}

fn mmp_create(p: &str, select: &str, group: &str, mode: &str) {
    Spi::run(&format!(
        "SELECT create_reflex_ivm('{p}_v', 'SELECT {group}, {select} FROM {p}_src GROUP BY {group}', \
           NULL, NULL, '{mode}', NULL, ARRAY[]::text[])"
    ))
    .expect("create IMV");
}

/// Sequential and index scans, in this transaction, of the leaves of `{p}_src`
/// whose plan is not one of `touched` (leaf-name labels: `2`, `n`, ...).
fn mmp_untouched_leaf_scans(p: &str, touched: &[&str]) -> i64 {
    let excluded = touched
        .iter()
        .map(|label| format!("'{p}_src_{label}'"))
        .collect::<Vec<_>>()
        .join(", ");
    Spi::get_one::<i64>(&format!(
        "SELECT COALESCE(sum(pg_stat_get_xact_numscans(i.inhrelid)), 0)::int8 \
         FROM pg_inherits i JOIN pg_inherits plan_level ON plan_level.inhrelid = i.inhparent \
         JOIN pg_class plan_rel ON plan_rel.oid = plan_level.inhrelid \
         WHERE plan_level.inhparent = '{p}_src'::regclass \
           AND plan_rel.relname NOT IN ({excluded})"
    ))
    .expect("scan count")
    .expect("scan count NULL")
}

/// The live groups of the intermediate are exactly those of the stored base
/// query, with the same scalars and counts, and each top-K heap a prefix of the
/// true top K (a heap may hold fewer than K after a retraction it did not need to
/// refill).
fn mmp_assert_intermediate_correct(p: &str) {
    let base_query = Spi::get_one::<String>(&format!(
        "SELECT base_query FROM public.__reflex_ivm_reference WHERE name = '{p}_v'"
    ))
    .expect("base query")
    .expect("base query NULL");
    let columns: Vec<String> = Spi::get_one::<Vec<String>>(&format!(
        "SELECT array_agg(attname::text ORDER BY attnum) FROM pg_attribute \
         WHERE attrelid = '__reflex_intermediate_{p}_v'::regclass AND attnum > 0 AND NOT attisdropped"
    ))
    .expect("columns")
    .expect("columns NULL");
    let same = |c: &String| format!("i.\"{c}\" IS NOT DISTINCT FROM f.\"{c}\"");
    let group_match = columns
        .iter()
        .filter(|c| !c.starts_with("__"))
        .map(same)
        .collect::<Vec<_>>()
        .join(" AND ");
    let differs = columns
        .iter()
        .filter(|c| c.starts_with("__"))
        .map(|c| {
            if c.ends_with("_topk") {
                format!("f.\"{c}\"[1:cardinality(i.\"{c}\")] IS DISTINCT FROM i.\"{c}\"")
            } else {
                format!("NOT ({})", same(c))
            }
        })
        .collect::<Vec<_>>()
        .join(" OR ");
    let wrong = Spi::get_one::<i64>(&format!(
        "WITH live AS (SELECT * FROM __reflex_intermediate_{p}_v WHERE __ivm_count <> 0), \
              fresh AS ({base_query}) \
         SELECT ((SELECT count(*) FROM live i WHERE NOT EXISTS \
                    (SELECT 1 FROM fresh f WHERE {group_match} AND NOT ({differs}))) \
               + (SELECT count(*) FROM fresh f WHERE NOT EXISTS \
                    (SELECT 1 FROM live i WHERE {group_match})))::int8"
    ))
    .expect("intermediate oracle")
    .expect("intermediate oracle NULL");
    assert_eq!(wrong, 0, "{p}: intermediate groups differ from the base query");
}

fn mmp_assert_correct(p: &str, fresh: &str) {
    assert_imv_correct(&format!("{p}_v"), fresh);
    mmp_assert_intermediate_correct(p);
}

/// Runs `dml` and, in DEFERRED mode, the flush; returns the scans it took of the
/// leaves of the plans outside `touched`.
fn mmp_apply(p: &str, mode: &str, dml: &str, touched: &[&str]) -> i64 {
    Spi::run("SET LOCAL max_parallel_workers_per_gather = 0").expect("serial plans");
    let before = mmp_untouched_leaf_scans(p, touched);
    Spi::run(dml).expect("dml");
    if mode == "DEFERRED" {
        Spi::run(&format!("SELECT reflex_flush_deferred('{p}_src')")).expect("flush");
    }
    mmp_untouched_leaf_scans(p, touched) - before
}

fn mmp_not_stale(p: &str) {
    let stale = Spi::get_one::<bool>(&format!(
        "SELECT COALESCE(known_stale, FALSE) FROM reflex_ivm_status() WHERE name = '{p}_v'"
    ))
    .expect("status")
    .unwrap_or(false);
    assert!(!stale, "{p}_v was marked stale by its maintenance");
}

const MMP_DATES: &str = "MIN(d) AS lo, MAX(d) AS hi";

/// Moves the 16 earliest days of each year of the matching rows 40 days later,
/// within the year: at least K = 16 rows leave the top-K heap of every group
/// they belong to, so only a recompute from the source restores its MIN.
fn mmp_beyond_the_heap(p: &str, rows: &str) -> String {
    format!(
        "UPDATE {p}_src SET d = d + 40 WHERE ({rows}) \
         AND (d < '2027-11-17' OR d BETWEEN '2028-01-01' AND '2028-01-16')"
    )
}

fn mmp_pruned_recompute(p: &str, mode: &str) {
    mmp_source(p, &["1", "2", "3"], "INT", "0", 3);
    mmp_create(p, MMP_DATES, "plan, yr", mode);
    let fresh = format!("SELECT plan, yr, MIN(d), MAX(d) FROM {p}_src GROUP BY plan, yr");

    let whole_plan = mmp_apply(
        p,
        mode,
        &format!("UPDATE {p}_src SET d = d + 1, k = k + 100 WHERE plan = 2"),
        &["2"],
    );
    mmp_not_stale(p);
    mmp_assert_correct(p, &fresh);

    let beyond_the_heap = mmp_apply(
        p,
        mode,
        &mmp_beyond_the_heap(p, "plan = 3"),
        &["3"],
    );
    mmp_not_stale(p);
    mmp_assert_correct(p, &fresh);

    let on_the_max = mmp_apply(
        p,
        mode,
        &format!("DELETE FROM {p}_src WHERE plan = 1 AND d = '2028-02-28'"),
        &["1"],
    );
    mmp_not_stale(p);
    mmp_assert_correct(p, &fresh);

    assert_eq!(
        (whole_plan, beyond_the_heap, on_the_max),
        (0, 0, 0),
        "the recompute of the shrunk groups must not read the leaves of other plans"
    );
}

#[pg_test]
fn v1122_minmax_recompute_prunes_deferred() {
    mmp_pruned_recompute("mmp_d", "DEFERRED");
}

#[pg_test]
fn v1122_minmax_recompute_prunes_immediate() {
    mmp_pruned_recompute("mmp_i", "IMMEDIATE");
}

/// A NULL group key is never matched by a literal list: the groups are
/// recomputed through the NULL-safe EXISTS form, which reads every leaf.
#[pg_test]
fn v1122_minmax_recompute_null_key_keeps_exists() {
    let p = "mmp_null";
    mmp_source(p, &["1", "2", "NULL"], "INT", "0", 3);
    mmp_create(p, MMP_DATES, "plan, yr", "DEFERRED");
    let fresh = format!("SELECT plan, yr, MIN(d), MAX(d) FROM {p}_src GROUP BY plan, yr");
    let scans = mmp_apply(
        p,
        "DEFERRED",
        &mmp_beyond_the_heap(p, "plan IS NULL OR plan = 2"),
        &["n", "2"],
    );
    mmp_not_stale(p);
    mmp_assert_correct(p, &fresh);
    assert!(scans > 0, "a NULL key must take the EXISTS form");
}

/// A float key printed with fewer digits than it holds would not match its own
/// group as a literal.
#[pg_test]
fn v1122_minmax_recompute_float_key_keeps_exists() {
    let p = "mmp_float";
    mmp_source(p, &["1", "2"], "FLOAT8", "0.1::float8 * k + 0.2::float8", 3);
    mmp_create(p, MMP_DATES, "plan, key_column", "DEFERRED");
    let fresh = format!("SELECT plan, key_column, MIN(d), MAX(d) FROM {p}_src GROUP BY plan, key_column");
    Spi::run("SET LOCAL extra_float_digits = -14").expect("short float output");
    let scans = mmp_apply(
        p,
        "DEFERRED",
        &mmp_beyond_the_heap(p, "plan = 2"),
        &["2"],
    );
    Spi::run("SET LOCAL extra_float_digits = 1").expect("float output");
    mmp_not_stale(p);
    mmp_assert_correct(p, &fresh);
    assert!(scans > 0, "a float key must take the EXISTS form");
}

/// Interval is outside the allowlist, and so is a domain over an allowlisted type.
#[pg_test]
fn v1122_minmax_recompute_unlisted_types_keep_exists() {
    let p = "mmp_iv";
    mmp_source(p, &["1", "2"], "INTERVAL", "make_interval(days => k)", 3);
    mmp_create(p, MMP_DATES, "plan, key_column", "DEFERRED");
    let fresh = format!("SELECT plan, key_column, MIN(d), MAX(d) FROM {p}_src GROUP BY plan, key_column");
    let scans = mmp_apply(
        p,
        "DEFERRED",
        &mmp_beyond_the_heap(p, "plan = 2"),
        &["2"],
    );
    mmp_not_stale(p);
    mmp_assert_correct(p, &fresh);
    assert!(scans > 0, "an interval key must take the EXISTS form");

    let p = "mmp_dom";
    Spi::run("CREATE DOMAIN mmp_dom_code AS TEXT CHECK (VALUE <> '')").expect("domain");
    mmp_source(p, &["1", "2"], "mmp_dom_code", "'c' || k", 3);
    mmp_create(p, MMP_DATES, "plan, key_column", "DEFERRED");
    let fresh = format!("SELECT plan, key_column, MIN(d), MAX(d) FROM {p}_src GROUP BY plan, key_column");
    mmp_apply(
        p,
        "DEFERRED",
        &mmp_beyond_the_heap(p, "plan = 2"),
        &["2"],
    );
    mmp_not_stale(p);
    mmp_assert_correct(p, &fresh);
}

/// Under DateStyle SQL a timestamptz prints its zone as an abbreviation, and
/// the abbreviation reads back through timezone_abbreviations: IST prints for
/// Asia/Kolkata (+05:30) and reads back as Israel (+02). Its keys are not listed.
#[pg_test]
fn v1122_minmax_recompute_timestamptz_key_keeps_exists() {
    Spi::run("SET LOCAL DateStyle = 'SQL, DMY'").expect("datestyle");
    Spi::run("SET LOCAL TimeZone = 'Asia/Kolkata'").expect("timezone");
    let p = "mmp_tz";
    mmp_source(
        p,
        &["1", "2"],
        "TIMESTAMPTZ",
        "TIMESTAMPTZ '2027-03-04 05:06:07.123456+02' + make_interval(secs => k)",
        3,
    );
    mmp_create(p, MMP_DATES, "plan, key_column", "DEFERRED");
    let fresh = format!("SELECT plan, key_column, MIN(d), MAX(d) FROM {p}_src GROUP BY plan, key_column");
    let scans = mmp_apply(p, "DEFERRED", &mmp_beyond_the_heap(p, "plan = 2"), &["2"]);
    mmp_not_stale(p);
    mmp_assert_correct(p, &fresh);
    assert!(scans > 0, "a timestamptz key must take the EXISTS form");
}

/// Up to the cap the keys are listed and the scan is pruned; one more group
/// takes the EXISTS form.
#[pg_test]
fn v1122_minmax_recompute_key_cap() {
    let cap = crate::trigger::MIN_MAX_RECOMPUTE_LITERAL_KEY_CAP as i32;
    for (p, groups, pruned) in [("mmp_cap", cap, true), ("mmp_over", cap + 1, false)] {
        mmp_source(p, &["1", "2"], "INT", "k", 1);
        Spi::run(&format!(
            "INSERT INTO {p}_src (plan, d, yr, k, key_column, v) \
             SELECT plan, DATE '2027-11-01', 2027, 0, g, 0 \
             FROM generate_series(1, 2) plan, generate_series(10, {groups} + 9) g"
        ))
        .expect("one row per extra group");
        Spi::run(&format!("ANALYZE {p}_src")).expect("analyze");
        mmp_create(p, MMP_DATES, "plan, key_column", "DEFERRED");
        let fresh = format!("SELECT plan, key_column, MIN(d), MAX(d) FROM {p}_src GROUP BY plan, key_column");
        let affected_groups = Spi::get_one::<i64>(&format!(
            "SELECT count(DISTINCT key_column)::int8 FROM {p}_src WHERE plan = 2"
        ))
        .expect("groups")
        .expect("groups NULL");
        assert_eq!(affected_groups, groups as i64 + 1, "fixture: groups of plan 2");
        let scans = mmp_apply(
            p,
            "DEFERRED",
            &format!(
                "UPDATE {p}_src SET d = d + 1 WHERE plan = 2 AND key_column <> 1"
            ),
            &["2"],
        );
        mmp_not_stale(p);
        mmp_assert_correct(p, &fresh);
        assert_eq!(
            scans == 0,
            pruned,
            "{groups} shrunk groups (cap {cap}): pruned expected {pruned}, scans {scans}"
        );
    }
}

/// Keys holding quotes, backslashes and non-ASCII are listed as literals and
/// still match their own groups.
#[pg_test]
fn v1122_minmax_recompute_text_key_quoting() {
    let p = "mmp_txt";
    mmp_source(
        p,
        &["1", "2"],
        "TEXT",
        "(ARRAY['O''Brien', 'back\\slash', 'é\"€ 漢字', E'tab\\tline'])[k]",
        4,
    );
    mmp_create(p, MMP_DATES, "plan, key_column", "DEFERRED");
    let fresh = format!("SELECT plan, key_column, MIN(d), MAX(d) FROM {p}_src GROUP BY plan, key_column");
    let scans = mmp_apply(
        p,
        "DEFERRED",
        &mmp_beyond_the_heap(p, "plan = 2"),
        &["2"],
    );
    mmp_not_stale(p);
    mmp_assert_correct(p, &fresh);
    assert_eq!(scans, 0, "text keys are listed as literals");
}

/// Other allowlisted key types, under a non-ISO DateStyle and a non-UTC zone.
#[pg_test]
fn v1122_minmax_recompute_allowlisted_types_round_trip() {
    Spi::run("SET LOCAL DateStyle = 'SQL, DMY'").expect("datestyle");
    Spi::run("SET LOCAL TimeZone = 'Asia/Kolkata'").expect("timezone");
    for (p, key_type, key_value) in [
        ("mmp_num", "NUMERIC(12,4)", "k / 3.0"),
        ("mmp_ts", "TIMESTAMP", "TIMESTAMP '2027-03-04 05:06:07.123456' + make_interval(secs => k)"),
        ("mmp_vc", "VARCHAR(10)", "'v' || k"),
        ("mmp_big", "INT8", "k * 10000000000"),
        ("mmp_bool", "BOOL", "k % 2 = 0"),
        ("mmp_day", "DATE", "DATE '2027-02-03' + k"),
        ("mmp_uid", "UUID", "md5(k::text)::uuid"),
        ("mmp_chr", "CHAR(6)", "'c' || k"),
    ] {
        mmp_source(p, &["1", "2"], key_type, key_value, 3);
        mmp_create(p, MMP_DATES, "plan, key_column", "DEFERRED");
        let fresh =
            format!("SELECT plan, key_column, MIN(d), MAX(d) FROM {p}_src GROUP BY plan, key_column");
        let scans = mmp_apply(
            p,
            "DEFERRED",
            &mmp_beyond_the_heap(p, "plan = 2"),
            &["2"],
        );
        mmp_not_stale(p);
        mmp_assert_correct(p, &fresh);
        assert_eq!(scans, 0, "{key_type} keys are listed as literals");
    }
}

/// MIN/MAX next to SUM/COUNT/AVG, an expression key, over an unpartitioned source.
#[pg_test]
fn v1122_minmax_recompute_mixed_aggregates_unpartitioned() {
    let p = "mmp_mix";
    Spi::run(&format!(
        "CREATE TABLE {p}_src (plan INT NOT NULL, d DATE NOT NULL, k INT NOT NULL, v INT NOT NULL)"
    ))
    .expect("source");
    Spi::run(&format!(
        "INSERT INTO {p}_src SELECT plan, d::date, k, k * plan \
         FROM generate_series(1, 3) plan, \
              generate_series(DATE '2027-11-01', DATE '2028-02-28', INTERVAL '1 day') d, \
              generate_series(1, 3) k"
    ))
    .expect("rows");
    Spi::run(&format!(
        "SELECT create_reflex_ivm('{p}_v', \
           'SELECT plan, EXTRACT(YEAR FROM d)::int AS yr, MIN(d) AS lo, MAX(v) AS hi, \
                   SUM(v) AS total, COUNT(*) AS n, AVG(v) AS mean \
            FROM {p}_src GROUP BY plan, EXTRACT(YEAR FROM d)::int', NULL, NULL, 'DEFERRED')"
    ))
    .expect("create IMV");
    let fresh = format!(
        "SELECT plan, EXTRACT(YEAR FROM d)::int, MIN(d), MAX(v), SUM(v), COUNT(*), AVG(v) \
         FROM {p}_src GROUP BY plan, EXTRACT(YEAR FROM d)::int"
    );
    for dml in [
        format!("UPDATE {p}_src SET d = d + 1, v = v + 5 WHERE plan = 2"),
        mmp_beyond_the_heap(p, "plan = 3"),
        format!("DELETE FROM {p}_src WHERE plan = 1 AND v = (SELECT max(v) FROM {p}_src WHERE plan = 1)"),
        format!("UPDATE {p}_src SET d = '2026-12-31' WHERE plan = 1 AND k = 1 AND d < '2027-11-05'"),
    ] {
        Spi::run(&dml).expect("dml");
        Spi::run(&format!("SELECT reflex_flush_deferred('{p}_src')")).expect("flush");
        mmp_not_stale(p);
        mmp_assert_correct(p, &fresh);
    }
}

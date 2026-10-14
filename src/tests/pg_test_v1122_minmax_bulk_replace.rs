// A DEFERRED flush of a grouped top-K MIN/MAX IMV whose staged rows reach
// `MIN_MAX_BULK_REPLACE_RATIO` of the source rows of the groups they touch
// replaces those groups from the base query instead of aggregating the old and
// new delta. Every other shape, and a smaller delta, keeps the algebraic path.
//
// Which path ran: the algebraic path truncates the IMV's delta scratch table
// first thing; the replacement never touches it. A marker row planted there
// before the flush survives only the replacement.

const MMB_MARKER_COUNT: i64 = -424242;

fn mmb_plant_scratch_marker(p: &str) {
    Spi::run(&format!(
        "TRUNCATE __reflex_scratch_{p}_v; \
         INSERT INTO __reflex_scratch_{p}_v SELECT * FROM __reflex_intermediate_{p}_v LIMIT 1; \
         UPDATE __reflex_scratch_{p}_v SET __ivm_count = {MMB_MARKER_COUNT}"
    ))
    .expect("plant marker");
    assert!(mmb_replaced(p), "{p}: the intermediate must hold a row to plant");
}

fn mmb_replaced(p: &str) -> bool {
    Spi::get_one::<bool>(&format!(
        "SELECT EXISTS (SELECT 1 FROM __reflex_scratch_{p}_v WHERE __ivm_count = {MMB_MARKER_COUNT})"
    ))
    .expect("marker")
    .expect("marker NULL")
}

/// Runs `create_reflex_ivm(<args>)` and fails on an error status.
fn mmb_create(args: &str) {
    let status = Spi::get_one::<String>(&format!("SELECT create_reflex_ivm({args})"))
        .expect("create IMV")
        .expect("create status NULL");
    assert!(!status.starts_with("ERROR"), "create_reflex_ivm({args}): {status}");
}

/// Runs `dmls` in order, flushes `sources`, and tells whether the flush replaced
/// the groups.
fn mmb_apply_all(p: &str, dmls: &[String], sources: &[&str]) -> bool {
    mmb_plant_scratch_marker(p);
    for dml in dmls {
        Spi::run(dml).expect("dml");
    }
    for source in sources {
        Spi::run(&format!("SELECT reflex_flush_deferred('{source}')")).expect("flush");
    }
    mmb_replaced(p)
}

fn mmb_apply(p: &str, dml: &str) -> bool {
    mmb_apply_all(p, &[dml.to_string()], &[&format!("{p}_src")])
}

fn mmb_check(p: &str, fresh: &str) {
    mmp_not_stale(p);
    mmp_assert_correct(p, fresh);
}

fn mmb_dates_fresh(p: &str) -> String {
    format!("SELECT plan, yr, MIN(d), MAX(d) FROM {p}_src GROUP BY plan, yr")
}

/// Plans 1..=`plans`, two (plan, year) groups each, `ks` rows a day.
fn mmb_dates_imv(p: &str, plans: usize, ks: i32) {
    let plan_labels: Vec<String> = (1..=plans).map(|plan| plan.to_string()).collect();
    let plan_refs: Vec<&str> = plan_labels.iter().map(String::as_str).collect();
    mmp_source(p, &plan_refs, "INT", "0", ks);
    mmp_create(p, MMP_DATES, "plan, yr", "DEFERRED");
}

/// A whole-plan UPDATE empties the top-K heaps of the plan's groups. Six plans:
/// the staged rows are twice the plan's rows but a third of all source rows.
#[pg_test]
fn v1122_minmax_bulk_whole_plan_update() {
    let p = "mmb_whole";
    mmb_dates_imv(p, 6, 3);
    let replaced = mmb_apply(p, &format!("UPDATE {p}_src SET d = d + 1, k = k + 100 WHERE plan = 2"));
    mmb_check(p, &mmb_dates_fresh(p));
    assert!(replaced, "a whole-plan UPDATE must replace the plan's groups");
}

/// Rows in the middle of the year move: no heap shrinks, the delta is still
/// larger than the group.
#[pg_test]
fn v1122_minmax_bulk_no_group_shrunk() {
    let p = "mmb_mid";
    mmb_dates_imv(p, 3, 3);
    let replaced = mmb_apply(
        p,
        &format!(
            "UPDATE {p}_src SET d = d + 1, k = k + 100 \
             WHERE plan = 2 AND d BETWEEN '2027-11-20' AND '2027-12-20'"
        ),
    );
    mmb_check(p, &mmb_dates_fresh(p));
    assert!(replaced, "staged rows above the ratio replace the group even when no heap shrinks");
}

#[pg_test]
fn v1122_minmax_bulk_creates_and_deletes_groups() {
    let p = "mmb_life";
    mmb_dates_imv(p, 3, 3);
    let fresh = mmb_dates_fresh(p);
    let group_count = |plan: i32, yr: i32| -> (i64, i64) {
        let count = |relation: String| {
            Spi::get_one::<i64>(&format!(
                "SELECT count(*) FROM {relation} WHERE plan = {plan} AND yr = {yr}"
            ))
            .expect("group count")
            .expect("group count NULL")
        };
        (count(format!("{p}_v")), count(format!("__reflex_intermediate_{p}_v")))
    };

    let created = mmb_apply(
        p,
        &format!(
            "INSERT INTO {p}_src (plan, d, yr, k, key_column, v) \
             SELECT 2, d::date, 2031, k, 0, k FROM generate_series(DATE '2027-12-01', DATE '2027-12-10', INTERVAL '1 day') d, \
             generate_series(1, 3) k"
        ),
    );
    mmb_check(p, &fresh);
    assert!(created, "a batch creating a group replaces it");
    assert_eq!(group_count(2, 2031), (1, 1), "the created group is in the target and the intermediate");

    let deleted = mmb_apply(p, &format!("DELETE FROM {p}_src WHERE plan = 2 AND yr = 2028"));
    mmb_check(p, &fresh);
    assert!(deleted, "a batch deleting a group replaces it");
    assert_eq!(group_count(2, 2028), (0, 0), "the deleted group is gone from the target and the intermediate");

    let moved = mmb_apply(p, &format!("UPDATE {p}_src SET yr = 2030 WHERE plan = 1 AND yr = 2027"));
    mmb_check(p, &fresh);
    assert!(moved, "a batch moving a group's rows to a new group replaces both");
    assert_eq!(group_count(1, 2027), (0, 0), "the emptied group is gone");
    assert_eq!(group_count(1, 2030), (1, 1), "the new group exists");
}

#[pg_test]
fn v1122_minmax_bulk_delete_whole_plan() {
    let p = "mmb_drop";
    mmb_dates_imv(p, 3, 3);
    let replaced = mmb_apply(p, &format!("DELETE FROM {p}_src WHERE plan = 3"));
    mmb_check(p, &mmb_dates_fresh(p));
    assert!(replaced, "deleting a whole plan replaces its groups");
    let left = Spi::get_one::<i64>(&format!(
        "SELECT (SELECT count(*) FROM {p}_v WHERE plan = 3) \
              + (SELECT count(*) FROM __reflex_intermediate_{p}_v WHERE plan = 3)"
    ))
    .expect("left rows")
    .expect("left rows NULL");
    assert_eq!(left, 0, "no row of the deleted plan may remain");
}

/// MIN/MAX next to SUM/COUNT/AVG over an expression key, and the same with a
/// HAVING, over an unpartitioned source: replaced above the ratio, algebraic below.
#[pg_test]
fn v1122_minmax_bulk_mixed_aggregates() {
    let p = "mmb_mix";
    Spi::run(&format!(
        "CREATE TABLE {p}_src (plan INT NOT NULL, d DATE NOT NULL, k INT NOT NULL, v INT NOT NULL); \
         INSERT INTO {p}_src SELECT plan, d::date, k, k * plan \
         FROM generate_series(1, 4) plan, \
              generate_series(DATE '2027-11-01', DATE '2028-02-28', INTERVAL '1 day') d, \
              generate_series(1, 3) k"
    ))
    .expect("source");
    let select = "SELECT plan, EXTRACT(YEAR FROM d)::int AS yr, MIN(d) AS lo, MAX(v) AS hi, \
                  SUM(v) AS total, COUNT(*) AS n, AVG(v) AS mean FROM {src} \
                  GROUP BY plan, EXTRACT(YEAR FROM d)::int";
    let views = [(p.to_string(), ""), (format!("{p}h"), " HAVING COUNT(*) > 100")];
    for (name, having) in &views {
        let query = format!("{}{}", select.replace("{src}", &format!("{p}_src")), having);
        mmb_create(&format!(
            "'{name}_v', '{}', NULL, NULL, 'DEFERRED', NULL, ARRAY[]::text[]",
            query.replace('\'', "''")
        ));
    }
    let fresh = |having: &str| {
        format!(
            "SELECT plan, EXTRACT(YEAR FROM d)::int, MIN(d), MAX(v), SUM(v), COUNT(*), AVG(v) \
             FROM {p}_src GROUP BY plan, EXTRACT(YEAR FROM d)::int{having}"
        )
    };
    let steps = [
        (format!("UPDATE {p}_src SET d = d + 1, v = v + 5 WHERE plan = 2"), true),
        (format!("UPDATE {p}_src SET v = v + 1000 WHERE plan = 3 AND d = '2027-11-05' AND k = 1"), false),
        (format!("DELETE FROM {p}_src WHERE plan = 4 AND d >= '2028-01-01' AND d < '2028-02-20'"), true),
        (format!("UPDATE {p}_src SET d = '2026-12-31' WHERE plan = 1 AND d < '2027-11-20'"), true),
    ];
    for (dml, expect_replaced) in &steps {
        for (name, _) in &views {
            mmb_plant_scratch_marker(name);
        }
        Spi::run(dml).expect("dml");
        Spi::run(&format!("SELECT reflex_flush_deferred('{p}_src')")).expect("flush");
        for (name, having) in &views {
            mmb_check(name, &fresh(having));
            assert_eq!(mmb_replaced(name), *expect_replaced, "{name}: replaced after `{dml}`");
        }
    }
}

/// One row moved off a group's minimum: far below the ratio.
#[pg_test]
fn v1122_minmax_bulk_below_ratio_keeps_algebraic_path() {
    let p = "mmb_small";
    mmb_dates_imv(p, 3, 3);
    let replaced = mmb_apply(
        p,
        &format!("UPDATE {p}_src SET d = d + 40, k = k + 100 WHERE plan = 2 AND d = '2027-11-01' AND k = 1"),
    );
    mmb_check(p, &mmb_dates_fresh(p));
    assert!(!replaced, "a small delta keeps the algebraic path");
}

/// Group (2, 2027) holds 61 days x 4 rows = 244 rows. Moving 61 of its rows
/// stages 122 = 244 / 2 rows: replaced. Moving 60 stages 120: algebraic.
#[pg_test]
fn v1122_minmax_bulk_ratio_boundary() {
    let p = "mmb_edge";
    mmb_dates_imv(p, 3, 4);
    let fresh = mmb_dates_fresh(p);
    let at = mmb_apply(
        p,
        &format!("UPDATE {p}_src SET d = d - 1, k = k + 100 WHERE plan = 2 AND yr = 2027 AND k = 1"),
    );
    mmb_check(p, &fresh);
    assert!(at, "staged rows equal to the ratio times the group rows replace the group");
    let below = mmb_apply(
        p,
        &format!(
            "UPDATE {p}_src SET d = d - 1, k = k + 100 WHERE plan = 2 AND yr = 2027 AND k = 2 \
             AND d > (SELECT min(d) FROM {p}_src WHERE plan = 2 AND yr = 2027 AND k = 2)"
        ),
    );
    mmb_check(p, &fresh);
    assert!(!below, "one staged row pair under the ratio keeps the algebraic path");
}

/// An IMMEDIATE statement trigger never replaces: another trigger of the same
/// statement (a writable CTE) applies its delta on top of what a replacement
/// would already have read. (An INSERT CTE followed by an UPDATE of the same
/// groups already leaves a phantom heap entry today: untreated_bugs/.)
#[pg_test]
fn v1122_minmax_bulk_immediate_keeps_algebraic_path() {
    let p = "mmb_imm";
    mmp_source(p, &["1", "2", "3"], "INT", "0", 3);
    mmp_create(p, &format!("{MMP_DATES}, COUNT(*) AS n"), "plan, yr", "IMMEDIATE");
    let fresh = format!("SELECT plan, yr, MIN(d), MAX(d), COUNT(*) FROM {p}_src GROUP BY plan, yr");
    let insert_plan = |plan: i32, shift: i32| {
        format!(
            "INSERT INTO {p}_src (plan, d, yr, k, key_column, v) \
             SELECT plan, d, yr, k + {shift}, key_column, v FROM {p}_src WHERE plan = {plan} AND k = 1"
        )
    };
    let update_plan = |plan: i32| format!("UPDATE {p}_src SET d = d + 1, k = k + 100 WHERE plan = {plan}");
    for dml in [
        update_plan(2),
        format!("WITH u AS ({} RETURNING 1) {}", update_plan(3), insert_plan(3, 1000)),
    ] {
        mmb_plant_scratch_marker(p);
        Spi::run(&dml).expect("dml");
        mmb_check(p, &fresh);
        assert!(!mmb_replaced(p), "IMMEDIATE must keep the algebraic path: `{dml}`");
    }
}

#[pg_test]
fn v1122_minmax_bulk_no_group_by_keeps_algebraic_path() {
    let p = "mmb_scalar";
    mmp_source(p, &["1", "2"], "INT", "0", 3);
    mmb_create(&format!(
        "'{p}_v', 'SELECT MIN(d) AS lo, MAX(d) AS hi, COUNT(*) AS n FROM {p}_src', \
         NULL, NULL, 'DEFERRED', NULL, ARRAY[]::text[]"
    ));
    let replaced = mmb_apply(p, &format!("UPDATE {p}_src SET d = d + 1, k = k + 100"));
    mmp_not_stale(p);
    assert_imv_correct(&format!("{p}_v"), &format!("SELECT MIN(d), MAX(d), COUNT(*) FROM {p}_src"));
    assert!(!replaced, "an IMV without GROUP BY keeps the algebraic path");
}

#[pg_test]
fn v1122_minmax_bulk_partitioned_target_keeps_algebraic_path() {
    let p = "mmb_part";
    mmp_source(p, &["1", "2", "3"], "INT", "0", 3);
    mmb_create(&format!(
        "'{p}_v', 'SELECT plan, yr, {MMP_DATES} FROM {p}_src GROUP BY plan, yr', \
         NULL, NULL, 'DEFERRED', NULL, ARRAY['plan']"
    ));
    let partitioned = Spi::get_one::<bool>(&format!(
        "SELECT relkind = 'p' FROM pg_class WHERE oid = '{p}_v'::regclass"
    ))
    .expect("relkind")
    .expect("relkind NULL");
    assert!(partitioned, "the fixture's target must be partitioned");
    let replaced = mmb_apply(p, &format!("UPDATE {p}_src SET d = d + 1, k = k + 100 WHERE plan = 2"));
    mmb_check(p, &mmb_dates_fresh(p));
    assert!(!replaced, "a partitioned target keeps the algebraic path");
}

#[pg_test]
fn v1122_minmax_bulk_ignored_source_keeps_algebraic_path() {
    let p = "mmb_ign";
    mmp_source(p, &["1", "2", "3"], "INT", "0", 3);
    Spi::run(&format!(
        "CREATE TABLE {p}_dim (plan INT PRIMARY KEY, region TEXT NOT NULL); \
         INSERT INTO {p}_dim VALUES (1, 'north'), (2, 'south'), (3, 'south')"
    ))
    .expect("dimension");
    mmb_create(&format!(
        "'{p}_v', 'SELECT s.plan, r.region, MIN(s.d) AS lo, MAX(s.d) AS hi FROM {p}_src s \
          JOIN {p}_dim r ON r.plan = s.plan GROUP BY s.plan, r.region', \
         NULL, NULL, 'DEFERRED', '!{p}_dim', ARRAY[]::text[]"
    ));
    let ignored = Spi::get_one::<i32>(&format!(
        "SELECT cardinality(ignored_sources) FROM public.__reflex_ivm_reference WHERE name = '{p}_v'"
    ))
    .expect("ignored")
    .unwrap_or(0);
    assert!(ignored > 0, "the fixture must ignore a source");
    let replaced = mmb_apply(p, &format!("UPDATE {p}_src SET d = d + 1, k = k + 100 WHERE plan = 2"));
    mmb_check(
        p,
        &format!(
            "SELECT s.plan, r.region, MIN(s.d), MAX(s.d) FROM {p}_src s JOIN {p}_dim r ON r.plan = s.plan \
             GROUP BY s.plan, r.region"
        ),
    );
    assert!(!replaced, "an IMV ignoring a source keeps the algebraic path");
}

#[pg_test]
fn v1122_minmax_bulk_without_topk_keeps_algebraic_path() {
    let p = "mmb_notopk";
    mmp_source(p, &["1", "2", "3"], "INT", "0", 3);
    mmb_create(&format!(
        "'{p}_v', 'SELECT plan, yr, {MMP_DATES} FROM {p}_src GROUP BY plan, yr', \
         NULL, NULL, 'DEFERRED', 0, NULL, ARRAY[]::text[]"
    ));
    let heaps = Spi::get_one::<i64>(&format!(
        "SELECT count(*) FROM pg_attribute WHERE attrelid = '__reflex_intermediate_{p}_v'::regclass \
           AND attname LIKE '%\\_topk'"
    ))
    .expect("heap columns")
    .expect("heap columns NULL");
    assert_eq!(heaps, 0, "the fixture must have no top-K heap");
    let replaced = mmb_apply(p, &format!("UPDATE {p}_src SET d = d + 1, k = k + 100 WHERE plan = 2"));
    mmb_check(p, &mmb_dates_fresh(p));
    assert!(!replaced, "MIN/MAX without top-K keeps the algebraic path");
}

/// Several statements before the flush: plan 2 moved twice, rows added to the
/// existing groups of plan 4, part of plan 5 deleted.
#[pg_test]
fn v1122_minmax_bulk_several_statements_one_flush() {
    let p = "mmb_multi";
    mmb_dates_imv(p, 5, 3);
    let move_plan_2 = format!("UPDATE {p}_src SET d = d + 1, k = k + 100 WHERE plan = 2");
    let replaced = mmb_apply_all(
        p,
        &[
            move_plan_2.clone(),
            move_plan_2,
            format!(
                "INSERT INTO {p}_src (plan, d, yr, k, key_column, v) \
                 SELECT plan, d, yr, k + 1000, key_column, v FROM {p}_src WHERE plan = 4 AND k = 1"
            ),
            format!("DELETE FROM {p}_src WHERE plan = 5 AND k = 3"),
        ],
        &[&format!("{p}_src")],
    );
    mmb_check(p, &mmb_dates_fresh(p));
    assert!(replaced, "the staged rows of the batch reach the ratio");
}

/// A subtransaction rolled back between two statements stages nothing.
#[pg_test]
fn v1122_minmax_bulk_rolled_back_subtransaction() {
    let p = "mmb_sub";
    mmb_dates_imv(p, 4, 3);
    let replaced = mmb_apply_all(
        p,
        &[
            format!("UPDATE {p}_src SET d = d + 1, k = k + 100 WHERE plan = 2"),
            format!(
                "DO $$ BEGIN \
                   UPDATE {p}_src SET d = d + 1, k = k + 100 WHERE plan = 3; \
                   DELETE FROM {p}_src WHERE plan = 1; \
                   INSERT INTO {p}_src (plan, d, yr, k, key_column, v) VALUES (4, '2027-12-01', 2099, 1, 0, 1); \
                   RAISE EXCEPTION 'roll back'; \
                 EXCEPTION WHEN raise_exception THEN NULL; END $$"
            ),
            format!("UPDATE {p}_src SET d = d + 2, k = k + 100 WHERE plan = 2 AND k > 101"),
        ],
        &[&format!("{p}_src")],
    );
    mmb_check(p, &mmb_dates_fresh(p));
    assert!(replaced, "the surviving staged rows reach the ratio");
    let untouched = Spi::get_one::<i64>(&format!(
        "SELECT count(*) FROM {p}_v WHERE plan IN (1, 3) OR yr = 2099"
    ))
    .expect("rows")
    .expect("rows NULL");
    assert_eq!(untouched, 4, "plans 1 and 3 keep their groups and no 2099 group exists");
}

/// A join: the group keys come from both tables. A change of either source
/// alone is replaced; both changed in one transaction take the cross-source
/// rebuild.
#[pg_test]
fn v1122_minmax_bulk_join_sources() {
    let p = "mmb_join";
    mmp_source(p, &["1", "2", "3", "4"], "INT", "0", 3);
    Spi::run(&format!(
        "CREATE TABLE {p}_dim (plan INT PRIMARY KEY, region TEXT NOT NULL); \
         INSERT INTO {p}_dim VALUES (1, 'north'), (2, 'south'), (3, 'south'), (4, 'east')"
    ))
    .expect("dimension");
    mmb_create(&format!(
        "'{p}_v', 'SELECT r.region, s.yr, MIN(s.d) AS lo, MAX(s.d) AS hi, COUNT(*) AS n FROM {p}_src s \
          JOIN {p}_dim r ON r.plan = s.plan GROUP BY r.region, s.yr', \
         NULL, NULL, 'DEFERRED', NULL, ARRAY[]::text[]"
    ));
    let fresh = format!(
        "SELECT r.region, s.yr, MIN(s.d), MAX(s.d), COUNT(*) FROM {p}_src s \
         JOIN {p}_dim r ON r.plan = s.plan GROUP BY r.region, s.yr"
    );
    let src = format!("{p}_src");
    let dim = format!("{p}_dim");
    let check = |replaced: bool, expected: bool, what: &str| {
        mmb_check(p, &fresh);
        assert_eq!(replaced, expected, "{what}: replaced");
    };

    let whole_plan = mmb_apply_all(
        p,
        &[format!("UPDATE {src} SET d = d + 1, k = k + 100 WHERE plan = 1")],
        &[&src],
    );
    check(whole_plan, true, "a whole-plan UPDATE of the fact table");

    let regroup = mmb_apply_all(
        p,
        &[format!("UPDATE {dim} SET region = 'west' WHERE plan = 4")],
        &[&dim],
    );
    check(regroup, true, "a dimension row moving a plan to a new region");

    mmb_apply_all(
        p,
        &[
            format!("UPDATE {dim} SET region = 'north' WHERE plan = 3"),
            format!("UPDATE {src} SET d = d + 1, k = k + 100 WHERE plan = 2"),
        ],
        &[&dim, &src],
    );
    mmb_check(p, &fresh);
}

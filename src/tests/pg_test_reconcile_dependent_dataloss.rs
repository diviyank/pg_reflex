// A full `reflex_reconcile` of a PARTITIONED IMV destroyed its dependents.
//
// The partitioned rebuild path replaces each mirror leaf with an atomic
// DETACH/ATTACH swap. Every one of those `ALTER TABLE` statements fires the
// `ddl_command_end` event trigger, whose auto-sync branch re-mirrors every
// partitioned IMV that lists the swapped IMV as a source -- against the
// parent's TRANSIENT, mid-swap child set. The dependent therefore grew a
// mirror of `<parent>___reflex_swap_tgt_*` and dropped its real child as a
// bound-collision orphan; when the swap renamed the parent's child back,
// nothing revisited the dependent. On top of that the partitioned branch
// returned without any dependent cascade at all, so even an intact dependent
// was left stale (DETACH/ATTACH moves no rows, so no data trigger fires).
//
// Fixtures are real IMVs over real partitioned sources; correctness is the
// bidirectional EXCEPT ALL oracle against a query computed from the BASE
// table, never against the parent IMV (which would hide a shared error).

/// Names of the direct partition children of a relation, sorted.
fn partition_child_names(parent: &str) -> Vec<String> {
    Spi::connect(|client| {
        client
            .select(
                &format!(
                    "SELECT c.relname::text AS n FROM pg_inherits i \
                     JOIN pg_class c ON c.oid = i.inhrelid \
                     JOIN pg_class p ON p.oid = i.inhparent \
                     WHERE p.relname = '{}' ORDER BY 1",
                    parent
                ),
                None,
                &[],
            )
            .unwrap()
            .filter_map(|r| r.get_by_name::<&str, _>("n").ok().flatten().map(String::from))
            .collect::<Vec<_>>()
    })
}

fn dep_row_count(view: &str) -> i64 {
    Spi::get_one::<i64>(&format!("SELECT count(*)::int8 FROM {}", view))
        .expect("count query")
        .expect("count NULL")
}

/// Every partition child of `view` must be a real mirror name -- never a
/// mirror of one of the parent's transient `__reflex_swap_*` relations.
fn assert_no_swap_residue_children(view: &str) {
    let children = partition_child_names(view);
    let residue: Vec<&String> = children
        .iter()
        .filter(|n| n.contains("__reflex_swap"))
        .collect();
    assert!(
        residue.is_empty(),
        "IMV '{}' mirrors transient swap tables: {:?} (full child set {:?})",
        view,
        residue,
        children
    );
}

/// Build a LIST(k)-partitioned base table with three branches, seeded.
fn build_rdd_source(src: &str) {
    Spi::run(&format!(
        "CREATE TABLE {src} (k TEXT NOT NULL, bucket INT NOT NULL, amt NUMERIC) \
         PARTITION BY LIST (k)"
    ))
    .expect("root");
    for (br, val) in [("a", "A"), ("b", "B"), ("c", "C")] {
        Spi::run(&format!(
            "CREATE TABLE {src}_{br} PARTITION OF {src} FOR VALUES IN ('{val}')"
        ))
        .expect("branch");
    }
    Spi::run(&format!(
        "INSERT INTO {src} (k, bucket, amt) \
         SELECT v.k, (g % 5), (g % 97)::numeric \
         FROM generate_series(1, 300) g CROSS JOIN (VALUES ('A'),('B'),('C')) v(k)"
    ))
    .expect("seed");
    Spi::run(&format!("ANALYZE {src}")).expect("analyze");
}

fn create_imv(name: &str, sql: &str) {
    let r = Spi::get_one::<String>(sql)
        .expect("create call")
        .expect("create result");
    assert!(!r.starts_with("ERROR"), "create of '{name}' returned: {r}");
}

/// T1 -- the end-to-end reproduction. A partitioned parent with an
/// auto-partitioned dependent: after `reflex_reconcile(parent)` the dependent
/// must be complete and correct. This is the assertion that had to catch
/// 716 661 rows -> 0.
///
/// The parent is deliberately DRIFTED first (with triggers live, so the
/// dependent follows it) -- that way the reconcile has real work to undo on
/// both relations, and a fix that merely stops the destruction while leaving
/// the dependent stale still fails here.
#[pg_test]
fn pg_rdd_reconcile_partitioned_parent_keeps_dependent_correct() {
    build_rdd_source("rdd1s");
    create_imv(
        "rdd1p",
        "SELECT create_reflex_ivm('rdd1p', \
         'SELECT k, bucket, SUM(amt) AS total FROM rdd1s GROUP BY k, bucket', \
         NULL, NULL, NULL, NULL, ARRAY['k'])",
    );
    create_imv(
        "rdd1d",
        "SELECT create_reflex_ivm('rdd1d', 'SELECT k, SUM(total) AS t FROM rdd1p GROUP BY k')",
    );

    let parent_fresh = "SELECT k, bucket, SUM(amt) AS total FROM rdd1s GROUP BY k, bucket";
    let dep_fresh = "SELECT k, SUM(total) AS t FROM \
                     (SELECT k, bucket, SUM(amt) AS total FROM rdd1s GROUP BY k, bucket) o \
                     GROUP BY k";
    assert_imv_correct("rdd1p", parent_fresh);
    assert_imv_correct("rdd1d", dep_fresh);
    let dep_rows = dep_row_count("rdd1d");
    assert!(dep_rows > 0, "fixture produced an empty dependent");

    // Drift the parent with its triggers enabled so the dependent follows.
    Spi::run("UPDATE rdd1p SET total = total + 1").expect("drift");
    assert_ne!(
        Spi::get_one::<i64>("SELECT count(*)::int8 FROM (SELECT * FROM rdd1d EXCEPT ALL SELECT * FROM (SELECT k, SUM(total) AS t FROM (SELECT k, bucket, SUM(amt) AS total FROM rdd1s GROUP BY k, bucket) o GROUP BY k) f) x")
            .unwrap()
            .unwrap(),
        0,
        "the drift did not reach the dependent — fixture does not exercise the path"
    );

    let res = Spi::get_one::<&str>("SELECT reflex_reconcile('rdd1p')")
        .expect("reconcile")
        .expect("reconcile result");
    assert_eq!(res, "RECONCILED");

    assert_imv_correct("rdd1p", parent_fresh);
    assert_eq!(
        dep_row_count("rdd1d"),
        dep_rows,
        "reconcile of the partitioned parent changed the dependent's row count"
    );
    assert_imv_correct("rdd1d", dep_fresh);
}

/// T2 -- the structural half. The dependent's partition set must never
/// contain a mirror of a transient swap table, and its real children must all
/// survive the parent's reconcile.
#[pg_test]
fn pg_rdd_reconcile_leaves_no_swap_residue_in_dependent_mirror() {
    build_rdd_source("rdd2s");
    create_imv(
        "rdd2p",
        "SELECT create_reflex_ivm('rdd2p', \
         'SELECT k, bucket, SUM(amt) AS total FROM rdd2s GROUP BY k, bucket', \
         NULL, NULL, NULL, NULL, ARRAY['k'])",
    );
    create_imv(
        "rdd2d",
        "SELECT create_reflex_ivm('rdd2d', 'SELECT k, SUM(total) AS t FROM rdd2p GROUP BY k')",
    );

    let before = partition_child_names("rdd2d");
    assert_eq!(
        before.len(),
        3,
        "dependent was not auto-partitioned into 3 children: {before:?}"
    );
    assert_no_swap_residue_children("rdd2d");
    let int_before = partition_child_names("__reflex_intermediate_rdd2d");

    let res = Spi::get_one::<&str>("SELECT reflex_reconcile('rdd2p')")
        .expect("reconcile")
        .expect("reconcile result");
    assert_eq!(res, "RECONCILED");

    assert_no_swap_residue_children("rdd2d");
    assert_no_swap_residue_children("__reflex_intermediate_rdd2d");
    assert_eq!(
        partition_child_names("rdd2d"),
        before,
        "the parent's reconcile changed the dependent's partition set"
    );
    assert_eq!(
        partition_child_names("__reflex_intermediate_rdd2d"),
        int_before,
        "the parent's reconcile changed the dependent's intermediate partition set"
    );
    assert_imv_correct(
        "rdd2d",
        "SELECT k, SUM(total) AS t FROM \
         (SELECT k, bucket, SUM(amt) AS total FROM rdd2s GROUP BY k, bucket) o GROUP BY k",
    );
}

/// T3 -- a two-level dependent chain A -> B -> C. Reconciling A must leave
/// both B and C correct and residue-free.
#[pg_test]
fn pg_rdd_reconcile_propagates_two_levels_deep() {
    build_rdd_source("rdd3s");
    create_imv(
        "rdd3a",
        "SELECT create_reflex_ivm('rdd3a', \
         'SELECT k, bucket, SUM(amt) AS total FROM rdd3s GROUP BY k, bucket', \
         NULL, NULL, NULL, NULL, ARRAY['k'])",
    );
    create_imv(
        "rdd3b",
        "SELECT create_reflex_ivm('rdd3b', 'SELECT k, SUM(total) AS t FROM rdd3a GROUP BY k')",
    );
    create_imv(
        "rdd3c",
        "SELECT create_reflex_ivm('rdd3c', 'SELECT k, MAX(t) AS m FROM rdd3b GROUP BY k')",
    );

    let b_fresh = "SELECT k, SUM(total) AS t FROM \
                   (SELECT k, bucket, SUM(amt) AS total FROM rdd3s GROUP BY k, bucket) o GROUP BY k";
    let c_fresh = "SELECT k, MAX(t) AS m FROM \
                   (SELECT k, SUM(total) AS t FROM \
                    (SELECT k, bucket, SUM(amt) AS total FROM rdd3s GROUP BY k, bucket) o \
                    GROUP BY k) b GROUP BY k";
    assert_imv_correct("rdd3b", b_fresh);
    assert_imv_correct("rdd3c", c_fresh);
    let b_rows = dep_row_count("rdd3b");
    let c_rows = dep_row_count("rdd3c");

    Spi::run("UPDATE rdd3a SET total = total + 1").expect("drift");

    let res = Spi::get_one::<&str>("SELECT reflex_reconcile('rdd3a')")
        .expect("reconcile")
        .expect("reconcile result");
    assert_eq!(res, "RECONCILED");

    assert_no_swap_residue_children("rdd3b");
    assert_eq!(dep_row_count("rdd3b"), b_rows, "level-1 dependent lost rows");
    assert_eq!(dep_row_count("rdd3c"), c_rows, "level-2 dependent lost rows");
    assert_imv_correct("rdd3b", b_fresh);
    assert_imv_correct("rdd3c", c_fresh);
}

/// T5 -- the swap primitive on its own, with NO cascade behind it to clean up
/// after it.
///
/// T1-T3 exercise `reflex_reconcile`, whose dependent cascade repairs a mirror
/// the swap corrupted, so they cannot tell "the swap never corrupts it" from
/// "the corruption is repaired afterwards". `crate_test_partition_swap_for_child`
/// drives one DETACH/ATTACH swap directly, so what the dependent looks like
/// afterwards is exactly what the event-trigger guard did or did not prevent.
///
/// This matters beyond tidiness: a cascade that FAILS (lock timeout, a broken
/// source, a refused repair) leaves an unguarded dependent EMPTY with a mirror
/// of a relation that no longer exists, instead of merely stale.
#[pg_test]
fn pg_rdd_bare_swap_does_not_touch_the_dependent_mirror() {
    build_rdd_source("rdd6s");
    create_imv(
        "rdd6p",
        "SELECT create_reflex_ivm('rdd6p', \
         'SELECT k, bucket, SUM(amt) AS total FROM rdd6s GROUP BY k, bucket', \
         NULL, NULL, NULL, NULL, ARRAY['k'])",
    );
    create_imv(
        "rdd6d",
        "SELECT create_reflex_ivm('rdd6d', 'SELECT k, SUM(total) AS t FROM rdd6p GROUP BY k')",
    );

    let dep_children = partition_child_names("rdd6d");
    let dep_int_children = partition_child_names("__reflex_intermediate_rdd6d");
    let dep_rows = dep_row_count("rdd6d");
    assert_eq!(dep_children.len(), 3, "fixture: {dep_children:?}");

    let swap = Spi::get_one::<String>(
        "SELECT tests.crate_test_partition_swap_for_child('rdd6p', 'rdd6s_c')",
    )
    .expect("swap call")
    .expect("swap result");
    assert_eq!(swap, "OK", "the swap primitive itself failed");

    assert_no_swap_residue_children("rdd6d");
    assert_no_swap_residue_children("__reflex_intermediate_rdd6d");
    assert_eq!(
        partition_child_names("rdd6d"),
        dep_children,
        "a bare swap of the parent rewrote the dependent's partition set"
    );
    assert_eq!(
        partition_child_names("__reflex_intermediate_rdd6d"),
        dep_int_children,
        "a bare swap of the parent rewrote the dependent's intermediate partition set"
    );
    assert_eq!(
        dep_row_count("rdd6d"),
        dep_rows,
        "a bare swap of the parent emptied the dependent"
    );
}

/// T4a -- an UNPARTITIONED dependent of a PARTITIONED parent. It cannot be
/// destroyed (nothing mirrors the swap tables into it), but the swap moves no
/// rows, so without an explicit cascade it is left silently STALE. Measured
/// RED at 10 oracle mismatches before the fix.
#[pg_test]
fn pg_rdd_unpartitioned_dependent_of_partitioned_parent_follows() {
    build_rdd_source("rdd4s");
    create_imv(
        "rdd4p",
        "SELECT create_reflex_ivm('rdd4p', \
         'SELECT k, bucket, SUM(amt) AS total FROM rdd4s GROUP BY k, bucket', \
         NULL, NULL, NULL, NULL, ARRAY['k'])",
    );
    // Groups by `bucket`, not the parent's partition column, so it is not
    // auto-partitioned.
    create_imv(
        "rdd4d",
        "SELECT create_reflex_ivm('rdd4d', 'SELECT bucket, SUM(total) AS t FROM rdd4p GROUP BY bucket')",
    );
    assert_eq!(
        partition_child_names("rdd4d").len(),
        0,
        "dependent was unexpectedly partitioned — fixture does not cover this shape"
    );

    let d_fresh = "SELECT bucket, SUM(total) AS t FROM \
                   (SELECT k, bucket, SUM(amt) AS total FROM rdd4s GROUP BY k, bucket) o \
                   GROUP BY bucket";
    assert_imv_correct("rdd4d", d_fresh);

    Spi::run("UPDATE rdd4p SET total = total + 1").expect("drift");
    let res = Spi::get_one::<&str>("SELECT reflex_reconcile('rdd4p')")
        .expect("reconcile")
        .expect("res");
    assert_eq!(res, "RECONCILED");

    assert_imv_correct("rdd4d", d_fresh);
}

/// T4b -- the genuine regression control: an UNPARTITIONED parent, whose
/// dependent propagates through the `AFTER TRUNCATE ... FOR EACH STATEMENT`
/// trigger plus the refill INSERT. Measured working before the fix; must not
/// regress.
#[pg_test]
fn pg_rdd_unpartitioned_parent_still_propagates() {
    Spi::run("CREATE TABLE rdd5u (k TEXT NOT NULL, bucket INT NOT NULL, amt NUMERIC)")
        .expect("plain source");
    Spi::run(
        "INSERT INTO rdd5u (k, bucket, amt) \
         SELECT v.k, (g % 5), (g % 97)::numeric \
         FROM generate_series(1, 300) g CROSS JOIN (VALUES ('A'),('B'),('C')) v(k)",
    )
    .expect("seed plain");
    Spi::run("ANALYZE rdd5u").expect("analyze plain");
    create_imv(
        "rdd5p",
        "SELECT create_reflex_ivm('rdd5p', \
         'SELECT k, bucket, SUM(amt) AS total FROM rdd5u GROUP BY k, bucket')",
    );
    create_imv(
        "rdd5d",
        "SELECT create_reflex_ivm('rdd5d', 'SELECT k, SUM(total) AS t FROM rdd5p GROUP BY k')",
    );

    let d_fresh = "SELECT k, SUM(total) AS t FROM \
                   (SELECT k, bucket, SUM(amt) AS total FROM rdd5u GROUP BY k, bucket) o \
                   GROUP BY k";
    assert_imv_correct("rdd5d", d_fresh);
    let rows = dep_row_count("rdd5d");

    Spi::run("UPDATE rdd5p SET total = total + 1").expect("drift");
    let res = Spi::get_one::<&str>("SELECT reflex_reconcile('rdd5p')")
        .expect("reconcile")
        .expect("res");
    assert_eq!(res, "RECONCILED");

    assert_eq!(dep_row_count("rdd5d"), rows, "unpartitioned control lost rows");
    assert_imv_correct("rdd5d", d_fresh);
}

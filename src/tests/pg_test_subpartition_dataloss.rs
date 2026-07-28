// Silent data loss on depth->=2 mirrors.
//
// A full `reflex_reconcile` of a partitioned IMV used to DETACH each top-level
// mirror child and ATTACH a `CREATE TABLE ... (LIKE old INCLUDING ALL)` copy in
// its place. `LIKE` never copies partitioning -- PostgreSQL has no
// `INCLUDING PARTITIONING` -- so when the mirror child was itself PARTITIONED
// (every top-level child of a depth->=2 mirror) the replacement was a plain
// table and the old child was dropped with its whole subtree. Data was still
// correct at that point, which is why it went unnoticed. The NEXT partition
// sync saw the shape drift, dropped those children and recreated them EMPTY,
// taking the IMV to zero rows with NOTICE-level output only.
//
// Fixtures here are real depth-2 IMVs over real LIST -> RANGE sources; the
// oracle is the bidirectional EXCEPT ALL check, never a row count alone.
//
// `imv_child_count` / `is_partitioned_rel` come from pg_test_subpartition.rs,
// which is included into the same test module.

/// Number of grandchildren (depth-2 mirror nodes) under an IMV root.
fn imv_grandchild_count(view: &str) -> i64 {
    Spi::get_one::<i64>(&format!(
        "SELECT count(*)::int8 FROM pg_class c \
         JOIN pg_inherits i  ON i.inhrelid  = c.oid \
         JOIN pg_inherits i2 ON i2.inhrelid = i.inhparent \
         JOIN pg_class root  ON root.oid    = i2.inhparent \
         WHERE root.relname = '{}'",
        view
    ))
    .unwrap()
    .unwrap()
}

/// Number of immediate children of an IMV root that are themselves partitioned.
fn imv_partitioned_child_count(view: &str) -> i64 {
    Spi::get_one::<i64>(&format!(
        "SELECT count(*)::int8 FROM pg_inherits i \
         JOIN pg_class c    ON c.oid = i.inhrelid \
         JOIN pg_class root ON root.oid = i.inhparent \
         WHERE root.relname = '{}' AND c.relkind = 'p'",
        view
    ))
    .unwrap()
    .unwrap()
}

fn imv_row_count(view: &str) -> i64 {
    Spi::get_one::<i64>(&format!("SELECT count(*)::int8 FROM {}", view))
        .expect("count query")
        .expect("count NULL")
}

/// Build a LIST(k) -> RANGE(d) source with two branches x two months, seeded
/// and ANALYZEd, plus a depth-2 passthrough IMV over it.
fn build_depth2_fixture(src: &str, view: &str) {
    Spi::run(&format!(
        "CREATE TABLE {src} (k TEXT NOT NULL, d DATE NOT NULL, id BIGINT, amt NUMERIC) \
         PARTITION BY LIST (k)"
    ))
    .expect("root");
    for br in ["a", "b"] {
        Spi::run(&format!(
            "CREATE TABLE {src}_{br} PARTITION OF {src} FOR VALUES IN ('{up}') \
             PARTITION BY RANGE (d)",
            up = br.to_uppercase()
        ))
        .expect("branch");
        Spi::run(&format!(
            "CREATE TABLE {src}_{br}_m1 PARTITION OF {src}_{br} \
             FOR VALUES FROM ('2026-01-01') TO ('2026-02-01')"
        ))
        .expect("m1");
        Spi::run(&format!(
            "CREATE TABLE {src}_{br}_m2 PARTITION OF {src}_{br} \
             FOR VALUES FROM ('2026-02-01') TO ('2026-03-01')"
        ))
        .expect("m2");
    }
    Spi::run(&format!(
        "INSERT INTO {src} (k, d, id, amt) \
         SELECT v.k, DATE '2026-01-01' + (g % 55), g, (g % 97)::numeric \
         FROM generate_series(1, 200) g CROSS JOIN (VALUES ('A'),('B')) v(k)"
    ))
    .expect("seed");
    Spi::run(&format!("ANALYZE {src}")).expect("analyze");
    let r = Spi::get_one::<String>(&format!(
        "SELECT create_reflex_ivm('{view}', 'SELECT k, d, id, amt FROM {src}', \
         'k,d,id', NULL, NULL, NULL, ARRAY['k','d'])"
    ))
    .expect("create call")
    .expect("create result");
    assert!(!r.starts_with("ERROR"), "create returned: {r}");
}

/// T1 -- the end-to-end reproduction. A depth-2 IMV that has been reconciled
/// must survive a subsequent partition sync with its contents intact. This is
/// the assertion that had to catch 2 800 000 rows -> 0.
#[pg_test]
fn pg_subpart_reconcile_then_sync_keeps_depth2_imv_data() {
    build_depth2_fixture("dl1s", "dl1v");
    let fresh = "SELECT k, d, id, amt FROM dl1s";
    let seeded = imv_row_count("dl1v");
    assert!(seeded > 0, "fixture seeded no rows");
    assert_imv_correct("dl1v", fresh);

    let res = Spi::get_one::<&str>("SELECT reflex_reconcile('dl1v')")
        .expect("rec")
        .expect("res");
    assert_eq!(res, "RECONCILED");
    assert_eq!(
        imv_row_count("dl1v"),
        seeded,
        "reconcile itself changed the row count"
    );
    assert_imv_correct("dl1v", fresh);

    let sync = Spi::get_one::<String>("SELECT reflex_sync_partitions('dl1v', true)")
        .expect("sync")
        .expect("sync res");
    assert!(!sync.starts_with("ERROR"), "sync returned: {sync}");
    assert_eq!(
        imv_row_count("dl1v"),
        seeded,
        "the partition sync following a reconcile emptied the IMV"
    );
    assert_imv_correct("dl1v", fresh);
}

/// T1b -- the realistic trigger: no `reflex_*` call at all after the reconcile,
/// just ordinary source DDL, which the `ddl_command_end` event trigger turns
/// into a partition sync.
///
/// Two shapes, because they reach the sync by different routes. Adding next
/// month's leaf names a SUB-level parent, which no IMV lists in `depends_on`;
/// that only enqueues the root, and the sync happens in the COMMIT-time flush
/// (drained here with `SET CONSTRAINTS ALL IMMEDIATE`). Adding a top-level
/// branch names the root itself and syncs INLINE — with `drop_orphans => FALSE`,
/// which is what pins that the flag does not protect the IMV.
#[pg_test]
fn pg_subpart_reconcile_then_source_ddl_keeps_depth2_imv_data() {
    build_depth2_fixture("dl2s", "dl2v");
    let fresh = "SELECT k, d, id, amt FROM dl2s";
    let seeded = imv_row_count("dl2v");
    assert!(seeded > 0, "fixture seeded no rows");

    let res = Spi::get_one::<&str>("SELECT reflex_reconcile('dl2v')")
        .expect("rec")
        .expect("res");
    assert_eq!(res, "RECONCILED");

    Spi::run(
        "CREATE TABLE dl2s_a_m3 PARTITION OF dl2s_a \
         FOR VALUES FROM ('2026-03-01') TO ('2026-04-01')",
    )
    .expect("new month");
    Spi::run("SET CONSTRAINTS ALL IMMEDIATE").expect("drain deferred flush");
    assert_eq!(
        imv_row_count("dl2v"),
        seeded,
        "adding next month's partition to the source emptied the IMV"
    );
    assert_imv_correct("dl2v", fresh);

    Spi::run(
        "CREATE TABLE dl2s_c PARTITION OF dl2s FOR VALUES IN ('C') PARTITION BY RANGE (d)",
    )
    .expect("new branch");
    assert_eq!(
        imv_row_count("dl2v"),
        seeded,
        "adding a top-level branch to the source emptied the IMV \
         (the inline auto-sync runs with drop_orphans => FALSE)"
    );
    assert_imv_correct("dl2v", fresh);
}

/// T2 -- the flattening itself. After a full reconcile the mirror must still be
/// depth 2: every top-level child partitioned, subtree intact.
#[pg_test]
fn pg_subpart_reconcile_keeps_depth2_mirror_shape() {
    build_depth2_fixture("dl3s", "dl3v");
    let partitioned_before = imv_partitioned_child_count("dl3v");
    let grandchildren_before = imv_grandchild_count("dl3v");
    assert_eq!(partitioned_before, 2, "fixture is not a depth-2 mirror");
    assert_eq!(grandchildren_before, 4, "fixture has the wrong leaf count");

    let res = Spi::get_one::<&str>("SELECT reflex_reconcile('dl3v')")
        .expect("rec")
        .expect("res");
    assert_eq!(res, "RECONCILED");

    assert_eq!(
        imv_partitioned_child_count("dl3v"),
        partitioned_before,
        "reconcile flattened the mirror — a top-level child is no longer partitioned"
    );
    assert_eq!(
        imv_grandchild_count("dl3v"),
        grandchildren_before,
        "reconcile dropped the mirror's sub-partition subtree"
    );
    assert!(
        is_partitioned_rel("dl3v_dl3s_a"),
        "top-level mirror child dl3v_dl3s_a must still be partitioned"
    );
    assert!(
        is_partitioned_rel("dl3v_dl3s_b"),
        "top-level mirror child dl3v_dl3s_b must still be partitioned"
    );
}

/// T3 -- the refusal. The per-child swap primitive cannot rebuild a PARTITIONED
/// mirror child (its `LIKE ... INCLUDING ALL` replacement would be a plain
/// table); it must say so and leave the mirror untouched, not flatten it.
#[pg_test]
fn pg_subpart_swap_refuses_partitioned_child_and_leaves_state_intact() {
    build_depth2_fixture("dl4s", "dl4v");
    let fresh = "SELECT k, d, id, amt FROM dl4s";
    let rows_before = imv_row_count("dl4v");
    let grandchildren_before = imv_grandchild_count("dl4v");

    // 'dl4s_a' is a source BRANCH, so the derived mirror child 'dl4v_dl4s_a'
    // is a partitioned relation — exactly the input the swap must refuse.
    let out =
        Spi::get_one::<String>("SELECT tests.crate_test_partition_swap_for_child('dl4v', 'dl4s_a')")
            .expect("swap call")
            .expect("swap result");
    assert!(
        out.starts_with("ERROR"),
        "swap of a partitioned mirror child must refuse loudly, got: {out}"
    );
    assert!(
        out.contains("dl4v_dl4s_a"),
        "the refusal must name the offending child, got: {out}"
    );
    assert!(
        out.contains("reflex_reconcile_partition"),
        "the refusal must name the primitive that can handle it, got: {out}"
    );

    assert!(
        is_partitioned_rel("dl4v_dl4s_a"),
        "the refused swap flattened the child anyway"
    );
    assert_eq!(
        imv_grandchild_count("dl4v"),
        grandchildren_before,
        "the refused swap dropped part of the mirror subtree"
    );
    assert_eq!(
        imv_row_count("dl4v"),
        rows_before,
        "the refused swap changed the IMV's contents"
    );
    assert_imv_correct("dl4v", fresh);
}

/// T4 -- depth-1 mirrors must be unaffected by the leaf-resolution change:
/// a full reconcile still rebuilds every child from the source, the children
/// stay plain relations, and the result is correct. Covered for both a depth-1
/// source and a depth-2 source mirrored shallowly (single-column partition_by).
#[pg_test]
fn pg_subpart_depth1_reconcile_unchanged_by_leaf_resolution() {
    Spi::run("CREATE TABLE dl5s (k TEXT NOT NULL, id BIGINT, amt NUMERIC) PARTITION BY LIST (k)")
        .expect("root");
    Spi::run("CREATE TABLE dl5s_a PARTITION OF dl5s FOR VALUES IN ('A')").expect("a");
    Spi::run("CREATE TABLE dl5s_b PARTITION OF dl5s FOR VALUES IN ('B')").expect("b");
    Spi::run(
        "INSERT INTO dl5s SELECT v.k, g, (g % 13)::numeric \
         FROM generate_series(1, 60) g CROSS JOIN (VALUES ('A'),('B')) v(k)",
    )
    .expect("seed");
    Spi::get_one::<String>(
        "SELECT create_reflex_ivm('dl5v', 'SELECT k, id, amt FROM dl5s', 'k,id', \
         NULL, NULL, NULL, ARRAY['k'])",
    )
    .expect("c")
    .expect("c");

    Spi::run("UPDATE dl5v SET amt = -1").expect("drift");
    let res = Spi::get_one::<&str>("SELECT reflex_reconcile('dl5v')")
        .expect("rec")
        .expect("res");
    assert_eq!(res, "RECONCILED");
    assert_imv_correct("dl5v", "SELECT k, id, amt FROM dl5s");
    assert_eq!(
        imv_partitioned_child_count("dl5v"),
        0,
        "a depth-1 mirror must have no partitioned children"
    );
    assert_eq!(imv_child_count("dl5v"), 2, "both depth-1 children present");

    // Depth-2 source mirrored at depth 1: the swap must still target the
    // top-level (leaf) mirror children, and the reconcile must still repair.
    Spi::run(
        "CREATE TABLE dl6s (k TEXT NOT NULL, d DATE NOT NULL, id BIGINT, amt NUMERIC) \
         PARTITION BY LIST (k)",
    )
    .expect("root2");
    Spi::run("CREATE TABLE dl6s_a PARTITION OF dl6s FOR VALUES IN ('A') PARTITION BY RANGE (d)")
        .expect("branch");
    Spi::run(
        "CREATE TABLE dl6s_a_m1 PARTITION OF dl6s_a \
         FOR VALUES FROM ('2026-01-01') TO ('2026-02-01')",
    )
    .expect("m1");
    Spi::run(
        "CREATE TABLE dl6s_a_m2 PARTITION OF dl6s_a \
         FOR VALUES FROM ('2026-02-01') TO ('2026-03-01')",
    )
    .expect("m2");
    Spi::run(
        "INSERT INTO dl6s SELECT 'A', DATE '2026-01-01' + (g % 55), g, (g % 13)::numeric \
         FROM generate_series(1, 60) g",
    )
    .expect("seed2");
    Spi::get_one::<String>(
        "SELECT create_reflex_ivm('dl6v', 'SELECT k, d, id, amt FROM dl6s', 'k,d,id', \
         NULL, NULL, NULL, ARRAY['k'])",
    )
    .expect("c")
    .expect("c");

    Spi::run("UPDATE dl6v SET amt = -1").expect("drift2");
    let res2 = Spi::get_one::<&str>("SELECT reflex_reconcile('dl6v')")
        .expect("rec")
        .expect("res");
    assert_eq!(res2, "RECONCILED");
    assert_imv_correct("dl6v", "SELECT k, d, id, amt FROM dl6s");
    assert_eq!(
        imv_partitioned_child_count("dl6v"),
        0,
        "a shallow (depth-1) mirror of a depth-2 source must have no partitioned children"
    );
}

/// The same defect on an AGGREGATE depth-2 IMV, which additionally carries a
/// partitioned `__reflex_intermediate_*` tree that the swap flattened in
/// lockstep with the target.
///
/// An aggregate reaches depth 2 only through the AUTO-mirror path: an explicit
/// multi-level `partition_by` is rejected for aggregates, because
/// `resolve_unique_columns` returns early when the plan is not passthrough, so
/// the level->=2 "must be in the unique key" check can never pass. The IMV below
/// therefore has `partition_columns = {k}` (length ONE) and
/// `partition_depth = 2` — which is why exposure is a function of
/// `partition_depth`, not of how many columns `partition_by` names.
#[pg_test]
fn pg_subpart_reconcile_then_sync_keeps_depth2_aggregate_imv_data() {
    Spi::run(
        "CREATE TABLE dl7s (k TEXT NOT NULL, d DATE NOT NULL, id BIGINT, amt NUMERIC) \
         PARTITION BY LIST (k)",
    )
    .expect("root");
    for br in ["a", "b"] {
        Spi::run(&format!(
            "CREATE TABLE dl7s_{br} PARTITION OF dl7s FOR VALUES IN ('{up}') \
             PARTITION BY RANGE (d)",
            up = br.to_uppercase()
        ))
        .expect("branch");
        Spi::run(&format!(
            "CREATE TABLE dl7s_{br}_m1 PARTITION OF dl7s_{br} \
             FOR VALUES FROM ('2026-01-01') TO ('2026-02-01')"
        ))
        .expect("m1");
        Spi::run(&format!(
            "CREATE TABLE dl7s_{br}_m2 PARTITION OF dl7s_{br} \
             FOR VALUES FROM ('2026-02-01') TO ('2026-03-01')"
        ))
        .expect("m2");
    }
    Spi::run(
        "INSERT INTO dl7s (k, d, id, amt) \
         SELECT v.k, DATE '2026-01-01' + (g % 55), g, (g % 97)::numeric \
         FROM generate_series(1, 200) g CROSS JOIN (VALUES ('A'),('B')) v(k)",
    )
    .expect("seed");
    Spi::run("ANALYZE dl7s").expect("analyze");
    let r = Spi::get_one::<String>(
        "SELECT create_reflex_ivm('dl7v', \
         'SELECT k, d, SUM(amt) AS total FROM dl7s GROUP BY k, d')",
    )
    .expect("c")
    .expect("c");
    assert!(!r.starts_with("ERROR"), "create returned: {r}");
    assert_eq!(
        Spi::get_one::<i32>(
            "SELECT partition_depth FROM public.__reflex_ivm_reference WHERE name = 'dl7v'"
        )
        .expect("q")
        .expect("partition_depth NULL"),
        2,
        "the auto-mirror did not reach depth 2 — fixture does not exercise the defect"
    );
    assert_eq!(
        Spi::get_one::<i64>(
            "SELECT array_length(partition_columns, 1)::int8 \
             FROM public.__reflex_ivm_reference WHERE name = 'dl7v'"
        )
        .expect("q")
        .expect("partition_columns NULL"),
        1,
        "a depth-2 mirror with a SINGLE partition column is the shape an exposure query \
         keyed off partition_columns would miss"
    );

    let fresh = "SELECT k, d, SUM(amt) AS total FROM dl7s GROUP BY k, d";
    let seeded = imv_row_count("dl7v");
    assert!(seeded > 0, "fixture seeded no groups");
    assert_imv_correct("dl7v", fresh);

    let res = Spi::get_one::<&str>("SELECT reflex_reconcile('dl7v')")
        .expect("rec")
        .expect("res");
    assert_eq!(res, "RECONCILED");
    assert_eq!(
        imv_partitioned_child_count("dl7v"),
        2,
        "reconcile flattened the aggregate IMV's target mirror"
    );
    assert_eq!(
        imv_partitioned_child_count("__reflex_intermediate_dl7v"),
        2,
        "reconcile flattened the aggregate IMV's intermediate mirror"
    );

    let sync = Spi::get_one::<String>("SELECT reflex_sync_partitions('dl7v', true)")
        .expect("sync")
        .expect("sync res");
    assert!(!sync.starts_with("ERROR"), "sync returned: {sync}");
    assert_eq!(
        imv_row_count("dl7v"),
        seeded,
        "the partition sync following a reconcile emptied the aggregate IMV"
    );
    assert_imv_correct("dl7v", fresh);
}

/// The recovery path for an IMV already flattened in the field.
///
/// Preventing future flattening does not repair a mirror that has already lost
/// a level: it holds correct data but the wrong shape, and the next partition
/// sync will empty it. This test builds that exact state by replaying, in SQL,
/// the statement sequence the old swap executed, then pins two things
/// operators need to be told:
///
///   * the flattened IMV's data IS still correct (so nothing is lost yet), and
///   * a single `reflex_reconcile` on a fixed build restores the depth-2 shape
///     AND the data -- so recovery does not require dropping and recreating the
///     IMV, and must NOT be attempted with a bare `reflex_sync_partitions`,
///     which heals the shape by recreating the children empty.
#[pg_test]
fn pg_subpart_reconcile_repairs_an_already_flattened_mirror() {
    build_depth2_fixture("dl8s", "dl8v");
    let fresh = "SELECT k, d, id, amt FROM dl8s";
    let seeded = imv_row_count("dl8v");
    let grandchildren_before = imv_grandchild_count("dl8v");

    // Exactly what `execute_partition_swap_for_child` used to do to a
    // partitioned mirror child: build a LIKE copy (which carries no
    // partitioning), fill it, swap it in, drop the old subtree, rename.
    for br in ["a", "b"] {
        Spi::run(&format!(
            "CREATE UNLOGGED TABLE dl8v_swap_{br} (LIKE dl8v_dl8s_{br} INCLUDING ALL)"
        ))
        .expect("swap table");
        Spi::run(&format!(
            "INSERT INTO dl8v_swap_{br} SELECT * FROM dl8v_dl8s_{br}"
        ))
        .expect("fill swap");
        Spi::run(&format!(
            "ALTER TABLE dl8v DETACH PARTITION dl8v_dl8s_{br}"
        ))
        .expect("detach old");
        Spi::run(&format!(
            "ALTER TABLE dl8v ATTACH PARTITION dl8v_swap_{br} FOR VALUES IN ('{up}')",
            up = br.to_uppercase()
        ))
        .expect("attach swap");
        Spi::run(&format!("DROP TABLE dl8v_dl8s_{br}")).expect("drop old");
        Spi::run(&format!(
            "ALTER TABLE dl8v_swap_{br} RENAME TO dl8v_dl8s_{br}"
        ))
        .expect("rename");
    }

    // The flattened state: correct data, wrong shape.
    assert_eq!(
        imv_partitioned_child_count("dl8v"),
        0,
        "the fixture did not reach the flattened state"
    );
    assert_eq!(imv_grandchild_count("dl8v"), 0, "subtree should be gone");
    assert_eq!(
        imv_row_count("dl8v"),
        seeded,
        "flattening must not lose rows — it is the SYNC that empties, not the swap"
    );
    assert_imv_correct("dl8v", fresh);

    // reflex_audit DOES notice — the mirror's real leaves read as missing and
    // the flattened parents as extra — but the remedy it prints for
    // partition-tree-drift is `reflex_sync_partitions(..., TRUE)`, which is the
    // operation that empties a flattened IMV. Pinned so the hazard is a
    // measured fact operators can be warned about, not an inference from
    // reading `src/audit/checks_b_drift.rs`.
    let report: String = Spi::get_one("SELECT reflex_audit('dl8v')")
        .expect("audit")
        .expect("audit NULL");
    assert!(
        report.contains("partition-tree-drift"),
        "reflex_audit did not flag a flattened mirror at all: {report}"
    );
    assert!(
        report.contains("reflex_sync_partitions"),
        "audit remedy changed — recheck the operator warning in HANDOFF.md: {report}"
    );

    // The prescribed remedy.
    let res = Spi::get_one::<&str>("SELECT reflex_reconcile('dl8v')")
        .expect("rec")
        .expect("res");
    assert_eq!(res, "RECONCILED");
    assert_eq!(
        imv_partitioned_child_count("dl8v"),
        2,
        "reflex_reconcile did not restore the depth-2 mirror shape"
    );
    assert_eq!(
        imv_grandchild_count("dl8v"),
        grandchildren_before,
        "reflex_reconcile did not restore the mirror's leaves"
    );
    assert_eq!(
        imv_row_count("dl8v"),
        seeded,
        "reflex_reconcile did not refill the repaired mirror"
    );
    assert_imv_correct("dl8v", fresh);
}

// The two queries operators are given to find affected IMVs on an unfixed
// build. They are asserted here verbatim so what is published is what was
// measured, not an inference from the registry schema.

/// Resolve a registry name to its `regclass`, quoting each component. A bare
/// `to_regclass(r.name)` down-cases an unquoted mixed-case name and yields
/// NULL, which would make query 1 MISS an affected IMV and query 2 FALSELY
/// report one (its `NOT EXISTS` over an empty set is TRUE). The codebase
/// documents the same hazard at `src/reconcile.rs:1170-1175`.
const REGISTRY_REGCLASS: &str = "to_regclass(CASE WHEN r.name LIKE '%.%' \
          THEN quote_ident(split_part(r.name, '.', 1)) || '.' \
               || quote_ident(split_part(r.name, '.', 2)) \
          ELSE quote_ident(r.name) END)";

/// Mirrors deeper than one level: on an affected build a `reflex_reconcile`
/// flattens these, and the next partition sync then empties them.
fn exposed_shaped_query() -> String {
    format!(
        "SELECT DISTINCT r.name \
         FROM public.__reflex_ivm_reference r \
         JOIN pg_inherits i ON i.inhparent = {rc} \
         JOIN pg_class c ON c.oid = i.inhrelid \
         WHERE r.enabled AND c.relkind = 'p'",
        rc = REGISTRY_REGCLASS
    )
}

/// Partitioned mirrors that are FLAT but should not be: already flattened, or
/// of unrecorded depth. Data is still correct; the next sync empties them.
///
/// A NULL `partition_depth` means "mirror the full source depth", so such a row
/// cannot be excluded — an already-flattened legacy IMV has no partitioned
/// child either, and excluding it would make it invisible to BOTH queries,
/// which is exactly the dangerous population. It is reported as UNKNOWN.
fn exposed_flattened_query() -> String {
    format!(
        "SELECT r.name, r.partition_depth, \
                CASE WHEN r.partition_depth IS NULL \
                     THEN 'UNKNOWN — mirror depth unrecorded, inspect the source tree' \
                     ELSE 'ALREADY FLATTENED' END AS status \
         FROM public.__reflex_ivm_reference r \
         WHERE r.enabled \
           AND r.partition_columns IS NOT NULL \
           AND array_length(r.partition_columns, 1) > 0 \
           AND (r.partition_depth IS NULL OR r.partition_depth >= 2) \
           AND NOT EXISTS (SELECT 1 FROM pg_inherits i \
                           JOIN pg_class c ON c.oid = i.inhrelid \
                           WHERE i.inhparent = {rc} AND c.relkind = 'p')",
        rc = REGISTRY_REGCLASS
    )
}

fn query_matches(sql: &str, name: &str) -> bool {
    Spi::get_one::<bool>(&format!(
        "SELECT EXISTS(SELECT 1 FROM ({}) q WHERE q.name = '{}')",
        sql, name
    ))
    .expect("exposure query failed")
    .expect("exposure query returned NULL")
}

/// The published exposure criteria, checked in both directions against real
/// IMVs: a depth-2 mirror must be reported, a depth-1 mirror must NOT be, and
/// a flattened mirror must move from the first query to the second.
#[pg_test]
fn pg_subpart_exposure_detection_queries_are_accurate() {
    build_depth2_fixture("dl9s", "dl9v");

    // A depth-1 control that must never be reported by either query.
    Spi::run("CREATE TABLE dl9x (k TEXT NOT NULL, id BIGINT) PARTITION BY LIST (k)").expect("root");
    Spi::run("CREATE TABLE dl9x_a PARTITION OF dl9x FOR VALUES IN ('A')").expect("a");
    Spi::run("INSERT INTO dl9x SELECT 'A', g FROM generate_series(1, 20) g").expect("seed");
    Spi::get_one::<String>(
        "SELECT create_reflex_ivm('dl9xv', 'SELECT k, id FROM dl9x', 'k,id', \
         NULL, NULL, NULL, ARRAY['k'])",
    )
    .expect("c")
    .expect("c");

    assert!(
        query_matches(&exposed_shaped_query(), "dl9v"),
        "the depth-2 exposure query missed a depth-2 mirror"
    );
    assert!(
        !query_matches(&exposed_shaped_query(), "dl9xv"),
        "the depth-2 exposure query falsely reported a depth-1 mirror"
    );
    assert!(
        !query_matches(&exposed_flattened_query(), "dl9v"),
        "an intact depth-2 mirror must not be reported as already flattened"
    );
    assert!(
        !query_matches(&exposed_flattened_query(), "dl9xv"),
        "a depth-1 mirror must never be reported as already flattened"
    );

    // Flatten it the way the old swap did, then re-check both queries.
    for br in ["a", "b"] {
        Spi::run(&format!(
            "CREATE UNLOGGED TABLE dl9v_swap_{br} (LIKE dl9v_dl9s_{br} INCLUDING ALL)"
        ))
        .expect("swap table");
        Spi::run(&format!(
            "INSERT INTO dl9v_swap_{br} SELECT * FROM dl9v_dl9s_{br}"
        ))
        .expect("fill swap");
        Spi::run(&format!("ALTER TABLE dl9v DETACH PARTITION dl9v_dl9s_{br}")).expect("detach");
        Spi::run(&format!(
            "ALTER TABLE dl9v ATTACH PARTITION dl9v_swap_{br} FOR VALUES IN ('{up}')",
            up = br.to_uppercase()
        ))
        .expect("attach");
        Spi::run(&format!("DROP TABLE dl9v_dl9s_{br}")).expect("drop old");
        Spi::run(&format!(
            "ALTER TABLE dl9v_swap_{br} RENAME TO dl9v_dl9s_{br}"
        ))
        .expect("rename");
    }

    assert!(
        !query_matches(&exposed_shaped_query(), "dl9v"),
        "a flattened mirror has no partitioned children and must drop out of the first query"
    );
    assert!(
        query_matches(&exposed_flattened_query(), "dl9v"),
        "the already-flattened query missed a mirror that lost a level"
    );
    assert!(
        !query_matches(&exposed_flattened_query(), "dl9xv"),
        "the already-flattened query falsely reported a legitimately depth-1 mirror"
    );

    // And the remedy converges: after the repair it is neither flattened nor
    // reported as such.
    let res = Spi::get_one::<&str>("SELECT reflex_reconcile('dl9v')")
        .expect("rec")
        .expect("res");
    assert_eq!(res, "RECONCILED");
    assert!(
        !query_matches(&exposed_flattened_query(), "dl9v"),
        "reflex_reconcile did not clear the already-flattened finding"
    );
    assert!(
        query_matches(&exposed_shaped_query(), "dl9v"),
        "reflex_reconcile did not restore the depth-2 shape"
    );
    assert_imv_correct("dl9v", "SELECT k, d, id, amt FROM dl9s");

    // A legacy row: real IMV, real mirror, but `partition_depth` unrecorded —
    // what an install upgraded from before the column existed looks like. Only
    // the recorded depth is legacy here; the relations are the ones
    // `create_reflex_ivm` built. A NULL depth means "mirror the full source
    // depth", so once flattened such an IMV has no partitioned child and is
    // invisible to the first query; the second must still surface it.
    build_depth2_fixture("dlbs", "dlbv");
    Spi::run(
        "UPDATE public.__reflex_ivm_reference SET partition_depth = NULL WHERE name = 'dlbv'",
    )
    .expect("legacy row");
    assert!(
        query_matches(&exposed_shaped_query(), "dlbv"),
        "an intact legacy-row mirror is still shaped and must be reported by the first query"
    );
    assert!(
        !query_matches(&exposed_flattened_query(), "dlbv"),
        "an intact mirror must not be reported as flattened, legacy row or not"
    );

    for br in ["a", "b"] {
        Spi::run(&format!(
            "CREATE UNLOGGED TABLE dlbv_swap_{br} (LIKE dlbv_dlbs_{br} INCLUDING ALL)"
        ))
        .expect("swap table");
        Spi::run(&format!(
            "INSERT INTO dlbv_swap_{br} SELECT * FROM dlbv_dlbs_{br}"
        ))
        .expect("fill swap");
        Spi::run(&format!("ALTER TABLE dlbv DETACH PARTITION dlbv_dlbs_{br}")).expect("detach");
        Spi::run(&format!(
            "ALTER TABLE dlbv ATTACH PARTITION dlbv_swap_{br} FOR VALUES IN ('{up}')",
            up = br.to_uppercase()
        ))
        .expect("attach");
        Spi::run(&format!("DROP TABLE dlbv_dlbs_{br}")).expect("drop old");
        Spi::run(&format!(
            "ALTER TABLE dlbv_swap_{br} RENAME TO dlbv_dlbs_{br}"
        ))
        .expect("rename");
    }

    assert!(
        !query_matches(&exposed_shaped_query(), "dlbv"),
        "a flattened mirror has no partitioned child, legacy row or not"
    );
    assert!(
        query_matches(&exposed_flattened_query(), "dlbv"),
        "a flattened legacy-row IMV was invisible to BOTH queries — the dangerous \
         population must never fall through the gap between them"
    );
}

// ---------------------------------------------------------------------------
// A full reconcile must rebuild EVERY mirror child, including those whose
// source node is partitioned but currently childless.
//
// Deriving the swap set from the source tree's leaves alone loses them: such a
// node is not a leaf (it has a sub-strategy) and has no descendants to stand in
// for it, so it contributes nothing and its mirror child — which exists and can
// hold rows — is never rebuilt, while the reconcile still returns RECONCILED.
// Both shapes below are ordinary: a branch pre-created ahead of next month's
// leaves, and a branch whose leaves have aged out under a retention policy.
// ---------------------------------------------------------------------------

/// The destructive half of the audit hazard, measured rather than asserted in
/// prose: running the remedy `reflex_audit` prints for a flattened mirror
/// EMPTIES it. Pinned because the operator guidance ("run reflex_reconcile,
/// never reflex_sync_partitions") rests on this being true, and because
/// `drop_orphans => FALSE` is widely assumed to be the safe option.
#[pg_test]
fn pg_subpart_sync_on_a_flattened_mirror_empties_it_silently() {
    build_depth2_fixture("dlas", "dlav");
    let seeded = imv_row_count("dlav");
    assert!(seeded > 0, "fixture seeded no rows");

    for br in ["a", "b"] {
        Spi::run(&format!(
            "CREATE UNLOGGED TABLE dlav_swap_{br} (LIKE dlav_dlas_{br} INCLUDING ALL)"
        ))
        .expect("swap table");
        Spi::run(&format!(
            "INSERT INTO dlav_swap_{br} SELECT * FROM dlav_dlas_{br}"
        ))
        .expect("fill swap");
        Spi::run(&format!("ALTER TABLE dlav DETACH PARTITION dlav_dlas_{br}")).expect("detach");
        Spi::run(&format!(
            "ALTER TABLE dlav ATTACH PARTITION dlav_swap_{br} FOR VALUES IN ('{up}')",
            up = br.to_uppercase()
        ))
        .expect("attach");
        Spi::run(&format!("DROP TABLE dlav_dlas_{br}")).expect("drop old");
        Spi::run(&format!(
            "ALTER TABLE dlav_swap_{br} RENAME TO dlav_dlas_{br}"
        ))
        .expect("rename");
    }
    assert_eq!(
        imv_row_count("dlav"),
        seeded,
        "the flattened mirror should still hold its rows"
    );

    // The conservative-looking call: drop_orphans => FALSE.
    let sync = Spi::get_one::<String>("SELECT reflex_sync_partitions('dlav', false)")
        .expect("sync")
        .expect("sync res");

    assert!(
        !sync.starts_with("ERROR"),
        "the destructive sync is expected to report SUCCESS — that is the hazard, \
         got: {sync}"
    );
    assert_eq!(
        imv_row_count("dlav"),
        0,
        "if this is no longer 0 the shape-drift heal has gained a refill and the \
         operator warning in HANDOFF.md must be revisited"
    );
    let stale = Spi::get_one::<bool>(
        "SELECT known_stale FROM public.__reflex_ivm_reference WHERE name = 'dlav'",
    )
    .expect("q")
    .expect("known_stale NULL");
    assert!(
        !stale,
        "known_stale was set — the emptying would then be visible to monitoring, and \
         the operator warning could be softened"
    );
    assert_eq!(
        imv_partitioned_child_count("dlav"),
        2,
        "the heal should have restored the shape while discarding the contents"
    );
}

/// A childless partitioned source branch, mirrored at depth 1. Drift injected
/// into its mirror child must be repaired by a full reconcile.
#[pg_test]
fn pg_subpart_reconcile_rebuilds_childless_branch_mirror_child() {
    Spi::run(
        "CREATE TABLE f1s (k TEXT NOT NULL, d DATE NOT NULL, id BIGINT) PARTITION BY LIST (k)",
    )
    .expect("root");
    Spi::run("CREATE TABLE f1s_p PARTITION OF f1s FOR VALUES IN ('P')").expect("plain child");
    // Pre-created branch, leaves not added yet — the standard rolling-window
    // pattern. It is partitioned and has no children.
    Spi::run("CREATE TABLE f1s_c PARTITION OF f1s FOR VALUES IN ('C') PARTITION BY RANGE (d)")
        .expect("childless branch");
    Spi::run("INSERT INTO f1s SELECT 'P', DATE '2026-01-05', g FROM generate_series(1, 30) g")
        .expect("seed");
    Spi::get_one::<String>(
        "SELECT create_reflex_ivm('f1v', 'SELECT k, d, id FROM f1s', 'k,d,id', \
         NULL, NULL, NULL, ARRAY['k'])",
    )
    .expect("c")
    .expect("c");
    assert_imv_correct("f1v", "SELECT k, d, id FROM f1s");

    // Drift: a row in the mirror that the source cannot produce.
    Spi::run("INSERT INTO f1v (k, d, id) VALUES ('C', '2026-01-09', 9999)").expect("inject drift");
    assert_eq!(
        imv_row_count("f1v"),
        31,
        "the drift row was not routed into the childless branch's mirror child"
    );

    let res = Spi::get_one::<&str>("SELECT reflex_reconcile('f1v')")
        .expect("rec")
        .expect("res");
    assert_eq!(res, "RECONCILED");
    assert_imv_correct("f1v", "SELECT k, d, id FROM f1s");
    assert_eq!(
        imv_row_count("f1v"),
        30,
        "the full reconcile skipped the childless branch's mirror child and left drift behind"
    );
}

/// Retention: a branch whose only source leaf has been dropped. Its mirror
/// child still holds that leaf's rows, and a full reconcile must remove them.
#[pg_test]
fn pg_subpart_reconcile_rebuilds_branch_whose_leaves_were_dropped() {
    Spi::run(
        "CREATE TABLE f2s (k TEXT NOT NULL, d DATE NOT NULL, id BIGINT) PARTITION BY LIST (k)",
    )
    .expect("root");
    for br in ["a", "b"] {
        Spi::run(&format!(
            "CREATE TABLE f2s_{br} PARTITION OF f2s FOR VALUES IN ('{up}') PARTITION BY RANGE (d)",
            up = br.to_uppercase()
        ))
        .expect("branch");
        Spi::run(&format!(
            "CREATE TABLE f2s_{br}_m1 PARTITION OF f2s_{br} \
             FOR VALUES FROM ('2026-01-01') TO ('2026-02-01')"
        ))
        .expect("leaf");
    }
    Spi::run(
        "INSERT INTO f2s SELECT v.k, DATE '2026-01-10', g \
         FROM generate_series(1, 25) g CROSS JOIN (VALUES ('A'),('B')) v(k)",
    )
    .expect("seed");
    // Mirror depth 1 over a depth-2 source: each mirror child is a plain table
    // holding its whole branch.
    Spi::get_one::<String>(
        "SELECT create_reflex_ivm('f2v', 'SELECT k, d, id FROM f2s', 'k,d,id', \
         NULL, NULL, NULL, ARRAY['k'])",
    )
    .expect("c")
    .expect("c");
    assert_eq!(imv_row_count("f2v"), 50, "fixture seeded wrong");
    assert_imv_correct("f2v", "SELECT k, d, id FROM f2s");

    // Retention drops branch A's only leaf. Branch A survives, childless.
    Spi::run("ALTER TABLE f2s_a DETACH PARTITION f2s_a_m1").expect("detach");
    Spi::run("DROP TABLE f2s_a_m1").expect("drop leaf");
    assert_eq!(
        Spi::get_one::<i64>("SELECT count(*)::int8 FROM f2s")
            .expect("q")
            .expect("c"),
        25,
        "the source should now hold only branch B"
    );

    let res = Spi::get_one::<&str>("SELECT reflex_reconcile('f2v')")
        .expect("rec")
        .expect("res");
    assert_eq!(res, "RECONCILED");
    assert_imv_correct("f2v", "SELECT k, d, id FROM f2s");
    assert_eq!(
        imv_row_count("f2v"),
        25,
        "the full reconcile left the dropped leaf's rows in the branch's mirror child"
    );
}

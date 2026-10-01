// Shared helpers + step-0 probes for the rebuild-propagation safeguard.

fn tree_xact_changes(rel: &str) -> i64 {
    Spi::get_one::<i64>(&format!(
        "SELECT COALESCE(sum(pg_stat_get_xact_tuples_inserted(relid) \
                           + pg_stat_get_xact_tuples_updated(relid) \
                           + pg_stat_get_xact_tuples_deleted(relid)), 0)::int8 \
         FROM pg_partition_tree('{rel}'::regclass) WHERE isleaf"
    ))
    .expect("tree xact stats")
    .unwrap_or(0)
}

fn rbd_build_rel(prefix: &str) {
    Spi::run(&format!(
        "CREATE TABLE {prefix}_anchor (id INT PRIMARY KEY, product_id INT NOT NULL, \
         location_id INT NOT NULL, qty INT)"
    ))
    .expect("anchor");
    Spi::run(&format!(
        "CREATE TABLE {prefix}_rel (product_id INT NOT NULL, location_id INT NOT NULL, is_active BOOL)"
    ))
    .expect("rel");
    Spi::run(&format!(
        "INSERT INTO {prefix}_anchor SELECT g, g % 7, g % 5, g FROM generate_series(1, 200) g"
    ))
    .expect("seed anchor");
    Spi::run(&format!(
        "INSERT INTO {prefix}_rel SELECT p, l, CASE WHEN p = 3 THEN NULL ELSE (p + l) % 2 = 0 END \
         FROM generate_series(0, 3) p CROSS JOIN generate_series(0, 4) l"
    ))
    .expect("seed rel");
}

fn rbd_dep_sql(prefix: &str, upstream: &str) -> String {
    format!(
        "SELECT a.id, a.product_id, a.location_id, a.qty, COALESCE(c.is_active, FALSE) AS active \
         FROM {prefix}_anchor a LEFT JOIN {upstream} c \
         ON c.product_id = a.product_id AND c.location_id = a.location_id"
    )
}

/// Step 0 (spec 3.1 #2): DELETE / UPDATE / INSERT statements written through a
/// partitioned IMV root reach a dependent's transition-table triggers with
/// exactly the changed rows (the primitive relies on nothing else).
#[pg_test]
fn pg_rbd_step0_dml_through_partitioned_root_maintains_dependent() {
    Spi::run(
        "CREATE TABLE rbd0_src (plan INT NOT NULL, id INT NOT NULL, qty INT) PARTITION BY LIST (plan)",
    )
    .expect("src");
    for p in [1, 2] {
        Spi::run(&format!(
            "CREATE TABLE rbd0_src_p{p} PARTITION OF rbd0_src FOR VALUES IN ({p})"
        ))
        .expect("part");
    }
    Spi::run("INSERT INTO rbd0_src SELECT p, g, g FROM generate_series(1, 50) g, (VALUES (1),(2)) v(p)")
        .expect("seed");
    create_imv(
        "rbd0_up",
        "SELECT create_reflex_ivm('rbd0_up', 'SELECT plan, id, qty FROM rbd0_src', \
         'plan, id', NULL, 'IMMEDIATE', NULL, ARRAY['plan'])",
    );
    let dep = crate::create_reflex_ivm(
        "rbd0_dep",
        "SELECT plan, SUM(qty) AS q, COUNT(*) AS n FROM rbd0_up GROUP BY plan",
        None, None, None, None,
    );
    assert_eq!(dep, "CREATE REFLEX INCREMENTAL VIEW");

    Spi::run("DELETE FROM rbd0_up WHERE plan = 2 AND id = 1").expect("delete via root");
    Spi::run("UPDATE rbd0_up SET qty = qty + 1 WHERE plan = 2 AND id = 2").expect("update via root");
    Spi::run("INSERT INTO rbd0_up VALUES (2, 1000, 7)").expect("insert via root");

    assert_imv_correct(
        "rbd0_dep",
        "SELECT plan, SUM(qty) AS q, COUNT(*) AS n FROM rbd0_up GROUP BY plan",
    );
}

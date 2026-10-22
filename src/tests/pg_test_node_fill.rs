// A brand-new mirror node whose leaves are all empty or fresh is filled with one
// INSERT routed through the node, not one base-query scan per leaf. Each test
// counts the INSERT statements that target a node with a statement trigger.

const NF_V_SQL: &str = "SELECT s.dem_plan_id, s.order_date, s.product_id, s.location_id, s.qty, l.canal \
     FROM nf_ss s JOIN nf_loc l ON l.id = s.location_id";
const NF_AGG_SQL: &str = "SELECT dem_plan_id, SUM(qty) AS q FROM nf_v GROUP BY dem_plan_id";

fn nf_load_dp_months(into: &str, dp: i64, months: &[u32]) {
    for m in months {
        Spi::run(&format!(
            "INSERT INTO {into} SELECT {dp}, make_date(2025, {m}, 1 + (g % 20)), g % 4, 1 + (g % 4), g * {m} \
             FROM generate_series(1, 12) g"
        ))
        .expect("load dp rows");
    }
}

fn nf_create_month_leaves(node: &str, months: &[u32]) {
    for m in months {
        Spi::run(&format!(
            "CREATE TABLE {node}_2025_0{m} PARTITION OF {node} \
             FOR VALUES FROM ('2025-0{m}-01') TO ('2025-0{next}-01')",
            next = m + 1
        ))
        .expect("month leaf");
    }
}

/// `nf_ss` LIST(dem_plan_id) -> RANGE(order_date), DP 1 populated in Jan-Mar,
/// and the `nf_loc` dimension it joins.
fn nf_source_with_dp1() {
    Spi::run("CREATE TABLE nf_loc (id BIGINT PRIMARY KEY, canal INT)").expect("dim");
    Spi::run("INSERT INTO nf_loc SELECT g, g % 2 FROM generate_series(1, 3) g").expect("dim rows");
    Spi::run(
        "CREATE TABLE nf_ss (dem_plan_id BIGINT NOT NULL, order_date DATE NOT NULL, \
         product_id BIGINT, location_id BIGINT, qty INT) PARTITION BY LIST (dem_plan_id)",
    )
    .expect("source root");
    Spi::run("CREATE TABLE nf_ss_1 PARTITION OF nf_ss FOR VALUES IN (1) PARTITION BY RANGE (order_date)")
        .expect("dp 1 node");
    nf_create_month_leaves("nf_ss_1", &[1, 2, 3]);
    nf_load_dp_months("nf_ss", 1, &[1, 2, 3]);
}

/// The passthrough `nf_v` mirroring both levels, and `nf_agg` reading it.
fn nf_fixture() {
    nf_source_with_dp1();
    let created = Spi::get_one::<String>(&format!(
        "SELECT create_reflex_ivm('nf_v', '{}', 'dem_plan_id,product_id,location_id,order_date', \
         NULL, NULL, NULL, ARRAY['dem_plan_id','order_date'])",
        NF_V_SQL.replace('\'', "''")
    ))
    .expect("create nf_v")
    .expect("create nf_v result");
    assert!(!created.starts_with("ERROR"), "create nf_v: {created}");
    let created = Spi::get_one::<String>(&format!(
        "SELECT create_reflex_ivm('nf_agg', '{}')",
        NF_AGG_SQL.replace('\'', "''")
    ))
    .expect("create nf_agg")
    .expect("create nf_agg result");
    assert!(!created.starts_with("ERROR"), "create nf_agg: {created}");
}

/// Build DP 2 detached as `nf_ss_2` RANGE(order_date) over Jan-Mar, load
/// `loaded_months`, and attach it to `nf_ss`.
fn nf_attach_new_dp(loaded_months: &[u32]) {
    Spi::run("CREATE TABLE nf_ss_2 (LIKE nf_ss) PARTITION BY RANGE (order_date)").expect("dp 2 node");
    nf_create_month_leaves("nf_ss_2", &[1, 2, 3]);
    nf_load_dp_months("nf_ss_2", 2, loaded_months);
    Spi::run("ALTER TABLE nf_ss ATTACH PARTITION nf_ss_2 FOR VALUES IN (2)").expect("attach dp 2");
}

/// Count the INSERT statements that target `node` under the label `probe`.
fn nf_probe(probe: &str, node: &str) {
    Spi::run(
        "CREATE TABLE IF NOT EXISTS nf_hits (probe TEXT PRIMARY KEY, n INT NOT NULL); \
         CREATE OR REPLACE FUNCTION nf_count_hit() RETURNS trigger LANGUAGE plpgsql AS $f$ \
         BEGIN UPDATE nf_hits SET n = n + 1 WHERE probe = TG_ARGV[0]; RETURN NULL; END $f$",
    )
    .expect("probe table");
    Spi::run(&format!("INSERT INTO nf_hits VALUES ('{probe}', 0)")).expect("probe row");
    Spi::run(&format!(
        "CREATE TRIGGER nf_probe_{probe} AFTER INSERT ON {node} \
         FOR EACH STATEMENT EXECUTE FUNCTION nf_count_hit('{probe}')"
    ))
    .expect("probe trigger");
}

fn nf_hits(probe: &str) -> i32 {
    Spi::get_one::<i32>(&format!("SELECT n FROM nf_hits WHERE probe = '{probe}'"))
        .expect("read probe")
        .expect("probe row")
}

fn nf_reconcile(view: &str, keys: &str) {
    let result = Spi::get_one::<String>(&format!(
        "SELECT reflex_reconcile_partition('{view}', '{keys}', '', true)"
    ))
    .expect("reconcile")
    .expect("reconcile result");
    assert!(result.starts_with("RECONCILED"), "reconcile {view}: {result}");
}

fn nf_assert_both_correct() {
    assert_imv_correct("nf_v", NF_V_SQL);
    assert_imv_correct("nf_agg", NF_AGG_SQL);
}

#[pg_test]
fn pg_node_fill_new_dp_inserts_once_through_the_node() {
    nf_fixture();
    nf_attach_new_dp(&[1, 2]);
    Spi::run("SELECT reflex_refresh_partition_snapshot_if_diverged('public.nf_ss')").expect("snapshot");
    nf_probe("node", "nf_v_nf_ss_2");

    nf_reconcile("nf_v", "2");

    assert_eq!(nf_hits("node"), 1, "the new DP node is filled by one routed INSERT");
    nf_assert_both_correct();
}

#[pg_test]
fn pg_node_fill_attach_flush_fills_through_the_node() {
    nf_fixture();
    nf_attach_new_dp(&[1, 2, 3]);
    nf_probe("node", "nf_v_nf_ss_2");

    Spi::run("SELECT reflex_flush_partitions()").expect("flush");

    assert_eq!(nf_hits("node"), 1, "the ATTACH flush fills the new DP node once");
    nf_assert_both_correct();
}

// The flush leaves an empty-source new leaf out of its reconcile, so the node
// is not wholly in the rebuild and its loaded leaves are filled one by one.
#[pg_test]
fn pg_node_fill_attach_flush_with_empty_month_fills_per_leaf() {
    nf_fixture();
    nf_attach_new_dp(&[1, 2]);
    nf_probe("node", "nf_v_nf_ss_2");

    Spi::run("SELECT reflex_flush_partitions()").expect("flush");

    assert_eq!(nf_hits("node"), 0, "a node with an unreconciled empty leaf is filled per leaf");
    nf_assert_both_correct();
    let march_empty = Spi::get_one::<bool>(
        "SELECT to_regclass('nf_v_nf_ss_2_2025_03') IS NOT NULL \
         AND NOT EXISTS (SELECT 1 FROM nf_v_nf_ss_2_2025_03)",
    )
    .expect("march leaf")
    .expect("march leaf");
    assert!(march_empty, "the March mirror leaf exists and is empty");
}

#[pg_test]
fn pg_node_fill_fresh_nonempty_leaves_are_replaced_once() {
    nf_fixture();
    nf_attach_new_dp(&[]);
    nf_load_dp_months("nf_ss", 2, &[1, 2]);
    let maintained = Spi::get_one::<i64>("SELECT count(*) FROM nf_v WHERE dem_plan_id = 2")
        .expect("count")
        .expect("count");
    assert!(maintained > 0, "IMMEDIATE maintenance wrote DP 2 into its fresh mirror leaves");
    nf_probe("node", "nf_v_nf_ss_2");

    nf_reconcile("nf_v", "2");

    assert_eq!(nf_hits("node"), 1, "fresh non-empty leaves are refilled by one node INSERT");
    nf_assert_both_correct();
    let in_imv = Spi::get_one::<i64>("SELECT count(*) FROM nf_v WHERE dem_plan_id = 2")
        .expect("count")
        .expect("count");
    let in_source = Spi::get_one::<i64>(&format!(
        "SELECT count(*) FROM ({NF_V_SQL}) v WHERE dem_plan_id = 2"
    ))
    .expect("count")
    .expect("count");
    assert_eq!(in_imv, in_source, "no DP 2 row is duplicated");
}

#[pg_test]
fn pg_node_fill_aggregate_fills_intermediate_then_target() {
    nf_source_with_dp1();
    let nf_va_sql = "SELECT dem_plan_id, order_date, product_id, SUM(qty) AS q FROM nf_ss \
                     GROUP BY dem_plan_id, order_date, product_id";
    // An explicit two-level `partition_by` is refused for an aggregate (its
    // sub-level check reads the passthrough unique key); auto-mirror keeps both
    // levels because both are bare GROUP BY columns.
    let created = Spi::get_one::<String>(&format!(
        "SELECT create_reflex_ivm('nf_va', '{}')",
        nf_va_sql.replace('\'', "''")
    ))
    .expect("create nf_va")
    .expect("create nf_va result");
    assert!(!created.starts_with("ERROR"), "create nf_va: {created}");
    nf_attach_new_dp(&[1, 2]);
    Spi::run("SELECT reflex_refresh_partition_snapshot_if_diverged('public.nf_ss')").expect("snapshot");
    let int_node = Spi::get_one::<String>(
        "SELECT to_regclass('__reflex_intermediate_nf_va_nf_ss_2')::text",
    )
    .expect("resolve intermediate node")
    .expect("intermediate node exists");
    let both_nodes_partitioned = Spi::get_one::<bool>(&format!(
        "SELECT bool_and(relkind = 'p') FROM pg_class \
         WHERE oid IN ('nf_va_nf_ss_2'::regclass, '{int_node}'::regclass)"
    ))
    .expect("node kinds")
    .expect("node kinds");
    assert!(both_nodes_partitioned, "nf_va mirrors both source levels");
    nf_probe("tgt", "nf_va_nf_ss_2");
    nf_probe("int", &int_node);

    nf_reconcile("nf_va", "2");

    assert_eq!(nf_hits("int"), 1, "the intermediate node is filled by one routed INSERT");
    assert_eq!(nf_hits("tgt"), 1, "the target node is filled by one routed INSERT");
    assert_imv_correct("nf_va", nf_va_sql);
}

#[pg_test]
fn pg_node_fill_aggregate_fresh_nonempty_leaves() {
    nf_source_with_dp1();
    let nf_vn_sql = "SELECT dem_plan_id, order_date, product_id, SUM(qty) AS q, COUNT(*) AS c FROM nf_ss \
                     GROUP BY dem_plan_id, order_date, product_id";
    let created = Spi::get_one::<String>(&format!(
        "SELECT create_reflex_ivm('nf_vn', '{}')",
        nf_vn_sql.replace('\'', "''")
    ))
    .expect("create nf_vn")
    .expect("create nf_vn result");
    assert!(!created.starts_with("ERROR"), "create nf_vn: {created}");
    nf_attach_new_dp(&[]);
    // Immediate maintenance fills the new leaves before the node fill.
    nf_load_dp_months("nf_ss", 2, &[1, 2]);
    let count_dp2_intermediate = || {
        Spi::get_one::<i64>("SELECT count(*) FROM __reflex_intermediate_nf_vn WHERE dem_plan_id = 2")
            .expect("count intermediate")
            .expect("count intermediate")
    };
    let intermediate_before = count_dp2_intermediate();
    assert!(intermediate_before > 0, "maintenance wrote DP 2 intermediate rows");
    let int_node = Spi::get_one::<String>(
        "SELECT to_regclass('__reflex_intermediate_nf_vn_nf_ss_2')::text",
    )
    .expect("resolve intermediate node")
    .expect("intermediate node exists");
    nf_probe("int", &int_node);
    nf_probe("tgt", "nf_vn_nf_ss_2");

    nf_reconcile("nf_vn", "2");

    assert_eq!(nf_hits("int"), 1, "the intermediate node is filled by one routed INSERT");
    assert_eq!(nf_hits("tgt"), 1, "the target node is filled by one routed INSERT");
    assert_eq!(count_dp2_intermediate(), intermediate_before, "intermediate rows are replaced, not duplicated");
    assert_imv_correct("nf_vn", nf_vn_sql);
}

#[pg_test]
fn pg_node_fill_mixed_node_keeps_per_leaf_path() {
    nf_fixture();
    Spi::run("TRUNCATE nf_v_nf_ss_1_2025_01").expect("empty one leaf");
    nf_probe("node", "nf_v_nf_ss_1");

    nf_reconcile("nf_v", "1");

    assert_eq!(nf_hits("node"), 0, "a node with populated leaves is never node-filled");
    nf_assert_both_correct();
}

#[pg_test]
fn pg_node_fill_depth_one_never_inserts_into_root() {
    nf_source_with_dp1();
    let created = Spi::get_one::<String>(&format!(
        "SELECT create_reflex_ivm('nf_v1', '{}', 'dem_plan_id,product_id,location_id,order_date', \
         NULL, NULL, NULL, ARRAY['dem_plan_id'])",
        NF_V_SQL.replace('\'', "''")
    ))
    .expect("create nf_v1")
    .expect("create nf_v1 result");
    assert!(!created.starts_with("ERROR"), "create nf_v1: {created}");
    nf_attach_new_dp(&[1, 2]);
    Spi::run("SELECT reflex_refresh_partition_snapshot_if_diverged('public.nf_ss')").expect("snapshot");
    nf_probe("root", "nf_v1");

    nf_reconcile("nf_v1", "2");

    assert_eq!(nf_hits("root"), 0, "a new DP leaf of the root is filled per leaf");
    assert_imv_correct("nf_v1", NF_V_SQL);

    // Every child of the root empty and in the rebuild: only the empty-constraint
    // refusal stands between the full reconcile and an INSERT into the root.
    Spi::run("TRUNCATE nf_v1_nf_ss_1, nf_v1_nf_ss_2").expect("empty every root child");
    let result = Spi::get_one::<String>("SELECT reflex_reconcile('nf_v1')")
        .expect("reconcile")
        .expect("reconcile result");
    assert!(!result.starts_with("ERROR"), "reconcile nf_v1: {result}");

    assert_eq!(nf_hits("root"), 0, "the IMV root is never node-filled");
    assert_imv_correct("nf_v1", NF_V_SQL);
}

#[pg_test]
fn pg_node_fill_default_leaf_matches_oracle() {
    nf_fixture();
    Spi::run("CREATE TABLE nf_ss_2 (LIKE nf_ss) PARTITION BY RANGE (order_date)").expect("dp 2 node");
    nf_create_month_leaves("nf_ss_2", &[1]);
    Spi::run("CREATE TABLE nf_ss_2_rest PARTITION OF nf_ss_2 DEFAULT").expect("default leaf");
    nf_load_dp_months("nf_ss_2", 2, &[1, 2, 3]);
    Spi::run("ALTER TABLE nf_ss ATTACH PARTITION nf_ss_2 FOR VALUES IN (2)").expect("attach dp 2");
    Spi::run("SELECT reflex_refresh_partition_snapshot_if_diverged('public.nf_ss')").expect("snapshot");
    nf_probe("node", "nf_v_nf_ss_2");

    nf_reconcile("nf_v", "2");

    assert_eq!(nf_hits("node"), 1, "a node with a DEFAULT leaf is filled by one routed INSERT");
    let in_default = Spi::get_one::<i64>("SELECT count(*) FROM nf_v_nf_ss_2_rest")
        .expect("count")
        .expect("count");
    assert!(in_default > 0, "Feb and Mar rows route into the DEFAULT leaf");
    nf_assert_both_correct();
}

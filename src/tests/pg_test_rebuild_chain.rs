/// Bug 3: reflex_rebuild_chain CASCADE-drops dependents and recreates only the
/// named IMV. Without an explicit cascade it must refuse rather than destroy.
#[pg_test]
fn pg_rebuild_chain_refuses_with_dependents() {
    Spi::run("CREATE TABLE rc_src (k TEXT, v INT)").expect("src");
    Spi::run("INSERT INTO rc_src VALUES ('a', 1), ('b', 2)").expect("seed");
    Spi::run("SELECT create_reflex_ivm('rc_base', 'SELECT k, sum(v) AS s FROM rc_src GROUP BY k', 'k')")
        .expect("base");
    Spi::run("SELECT create_reflex_ivm('rc_dep', 'SELECT count(*) AS c FROM rc_base', 'c')")
        .expect("dep");

    let out = Spi::get_one::<String>("SELECT reflex_rebuild_chain('rc_base')")
        .expect("rebuild call")
        .expect("rebuild result");
    assert!(out.starts_with("ERROR"), "must refuse, got: {out}");
    assert!(out.contains("rc_dep"), "error must name the dependent: {out}");

    let dep_alive: i64 = Spi::get_one(
        "SELECT count(*) FROM public.__reflex_ivm_reference WHERE name = 'rc_dep'",
    ).expect("dep query").unwrap_or(0);
    assert_eq!(dep_alive, 1, "the dependent must still exist after the refusal");
}

/// No dependents: behaviour is unchanged.
#[pg_test]
fn pg_rebuild_chain_succeeds_without_dependents() {
    Spi::run("CREATE TABLE rs_src (k TEXT, v INT)").expect("src");
    Spi::run("INSERT INTO rs_src VALUES ('a', 1)").expect("seed");
    Spi::run("SELECT create_reflex_ivm('rs_base', 'SELECT k, sum(v) AS s FROM rs_src GROUP BY k', 'k')")
        .expect("base");

    let out = Spi::get_one::<String>("SELECT reflex_rebuild_chain('rs_base')")
        .expect("rebuild call")
        .expect("rebuild result");
    assert!(!out.starts_with("ERROR"), "rebuild returned: {out}");

    let rows: i64 = Spi::get_one("SELECT count(*) FROM rs_base")
        .expect("count").unwrap_or(-1);
    assert_eq!(rows, 1, "rebuilt IMV must hold its rows");
}

/// With cascade => TRUE, dependents are recreated from their stored create_args.
#[pg_test]
fn pg_rebuild_chain_cascade_restores_dependents() {
    Spi::run("CREATE TABLE rcc_src (k TEXT, v INT)").expect("src");
    Spi::run("INSERT INTO rcc_src VALUES ('a', 1), ('b', 2)").expect("seed");
    Spi::run("SELECT create_reflex_ivm('rcc_base', 'SELECT k, sum(v) AS s FROM rcc_src GROUP BY k', 'k')")
        .expect("base");
    Spi::run("SELECT create_reflex_ivm('rcc_dep', 'SELECT count(*) AS c FROM rcc_base', 'c')")
        .expect("dep");

    let out = Spi::get_one::<String>("SELECT reflex_rebuild_chain('rcc_base', cascade => TRUE)")
        .expect("rebuild call")
        .expect("rebuild result");
    assert!(!out.starts_with("ERROR"), "cascade rebuild returned: {out}");

    let dep_alive: i64 = Spi::get_one(
        "SELECT count(*) FROM public.__reflex_ivm_reference WHERE name = 'rcc_dep'",
    ).expect("dep query").unwrap_or(0);
    assert_eq!(dep_alive, 1, "dependent must be restored in the registry");

    let dep_rows: i64 = Spi::get_one("SELECT c::bigint FROM rcc_dep")
        .expect("dep rows").unwrap_or(-1);
    assert_eq!(dep_rows, 2, "restored dependent must hold correct data");
}

/// CASCADE drop recurses over the whole dependent tree, so cascade recreate must
/// restore TRANSITIVE dependents too, not just direct ones. A depth+2 dependent
/// dropped-but-not-recreated is silent data loss reported as success — exactly
/// the design's own 4-level motivating chain.
#[pg_test]
fn pg_rebuild_chain_cascade_restores_transitive_dependents() {
    Spi::run("CREATE TABLE rct_src (k TEXT, v INT)").expect("src");
    Spi::run("INSERT INTO rct_src VALUES ('a', 1), ('b', 2)").expect("seed");
    Spi::run("SELECT create_reflex_ivm('rct_base', 'SELECT k, sum(v) AS s FROM rct_src GROUP BY k', 'k')")
        .expect("base");
    // rct_mid depends on rct_base (depth+1); rct_leaf depends on rct_mid (depth+2).
    Spi::run("SELECT create_reflex_ivm('rct_mid', 'SELECT k, s FROM rct_base', 'k')")
        .expect("mid");
    Spi::run("SELECT create_reflex_ivm('rct_leaf', 'SELECT count(*) AS c FROM rct_mid', 'c')")
        .expect("leaf");

    let out = Spi::get_one::<String>("SELECT reflex_rebuild_chain('rct_base', cascade => TRUE)")
        .expect("rebuild call")
        .expect("rebuild result");
    assert!(!out.starts_with("ERROR"), "cascade rebuild returned: {out}");

    let mid_alive: i64 = Spi::get_one(
        "SELECT count(*) FROM public.__reflex_ivm_reference WHERE name = 'rct_mid'",
    ).expect("mid query").unwrap_or(0);
    assert_eq!(mid_alive, 1, "direct dependent must be restored");

    let leaf_alive: i64 = Spi::get_one(
        "SELECT count(*) FROM public.__reflex_ivm_reference WHERE name = 'rct_leaf'",
    ).expect("leaf query").unwrap_or(0);
    assert_eq!(leaf_alive, 1, "transitive (depth+2) dependent must be restored, not silently lost");

    let leaf_rows: i64 = Spi::get_one("SELECT c::bigint FROM rct_leaf")
        .expect("leaf rows").unwrap_or(-1);
    assert_eq!(leaf_rows, 2, "restored transitive dependent must hold correct data");
}

/// A dependent predating create_args (1.10.8) cannot be faithfully recreated.
/// Recreating it from `{}` would silently reset storage/refresh/partitioning,
/// which is the same data-loss shape as the bug. It must refuse.
#[pg_test]
fn pg_rebuild_chain_cascade_refuses_null_create_args() {
    Spi::run("CREATE TABLE rcn_src (k TEXT, v INT)").expect("src");
    Spi::run("INSERT INTO rcn_src VALUES ('a', 1)").expect("seed");
    Spi::run("SELECT create_reflex_ivm('rcn_base', 'SELECT k, sum(v) AS s FROM rcn_src GROUP BY k', 'k')")
        .expect("base");
    Spi::run("SELECT create_reflex_ivm('rcn_dep', 'SELECT count(*) AS c FROM rcn_base', 'c')")
        .expect("dep");
    Spi::run("UPDATE public.__reflex_ivm_reference SET create_args = NULL WHERE name = 'rcn_dep'")
        .expect("simulate pre-1.10.8 dependent");

    let out = Spi::get_one::<String>("SELECT reflex_rebuild_chain('rcn_base', cascade => TRUE)")
        .expect("rebuild call")
        .expect("rebuild result");
    assert!(out.starts_with("ERROR"), "must refuse, got: {out}");
    assert!(out.contains("rcn_dep"), "error must name the dependent: {out}");
}

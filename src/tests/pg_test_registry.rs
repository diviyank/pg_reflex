// Round-trip tests for the registry read seam (sql_writer::registry).
// These are #[pg_test] (need a live SPI client) and are include!'d into the
// `tests` module in lib.rs. They lock the contract: a row written by the
// real create path is read back field-for-field by read_imv, and the typed
// setters mutate exactly the column they name.

#[pg_test]
fn read_imv_round_trips_an_aggregate_ivm() {
    Spi::run(
        "CREATE TABLE rt_sales (region TEXT, amount NUMERIC);
         INSERT INTO rt_sales VALUES ('US', 100), ('EU', 50);",
    )
    .unwrap();
    Spi::get_one::<&str>(
        "SELECT create_reflex_ivm('rt_by_region', \
         'SELECT region, SUM(amount) AS total FROM rt_sales GROUP BY region')",
    )
    .unwrap();

    let found = Spi::connect_mut(|client| {
        let rec = crate::sql_writer::registry::read_imv(client, "rt_by_region")
            .expect("read_imv returned None for an existing IMV");
        assert_eq!(rec.view_name, "rt_by_region");
        assert!(rec.enabled);
        assert!(
            rec.base_query.to_lowercase().contains("from rt_sales"),
            "base_query not round-tripped: {}",
            rec.base_query
        );
        assert!(!rec.end_query.is_empty(), "end_query empty");
        let plan = rec.plan.as_ref().expect("aggregations did not parse to a plan");
        assert!(!plan.is_passthrough, "aggregate IMV wrongly flagged passthrough");
        true
    });
    assert!(found);
}

#[pg_test]
fn read_imv_returns_none_for_missing() {
    let got = Spi::connect_mut(|client| {
        crate::sql_writer::registry::read_imv(client, "does_not_exist").is_some()
    });
    assert!(!got, "read_imv must return None for an unregistered name");
}

#[pg_test]
fn set_wipe_floor_rows_sets_only_that_column() {
    Spi::run(
        "CREATE TABLE rt_wf (g TEXT, v NUMERIC);
         INSERT INTO rt_wf VALUES ('a', 1);",
    )
    .unwrap();
    Spi::get_one::<&str>(
        "SELECT create_reflex_ivm('rt_wf_imv', \
         'SELECT g, SUM(v) AS s FROM rt_wf GROUP BY g')",
    )
    .unwrap();

    let affected = Spi::connect_mut(|client| {
        crate::sql_writer::registry::set_wipe_floor_rows(client, "rt_wf_imv", Some(2000))
            .expect("set_wipe_floor_rows failed")
    });
    assert_eq!(affected, 1, "expected exactly one row updated");

    let stored = Spi::get_one::<i64>(
        "SELECT wipe_floor_rows FROM public.__reflex_ivm_reference WHERE name = 'rt_wf_imv'",
    )
    .unwrap()
    .unwrap();
    assert_eq!(stored, 2000);
}

#[pg_test]
fn remove_graph_child_unlinks_one_child() {
    Spi::run("CREATE TABLE rt_gc (g TEXT, v NUMERIC); INSERT INTO rt_gc VALUES ('a', 1);").unwrap();
    Spi::get_one::<&str>(
        "SELECT create_reflex_ivm('rt_gc_parent', 'SELECT g, SUM(v) AS s FROM rt_gc GROUP BY g')",
    )
    .unwrap();
    Spi::run(
        "UPDATE public.__reflex_ivm_reference \
         SET graph_child = ARRAY['rt_gc_child'] WHERE name = 'rt_gc_parent'",
    )
    .unwrap();

    Spi::connect_mut(|client| {
        crate::sql_writer::registry::remove_graph_child(client, "rt_gc_parent", "rt_gc_child")
            .expect("remove_graph_child failed");
    });

    let remaining = Spi::get_one::<i64>(
        "SELECT COALESCE(array_length(graph_child, 1), 0) \
         FROM public.__reflex_ivm_reference WHERE name = 'rt_gc_parent'",
    )
    .unwrap()
    .unwrap();
    assert_eq!(remaining, 0, "child was not removed from graph_child");
}

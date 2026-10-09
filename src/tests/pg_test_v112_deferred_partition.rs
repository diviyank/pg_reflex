fn v112_dp_reflex_triggers_on(table: &str) -> Vec<String> {
    Spi::connect(|client| {
        client
            .select(
                &format!(
                    "SELECT tgname::text AS n FROM pg_trigger \
                     WHERE tgrelid = '{table}'::regclass AND NOT tgisinternal \
                       AND tgname LIKE '\\_\\_reflex\\_%' ORDER BY 1"
                ),
                None,
                &[],
            )
            .expect("triggers")
            .filter_map(|row| row.get_by_name::<String, _>("n").ok().flatten())
            .collect()
    })
}

fn v112_dp_reflex_trigger_functions_naming(fragment: &str) -> i64 {
    Spi::get_one::<i64>(&format!(
        "SELECT count(*) FROM pg_proc WHERE proname LIKE '\\_\\_reflex\\_%trigger\\_on\\_%' \
           AND proname LIKE '%{fragment}%'"
    ))
    .expect("functions")
    .unwrap_or(0)
}

/// Every statement-trigger name of a 45-character source, and the truncate
/// trigger name of a 38-character one, exceeds 63 bytes and is stored shortened.
#[pg_test]
fn v112_dp_drop_removes_source_triggers_whose_names_were_shortened() {
    for (source, mode) in [
        ("v112_dp_zz_long_named_source_table_for_a_drop", "IMMEDIATE"),
        ("v112_dp_zz_long_named_source_table_for_a_drop", "DEFERRED"),
        ("v112_dp_zy_a_long_named_table_for_drop", "IMMEDIATE"),
        ("v112_dp_zy_a_long_named_table_for_drop", "DEFERRED"),
    ] {
        Spi::run(&format!("CREATE TABLE {source}(id SERIAL PRIMARY KEY, g TEXT, v INT)"))
            .expect("source");
        Spi::run(&format!("INSERT INTO {source}(g, v) VALUES ('a', 1), ('b', 2)")).expect("seed");
        let query = format!("SELECT g, SUM(v) AS r FROM {source} GROUP BY g");
        assert_eq!(
            crate::create_reflex_ivm("v112_dp_long_v", &query, None, None, Some(mode), None),
            "CREATE REFLEX INCREMENTAL VIEW"
        );
        assert_eq!(
            v112_dp_reflex_triggers_on(source).len(),
            4,
            "fixture: {source} carries its four statement triggers"
        );
        assert_eq!(crate::drop_reflex_ivm("v112_dp_long_v"), "DROP REFLEX INCREMENTAL VIEW");
        assert_eq!(
            v112_dp_reflex_triggers_on(source),
            Vec::<String>::new(),
            "{mode}: triggers left on {source}"
        );
        assert_eq!(
            v112_dp_reflex_trigger_functions_naming(&source[..30]),
            0,
            "{mode}: trigger functions left for {source}"
        );
        Spi::run(&format!("INSERT INTO {source}(g, v) VALUES ('c', 3)")).expect("write after drop");
        Spi::run(&format!("DROP TABLE {source}")).expect("drop source");
    }
}

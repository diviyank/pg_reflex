// `__reflex_named_relation` and `__reflex_reads_relation` run for every registry
// row in each query of the DDL event trigger and of the partition flush, so for
// every ALTER TABLE / CREATE TABLE of the database, pg_reflex's own included.
// They must give the answers of their set-based definition, kept here as the
// reference, at a cost per call that does not grow with planning work.

const V112_NR_REFERENCE_FUNCTIONS: &str = r#"
    CREATE FUNCTION pg_temp.reference_named_relation(spelling TEXT)
    RETURNS TEXT[] LANGUAGE sql IMMUTABLE AS $fn$
        SELECT CASE
                 WHEN p.part IS NULL THEN ARRAY[spelling]
                 WHEN p.part[2] IS NULL THEN ARRAY['public', f.first_part]
                 ELSE ARRAY[f.first_part, f.second_part]
               END
          FROM (SELECT regexp_match(
                    spelling,
                    '^("(?:[^"]|"")*"|[^."]+)(?:\.("(?:[^"]|"")*"|[^."]+))?$') AS part) p
         CROSS JOIN LATERAL (
            SELECT CASE WHEN p.part[1] LIKE '"%"'
                        THEN replace(substr(p.part[1], 2, length(p.part[1]) - 2), '""', '"')
                        ELSE translate(p.part[1], 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz')
                   END AS first_part,
                   CASE WHEN p.part[2] LIKE '"%"'
                        THEN replace(substr(p.part[2], 2, length(p.part[2]) - 2), '""', '"')
                        ELSE translate(p.part[2], 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz')
                   END AS second_part) f
    $fn$;
    CREATE FUNCTION pg_temp.reference_reads_relation(registered TEXT[], relation TEXT)
    RETURNS BOOLEAN LANGUAGE sql IMMUTABLE AS $fn$
        SELECT EXISTS (
            SELECT 1 FROM unnest(registered) AS spelling
             WHERE pg_temp.reference_named_relation(spelling) = pg_temp.reference_named_relation(relation))
    $fn$;
"#;

/// Spellings built from parts that are unquoted, quoted, mixed-case, empty,
/// dotted, with doubled or unbalanced quotes, blanks, a newline or non-ASCII
/// letters, alone, joined by one dot and by two.
const V112_NR_SPELLINGS: &str = r#"
    WITH part(p) AS (
        VALUES ('t'), ('T'), ('Ta_1'), ('"T"'), ('"t"'), ('"a.b"'), ('"x""y"'), ('""'), ('"'),
               ('"a'), ('a"'), ('x"y'), (''), (' t'), ('t '), (E't\n'), ('ÀB'), ('"ÀB"'),
               ('public'), ('"public"'), ('"A""".b')
    )
    SELECT NULL::text AS spelling
    UNION ALL SELECT p FROM part
    UNION ALL SELECT a.p || '.' || b.p FROM part a, part b
    UNION ALL SELECT a.p || '.' || b.p || '.' || c.p
                FROM part a, part b, part c WHERE a.p IN ('t', '"T"', '') AND c.p IN ('t', '')
    UNION ALL VALUES ('.'), ('..'), ('.t'), ('t.'), ('a..b')
"#;

fn v112_nr_count(sql: &str) -> i64 {
    Spi::get_one::<i64>(sql)
        .expect("count query runs")
        .expect("count is not null")
}

#[pg_test]
fn v112_nr_named_relation_answers_as_its_set_based_definition() {
    Spi::run(V112_NR_REFERENCE_FUNCTIONS).expect("reference functions are created");
    let compared = v112_nr_count(&format!(
        "SELECT count(*) FROM ({V112_NR_SPELLINGS}) s"
    ));
    assert!(compared > 500, "the corpus holds {compared} spellings");
    let differing: Vec<String> = Spi::connect(|client| {
        client
            .select(
                &format!(
                    "SELECT format('%L: %s, reference %s', spelling, \
                                   public.__reflex_named_relation(spelling), \
                                   pg_temp.reference_named_relation(spelling)) AS line \
                       FROM ({V112_NR_SPELLINGS}) s \
                      WHERE public.__reflex_named_relation(spelling) \
                            IS DISTINCT FROM pg_temp.reference_named_relation(spelling)"
                ),
                None,
                &[],
            )
            .expect("comparison runs")
            .filter_map(|row| row.get_by_name::<String, _>("line").ok().flatten())
            .collect()
    });
    assert!(differing.is_empty(), "{}", differing.join("\n"));
}

#[pg_test]
fn v112_nr_reads_relation_answers_as_its_set_based_definition() {
    Spi::run(V112_NR_REFERENCE_FUNCTIONS).expect("reference functions are created");
    let differing = v112_nr_count(&format!(
        "WITH spelling AS ({V112_NR_SPELLINGS}), \
              registered(a) AS ( \
                SELECT NULL::text[] UNION ALL SELECT ARRAY[]::text[] \
                UNION ALL SELECT ARRAY[NULL::text] \
                UNION ALL SELECT ARRAY[spelling] FROM spelling \
                UNION ALL SELECT ARRAY['other.x', spelling, NULL] FROM spelling \
                UNION ALL SELECT ARRAY[ARRAY['t', 'u'], ARRAY['\"T\"', 'ta.t']]) \
         SELECT count(*) FROM registered r, \
                (SELECT spelling FROM spelling \
                  WHERE spelling IS NULL OR spelling IN ('t', 'T', '\"t\"', 'public.t', '\"public\".\"T\"', \
                                                         'ta.t', '\"ta\".\"t\"', 'Ta.T', '', '.t', 'x\"y')) s \
          WHERE public.__reflex_reads_relation(r.a, s.spelling) \
                IS DISTINCT FROM pg_temp.reference_reads_relation(r.a, s.spelling)"
    ));
    assert_eq!(differing, 0, "registered spellings read differently from the reference");
}

#[pg_test]
fn v112_nr_registry_predicates_are_not_planned_on_every_call() {
    // A SQL-language function PostgreSQL cannot inline (a FROM, a sub-select) is
    // planned again on every call before PostgreSQL 18; PL/pgSQL keeps its plans
    // for the session.
    let planned_per_call = v112_nr_count(
        "SELECT count(*) FROM pg_proc p JOIN pg_language l ON l.oid = p.prolang \
          WHERE p.oid IN ('public.__reflex_named_relation(text)'::regprocedure, \
                          'public.__reflex_reads_relation(text[], text)'::regprocedure) \
            AND l.lanname <> 'plpgsql'",
    );
    assert_eq!(planned_per_call, 0, "a registry predicate is planned on every call");
}

/// A partitioned source `{prefix}_src` and three partitioned IMVs, each reading
/// the one before: `{prefix}_base` over the source, `{prefix}_mid`, `{prefix}_top`.
fn v112_nr_open_chain(prefix: &str) {
    Spi::run(&format!(
        "CREATE TABLE {prefix}_src (id INT NOT NULL, g TEXT NOT NULL, v INT NOT NULL) \
           PARTITION BY LIST (g); \
         CREATE TABLE {prefix}_src_a PARTITION OF {prefix}_src FOR VALUES IN ('a'); \
         CREATE TABLE {prefix}_src_b PARTITION OF {prefix}_src FOR VALUES IN ('b'); \
         INSERT INTO {prefix}_src SELECT i, CASE WHEN i % 2 = 0 THEN 'a' ELSE 'b' END, i \
           FROM generate_series(1, 20) i"
    ))
    .expect("source is created");
    for (imv, query) in [
        ("base", format!("SELECT g, SUM(v) AS total, COUNT(*) AS n FROM {prefix}_src GROUP BY g")),
        ("mid", format!("SELECT g, total, n FROM {prefix}_base")),
        ("top", format!("SELECT g, total FROM {prefix}_mid")),
    ] {
        Spi::run(&format!(
            "SELECT create_reflex_ivm('{prefix}_{imv}', {}, 'g', NULL, 'IMMEDIATE', NULL, ARRAY['g'])",
            sql_lit(&query)
        ))
        .expect("IMV is created");
    }
}

fn v112_nr_syncs_in_this_transaction() -> i64 {
    v112_nr_count(
        "SELECT COALESCE(sum(pg_stat_get_xact_function_calls(oid)), 0)::bigint \
           FROM pg_proc WHERE proname = 'reflex_sync_partitions'",
    )
}

#[pg_test]
fn v112_nr_trigger_toggle_of_a_relocation_syncs_no_reader() {
    v112_nr_open_chain("v112_nr_tg");
    Spi::run("SET LOCAL track_functions = 'all'").expect("function calls are counted");
    let before = v112_nr_syncs_in_this_transaction();
    Spi::run(
        "SET LOCAL pg_reflex.internal_trigger_toggle_root = 'v112_nr_tg_base'; \
         ALTER TABLE v112_nr_tg_base DISABLE TRIGGER USER; \
         ALTER TABLE v112_nr_tg_base ENABLE TRIGGER USER; \
         SET LOCAL pg_reflex.internal_trigger_toggle_root = ''",
    )
    .expect("toggle runs");
    assert_eq!(
        v112_nr_syncs_in_this_transaction() - before,
        0,
        "pg_reflex's own trigger toggle synced the readers of the toggled IMV"
    );
    Spi::run(
        "ALTER TABLE v112_nr_tg_base DISABLE TRIGGER USER; \
         ALTER TABLE v112_nr_tg_base ENABLE TRIGGER USER",
    )
    .expect("user ALTER runs");
    assert!(
        v112_nr_syncs_in_this_transaction() - before >= 1,
        "a user's ALTER of an IMV no longer syncs its partitioned readers"
    );
}

#[pg_test]
fn v112_nr_attached_partition_syncs_each_imv_of_a_chain_at_most_twice() {
    v112_nr_open_chain("v112_nr_at");
    Spi::run("SET LOCAL track_functions = 'all'").expect("function calls are counted");
    let before = v112_nr_syncs_in_this_transaction();
    Spi::run(
        "CREATE TABLE v112_nr_at_src_c (LIKE v112_nr_at_src); \
         INSERT INTO v112_nr_at_src_c SELECT i, 'c', i FROM generate_series(100, 110) i; \
         ALTER TABLE v112_nr_at_src ATTACH PARTITION v112_nr_at_src_c FOR VALUES IN ('c'); \
         SELECT reflex_flush_partition_source('v112_nr_at_src')",
    )
    .expect("partition is attached and flushed");
    let syncs = v112_nr_syncs_in_this_transaction() - before;
    assert!(
        syncs <= 6,
        "{syncs} partition syncs for one attach under a chain of three IMVs"
    );
    let fresh = "SELECT g, SUM(v) AS total, COUNT(*) AS n FROM v112_nr_at_src GROUP BY g";
    assert_imv_correct("v112_nr_at_base", fresh);
    assert_imv_correct("v112_nr_at_mid", fresh);
    assert_imv_correct(
        "v112_nr_at_top",
        "SELECT g, SUM(v) AS total FROM v112_nr_at_src GROUP BY g",
    );
}

// An UPDATE statement whose rows keep, as a multiset, the same values on every
// column the IMVs of the source read changes no IMV: the source's UPDATE trigger
// returns before capturing, staging or maintaining anything. The check compares
// values, not the SET list, so every way a read column can change without being
// named (BEFORE triggers, row movement, generated columns) is still maintained.

const UCC_ROWS: i64 = 2000;

fn ucc_staged_rows(source: &str) -> i64 {
    let delta = crate::query_decomposer::staging_delta_table_name(source);
    Spi::get_one::<i64>(&format!("SELECT count(*) FROM {delta}"))
        .expect("count staged rows")
        .expect("count is not null")
}

fn ucc_pending_flushes(source: &str) -> i64 {
    Spi::get_one::<i64>(&format!(
        "SELECT count(*) FROM public.__reflex_deferred_pending WHERE source_table = '{source}'"
    ))
    .expect("count pending flushes")
    .expect("count is not null")
}

fn ucc_flush(source: &str) {
    Spi::run(&format!("SELECT reflex_flush_deferred('{source}')")).expect("flush");
}

fn ucc_flush_if_deferred(source: &str, mode: &str) {
    if mode == "DEFERRED" {
        ucc_flush(source);
    }
}

fn ucc_assert_nothing_captured(source: &str, statement: &str) {
    Spi::run(statement).expect("statement leaving every read column unchanged");
    assert_eq!(
        (ucc_staged_rows(source), ucc_pending_flushes(source)),
        (0, 0),
        "`{statement}` leaves every column an IMV of {source} reads unchanged, \
         yet its delta was captured"
    );
}

fn ucc_assert_captured(source: &str, statement: &str) {
    Spi::run(statement).expect("statement changing a read column");
    assert!(
        ucc_staged_rows(source) > 0,
        "`{statement}` changes a column an IMV of {source} reads: its delta must be captured"
    );
}

fn ucc_create(view: &str, sql: &str, unique: Option<&str>, mode: &str) {
    assert_eq!(
        crate::create_reflex_ivm(view, sql, unique, None, Some(mode), None),
        "CREATE REFLEX INCREMENTAL VIEW",
        "create {view}"
    );
}

fn ucc_source(table: &str) {
    Spi::run(&format!(
        "CREATE TABLE {table} (id INT PRIMARY KEY, grp INT, amount NUMERIC, note TEXT, extra INT)"
    ))
    .expect("create source");
    Spi::run(&format!(
        "INSERT INTO {table} SELECT g, g % 5, g, 'n' || g, 0 FROM generate_series(1, {UCC_ROWS}) g"
    ))
    .expect("seed source");
}

fn ucc_sum_sql(source: &str) -> String {
    format!("SELECT grp, SUM(amount) AS total, COUNT(*) AS n FROM {source} GROUP BY grp")
}

fn ucc_partitioned_source(table: &str) {
    Spi::run(&format!(
        "CREATE TABLE {table} (id INT, region TEXT, amount NUMERIC, note TEXT, \
         PRIMARY KEY (id, region)) PARTITION BY LIST (region)"
    ))
    .expect("root");
    for region in ["a", "b"] {
        Spi::run(&format!(
            "CREATE TABLE {table}_{region} PARTITION OF {table} FOR VALUES IN ('{region}')"
        ))
        .expect("leaf");
    }
    Spi::run(&format!(
        "INSERT INTO {table} SELECT g, CASE WHEN g % 2 = 0 THEN 'a' ELSE 'b' END, g, 'n' \
         FROM generate_series(1, {UCC_ROWS}) g"
    ))
    .expect("seed");
}

#[pg_test]
fn test_ucc_unread_column_update_captures_nothing() {
    ucc_source("ucc1_src");
    let pt = "SELECT id, grp, amount FROM ucc1_src";
    ucc_create("ucc1_sum", &ucc_sum_sql("ucc1_src"), None, "DEFERRED");
    ucc_create("ucc1_pt", pt, Some("id"), "DEFERRED");
    let check = || {
        ucc_flush("ucc1_src");
        assert_imv_correct("ucc1_sum", &ucc_sum_sql("ucc1_src"));
        assert_imv_correct("ucc1_pt", pt);
    };

    ucc_assert_nothing_captured("ucc1_src", "UPDATE ucc1_src SET note = 'changed'");
    ucc_assert_nothing_captured("ucc1_src", "UPDATE ucc1_src SET note = note, extra = 7");
    ucc_assert_nothing_captured("ucc1_src", "UPDATE ucc1_src SET amount = amount, note = 'x'");
    check();

    ucc_assert_captured("ucc1_src", "UPDATE ucc1_src SET amount = amount + 1");
    check();
    ucc_assert_captured(
        "ucc1_src",
        "UPDATE ucc1_src SET note = 'mixed', grp = grp + 1 WHERE id IN (3, 7, 1500)",
    );
    check();
    ucc_assert_captured(
        "ucc1_src",
        &format!(
            "UPDATE ucc1_src SET note = 'partial', \
             amount = amount + CASE WHEN id > {} THEN 1 ELSE 0 END",
            UCC_ROWS - 3
        ),
    );
    check();
    ucc_assert_captured(
        "ucc1_src",
        "UPDATE ucc1_src SET amount = CASE WHEN id = 1 THEN 2 WHEN id = 2 THEN 1 ELSE amount END, \
         grp = CASE WHEN id = 1 THEN 4 ELSE grp END",
    );
    check();
}

#[pg_test]
fn test_ucc_permutation_of_read_values_is_skipped_only_when_the_multiset_is_unchanged() {
    ucc_source("ucc2_src");
    let by_amount = "SELECT amount, COUNT(*) AS n FROM ucc2_src GROUP BY amount";
    ucc_create("ucc2_by_amount", by_amount, None, "DEFERRED");
    ucc_assert_nothing_captured(
        "ucc2_src",
        "UPDATE ucc2_src SET amount = CASE WHEN id = 1 THEN 2 WHEN id = 2 THEN 1 ELSE amount END",
    );
    assert_imv_correct("ucc2_by_amount", by_amount);
    ucc_assert_captured(
        "ucc2_src",
        "UPDATE ucc2_src SET amount = CASE WHEN id = 1 THEN 2 ELSE amount END",
    );
    ucc_flush("ucc2_src");
    assert_imv_correct("ucc2_by_amount", by_amount);
}

#[pg_test]
fn test_ucc_immediate_imv_on_a_mixed_source_is_not_maintained_for_unread_update() {
    ucc_source("ucc3_src");
    let by_extra = "SELECT extra, COUNT(*) AS n FROM ucc3_src GROUP BY extra";
    let hi = "SELECT grp, MAX(amount) AS hi FROM ucc3_src GROUP BY grp";
    ucc_create("ucc3_imm", by_extra, None, "IMMEDIATE");
    ucc_create("ucc3_def", hi, None, "DEFERRED");
    Spi::run("SET LOCAL track_functions = 'all'").expect("track functions");
    let delta_builds = || {
        Spi::get_one::<i64>(
            "SELECT COALESCE(sum(calls), 0)::bigint FROM pg_stat_xact_user_functions \
             WHERE funcname = 'reflex_build_delta_sql'",
        )
        .expect("function stats")
        .expect("not null")
    };

    let before = delta_builds();
    ucc_assert_nothing_captured("ucc3_src", "UPDATE ucc3_src SET note = 'unread'");
    assert_eq!(
        delta_builds(),
        before,
        "an UPDATE of an unread column must not maintain the IMMEDIATE IMV"
    );

    Spi::run("UPDATE ucc3_src SET extra = id % 3 WHERE id <= 1500").expect("immediate-only column");
    assert_imv_correct("ucc3_imm", by_extra);
    ucc_flush("ucc3_src");
    assert_imv_correct("ucc3_def", hi);

    ucc_assert_captured("ucc3_src", "UPDATE ucc3_src SET amount = amount * 2 WHERE grp = 1");
    assert_imv_correct("ucc3_imm", by_extra);
    ucc_flush("ucc3_src");
    assert_imv_correct("ucc3_def", hi);
}

#[pg_test]
fn test_ucc_column_read_by_one_deferred_consumer_only() {
    ucc_source("ucc4_src");
    let by_extra = "SELECT extra, COUNT(*) AS n FROM ucc4_src GROUP BY extra";
    ucc_create("ucc4_sum", &ucc_sum_sql("ucc4_src"), None, "DEFERRED");
    ucc_create("ucc4_by_extra", by_extra, None, "DEFERRED");
    ucc_assert_nothing_captured("ucc4_src", "UPDATE ucc4_src SET note = 'unread'");
    ucc_assert_captured("ucc4_src", "UPDATE ucc4_src SET extra = id % 4");
    ucc_flush("ucc4_src");
    assert_imv_correct("ucc4_sum", &ucc_sum_sql("ucc4_src"));
    assert_imv_correct("ucc4_by_extra", by_extra);
}

#[pg_test]
fn test_ucc_where_only_column_is_read() {
    Spi::run(
        "CREATE TABLE ucc5_src (id INT PRIMARY KEY, grp INT, amount NUMERIC, status TEXT, note TEXT)",
    )
    .expect("create");
    Spi::run(&format!(
        "INSERT INTO ucc5_src SELECT g, g % 4, g, 'on', 'n' FROM generate_series(1, {UCC_ROWS}) g"
    ))
    .expect("seed");
    let sql = "SELECT grp, SUM(amount) AS total FROM ucc5_src WHERE status = 'on' GROUP BY grp";
    ucc_create("ucc5_sum", sql, None, "DEFERRED");

    ucc_assert_nothing_captured("ucc5_src", "UPDATE ucc5_src SET note = 'x'");
    ucc_assert_captured("ucc5_src", "UPDATE ucc5_src SET status = 'off' WHERE id > 1000");
    ucc_flush("ucc5_src");
    assert_imv_correct("ucc5_sum", sql);
}

fn ucc_rewrite_on_insert(table: &str, rewrite: &str) {
    Spi::run(&format!(
        "CREATE FUNCTION {table}_on_insert() RETURNS trigger LANGUAGE plpgsql AS $$ \
         BEGIN {rewrite} END $$"
    ))
    .expect("trigger function");
    Spi::run(&format!(
        "CREATE TRIGGER {table}_on_insert BEFORE INSERT ON {table} \
         FOR EACH ROW EXECUTE FUNCTION {table}_on_insert()"
    ))
    .expect("before insert trigger");
}

#[pg_test]
fn test_ucc_row_movement_rewritten_by_a_before_insert_trigger() {
    for mode in ["IMMEDIATE", "DEFERRED"] {
        let table = format!("ucc6_{}", mode.to_lowercase());
        ucc_partitioned_source(&table);
        ucc_rewrite_on_insert(&table, "NEW.amount := NEW.amount + 100; RETURN NEW;");
        let view = format!("{table}_sum");
        let sql = format!("SELECT id, SUM(amount) AS total FROM {table} GROUP BY id");
        ucc_create(&view, &sql, None, mode);

        Spi::run(&format!("UPDATE {table} SET region = 'a'")).expect("row movement");
        ucc_flush_if_deferred(&table, mode);
        assert_imv_correct(&view, &sql);
    }
}

#[pg_test]
fn test_ucc_row_movement_whose_insert_a_before_insert_trigger_cancels() {
    for mode in ["IMMEDIATE", "DEFERRED"] {
        let table = format!("ucc7_{}", mode.to_lowercase());
        ucc_partitioned_source(&table);
        ucc_rewrite_on_insert(
            &table,
            "IF NEW.id % 10 = 1 THEN RETURN NULL; END IF; RETURN NEW;",
        );
        let view = format!("{table}_sum");
        let sql = format!("SELECT id, SUM(amount) AS total FROM {table} GROUP BY id");
        ucc_create(&view, &sql, None, mode);

        Spi::run(&format!("UPDATE {table} SET region = 'a'")).expect("row movement");
        assert!(
            Spi::get_one::<i64>(&format!("SELECT count(*) FROM {table}"))
                .expect("count")
                .expect("count not null")
                < UCC_ROWS,
            "fixture: the cancelled inserts must remove rows from {table}"
        );
        ucc_flush_if_deferred(&table, mode);
        assert_imv_correct(&view, &sql);
    }
}

fn ucc_rewrite_on_update(table: &str) {
    Spi::run(&format!(
        "CREATE FUNCTION {table}_on_update() RETURNS trigger LANGUAGE plpgsql AS $$ \
         BEGIN \
           IF NEW.note IS DISTINCT FROM OLD.note THEN NEW.amount := NEW.amount + 100; END IF; \
           RETURN NEW; \
         END $$"
    ))
    .expect("trigger function");
}

#[pg_test]
fn test_ucc_before_update_trigger_rewriting_a_read_column() {
    for mode in ["IMMEDIATE", "DEFERRED"] {
        let table = format!("ucc8_{}", mode.to_lowercase());
        ucc_source(&table);
        let view = format!("{table}_sum");
        let sql = ucc_sum_sql(&table);
        ucc_create(&view, &sql, None, mode);
        ucc_rewrite_on_update(&table);
        Spi::run(&format!(
            "CREATE TRIGGER {table}_on_update BEFORE UPDATE ON {table} \
             FOR EACH ROW EXECUTE FUNCTION {table}_on_update()"
        ))
        .expect("before update trigger");

        Spi::run(&format!("UPDATE {table} SET note = 'rewritten'")).expect("update");
        ucc_flush_if_deferred(&table, mode);
        assert_imv_correct(&view, &sql);
    }
}

#[pg_test]
fn test_ucc_before_update_trigger_on_one_leaf_only() {
    ucc_partitioned_source("ucc9_src");
    let sql = "SELECT region, SUM(amount) AS total FROM ucc9_src GROUP BY region";
    ucc_create("ucc9_sum", sql, None, "DEFERRED");
    ucc_rewrite_on_update("ucc9_src");
    Spi::run(
        "CREATE TRIGGER ucc9_on_update BEFORE UPDATE ON ucc9_src_b \
         FOR EACH ROW EXECUTE FUNCTION ucc9_src_on_update()",
    )
    .expect("leaf-only before update trigger");
    Spi::run("UPDATE ucc9_src SET note = 'x'").expect("unread-column update");
    ucc_flush("ucc9_src");
    assert_imv_correct("ucc9_sum", sql);
}

#[pg_test]
fn test_ucc_generated_column_read_through_its_input() {
    Spi::run(
        "CREATE TABLE ucc10_src (id INT PRIMARY KEY, grp INT, amount NUMERIC, note TEXT, \
         doubled NUMERIC GENERATED ALWAYS AS (amount * 2) STORED)",
    )
    .expect("create");
    Spi::run(&format!(
        "INSERT INTO ucc10_src (id, grp, amount, note) \
         SELECT g, g % 3, g, 'n' FROM generate_series(1, {UCC_ROWS}) g"
    ))
    .expect("seed");
    let sql = "SELECT grp, SUM(doubled) AS total FROM ucc10_src GROUP BY grp";
    ucc_create("ucc10_sum", sql, None, "DEFERRED");

    ucc_assert_nothing_captured("ucc10_src", "UPDATE ucc10_src SET note = 'x'");
    ucc_assert_captured("ucc10_src", "UPDATE ucc10_src SET amount = amount + 1");
    ucc_flush("ucc10_src");
    assert_imv_correct("ucc10_sum", sql);
}

#[pg_test]
fn test_ucc_one_statement_writing_the_source_several_ways() {
    ucc_source("ucc11_src");
    let pt = "SELECT id, grp, amount FROM ucc11_src";
    ucc_create("ucc11_sum", &ucc_sum_sql("ucc11_src"), None, "DEFERRED");
    ucc_create("ucc11_pt", pt, Some("id"), "DEFERRED");
    let check = || {
        ucc_flush("ucc11_src");
        assert_imv_correct("ucc11_sum", &ucc_sum_sql("ucc11_src"));
        assert_imv_correct("ucc11_pt", pt);
    };

    ucc_assert_nothing_captured(
        "ucc11_src",
        "WITH a AS (UPDATE ucc11_src SET note = 'cte' WHERE id <= 1000 RETURNING 1), \
              b AS (UPDATE ucc11_src SET extra = 5 WHERE id > 1000 RETURNING 1) \
         SELECT (SELECT count(*) FROM a) + (SELECT count(*) FROM b)",
    );
    ucc_assert_captured(
        "ucc11_src",
        "WITH a AS (UPDATE ucc11_src SET note = 'cte' WHERE id <= 1000 RETURNING 1), \
              b AS (UPDATE ucc11_src SET amount = amount + 50 WHERE id = 1999 RETURNING 1) \
         SELECT (SELECT count(*) FROM a) + (SELECT count(*) FROM b)",
    );
    check();

    ucc_assert_nothing_captured(
        "ucc11_src",
        &format!(
            "INSERT INTO ucc11_src (id, grp, amount, note) \
             SELECT g, g % 5, g, 'upsert' FROM generate_series(1, {UCC_ROWS}) g \
             ON CONFLICT (id) DO UPDATE SET note = EXCLUDED.note"
        ),
    );
    ucc_assert_captured(
        "ucc11_src",
        &format!(
            "INSERT INTO ucc11_src (id, grp, amount, note) \
             SELECT g, g % 5, g + CASE WHEN g = {UCC_ROWS} THEN 77 ELSE 0 END, 'upsert' \
             FROM generate_series(1, {UCC_ROWS}) g \
             ON CONFLICT (id) DO UPDATE SET amount = EXCLUDED.amount"
        ),
    );
    check();

    ucc_assert_nothing_captured(
        "ucc11_src",
        "MERGE INTO ucc11_src t USING (SELECT g AS id FROM generate_series(1, 2000) g) v \
         ON t.id = v.id WHEN MATCHED THEN UPDATE SET note = 'm'",
    );
    ucc_assert_captured(
        "ucc11_src",
        "MERGE INTO ucc11_src t USING (SELECT g AS id FROM generate_series(1, 2000) g) v \
         ON t.id = v.id \
         WHEN MATCHED AND v.id < 1990 THEN UPDATE SET note = 'm' \
         WHEN MATCHED THEN UPDATE SET amount = t.amount + 9",
    );
    check();
}

#[pg_test]
fn test_ucc_leaf_write_of_a_partitioned_source() {
    ucc_partitioned_source("ucc12_src");
    let sql = "SELECT region, SUM(amount) AS total, COUNT(*) AS n FROM ucc12_src GROUP BY region";
    ucc_create("ucc12_sum", sql, None, "DEFERRED");

    ucc_assert_nothing_captured("ucc12_src", "UPDATE ucc12_src SET note = 'root'");
    ucc_assert_nothing_captured("ucc12_src", "UPDATE ucc12_src_a SET note = 'leaf'");
    ucc_assert_captured("ucc12_src", "UPDATE ucc12_src_b SET amount = amount + 2");
    ucc_flush("ucc12_src");
    assert_imv_correct("ucc12_sum", sql);
    ucc_assert_captured(
        "ucc12_src",
        "UPDATE ucc12_src_a SET amount = amount + CASE WHEN id = 2000 THEN 1 ELSE 0 END",
    );
    ucc_flush("ucc12_src");
    assert_imv_correct("ucc12_sum", sql);
}

#[pg_test]
fn test_ucc_imv_created_later_reads_a_previously_unread_column() {
    ucc_source("ucc13_src");
    ucc_create("ucc13_sum", &ucc_sum_sql("ucc13_src"), None, "DEFERRED");
    ucc_assert_nothing_captured("ucc13_src", "UPDATE ucc13_src SET note = 'unread so far'");

    let by_note = "SELECT note, COUNT(*) AS n FROM ucc13_src GROUP BY note";
    ucc_create("ucc13_by_note", by_note, None, "IMMEDIATE");
    Spi::run("UPDATE ucc13_src SET note = 'read now' WHERE id > 1000").expect("now-read update");
    assert_imv_correct("ucc13_by_note", by_note);

    let by_extra = "SELECT extra, COUNT(*) AS n FROM ucc13_src GROUP BY extra";
    ucc_create("ucc13_by_extra", by_extra, None, "DEFERRED");
    ucc_assert_captured("ucc13_src", "UPDATE ucc13_src SET extra = id % 2");
    ucc_flush("ucc13_src");
    assert_imv_correct("ucc13_sum", &ucc_sum_sql("ucc13_src"));
    assert_imv_correct("ucc13_by_note", by_note);
    assert_imv_correct("ucc13_by_extra", by_extra);

    Spi::run("SELECT drop_reflex_ivm('ucc13_by_note')").expect("drop");
    ucc_assert_nothing_captured("ucc13_src", "UPDATE ucc13_src SET note = 'unread again'");
}

#[pg_test]
fn test_ucc_source_read_twice_is_always_maintained() {
    ucc_source("ucc14_src");
    let sql = "SELECT e.id, m.note AS manager_note FROM ucc14_src e \
               LEFT JOIN ucc14_src m ON m.id = e.grp";
    ucc_create("ucc14_pairs", sql, Some("id"), "DEFERRED");
    ucc_create("ucc14_sum", &ucc_sum_sql("ucc14_src"), None, "DEFERRED");
    ucc_assert_captured("ucc14_src", "UPDATE ucc14_src SET note = 'renamed'");
    ucc_flush("ucc14_src");
    assert_imv_correct("ucc14_pairs", sql);
    assert_imv_correct("ucc14_sum", &ucc_sum_sql("ucc14_src"));
}

#[pg_test]
fn test_ucc_json_read_column() {
    Spi::run("CREATE TABLE ucc15_src (id INT PRIMARY KEY, doc JSON, note TEXT)").expect("create");
    Spi::run(&format!(
        "INSERT INTO ucc15_src SELECT g, json_build_object('g', g), 'n' \
         FROM generate_series(1, {UCC_ROWS}) g"
    ))
    .expect("seed");
    let sql = "SELECT id, doc::text AS doc FROM ucc15_src";
    ucc_create("ucc15_pt", sql, Some("id"), "DEFERRED");
    ucc_assert_nothing_captured("ucc15_src", "UPDATE ucc15_src SET note = 'x'");
    ucc_assert_captured(
        "ucc15_src",
        "UPDATE ucc15_src SET doc = CASE WHEN id = 1999 THEN '{\"g\": 0}'::json ELSE doc END",
    );
    ucc_flush("ucc15_src");
    assert_imv_correct("ucc15_pt", sql);
}

/// The real COMMIT-time flush, in a private database: unread statements stage
/// nothing, and the read ones around them are maintained at COMMIT.
#[pg_test]
fn test_ucc_transaction_mixing_unread_and_read_updates_commits_correctly() {
    probe_db_open("ucc_commit_db");
    worker_exec(&format!(
        "CREATE TABLE ucc_c (id INT PRIMARY KEY, grp INT, amount NUMERIC, note TEXT); \
         INSERT INTO ucc_c SELECT g, g % 5, g, 'n' FROM generate_series(1, {UCC_ROWS}) g"
    ));
    let sum = "SELECT grp, SUM(amount) AS total, COUNT(*) AS n FROM ucc_c GROUP BY grp";
    let pt = "SELECT id, grp, amount FROM ucc_c";
    for (name, sql, key) in [("ucc_c_sum", sum, "NULL"), ("ucc_c_pt", pt, "'id'")] {
        let created = worker_scalar_i64(&format!(
            "SELECT (create_reflex_ivm({}, {}, {key}, NULL, 'DEFERRED') \
             = 'CREATE REFLEX INCREMENTAL VIEW')::int::int8",
            sql_lit(name),
            sql_lit(sql)
        ));
        assert_eq!(created, 1, "create {name}");
    }
    worker_exec(
        "BEGIN; \
         UPDATE ucc_c SET note = 'unread 1'; \
         INSERT INTO ucc_c VALUES (5001, 1, 10, 'new'); \
         UPDATE ucc_c SET note = 'unread 2'; \
         UPDATE ucc_c SET amount = amount + 1 WHERE id IN (1, 5001); \
         UPDATE ucc_c SET note = 'unread 3'; \
         DELETE FROM ucc_c WHERE id = 2; \
         COMMIT",
    );
    let delta = crate::query_decomposer::staging_delta_table_name("ucc_c");
    let mismatches = |imv: &str, fresh: &str| {
        worker_scalar_i64(&format!(
            "SELECT count(*)::int8 FROM ((SELECT * FROM {imv} EXCEPT ALL ({fresh})) \
             UNION ALL (({fresh}) EXCEPT ALL SELECT * FROM {imv})) d"
        ))
    };
    let committed = (
        mismatches("ucc_c_sum", sum),
        mismatches("ucc_c_pt", pt),
        worker_scalar_i64(&format!("SELECT count(*)::int8 FROM {delta}")),
    );
    worker_exec("BEGIN; UPDATE ucc_c SET note = 'unread alone'");
    let staged_by_unread = worker_scalar_i64(&format!("SELECT count(*)::int8 FROM {delta}"))
        + worker_scalar_i64("SELECT count(*)::int8 FROM public.__reflex_deferred_pending");
    worker_exec("COMMIT");
    probe_db_close("ucc_commit_db");
    assert_eq!(committed, (0, 0, 0), "(sum mismatches, mirror mismatches, staged rows) after COMMIT");
    assert_eq!(staged_by_unread, 0, "an unread UPDATE must stage nothing before COMMIT");
}

/// The second hop of the alp chain: the mirror's refresh UPDATEs its rows on every
/// non-key column, but the reader of the mirror reads only plan, date and year,
/// which the refresh leaves unchanged. The mirror's UPDATE stages nothing for it.
#[pg_test]
fn test_ucc_mirror_refresh_stages_nothing_for_a_reader_of_unchanged_columns() {
    Spi::run(
        "CREATE TABLE ucc16_src (id INT, plan INT, day DATE, year INT, qty INT, \
         PRIMARY KEY (id, plan)) PARTITION BY LIST (plan)",
    )
    .expect("source root");
    for plan in [1, 2] {
        Spi::run(&format!(
            "CREATE TABLE ucc16_src_{plan} PARTITION OF ucc16_src FOR VALUES IN ({plan})"
        ))
        .expect("source leaf");
    }
    Spi::run(
        "INSERT INTO ucc16_src SELECT g, 1 + g % 2, DATE '2026-01-01' + g % 700, \
         EXTRACT(YEAR FROM DATE '2026-01-01' + g % 700)::int, g % 30 \
         FROM generate_series(1, 6000) g",
    )
    .expect("seed");
    let mirror = "SELECT id, plan, day, year, qty AS quantity, qty * 2 AS turnover FROM ucc16_src";
    let reader = "SELECT plan, year, MIN(day) AS lo, MAX(day) AS hi FROM ucc16_mirror GROUP BY plan, year";
    let reader_fresh = "SELECT plan, year, MIN(day) AS lo, MAX(day) AS hi \
                        FROM (SELECT id, plan, day, year FROM ucc16_src) m GROUP BY plan, year";
    assert_eq!(
        Spi::get_one::<String>(&format!(
            "SELECT create_reflex_ivm('ucc16_mirror', {}, 'id, plan', 'UNLOGGED', 'DEFERRED', \
             NULL, ARRAY['plan'])",
            sql_lit(mirror)
        ))
        .expect("create mirror")
        .as_deref(),
        Some("CREATE REFLEX INCREMENTAL VIEW")
    );
    ucc_create("ucc16_reader", reader, Some("plan, year"), "DEFERRED");

    Spi::run("UPDATE ucc16_src SET qty = qty + 1 WHERE plan = 1").expect("read-by-mirror update");
    ucc_flush("ucc16_src");
    assert_imv_correct("ucc16_mirror", mirror);
    assert_eq!(
        (ucc_staged_rows("ucc16_mirror"), ucc_pending_flushes("ucc16_mirror")),
        (0, 0),
        "the mirror's refresh changes no column its reader reads, yet it was staged"
    );
    ucc_flush("ucc16_mirror");
    assert_imv_correct("ucc16_reader", reader_fresh);

    Spi::run("UPDATE ucc16_src SET day = day + 1, qty = qty + 1 WHERE plan = 2")
        .expect("date-moving update");
    ucc_flush("ucc16_src");
    assert_imv_correct("ucc16_mirror", mirror);
    ucc_flush("ucc16_mirror");
    assert_imv_correct("ucc16_reader", reader_fresh);
}

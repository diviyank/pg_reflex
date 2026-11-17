// Step 0 of the exact joint-term correction: the per-transaction write counts it
// reads grow by exactly one per row written, for every kind of write, and follow
// subtransaction commit and abort. A TRUNCATE, or statistics not counted, make
// them unknown.

#[pg_extern]
fn tsc_writes(rel: pg_sys::Oid) -> Option<i64> {
    crate::trigger::delta_sources::transaction_writes(rel)
}

fn tsc_w(rel: &str) -> Option<i64> {
    Spi::get_one::<i64>(&format!("SELECT tests.tsc_writes('{rel}'::regclass::oid)"))
        .expect("tsc_writes")
}

fn tsc_grows_by(rels: &[&str], statement: &str, expected: i64) {
    let before: Vec<Option<i64>> = rels.iter().map(|r| tsc_w(r)).collect();
    Spi::run(statement).unwrap_or_else(|e| panic!("<{statement}>: {e}"));
    let grown: i64 = rels
        .iter()
        .zip(before)
        .map(|(r, b)| tsc_w(r).expect("counted") - b.expect("counted"))
        .sum();
    assert_eq!(grown, expected, "writes counted for <{statement}> on {rels:?}");
}

#[pg_test]
fn pg_two_source_counters_one_per_row() {
    for ddl in [
        "CREATE TABLE tsc_t (k INT PRIMARY KEY, v INT, pad TEXT) WITH (fillfactor = 50)",
        "INSERT INTO tsc_t SELECT g, g, 'x' FROM generate_series(1, 20) g",
        "CREATE TABLE tsc_skip (k INT)",
        "CREATE FUNCTION tsc_skip_row() RETURNS trigger LANGUAGE plpgsql AS \
         $$ BEGIN RETURN NULL; END $$",
        "CREATE TRIGGER tsc_skip BEFORE INSERT ON tsc_skip FOR EACH ROW EXECUTE FUNCTION tsc_skip_row()",
        "CREATE TABLE tsc_parent (k INT PRIMARY KEY)",
        "CREATE TABLE tsc_child (id INT PRIMARY KEY, \
           k INT REFERENCES tsc_parent ON DELETE CASCADE ON UPDATE CASCADE)",
        "CREATE TABLE tsc_nulled (id INT PRIMARY KEY, \
           k INT REFERENCES tsc_parent ON DELETE SET NULL ON UPDATE SET NULL)",
        "INSERT INTO tsc_parent SELECT g FROM generate_series(1, 4) g",
        "INSERT INTO tsc_child SELECT g, (g % 4) + 1 FROM generate_series(1, 12) g",
        "INSERT INTO tsc_nulled SELECT g, (g % 4) + 1 FROM generate_series(1, 12) g",
        "CREATE TABLE tsc_p (k INT NOT NULL, v INT) PARTITION BY LIST (k)",
        "CREATE TABLE tsc_p1 PARTITION OF tsc_p FOR VALUES IN (1)",
        "CREATE TABLE tsc_p2 PARTITION OF tsc_p FOR VALUES IN (2)",
        "INSERT INTO tsc_p SELECT 1, g FROM generate_series(1, 5) g",
    ] {
        Spi::run(ddl).expect(ddl);
    }
    let t = &["tsc_t"];
    tsc_grows_by(t, "INSERT INTO tsc_t SELECT g, g, 'y' FROM generate_series(101, 105) g", 5);
    let hot_before = Spi::get_one::<i64>(
        "SELECT pg_stat_get_xact_tuples_hot_updated('tsc_t'::regclass)",
    )
    .expect("hot")
    .expect("hot");
    tsc_grows_by(t, "UPDATE tsc_t SET pad = 'z' WHERE k <= 3", 3);
    let hot_after = Spi::get_one::<i64>(
        "SELECT pg_stat_get_xact_tuples_hot_updated('tsc_t'::regclass)",
    )
    .expect("hot")
    .expect("hot");
    assert_eq!(hot_after - hot_before, 3, "fixture: the update is HOT");
    tsc_grows_by(t, "UPDATE tsc_t SET k = k + 1000 WHERE k IN (4, 5)", 2);
    tsc_grows_by(t, "DELETE FROM tsc_t WHERE k IN (6, 7)", 2);
    tsc_grows_by(
        t,
        "MERGE INTO tsc_t t USING (VALUES (8, 0), (9, 1), (500, 1)) s(k, d) ON t.k = s.k \
         WHEN MATCHED AND s.d = 0 THEN DELETE \
         WHEN MATCHED THEN UPDATE SET v = v + 1 \
         WHEN NOT MATCHED THEN INSERT VALUES (s.k, 0, 'm')",
        3,
    );
    tsc_grows_by(
        t,
        "INSERT INTO tsc_t VALUES (10, 0, 'c'), (600, 0, 'c') \
         ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v",
        2,
    );
    tsc_grows_by(
        t,
        "COPY tsc_t FROM PROGRAM 'printf \"900,1,a\\n901,2,b\\n902,3,c\\n\"' WITH (FORMAT csv)",
        3,
    );
    tsc_grows_by(&["tsc_skip"], "INSERT INTO tsc_skip SELECT generate_series(1, 3)", 0);
    tsc_grows_by(&["tsc_child"], "DELETE FROM tsc_parent WHERE k = 1", 3);
    tsc_grows_by(&["tsc_nulled"], "UPDATE tsc_parent SET k = 20 WHERE k = 2", 3);
    tsc_grows_by(&["tsc_child"], "UPDATE tsc_parent SET k = 30 WHERE k = 3", 3);
    tsc_grows_by(&["tsc_nulled"], "DELETE FROM tsc_parent WHERE k = 4", 3);
    tsc_grows_by(&["tsc_p1", "tsc_p2"], "INSERT INTO tsc_p VALUES (2, 1), (2, 2)", 2);
    tsc_grows_by(
        &["tsc_p1", "tsc_p2"],
        "UPDATE tsc_p SET k = 2 WHERE k = 1 AND v <= 2",
        4,
    );

    let before = tsc_w("tsc_t").expect("counted");
    Spi::run(
        "DO $$ BEGIN \
           BEGIN \
             INSERT INTO tsc_t SELECT g, 0, 's' FROM generate_series(2001, 2004) g; \
             RAISE EXCEPTION '%', tests.tsc_writes('tsc_t'::regclass::oid); \
           EXCEPTION WHEN raise_exception THEN \
             PERFORM set_config('tsc.inside', SQLERRM, true); \
           END; \
         END $$",
    )
    .expect("subtransaction");
    let inside: i64 = Spi::get_one::<String>("SELECT current_setting('tsc.inside')")
        .expect("inside")
        .expect("inside")
        .parse()
        .expect("number");
    assert_eq!(inside, before + 4, "a live subtransaction's writes are counted");
    assert_eq!(
        tsc_w("tsc_t"),
        Some(before),
        "an aborted subtransaction's writes are no longer counted"
    );
    tsc_grows_by(
        t,
        "DO $$ BEGIN \
           BEGIN INSERT INTO tsc_t SELECT g, 0, 's' FROM generate_series(3001, 3002) g; \
           EXCEPTION WHEN others THEN NULL; END; \
         END $$",
        2,
    );

    Spi::run("TRUNCATE tsc_skip").expect("truncate");
    assert_eq!(tsc_w("tsc_skip"), None, "a TRUNCATE leaves the counts unknown");
    Spi::run("SET LOCAL track_counts = off").expect("track_counts");
    assert_eq!(tsc_w("tsc_t"), None, "uncounted statistics are unknown");
    Spi::run("SET LOCAL track_counts = on").expect("track_counts");
}

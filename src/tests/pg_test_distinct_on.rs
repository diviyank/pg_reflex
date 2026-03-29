
#[pg_test]
fn test_distinct_on_basic() {
    Spi::run("CREATE TABLE don_basic (city TEXT, name TEXT, val INT)").expect("create");
    Spi::run("INSERT INTO don_basic VALUES
        ('A', 'alice', 10), ('A', 'bob', 20),
        ('B', 'carol', 30), ('B', 'dave', 5)").expect("seed");

    let result = crate::create_reflex_ivm("don_basic_v",
        "SELECT DISTINCT ON (city) city, name, val FROM don_basic ORDER BY city, val DESC",
        None, None, None);
    assert!(!result.starts_with("ERROR"), "Should succeed: {}", result);

    // A: bob (val=20 wins DESC), B: carol (val=30 wins DESC)
    let a_name = Spi::get_one::<&str>("SELECT name FROM don_basic_v WHERE city = 'A'")
        .expect("query").expect("null");
    assert_eq!(a_name, "bob");
    let b_name = Spi::get_one::<&str>("SELECT name FROM don_basic_v WHERE city = 'B'")
        .expect("query").expect("null");
    assert_eq!(b_name, "carol");

    crate::drop_reflex_ivm("don_basic_v");
}

#[pg_test]
fn test_distinct_on_insert_reranks() {
    Spi::run("CREATE TABLE don_ins (city TEXT, name TEXT, val INT)").expect("create");
    Spi::run("INSERT INTO don_ins VALUES ('A', 'alice', 10), ('A', 'bob', 20)").expect("seed");

    let result = crate::create_reflex_ivm("don_ins_v",
        "SELECT DISTINCT ON (city) city, name, val FROM don_ins ORDER BY city, val DESC",
        None, None, None);
    assert!(!result.starts_with("ERROR"), "Should succeed: {}", result);

    // Initially: bob wins (val=20)
    let name = Spi::get_one::<&str>("SELECT name FROM don_ins_v WHERE city = 'A'")
        .expect("query").expect("null");
    assert_eq!(name, "bob");

    // Insert new row with higher val → should take over
    Spi::run("INSERT INTO don_ins VALUES ('A', 'charlie', 99)").expect("insert");
    let name = Spi::get_one::<&str>("SELECT name FROM don_ins_v WHERE city = 'A'")
        .expect("query").expect("null");
    assert_eq!(name, "charlie", "charlie with val=99 should win");

    crate::drop_reflex_ivm("don_ins_v");
}

#[pg_test]
fn test_distinct_on_delete_first() {
    Spi::run("CREATE TABLE don_del (city TEXT, name TEXT, val INT)").expect("create");
    Spi::run("INSERT INTO don_del VALUES ('A', 'alice', 10), ('A', 'bob', 20), ('A', 'carol', 15)").expect("seed");

    let result = crate::create_reflex_ivm("don_del_v",
        "SELECT DISTINCT ON (city) city, name, val FROM don_del ORDER BY city, val DESC",
        None, None, None);
    assert!(!result.starts_with("ERROR"), "Should succeed: {}", result);

    // Initially: bob wins (val=20)
    let name = Spi::get_one::<&str>("SELECT name FROM don_del_v WHERE city = 'A'")
        .expect("query").expect("null");
    assert_eq!(name, "bob");

    // Delete bob → carol (val=15) should take over
    Spi::run("DELETE FROM don_del WHERE name = 'bob'").expect("delete");
    let name = Spi::get_one::<&str>("SELECT name FROM don_del_v WHERE city = 'A'")
        .expect("query").expect("null");
    assert_eq!(name, "carol", "carol with val=15 should be next");

    crate::drop_reflex_ivm("don_del_v");
}

#[pg_test]
fn test_distinct_on_update_reranks() {
    Spi::run("CREATE TABLE don_upd (id SERIAL PRIMARY KEY, city TEXT, name TEXT, val INT)").expect("create");
    Spi::run("INSERT INTO don_upd (city, name, val) VALUES ('A', 'alice', 10), ('A', 'bob', 20)").expect("seed");

    let result = crate::create_reflex_ivm("don_upd_v",
        "SELECT DISTINCT ON (city) city, name, val FROM don_upd ORDER BY city, val DESC",
        None, None, None);
    assert!(!result.starts_with("ERROR"), "Should succeed: {}", result);

    // Initially: bob wins (val=20)
    let name = Spi::get_one::<&str>("SELECT name FROM don_upd_v WHERE city = 'A'")
        .expect("query").expect("null");
    assert_eq!(name, "bob");

    // Update alice's val to 50 → alice should win
    Spi::run("UPDATE don_upd SET val = 50 WHERE name = 'alice'").expect("update");
    let name = Spi::get_one::<&str>("SELECT name FROM don_upd_v WHERE city = 'A'")
        .expect("query").expect("null");
    assert_eq!(name, "alice", "alice with val=50 should now win");

    crate::drop_reflex_ivm("don_upd_v");
}

#[pg_test]
fn test_distinct_on_multi_column() {
    Spi::run("CREATE TABLE don_multi (city TEXT, dept TEXT, name TEXT, val INT)").expect("create");
    Spi::run("INSERT INTO don_multi VALUES
        ('A', 'eng', 'alice', 10), ('A', 'eng', 'bob', 20),
        ('A', 'sales', 'carol', 30), ('B', 'eng', 'dave', 5)").expect("seed");

    let result = crate::create_reflex_ivm("don_multi_v",
        "SELECT DISTINCT ON (city, dept) city, dept, name, val FROM don_multi ORDER BY city, dept, val DESC",
        None, None, None);
    assert!(!result.starts_with("ERROR"), "Should succeed: {}", result);

    let cnt = Spi::get_one::<i64>("SELECT COUNT(*)::BIGINT FROM don_multi_v")
        .expect("query").expect("null");
    assert_eq!(cnt, 3, "Should have 3 groups: (A,eng), (A,sales), (B,eng)");

    let a_eng = Spi::get_one::<&str>("SELECT name FROM don_multi_v WHERE city = 'A' AND dept = 'eng'")
        .expect("query").expect("null");
    assert_eq!(a_eng, "bob", "bob (val=20) should win in A/eng");

    crate::drop_reflex_ivm("don_multi_v");
}

#[pg_test]
fn test_distinct_on_with_where() {
    Spi::run("CREATE TABLE don_where (city TEXT, name TEXT, val INT, active BOOLEAN)").expect("create");
    Spi::run("INSERT INTO don_where VALUES
        ('A', 'alice', 50, false), ('A', 'bob', 20, true),
        ('A', 'carol', 30, true), ('B', 'dave', 10, true)").expect("seed");

    let result = crate::create_reflex_ivm("don_where_v",
        "SELECT DISTINCT ON (city) city, name, val FROM don_where WHERE active ORDER BY city, val DESC",
        None, None, None);
    assert!(!result.starts_with("ERROR"), "Should succeed: {}", result);

    // A: carol (val=30, active) wins over bob (val=20, active). alice excluded (inactive).
    let a_name = Spi::get_one::<&str>("SELECT name FROM don_where_v WHERE city = 'A'")
        .expect("query").expect("null");
    assert_eq!(a_name, "carol");

    crate::drop_reflex_ivm("don_where_v");
}

#[pg_test]
fn test_distinct_on_with_join() {
    Spi::run("CREATE TABLE don_j1 (id INT PRIMARY KEY, city TEXT)").expect("create");
    Spi::run("CREATE TABLE don_j2 (id INT, name TEXT, val INT)").expect("create");
    Spi::run("INSERT INTO don_j1 VALUES (1, 'A'), (2, 'B')").expect("seed1");
    Spi::run("INSERT INTO don_j2 VALUES (1, 'alice', 10), (1, 'bob', 20), (2, 'carol', 30)").expect("seed2");

    let result = crate::create_reflex_ivm("don_j_v",
        "SELECT DISTINCT ON (j1.city) j1.city, j2.name, j2.val \
         FROM don_j1 j1 JOIN don_j2 j2 ON j1.id = j2.id ORDER BY j1.city, j2.val DESC",
        None, None, None);
    assert!(!result.starts_with("ERROR"), "Should succeed: {}", result);

    let a_name = Spi::get_one::<&str>("SELECT name FROM don_j_v WHERE city = 'A'")
        .expect("query").expect("null");
    assert_eq!(a_name, "bob", "bob (val=20) should win in A");

    crate::drop_reflex_ivm("don_j_v");
}

#[pg_test]
fn test_distinct_on_truncate_reinsert() {
    Spi::run("CREATE TABLE don_trunc (city TEXT, name TEXT, val INT)").expect("create");
    Spi::run("INSERT INTO don_trunc VALUES ('A', 'alice', 10), ('A', 'bob', 20)").expect("seed");

    let result = crate::create_reflex_ivm("don_trunc_v",
        "SELECT DISTINCT ON (city) city, name, val FROM don_trunc ORDER BY city, val DESC",
        None, None, None);
    assert!(!result.starts_with("ERROR"), "Should succeed: {}", result);

    // Truncate and re-insert different data
    Spi::run("TRUNCATE don_trunc").expect("truncate");
    Spi::run("INSERT INTO don_trunc VALUES ('A', 'zara', 99)").expect("reinsert");

    let name = Spi::get_one::<&str>("SELECT name FROM don_trunc_v WHERE city = 'A'")
        .expect("query").expect("null");
    assert_eq!(name, "zara");

    crate::drop_reflex_ivm("don_trunc_v");
}

#[pg_test]
fn test_distinct_on_correctness_oracle() {
    Spi::run("CREATE TABLE don_oracle (city TEXT, name TEXT, val INT)").expect("create");
    Spi::run("INSERT INTO don_oracle VALUES
        ('A', 'alice', 10), ('A', 'bob', 20), ('A', 'carol', 15),
        ('B', 'dave', 30), ('B', 'eve', 25)").expect("seed");

    let result = crate::create_reflex_ivm("don_oracle_v",
        "SELECT DISTINCT ON (city) city, name, val FROM don_oracle ORDER BY city, val DESC",
        None, None, None);
    assert!(!result.starts_with("ERROR"), "Should succeed: {}", result);

    // Perform mutations
    Spi::run("INSERT INTO don_oracle VALUES ('A', 'frank', 99), ('C', 'grace', 50)").expect("ins");
    Spi::run("DELETE FROM don_oracle WHERE name = 'dave'").expect("del");
    Spi::run("UPDATE don_oracle SET val = 1 WHERE name = 'bob'").expect("upd");

    // Oracle check
    let fresh_sql = "SELECT DISTINCT ON (city) city, name, val FROM don_oracle ORDER BY city, val DESC";
    assert_imv_correct("don_oracle_v", fresh_sql);

    crate::drop_reflex_ivm("don_oracle_v");
}

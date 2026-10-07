fn v112_kw_table(table: &str) {
    Spi::run(&format!(
        "CREATE TABLE {table}(id SERIAL PRIMARY KEY, g TEXT, city TEXT, v INT)"
    ))
    .expect("create");
    Spi::run(&format!(
        "INSERT INTO {table}(g, city, v) \
         SELECT 'g' || (i % 3), CASE WHEN i % 2 = 0 THEN 'Lyon' ELSE 'Paris' END, i \
         FROM generate_series(1, 60) i"
    ))
    .expect("seed");
}

/// Retract the extremes so a MIN/MAX IMV with topk=0 must recompute, then add and move rows.
fn v112_kw_dml(table: &str) {
    Spi::run(&format!("DELETE FROM {table} WHERE v IN (1, 2, 3, 58, 59, 60)")).expect("delete");
    Spi::run(&format!("INSERT INTO {table}(g, city, v) VALUES ('g0','Lyon',0),('g9','Paris',99)"))
        .expect("insert");
    Spi::run(&format!("UPDATE {table} SET v = v + 200 WHERE v IN (4, 57)")).expect("update");
}

fn v112_kw_roundtrip(tag: &str, select_tail: &str) {
    let table = format!("v112_kw_{tag}_src");
    let imv = format!("v112_kw_{tag}");
    v112_kw_table(&table);
    let q = select_tail.replace("{t}", &table);
    assert_eq!(
        crate::create_reflex_ivm_with_topk(&imv, &q, None, None, None, 0, None),
        "CREATE REFLEX INCREMENTAL VIEW",
        "{q}"
    );
    v112_kw_dml(&table);
    assert_imv_correct(&imv, &q);
}

#[pg_test]
fn v112_kw_group_by_literal_in_having() {
    v112_kw_roundtrip(
        "b1",
        "SELECT g, MAX(v) AS mx FROM {t} WHERE city <> 'x' GROUP BY g HAVING MAX(v) <> length(' group by ')",
    );
}

#[pg_test]
fn v112_kw_where_literal_without_where_clause() {
    v112_kw_roundtrip(
        "b2",
        "SELECT g, MIN(CASE WHEN city <> 'a where b' THEN v END) AS mn FROM {t} GROUP BY g",
    );
}

#[pg_test]
fn v112_kw_group_by_literal_in_where() {
    v112_kw_roundtrip(
        "w",
        "SELECT g, MIN(v) AS mn, MAX(v) AS mx FROM {t} WHERE city <> 'x group by y' GROUP BY g",
    );
}

#[pg_test]
fn v112_kw_literal_in_aggregate_argument() {
    v112_kw_roundtrip(
        "arg",
        "SELECT g, MAX(CASE WHEN city <> ' group by ' THEN v END) AS mx FROM {t} WHERE v > -1 GROUP BY g",
    );
}

#[pg_test]
fn v112_kw_dollar_quoted_literal() {
    v112_kw_roundtrip(
        "dq",
        "SELECT g, MAX(v) AS mx FROM {t} WHERE city <> $$ group by $$ GROUP BY g",
    );
}

#[pg_test]
fn v112_kw_multibyte_literal_before_group_by() {
    v112_kw_roundtrip(
        "mb",
        "SELECT g, MIN(v) AS mn FROM {t} WHERE city <> 'ıııııııııııı where ıııı' GROUP BY g",
    );
}

#[pg_test]
fn v112_kw_literal_in_join_condition() {
    Spi::run("CREATE TABLE v112_kw_j_dim(g TEXT PRIMARY KEY, label TEXT)").expect("dim");
    Spi::run("INSERT INTO v112_kw_j_dim VALUES ('g0','zero'),('g1','one'),('g2','two'),('g9','nine')")
        .expect("dim seed");
    v112_kw_roundtrip(
        "j",
        "SELECT d.label, MAX(s.v) AS mx FROM {t} s JOIN v112_kw_j_dim d \
         ON d.g = s.g AND d.label <> ' where ' GROUP BY d.label",
    );
}

/// Hypothesis B3 from the spec: outer query has no WHERE, a FROM subquery does.
#[pg_test]
fn v112_kw_where_only_inside_from_subquery() {
    v112_kw_roundtrip(
        "b3",
        "SELECT q.g, MIN(q.v) AS mn FROM (SELECT g, v FROM {t} WHERE v > -1) q GROUP BY q.g",
    );
}

#[pg_test]
fn v112_kw_quoted_identifier_named_group_by() {
    v112_kw_roundtrip(
        "qi",
        "SELECT g, MAX(v) AS \"x group by y\" FROM {t} GROUP BY g",
    );
}

#[pg_test]
fn v112_kw_literal_shorter_once_upper_cased() {
    v112_kw_roundtrip(
        "sh",
        "SELECT g, MIN(v) AS mn FROM {t} WHERE city <> 'ıı' GROUP BY g",
    );
}

/// A one-row filter flip on the joined dimension, among enough groups to stay
/// off the bulk fan-out path, is maintained as an insert with no target DELETE.
#[pg_test]
fn v112_kw_unspaced_group_by_literal_on_promoted_insert() {
    Spi::run(
        "CREATE TABLE v112_kw_np_fact (id INT PRIMARY KEY, dim_id INT, val BIGINT); \
         CREATE TABLE v112_kw_np_dim (id INT PRIMARY KEY, status TEXT);",
    )
    .expect("create");
    Spi::run(
        "INSERT INTO v112_kw_np_dim \
         SELECT i, CASE WHEN i = 40 THEN 'off' ELSE 'on' END FROM generate_series(1, 40) i; \
         INSERT INTO v112_kw_np_fact SELECT i, 1 + i % 40, 100 + i FROM generate_series(1, 120) i;",
    )
    .expect("seed");
    let q = "SELECT dim_id, SUM(val) AS s FROM v112_kw_np_fact \
             INNER JOIN v112_kw_np_dim ON v112_kw_np_dim.id = v112_kw_np_fact.dim_id \
             WHERE v112_kw_np_dim.status = 'on' GROUP BY dim_id \
             HAVING SUM(val) <> length('xGROUP BYx')";
    let created = crate::create_reflex_ivm("v112_kw_np", q, None, None, None, None);
    assert_eq!(created, "CREATE REFLEX INCREMENTAL VIEW", "{q}");

    Spi::run("UPDATE v112_kw_np_dim SET status = 'on' WHERE id = 40").expect("flip in");
    assert_imv_correct("v112_kw_np", q);
    Spi::run("UPDATE v112_kw_np_dim SET status = 'off' WHERE id = 7").expect("flip out");
    assert_imv_correct("v112_kw_np", q);
    Spi::run("DELETE FROM v112_kw_np_dim WHERE id = 30").expect("delete dim");
    Spi::run("INSERT INTO v112_kw_np_fact VALUES (500, 20, 1)").expect("insert fact");
    assert_imv_correct("v112_kw_np", q);
}

#[pg_test]
fn v112_kw_count_distinct_alias_shorter_once_upper_cased() {
    v112_kw_roundtrip(
        "cda",
        "SELECT g, COUNT(DISTINCT v) AS \"nıı\" FROM {t} GROUP BY g",
    );
}

/// Group G never has a non-NULL `a`, so its MIN slot stays NULL and every
/// recompute rewrites its MAX from whatever rows the scoped source returns.
fn v112_or_roundtrip(tag: &str, predicate: &str) {
    let table = format!("v112_or_{tag}_src");
    let imv = format!("v112_or_{tag}");
    Spi::run(&format!(
        "CREATE TABLE {table}(id SERIAL PRIMARY KEY, g TEXT, city TEXT, \
         flag BOOL, other BOOL, third BOOL, a INT, b INT)"
    ))
    .expect("create");
    Spi::run(&format!(
        "INSERT INTO {table}(g, city, flag, other, third, a, b) VALUES \
         ('G','Lyon',true,false,false,NULL,1), \
         ('G','Lyon',false,true,false,NULL,100), \
         ('G','Lyon',false,false,true,NULL,1000), \
         ('G','Paris',true,true,false,NULL,50), \
         ('H','Lyon',true,false,false,1,1), \
         ('H','Lyon',true,false,false,2,2), \
         ('H','Paris',false,true,false,3,3), \
         ('H','Lyon',false,false,true,4,4), \
         ('H','Paris',true,true,true,5,5)"
    ))
    .expect("seed");
    let q = format!("SELECT g, MIN(a) AS mn, MAX(b) AS mx FROM {table} WHERE {predicate} GROUP BY g");
    assert_eq!(
        crate::create_reflex_ivm_with_topk(&imv, &q, None, None, None, 0, None),
        "CREATE REFLEX INCREMENTAL VIEW",
        "{q}"
    );
    Spi::run(&format!("DELETE FROM {table} WHERE g = 'H' AND a = 1")).expect("delete");
    assert_imv_correct(&imv, &q);
    Spi::run(&format!(
        "INSERT INTO {table}(g, city, flag, other, third, a, b) VALUES \
         ('G','Lyon',true,false,false,NULL,2), ('K','Lyon',true,true,true,7,7)"
    ))
    .expect("insert");
    assert_imv_correct(&imv, &q);
    Spi::run(&format!("UPDATE {table} SET b = b - 1 WHERE g = 'H' AND a = 5")).expect("update");
    assert_imv_correct(&imv, &q);
}

#[pg_test]
fn v112_or_two_way_or_predicate() {
    v112_or_roundtrip("two", "flag OR other");
}

#[pg_test]
fn v112_or_three_way_or_predicate() {
    v112_or_roundtrip("three", "flag OR other OR third");
}

#[pg_test]
fn v112_or_parenthesised_or_then_and() {
    v112_or_roundtrip("paren", "(flag OR other) AND city <> 'x'");
}

#[pg_test]
fn v112_or_and_only_predicate() {
    v112_or_roundtrip("and", "flag AND city <> 'x'");
}

#[pg_test]
fn v112_or_only_inside_a_literal() {
    v112_or_roundtrip("lit", "city <> 'x or y'");
}

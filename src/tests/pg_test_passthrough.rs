
#[pg_test]
fn test_passthrough_simple() {
    Spi::run("CREATE TABLE pt_src (id SERIAL, name TEXT, active BOOLEAN)")
        .expect("create table");
    Spi::run("INSERT INTO pt_src (name, active) VALUES ('Alice', true), ('Bob', false)")
        .expect("seed");

    let result = crate::create_reflex_ivm(
        "pt_view",
        "SELECT id, name FROM pt_src WHERE active = true",
        None,
        None,
        None,
    );
    assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");

    // Verify initial data
    let count = Spi::get_one::<i64>("SELECT COUNT(*) FROM pt_view")
        .expect("q").expect("v");
    assert_eq!(count, 1); // Only Alice (active=true)

    // INSERT a matching row → appears in target
    Spi::run("INSERT INTO pt_src (name, active) VALUES ('Carol', true)").expect("insert");
    let count2 = Spi::get_one::<i64>("SELECT COUNT(*) FROM pt_view")
        .expect("q").expect("v");
    assert_eq!(count2, 2);

    // INSERT a non-matching row → does not appear
    Spi::run("INSERT INTO pt_src (name, active) VALUES ('Dave', false)").expect("insert");
    let count3 = Spi::get_one::<i64>("SELECT COUNT(*) FROM pt_view")
        .expect("q").expect("v");
    assert_eq!(count3, 2); // Still 2
}

#[pg_test]
fn test_passthrough_join() {
    Spi::run("CREATE TABLE pt_orders (id SERIAL, product_id INT, amount NUMERIC)")
        .expect("create orders");
    Spi::run("CREATE TABLE pt_products (id SERIAL PRIMARY KEY, name TEXT)")
        .expect("create products");
    Spi::run("INSERT INTO pt_products (id, name) VALUES (1, 'Widget'), (2, 'Gadget')")
        .expect("seed products");
    Spi::run("INSERT INTO pt_orders (product_id, amount) VALUES (1, 100), (2, 200)")
        .expect("seed orders");

    let result = crate::create_reflex_ivm(
        "pt_join_view",
        "SELECT o.id, p.name, o.amount FROM pt_orders o JOIN pt_products p ON o.product_id = p.id",
        None,
        None,
        None,
    );
    assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");

    let count = Spi::get_one::<i64>("SELECT COUNT(*) FROM pt_join_view")
        .expect("q").expect("v");
    assert_eq!(count, 2);

    // INSERT into orders → trigger fires, new row appears
    Spi::run("INSERT INTO pt_orders (product_id, amount) VALUES (1, 300)")
        .expect("insert");
    let count2 = Spi::get_one::<i64>("SELECT COUNT(*) FROM pt_join_view")
        .expect("q").expect("v");
    assert_eq!(count2, 3);
}

#[pg_test]
fn test_passthrough_delete_refreshes() {
    Spi::run("CREATE TABLE pt_del (id SERIAL, val TEXT)").expect("create");
    Spi::run("INSERT INTO pt_del (val) VALUES ('a'), ('b'), ('c')").expect("seed");

    crate::create_reflex_ivm("pt_del_view", "SELECT id, val FROM pt_del", None, None, None);

    // DELETE → full refresh
    Spi::run("DELETE FROM pt_del WHERE val = 'b'").expect("delete");
    let count = Spi::get_one::<i64>("SELECT COUNT(*) FROM pt_del_view")
        .expect("q").expect("v");
    assert_eq!(count, 2);
}

#[pg_test]
fn test_passthrough_incremental_delete() {
    Spi::run(
        "CREATE TABLE pt_del_src (id SERIAL PRIMARY KEY, region TEXT NOT NULL, val INT NOT NULL)",
    )
    .expect("create table");
    Spi::run(
        "INSERT INTO pt_del_src (region, val) VALUES ('A', 1), ('A', 2), ('B', 3), ('B', 4), ('C', 5)",
    )
    .expect("seed");
    crate::create_reflex_ivm(
        "pt_del_view",
        "SELECT id, region, val FROM pt_del_src",
        None,
        None,
        None,
    );
    assert_eq!(
        Spi::get_one::<i64>("SELECT COUNT(*) FROM pt_del_view").expect("q").expect("v"),
        5,
        "Initial view should have 5 rows"
    );

    // Delete 2 specific rows
    Spi::run("DELETE FROM pt_del_src WHERE id IN (2, 4)").expect("delete");

    let count =
        Spi::get_one::<i64>("SELECT COUNT(*) FROM pt_del_view").expect("q").expect("v");
    assert_eq!(count, 3, "View should have 3 rows after deleting 2");

    // Verify exact content matches source
    let mismatches = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM (
            SELECT id, region, val FROM pt_del_view
            EXCEPT
            SELECT id, region, val FROM pt_del_src
        ) x",
    )
    .expect("q")
    .expect("v");
    assert_eq!(mismatches, 0, "View should exactly match source after delete");
}

#[pg_test]
fn test_passthrough_incremental_update() {
    Spi::run(
        "CREATE TABLE pt_upd_src (id SERIAL PRIMARY KEY, region TEXT NOT NULL, val INT NOT NULL)",
    )
    .expect("create table");
    Spi::run(
        "INSERT INTO pt_upd_src (region, val) VALUES ('A', 10), ('B', 20), ('C', 30)",
    )
    .expect("seed");
    crate::create_reflex_ivm(
        "pt_upd_view",
        "SELECT id, region, val FROM pt_upd_src",
        None,
        None,
        None,
    );

    // Update a value
    Spi::run("UPDATE pt_upd_src SET val = 99 WHERE region = 'B'").expect("update");

    let val = Spi::get_one::<i32>(
        "SELECT val FROM pt_upd_view WHERE region = 'B'",
    )
    .expect("q")
    .expect("v");
    assert_eq!(val, 99, "Updated value should propagate to view");

    // Update region (changes a different column)
    Spi::run("UPDATE pt_upd_src SET region = 'D' WHERE val = 99").expect("update region");

    let count_b =
        Spi::get_one::<i64>("SELECT COUNT(*) FROM pt_upd_view WHERE region = 'B'")
            .expect("q")
            .expect("v");
    assert_eq!(count_b, 0, "Old region B should be gone from view");

    let count_d =
        Spi::get_one::<i64>("SELECT COUNT(*) FROM pt_upd_view WHERE region = 'D'")
            .expect("q")
            .expect("v");
    assert_eq!(count_d, 1, "New region D should appear in view");

    // Full content check
    let mismatches = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM (
            SELECT id, region, val FROM pt_upd_view
            EXCEPT
            SELECT id, region, val FROM pt_upd_src
        ) x",
    )
    .expect("q")
    .expect("v");
    assert_eq!(mismatches, 0, "View should exactly match source after updates");
}

#[pg_test]
fn test_passthrough_join_delete_secondary_table() {
    // Setup: two source tables with a JOIN
    Spi::run(
        "CREATE TABLE ptj_products (id SERIAL PRIMARY KEY, name TEXT NOT NULL)",
    )
    .expect("create products");
    Spi::run(
        "CREATE TABLE ptj_sales (id SERIAL PRIMARY KEY, product_id INT NOT NULL, amount NUMERIC NOT NULL)",
    )
    .expect("create sales");
    Spi::run(
        "INSERT INTO ptj_products (id, name) VALUES (1, 'Widget'), (2, 'Gadget'), (3, 'Doohickey')",
    )
    .expect("seed products");
    Spi::run(
        "INSERT INTO ptj_sales (product_id, amount) VALUES (1, 100), (1, 200), (2, 300), (3, 50)",
    )
    .expect("seed sales");

    // Create passthrough JOIN IMV with explicit unique key (id comes from ptj_sales)
    let result = crate::create_reflex_ivm(
        "ptj_view",
        "SELECT s.id, s.product_id, s.amount, p.name FROM ptj_sales s JOIN ptj_products p ON s.product_id = p.id",
        Some("id"),
        None,
        None,
    );
    assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");

    let count = Spi::get_one::<i64>("SELECT COUNT(*) FROM ptj_view")
        .expect("q").expect("v");
    assert_eq!(count, 4, "Initial view should have 4 rows");

    // DELETE from the SECONDARY table (products) — this is the critical test
    // Deleting product 2 should remove all sales rows referencing it
    Spi::run("DELETE FROM ptj_products WHERE id = 2").expect("delete product");

    let count_after = Spi::get_one::<i64>("SELECT COUNT(*) FROM ptj_view")
        .expect("q").expect("v");
    assert_eq!(count_after, 3, "View should have 3 rows after deleting product 2");

    // Verify no rows reference the deleted product
    let orphans = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM ptj_view WHERE product_id = 2",
    )
    .expect("q").expect("v");
    assert_eq!(orphans, 0, "No rows should reference deleted product");

    // Verify remaining data is correct
    let mismatches = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM (
            SELECT id, product_id, amount, name FROM ptj_view
            EXCEPT
            SELECT s.id, s.product_id, s.amount, p.name
            FROM ptj_sales s JOIN ptj_products p ON s.product_id = p.id
        ) x",
    )
    .expect("q").expect("v");
    assert_eq!(mismatches, 0, "View should exactly match source after delete");
}

#[pg_test]
fn test_passthrough_join_update_secondary_table() {
    Spi::run(
        "CREATE TABLE ptju_products (id SERIAL PRIMARY KEY, name TEXT NOT NULL)",
    )
    .expect("create products");
    Spi::run(
        "CREATE TABLE ptju_sales (id SERIAL PRIMARY KEY, product_id INT NOT NULL, qty INT NOT NULL)",
    )
    .expect("create sales");
    Spi::run("INSERT INTO ptju_products VALUES (1, 'Alpha'), (2, 'Beta')").expect("seed products");
    Spi::run("INSERT INTO ptju_sales (product_id, qty) VALUES (1, 10), (2, 20)").expect("seed sales");

    crate::create_reflex_ivm(
        "ptju_view",
        "SELECT s.id, s.qty, p.name FROM ptju_sales s JOIN ptju_products p ON s.product_id = p.id",
        Some("id"),
        None,
        None,
    );

    // UPDATE the secondary table (product name change)
    Spi::run("UPDATE ptju_products SET name = 'Alpha-v2' WHERE id = 1").expect("update product");

    // The view should reflect the updated product name
    let name = Spi::get_one::<String>(
        "SELECT name FROM ptju_view WHERE id = 1",
    )
    .expect("q").expect("v");
    assert_eq!(name, "Alpha-v2", "View should reflect updated product name");
}

/// JOIN passthrough with no explicit key: DELETE on secondary table should fall back
/// to full refresh and still produce correct results.
#[pg_test]
fn test_passthrough_join_no_key_delete_secondary() {
    Spi::run("CREATE TABLE ptjnk_products (id SERIAL PRIMARY KEY, name TEXT NOT NULL)")
        .expect("create products");
    Spi::run("CREATE TABLE ptjnk_sales (id SERIAL PRIMARY KEY, product_id INT NOT NULL, amount INT NOT NULL)")
        .expect("create sales");
    Spi::run("INSERT INTO ptjnk_products VALUES (1, 'A'), (2, 'B'), (3, 'C')").expect("seed products");
    Spi::run("INSERT INTO ptjnk_sales (product_id, amount) VALUES (1, 10), (2, 20), (3, 30)").expect("seed sales");

    // No explicit key → JOIN triggers fall back to full refresh
    crate::create_reflex_ivm(
        "ptjnk_view",
        "SELECT s.id, s.amount, p.name FROM ptjnk_sales s JOIN ptjnk_products p ON s.product_id = p.id",
        None,
        None,
        None,
    );
    assert_eq!(
        Spi::get_one::<i64>("SELECT COUNT(*) FROM ptjnk_view").expect("q").expect("v"),
        3
    );

    // DELETE from secondary table → full refresh should still be correct
    Spi::run("DELETE FROM ptjnk_products WHERE id = 2").expect("delete product");
    let count = Spi::get_one::<i64>("SELECT COUNT(*) FROM ptjnk_view").expect("q").expect("v");
    assert_eq!(count, 2, "Full refresh should remove orphaned rows");

    let mismatches = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM (
            SELECT id, amount, name FROM ptjnk_view
            EXCEPT
            SELECT s.id, s.amount, p.name FROM ptjnk_sales s JOIN ptjnk_products p ON s.product_id = p.id
        ) x",
    ).expect("q").expect("v");
    assert_eq!(mismatches, 0, "View should exactly match source");
}

/// JOIN passthrough with explicit key: DELETE on the key-owner table should use
/// direct key extraction (fast path, no JOINs).
#[pg_test]
fn test_passthrough_join_delete_key_owner_table() {
    Spi::run("CREATE TABLE ptjko_products (id SERIAL PRIMARY KEY, name TEXT NOT NULL)")
        .expect("create products");
    Spi::run("CREATE TABLE ptjko_sales (id SERIAL PRIMARY KEY, product_id INT NOT NULL, amount INT NOT NULL)")
        .expect("create sales");
    Spi::run("INSERT INTO ptjko_products VALUES (1, 'A'), (2, 'B')").expect("seed products");
    Spi::run("INSERT INTO ptjko_sales (product_id, amount) VALUES (1, 10), (1, 20), (2, 30)")
        .expect("seed sales");

    crate::create_reflex_ivm(
        "ptjko_view",
        "SELECT s.id, s.product_id, s.amount, p.name FROM ptjko_sales s JOIN ptjko_products p ON s.product_id = p.id",
        Some("id"),
        None,
        None,
    );
    assert_eq!(
        Spi::get_one::<i64>("SELECT COUNT(*) FROM ptjko_view").expect("q").expect("v"),
        3
    );

    // DELETE from key-owner table (sales) → direct key extraction
    Spi::run("DELETE FROM ptjko_sales WHERE id = 2").expect("delete sale");
    assert_eq!(
        Spi::get_one::<i64>("SELECT COUNT(*) FROM ptjko_view").expect("q").expect("v"),
        2,
        "Should remove exactly 1 row"
    );

    let mismatches = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM (
            SELECT id, product_id, amount, name FROM ptjko_view
            EXCEPT
            SELECT s.id, s.product_id, s.amount, p.name FROM ptjko_sales s JOIN ptjko_products p ON s.product_id = p.id
        ) x",
    ).expect("q").expect("v");
    assert_eq!(mismatches, 0);
}

/// 3-table JOIN passthrough: verify DELETE on each table produces correct results.
#[pg_test]
fn test_passthrough_three_table_join() {
    Spi::run("CREATE TABLE pt3_regions (id SERIAL PRIMARY KEY, name TEXT NOT NULL)")
        .expect("create regions");
    Spi::run("CREATE TABLE pt3_products (id SERIAL PRIMARY KEY, name TEXT NOT NULL)")
        .expect("create products");
    Spi::run("CREATE TABLE pt3_sales (id SERIAL PRIMARY KEY, product_id INT NOT NULL, region_id INT NOT NULL, qty INT NOT NULL)")
        .expect("create sales");
    Spi::run("INSERT INTO pt3_regions VALUES (1, 'North'), (2, 'South')").expect("seed regions");
    Spi::run("INSERT INTO pt3_products VALUES (1, 'Widget'), (2, 'Gadget')").expect("seed products");
    Spi::run("INSERT INTO pt3_sales (product_id, region_id, qty) VALUES (1,1,10), (1,2,20), (2,1,30), (2,2,40)")
        .expect("seed sales");

    crate::create_reflex_ivm(
        "pt3_view",
        "SELECT s.id, s.qty, p.name AS product_name, r.name AS region_name \
         FROM pt3_sales s \
         JOIN pt3_products p ON s.product_id = p.id \
         JOIN pt3_regions r ON s.region_id = r.id",
        Some("id"),
        None,
        None,
    );
    assert_eq!(
        Spi::get_one::<i64>("SELECT COUNT(*) FROM pt3_view").expect("q").expect("v"),
        4
    );

    // DELETE from 2nd secondary table (regions)
    Spi::run("DELETE FROM pt3_regions WHERE id = 2").expect("delete region");
    assert_eq!(
        Spi::get_one::<i64>("SELECT COUNT(*) FROM pt3_view").expect("q").expect("v"),
        2,
        "Should remove 2 rows (both sales in South region)"
    );

    // DELETE from 1st secondary table (products)
    Spi::run("DELETE FROM pt3_products WHERE id = 1").expect("delete product");
    assert_eq!(
        Spi::get_one::<i64>("SELECT COUNT(*) FROM pt3_view").expect("q").expect("v"),
        1,
        "Should remove 1 more row (Widget in North)"
    );

    // Verify exact match with source
    let mismatches = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM (
            SELECT id, qty, product_name, region_name FROM pt3_view
            EXCEPT
            SELECT s.id, s.qty, p.name, r.name FROM pt3_sales s
                JOIN pt3_products p ON s.product_id = p.id
                JOIN pt3_regions r ON s.region_id = r.id
        ) x",
    ).expect("q").expect("v");
    assert_eq!(mismatches, 0, "View should exactly match 3-table JOIN");
}

/// JOIN passthrough with composite key: multiple key columns from the key-owner table.
#[pg_test]
fn test_passthrough_join_composite_key() {
    Spi::run("CREATE TABLE ptck_dims (id SERIAL PRIMARY KEY, label TEXT NOT NULL)")
        .expect("create dims");
    Spi::run(
        "CREATE TABLE ptck_facts (product_id INT NOT NULL, region_id INT NOT NULL, dim_id INT NOT NULL, val INT NOT NULL, \
         PRIMARY KEY (product_id, region_id))",
    ).expect("create facts");
    Spi::run("INSERT INTO ptck_dims VALUES (1, 'X'), (2, 'Y')").expect("seed dims");
    Spi::run(
        "INSERT INTO ptck_facts VALUES (1,1,1,10), (1,2,1,20), (2,1,2,30), (2,2,2,40)",
    ).expect("seed facts");

    crate::create_reflex_ivm(
        "ptck_view",
        "SELECT f.product_id, f.region_id, f.val, d.label \
         FROM ptck_facts f JOIN ptck_dims d ON f.dim_id = d.id",
        Some("product_id, region_id"),
        None,
        None,
    );
    assert_eq!(
        Spi::get_one::<i64>("SELECT COUNT(*) FROM ptck_view").expect("q").expect("v"),
        4
    );

    // DELETE from key-owner table using composite key
    Spi::run("DELETE FROM ptck_facts WHERE product_id = 1 AND region_id = 2").expect("delete");
    assert_eq!(
        Spi::get_one::<i64>("SELECT COUNT(*) FROM ptck_view").expect("q").expect("v"),
        3
    );

    // DELETE from secondary table
    Spi::run("DELETE FROM ptck_dims WHERE id = 2").expect("delete dim");
    assert_eq!(
        Spi::get_one::<i64>("SELECT COUNT(*) FROM ptck_view").expect("q").expect("v"),
        1,
        "Should remove both rows referencing dim 2"
    );

    let mismatches = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM (
            SELECT product_id, region_id, val, label FROM ptck_view
            EXCEPT
            SELECT f.product_id, f.region_id, f.val, d.label
            FROM ptck_facts f JOIN ptck_dims d ON f.dim_id = d.id
        ) x",
    ).expect("q").expect("v");
    assert_eq!(mismatches, 0);
}

/// JOIN passthrough with aliased key column: target uses alias, source uses original name.
#[pg_test]
fn test_passthrough_join_aliased_key() {
    Spi::run("CREATE TABLE ptak_cats (id SERIAL PRIMARY KEY, cat_name TEXT NOT NULL)")
        .expect("create cats");
    Spi::run(
        "CREATE TABLE ptak_items (item_id SERIAL PRIMARY KEY, cat_id INT NOT NULL, price INT NOT NULL)",
    ).expect("create items");
    Spi::run("INSERT INTO ptak_cats VALUES (1, 'Electronics'), (2, 'Books')").expect("seed cats");
    Spi::run("INSERT INTO ptak_items (cat_id, price) VALUES (1, 100), (1, 200), (2, 50)")
        .expect("seed items");

    crate::create_reflex_ivm(
        "ptak_view",
        "SELECT i.item_id AS id, i.price, c.cat_name AS category \
         FROM ptak_items i JOIN ptak_cats c ON i.cat_id = c.id",
        Some("id"),
        None,
        None,
    );
    assert_eq!(
        Spi::get_one::<i64>("SELECT COUNT(*) FROM ptak_view").expect("q").expect("v"),
        3
    );

    // DELETE from secondary table (cats) — mapping should resolve cat_id→id
    Spi::run("DELETE FROM ptak_cats WHERE id = 1").expect("delete cat");
    assert_eq!(
        Spi::get_one::<i64>("SELECT COUNT(*) FROM ptak_view").expect("q").expect("v"),
        1,
        "Should remove 2 electronics items"
    );

    let mismatches = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM (
            SELECT id, price, category FROM ptak_view
            EXCEPT
            SELECT i.item_id, i.price, c.cat_name FROM ptak_items i JOIN ptak_cats c ON i.cat_id = c.id
        ) x",
    ).expect("q").expect("v");
    assert_eq!(mismatches, 0);
}

/// INSERT on secondary table in a JOIN passthrough should add rows correctly.
#[pg_test]
fn test_passthrough_join_insert_secondary() {
    Spi::run("CREATE TABLE ptjis_products (id SERIAL PRIMARY KEY, name TEXT NOT NULL)")
        .expect("create products");
    Spi::run("CREATE TABLE ptjis_sales (id SERIAL PRIMARY KEY, product_id INT NOT NULL, amount INT NOT NULL)")
        .expect("create sales");
    Spi::run("INSERT INTO ptjis_products VALUES (1, 'Alpha')").expect("seed products");
    Spi::run("INSERT INTO ptjis_sales (product_id, amount) VALUES (1, 100)").expect("seed sales");

    crate::create_reflex_ivm(
        "ptjis_view",
        "SELECT s.id, s.amount, p.name FROM ptjis_sales s JOIN ptjis_products p ON s.product_id = p.id",
        Some("id"),
        None,
        None,
    );
    assert_eq!(
        Spi::get_one::<i64>("SELECT COUNT(*) FROM ptjis_view").expect("q").expect("v"),
        1
    );

    // INSERT a new product — no new sales reference it, so view should not change
    Spi::run("INSERT INTO ptjis_products VALUES (2, 'Beta')").expect("insert product");
    assert_eq!(
        Spi::get_one::<i64>("SELECT COUNT(*) FROM ptjis_view").expect("q").expect("v"),
        1,
        "New product with no sales should not affect view"
    );

    // Now add a sale referencing the new product
    Spi::run("INSERT INTO ptjis_sales (product_id, amount) VALUES (2, 200)").expect("insert sale");
    assert_eq!(
        Spi::get_one::<i64>("SELECT COUNT(*) FROM ptjis_view").expect("q").expect("v"),
        2,
        "New sale should appear in view"
    );

    let name = Spi::get_one::<String>("SELECT name FROM ptjis_view WHERE amount = 200")
        .expect("q").expect("v");
    assert_eq!(name, "Beta");
}

#[pg_test]
fn test_passthrough_auto_pk_from_source() {
    Spi::run("CREATE TABLE pt_pk_src (id INTEGER PRIMARY KEY, name TEXT, status TEXT)")
        .expect("create table");
    Spi::run("INSERT INTO pt_pk_src VALUES (1, 'a', 'active'), (2, 'b', 'active'), (3, 'c', 'inactive')")
        .expect("seed");

    let result = crate::create_reflex_ivm(
        "pt_pk_view",
        "SELECT id, name, status FROM pt_pk_src WHERE status = 'active'",
        None,
        None,
        None,
    );
    assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");

    assert_eq!(
        Spi::get_one::<i64>("SELECT COUNT(*) FROM pt_pk_view").expect("q").expect("v"),
        2,
    );

    Spi::run("DELETE FROM pt_pk_src WHERE id = 1").expect("delete");
    assert_eq!(
        Spi::get_one::<i64>("SELECT COUNT(*) FROM pt_pk_view").expect("q").expect("v"),
        1,
        "DELETE on source should propagate via inferred PK without explicit unique_columns",
    );

    let remaining_name: String = Spi::get_one("SELECT name FROM pt_pk_view")
        .expect("q").expect("v");
    assert_eq!(remaining_name, "b");
}

#[pg_test]
fn test_passthrough_no_pk_no_inference() {
    Spi::run("CREATE TABLE pt_nopk_src (id INTEGER, val TEXT)")
        .expect("create table");
    Spi::run("INSERT INTO pt_nopk_src VALUES (1, 'a'), (2, 'b')").expect("seed");

    let result = crate::create_reflex_ivm(
        "pt_nopk_view",
        "SELECT id, val FROM pt_nopk_src",
        None,
        None,
        None,
    );
    assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");

    Spi::run("INSERT INTO pt_nopk_src VALUES (3, 'c')").expect("insert");
    assert_eq!(
        Spi::get_one::<i64>("SELECT COUNT(*) FROM pt_nopk_view").expect("q").expect("v"),
        3,
        "INSERT should still propagate without a PK",
    );
}

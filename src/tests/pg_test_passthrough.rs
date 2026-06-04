
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

    crate::create_reflex_ivm("pt_del_view", "SELECT id, val FROM pt_del", None, None, None, None);

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

/// audit #3: passthrough IMV with a LEFT JOIN secondary. Exercise INSERT /
/// DELETE / UPDATE on the secondary (incl. NULL↔value and a no-match key) and
/// assert the IMV matches a fresh recompute after each op. Keyed maintenance
/// must produce identical results to the full rebuild it replaces.
#[pg_test]
fn pt_secondary_keyed_left_join_all_ops_immediate() {
    Spi::run("CREATE TABLE sk_anchor (id INT PRIMARY KEY, product_id INT NOT NULL, location_id INT NOT NULL, qty INT)").expect("anchor");
    Spi::run("CREATE TABLE sk_act (product_id INT NOT NULL, location_id INT NOT NULL, is_active BOOL)").expect("sec");
    Spi::run("INSERT INTO sk_anchor VALUES (1,10,100,5),(2,10,101,6),(3,11,100,7)").expect("seed anchor");
    Spi::run("INSERT INTO sk_act VALUES (10,100,TRUE)").expect("seed sec");
    let sql = "SELECT a.id, a.product_id, a.location_id, a.qty, COALESCE(c.is_active, FALSE) AS active \
               FROM sk_anchor a LEFT JOIN sk_act c ON c.product_id = a.product_id AND c.location_id = a.location_id";
    let res = crate::create_reflex_ivm("skv", sql, Some("id"), None, None, None);
    assert_eq!(res, "CREATE REFLEX INCREMENTAL VIEW");
    assert_imv_correct("skv", sql);

    // INSERT into secondary: activate a key that exists in the anchor.
    Spi::run("INSERT INTO sk_act VALUES (10,101,TRUE)").expect("insert sec (activate key)");
    assert_imv_correct("skv", sql);
    // UPDATE the secondary's value (TRUE -> FALSE).
    Spi::run("UPDATE sk_act SET is_active = FALSE WHERE product_id = 10 AND location_id = 100").expect("update sec");
    assert_imv_correct("skv", sql);
    // DELETE from secondary: affected anchor rows revert to COALESCE(NULL,FALSE).
    Spi::run("DELETE FROM sk_act WHERE product_id = 10 AND location_id = 101").expect("delete sec");
    assert_imv_correct("skv", sql);
    // no-match key: secondary row for a (product,location) not present in anchor.
    Spi::run("INSERT INTO sk_act VALUES (99,999,TRUE)").expect("insert no-match");
    assert_imv_correct("skv", sql);
}

/// Same as above but DEFERRED: the whole batch collapses into one flush, and
/// the keyed delete/reinsert runs once over the netted union of changed keys.
#[pg_test]
fn pt_secondary_keyed_left_join_all_ops_deferred() {
    Spi::run("CREATE TABLE skd_anchor (id INT PRIMARY KEY, product_id INT NOT NULL, location_id INT NOT NULL, qty INT)").expect("anchor");
    Spi::run("CREATE TABLE skd_act (product_id INT NOT NULL, location_id INT NOT NULL, is_active BOOL)").expect("sec");
    Spi::run("INSERT INTO skd_anchor VALUES (1,10,100,5),(2,10,101,6),(3,11,100,7)").expect("seed anchor");
    Spi::run("INSERT INTO skd_act VALUES (10,100,TRUE)").expect("seed sec");
    let sql = "SELECT a.id, a.product_id, a.location_id, a.qty, COALESCE(c.is_active, FALSE) AS active \
               FROM skd_anchor a LEFT JOIN skd_act c ON c.product_id = a.product_id AND c.location_id = a.location_id";
    let res = crate::create_reflex_ivm("skdv", sql, Some("id"), None, Some("DEFERRED"), None);
    assert_eq!(res, "CREATE REFLEX INCREMENTAL VIEW");
    assert_imv_correct("skdv", sql);

    Spi::run("INSERT INTO skd_act VALUES (10,101,TRUE)").expect("ins");
    Spi::run("UPDATE skd_act SET is_active = FALSE WHERE product_id = 10 AND location_id = 100").expect("upd");
    Spi::run("DELETE FROM skd_act WHERE product_id = 11 AND location_id = 100").expect("del-nomatch");
    Spi::run("SELECT reflex_flush_deferred('skd_act')").expect("flush");
    assert_imv_correct("skdv", sql);
}

/// audit #3: a passthrough IMV with a LEFT JOIN secondary gets an index on the
/// secondary join-key columns auto-created on the IMV, so the keyed secondary
/// DELETE is index-served. A secondary mapping is only derived when the join's
/// other side is a UNIQUE-KEY column, so the key is composite
/// (region, product_id, location_id) and the secondary maps the NON-leading
/// subset (product_id, location_id) — which the `__reflex_uk_*` index (leading
/// with region) does NOT cover, so a dedicated index must be created. The
/// prefix is matched via the 0-indexed `indkey` int2vector (no fragile
/// `int2vector::int2[]` cast). This mirrors the audit's sop_forecast_view, whose
/// key leads with dem_plan_id and whose caav secondary maps product_id/location_id.
#[pg_test]
fn pt_secondary_autoindex_created() {
    Spi::run("CREATE TABLE ai_anchor (region INT NOT NULL, product_id INT NOT NULL, location_id INT NOT NULL, qty INT)").expect("anchor");
    Spi::run("CREATE TABLE ai_act (product_id INT NOT NULL, location_id INT NOT NULL, is_active BOOL)").expect("sec");
    let sql = "SELECT a.region, a.product_id, a.location_id, a.qty, COALESCE(c.is_active, FALSE) AS active \
               FROM ai_anchor a LEFT JOIN ai_act c ON c.product_id = a.product_id AND c.location_id = a.location_id";
    let res = crate::create_reflex_ivm("aiv", sql, Some("region,product_id,location_id"), None, None, None);
    assert_eq!(res, "CREATE REFLEX INCREMENTAL VIEW");

    let has_idx = Spi::get_one::<bool>(
        "SELECT EXISTS ( \
            SELECT 1 FROM pg_index i \
            WHERE i.indrelid = 'aiv'::regclass \
              AND i.indnkeyatts >= 2 \
              AND NOT EXISTS ( \
                  SELECT 1 FROM unnest(ARRAY['product_id','location_id']::text[]) \
                       WITH ORDINALITY t(cname, ord) \
                  WHERE i.indkey[ord - 1] IS DISTINCT FROM ( \
                      SELECT a.attnum FROM pg_attribute a \
                      WHERE a.attrelid = i.indrelid AND a.attname = t.cname \
                            AND NOT a.attisdropped ) ) )",
    ).expect("q").expect("c");
    assert!(has_idx, "secondary-key index leading with (product_id, location_id) must be auto-created on the IMV");
}

/// The coverage check must skip creation when an index already covers the
/// secondary columns as a leading prefix. Here the key IS (product_id,
/// location_id), so the secondary's mapped columns are exactly the unique key —
/// the `__reflex_uk_*` index already covers them and no second index is added.
#[pg_test]
fn pt_secondary_autoindex_skipped_when_covered() {
    Spi::run("CREATE TABLE aic_anchor (product_id INT NOT NULL, location_id INT NOT NULL, qty INT)").expect("anchor");
    Spi::run("CREATE TABLE aic_act (product_id INT NOT NULL, location_id INT NOT NULL, is_active BOOL)").expect("sec");
    let sql = "SELECT a.product_id, a.location_id, a.qty, COALESCE(c.is_active, FALSE) AS active \
               FROM aic_anchor a LEFT JOIN aic_act c ON c.product_id = a.product_id AND c.location_id = a.location_id";
    let res = crate::create_reflex_ivm("aicv", sql, Some("product_id,location_id"), None, None, None);
    assert_eq!(res, "CREATE REFLEX INCREMENTAL VIEW");
    let n = Spi::get_one::<i64>(
        "SELECT count(*) FROM pg_index i \
         WHERE i.indrelid = 'aicv'::regclass \
           AND i.indnkeyatts >= 2 \
           AND NOT EXISTS ( \
               SELECT 1 FROM unnest(ARRAY['product_id','location_id']::text[]) \
                    WITH ORDINALITY t(cname, ord) \
               WHERE i.indkey[ord - 1] IS DISTINCT FROM ( \
                   SELECT a.attnum FROM pg_attribute a \
                   WHERE a.attrelid = i.indrelid AND a.attname = t.cname \
                         AND NOT a.attisdropped ) )",
    ).expect("q").expect("c");
    assert!(n == 1, "exactly the unique-key index should cover (product_id, location_id) — no duplicate, found {n}");
}

// Single-source passthrough must auto-detect the source PK into unique_columns
// (catalog-level guard for the attname::text cast; row-count tests alone pass
// even when keyless full-rebuild silently substitutes for keyed maintenance).
#[pg_test]
fn test_passthrough_auto_pk_recorded_in_catalog() {
    Spi::run("CREATE TABLE ptkr_src (id INT PRIMARY KEY, name TEXT, status TEXT)")
        .expect("create table");
    Spi::run("INSERT INTO ptkr_src VALUES (1, 'a', 'active'), (2, 'b', 'active')")
        .expect("seed");

    crate::create_reflex_ivm(
        "ptkr_view",
        "SELECT id, name, status FROM ptkr_src WHERE status = 'active'",
        None,
        None,
        None,
        None,
    );

    let uk = Spi::get_one::<Vec<String>>(
        "SELECT unique_columns FROM public.__reflex_ivm_reference WHERE name = 'ptkr_view'",
    )
    .expect("q")
    .expect("auto-detected key");
    assert_eq!(
        uk,
        vec!["id".to_string()],
        "single-source passthrough must record the auto-detected PK as its unique key"
    );
}

/// Partitioned passthrough IMV with a WHERE filter and a LEFT JOIN. Forces the
/// COLD dispatch path (high wipe_threshold) so it exercises the keyed cold
/// maintenance that Phase 2 turns into an in-place upsert. Covers: (a) pure-data
/// UPDATE, (b) a row LEAVING the filter without the source row vanishing, (c) a
/// new key entering, (d) a key-column change, (e) a delete. The IMV must match a
/// fresh recompute after each op (today via DELETE+INSERT; Phase 2 via upsert).
#[pg_test]
fn pt_inplace_upsert_filter_and_keychange_oracle() {
    Spi::run("CREATE TABLE up_src (id BIGINT NOT NULL, region TEXT NOT NULL, status TEXT NOT NULL, qty BIGINT, PRIMARY KEY (id, region)) PARTITION BY LIST (region)").expect("src");
    for r in ["A", "B"] {
        Spi::run(&format!("CREATE TABLE up_src_{} PARTITION OF up_src FOR VALUES IN ('{}')", r, r)).expect("p");
    }
    Spi::run("CREATE TABLE up_price (id BIGINT PRIMARY KEY, price BIGINT)").expect("price");
    Spi::run("INSERT INTO up_price VALUES (1,10),(2,20),(3,30),(4,40)").expect("seed price");
    Spi::run("INSERT INTO up_src VALUES (1,'A','ok',5),(2,'A','ok',6),(3,'B','ok',7)").expect("seed src");

    let sql = "SELECT s.id, s.region, s.qty * COALESCE(p.price,0) AS turnover \
               FROM up_src s LEFT JOIN up_price p ON p.id = s.id WHERE s.status = 'ok'";

    // Call create_reflex_ivm via SQL to pass partition_by as ARRAY.
    // The Rust wrapper only supports 6 parameters; the 7th partition_by
    // parameter requires the PostgreSQL overload. Partition by 'region' only,
    // matching the source table's LIST partition strategy.
    let res = Spi::get_one::<String>(
        &format!(
            "SELECT create_reflex_ivm('up_v', '{}', 'id,region', NULL, NULL, NULL, ARRAY['region'])",
            sql.replace("'", "''")
        ),
    )
    .expect("create_reflex_ivm call")
    .expect("create_reflex_ivm result");
    assert_eq!(res, "CREATE REFLEX INCREMENTAL VIEW");

    // Force the COLD dispatch path by setting an impossible wipe_threshold (2.0).
    // With default wipe_threshold (0.5), small changes on small partitions would
    // get classified HOT and take the atomic-swap reconcile path, bypassing the
    // keyed-maintenance code that Phase 2 optimizes. This guard ensures Phase 2's
    // in-place upsert actually exercises the cold path.
    Spi::run("SELECT reflex_set_wipe_threshold('up_v', 2.0::NUMERIC)").expect("force cold");

    assert_imv_correct("up_v", sql);

    // (a) pure-data update: qty changes, key + region unchanged.
    Spi::run("UPDATE up_src SET qty = qty + 100 WHERE id = 1").expect("pure-data");
    assert_imv_correct("up_v", sql);

    // (b) row leaves the filter (status -> not ok) WITHOUT the source row vanishing.
    Spi::run("UPDATE up_src SET status = 'archived' WHERE id = 2").expect("filter-exit");
    assert_imv_correct("up_v", sql);

    // (c) brand-new key entering the filter.
    Spi::run("INSERT INTO up_src VALUES (4,'B','ok',8)").expect("new key");
    assert_imv_correct("up_v", sql);

    // (d) key change: move id=3 to region B->A (the key col `region` changes).
    Spi::run("UPDATE up_src SET region = 'A' WHERE id = 3").expect("key change");
    assert_imv_correct("up_v", sql);

    // (e) delete a key.
    Spi::run("DELETE FROM up_src WHERE id = 1").expect("delete");
    assert_imv_correct("up_v", sql);
}

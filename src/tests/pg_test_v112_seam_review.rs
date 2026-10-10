// The seams between the query text PostgreSQL runs, the names the registry holds
// and the names the analysis compares: every cell is a real IMV over real tables,
// compared both ways with its own query after create and after every write.

/// `ok`, or the first step whose write raised or left the IMV different from `query`.
fn v112_sr_cell(imv: &str, query: &str, key: &str, steps: &[String]) -> String {
    let key_argument = if key.is_empty() {
        String::new()
    } else {
        format!(", '{key}'")
    };
    let created = v112_j_try(&format!(
        "SELECT create_reflex_ivm('{imv}', $v112sel${query}$v112sel${key_argument})"
    ));
    if created != "CREATE REFLEX INCREMENTAL VIEW" {
        return format!("NOT-CREATED {created}");
    }
    let at_create = v112_j_oracle(imv, query);
    if at_create != "0" {
        return format!("create={at_create}");
    }
    for (position, dml) in steps.iter().enumerate() {
        let ran = v112_j_try_dml(dml);
        if ran != "OK" {
            return format!("step{position}={ran} [{dml}]");
        }
        let compared = v112_j_oracle(imv, query);
        if compared != "0" {
            return format!("step{position}={compared} [{dml}]");
        }
    }
    "ok".to_string()
}

fn v112_sr_stored_query(imv: &str) -> String {
    Spi::get_one::<String>(&format!(
        "SELECT sql_query FROM public.__reflex_ivm_reference WHERE name = '{imv}'"
    ))
    .expect("registry")
    .unwrap_or_default()
}

fn v112_sr_registered(imv: &str) -> bool {
    Spi::get_one::<bool>(&format!(
        "SELECT EXISTS (SELECT 1 FROM public.__reflex_ivm_reference WHERE name = '{imv}')"
    ))
    .expect("registry")
    .unwrap_or(false)
}

fn v112_sr_server_version() -> i32 {
    Spi::get_one::<i32>("SELECT current_setting('server_version_num')::int")
        .expect("version")
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// PostgreSQL runs the caller's text
// ---------------------------------------------------------------------------

const V112_SR_VALUE_COLUMNS: &str = "id INT PRIMARY KEY, u INT, v INT, b BOOLEAN, s TEXT";

fn v112_sr_value_table(table: &str) {
    Spi::run(&format!("CREATE TABLE {table}({V112_SR_VALUE_COLUMNS})")).expect("table");
    Spi::run(&format!(
        "INSERT INTO {table} VALUES (1,3,5,true,'aB'),(2,6,NULL,false,'Cd'),(3,NULL,2,NULL,NULL),(4,0,0,true,'')"
    ))
    .expect("seed");
}

fn v112_sr_value_steps(table: &str) -> Vec<String> {
    vec![
        format!("INSERT INTO {table} VALUES (10,7,NULL,true,'Ab'),(11,NULL,31,NULL,NULL),(12,1,1,false,'A')"),
        format!("UPDATE {table} SET v = NULL WHERE id = 1"),
        format!("UPDATE {table} SET v = 4, u = 9 WHERE id = 2"),
        format!("UPDATE {table} SET b = NOT b, s = s || 'Z'"),
        format!("UPDATE {table} SET v = u, u = v WHERE id IN (3, 10, 12)"),
        format!("DELETE FROM {table} WHERE id = 3"),
        format!("DELETE FROM {table} WHERE v IS NULL"),
        format!("INSERT INTO {table} VALUES (20,NULL,NULL,NULL,NULL),(21,2,3,true,'x')"),
    ]
}

/// `(tag, select item as the caller writes it, minimum server version)`.
const V112_SR_SELECT_ITEMS: [(&str, &str, i32); 36] = [
    ("isnull", "v ISNULL", 0),
    ("notnull", "v NOTNULL AS x", 0),
    ("and_isnull", "b AND v ISNULL", 0),
    ("uescape_ident", "u&\"v\" AS x", 0),
    ("uescape_ident_upper", "U&\"v\" AS x", 0),
    ("uescape_ident_qualified", "{t}.U&\"v\" AS x", 0),
    ("uescape_string", "u&'\\0041b' || s AS x", 0),
    ("double_minus", "- -v AS x", 0),
    ("double_minus_spaced", "- - v", 0),
    ("minus_plus", "- +v AS x", 0),
    ("hex", "0x1F + v AS x", 160000),
    ("octal", "0o17 + v AS x", 160000),
    ("binary", "0b101 + v AS x", 160000),
    ("underscore_number", "1_000 + v AS x", 160000),
    ("exponent", "v + 1e3 AS x", 0),
    ("abs_prefix", "@ v AS x", 0),
    ("not_prefix", "~ v AS x", 0),
    ("xor", "v # u AS x", 0),
    ("shift", "v << 2 AS x", 0),
    ("regex", "s !~ '^a' AS x", 0),
    ("regex_nocase", "s ~* 'ab' AS x", 0),
    ("operator_syntax", "v OPERATOR(pg_catalog.+) u AS x", 0),
    ("dollar", "$q$Ab\"C'$q$ || s AS x", 0),
    ("dollar_empty_tag", "$$Id \"Data\"$$ || s AS x", 0),
    ("block_comment", "v /* Id \"x\" 'y */ + 1 AS x", 0),
    ("line_comment", "v -- Id \"x\"\n + 1 AS x", 0),
    ("escape_string", "E'a\\'B\\n' || s AS x", 0),
    ("quote_in_string", "'It''s Id' || s AS x", 0),
    ("cast_colon", "v::INT8 AS x", 0),
    ("cast_typmod", "CAST(v AS NUMERIC(10,2)) AS x", 0),
    ("cast_string", "'12'::int + v AS x", 0),
    ("typed_literal", "DATE '2020-01-01' + v AS x", 0),
    ("not_distinct", "v IS NOT DISTINCT FROM u AS x", 0),
    ("collate", "s COLLATE \"C\" < 'b' AS x", 0),
    ("like_escape", "s LIKE 'a!%' ESCAPE '!' AS x", 0),
    ("keyword_column", "v AS \"select\"", 0),
];

/// The select items the parser does not read, whatever the spelling of the
/// identifiers around them: refused at create, loudly.
const V112_SR_PARSER_REFUSES: [&str; 2] = ["octal", "binary"];

/// The select item as the registry stores it, where that is not as written: a
/// `U&"name"` that holds no escape is the plain name.
fn v112_sr_stored_select_item(tag: &str, written: &str) -> String {
    match tag {
        "uescape_ident" | "uescape_ident_upper" => "v AS x".to_string(),
        "uescape_ident_qualified" => "{t}.v AS x".to_string(),
        _ => written.to_string(),
    }
}

/// `ok`, `refused by the parser`, or what went wrong.
fn v112_sr_select_item_cell(imv: &str, table: &str, query: &str, stored: &str) -> String {
    let outcome = v112_sr_cell(imv, query, "", &v112_sr_value_steps(table));
    if outcome.starts_with("NOT-CREATED ERROR: [reflex-unsupported] Failed to parse SQL") {
        return "refused by the parser".to_string();
    }
    if outcome == "ok" && v112_sr_stored_query(imv) != stored {
        return format!("stored {:?}", v112_sr_stored_query(imv));
    }
    outcome
}

/// Every select item in a keyed passthrough, written with no identifier to fold,
/// with upper-case identifiers, and over a table named by an unreserved keyword
/// written in quotes: the three behave alike, and the stored text is the caller's
/// but for the folded identifiers and the dropped quotes.
#[pg_test]
fn v112_sr_text_select_items() {
    lock_shared_fixtures();
    let version = v112_sr_server_version();
    let mut wrong: Vec<String> = Vec::new();
    for (tag, item, minimum_version) in V112_SR_SELECT_ITEMS {
        if version < minimum_version {
            continue;
        }
        let expected = if V112_SR_PARSER_REFUSES.contains(&tag) {
            "refused by the parser"
        } else {
            "ok"
        };
        let stored_item = v112_sr_stored_select_item(tag, item);

        let plain_table = format!("v112_sr_c1p_{tag}");
        v112_sr_value_table(&plain_table);
        let plain = v112_sr_select_item_cell(
            &format!("v112_sr_c1_plain_{tag}"),
            &plain_table,
            &format!("SELECT id, {} FROM {plain_table}", item.replace("{t}", &plain_table)),
            &format!("SELECT id, {} FROM {plain_table}", stored_item.replace("{t}", &plain_table)),
        );
        if plain != expected {
            wrong.push(format!("{tag} plain: {plain}"));
        }

        let folded_table = format!("v112_sr_c1f_{tag}");
        v112_sr_value_table(&folded_table);
        let upper_table = folded_table.to_uppercase();
        let folded = v112_sr_select_item_cell(
            &format!("v112_sr_c1_folded_{tag}"),
            &folded_table,
            &format!("SELECT Id, {} FROM {upper_table}", item.replace("{t}", &upper_table)),
            &format!("SELECT id, {} FROM {folded_table}", stored_item.replace("{t}", &folded_table)),
        );
        if folded != expected {
            wrong.push(format!("{tag} folded: {folded}"));
        }

        v112_sr_value_table("\"whitespace\"");
        let keyword = v112_sr_select_item_cell(
            &format!("v112_sr_c1_keyword_{tag}"),
            "\"whitespace\"",
            &format!("SELECT id, {} FROM \"whitespace\"", item.replace("{t}", "\"whitespace\"")),
            &format!(
                "SELECT id, {} FROM whitespace",
                stored_item.replace("{t}", "\"whitespace\"")
            ),
        );
        if keyword != expected {
            wrong.push(format!("{tag} keyword: {keyword}"));
        }
        Spi::run("DROP TABLE \"whitespace\" CASCADE").expect("drop keyword table");
    }
    assert!(wrong.is_empty(), "\n{}", wrong.join("\n"));
}

/// An identifier written with a Unicode escape sequence is refused, and the remedy
/// the message gives is maintained.
#[pg_test]
fn v112_sr_unicode_escape_sequence_in_an_identifier_is_refused() {
    v112_sr_value_table("v112_sr_ue");
    for (tag, select) in [
        ("code", "SELECT id, u&\"\\0076\" AS x FROM v112_sr_ue"),
        ("upper", "SELECT Id, U&\"\\0076\" AS x FROM V112_SR_UE"),
        ("uescape", "SELECT id, U&\"!0076\" UESCAPE '!' AS x FROM v112_sr_ue"),
        ("where", "SELECT id, s FROM v112_sr_ue WHERE u&\"\\0076\" > 1"),
    ] {
        let refused = crate::create_reflex_ivm(&format!("v112_sr_ue_{tag}"), select, None, None, None, None);
        assert!(
            refused.starts_with("ERROR: [reflex-unsupported] pg_reflex does not read the identifier")
                && refused.contains("Fix: write the name itself between double quotes, without the U& prefix."),
            "{select}: {refused}"
        );
        assert!(!v112_sr_registered(&format!("v112_sr_ue_{tag}")));
    }
    assert_eq!(
        v112_sr_cell(
            "v112_sr_ue_remedy",
            "SELECT id, \"v\" AS x FROM v112_sr_ue WHERE \"v\" > 1",
            "",
            &v112_sr_value_steps("v112_sr_ue"),
        ),
        "ok"
    );
}

/// `(tag, predicate, minimum server version)`.
const V112_SR_PREDICATES: [(&str, &str, i32); 8] = [
    ("notnull", "v NOTNULL", 0),
    ("uescape_ident", "u&\"v\" > 1", 0),
    ("uescape_ident_upper", "U&\"v\" > 1 AND U&\"u\" < 5", 0),
    ("regex", "s !~ '^a'", 0),
    ("dollar", "s <> $q$Ab\"C'$q$", 0),
    ("comment", "v /* Id \"x\" 'y */ > 1", 0),
    ("escape_string", "s <> E'a\\'B'", 0),
    ("cast", "v::INT8 > '1'::int", 0),
];

/// The same in a WHERE clause and in the FILTER of an aggregate, lower case and
/// upper case.
#[pg_test]
fn v112_sr_text_predicates() {
    let version = v112_sr_server_version();
    let mut wrong: Vec<String> = Vec::new();
    for (tag, predicate, minimum_version) in V112_SR_PREDICATES {
        if version < minimum_version {
            continue;
        }
        for (form, select) in [
            ("where_plain", "SELECT id, v FROM {t} WHERE {p}"),
            ("where_folded", "SELECT Id, V FROM {T} WHERE {p}"),
            ("filter_plain", "SELECT b, COUNT(*) FILTER (WHERE {p}) AS c, COUNT(*) AS n FROM {t} GROUP BY b"),
            ("filter_folded", "SELECT B, COUNT(*) FILTER (WHERE {p}) AS c, COUNT(*) AS n FROM {T} GROUP BY B"),
        ] {
            let table = format!("v112_sr_c1{form}_{tag}");
            v112_sr_value_table(&table);
            let query = select
                .replace("{t}", &table)
                .replace("{T}", &table.to_uppercase())
                .replace("{p}", predicate);
            let outcome = v112_sr_cell(
                &format!("v112_sr_c1_{form}_{tag}"),
                &query,
                "",
                &v112_sr_value_steps(&table),
            );
            if outcome != "ok" {
                wrong.push(format!("{tag} {form}: {outcome}"));
            }
        }
    }
    assert!(wrong.is_empty(), "\n{}", wrong.join("\n"));
}

/// The rows the review printed: `v ISNULL` is a boolean, `u&"v"` is the column `v`.
#[pg_test]
fn v112_sr_text_review_rows() {
    Spi::run("CREATE TABLE v112_sr_rows_i(id INT PRIMARY KEY, v INT)").expect("table");
    Spi::run("INSERT INTO v112_sr_rows_i VALUES (1, 5), (2, NULL)").expect("seed");
    crate::create_reflex_ivm(
        "v112_sr_rows_x",
        "SELECT Id, v ISNULL FROM v112_sr_rows_i",
        None,
        None,
        None,
        None,
    );
    assert_eq!(
        Spi::get_one::<String>(
            "SELECT string_agg(r::text, ' ' ORDER BY r::text) FROM v112_sr_rows_x r"
        )
        .expect("rows")
        .as_deref(),
        Some("(1,f) (2,t)")
    );
    Spi::run("CREATE TABLE v112_sr_rows_u(id INT PRIMARY KEY, u INT, v INT)").expect("table");
    Spi::run("INSERT INTO v112_sr_rows_u VALUES (1, 3, 5)").expect("seed");
    crate::create_reflex_ivm(
        "v112_sr_rows_y",
        "SELECT Id, U&\"v\" AS x FROM v112_sr_rows_u",
        None,
        None,
        None,
        None,
    );
    Spi::run("INSERT INTO v112_sr_rows_u VALUES (2, 1, 9)").expect("write");
    assert_eq!(
        Spi::get_one::<String>(
            "SELECT string_agg(r::text, ' ' ORDER BY r::text) FROM v112_sr_rows_y r"
        )
        .expect("rows")
        .as_deref(),
        Some("(1,5) (2,9)")
    );
}

// ---------------------------------------------------------------------------
// An unquoted identifier is folded the way PostgreSQL folds it
// ---------------------------------------------------------------------------

#[pg_test]
fn v112_sr_postgres_folds_ascii_letters_only_here() {
    assert_eq!(
        Spi::get_one::<String>("SELECT current_setting('server_encoding')")
            .expect("encoding")
            .as_deref(),
        Some("UTF8")
    );
    assert_eq!(
        Spi::get_one::<String>("SELECT (parse_ident('ÉCOLE_Ö'))[1]")
            .expect("fold")
            .as_deref(),
        Some("École_Ö")
    );
}

#[pg_test]
fn v112_sr_non_ascii_upper_case_column() {
    let cells = [
        ("pt", "SELECT id, É AS x FROM {t}"),
        ("pt_folded", "SELECT Id, É AS x, é AS y FROM {T}"),
        ("pt_bare", "SELECT id, É FROM {t}"),
        ("agg", "SELECT g, SUM(É) AS s, SUM(é) AS t FROM {t} GROUP BY g"),
        ("agg_key", "SELECT É, COUNT(*) AS c FROM {t} GROUP BY É"),
        ("agg_key_folded", "SELECT É, MAX(é) AS m, COUNT(*) AS c FROM {T} X GROUP BY É"),
        ("where", "SELECT id, g FROM {t} WHERE É > 50"),
        ("qualified", "SELECT X.id, X.É AS x FROM {t} X"),
    ];
    let mut wrong: Vec<String> = Vec::new();
    for (tag, select) in cells {
        let table = format!("v112_sr_c2_e_{tag}");
        Spi::run(&format!(
            "CREATE TABLE {table}(id INT PRIMARY KEY, \"É\" INT, \"é\" INT, g TEXT)"
        ))
        .expect("table");
        Spi::run(&format!(
            "INSERT INTO {table} VALUES (1, 100, 200, 'a'), (2, 300, 400, 'b')"
        ))
        .expect("seed");
        let steps = vec![
            format!("INSERT INTO {table} VALUES (3, 5, 6, 'a')"),
            format!("UPDATE {table} SET \"É\" = \"É\" + 1 WHERE id = 1"),
            format!("UPDATE {table} SET \"é\" = \"é\" + 1 WHERE id = 2"),
            format!("UPDATE {table} SET \"É\" = \"é\", \"é\" = \"É\" WHERE id = 3"),
            format!("DELETE FROM {table} WHERE id = 2"),
        ];
        let query = select
            .replace("{t}", &table)
            .replace("{T}", &table.to_ascii_uppercase());
        let outcome = v112_sr_cell(&format!("v112_sr_c2_e_{tag}_imv"), &query, "", &steps);
        if outcome != "ok" {
            wrong.push(format!("{tag}: {outcome}"));
        }
    }
    assert!(wrong.is_empty(), "\n{}", wrong.join("\n"));
}

#[pg_test]
fn v112_sr_non_ascii_upper_case_only_column_and_table() {
    for (tag, select) in [
        ("pt", "SELECT id, Ö AS x FROM {t}"),
        ("pt_upper", "SELECT ID, Ö AS x FROM {T}"),
        ("agg", "SELECT Ö, COUNT(*) AS c FROM {T} X GROUP BY Ö"),
    ] {
        let table = format!("v112_sr_c2_{tag}_Öt");
        Spi::run(&format!("CREATE TABLE {table}(id INT PRIMARY KEY, Ö INT)")).expect("table");
        Spi::run(&format!("INSERT INTO {table} VALUES (1, 10), (2, 20)")).expect("seed");
        let steps = vec![
            format!("INSERT INTO {table} VALUES (3, 5)"),
            format!("UPDATE {table} SET Ö = Ö + 1 WHERE id = 1"),
            format!("DELETE FROM {table} WHERE id = 2"),
        ];
        let query = select
            .replace("{t}", &table)
            .replace("{T}", &table.to_ascii_uppercase());
        assert_eq!(
            v112_sr_cell(&format!("v112_sr_c2_o_{tag}"), &query, "", &steps),
            "ok",
            "{query}"
        );
    }
}

// ---------------------------------------------------------------------------
// Dropping a source named by a keyword
// ---------------------------------------------------------------------------

/// A table written `written` in FROM and created as `table`, read by a keyed
/// passthrough, by a join with another table and by an IMV over the first IMV:
/// all are maintained, all go when the table is dropped, and the other table then
/// takes writes.
fn v112_sr_keyword_source_dropped(tag: &str, table: &str, written: &str) -> String {
    let other = format!("v112_sr_i1_{tag}_p");
    Spi::run(&format!("CREATE TABLE {table}(id INT PRIMARY KEY, v INT)")).expect("keyword table");
    Spi::run(&format!("INSERT INTO {table} VALUES (1, 10), (2, -20), (3, 30)")).expect("seed");
    Spi::run(&format!("CREATE TABLE {other}(id INT PRIMARY KEY, label TEXT)")).expect("other");
    Spi::run(&format!("INSERT INTO {other} VALUES (1, 'one'), (2, 'two'), (4, 'four')")).expect("seed");
    let single = format!("v112_sr_i1_{tag}_single");
    let joined = format!("v112_sr_i1_{tag}_joined");
    let grouped = format!("v112_sr_i1_{tag}_grouped");
    let chained = format!("v112_sr_i1_{tag}_chained");
    let qualified = format!("v112_sr_i1_{tag}_qualified");
    let steps = vec![
        format!("INSERT INTO {table} VALUES (4, 40), (5, 50)"),
        format!("UPDATE {table} SET v = v + 1 WHERE id IN (1, 4)"),
        format!("UPDATE {other} SET label = label || '!' WHERE id < 4"),
        format!("DELETE FROM {table} WHERE id = 2"),
        format!("INSERT INTO {other} VALUES (5, 'five')"),
    ];
    let qualifier = written.rsplit('.').next().unwrap_or(written);
    let cells = [
        (&single, format!("SELECT id, v FROM {written}"), Vec::new()),
        (
            &joined,
            format!("SELECT p.id, p.label, k.v FROM {written} k JOIN {other} p ON p.id = k.id"),
            Vec::new(),
        ),
        (
            &qualified,
            format!(
                "SELECT {qualifier}.id, {qualifier}.v, p.label FROM {written} JOIN {other} p ON p.id = {qualifier}.id WHERE {qualifier}.v > 0"
            ),
            Vec::new(),
        ),
        (
            &grouped,
            format!("SELECT v > 0 AS positive, COUNT(*) AS c, SUM(v) AS s FROM {written} GROUP BY v > 0"),
            Vec::new(),
        ),
        (
            &chained,
            format!("SELECT a.id, a.v, p.label FROM {single} a JOIN {other} p ON p.id = a.id"),
            steps,
        ),
    ];
    let mut queries: Vec<(&String, String)> = Vec::new();
    for (imv, query, steps) in &cells {
        let outcome = v112_sr_cell(imv, query, "", steps);
        if outcome != "ok" {
            return format!("{imv}: {outcome}");
        }
        queries.push((imv, query.clone()));
    }
    for (imv, query) in &queries {
        let compared = v112_j_oracle(imv, query);
        if compared != "0" {
            return format!("{imv} after the writes: {compared}");
        }
    }
    let dropped = v112_j_try_dml(&format!("DROP TABLE {table} CASCADE"));
    if dropped != "OK" {
        return format!("drop: {dropped}");
    }
    let left: Vec<&String> = queries
        .iter()
        .map(|(imv, _)| *imv)
        .filter(|imv| v112_sr_registered(imv))
        .collect();
    if !left.is_empty() {
        return format!("left registered after the drop: {left:?}");
    }
    for dml in [
        format!("UPDATE {other} SET label = 'uno' WHERE id = 1"),
        format!("INSERT INTO {other} VALUES (9, 'nine')"),
        format!("DELETE FROM {other} WHERE id = 2"),
    ] {
        let ran = v112_j_try_dml(&dml);
        if ran != "OK" {
            return format!("write after the drop: {ran}");
        }
    }
    "ok".to_string()
}

macro_rules! v112_sr_keyword_drop_cells {
    ($($name:ident => ($table:expr, $written:expr),)+) => {
        $(
            #[pg_test]
            fn $name() {
                lock_shared_fixtures();
                let tag = stringify!($name).trim_start_matches("v112_sr_drop_");
                assert_eq!(v112_sr_keyword_source_dropped(tag, $table, $written), "ok");
            }
        )+
    };
}

v112_sr_keyword_drop_cells! {
    v112_sr_drop_column_name_keyword_quoted => ("\"overlay\"", "\"overlay\""),
    v112_sr_drop_column_name_keyword_bare => ("\"overlay\"", "overlay"),
    v112_sr_drop_column_name_keyword_upper => ("\"overlay\"", "OVERLAY"),
    v112_sr_drop_column_name_keyword_public => ("\"overlay\"", "public.\"overlay\""),
    v112_sr_drop_position_quoted => ("\"position\"", "\"position\""),
    v112_sr_drop_values_quoted => ("\"values\"", "\"values\""),
    v112_sr_drop_time_bare => ("\"time\"", "time"),
    v112_sr_drop_unreserved_keyword_quoted => ("\"whitespace\"", "\"whitespace\""),
    v112_sr_drop_unreserved_keyword_bare => ("\"whitespace\"", "whitespace"),
    v112_sr_drop_unreserved_keyword_upper => ("\"whitespace\"", "WhiteSpace"),
    v112_sr_drop_reserved_keyword => ("\"lateral\"", "\"lateral\""),
    v112_sr_drop_type_function_keyword => ("\"tablesample\"", "\"tablesample\""),
    v112_sr_drop_plain_name_quoted => ("v112_sr_i1_plain_t", "\"v112_sr_i1_plain_t\""),
    v112_sr_drop_mixed_case_name => ("\"V112_sr_i1_Mixed\"", "\"V112_sr_i1_Mixed\""),
}

/// The review's statement sequence.
#[pg_test]
fn v112_sr_drop_position_join_then_write_the_other_table() {
    lock_shared_fixtures();
    Spi::run("CREATE TABLE \"position\"(id INT PRIMARY KEY, v INT)").expect("table");
    Spi::run("CREATE TABLE v112_sr_i1_review_p(id INT PRIMARY KEY, label TEXT)").expect("table");
    Spi::run("INSERT INTO \"position\" VALUES (1, 1)").expect("seed");
    Spi::run("INSERT INTO v112_sr_i1_review_p VALUES (1, 'one')").expect("seed");
    crate::create_reflex_ivm(
        "v112_sr_i1_review_pv",
        "SELECT p.label, \"position\".v FROM \"position\" JOIN v112_sr_i1_review_p p ON p.id = \"position\".id",
        None,
        None,
        None,
        None,
    );
    Spi::run("DROP TABLE \"position\" CASCADE").expect("drop");
    assert!(!v112_sr_registered("v112_sr_i1_review_pv"));
    Spi::run("UPDATE v112_sr_i1_review_p SET label = 'uno' WHERE id = 1").expect("write after drop");
}

// ---------------------------------------------------------------------------
// A group column and a join column that share a name
// ---------------------------------------------------------------------------

fn v112_sr_fact_and_dimension(tag: &str) -> (String, String, Vec<String>) {
    let fact = format!("v112_sr_{tag}_f");
    let dimension = format!("v112_sr_{tag}_d");
    Spi::run(&format!(
        "CREATE TABLE {fact}(id INT PRIMARY KEY, k INT, aid INT, v INT)"
    ))
    .expect("fact");
    Spi::run(&format!(
        "CREATE TABLE {dimension}(id INT PRIMARY KEY, k INT, v INT)"
    ))
    .expect("dimension");
    Spi::run(&format!(
        "INSERT INTO {fact} VALUES (1,1,1,10),(2,1,2,20),(3,2,1,30),(4,3,2,40),(5,NULL,1,50)"
    ))
    .expect("seed");
    Spi::run(&format!(
        "INSERT INTO {dimension} VALUES (1,2,100),(2,1,200),(3,3,300)"
    ))
    .expect("seed");
    let steps = vec![
        format!("DELETE FROM {dimension} WHERE id = 2"),
        format!("INSERT INTO {dimension} VALUES (2, 7, 250)"),
        format!("UPDATE {dimension} SET k = 9 WHERE id = 1"),
        format!("UPDATE {dimension} SET v = v + 1"),
        format!("UPDATE {dimension} SET id = 4 WHERE id = 3"),
        format!("INSERT INTO {fact} VALUES (6,2,2,60),(7,4,1,70)"),
        format!("UPDATE {fact} SET k = 1 WHERE id = 3"),
        format!("UPDATE {fact} SET v = v + 5 WHERE k = 1"),
        format!("DELETE FROM {fact} WHERE id IN (1, 6)"),
        format!("DELETE FROM {dimension} WHERE id IN (1, 4)"),
        format!("INSERT INTO {dimension} VALUES (1, 1, 1), (3, 1, 3)"),
        format!("DELETE FROM {dimension}"),
        format!("INSERT INTO {dimension} VALUES (1, 5, 1), (2, 5, 2)"),
        format!("DELETE FROM {fact}"),
    ];
    (fact, dimension, steps)
}

macro_rules! v112_sr_shared_name_cells {
    ($($name:ident => $select:expr,)+) => {
        $(
            #[pg_test]
            fn $name() {
                let tag = stringify!($name).trim_start_matches("v112_sr_");
                let (fact, dimension, steps) = v112_sr_fact_and_dimension(tag);
                let query = $select.replace("{f}", &fact).replace("{d}", &dimension);
                assert_eq!(v112_sr_cell(&format!("v112_sr_{tag}"), &query, "", &steps), "ok", "{query}");
            }
        )+
    };
}

v112_sr_shared_name_cells! {
    v112_sr_p1_inner => "SELECT d.k, SUM(f.v) AS s, COUNT(*) AS c FROM {f} f JOIN {d} d ON d.id = f.k GROUP BY d.k",
    v112_sr_p1_inner_reversed => "SELECT d.k, SUM(f.v) AS s, COUNT(*) AS c FROM {f} f JOIN {d} d ON f.k = d.id GROUP BY d.k",
    v112_sr_p1_dimension_first => "SELECT d.k, SUM(f.v) AS s, COUNT(*) AS c FROM {d} d JOIN {f} f ON d.id = f.k GROUP BY d.k",
    v112_sr_p1_no_alias => "SELECT {d}.k, SUM({f}.v) AS s, COUNT(*) AS c FROM {f} JOIN {d} ON {d}.id = {f}.k GROUP BY {d}.k",
    v112_sr_p1_where => "SELECT d.k, SUM(f.v) AS s, COUNT(*) AS c FROM {f} f JOIN {d} d ON d.id = f.k WHERE d.v > 150 GROUP BY d.k",
    v112_sr_p1_left => "SELECT d.k, SUM(f.v) AS s, COUNT(*) AS c FROM {f} f LEFT JOIN {d} d ON d.id = f.k GROUP BY d.k",
    v112_sr_p1_three_tables => "SELECT a.k, SUM(f.v) AS s, COUNT(*) AS c FROM {f} f JOIN {d} a ON a.id = f.aid JOIN {d} d ON d.id = f.k GROUP BY a.k",
    v112_sr_p1_minmax => "SELECT d.k, MIN(f.v) AS mn, MAX(f.v) AS mx FROM {f} f JOIN {d} d ON d.id = f.k GROUP BY d.k",
    v112_sr_p1_group_by_the_join_column => "SELECT d.id, SUM(f.v) AS s, COUNT(*) AS c FROM {f} f JOIN {d} d ON d.id = f.k GROUP BY d.id",
    v112_sr_p1_group_by_the_fact_side => "SELECT f.k, SUM(f.v) AS s, SUM(d.v) AS t, COUNT(*) AS c FROM {f} f JOIN {d} d ON d.id = f.k GROUP BY f.k",
    v112_sr_p1_unqualified_group => "SELECT aid, SUM(d.v) AS t, COUNT(*) AS c FROM {f} f JOIN {d} d ON d.id = aid GROUP BY aid",
}

fn v112_sr_quoted_case_tables(tag: &str) -> (String, String, Vec<String>) {
    let left = format!("v112_sr_{tag}_q");
    let right = format!("v112_sr_{tag}_r");
    Spi::run(&format!(
        "CREATE TABLE {left}(id INT PRIMARY KEY, \"ID\" INT, v INT)"
    ))
    .expect("left");
    Spi::run(&format!(
        "CREATE TABLE {right}(id INT PRIMARY KEY, \"ID\" INT)"
    ))
    .expect("right");
    Spi::run(&format!(
        "INSERT INTO {left} VALUES (1,7,10),(2,7,20),(3,8,30),(4,9,40),(5,NULL,50)"
    ))
    .expect("seed");
    Spi::run(&format!(
        "INSERT INTO {right} VALUES (1,100),(2,200),(3,300),(4,7),(6,8)"
    ))
    .expect("seed");
    let steps = vec![
        format!("DELETE FROM {right} WHERE id = 4"),
        format!("INSERT INTO {right} VALUES (4, 9), (5, 7)"),
        format!("UPDATE {right} SET \"ID\" = \"ID\" + 1 WHERE id < 3"),
        format!("UPDATE {right} SET id = 7 WHERE id = 3"),
        format!("UPDATE {left} SET \"ID\" = 8 WHERE id = 1"),
        format!("UPDATE {left} SET id = 13 WHERE id = 5"),
        format!("DELETE FROM {left} WHERE id = 2"),
        format!("INSERT INTO {left} VALUES (2, 9, 5), (6, 8, 6)"),
        format!("DELETE FROM {right}"),
        format!("INSERT INTO {right} VALUES (1, 1), (2, 2), (6, 6)"),
        format!("DELETE FROM {left}"),
    ];
    (left, right, steps)
}

macro_rules! v112_sr_quoted_case_cells {
    ($($name:ident => $select:expr,)+) => {
        $(
            #[pg_test]
            fn $name() {
                let tag = stringify!($name).trim_start_matches("v112_sr_");
                let (left, right, steps) = v112_sr_quoted_case_tables(tag);
                let query = $select.replace("{q}", &left).replace("{r}", &right);
                assert_eq!(v112_sr_cell(&format!("v112_sr_{tag}"), &query, "", &steps), "ok", "{query}");
            }
        )+
    };
}

v112_sr_quoted_case_cells! {
    v112_sr_i4_review => "SELECT \"a\".\"ID\", SUM(\"b\".\"ID\") AS s FROM \"{q}\" \"a\" JOIN \"{r}\" \"b\" ON \"a\".\"id\" = \"b\".\"id\" GROUP BY \"a\".\"ID\"",
    v112_sr_i4_bare_aliases => "SELECT a.\"ID\", SUM(b.\"ID\") AS s, COUNT(*) AS c FROM {q} a JOIN {r} b ON a.id = b.id GROUP BY a.\"ID\"",
    v112_sr_i4_join_on_the_quoted_column => "SELECT a.\"ID\", SUM(a.v) AS s, COUNT(*) AS c FROM {q} a JOIN {r} b ON a.\"ID\" = b.\"ID\" GROUP BY a.\"ID\"",
    v112_sr_i4_join_quoted_to_bare => "SELECT a.\"ID\", SUM(a.v) AS s, COUNT(*) AS c FROM {q} a JOIN {r} b ON a.\"ID\" = b.id GROUP BY a.\"ID\"",
    v112_sr_i4_group_by_bare_join_on_quoted => "SELECT a.id, SUM(b.\"ID\") AS s, COUNT(*) AS c FROM {q} a JOIN {r} b ON a.id = b.\"ID\" GROUP BY a.id",
    v112_sr_i4_group_by_the_other_side => "SELECT b.\"ID\", SUM(a.v) AS s, COUNT(*) AS c FROM {q} a JOIN {r} b ON a.id = b.id GROUP BY b.\"ID\"",
    v112_sr_i4_upper_case_unquoted => "SELECT A.ID, SUM(B.\"ID\") AS s, COUNT(*) AS c FROM {q} A JOIN {r} B ON A.ID = B.ID GROUP BY A.ID",
    v112_sr_i4_lower_case => "SELECT a.id, SUM(b.\"ID\") AS s, COUNT(*) AS c FROM {q} a JOIN {r} b ON a.id = b.id GROUP BY a.id",
}

// ---------------------------------------------------------------------------
// A table in a mixed-case schema
// ---------------------------------------------------------------------------

fn v112_sr_mixed_case_schema_cell(tag: &str, select: &str) -> String {
    let lower = format!("v112_sr_p4{tag}_s");
    let mixed = format!("\"v112_sr_p4{tag}_S\"");
    for schema in [&lower, &mixed] {
        Spi::run(&format!("CREATE SCHEMA {schema}")).expect("schema");
    }
    Spi::run(&format!("CREATE TABLE {lower}.t(id INT PRIMARY KEY, g TEXT, v INT)")).expect("lower");
    Spi::run(&format!("CREATE TABLE {mixed}.\"T\"(id INT PRIMARY KEY, g TEXT, v INT)")).expect("mixed");
    Spi::run(&format!("CREATE TABLE {mixed}.u(id INT PRIMARY KEY, g TEXT, v INT)")).expect("mixed lower");
    for table in [format!("{lower}.t"), format!("{mixed}.\"T\""), format!("{mixed}.u")] {
        Spi::run(&format!("INSERT INTO {table} VALUES (1,'a',10),(2,'a',20),(3,'b',30)")).expect("seed");
    }
    let mut steps = Vec::new();
    for table in [format!("{mixed}.\"T\""), format!("{lower}.t"), format!("{mixed}.u")] {
        steps.push(format!("INSERT INTO {table} VALUES (4,'b',40),(5,'c',50)"));
        steps.push(format!("UPDATE {table} SET v = v + 1 WHERE id IN (1, 4)"));
        steps.push(format!("UPDATE {table} SET g = 'c' WHERE id = 2"));
        steps.push(format!("DELETE FROM {table} WHERE id = 3"));
        steps.push(format!("UPDATE {table} SET id = 30 WHERE id = 5"));
    }
    for table in [format!("{mixed}.\"T\""), format!("{lower}.t"), format!("{mixed}.u")] {
        steps.push(format!("DELETE FROM {table}"));
        steps.push(format!("INSERT INTO {table} VALUES (1,'z',1),(7,'y',7)"));
    }
    let query = select
        .replace("{S.T}", &format!("{mixed}.\"T\""))
        .replace("{S.u}", &format!("{mixed}.u"))
        .replace("{s.t}", &format!("{lower}.t"));
    v112_sr_cell(&format!("v112_sr_p4{tag}_imv"), &query, "", &steps)
}

macro_rules! v112_sr_mixed_case_schema_cells {
    ($($name:ident => $select:expr,)+) => {
        $(
            #[pg_test]
            fn $name() {
                let tag = stringify!($name).trim_start_matches("v112_sr_p4");
                assert_eq!(v112_sr_mixed_case_schema_cell(tag, $select), "ok", "{}", $select);
            }
        )+
    };
}

v112_sr_mixed_case_schema_cells! {
    v112_sr_p4_passthrough => "SELECT id, v FROM {S.T}",
    v112_sr_p4_aggregate => "SELECT g, SUM(v) AS s FROM {S.T} GROUP BY g",
    v112_sr_p4_join => "SELECT a.id, b.v AS bv FROM {s.t} a JOIN {S.T} b ON b.id = a.id",
    v112_sr_p4_join_aggregate => "SELECT a.g, SUM(b.v) AS s, COUNT(*) AS c FROM {s.t} a JOIN {S.T} b ON b.id = a.id GROUP BY a.g",
    v112_sr_p4_lower_table => "SELECT id, v FROM {S.u}",
    v112_sr_p4_lower_table_aggregate => "SELECT g, SUM(v) AS s, COUNT(*) AS c FROM {S.u} GROUP BY g",
    v112_sr_p4_both_join => "SELECT a.id, a.v, b.v AS bv FROM {S.u} a JOIN {S.T} b ON b.id = a.id",
    v112_sr_p4_alias => "SELECT x.id, x.g, x.v FROM {S.T} AS x WHERE x.v > 5",
    v112_sr_p4_min_max => "SELECT g, MIN(v) AS mn, MAX(v) AS mx FROM {S.T} GROUP BY g",
}

// ---------------------------------------------------------------------------
// Quotes glued to a keyword; a user function named like an aggregate
// ---------------------------------------------------------------------------

#[pg_test]
fn v112_sr_quotes_glued_to_keywords() {
    for (tag, select) in [
        ("all", "SELECT\"id\",\"v\"FROM\"{t}\""),
        ("from", "SELECT id, v FROM\"{t}\""),
        ("where", "SELECT id, v FROM\"{t}\"WHERE\"v\">0"),
        ("alias", "SELECT\"a\".\"id\",\"a\".\"v\"FROM\"{t}\"AS\"a\""),
        ("aggregate", "SELECT\"g\",SUM(\"v\")AS\"s\"FROM\"{t}\"GROUP BY\"g\""),
    ] {
        let table = format!("v112_sr_m1_{tag}");
        Spi::run(&format!("CREATE TABLE {table}(id INT PRIMARY KEY, g TEXT, v INT)")).expect("table");
        Spi::run(&format!("INSERT INTO {table} VALUES (1,'a',10),(2,'a',-20),(3,'b',30)")).expect("seed");
        let steps = vec![
            format!("INSERT INTO {table} VALUES (4,'b',40),(5,'c',50)"),
            format!("UPDATE {table} SET v = v + 1 WHERE id IN (1, 4)"),
            format!("UPDATE {table} SET g = 'c', v = -v WHERE id = 2"),
            format!("DELETE FROM {table} WHERE id = 3"),
            format!("DELETE FROM {table}"),
        ];
        let query = select.replace("{t}", &table);
        assert_eq!(
            v112_sr_cell(&format!("v112_sr_m1_{tag}_imv"), &query, "", &steps),
            "ok",
            "{query}"
        );
    }
}

#[pg_test]
fn v112_sr_user_function_named_like_an_aggregate() {
    lock_shared_fixtures();
    Spi::run(
        "CREATE FUNCTION public.sum(a INT, b INT) RETURNS INT LANGUAGE sql IMMUTABLE AS 'SELECT a + b'",
    )
    .expect("function");
    Spi::run(
        "CREATE FUNCTION public.max(a INT, b INT, c INT) RETURNS INT LANGUAGE sql IMMUTABLE AS 'SELECT greatest(a, b, c)'",
    )
    .expect("function");
    for (tag, select) in [
        ("unqualified", "SELECT id, sum(v, w) AS x FROM {t}"),
        ("upper", "SELECT id, SUM(v, w) AS x FROM {t}"),
        ("qualified", "SELECT id, public.sum(v, w) AS x FROM {t}"),
        ("three", "SELECT id, max(v, w, id) AS x FROM {t}"),
        ("inside_aggregate", "SELECT g, SUM(sum(v, w)) AS s, COUNT(*) AS c FROM {t} GROUP BY g"),
        ("beside_aggregate", "SELECT g, sum(MIN(v), MAX(w)) AS x, SUM(v) AS s FROM {t} GROUP BY g"),
    ] {
        let table = format!("v112_sr_m2_{tag}");
        Spi::run(&format!("CREATE TABLE {table}(id INT PRIMARY KEY, g TEXT, v INT, w INT)")).expect("table");
        Spi::run(&format!("INSERT INTO {table} VALUES (1,'a',10,1),(2,'a',-20,NULL),(3,'b',30,3)")).expect("seed");
        let steps = vec![
            format!("INSERT INTO {table} VALUES (4,'b',40,4),(5,'c',50,5)"),
            format!("UPDATE {table} SET v = v + 1, w = w + 2 WHERE id IN (1, 4)"),
            format!("UPDATE {table} SET g = 'c' WHERE id = 2"),
            format!("DELETE FROM {table} WHERE id = 3"),
            format!("DELETE FROM {table}"),
        ];
        let query = select.replace("{t}", &table);
        assert_eq!(
            v112_sr_cell(&format!("v112_sr_m2_{tag}_imv"), &query, "", &steps),
            "ok",
            "{query}"
        );
    }
}

/// The built-in aggregate stays one whatever functions share its name.
#[pg_test]
fn v112_sr_one_argument_call_stays_the_builtin_aggregate() {
    Spi::run("CREATE TABLE v112_sr_m2_builtin(id INT PRIMARY KEY, v INT)").expect("table");
    let refused = crate::create_reflex_ivm(
        "v112_sr_m2_builtin_imv",
        "SELECT id, sum(v) AS x FROM v112_sr_m2_builtin",
        None,
        None,
        None,
        None,
    );
    assert!(refused.starts_with("ERROR: [reflex-unsupported]"), "{refused}");
}

// ---------------------------------------------------------------------------
// An alias declared again inside a subquery
// ---------------------------------------------------------------------------

fn v112_sr_alias_tables(tag: &str) -> (String, String, Vec<String>) {
    let outer = format!("v112_sr_{tag}_o");
    let inner = format!("v112_sr_{tag}_c");
    Spi::run(&format!(
        "CREATE TABLE {outer}(id INT PRIMARY KEY, cid INT, amount INT, name TEXT)"
    ))
    .expect("outer");
    Spi::run(&format!(
        "CREATE TABLE {inner}(id INT PRIMARY KEY, active BOOLEAN, amount INT)"
    ))
    .expect("inner");
    Spi::run(&format!(
        "INSERT INTO {outer} VALUES (1,1,10,'a'),(2,2,20,'b'),(3,3,30,'a'),(4,9,40,'c')"
    ))
    .expect("seed");
    Spi::run(&format!(
        "INSERT INTO {inner} VALUES (1,true,1),(2,true,2),(3,false,3)"
    ))
    .expect("seed");
    let steps = vec![
        format!("UPDATE {outer} SET amount = 99 WHERE id = 1"),
        format!("UPDATE {outer} SET name = 'z' WHERE id = 2"),
        format!("UPDATE {outer} SET cid = 2 WHERE id = 3"),
        format!("UPDATE {inner} SET active = NOT active WHERE id IN (2, 3)"),
        format!("UPDATE {inner} SET amount = amount + 100"),
        format!("INSERT INTO {outer} VALUES (5,3,50,'d')"),
        format!("DELETE FROM {inner} WHERE id = 1"),
        format!("UPDATE {outer} SET amount = amount + 1"),
        format!("DELETE FROM {outer} WHERE id = 2"),
    ];
    (outer, inner, steps)
}

macro_rules! v112_sr_alias_cells {
    ($($name:ident => $select:expr,)+) => {
        $(
            #[pg_test]
            fn $name() {
                let tag = stringify!($name).trim_start_matches("v112_sr_");
                let (outer, inner, steps) = v112_sr_alias_tables(tag);
                let query = $select.replace("{o}", &outer).replace("{c}", &inner);
                assert_eq!(v112_sr_cell(&format!("v112_sr_{tag}"), &query, "", &steps), "ok", "{query}");
            }
        )+
    };
}

v112_sr_alias_cells! {
    v112_sr_p2_review => "SELECT id, t.amount FROM {o} t WHERE t.cid IN (SELECT t.id FROM {c} t WHERE t.active)",
    v112_sr_p2_exists => "SELECT id, t.amount, t.name FROM {o} t WHERE EXISTS (SELECT 1 FROM {c} t WHERE t.active)",
    v112_sr_p2_aggregate => "SELECT name, SUM(t.amount) AS s, COUNT(*) AS c FROM {o} t WHERE t.cid IN (SELECT t.id FROM {c} t) GROUP BY name",
    v112_sr_p2_derived => "SELECT o.id, t.amount FROM {o} t JOIN (SELECT t.id FROM {c} t WHERE t.active) q ON q.id = t.cid JOIN {o} o ON o.id = t.id",
    v112_sr_p2_alias_named_like_the_outer_table => "SELECT {o}.id, {o}.amount FROM {o} WHERE {o}.cid IN (SELECT {o}.id FROM {c} {o} WHERE {o}.active)",
    v112_sr_p2_distinct_aliases => "SELECT id, t.amount FROM {o} t WHERE t.cid IN (SELECT u.id FROM {c} u WHERE u.active)",
}

// ---------------------------------------------------------------------------
// The remedy printed for a COUNT(DISTINCT) held by HAVING alone
// ---------------------------------------------------------------------------

#[pg_test]
fn v112_sr_unselected_count_distinct_remedy_keeps_the_other_aggregates() {
    Spi::run("CREATE TABLE v112_sr_cd(id SERIAL PRIMARY KEY, g TEXT, v INT)").expect("table");
    Spi::run(
        "INSERT INTO v112_sr_cd(g, v) VALUES ('a',1),('a',1),('a',2),('b',5),(NULL,7),(NULL,8),('c',NULL),('c',3)",
    )
    .expect("seed");
    let written = "SELECT g, SUM(v) AS s, COUNT(*) AS c FROM v112_sr_cd GROUP BY g HAVING COUNT(DISTINCT v) > 1";
    let refused = crate::create_reflex_ivm("v112_sr_cd_refused", written, None, None, None, None);
    assert!(refused.starts_with("ERROR: [reflex-unsupported] HAVING holds COUNT(DISTINCT v)"), "{refused}");
    for part in [
        "WITH cd AS (SELECT grp, COUNT(DISTINCT col) AS n FROM t GROUP BY grp), \
         agg AS (SELECT grp, SUM(x) AS total FROM t GROUP BY grp) \
         SELECT agg.grp, agg.total FROM agg JOIN cd ON cd.grp IS NOT DISTINCT FROM agg.grp WHERE cd.n > 1",
    ] {
        assert!(refused.contains(part), "{refused}");
    }
    let rewritten = "WITH cd AS (SELECT g, COUNT(DISTINCT v) AS n FROM v112_sr_cd GROUP BY g), \
         agg AS (SELECT g, SUM(v) AS s, COUNT(*) AS c FROM v112_sr_cd GROUP BY g) \
         SELECT agg.g, agg.s, agg.c FROM agg JOIN cd ON cd.g IS NOT DISTINCT FROM agg.g WHERE cd.n > 1";
    let steps = vec![
        "INSERT INTO v112_sr_cd(g, v) VALUES ('b',6),('d',1),('d',1),(NULL,7)".to_string(),
        "UPDATE v112_sr_cd SET v = 1 WHERE g = 'a'".to_string(),
        "UPDATE v112_sr_cd SET g = 'd', v = 9 WHERE g = 'c' AND v = 3".to_string(),
        "DELETE FROM v112_sr_cd WHERE g IS NULL AND v = 8".to_string(),
        "UPDATE v112_sr_cd SET g = NULL WHERE g = 'b'".to_string(),
        "DELETE FROM v112_sr_cd WHERE g = 'd'".to_string(),
        "DELETE FROM v112_sr_cd".to_string(),
        "INSERT INTO v112_sr_cd(g, v) VALUES (NULL,1),(NULL,2),('a',1)".to_string(),
    ];
    let created = v112_j_try(&format!(
        "SELECT create_reflex_ivm('v112_sr_cd_rewritten', $v112sel${rewritten}$v112sel$)"
    ));
    assert_eq!(created, "CREATE REFLEX INCREMENTAL VIEW");
    assert_eq!(v112_j_oracle("v112_sr_cd_rewritten", written), "0", "create");
    for dml in steps {
        assert_eq!(v112_j_try_dml(&dml), "OK", "{dml}");
        assert_eq!(v112_j_oracle("v112_sr_cd_rewritten", written), "0", "{dml}");
    }
}

// ---------------------------------------------------------------------------
// A comment inside the join keyword of a keyed passthrough that filters the
// null-extended table
// ---------------------------------------------------------------------------

v112_jr_keyed_cells! { false;
    v112_sr_jc_kp_lj_admits_null => "SELECT d.kind, d.label, f.v FROM {d} d LEFT /* c */ JOIN {f} f ON d.kind = f.kind WHERE f.v > 2 OR f.v IS NULL",
    v112_sr_jc_kp_lj_only_null => "SELECT d.kind, d.label FROM {d} d LEFT /* RIGHT JOIN */ JOIN {f} f ON d.kind = f.kind WHERE f.kind IS NULL",
    v112_sr_jc_kp_lj_rejects_null => "SELECT d.kind, d.label, f.v FROM {d} d LEFT -- c\n JOIN {f} f ON d.kind = f.kind WHERE f.v > 2",
    v112_sr_jc_kp_lj_outer_coalesce => "SELECT d.kind, d.label, f.v FROM {d} d LEFT /* c */ OUTER /* c */ JOIN {f} f ON d.kind = f.kind WHERE COALESCE(f.v, 0) < 3",
    v112_sr_jc_kp_rj_admits_null => "SELECT d.kind, d.label, f.v FROM {f} f RIGHT /* c */ JOIN {d} d ON d.kind = f.kind WHERE f.v > 2 OR f.v IS NULL",
    v112_sr_jc_kp_rj_only_null => "SELECT d.kind, d.label FROM {f} f RIGHT /* LEFT JOIN */ OUTER JOIN {d} d ON d.kind = f.kind WHERE f.kind IS NULL",
    v112_sr_jc_kp_rj_rejects_null => "SELECT d.kind, d.label, f.v FROM {f} f RIGHT -- c\n OUTER JOIN {d} d ON d.kind = f.kind WHERE f.v > 2",
    v112_sr_jc_kp_lj_dim_filter => "SELECT d.kind, d.label, f.v FROM {d} d LEFT /* c */ JOIN {f} f ON d.kind = f.kind WHERE d.label <> 'B'",
}

v112_jr_keyed_cells! { true;
    v112_sr_jc_kp_lj_admits_null_deferred => "SELECT d.kind, d.label, f.v FROM {d} d LEFT /* c */ JOIN {f} f ON d.kind = f.kind WHERE f.v > 2 OR f.v IS NULL",
    v112_sr_jc_kp_lj_only_null_deferred => "SELECT d.kind, d.label FROM {d} d LEFT /* RIGHT JOIN */ JOIN {f} f ON d.kind = f.kind WHERE f.kind IS NULL",
    v112_sr_jc_kp_rj_admits_null_deferred => "SELECT d.kind, d.label, f.v FROM {f} f RIGHT /* c */ JOIN {d} d ON d.kind = f.kind WHERE f.v > 2 OR f.v IS NULL",
    v112_sr_jc_kp_rj_only_null_deferred => "SELECT d.kind, d.label FROM {f} f RIGHT /* LEFT JOIN */ OUTER JOIN {d} d ON d.kind = f.kind WHERE f.kind IS NULL",
}

macro_rules! v112_sr_join_comment_aggregate_cells {
    ($deferred:expr; $($name:ident => $select:expr,)+) => {
        $(
            #[pg_test]
            fn $name() {
                let tag = stringify!($name).trim_start_matches("v112_");
                assert_eq!(
                    v112_sq_cell(&V112_SQ_NULL_EXTENDED_CHAIN, tag, $select, None, $deferred),
                    "ok",
                    "{}",
                    $select
                );
            }
        )+
    };
}

v112_sr_join_comment_aggregate_cells! { false;
    v112_sr_jc_agg_lj_admits_null => "SELECT d.label, COUNT(*) AS c, SUM(f.v) AS s FROM {d} d LEFT -- RIGHT JOIN\n OUTER JOIN {f} f ON d.kind = f.kind WHERE f.v > 0 OR f.v IS NULL GROUP BY d.label",
    v112_sr_jc_agg_lj_rejects_null => "SELECT d.kind, COUNT(*) AS c, MIN(f.v) AS mn FROM {d} d LEFT /* INNER JOIN */ JOIN {f} f ON d.kind = f.kind WHERE f.v > 0 GROUP BY d.kind",
    v112_sr_jc_agg_lj_only_null => "SELECT d.label, COUNT(*) AS c FROM {d} d LEFT /* FULL JOIN */ JOIN {f} f ON d.kind = f.kind WHERE f.id IS NULL GROUP BY d.label",
    v112_sr_jc_agg_rj_only_null => "SELECT d.kind, COUNT(*) AS c FROM {f} f RIGHT /* LEFT JOIN */ OUTER JOIN {d} d ON d.kind = f.kind WHERE f.id IS NULL GROUP BY d.kind",
    v112_sr_jc_agg_fj_rejects_null => "SELECT d.label, COUNT(*) AS c, SUM(f.v) AS s FROM {d} d FULL -- LEFT JOIN\n OUTER JOIN {f} f ON d.kind = f.kind WHERE f.v > 0 GROUP BY d.label",
    v112_sr_jc_agg_fj_admits_null => "SELECT d.kind, COUNT(*) AS c, SUM(f.v) AS s FROM {d} d FULL /* INNER JOIN */ JOIN {f} f ON d.kind = f.kind WHERE f.v > 0 OR f.v IS NULL GROUP BY d.kind",
    v112_sr_jc_agg_chain_other_null => "SELECT d.label, COUNT(*) AS c, SUM(f.v) AS s FROM {d} d LEFT /* JOIN */ JOIN {f} f ON d.kind = f.kind LEFT -- JOIN\n JOIN {o} o ON o.kind = f.kind WHERE o.w IS NULL GROUP BY d.label",
    v112_sr_jc_agg_chain_other_rejects_null => "SELECT d.kind, COUNT(*) AS c, SUM(f.v) AS s FROM {d} d LEFT /* c */ OUTER /* c */ JOIN {f} f ON d.kind = f.kind LEFT /* RIGHT JOIN */ JOIN {o} o ON o.kind = f.kind WHERE o.w > 1 GROUP BY d.kind",
}

v112_sr_join_comment_aggregate_cells! { true;
    v112_sr_jc_agg_lj_admits_null_deferred => "SELECT d.label, COUNT(*) AS c, SUM(f.v) AS s FROM {d} d LEFT -- RIGHT JOIN\n OUTER JOIN {f} f ON d.kind = f.kind WHERE f.v > 0 OR f.v IS NULL GROUP BY d.label",
    v112_sr_jc_agg_rj_only_null_deferred => "SELECT d.kind, COUNT(*) AS c FROM {f} f RIGHT /* LEFT JOIN */ OUTER JOIN {d} d ON d.kind = f.kind WHERE f.id IS NULL GROUP BY d.kind",
    v112_sr_jc_agg_chain_other_null_deferred => "SELECT d.label, COUNT(*) AS c, SUM(f.v) AS s FROM {d} d LEFT /* JOIN */ JOIN {f} f ON d.kind = f.kind LEFT -- JOIN\n JOIN {o} o ON o.kind = f.kind WHERE o.w IS NULL GROUP BY d.label",
}

// ---------------------------------------------------------------------------
// A keyword-named table outside `public`
// ---------------------------------------------------------------------------

/// A table of another schema named by a keyword, written `written`: maintained as
/// a passthrough, an aggregate and a join, and its IMVs go when it is dropped.
fn v112_sr_keyword_table_of_another_schema(tag: &str, table: &str, written: &str) -> String {
    let schema = format!("v112_sr_kws_{tag}");
    Spi::run(&format!("CREATE SCHEMA {schema}")).expect("schema");
    let table = format!("{schema}.{table}");
    let written = format!("{schema}.{written}");
    let other = format!("{schema}.p");
    Spi::run(&format!("CREATE TABLE {table}(id INT PRIMARY KEY, g TEXT, v INT)")).expect("table");
    Spi::run(&format!("INSERT INTO {table} VALUES (1,'a',10),(2,'a',-20),(3,'b',30)")).expect("seed");
    Spi::run(&format!("CREATE TABLE {other}(id INT PRIMARY KEY, label TEXT)")).expect("other");
    Spi::run(&format!("INSERT INTO {other} VALUES (1,'one'),(2,'two'),(4,'four')")).expect("seed");
    let steps = vec![
        format!("INSERT INTO {table} VALUES (4,'b',40),(5,'c',50)"),
        format!("UPDATE {table} SET v = v + 1 WHERE id IN (1, 4)"),
        format!("UPDATE {table} SET g = 'c' WHERE id = 2"),
        format!("UPDATE {other} SET label = label || '!' WHERE id < 4"),
        format!("DELETE FROM {table} WHERE id = 3"),
        format!("INSERT INTO {other} VALUES (5,'five')"),
        format!("UPDATE {table} SET id = 30 WHERE id = 5"),
    ];
    let cells = [
        ("pt", format!("SELECT id, v FROM {written}")),
        ("agg", format!("SELECT g, SUM(v) AS s, COUNT(*) AS c FROM {written} GROUP BY g")),
        (
            "join",
            format!("SELECT k.id, k.v, p.label FROM {written} k JOIN {other} p ON p.id = k.id"),
        ),
        (
            "join_agg",
            format!("SELECT p.label, SUM(k.v) AS s, COUNT(*) AS c FROM {written} AS k JOIN {other} p ON p.id = k.id GROUP BY p.label"),
        ),
    ];
    let mut imvs: Vec<(String, String)> = Vec::new();
    for (form, query) in cells {
        let imv = format!("v112_sr_kws_{tag}_{form}");
        let outcome = v112_sr_cell(&imv, &query, "", &[]);
        if outcome != "ok" {
            return format!("{form}: {outcome}");
        }
        imvs.push((imv, query));
    }
    for dml in &steps {
        let ran = v112_j_try_dml(dml);
        if ran != "OK" {
            return format!("{ran} [{dml}]");
        }
        for (imv, query) in &imvs {
            let compared = v112_j_oracle(imv, query);
            if compared != "0" {
                return format!("{imv}: {compared} [{dml}]");
            }
        }
    }
    let dropped = v112_j_try_dml(&format!("DROP TABLE {table} CASCADE"));
    if dropped != "OK" {
        return format!("drop: {dropped}");
    }
    let left: Vec<&String> = imvs
        .iter()
        .map(|(imv, _)| imv)
        .filter(|imv| v112_sr_registered(imv))
        .collect();
    if !left.is_empty() {
        return format!("left registered after the drop: {left:?}");
    }
    let ran = v112_j_try_dml(&format!("UPDATE {other} SET label = 'uno' WHERE id = 1"));
    if ran != "OK" {
        return format!("write after the drop: {ran}");
    }
    "ok".to_string()
}

macro_rules! v112_sr_keyword_table_of_another_schema_cells {
    ($($name:ident => ($table:expr, $written:expr),)+) => {
        $(
            #[pg_test]
            fn $name() {
                let tag = stringify!($name).trim_start_matches("v112_sr_kws_");
                assert_eq!(v112_sr_keyword_table_of_another_schema(tag, $table, $written), "ok");
            }
        )+
    };
}

v112_sr_keyword_table_of_another_schema_cells! {
    v112_sr_kws_column_name_quoted => ("\"position\"", "\"position\""),
    v112_sr_kws_column_name_bare => ("\"position\"", "position"),
    v112_sr_kws_column_name_upper => ("\"position\"", "Position"),
    v112_sr_kws_unreserved_quoted => ("\"zone\"", "\"zone\""),
    v112_sr_kws_unreserved_bare => ("\"zone\"", "zone"),
    v112_sr_kws_reserved => ("\"select\"", "\"select\""),
    v112_sr_kws_mixed_case => ("\"Mixed\"", "\"Mixed\""),
    v112_sr_kws_plain => ("plain", "plain"),
    v112_sr_kws_plain_quoted => ("plain", "\"plain\""),
}

// ---------------------------------------------------------------------------
// Every reader of a registered source name takes the quoted spelling
// ---------------------------------------------------------------------------

/// Before PostgreSQL 16, `to_regclass` raises on a name it cannot parse instead of
/// answering NULL: a reader that mangles a quoted source name is loud there and
/// silent after. Each maintenance entry point runs over IMVs whose source name
/// needs quotes, then the IMVs are written and compared again.
fn v112_sr_admin_paths_over(tag: &str, table: &str, deferred: bool) -> String {
    Spi::run(&format!("CREATE TABLE {table}(id INT PRIMARY KEY, g TEXT NOT NULL, v INT)")).expect("table");
    Spi::run(&format!("INSERT INTO {table} VALUES (1,'a',10),(2,'a',-20),(3,'b',30)")).expect("seed");
    let mode = if deferred { "DEFERRED" } else { "IMMEDIATE" };
    let imvs = [
        (format!("v112_sr_adm_{tag}_pt"), format!("SELECT id, g, v FROM {table}")),
        (
            format!("v112_sr_adm_{tag}_agg"),
            format!("SELECT g, SUM(v) AS s, MIN(v) AS mn, COUNT(*) AS c FROM {table} GROUP BY g"),
        ),
    ];
    for (imv, query) in &imvs {
        let created = v112_j_try(&format!(
            "SELECT create_reflex_ivm('{imv}', $v112sel${query}$v112sel$, NULL, 'UNLOGGED', '{mode}')"
        ));
        if created != "CREATE REFLEX INCREMENTAL VIEW" {
            return format!("{imv}: NOT-CREATED {created}");
        }
    }
    let check = |after: &str| -> Option<String> {
        if deferred {
            let flushed = v112_j_flush_every_source(&imvs[0].0);
            if flushed.contains("RAISED") || flushed.contains("ERROR") {
                return Some(format!("flush after {after}: {flushed}"));
            }
        }
        imvs.iter().find_map(|(imv, query)| {
            let compared = v112_j_oracle(imv, query);
            (compared != "0").then(|| format!("{imv} after {after}: {compared}"))
        })
    };
    let write = |dml: String| -> Option<String> {
        let ran = v112_j_try_dml(&dml);
        if ran != "OK" {
            return Some(format!("{ran} [{dml}]"));
        }
        check(&dml)
    };
    if let Some(wrong) = write(format!("INSERT INTO {table} VALUES (4,'b',40),(5,'c',50)")) {
        return wrong;
    }
    let source = Spi::get_one::<String>(&format!(
        "SELECT depends_on[1] FROM public.__reflex_ivm_reference WHERE name = '{}'",
        imvs[0].0
    ))
    .expect("registry")
    .unwrap_or_default();
    let source_literal = source.replace('\'', "''");
    let mut calls = vec![
        "SELECT reflex_audit()".to_string(),
        "SELECT count(*)::text FROM reflex_ivm_status()".to_string(),
        format!("SELECT reflex_rebuild_triggers('{source_literal}')"),
        format!("SELECT refresh_imv_depending_on('{source_literal}')"),
    ];
    for (imv, _) in &imvs {
        calls.extend([
            format!("SELECT reflex_audit('{imv}')"),
            format!("SELECT count(*)::text FROM reflex_doctor('{imv}')"),
            format!("SELECT reflex_probe_not_null_columns('{imv}')"),
            format!("SELECT reflex_rebuild_imv_metadata('{imv}')"),
            format!("SELECT reflex_reconcile('{imv}')"),
            format!("SELECT refresh_reflex_imv('{imv}')"),
            format!("SELECT reflex_rebuild_imv('{imv}')"),
        ]);
    }
    for call in calls {
        let answered = v112_j_try(&call);
        if answered.starts_with("RAISED") || answered.starts_with("ERROR") {
            return format!("{call}: {answered}");
        }
        if let Some(wrong) = check(&call) {
            return wrong;
        }
    }
    for dml in [
        format!("UPDATE {table} SET v = v + 1, g = 'c' WHERE id IN (1, 4)"),
        format!("DELETE FROM {table} WHERE id = 3"),
        format!("INSERT INTO {table} VALUES (6,'a',NULL)"),
    ] {
        if let Some(wrong) = write(dml) {
            return wrong;
        }
    }
    let dropped = v112_j_try_dml(&format!("DROP TABLE {table} CASCADE"));
    if dropped != "OK" {
        return format!("drop: {dropped}");
    }
    match imvs.iter().find(|(imv, _)| v112_sr_registered(imv)) {
        Some((imv, _)) => format!("{imv} left registered after the drop"),
        None => "ok".to_string(),
    }
}

macro_rules! v112_sr_admin_path_cells {
    ($($name:ident => ($schema:expr, $table:expr, $deferred:expr),)+) => {
        $(
            #[pg_test]
            fn $name() {
                lock_shared_fixtures();
                let tag = stringify!($name).trim_start_matches("v112_sr_adm_");
                let table = match $schema {
                    "" => $table.to_string(),
                    schema => {
                        let schema = schema.replace("{tag}", tag);
                        Spi::run(&format!("CREATE SCHEMA {schema}")).expect("schema");
                        format!("{schema}.{}", $table)
                    }
                };
                assert_eq!(v112_sr_admin_paths_over(tag, &table, $deferred), "ok");
            }
        )+
    };
}

v112_sr_admin_path_cells! {
    v112_sr_adm_public_column_name_keyword => ("", "\"overlay\"", false),
    v112_sr_adm_public_column_name_keyword_deferred => ("", "\"overlay\"", true),
    v112_sr_adm_public_mixed_case => ("", "\"V112_sr_adm_Mixed\"", false),
    v112_sr_adm_schema_column_name_keyword => ("v112_sr_adm_{tag}", "\"position\"", false),
    v112_sr_adm_schema_reserved_keyword_deferred => ("v112_sr_adm_{tag}", "\"select\"", true),
    v112_sr_adm_schema_mixed_case_table => ("v112_sr_adm_{tag}", "\"Mixed\"", false),
    v112_sr_adm_mixed_case_schema => ("\"v112_sr_adm_{tag}_S\"", "\"T\"", false),
    v112_sr_adm_mixed_case_schema_deferred => ("\"v112_sr_adm_{tag}_S\"", "\"T\"", true),
    v112_sr_adm_mixed_case_schema_lower_table => ("\"v112_sr_adm_{tag}_S\"", "u", false),
    v112_sr_adm_plain => ("v112_sr_adm_{tag}", "plain", false),
}

/// A partitioned source read by an aggregate and by a passthrough: writes through
/// the parent and through a partition, a partition attached with rows, a
/// partition detached. `ok`, or what went wrong; `REFUSED` when the create is
/// refused with the message for a partitioned source whose name needs quotes.
fn v112_sr_partitioned_source_cell(schema: &str, parent: &str, first: &str, attached: &str) -> String {
    Spi::run(&format!("CREATE SCHEMA {schema}")).expect("schema");
    let table = &format!("{schema}.{parent}");
    let first = &format!("{schema}.{first}");
    let second = &format!("{schema}.p_b");
    let attached = &format!("{schema}.{attached}");
    let tag = schema.trim_matches('"').to_lowercase();
    Spi::run(&format!(
        "CREATE TABLE {table}(id INT, g TEXT NOT NULL, v INT) PARTITION BY LIST (g)"
    ))
    .expect("parent");
    Spi::run(&format!("CREATE TABLE {first} PARTITION OF {table} FOR VALUES IN ('a')")).expect("partition");
    Spi::run(&format!("CREATE TABLE {second} PARTITION OF {table} FOR VALUES IN ('b')")).expect("partition");
    Spi::run(&format!("INSERT INTO {table} VALUES (1,'a',10),(2,'a',-20),(3,'b',30)")).expect("seed");
    let imvs = [
        (format!("{tag}_agg"), format!("SELECT g, SUM(v) AS s, COUNT(*) AS c FROM {table} GROUP BY g")),
        (format!("{tag}_pt"), format!("SELECT id, g, v FROM {table}")),
    ];
    let mut refused = 0;
    for (imv, query) in &imvs {
        let created = v112_j_try(&format!(
            "SELECT create_reflex_ivm('{imv}', $v112sel${query}$v112sel$)"
        ));
        if created.starts_with("ERROR: [reflex-unsupported] the source ")
            && created.contains("is a partitioned table whose name PostgreSQL writes in double quotes")
            && created.contains("Fix: give the table and its schema lower-case names that are not keywords")
            && !v112_sr_registered(imv)
        {
            refused += 1;
        } else if created != "CREATE REFLEX INCREMENTAL VIEW" {
            return format!("{imv}: NOT-CREATED {created}");
        }
    }
    if refused == imvs.len() {
        return "REFUSED".to_string();
    }
    if refused > 0 {
        return "one refused, one created".to_string();
    }
    let steps = [
        (format!("INSERT INTO {table} VALUES (4,'b',40),(5,'a',50)"), true),
        (format!("INSERT INTO {first} VALUES (6,'a',60)"), true),
        (format!("UPDATE {table} SET v = v + 1 WHERE id IN (1, 4)"), true),
        (format!("UPDATE {second} SET v = v * 2"), true),
        (format!("UPDATE {table} SET g = 'b' WHERE id = 2"), true),
        (format!("DELETE FROM {first} WHERE id = 5"), true),
        (format!("CREATE TABLE {attached} (id INT, g TEXT NOT NULL, v INT)"), true),
        (format!("INSERT INTO {attached} VALUES (7,'c',70)"), true),
        (format!("ALTER TABLE {table} ATTACH PARTITION {attached} FOR VALUES IN ('c')"), false),
        ("SELECT reflex_flush_partitions()".to_string(), true),
        (format!("INSERT INTO {table} VALUES (8,'c',80)"), true),
        (format!("ALTER TABLE {table} DETACH PARTITION {second}"), false),
        ("SELECT reflex_flush_partitions()".to_string(), true),
        (format!("DELETE FROM {table} WHERE g = 'c' AND id = 7"), true),
    ];
    for (dml, compared_after) in steps {
        let ran = v112_j_try_dml(&dml);
        if ran != "OK" {
            return format!("{ran} [{dml}]");
        }
        if !compared_after {
            continue;
        }
        for (imv, query) in &imvs {
            let compared = v112_j_oracle(imv, query);
            if compared != "0" {
                return format!("{imv}: {compared} [{dml}]");
            }
        }
    }
    "ok".to_string()
}

#[pg_test]
fn v112_sr_adm_partitioned_source_plain_names() {
    assert_eq!(v112_sr_partitioned_source_cell("v112_sr_admp_plain", "p", "p_a", "p_c"), "ok");
}

/// Quotes on a partition do not matter: the source is the parent.
#[pg_test]
fn v112_sr_adm_partitioned_source_plain_parent_quoted_partitions() {
    assert_eq!(
        v112_sr_partitioned_source_cell("v112_sr_admp_parts", "p", "\"P_a\"", "\"select\""),
        "ok"
    );
}

/// An unreserved keyword is printed bare.
#[pg_test]
fn v112_sr_adm_partitioned_source_named_by_an_unreserved_keyword() {
    assert_eq!(v112_sr_partitioned_source_cell("v112_sr_admp_zone", "\"zone\"", "p_a", "p_c"), "ok");
}

#[pg_test]
fn v112_sr_adm_partitioned_source_in_a_mixed_case_schema_is_refused() {
    assert_eq!(v112_sr_partitioned_source_cell("\"v112_sr_admp_S\"", "p", "p_a", "p_c"), "REFUSED");
}

#[pg_test]
fn v112_sr_adm_partitioned_source_with_a_mixed_case_name_is_refused() {
    assert_eq!(v112_sr_partitioned_source_cell("v112_sr_admp_mixed", "\"P\"", "p_a", "p_c"), "REFUSED");
}

#[pg_test]
fn v112_sr_adm_partitioned_source_named_by_a_column_name_keyword_is_refused() {
    assert_eq!(
        v112_sr_partitioned_source_cell("v112_sr_admp_kw", "\"position\"", "p_a", "p_c"),
        "REFUSED"
    );
}

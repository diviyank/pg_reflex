// Definitions refused at create because nothing pg_reflex installs can keep them
// equal to their query: two sources whose triggers would carry the same names, and
// an expression pg_reflex writes back as text PostgreSQL reads differently. Every
// refusal leaves nothing behind, and the remedy its message gives is created over
// real tables and compared with its query after every write.

const V112_CR_CREATED: &str = "CREATE REFLEX INCREMENTAL VIEW";

fn v112_cr_create(imv: &str, query: &str, key: &str, deferred: bool) -> String {
    let mode = if deferred { "DEFERRED" } else { "IMMEDIATE" };
    let key = if key.is_empty() {
        "NULL".to_string()
    } else {
        format!("'{key}'")
    };
    v112_j_try(&format!(
        "SELECT create_reflex_ivm('{imv}', $v112sel${query}$v112sel$, {key}, 'UNLOGGED', '{mode}')"
    ))
}

/// Registry rows and relations whose name holds `imv`.
fn v112_cr_left_behind(imv: &str) -> i64 {
    Spi::get_one::<i64>(&format!(
        "SELECT (SELECT count(*) FROM public.__reflex_ivm_reference WHERE name LIKE '%{imv}%') \
              + (SELECT count(*) FROM pg_class WHERE relname LIKE '%{imv}%')"
    ))
    .expect("left behind")
    .unwrap_or(-1)
}

fn v112_cr_user_triggers(table: &str) -> i64 {
    Spi::get_one::<i64>(&format!(
        "SELECT count(*) FROM pg_trigger WHERE NOT tgisinternal AND tgrelid = '{table}'::regclass"
    ))
    .expect("triggers")
    .unwrap_or(-1)
}

/// Why `outcome` is not a tagged refusal that names `problem` and gives its remedy
/// within the first 300 characters, or `None` when it is one.
fn v112_cr_not_the_refusal(outcome: &str, problem: &str) -> Option<String> {
    let head: String = outcome.chars().take(300).collect();
    let tagged = outcome.starts_with("ERROR: [reflex-unsupported] ");
    if tagged && head.contains(problem) && head.contains("Fix: ") {
        None
    } else {
        Some(format!("expected a refusal naming '{problem}', got: {outcome}"))
    }
}

/// `ok`, or the first write that raised or left the IMV different from `query`.
fn v112_cr_maintained(imv: &str, query: &str, deferred: bool, steps: &[String]) -> String {
    let at_start = v112_j_oracle(imv, query);
    if at_start != "0" {
        return format!("before the writes={at_start}");
    }
    for dml in steps {
        let ran = v112_j_try_dml(dml);
        if ran != "OK" {
            return format!("{ran} [{dml}]");
        }
        if deferred {
            let flushed = v112_j_flush_every_source(imv);
            if flushed.contains("RAISED") || flushed.contains("ERROR") {
                return format!("flush {flushed} [{dml}]");
            }
        }
        let compared = v112_j_oracle(imv, query);
        if compared != "0" {
            return format!("{compared} [{dml}]");
        }
    }
    "ok".to_string()
}

// ---------------------------------------------------------------------------
// Two sources whose triggers would carry the same names
// ---------------------------------------------------------------------------

fn v112_cr_two_tables(first: &str, second: &str) {
    for table in [first, second] {
        Spi::run(&format!(
            "CREATE TABLE {table}(id INT PRIMARY KEY, v INT); INSERT INTO {table} VALUES (1,1),(2,2),(3,3)"
        ))
        .expect("table");
    }
}

fn v112_cr_two_table_steps(first: &str, second: &str) -> Vec<String> {
    vec![
        format!("INSERT INTO {first} VALUES (10,10),(11,11)"),
        format!("INSERT INTO {second} VALUES (10,20),(12,12)"),
        format!("UPDATE {first} SET v = v + 100 WHERE id IN (1, 10)"),
        format!("UPDATE {second} SET v = v + 200 WHERE id IN (2, 10)"),
        format!("DELETE FROM {first} WHERE id IN (2, 11)"),
        format!("DELETE FROM {second} WHERE id IN (1, 12)"),
    ]
}

/// The second of two tables whose names differ by a `.` against a `_` is refused as
/// a source, whichever comes first and in both modes; the IMV of the first goes on
/// being maintained; after the rename the message asks for, both are.
#[pg_test]
fn v112_cr_sources_sharing_trigger_names_are_refused() {
    lock_shared_fixtures();
    let mut wrong: Vec<String> = Vec::new();
    for (schema, dotted_first, deferred) in [
        ("v112_crt_sa", true, false),
        ("v112_crt_sb", false, false),
        ("v112_crt_sc", true, true),
        ("v112_crt_sd", false, true),
    ] {
        Spi::run(&format!("CREATE SCHEMA {schema}")).expect("schema");
        let dotted = format!("{schema}.t");
        let underscored = format!("{schema}_t");
        v112_cr_two_tables(&dotted, &underscored);
        let (first, second) = if dotted_first {
            (dotted.clone(), underscored.clone())
        } else {
            (underscored.clone(), dotted.clone())
        };
        let first_imv = format!("v112_cri_{}_first", &schema[9..]);
        let second_imv = format!("v112_cri_{}_second", &schema[9..]);
        let first_query = format!("SELECT id, v FROM {first}");
        let created = v112_cr_create(&first_imv, &first_query, "", deferred);
        if created != V112_CR_CREATED {
            wrong.push(format!("{first_imv}: {created}"));
            continue;
        }
        let triggers_of_first = v112_cr_user_triggers(&first);
        for (tag, query) in [
            ("rows", format!("SELECT id, v FROM {second}")),
            ("agg", format!("SELECT COUNT(*) AS c, SUM(v) AS sv FROM {second}")),
            ("cte", format!("WITH c AS (SELECT id, v FROM {second}) SELECT id, v FROM c")),
            ("join", format!("SELECT a.id, b.v FROM {first} a JOIN {second} b ON b.id = a.id")),
            ("in_subquery", format!("SELECT id, v FROM {first} WHERE id IN (SELECT id FROM {second})")),
        ] {
            let outcome = v112_cr_create(&format!("{second_imv}_{tag}"), &query, "", deferred);
            let refusal = v112_cr_not_the_refusal(&outcome, &format!("{second} and {first}"));
            if let Some(reason) = refusal {
                wrong.push(format!("{second_imv}_{tag}: {reason}"));
            }
        }
        if v112_cr_left_behind(&second_imv) != 0 {
            wrong.push(format!("{second_imv}: a refused definition left a registry row or a relation"));
        }
        if v112_cr_user_triggers(&second) != 0 {
            wrong.push(format!("triggers left on {second}"));
        }
        if v112_cr_user_triggers(&first) != triggers_of_first || triggers_of_first == 0 {
            wrong.push(format!("the triggers of {first} changed"));
        }
        let steps = v112_cr_two_table_steps(&first, &second);
        let first_kept = v112_cr_maintained(&first_imv, &first_query, deferred, &steps[..2]);
        if first_kept != "ok" {
            wrong.push(format!("{first_imv} after the refusals: {first_kept}"));
        }

        let renamed = format!("{second}_renamed");
        Spi::run(&format!("ALTER TABLE {second} RENAME TO t_renamed")).expect("rename");
        let renamed = if second == dotted {
            format!("{schema}.t_renamed")
        } else {
            Spi::run(&format!("ALTER TABLE t_renamed RENAME TO {renamed}")).expect("rename");
            renamed
        };
        let second_query = format!("SELECT id, v FROM {renamed}");
        let created = v112_cr_create(&second_imv, &second_query, "", deferred);
        if created != V112_CR_CREATED {
            wrong.push(format!("{second_imv} over the renamed table: {created}"));
            continue;
        }
        for dml in &v112_cr_two_table_steps(&first, &renamed)[2..] {
            let ran = v112_j_try_dml(dml);
            if ran != "OK" {
                wrong.push(format!("{ran} [{dml}]"));
                break;
            }
            for (imv, query) in [(&first_imv, &first_query), (&second_imv, &second_query)] {
                let flushed = if deferred {
                    v112_j_flush_every_source(imv)
                } else {
                    String::new()
                };
                let compared = v112_j_oracle(imv, query);
                if compared != "0" || flushed.contains("RAISED") || flushed.contains("ERROR") {
                    wrong.push(format!("{imv} after the rename: {flushed} {compared} [{dml}]"));
                }
            }
        }
    }
    assert!(wrong.is_empty(), "\n{}", wrong.join("\n"));
}

/// Two colliding tables named in one definition, and a schema and a table that
/// split the same letters differently, are refused; the renamed table is maintained.
#[pg_test]
fn v112_cr_sources_sharing_trigger_names_in_one_definition() {
    lock_shared_fixtures();
    Spi::run("CREATE SCHEMA v112_crt_se; CREATE SCHEMA v112_crt_se_a").expect("schemas");
    let mut wrong: Vec<String> = Vec::new();
    for (tag, first, second, renamed) in [
        ("dot", "v112_crt_se.t", "v112_crt_se_t", "v112_crt_se_t2"),
        ("split", "v112_crt_se_a.b_c", "v112_crt_se.a_b_c", "v112_crt_se.a_b_c2"),
        ("public", "public.v112_crt_sh", "public_v112_crt_sh", "public_v112_crt_sh2"),
    ] {
        v112_cr_two_tables(first, second);
        let imv = format!("v112_cri_se_{tag}");
        let query = |second: &str| format!("SELECT a.id, b.v FROM {first} a JOIN {second} b ON b.id = a.id");
        for deferred in [false, true] {
            let outcome = v112_cr_create(&imv, &query(second), "id", deferred);
            let refusal = v112_cr_not_the_refusal(&outcome, &format!("{first} and {second}"));
            if let Some(reason) = refusal {
                wrong.push(format!("{imv}: {reason}"));
            }
        }
        if v112_cr_left_behind(&imv) != 0 {
            wrong.push(format!("{imv}: a refused definition left a registry row or a relation"));
        }
        if v112_cr_user_triggers(first) + v112_cr_user_triggers(second) != 0 {
            wrong.push(format!("triggers left on {first} or {second}"));
        }
        let bare_new_name = renamed.rsplit('.').next().expect("name");
        Spi::run(&format!("ALTER TABLE {second} RENAME TO {bare_new_name}")).expect("rename");
        let created = v112_cr_create(&imv, &query(renamed), "id", false);
        if created != V112_CR_CREATED {
            wrong.push(format!("{imv} over the renamed table: {created}"));
            continue;
        }
        let kept = v112_cr_maintained(&imv, &query(renamed), false, &v112_cr_two_table_steps(first, renamed));
        if kept != "ok" {
            wrong.push(format!("{imv} over the renamed table: {kept}"));
        }
    }
    assert!(wrong.is_empty(), "\n{}", wrong.join("\n"));
}

/// A table reached through the `search_path` is the table its schema-qualified name
/// reaches: it is refused beside the table whose trigger names it would take.
#[pg_test]
fn v112_cr_source_named_through_the_search_path_is_refused() {
    lock_shared_fixtures();
    Spi::run("CREATE SCHEMA v112_crt_si").expect("schema");
    v112_cr_two_tables("v112_crt_si.t", "v112_crt_si_t");
    let first_query = "SELECT id, v FROM v112_crt_si_t";
    assert_eq!(v112_cr_create("v112_cri_si_first", first_query, "", false), V112_CR_CREATED);
    Spi::run("SET LOCAL search_path = v112_crt_si, public").expect("search_path");
    let outcome = v112_cr_create("public.v112_cri_si_second", "SELECT id, v FROM t", "", false);
    assert_eq!(
        v112_cr_not_the_refusal(&outcome, "v112_crt_si.t and v112_crt_si_t"),
        None
    );
    assert_eq!(v112_cr_left_behind("v112_cri_si_second"), 0);
    assert_eq!(v112_cr_user_triggers("v112_crt_si.t"), 0);
    let steps = v112_cr_two_table_steps("v112_crt_si_t", "v112_crt_si.t");
    assert_eq!(v112_cr_maintained("v112_cri_si_first", first_query, false, &steps), "ok");
}

/// A name no relation answers to is not taken for two tables: the definition fails
/// on the missing relation.
#[pg_test]
fn v112_cr_missing_relation_is_not_a_trigger_name_collision() {
    lock_shared_fixtures();
    for (tag, query) in [
        ("rows", "SELECT id, v FROM v112_crt_missing"),
        ("twice", "SELECT a.id, b.v FROM v112_crt_missing a JOIN v112_crt_missing b ON b.id = a.id"),
        ("qualified", "SELECT id, v FROM public.v112_crt_missing"),
    ] {
        let outcome = v112_cr_create(&format!("v112_cri_missing_{tag}"), query, "", false);
        assert!(outcome.contains("v112_crt_missing") && outcome.contains("does not exist"), "{tag}: {outcome}");
        assert_eq!(v112_cr_left_behind("v112_cri_missing"), 0, "{tag}");
    }
}

/// One table under several spellings, and two IMVs over it, share triggers and are
/// accepted; so are neighbours whose names differ by a `.` against a `_` elsewhere.
#[pg_test]
fn v112_cr_one_relation_under_several_spellings_is_accepted() {
    lock_shared_fixtures();
    Spi::run("CREATE SCHEMA v112_crt_sf").expect("schema");
    v112_cr_two_tables("v112_crt_sf.t", "v112_crt_sf.t_x");
    v112_cr_two_tables("v112_crt_sf_tx", "v112_crt_sg");
    Spi::run("SET LOCAL search_path = v112_crt_sf, public").expect("search_path");
    let definitions = [
        ("qualified", "SELECT id, v FROM v112_crt_sf.t", "v112_crt_sf.t"),
        ("qualified_again", "SELECT id, v + 1 AS w FROM v112_crt_sf.t", "v112_crt_sf.t"),
        ("upper", "SELECT ID, V FROM V112_CRT_SF.T", "v112_crt_sf.t"),
        ("quoted", "SELECT id, v FROM \"v112_crt_sf\".\"t\"", "v112_crt_sf.t"),
        ("by_search_path", "SELECT id, v FROM t", "v112_crt_sf.t"),
        ("self_join", "SELECT a.id, b.v FROM v112_crt_sf.t a JOIN t b ON b.id = a.id", "v112_crt_sf.t"),
        ("neighbour_schema", "SELECT id, v FROM v112_crt_sf.t_x", "v112_crt_sf.t_x"),
        ("neighbour_public", "SELECT id, v FROM v112_crt_sf_tx", "v112_crt_sf_tx"),
        ("public_bare", "SELECT id, v FROM v112_crt_sg", "v112_crt_sg"),
        ("public_qualified", "SELECT id, v FROM public.v112_crt_sg", "v112_crt_sg"),
    ];
    let mut wrong: Vec<String> = Vec::new();
    for (tag, query, _) in &definitions {
        let created = v112_cr_create(&format!("public.v112_cri_sf_{tag}"), query, "", false);
        if created != V112_CR_CREATED {
            wrong.push(format!("{tag}: {created}"));
        }
    }
    assert!(wrong.is_empty(), "\n{}", wrong.join("\n"));
    for table in ["v112_crt_sf.t", "v112_crt_sf.t_x", "v112_crt_sf_tx", "v112_crt_sg"] {
        for dml in [
            format!("INSERT INTO {table} VALUES (10,10),(11,11)"),
            format!("UPDATE {table} SET v = v + 100 WHERE id IN (1, 10)"),
            format!("DELETE FROM {table} WHERE id IN (2, 11)"),
        ] {
            assert_eq!(v112_j_try_dml(&dml), "OK", "{dml}");
            for (tag, query, _) in &definitions {
                let compared = v112_j_oracle(&format!("public.v112_cri_sf_{tag}"), query);
                if compared != "0" {
                    wrong.push(format!("{tag} differs by {compared} after [{dml}]"));
                }
            }
        }
    }
    assert!(wrong.is_empty(), "\n{}", wrong.join("\n"));
}

// ---------------------------------------------------------------------------
// Expressions pg_reflex writes back as text PostgreSQL reads differently
// ---------------------------------------------------------------------------

fn v112_cr_expression_query(position: &str, t: &str, e: &str) -> (String, &'static str) {
    match position {
        "pt_select" => (format!("SELECT id, {e} AS x FROM {t}"), ""),
        "pt_join_on" => (
            format!("SELECT {t}.id, {t}_d.w FROM {t} JOIN {t}_d ON {t}_d.id = {t}.id AND {e} > 1"),
            "id",
        ),
        "derived_select" => (format!("SELECT q.id, q.x FROM (SELECT id, {e} AS x FROM {t}) q"), ""),
        "pt_where" => (format!("SELECT id, v FROM {t} WHERE {e} > 1"), ""),
        "where_subquery" => (
            format!("SELECT id, v FROM {t} WHERE id IN (SELECT id FROM {t}_d WHERE {e} > 1)"),
            "",
        ),
        "agg_group_key" => (format!("SELECT {e} AS x, COUNT(*) AS c FROM {t} GROUP BY {e}"), ""),
        "agg_arg" => (format!("SELECT b, SUM({e}) AS s, COUNT(*) AS c FROM {t} GROUP BY b"), ""),
        "agg_where" => (format!("SELECT b, COUNT(*) AS c FROM {t} WHERE {e} > 1 GROUP BY b"), ""),
        "agg_having" => (format!("SELECT b, COUNT(*) AS c FROM {t} GROUP BY b HAVING SUM({e}) > 1"), ""),
        "agg_filter" => (format!("SELECT b, COUNT(*) FILTER (WHERE {e} > 1) AS c FROM {t} GROUP BY b"), ""),
        "agg_case" => (
            format!("SELECT b, SUM(CASE WHEN {e} > 1 THEN 1 ELSE 0 END) AS s FROM {t} GROUP BY b"),
            "",
        ),
        "agg_join_on" => (
            format!(
                "SELECT {t}.b, COUNT(*) AS c FROM {t} JOIN {t}_d ON {t}_d.id = {t}.id AND {e} > 1 GROUP BY {t}.b"
            ),
            "",
        ),
        "agg_order_by" => (format!("SELECT b, SUM(v ORDER BY {e}) AS s, COUNT(*) AS c FROM {t} GROUP BY b"), ""),
        "cte_select" => (format!("WITH c AS (SELECT id, {e} AS x FROM {t}) SELECT id, x FROM c"), ""),
        "cte_outer_select" => (format!("WITH c AS (SELECT id, v FROM {t}) SELECT id, {e} AS x FROM c"), ""),
        "union_select" => (format!("SELECT id, {e} AS x FROM {t} UNION ALL SELECT id, u FROM {t}"), ""),
        "distinct" => (format!("SELECT DISTINCT {e} AS x FROM {t}"), ""),
        "distinct_on_item" => (format!("SELECT DISTINCT ON (b) b, id, {e} AS x FROM {t} ORDER BY b, id"), ""),
        "window_item" => (
            format!("SELECT id, b, v, {e} AS x, SUM(v) OVER (PARTITION BY b) AS w FROM {t}"),
            "",
        ),
        "window_where" => (
            format!("SELECT id, b, v, SUM(v) OVER (PARTITION BY b) AS w FROM {t} WHERE {e} > 1"),
            "",
        ),
        other => panic!("unknown position {other}"),
    }
}

/// The select list and the FROM clause of a plain SELECT, which PostgreSQL runs as
/// the caller wrote them.
const V112_CR_RUN_AS_WRITTEN: [&str; 3] = ["pt_select", "pt_join_on", "derived_select"];

/// The clauses pg_reflex writes back from the parsed query.
const V112_CR_REWRITTEN: [&str; 17] = [
    "pt_where",
    "where_subquery",
    "agg_group_key",
    "agg_arg",
    "agg_where",
    "agg_having",
    "agg_filter",
    "agg_case",
    "agg_join_on",
    "agg_order_by",
    "cte_select",
    "cte_outer_select",
    "union_select",
    "distinct",
    "distinct_on_item",
    "window_item",
    "window_where",
];

fn v112_cr_expression_cell(tag: &str, position: &str, e: &str, deferred: bool) -> String {
    let mode = if deferred { "d" } else { "i" };
    let table = format!("v112_crt_e{mode}_{tag}_{position}");
    v112_sr_value_table(&table);
    Spi::run(&format!(
        "CREATE TABLE {table}_d(id INT PRIMARY KEY, w INT); \
         INSERT INTO {table}_d VALUES (1,1),(2,2),(3,3),(4,4),(10,10),(21,21)"
    ))
    .expect("second table");
    let expression = if position == "where_subquery" {
        e.replace('v', "w")
    } else {
        e.to_string()
    };
    let (query, key) = v112_cr_expression_query(position, &table, &expression);
    let imv = format!("v112_cri_e{mode}_{tag}_{position}");
    let created = v112_cr_create(&imv, &query, key, deferred);
    if created != V112_CR_CREATED {
        let left = v112_cr_left_behind(&imv);
        let triggers = v112_cr_user_triggers(&table) + v112_cr_user_triggers(&format!("{table}_d"));
        return format!("NOT-CREATED left={left} triggers={triggers} {created}");
    }
    let mut steps = v112_sr_value_steps(&table);
    steps.push(format!("UPDATE {table}_d SET w = w + 30 WHERE id IN (2, 10)"));
    steps.push(format!("DELETE FROM {table}_d WHERE id = 1"));
    v112_cr_maintained(&imv, &query, deferred, &steps)
}

/// `None` when the cell is refused with a message naming `problem` and nothing is
/// created; what happened otherwise.
fn v112_cr_not_refused(tag: &str, position: &str, e: &str, problem: &str, deferred: bool) -> Option<String> {
    let problem = if position == "where_subquery" {
        problem.replace('v', "w")
    } else {
        problem.to_string()
    };
    let outcome = v112_cr_expression_cell(tag, position, e, deferred);
    let refused = outcome
        .strip_prefix("NOT-CREATED left=0 triggers=0 ")
        .is_some_and(|message| v112_cr_not_the_refusal(message, &problem).is_none());
    (!refused).then(|| format!("{tag} {position} deferred={deferred}: {outcome}"))
}

/// `(tag, expression, what the refusal names, minimum server version)`.
const V112_CR_REPRINTED_DIFFERENTLY: [(&str, &str, &str, i32); 7] = [
    ("mm", "- -v", "'- -v' back as '--v'", 0),
    ("mm_literal", "- -1 + v", "'- -1' back as '--1'", 0),
    ("abs_minus", "@ -v", "'@ -v' back as '@-v'", 0),
    ("not_minus", "~ -v", "'~ -v' back as '~-v'", 0),
    ("minus_abs", "- @v", "'- @v' back as '-@v'", 0),
    ("minus_not", "- ~v", "'- ~v' back as '-~v'", 0),
    ("hex", "0x1F + v", "0x1F back as the bit string X'1F'", 160000),
];

/// A doubled minus, a prefix operator directly before another, and a hexadecimal
/// integer are refused in every clause pg_reflex writes back, and nothing is created.
#[pg_test]
fn v112_cr_reprinted_expressions_are_refused() {
    lock_shared_fixtures();
    let version = v112_sr_server_version();
    let mut wrong: Vec<String> = Vec::new();
    for (tag, e, problem, minimum_version) in V112_CR_REPRINTED_DIFFERENTLY {
        if version < minimum_version {
            continue;
        }
        for position in V112_CR_REWRITTEN {
            wrong.extend(v112_cr_not_refused(tag, position, e, problem, false));
        }
    }
    for (tag, e, problem) in [("mm", "- -v", "'- -v' back as '--v'"), ("abs_minus", "@ -v", "'@ -v' back as '@-v'")] {
        for position in ["pt_where", "agg_group_key", "cte_select"] {
            wrong.extend(v112_cr_not_refused(tag, position, e, problem, true));
        }
    }
    assert!(wrong.is_empty(), "\n{}", wrong.join("\n"));
}

/// The same expressions in the select list and the FROM clause of a plain SELECT,
/// which PostgreSQL runs as written, are created and maintained in both modes.
#[pg_test]
fn v112_cr_reprinted_expressions_run_as_written_are_accepted() {
    lock_shared_fixtures();
    let version = v112_sr_server_version();
    let mut wrong: Vec<String> = Vec::new();
    for (tag, e, _, minimum_version) in V112_CR_REPRINTED_DIFFERENTLY {
        if version < minimum_version {
            continue;
        }
        for position in V112_CR_RUN_AS_WRITTEN {
            for deferred in [false, true] {
                // No DEFERRED passthrough over a derived table is flushed, whatever
                // its select list holds.
                if deferred && position == "derived_select" {
                    continue;
                }
                let outcome = v112_cr_expression_cell(tag, position, e, deferred);
                if outcome != "ok" {
                    wrong.push(format!("{tag} {position} deferred={deferred}: {outcome}"));
                }
            }
        }
    }
    assert!(wrong.is_empty(), "\n{}", wrong.join("\n"));
}

/// The rewrites the refusals ask for, and the neighbouring expressions that are not
/// refused, are maintained in every clause.
#[pg_test]
fn v112_cr_reprinted_expression_remedies_are_maintained() {
    lock_shared_fixtures();
    let version = v112_sr_server_version();
    let mut wrong: Vec<String> = Vec::new();
    for (tag, e, minimum_version) in [
        ("minus", "-v", 0),
        ("mm_paren", "-(-v)", 0),
        ("mm_literal_paren", "-(-1) + v", 0),
        ("abs_paren", "@(-v)", 0),
        ("not_paren", "~(-v)", 0),
        ("minus_abs_paren", "-(@v)", 0),
        ("minus_not_paren", "-(~v)", 0),
        ("decimal", "31 + v", 0),
        ("plus_minus", "+ -v", 0),
        ("minus_minus_binary", "v - -1", 0),
        ("bit_string", "(X'1F')::int + v", 0),
        ("bit_string_lower", "(x'1f')::int + v", 0),
        ("underscore", "1_000 + v", 160000),
    ] {
        if version < minimum_version {
            continue;
        }
        for position in V112_CR_RUN_AS_WRITTEN.iter().chain(&V112_CR_REWRITTEN) {
            let outcome = v112_cr_expression_cell(tag, position, e, false);
            if outcome != "ok" {
                wrong.push(format!("{tag} {position}: {outcome}"));
            }
        }
    }
    assert!(wrong.is_empty(), "\n{}", wrong.join("\n"));
}

// ---------------------------------------------------------------------------
// Definitions around the refusals that stay accepted
// ---------------------------------------------------------------------------

/// `(query, unique key)`; `{kt}` is a public table, `{k1}` and `{k2}` two schemas
/// holding a table `t`, `{k1}.t_x` and `{k1}_tx` two tables whose names differ by a
/// `.` against a `_` without colliding, `{kp}` a partitioned table.
const V112_CR_ACCEPTED: [(&str, &str); 68] = [
    ("SELECT id, v FROM {kp} WHERE g = 'a'", ""),
    ("SELECT id, g, v FROM {kp} WHERE g IS NOT NULL AND g = 'a'", ""),
    ("SELECT g, COUNT(*) AS c, SUM(v) AS sv FROM {kp} WHERE g IN ('a', 'b') GROUP BY g", ""),
    ("SELECT COUNT(*) AS c, SUM(v) AS sv FROM {kp} WHERE ((g IS NOT NULL) AND (g = 'a'::text))", ""),
    ("SELECT id, g, v FROM {kp}", ""),
    ("SELECT g, MAX(v) AS mx, COUNT(*) AS c FROM {kp} GROUP BY g", ""),
    ("SELECT {kp}.id, {kt}.v FROM {kp} JOIN {kt} ON {kt}.id = {kp}.id WHERE {kp}.g = 'a'", ""),
    ("SELECT COUNT(*) AS c FROM {kp} WHERE NOT (g IS NOT NULL AND g IN ('a', 'b'))", ""),
    ("SELECT id, v FROM {k1}.t", ""),
    ("SELECT id, v FROM {k2}.t", ""),
    ("SELECT a.id, b.v FROM {k1}.t a JOIN {k2}.t b ON b.id = a.id", "id"),
    ("SELECT g, SUM(v) AS sv, COUNT(*) AS c FROM {k1}.t GROUP BY g", ""),
    ("SELECT g, SUM(v) AS sv, COUNT(*) AS c FROM {k2}.t GROUP BY g", ""),
    ("SELECT id, v FROM {k1}.t_x", ""),
    ("SELECT id, v FROM {k1}_tx", ""),
    ("SELECT a.id, b.v FROM {k1}.t_x a JOIN {k1}_tx b ON b.id = a.id", "id"),
    ("SELECT id, v FROM {kt}", ""),
    ("SELECT id, v FROM public.{kt}", ""),
    ("SELECT ID, V FROM {KT}", ""),
    ("WITH c AS (SELECT id, v FROM {k1}.t) SELECT id, v FROM c", ""),
    ("SELECT id, -v AS x FROM {kt}", ""),
    ("SELECT id, v FROM {kt} WHERE -v < 2", ""),
    ("SELECT g, SUM(-v) AS s, COUNT(*) AS c FROM {kt} GROUP BY g", ""),
    ("SELECT -v AS x, COUNT(*) AS c FROM {kt} GROUP BY -v", ""),
    ("SELECT id, - v AS x FROM {kt} WHERE - v < 2", ""),
    ("SELECT g, COUNT(*) AS c FROM {kt} GROUP BY g HAVING SUM(-v) < 5", ""),
    ("SELECT id, +v AS x FROM {kt} WHERE +v > 0", ""),
    ("SELECT id, v FROM {kt} WHERE -(v + 1) < 0", ""),
    ("SELECT g, SUM(-(v)) AS s, COUNT(*) AS c FROM {kt} GROUP BY g", ""),
    ("SELECT g, SUM(-(-v)) AS s, COUNT(*) AS c FROM {kt} GROUP BY g", ""),
    ("SELECT g, SUM(- (- v)) AS s, COUNT(*) AS c FROM {kt} WHERE - (- v) > -100 GROUP BY g", ""),
    ("SELECT id, v FROM {kt} WHERE ABS(-v) > 1", ""),
    ("SELECT id, v FROM {kt} WHERE v > -1", ""),
    ("SELECT id, v FROM {kt} WHERE v IN (-4, -9, 3)", ""),
    ("SELECT id, v FROM {kt} WHERE v BETWEEN -5 AND -1", ""),
    ("SELECT g, SUM(COALESCE(v, -1)) AS s, COUNT(*) AS c FROM {kt} GROUP BY g", ""),
    ("SELECT g, SUM(v * -1) AS s, COUNT(*) AS c FROM {kt} GROUP BY g", ""),
    ("SELECT g, SUM(-1 * v) AS s, COUNT(*) AS c FROM {kt} WHERE -1 < v GROUP BY g", ""),
    ("SELECT g, COUNT(*) FILTER (WHERE v < -1) AS c FROM {kt} GROUP BY g", ""),
    ("SELECT g, SUM(CASE WHEN v < -1 THEN -1 ELSE 1 END) AS s, COUNT(*) AS c FROM {kt} GROUP BY g", ""),
    ("SELECT id, v FROM {kt} WHERE v % -2 = 0", ""),
    ("SELECT id, v - -1 AS x FROM {kt}", ""),
    ("SELECT id, v FROM {kt} WHERE v - -1 > 0", ""),
    ("SELECT g, SUM(v - -1) AS s, COUNT(*) AS c FROM {kt} GROUP BY g", ""),
    ("SELECT v - -1 AS x, COUNT(*) AS c FROM {kt} GROUP BY v - -1", ""),
    ("SELECT g, COUNT(*) AS c FROM {kt} WHERE v + -1 < 10 AND v - - 1 > -10 GROUP BY g", ""),
    ("SELECT g, SUM(v) AS s, COUNT(*) AS c FROM {kt} WHERE 1 - -v > -100 GROUP BY g", ""),
    ("SELECT id, v FROM {kt} WHERE v > 1.5", ""),
    ("SELECT g, SUM(v * 0.25) AS s, COUNT(*) AS c FROM {kt} GROUP BY g", ""),
    ("SELECT id, v FROM {kt} WHERE v < 1e2", ""),
    ("SELECT g, SUM(v * 1.5e-3) AS s, COUNT(*) AS c FROM {kt} GROUP BY g", ""),
    ("SELECT id, v FROM {kt} WHERE v > .5", ""),
    ("SELECT id, v FROM {kt} WHERE v > -2.5e0", ""),
    ("SELECT g, COUNT(*) AS c FROM {kt} WHERE v <> 1000000 GROUP BY g", ""),
    ("SELECT id, v + 31 AS x FROM {kt} WHERE 31 > v", ""),
    ("SELECT id, s FROM {kt} WHERE s <> '--x'", ""),
    ("SELECT id, s FROM {kt} WHERE s = '0x1F' OR s LIKE '%--%'", ""),
    ("SELECT g, COUNT(*) FILTER (WHERE s LIKE '%--%') AS c FROM {kt} GROUP BY g", ""),
    ("SELECT s || '--' AS x, COUNT(*) AS c FROM {kt} GROUP BY s || '--'", ""),
    ("SELECT g, MAX(s || ' 0x1F -- ') AS m, COUNT(*) AS c FROM {kt} GROUP BY g", ""),
    ("SELECT id, s FROM {kt} WHERE s IS DISTINCT FROM '- -v'", ""),
    ("SELECT g, COUNT(*) AS c FROM {kt} WHERE s IS DISTINCT FROM (X'1F')::text GROUP BY g", ""),
    ("SELECT id, v /* 0x1F - -v */ FROM {kt} -- trailing 0x1F\n WHERE v > 0", ""),
    ("SELECT g, /* - -v */ COUNT(*) AS c FROM {kt} -- - -v\n GROUP BY g", ""),
    ("SELECT g, SUM(v) AS s, COUNT(*) AS c FROM {kt} WHERE v > -1 -- 0x1F\n GROUP BY g", ""),
    ("SELECT id, v -- note\n FROM {kt} WHERE s <> '/* -- */'", ""),
    ("SELECT id, - -v AS x, @ -v AS y FROM {kt}", ""),
    ("SELECT a.id, - -b.v AS x FROM {kt} a JOIN {k1}.t b ON b.id = a.id AND - -b.v > -100", "id"),
];

/// Every definition of the list is created and equals its query after writes to
/// every table.
#[pg_test]
fn v112_cr_definitions_around_the_refusals_stay_accepted() {
    lock_shared_fixtures();
    let (kt, k1, k2, kp) = ("v112_crt_kt", "v112_crt_k1", "v112_crt_k2", "v112_crt_kp");
    Spi::run(&format!("CREATE SCHEMA {k1}; CREATE SCHEMA {k2}")).expect("schemas");
    let tables = [
        kt.to_string(),
        format!("{k1}.t"),
        format!("{k2}.t"),
        format!("{k1}.t_x"),
        format!("{k1}_tx"),
    ];
    for table in &tables {
        Spi::run(&format!("CREATE TABLE {table}(id INT PRIMARY KEY, g TEXT, v INT, s TEXT)")).expect("table");
    }
    Spi::run(&format!(
        "CREATE TABLE {kp}(id INT, g TEXT, v INT, s TEXT) PARTITION BY LIST (g); \
         CREATE TABLE {kp}_a PARTITION OF {kp} FOR VALUES IN ('a'); \
         CREATE TABLE {kp}_b PARTITION OF {kp} FOR VALUES IN ('b'); \
         CREATE TABLE {kp}_rest PARTITION OF {kp} DEFAULT"
    ))
    .expect("partitioned table");
    let every_table: Vec<String> = tables.iter().cloned().chain([kp.to_string()]).collect();
    for table in &every_table {
        Spi::run(&format!(
            "INSERT INTO {table} VALUES (1,'a',3,'x--y'),(2,'b',-4,'0x1F'),(3,'a',NULL,NULL),(4,NULL,0,'--'),(5,'b',7,'plain')"
        ))
        .expect("seed");
    }
    let mut wrong: Vec<String> = Vec::new();
    let mut created: Vec<(String, String)> = Vec::new();
    for (position, (query, key)) in V112_CR_ACCEPTED.iter().enumerate() {
        let query = query
            .replace("{kt}", kt)
            .replace("{KT}", &kt.to_uppercase())
            .replace("{k1}", k1)
            .replace("{k2}", k2)
            .replace("{kp}", kp);
        let imv = format!("v112_cri_k{position}");
        let outcome = v112_cr_create(&imv, &query, key, false);
        if outcome != V112_CR_CREATED {
            wrong.push(format!("{position} [{query}]: {outcome}"));
            continue;
        }
        created.push((imv, query));
    }
    let compare = |wrong: &mut Vec<String>, after: &str| {
        for (imv, query) in &created {
            let compared = v112_j_oracle(imv, query);
            if compared != "0" {
                wrong.push(format!("{imv} [{query}] differs by {compared} after [{after}]"));
            }
        }
    };
    compare(&mut wrong, "create");
    for table in &every_table {
        for dml in [
            format!("INSERT INTO {table} VALUES (10,'a',-9,'--'),(11,'c',12,'0xFF'),(12,NULL,NULL,NULL)"),
            format!("UPDATE {table} SET v = -v WHERE id IN (1, 2, 10)"),
            format!("UPDATE {table} SET g = 'b', s = s || '--' WHERE id = 1"),
            format!("DELETE FROM {table} WHERE id IN (2, 11)"),
        ] {
            let ran = v112_j_try_dml(&dml);
            if ran != "OK" {
                wrong.push(format!("{ran} [{dml}]"));
                continue;
            }
            compare(&mut wrong, &dml);
        }
    }
    assert!(wrong.is_empty(), "\n{}", wrong.join("\n"));
}

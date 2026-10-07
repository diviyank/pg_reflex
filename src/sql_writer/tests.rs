//! Phase 0 — snapshot tests for the pre-1.6.1 SQL emitters.
//!
//! These goldens lock the exact strings the pre-refactor `build_*_ddl`
//! family produced for representative `AggregationPlan` inputs. The
//! refactor must hold these strings byte-for-byte — any whitespace or
//! ordering diff fails a test and must be reviewed.
//!
//! Also covers the union of edge cases historically handled across the
//! four identifier rewriters (`query_decomposer::replace_identifier`,
//! `query_decomposer::strip_redundant_bare_alias`,
//! `partition::substitute_identifier`,
//! `trigger::replace_source_with_transition`) so they survive
//! consolidation into [`super::identifier`].

use std::collections::HashMap;

use super::identifier::{
    format_pg_text_array, quote, replace_identifier, replace_source_with_transition, safe,
    split_qualified, strip_redundant_bare_alias, substitute_identifier_ci,
};
use super::{slot_replace, CreateIndex, CreateTable};

use crate::aggregation::{AggregationPlan, EndQueryMapping, IntermediateColumn};

// ---------- identifier rewriters ----------

#[test]
fn replace_identifier_word_boundary() {
    let out = replace_identifier(
        "SELECT * FROM regional WHERE x > 1",
        "regional",
        "my_view__cte_regional",
    );
    assert!(out.contains("my_view__cte_regional"));
    assert!(!out.contains(" regional "));
}

#[test]
fn replace_identifier_skips_partial_match() {
    // "regional_backup" must not have "regional" replaced.
    let out = replace_identifier("SELECT * FROM regional_backup", "regional", "replaced");
    assert_eq!(out, "SELECT * FROM regional_backup");
}

#[test]
fn replace_identifier_skips_after_as() {
    // `AS regional` is an output alias, not a table ref — must not rewrite.
    let out = replace_identifier("SELECT 1 AS regional FROM other", "regional", "REPLACED");
    assert_eq!(out, "SELECT 1 AS regional FROM other");
}

#[test]
fn replace_identifier_multiple_occurrences() {
    let out = replace_identifier(
        "SELECT a.x FROM regional a JOIN regional b ON a.id = b.id",
        "regional",
        "new_tbl",
    );
    assert_eq!(out.matches("new_tbl").count(), 2);
}

#[test]
fn replace_identifier_dotted_boundary() {
    // A leading dot is a column qualifier — the LHS belongs to a longer
    // identifier (it's a schema or table name), so this must not match.
    let out = replace_identifier("foo.bar JOIN bar", "bar", "BAZ");
    assert!(out.contains("foo.bar"), "Got: {}", out);
    assert!(out.contains("JOIN BAZ"), "Got: {}", out);
}

#[test]
fn replace_identifier_adjacent_paren() {
    // `bar(` is a function call site. The byte-level rewriter treats `(`
    // as a non-identifier byte → match qualifies → rewrite. This is what
    // the trigger codegen relies on for identifiers like `transition_tbl`
    // appearing in CAST / FILTER positions.
    let out = replace_identifier("foo FROM bar(x)", "bar", "BAZ");
    assert_eq!(out, "foo FROM BAZ(x)");
}

#[test]
fn replace_identifier_empty_pattern() {
    // Pathological caller — must return the input unchanged, not loop.
    let out = replace_identifier("anything", "", "X");
    assert_eq!(out, "anything");
}

#[test]
fn strip_redundant_bare_alias_basic() {
    let out = strip_redundant_bare_alias(
        "SELECT * FROM alp.product AS product WHERE product.id > 0",
        "alp.product",
    );
    assert_eq!(
        out, "SELECT * FROM alp.product WHERE product.id > 0",
        "redundant `AS product` must be stripped"
    );
}

#[test]
fn strip_redundant_bare_alias_bare_form() {
    let out =
        strip_redundant_bare_alias("FROM alp.product product JOIN other ON 1=1", "alp.product");
    assert_eq!(out, "FROM alp.product JOIN other ON 1=1");
}

#[test]
fn strip_redundant_bare_alias_no_op_unqualified() {
    let out = strip_redundant_bare_alias("SELECT * FROM product AS product", "product");
    assert_eq!(out, "SELECT * FROM product AS product");
}

#[test]
fn strip_redundant_bare_alias_keeps_user_alias() {
    // The user-chosen alias differs from the bare form — keep it.
    let out = strip_redundant_bare_alias("FROM alp.product AS p WHERE p.id > 0", "alp.product");
    assert_eq!(out, "FROM alp.product AS p WHERE p.id > 0");
}

#[test]
fn substitute_identifier_ci_lowercase_match() {
    let out = substitute_identifier_ci("Foo + foo + FOO", "foo", "X");
    assert_eq!(out, "X + X + X");
}

#[test]
fn substitute_identifier_ci_word_boundary() {
    let out = substitute_identifier_ci("Foo_bar + foo", "foo", "X");
    // `Foo_bar` is one identifier — must not be touched.
    assert_eq!(out, "Foo_bar + X");
}

#[test]
fn replace_source_with_transition_qualified() {
    let out = replace_source_with_transition(
        "SELECT alp.product.id FROM alp.product",
        "alp.product",
        "__reflex_new_alp_product",
    );
    assert!(out.contains("\"__reflex_new_alp_product\""), "Got: {}", out);
    assert!(!out.contains("alp.product"));
}

#[test]
fn replace_source_with_transition_quoted_passthrough() {
    // An already-quoted transition table name must NOT have outer quotes added.
    let out =
        replace_source_with_transition("FROM alp.product", "alp.product", "\"sch\".\"pt_new\"");
    assert!(out.contains("\"sch\".\"pt_new\""), "Got: {}", out);
    assert!(!out.contains("\"\"sch"), "must not double-quote: {}", out);
}

// ---------- naming utilities ----------

#[test]
fn quote_unqualified() {
    assert_eq!(quote("my_view"), "\"my_view\"");
}

#[test]
fn quote_qualified() {
    assert_eq!(quote("myschema.my_view"), "\"myschema\".\"my_view\"");
}

#[test]
fn split_qualified_simple() {
    assert_eq!(split_qualified("x"), (None, "x"));
    assert_eq!(split_qualified("a.b"), (Some("a"), "b"));
}

#[test]
fn safe_passthrough_short() {
    assert_eq!(safe("short"), "short");
}

#[test]
fn safe_truncates_long() {
    let raw = "a".repeat(80);
    let truncated = safe(&raw);
    assert_eq!(truncated.len(), 63);
    assert!(truncated.starts_with("aaaa"));
}

#[test]
fn format_pg_text_array_empty() {
    assert_eq!(format_pg_text_array(&[]), "{}");
}

#[test]
fn format_pg_text_array_basic() {
    let v = vec!["a".to_string(), "b".to_string()];
    assert_eq!(format_pg_text_array(&v), r#"{"a","b"}"#);
}

#[test]
fn format_pg_text_array_escapes() {
    let v = vec!["he said \"hi\"".to_string(), "back\\slash".to_string()];
    assert_eq!(
        format_pg_text_array(&v),
        r#"{"he said \"hi\"","back\\slash"}"#
    );
}

#[test]
fn quote_escapes_embedded_double_quote() {
    // A bare identifier containing a double-quote must double it,
    // otherwise the generated SQL is malformed / injectable.
    assert_eq!(quote(r#"weird"name"#), r#""weird""name""#);
}

#[test]
fn quote_escapes_each_qualified_component() {
    // schema.table where BOTH parts contain a quote — each component
    // is escaped independently and the dot separator is preserved.
    assert_eq!(quote(r#"sch"ema.ta"ble"#), r#""sch""ema"."ta""ble""#);
}

#[test]
fn quote_plain_identifier_unchanged() {
    // The common case (no embedded quote) is byte-for-byte unchanged.
    assert_eq!(quote("my_view"), r#""my_view""#);
    assert_eq!(quote("myschema.my_view"), r#""myschema"."my_view""#);
}

// ---------- DDL builders ----------

#[test]
fn create_table_basic() {
    let out = CreateTable::new("\"foo\"")
        .if_not_exists(true)
        .column("    \"x\" TEXT")
        .column("    \"y\" BIGINT DEFAULT 0")
        .with_options("fillfactor=70")
        .build();
    let expected = "CREATE TABLE IF NOT EXISTS \"foo\" (\n    \"x\" TEXT,\n    \"y\" BIGINT DEFAULT 0\n) WITH (fillfactor=70)";
    assert_eq!(out, expected);
}

#[test]
fn create_table_unlogged_partition_by() {
    let out = CreateTable::new("\"foo\"")
        .unlogged(true)
        .if_not_exists(true)
        .column("    \"x\" TEXT")
        .partition_by("PARTITION BY LIST (\"x\")")
        .build();
    let expected =
        "CREATE UNLOGGED TABLE IF NOT EXISTS \"foo\" (\n    \"x\" TEXT\n) PARTITION BY LIST (\"x\")";
    assert_eq!(out, expected);
}

#[test]
fn create_table_like_form() {
    let out = CreateTable::new("\"delta\"")
        .unlogged(true)
        .if_not_exists(true)
        .column("__reflex_op TEXT NOT NULL")
        .like("orders")
        .build();
    let expected =
        "CREATE UNLOGGED TABLE IF NOT EXISTS \"delta\" (__reflex_op TEXT NOT NULL, LIKE orders INCLUDING DEFAULTS)";
    assert_eq!(out, expected);
}

#[test]
fn create_index_hash_single_column() {
    let out = CreateIndex::quoted_name("idx_int_view", "\"__reflex_intermediate_view\"")
        .if_not_exists(true)
        .using("hash")
        .columns(vec!["\"city\"".to_string()])
        .build();
    let expected =
        "CREATE INDEX IF NOT EXISTS \"idx_int_view\" ON \"__reflex_intermediate_view\" USING hash (\"city\")";
    assert_eq!(out, expected);
}

#[test]
fn create_index_unique_nulls_not_distinct() {
    let out = CreateIndex::quoted_name("idx_int_view", "\"__reflex_intermediate_view\"")
        .unique(true)
        .if_not_exists(true)
        .columns(vec!["\"city\"".to_string(), "\"year\"".to_string()])
        .nulls_not_distinct(true)
        .build();
    let expected = "CREATE UNIQUE INDEX IF NOT EXISTS \"idx_int_view\" ON \"__reflex_intermediate_view\" (\"city\", \"year\") NULLS NOT DISTINCT";
    assert_eq!(out, expected);
}

// ---------- slot replacer ----------

#[test]
fn slot_replace_basic() {
    let out = slot_replace(
        "FROM __REFLEX_SLOT_TBL__ WHERE __REFLEX_SLOT_TBL__.id = 1",
        &[("__REFLEX_SLOT_TBL__", "\"new_orders\"")],
    );
    assert_eq!(out, "FROM \"new_orders\" WHERE \"new_orders\".id = 1");
}

#[test]
fn slot_replace_order_preserved() {
    let out = slot_replace("A=__A__ B=__B__", &[("__A__", "alpha"), ("__B__", "beta")]);
    assert_eq!(out, "A=alpha B=beta");
}

#[test]
fn slot_replace_missing_slot_is_noop() {
    let out = slot_replace("nothing to replace", &[("__X__", "Y")]);
    assert_eq!(out, "nothing to replace");
}

// ---------- snapshots: the 6 schema-builder DDL emitters ----------
//
// These goldens lock the current strings byte-for-byte. If a later phase
// emits semantically equivalent but whitespace-different SQL, update the
// snapshot **once** with a clear journal note and confirm the EXCEPT ALL
// oracle in pg_test_correctness.rs still passes.

fn sample_aggregate_plan() -> AggregationPlan {
    AggregationPlan {
        group_by_columns: vec!["city".to_string()],
        intermediate_columns: vec![
            IntermediateColumn {
                name: "__sum_amount".to_string(),
                pg_type: "NUMERIC".to_string(),
                source_aggregate: "SUM".to_string(),
                source_arg: "amount".to_string(),
                topk_k: None,
            },
            IntermediateColumn {
                name: "__count_star".to_string(),
                pg_type: "BIGINT".to_string(),
                source_aggregate: "COUNT".to_string(),
                source_arg: "*".to_string(),
                topk_k: None,
            },
        ],
        end_query_mappings: vec![
            EndQueryMapping {
                intermediate_expr: "__sum_amount".to_string(),
                output_alias: "total".to_string(),
                aggregate_type: "SUM".to_string(),
                cast_type: None,
            },
            EndQueryMapping {
                intermediate_expr: "__count_star".to_string(),
                output_alias: "cnt".to_string(),
                aggregate_type: "COUNT".to_string(),
                cast_type: None,
            },
        ],
        has_distinct: false,
        needs_ivm_count: true,
        distinct_columns: vec![],
        is_passthrough: false,
        passthrough_columns: vec![],
        passthrough_key_mappings: std::collections::HashMap::new(),
        having_clause: None,
        not_null_columns: std::collections::HashSet::new(),
        group_by_aliases: std::collections::HashMap::new(),
        output_column_order: vec![],
        imv_relevant_columns: std::collections::HashMap::new(),
        imv_relevant_where: std::collections::HashMap::new(),
        source_join_keys: std::collections::HashMap::new(),
        partition_columns: Vec::new(),
        partition_strategy: String::new(),
        anchor_source: String::new(),
        partition_join_paths: std::collections::HashMap::new(),
        ignore_heal_keys: std::collections::HashMap::new(),
    }
}

fn sample_types() -> HashMap<String, String> {
    let mut m = HashMap::new();
    m.insert("city".to_string(), "TEXT".to_string());
    m.insert("amount".to_string(), "NUMERIC".to_string());
    m
}

#[test]
fn snapshot_intermediate_table_ddl() {
    let plan = sample_aggregate_plan();
    let types = sample_types();
    let ddl =
        crate::schema_builder::build_intermediate_table_ddl("test_view", &plan, &types, false)
            .unwrap();
    let expected = "CREATE UNLOGGED TABLE IF NOT EXISTS \"__reflex_intermediate_test_view\" (\n    \"city\" TEXT,\n    \"__sum_amount\" NUMERIC DEFAULT 0,\n    \"__count_star\" BIGINT DEFAULT 0,\n    __ivm_count BIGINT DEFAULT 0\n) WITH (fillfactor=70)";
    assert_eq!(ddl, expected);
}

#[test]
fn snapshot_intermediate_table_ddl_logged() {
    let plan = sample_aggregate_plan();
    let types = sample_types();
    let ddl = crate::schema_builder::build_intermediate_table_ddl("test_view", &plan, &types, true)
        .unwrap();
    let expected = "CREATE TABLE IF NOT EXISTS \"__reflex_intermediate_test_view\" (\n    \"city\" TEXT,\n    \"__sum_amount\" NUMERIC DEFAULT 0,\n    \"__count_star\" BIGINT DEFAULT 0,\n    __ivm_count BIGINT DEFAULT 0\n) WITH (fillfactor=70)";
    assert_eq!(ddl, expected);
}

#[test]
fn snapshot_target_table_ddl() {
    let plan = sample_aggregate_plan();
    let types = sample_types();
    let ddl = crate::schema_builder::build_target_table_ddl("test_view", &plan, &types, false);
    let expected = "CREATE UNLOGGED TABLE IF NOT EXISTS \"test_view\" (\n    \"city\" TEXT,\n    \"total\" NUMERIC,\n    \"cnt\" BIGINT\n) WITH (fillfactor=70)";
    assert_eq!(ddl, expected);
}

#[test]
fn snapshot_delta_scratch_table_ddl() {
    let plan = sample_aggregate_plan();
    let types = sample_types();
    let ddl =
        crate::schema_builder::build_delta_scratch_table_ddl("test_view", &plan, &types).unwrap();
    let expected = "CREATE UNLOGGED TABLE IF NOT EXISTS \"__reflex_scratch_test_view\" (\n    \"city\" TEXT,\n    \"__sum_amount\" NUMERIC DEFAULT 0,\n    \"__count_star\" BIGINT DEFAULT 0,\n    __ivm_count BIGINT DEFAULT 0\n)";
    assert_eq!(ddl, expected);
}

#[test]
fn snapshot_staging_table_ddl() {
    let ddl = crate::schema_builder::build_staging_table_ddl("orders");
    let expected = "CREATE UNLOGGED TABLE IF NOT EXISTS \"__reflex_delta_orders\" (__reflex_op TEXT NOT NULL, LIKE orders INCLUDING DEFAULTS)";
    assert_eq!(ddl, expected);
}

#[test]
fn snapshot_passthrough_scratch_ddls() {
    let ddls = crate::schema_builder::build_passthrough_scratch_ddls("my_view", "orders");
    assert_eq!(ddls.len(), 2);
    let expected_new =
        "CREATE UNLOGGED TABLE IF NOT EXISTS \"__reflex_pt_new_my_view_orders\" (LIKE orders INCLUDING DEFAULTS)";
    let expected_old =
        "CREATE UNLOGGED TABLE IF NOT EXISTS \"__reflex_pt_old_my_view_orders\" (LIKE orders INCLUDING DEFAULTS)";
    assert_eq!(ddls[0], expected_new);
    assert_eq!(ddls[1], expected_old);
}

#[test]
fn snapshot_indexes_ddl_single_group_by() {
    let plan = sample_aggregate_plan();
    let indexes = crate::schema_builder::build_indexes_ddl("test_view", &plan);
    assert_eq!(indexes.len(), 2);
    let expected_int = "CREATE INDEX IF NOT EXISTS \"idx__reflex_int_test_view\" ON \"__reflex_intermediate_test_view\" USING hash (\"city\")";
    let expected_target =
        "CREATE INDEX IF NOT EXISTS \"idx__reflex_target_test_view\" ON \"test_view\" (\"city\")";
    assert_eq!(indexes[0], expected_int);
    assert_eq!(indexes[1], expected_target);
}

#[test]
fn snapshot_indexes_ddl_multi_group_by() {
    let mut plan = sample_aggregate_plan();
    plan.group_by_columns = vec!["city".to_string(), "year".to_string()];
    let indexes = crate::schema_builder::build_indexes_ddl("test_view", &plan);
    assert_eq!(indexes.len(), 2);
    let expected_int = "CREATE UNIQUE INDEX IF NOT EXISTS \"idx__reflex_int_test_view\" ON \"__reflex_intermediate_test_view\" (\"city\", \"year\") NULLS NOT DISTINCT";
    let expected_target = "CREATE INDEX IF NOT EXISTS \"idx__reflex_target_test_view\" ON \"test_view\" (\"city\", \"year\")";
    assert_eq!(indexes[0], expected_int);
    assert_eq!(indexes[1], expected_target);
}

#[test]
fn replace_identifier_preserves_multibyte_text() {
    let sql = "SELECT season FROM product WHERE season = 'Été' AND city = 'Gießen'";
    assert_eq!(replace_identifier(sql, "absent_table", "x"), sql);
    assert_eq!(
        replace_identifier(sql, "product", "\"__reflex_new_product\""),
        "SELECT season FROM \"__reflex_new_product\" WHERE season = 'Été' AND city = 'Gießen'"
    );
}

#[test]
fn replace_identifier_matches_multibyte_pattern() {
    assert_eq!(
        replace_identifier("SELECT a FROM données WHERE a = 'é'", "données", "\"t\""),
        "SELECT a FROM \"t\" WHERE a = 'é'"
    );
}

#[test]
fn strip_redundant_bare_alias_preserves_multibyte_text() {
    let sql = "SELECT p.season FROM alp.product AS product WHERE p.season = 'Été'";
    let out = strip_redundant_bare_alias(sql, "alp.product");
    assert!(out.contains("'Été'"), "Got: {}", out);
}

#[test]
fn substitute_identifier_ci_preserves_multibyte_text() {
    assert_eq!(
        substitute_identifier_ci("(season = 'Été'::text)", "season", "'Hiver'"),
        "('Hiver' = 'Été'::text)"
    );
}

// ---------- source rewrite: literals, multi-byte neighbours, mixed quoting ----------

/// Verbatim copies of the rewriters as they were before they became
/// literal-, quoting- and UTF-8-aware, kept as the reference for the
/// differential tests below.
mod legacy {
    use super::super::identifier::{consume_table_alias, split_qualified};

    pub fn replace_identifier(sql: &str, old_name: &str, new_name: &str) -> String {
        if old_name.is_empty() {
            return sql.to_string();
        }
        let mut result = String::with_capacity(sql.len());
        let bytes = sql.as_bytes();
        let old_bytes = old_name.as_bytes();
        let mut unmatched_start = 0;
        let mut i = 0;
        while i < bytes.len() {
            if i + old_bytes.len() <= bytes.len() && &bytes[i..i + old_bytes.len()] == old_bytes {
                let before_ok = i == 0
                    || !(bytes[i - 1].is_ascii_alphanumeric()
                        || bytes[i - 1] == b'_'
                        || bytes[i - 1] == b'.');
                let after_as = i >= 3
                    && (bytes[i - 1] == b' ')
                    && (bytes[i - 2] == b'S' || bytes[i - 2] == b's')
                    && (bytes[i - 3] == b'A' || bytes[i - 3] == b'a')
                    && (i < 4 || !bytes[i - 4].is_ascii_alphanumeric());
                let after_pos = i + old_bytes.len();
                let after_ok = after_pos >= bytes.len()
                    || !(bytes[after_pos].is_ascii_alphanumeric() || bytes[after_pos] == b'_');
                if before_ok && after_ok && !after_as {
                    result.push_str(&sql[unmatched_start..i]);
                    result.push_str(new_name);
                    i += old_bytes.len();
                    unmatched_start = i;
                    continue;
                }
            }
            i += 1;
        }
        result.push_str(&sql[unmatched_start..]);
        result
    }

    pub fn strip_redundant_bare_alias(sql: &str, source_table: &str) -> String {
        let (schema, bare) = split_qualified(source_table);
        if schema.is_none() || bare == source_table {
            return sql.to_string();
        }
        let bytes = sql.as_bytes();
        let pat = source_table.as_bytes();
        let mut out = String::with_capacity(sql.len());
        let mut unmatched_start = 0;
        let mut i = 0;
        while i < bytes.len() {
            if i + pat.len() <= bytes.len() && &bytes[i..i + pat.len()] == pat {
                let before_ok = i == 0
                    || !(bytes[i - 1].is_ascii_alphanumeric()
                        || bytes[i - 1] == b'_'
                        || bytes[i - 1] == b'.');
                let after_pos = i + pat.len();
                let after_ok = after_pos >= bytes.len()
                    || !(bytes[after_pos].is_ascii_alphanumeric() || bytes[after_pos] == b'_');
                if before_ok && after_ok {
                    let (user_alias, consumed_to) = consume_table_alias(bytes, after_pos);
                    if matches!(&user_alias, Some(a) if a == bare) {
                        out.push_str(&sql[unmatched_start..i]);
                        out.push_str(source_table);
                        i = consumed_to;
                        unmatched_start = i;
                        continue;
                    }
                }
            }
            i += 1;
        }
        out.push_str(&sql[unmatched_start..]);
        out
    }

    pub fn replace_source_with_transition(
        base_query: &str,
        source_table: &str,
        transition_tbl: &str,
    ) -> String {
        let quoted_tbl = if transition_tbl.contains('"') {
            transition_tbl.to_string()
        } else {
            format!("\"{}\"", transition_tbl)
        };
        let stripped = strip_redundant_bare_alias(base_query, source_table);
        let (schema, bare_source) = split_qualified(source_table);
        let bare_unq = bare_source.trim_matches('"');
        let mut out = stripped;
        if let Some(s) = schema {
            let schema_unq = s.trim_matches('"');
            out = replace_identifier(
                &out,
                &format!("\"{}\".\"{}\"", schema_unq, bare_unq),
                &quoted_tbl,
            );
        }
        out = replace_identifier(&out, &format!("\"{}\"", bare_unq), &quoted_tbl);
        out = replace_identifier(&out, source_table, &quoted_tbl);
        if bare_source != source_table {
            out = replace_identifier(&out, bare_unq, &quoted_tbl);
        }
        out
    }
}

struct Lcg(u64);

impl Lcg {
    fn below(&mut self, bound: usize) -> usize {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        ((self.0 >> 33) as usize) % bound
    }

    fn concatenation_of(&mut self, fragments: &[&str], max_fragments: usize) -> String {
        let count = 1 + self.below(max_fragments);
        (0..count)
            .map(|_| fragments[self.below(fragments.len())])
            .collect()
    }
}

const TRANSITIONS: [&str; 2] = ["__reflex_new_alp_product", "\"sch\".\"__reflex_pt_new_v\""];

#[test]
fn source_rewrite_leaves_single_quoted_literal_alone() {
    assert_eq!(
        replace_source_with_transition(
            "SELECT season FROM prod WHERE season = 'a-prod-b' AND prod.v > 0",
            "prod",
            "__reflex_new_prod"
        ),
        "SELECT season FROM \"__reflex_new_prod\" WHERE season = 'a-prod-b' AND \"__reflex_new_prod\".v > 0"
    );
}

#[test]
fn source_rewrite_leaves_literal_next_to_multibyte_chars_alone() {
    assert_eq!(
        replace_source_with_transition(
            "SELECT season FROM prod WHERE season = 'éprodé'",
            "prod",
            "__reflex_new_prod"
        ),
        "SELECT season FROM \"__reflex_new_prod\" WHERE season = 'éprodé'"
    );
}

#[test]
fn source_rewrite_leaves_literal_with_doubled_quote_alone() {
    assert_eq!(
        replace_source_with_transition(
            "SELECT v FROM prod WHERE c = 'it''s prod' AND prod.v > 0",
            "prod",
            "__reflex_new_prod"
        ),
        "SELECT v FROM \"__reflex_new_prod\" WHERE c = 'it''s prod' AND \"__reflex_new_prod\".v > 0"
    );
}

#[test]
fn source_rewrite_leaves_escape_string_alone() {
    assert_eq!(
        replace_source_with_transition(
            "SELECT v FROM prod WHERE c = E'a\\' prod' AND prod.v > 0",
            "prod",
            "__reflex_new_prod"
        ),
        "SELECT v FROM \"__reflex_new_prod\" WHERE c = E'a\\' prod' AND \"__reflex_new_prod\".v > 0"
    );
}

#[test]
fn source_rewrite_leaves_dollar_quoted_strings_alone() {
    assert_eq!(
        replace_source_with_transition(
            "SELECT v FROM prod WHERE c = $$a prod b$$ AND d = $tag$ prod $tag$ AND prod.v > 0",
            "prod",
            "__reflex_new_prod"
        ),
        "SELECT v FROM \"__reflex_new_prod\" WHERE c = $$a prod b$$ AND d = $tag$ prod $tag$ AND \"__reflex_new_prod\".v > 0"
    );
}

#[test]
fn source_rewrite_treats_positional_parameter_as_code() {
    assert_eq!(
        replace_source_with_transition(
            "SELECT v FROM prod WHERE c = $1 AND prod.v > $2",
            "prod",
            "__reflex_new_prod"
        ),
        "SELECT v FROM \"__reflex_new_prod\" WHERE c = $1 AND \"__reflex_new_prod\".v > $2"
    );
}

#[test]
fn source_rewrite_is_not_derailed_by_apostrophe_in_quoted_identifier() {
    assert_eq!(
        replace_source_with_transition(
            "SELECT v AS \"it's\" FROM prod WHERE c = 'prod'",
            "prod",
            "__reflex_new_prod"
        ),
        "SELECT v AS \"it's\" FROM \"__reflex_new_prod\" WHERE c = 'prod'"
    );
}

#[test]
fn source_rewrite_is_not_derailed_by_apostrophe_in_comments() {
    assert_eq!(
        replace_source_with_transition(
            "SELECT v /* it's */ FROM prod -- don't\nWHERE c = 'prod'",
            "prod",
            "__reflex_new_prod"
        ),
        "SELECT v /* it's */ FROM \"__reflex_new_prod\" -- don't\nWHERE c = 'prod'"
    );
}

#[test]
fn source_rewrite_leaves_schema_qualified_name_in_literal_alone() {
    assert_eq!(
        replace_source_with_transition(
            "SELECT v FROM alp.product WHERE c = 'a-alp.product-b' AND d = 'product' AND product.v > 0",
            "alp.product",
            "__reflex_new_alp_product"
        ),
        "SELECT v FROM \"__reflex_new_alp_product\" WHERE c = 'a-alp.product-b' AND d = 'product' AND \"__reflex_new_alp_product\".v > 0"
    );
}

#[test]
fn redundant_alias_inside_literal_is_kept() {
    assert_eq!(
        strip_redundant_bare_alias(
            "SELECT 'alp.product AS product' FROM alp.product AS product",
            "alp.product"
        ),
        "SELECT 'alp.product AS product' FROM alp.product"
    );
}

#[test]
fn replace_identifier_still_rewrites_inside_literals() {
    assert_eq!(
        replace_identifier("SELECT 'a-prod-b' FROM prod", "prod", "X"),
        "SELECT 'a-X-b' FROM X"
    );
}

#[test]
fn literal_aware_rewrite_matches_legacy_when_query_has_no_literal() {
    const FRAGMENTS: &[&str] = &[
        "alp.product",
        "product",
        "\"alp\".\"product\"",
        "\"product\"",
        "alp",
        ".",
        "_",
        "\"",
        " AS ",
        " as ",
        " AS product",
        " product",
        "x",
        " ",
        "(",
        ")",
        ",",
        "SELECT ",
        " FROM ",
        " JOIN ",
        " WHERE ",
        " ON ",
        " = ",
        "p",
        "1",
        "\n",
        "-",
        "--",
        "/*",
        "*/",
    ];
    let mut rng = Lcg(0x5eed);
    let mut compared = 0;
    let mut rewritten = 0;
    let mut stripped = 0;
    for _ in 0..8000 {
        let sql = rng.concatenation_of(FRAGMENTS, 14);
        if sql.contains("\"alp\".product") || sql.contains("alp.\"product\"") {
            continue;
        }
        for source in ["alp.product", "product"] {
            for transition in TRANSITIONS {
                let expected = legacy::replace_source_with_transition(&sql, source, transition);
                assert_eq!(
                    replace_source_with_transition(&sql, source, transition),
                    expected,
                    "sql: {sql:?}, source: {source}"
                );
                rewritten += usize::from(expected != sql);
            }
            let expected = legacy::strip_redundant_bare_alias(&sql, source);
            assert_eq!(
                strip_redundant_bare_alias(&sql, source),
                expected,
                "sql: {sql:?}, source: {source}"
            );
            stripped += usize::from(expected != sql);
            compared += 1;
        }
    }
    assert!(compared > 6000, "compared only {compared}");
    assert!(rewritten > 3000, "rewrote only {rewritten}");
    assert!(stripped > 20, "stripped only {stripped}");
}

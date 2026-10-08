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

#[test]
fn replace_identifier_skips_match_followed_by_multibyte_char() {
    assert_eq!(
        replace_identifier("SELECT prodé FROM prod", "prod", "X"),
        "SELECT prodé FROM X"
    );
    assert_eq!(
        replace_identifier("SELECT prod夏, prod🍉 FROM prod", "prod", "X"),
        "SELECT prod夏, prod🍉 FROM X"
    );
}

#[test]
fn replace_identifier_skips_match_preceded_by_multibyte_char() {
    assert_eq!(
        replace_identifier("SELECT éprod FROM prod", "prod", "X"),
        "SELECT éprod FROM X"
    );
    assert_eq!(
        replace_identifier("SELECT 夏prod, 🍉prod FROM prod", "prod", "X"),
        "SELECT 夏prod, 🍉prod FROM X"
    );
}

#[test]
fn redundant_alias_after_multibyte_prefixed_name_is_kept() {
    let sql = "SELECT 1 FROM éalp.product product";
    assert_eq!(strip_redundant_bare_alias(sql, "alp.product"), sql);
}

#[test]
fn source_rewrite_skips_identifier_extending_source_name_with_multibyte_char() {
    assert_eq!(
        replace_source_with_transition(
            "SELECT prodé, éprod FROM prod GROUP BY prodé, éprod",
            "prod",
            "__reflex_new_prod"
        ),
        "SELECT prodé, éprod FROM \"__reflex_new_prod\" GROUP BY prodé, éprod"
    );
}

#[test]
fn replace_identifier_matches_legacy_on_ascii_input() {
    const FRAGMENTS: &[&str] = &[
        "prod",
        "alp.product",
        "\"alp\".\"product\"",
        "\"product\"",
        "product",
        ".",
        "_",
        "\"",
        "'",
        "''",
        "$",
        "$$",
        "$t$",
        "E'",
        "\\",
        " AS ",
        " as ",
        "xAS ",
        " ",
        "(",
        ")",
        ",",
        "SELECT ",
        " FROM ",
        " WHERE ",
        "=",
        "a",
        "Z",
        "0",
        "\n",
        "\t",
        "-",
        "/*",
        "*/",
    ];
    const PATTERNS: &[&str] = &[
        "prod",
        "product",
        "alp.product",
        "\"alp\".\"product\"",
        "\"product\"",
        "a",
        "",
    ];
    let mut rng = Lcg(0xb17e);
    let mut rewritten = 0;
    for case in 0..6000 {
        let mut sql = rng.concatenation_of(FRAGMENTS, 14);
        if case % 3 == 0 {
            let position = rng.below(sql.len() + 1);
            sql.insert(position, (rng.below(0x80) as u8) as char);
        }
        assert!(sql.is_ascii());
        for pattern in PATTERNS {
            let expected = legacy::replace_identifier(&sql, pattern, "\"__t\"");
            assert_eq!(
                replace_identifier(&sql, pattern, "\"__t\""),
                expected,
                "sql: {sql:?}, pattern: {pattern}"
            );
            rewritten += usize::from(expected != sql);
        }
    }
    assert!(rewritten > 1000, "rewrote only {rewritten}");
}

#[test]
fn source_rewrite_recognises_quoted_schema_with_unquoted_table() {
    assert_eq!(
        replace_source_with_transition(
            "SELECT product.v FROM \"alp\".product WHERE \"alp\".product.v > 0",
            "alp.product",
            "__reflex_new_alp_product"
        ),
        "SELECT \"__reflex_new_alp_product\".v FROM \"__reflex_new_alp_product\" WHERE \"__reflex_new_alp_product\".v > 0"
    );
}

#[test]
fn source_rewrite_recognises_unquoted_schema_with_quoted_table() {
    assert_eq!(
        replace_source_with_transition(
            "SELECT p.v FROM alp.\"product\" p WHERE alp.\"product\".v > 0",
            "alp.product",
            "__reflex_new_alp_product"
        ),
        "SELECT p.v FROM \"__reflex_new_alp_product\" p WHERE \"__reflex_new_alp_product\".v > 0"
    );
}

#[test]
fn source_rewrite_leaves_mixed_spelling_of_other_names_alone() {
    for sql in [
        "SELECT 1 FROM \"alp\".product_backup",
        "SELECT 1 FROM alp.\"product_backup\"",
        "SELECT 1 FROM \"zalp\".product",
    ] {
        assert_eq!(
            replace_source_with_transition(sql, "alp.product", "__reflex_new_alp_product"),
            sql
        );
    }
}

#[test]
fn source_rewrite_matches_legacy_for_unquoted_fully_quoted_and_bare_references() {
    const ALIASES: [&str; 5] = ["", " p", " AS p", " AS product", " product"];
    const SELECTS: [&str; 7] = [
        "*",
        "product.id, product.v",
        "p.id",
        "\"product\".id",
        "alp.product.id",
        "\"alp\".\"product\".v",
        "SUM(v) AS product",
    ];
    const JOINS: [&str; 4] = [
        "",
        " JOIN alp.dim d ON d.id = product.dim_id",
        " LEFT JOIN \"alp\".\"dim\" ON dim.id = p.dim_id",
        " JOIN alp.product_backup b ON b.id = p.id",
    ];
    const WHERES: [&str; 4] = [
        "",
        " WHERE product.v > 0",
        " WHERE p.v > 0 AND alp.product.id < 5",
        " WHERE \"product\".v IS NULL",
    ];
    const GROUPS: [&str; 3] = ["", " GROUP BY product.id", " GROUP BY p.id"];
    const REFERENCES: [(&str, &str); 4] = [
        ("alp.product", "alp.product"),
        ("alp.product", "\"alp\".\"product\""),
        ("product", "product"),
        ("product", "\"product\""),
    ];
    let mut compared = 0;
    let mut rewritten = 0;
    for (source, reference) in REFERENCES {
        for alias in ALIASES {
            for select in SELECTS {
                for join in JOINS {
                    for filter in WHERES {
                        for group in GROUPS {
                            let sql = format!(
                                "SELECT {select} FROM {reference}{alias}{join}{filter}{group}"
                            );
                            for transition in TRANSITIONS {
                                let expected = legacy::replace_source_with_transition(
                                    &sql, source, transition,
                                );
                                assert_eq!(
                                    replace_source_with_transition(&sql, source, transition),
                                    expected,
                                    "sql: {sql:?}, source: {source}"
                                );
                                compared += 1;
                                rewritten += usize::from(expected != sql);
                            }
                        }
                    }
                }
            }
        }
    }
    assert_eq!(compared, 13440);
    assert_eq!(rewritten, compared);
}

fn legacy_safe(raw: &str) -> String {
    const MAX_IDENT: usize = 63;
    if raw.len() <= MAX_IDENT {
        return raw.to_string();
    }
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut hasher = DefaultHasher::new();
    raw.hash(&mut hasher);
    let hash = hasher.finish();
    format!("{}_{:08x}", &raw[..MAX_IDENT - 9], hash as u32)
}

fn name_with_char_straddling_the_cut(ascii_lead: usize, straddling: char) -> String {
    format!("{}{}{}", "a".repeat(ascii_lead), straddling, "b".repeat(20))
}

fn assert_truncated_before_straddling_char(ascii_lead: usize, straddling: char) {
    let truncated = safe(&name_with_char_straddling_the_cut(ascii_lead, straddling));
    let (kept, suffix) = truncated.split_at(ascii_lead);
    assert_eq!(kept, "a".repeat(ascii_lead));
    assert_eq!(suffix.len(), 9, "suffix: {suffix}");
    assert!(suffix.starts_with('_'), "suffix: {suffix}");
    assert!(
        suffix[1..].chars().all(|c| c.is_ascii_hexdigit()),
        "suffix: {suffix}"
    );
    assert!(truncated.len() <= 63);
}

#[test]
fn safe_truncates_before_two_byte_char_straddling_the_cut() {
    assert_truncated_before_straddling_char(53, 'é');
}

#[test]
fn safe_truncates_before_three_byte_char_straddling_the_cut() {
    assert_truncated_before_straddling_char(53, '夏');
    assert_truncated_before_straddling_char(52, '夏');
}

#[test]
fn safe_truncates_before_four_byte_char_straddling_the_cut() {
    assert_truncated_before_straddling_char(53, '🍉');
    assert_truncated_before_straddling_char(52, '🍉');
    assert_truncated_before_straddling_char(51, '🍉');
}

#[test]
fn safe_keeps_distinct_long_multibyte_names_distinct() {
    let summer = format!("__reflex_new_{}_été", "é".repeat(30));
    let winter = format!("__reflex_new_{}_hiver", "é".repeat(30));
    assert_ne!(safe(&summer), safe(&winter));
    assert!(safe(&summer).len() <= 63);
}

#[test]
fn safe_keeps_whole_char_ending_exactly_at_the_cut() {
    let name = format!("{}é{}", "a".repeat(52), "b".repeat(20));
    assert!(safe(&name).starts_with(&format!("{}é_", "a".repeat(52))));
    assert_eq!(safe(&name).len(), 63);
}

#[test]
fn safe_matches_legacy_on_ascii_and_short_names() {
    const ALPHABET: &[u8] = b"abcXYZ019_$. \"";
    let mut rng = Lcg(0x5afe);
    let mut truncated = 0;
    for _ in 0..4000 {
        let length = rng.below(130);
        let name: String = (0..length)
            .map(|_| ALPHABET[rng.below(ALPHABET.len())] as char)
            .collect();
        assert_eq!(safe(&name), legacy_safe(&name), "name: {name:?}");
        truncated += usize::from(name.len() > 63);
    }
    assert!(truncated > 1500, "truncated only {truncated}");
    for chars in 0..=31 {
        let short_multibyte = "é".repeat(chars);
        assert_eq!(safe(&short_multibyte), short_multibyte);
        assert_eq!(safe(&short_multibyte), legacy_safe(&short_multibyte));
    }
}

// ---------- one definition of "this token names the source" ----------

use super::identifier::{rewritten_source_spellings, token_names_source};

const SOURCE_SPELLINGS: [&str; 4] = ["alp.dim", "\"alp\".\"dim\"", "\"alp\".dim", "alp.\"dim\""];

#[test]
fn token_names_source_in_every_qualified_spelling() {
    for registered in SOURCE_SPELLINGS {
        for token in SOURCE_SPELLINGS {
            assert!(
                token_names_source(token, registered),
                "registered {registered}, token {token}"
            );
        }
        for bare_token in ["dim", "\"dim\""] {
            assert!(
                token_names_source(bare_token, registered),
                "registered {registered}, token {bare_token}"
            );
        }
    }
}

#[test]
fn token_names_source_in_every_unqualified_spelling() {
    for registered in ["dim", "\"dim\""] {
        for token in ["dim", "\"dim\""] {
            assert!(
                token_names_source(token, registered),
                "registered {registered}, token {token}"
            );
        }
    }
}

#[test]
fn token_naming_another_relation_is_not_the_source() {
    for registered in SOURCE_SPELLINGS {
        for token in [
            "alp.dim_backup",
            "\"alp\".\"dimension\"",
            "other.dim",
            "\"other\".dim",
            "other.\"dim\"",
            "\"other\".\"dim\"",
            "alp.dim.k",
            "alp",
            "\"alp\"",
            "",
        ] {
            assert!(
                !token_names_source(token, registered),
                "registered {registered}, token {token}"
            );
        }
    }
    assert!(!token_names_source("alp.dim", "dim"));
    assert!(!token_names_source("DIM", "dim"));
}

#[test]
fn every_spelling_the_rewrite_replaces_names_the_source() {
    for registered in ["alp.dim", "\"alp\".\"dim\"", "dim", "\"dim\""] {
        let spellings = rewritten_source_spellings(registered);
        assert!(spellings.len() >= 2, "registered {registered}");
        for spelling in spellings {
            assert!(
                token_names_source(&spelling, registered),
                "registered {registered}, spelling {spelling}"
            );
            assert_ne!(
                replace_source_with_transition(
                    &format!("SELECT 1 FROM {spelling} x"),
                    registered,
                    "__reflex_new_dim"
                ),
                format!("SELECT 1 FROM {spelling} x"),
                "registered {registered}, spelling {spelling}"
            );
        }
    }
}

// ---------- a rewrite that would land inside another quoted identifier ----------

use super::identifier::source_rewrite_breaks_quoted_identifier;

#[test]
fn rewrite_inside_a_quoted_identifier_of_another_relation_is_detected() {
    for (sql, source) in [
        (
            "SELECT 1 FROM f JOIN \"sa\".dim a ON TRUE JOIN sb.\"dim\" b ON TRUE",
            "sa.dim",
        ),
        (
            "SELECT 1 FROM f JOIN sa.dim a ON TRUE JOIN \"sb\".\"dim\" b ON TRUE",
            "sa.dim",
        ),
        ("SELECT 1 FROM dim a JOIN sb.\"dim\" b ON TRUE", "dim"),
        ("SELECT \"alp\".\"product\".v FROM product", "product"),
        ("SELECT x.\"dim\" FROM sa.dim x", "sa.dim"),
    ] {
        assert!(
            source_rewrite_breaks_quoted_identifier(sql, source),
            "{sql} / {source}"
        );
        assert!(
            replace_source_with_transition(sql, source, "__reflex_new_x").contains("\"\""),
            "{sql} / {source}"
        );
    }
}

#[test]
fn rewrite_of_every_source_spelling_breaks_no_quoted_identifier() {
    for registered in ["alp.dim", "\"alp\".\"dim\"", "dim", "\"dim\""] {
        for spelling in rewritten_source_spellings(registered) {
            for sql in [
                format!("SELECT x.k FROM {spelling} x JOIN other.dim_backup b ON b.k = x.k"),
                format!("SELECT {spelling}.k FROM {spelling} WHERE {spelling}.k > 0"),
                format!("SELECT x.k FROM {spelling} AS x JOIN sb.dim y ON y.k = x.k"),
                format!("SELECT \"dimension\".k FROM {spelling} x JOIN \"dimension\" ON TRUE"),
            ] {
                assert!(
                    !source_rewrite_breaks_quoted_identifier(&sql, registered),
                    "{sql} / {registered}"
                );
                assert!(
                    !replace_source_with_transition(&sql, registered, "__reflex_new_x")
                        .contains("\"\""),
                    "{sql} / {registered}"
                );
            }
        }
    }
}

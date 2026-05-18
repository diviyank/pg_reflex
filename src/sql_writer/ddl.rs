//! Typed DDL builders that consolidate the `CREATE TABLE` / `CREATE INDEX`
//! string construction previously scattered across `schema_builder.rs`.
//!
//! The builders own identifier quoting via [`super::identifier::quote`] so
//! callers can no longer forget it — that omission was the source of the
//! 1.5.2 `mixed_case_identifier` bug class.
//!
//! Output is intentionally byte-identical to the pre-1.6.1 `format!()`-based
//! emitters so the EXCEPT ALL correctness oracle in `pg_test_correctness.rs`
//! sees no diff.

use crate::sql_writer::identifier;

/// `CREATE [UNLOGGED] TABLE [IF NOT EXISTS] <name> (<cols>) [PARTITION BY <expr>] [WITH (...)]`
///
/// Used by `build_intermediate_table_ddl`, `build_target_table_ddl`,
/// `build_delta_scratch_table_ddl`, `build_staging_table_ddl`, and
/// `build_passthrough_scratch_ddls`.
pub struct CreateTable {
    name: String,
    unlogged: bool,
    if_not_exists: bool,
    columns: Vec<String>,
    like_source: Option<String>,
    partition_by: Option<String>,
    with_options: Option<String>,
}

impl CreateTable {
    /// `name` is taken verbatim — callers either pass an already-quoted
    /// reference (e.g. from `intermediate_table_name`) or use [`Self::quoted_name`].
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            unlogged: false,
            if_not_exists: false,
            columns: Vec::new(),
            like_source: None,
            partition_by: None,
            with_options: None,
        }
    }

    /// Quote a bare or schema-qualified name and use it as the table name.
    pub fn quoted_name(name: &str) -> Self {
        Self::new(identifier::quote(name))
    }

    pub fn unlogged(mut self, flag: bool) -> Self {
        self.unlogged = flag;
        self
    }

    pub fn if_not_exists(mut self, flag: bool) -> Self {
        self.if_not_exists = flag;
        self
    }

    /// Append a single column definition. The string is taken verbatim —
    /// the caller is responsible for any quoting on the column name.
    pub fn column(mut self, def: impl Into<String>) -> Self {
        self.columns.push(def.into());
        self
    }

    /// Replace all column definitions with the given list.
    pub fn columns(mut self, defs: Vec<String>) -> Self {
        self.columns = defs;
        self
    }

    /// `LIKE <source>` shape (used by staging + passthrough scratch DDLs).
    /// Mutually exclusive with [`Self::column`] / [`Self::columns`] — the
    /// caller must use one or the other.
    pub fn like(mut self, source: impl Into<String>) -> Self {
        self.like_source = Some(source.into());
        self
    }

    /// Trailing `PARTITION BY <expr>` clause. Already includes the
    /// `PARTITION BY` keyword.
    pub fn partition_by(mut self, expr: impl Into<String>) -> Self {
        self.partition_by = Some(expr.into());
        self
    }

    /// Trailing `WITH (<options>)` clause; pass the bare option list.
    pub fn with_options(mut self, options: impl Into<String>) -> Self {
        self.with_options = Some(options.into());
        self
    }

    pub fn build(self) -> String {
        let mut out = String::with_capacity(128);
        out.push_str("CREATE ");
        if self.unlogged {
            out.push_str("UNLOGGED ");
        }
        out.push_str("TABLE ");
        if self.if_not_exists {
            out.push_str("IF NOT EXISTS ");
        }
        out.push_str(&self.name);

        if let Some(src) = &self.like_source {
            // `LIKE` form: short, columns-or-LIKE is mutually exclusive in
            // pre-1.6.1 callers, but two callers (staging, passthrough)
            // append a leading column before LIKE. Re-create that shape by
            // joining columns with comma when present.
            out.push_str(" (");
            if !self.columns.is_empty() {
                out.push_str(&self.columns.join(", "));
                out.push_str(", ");
            }
            out.push_str("LIKE ");
            out.push_str(src);
            out.push_str(" INCLUDING DEFAULTS");
            out.push(')');
        } else {
            out.push_str(" (\n");
            out.push_str(&self.columns.join(",\n"));
            out.push_str("\n)");
        }

        if let Some(part) = &self.partition_by {
            out.push(' ');
            out.push_str(part);
        }
        if let Some(opts) = &self.with_options {
            out.push_str(" WITH (");
            out.push_str(opts);
            out.push(')');
        }
        out
    }
}

/// `CREATE [UNIQUE] INDEX [IF NOT EXISTS] <name> ON <table> [USING <method>] (<cols>) [NULLS NOT DISTINCT]`
pub struct CreateIndex {
    name: String,
    table: String,
    columns: Vec<String>,
    unique: bool,
    if_not_exists: bool,
    using: Option<String>,
    nulls_not_distinct: bool,
}

impl CreateIndex {
    /// `name` is taken verbatim. Use [`Self::quoted_name`] to quote a bare
    /// identifier.
    pub fn new(name: impl Into<String>, table: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            table: table.into(),
            columns: Vec::new(),
            unique: false,
            if_not_exists: false,
            using: None,
            nulls_not_distinct: false,
        }
    }

    /// Quote a bare or schema-qualified index name.
    pub fn quoted_name(name: &str, table: impl Into<String>) -> Self {
        Self::new(format!("\"{}\"", name), table)
    }

    pub fn unique(mut self, flag: bool) -> Self {
        self.unique = flag;
        self
    }

    pub fn if_not_exists(mut self, flag: bool) -> Self {
        self.if_not_exists = flag;
        self
    }

    pub fn using(mut self, method: impl Into<String>) -> Self {
        self.using = Some(method.into());
        self
    }

    pub fn nulls_not_distinct(mut self, flag: bool) -> Self {
        self.nulls_not_distinct = flag;
        self
    }

    /// Replace the column list with `cols`.
    pub fn columns(mut self, cols: Vec<String>) -> Self {
        self.columns = cols;
        self
    }

    pub fn build(self) -> String {
        let mut out = String::with_capacity(96);
        out.push_str("CREATE ");
        if self.unique {
            out.push_str("UNIQUE ");
        }
        out.push_str("INDEX ");
        if self.if_not_exists {
            out.push_str("IF NOT EXISTS ");
        }
        out.push_str(&self.name);
        out.push_str(" ON ");
        out.push_str(&self.table);
        if let Some(method) = &self.using {
            out.push_str(" USING ");
            out.push_str(method);
        }
        out.push_str(" (");
        out.push_str(&self.columns.join(", "));
        out.push(')');
        if self.nulls_not_distinct {
            out.push_str(" NULLS NOT DISTINCT");
        }
        out
    }
}

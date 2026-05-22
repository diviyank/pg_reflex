// Differential correctness fuzz harness. See
// docs/superpowers/specs/2026-05-22-imv-differential-correctness-design.md
// and docs/superpowers/plans/2026-05-22-imv-differential-correctness.md.

mod model {
    /// Exact-comparable scalar types plus float (float8 uses epsilon compare).
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum ColType {
        Int,
        BigInt,
        Numeric,
        Bool,
        Text,
        Date,
        Float8,
    }

    impl ColType {
        pub fn sql(self) -> &'static str {
            match self {
                ColType::Int => "int",
                ColType::BigInt => "bigint",
                ColType::Numeric => "numeric",
                ColType::Bool => "bool",
                ColType::Text => "text",
                ColType::Date => "date",
                ColType::Float8 => "float8",
            }
        }
        pub fn is_float(self) -> bool {
            matches!(self, ColType::Float8)
        }
    }

    #[derive(Debug, Clone)]
    pub struct Column {
        pub name: String,
        pub ty: ColType,
        pub nullable: bool,
    }

    #[derive(Debug, Clone)]
    pub struct Table {
        pub name: String,
        pub pk: String,        // single-column integer PK name
        pub columns: Vec<Column>, // includes the pk column
    }

    /// One generated test case: schema + query body + DML, all as a model the
    /// renderer turns into SQL. `output_columns` are the SELECT output columns
    /// with their inferred types, used to build the comparison query.
    #[derive(Debug, Clone)]
    pub struct FuzzCase {
        pub tables: Vec<Table>,
        pub select_body: SelectBody,
        pub unique_columns: Vec<String>,
        pub deferred: bool,
        pub dml: Vec<DmlTxn>,
        pub output_columns: Vec<Column>,
    }

    #[derive(Debug, Clone)]
    pub struct SelectBody {
        pub rendered_sql: String, // the SELECT text passed to create_reflex_ivm / the MV
    }

    /// A transaction is a non-empty ordered list of statements applied
    /// atomically to the shared base tables.
    #[derive(Debug, Clone)]
    pub struct DmlTxn {
        pub statements: Vec<DmlStmt>,
    }

    #[derive(Debug, Clone)]
    pub enum DmlStmt {
        Insert { table: String, rows: Vec<Vec<String>> }, // pre-rendered SQL literals per column
        Delete { table: String, where_sql: String },
        Update { table: String, set_sql: String, where_sql: String },
        Truncate { table: String },
    }
}

mod render {
    use super::model::*;

    pub fn create_table_sql(t: &Table) -> String {
        let cols: Vec<String> = t
            .columns
            .iter()
            .map(|c| {
                let null = if c.name == t.pk {
                    "primary key".to_string()
                } else if c.nullable {
                    String::new()
                } else {
                    "not null".to_string()
                };
                format!("{} {} {}", c.name, c.ty.sql(), null).trim().to_string()
            })
            .collect();
        format!("CREATE TABLE {} ({})", t.name, cols.join(", "))
    }

    pub fn create_mv_sql(mv_name: &str, body: &SelectBody) -> String {
        format!("CREATE MATERIALIZED VIEW {} AS {}", mv_name, body.rendered_sql)
    }

    pub fn refresh_mv_sql(mv_name: &str) -> String {
        format!("REFRESH MATERIALIZED VIEW {}", mv_name)
    }

    pub fn dml_sql(stmt: &DmlStmt, columns_of: &dyn Fn(&str) -> Vec<String>) -> String {
        match stmt {
            DmlStmt::Insert { table, rows } => {
                let cols = columns_of(table).join(", ");
                let values: Vec<String> =
                    rows.iter().map(|r| format!("({})", r.join(", "))).collect();
                format!("INSERT INTO {} ({}) VALUES {}", table, cols, values.join(", "))
            }
            DmlStmt::Delete { table, where_sql } => {
                format!("DELETE FROM {} WHERE {}", table, where_sql)
            }
            DmlStmt::Update { table, set_sql, where_sql } => {
                format!("UPDATE {} SET {} WHERE {}", table, set_sql, where_sql)
            }
            DmlStmt::Truncate { table } => format!("TRUNCATE {}", table),
        }
    }
}

#[cfg(test)]
mod model_tests {
    use super::model::*;

    #[test]
    fn coltype_sql_and_float_flag() {
        assert_eq!(ColType::BigInt.sql(), "bigint");
        assert!(ColType::Float8.is_float());
        assert!(!ColType::Numeric.is_float());
    }
}

#[cfg(test)]
mod render_tests {
    use super::model::*;
    use super::render::*;

    #[test]
    fn renders_create_table_with_pk_and_notnull() {
        let t = Table {
            name: "t0".into(),
            pk: "id".into(),
            columns: vec![
                Column { name: "id".into(), ty: ColType::Int, nullable: false },
                Column { name: "v".into(), ty: ColType::Numeric, nullable: true },
            ],
        };
        let sql = create_table_sql(&t);
        assert_eq!(sql, "CREATE TABLE t0 (id int primary key, v numeric)");
    }

    #[test]
    fn renders_insert_with_columns() {
        let stmt = DmlStmt::Insert {
            table: "t0".into(),
            rows: vec![vec!["1".into(), "2.5".into()]],
        };
        let cols = |_t: &str| vec!["id".to_string(), "v".to_string()];
        assert_eq!(dml_sql(&stmt, &cols), "INSERT INTO t0 (id, v) VALUES (1, 2.5)");
    }
}

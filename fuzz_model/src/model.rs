/// Exact-comparable scalar types plus float (float8 uses epsilon compare).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ColType {
    Int,
    BigInt,
    Numeric,
    Bool,
    Text,
    Date,
    Float8,
    Timestamptz,
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
            ColType::Timestamptz => "timestamptz",
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
    #[allow(dead_code)] // rendered + parked; mutation generator does not yet emit TRUNCATE (see Task 13 follow-up)
    Truncate { table: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn coltype_sql_and_float_flag() {
        assert_eq!(ColType::BigInt.sql(), "bigint");
        assert!(ColType::Float8.is_float());
        assert!(!ColType::Numeric.is_float());
    }

    #[test]
    fn timestamptz_renders_and_is_not_float() {
        assert_eq!(ColType::Timestamptz.sql(), "timestamptz");
        assert!(!ColType::Timestamptz.is_float());
    }
}

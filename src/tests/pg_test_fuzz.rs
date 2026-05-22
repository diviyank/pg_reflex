// Differential correctness fuzz harness. See
// docs/superpowers/specs/2026-05-22-imv-differential-correctness-design.md
// and docs/superpowers/plans/2026-05-22-imv-differential-correctness.md.

pub mod model {
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

pub mod render {
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

#[cfg(any(test, feature = "pg_test"))]
pub mod generate {
    use super::model::*;
    use proptest::prelude::*;

    /// Exact-only column types for the CI gate (Task 10 adds Float8).
    fn exact_coltype() -> impl Strategy<Value = ColType> {
        prop_oneof![
            Just(ColType::Int),
            Just(ColType::BigInt),
            Just(ColType::Numeric),
            Just(ColType::Bool),
            Just(ColType::Text),
            Just(ColType::Date),
        ]
    }

    /// One base table `t0` with an int PK `id`, a numeric measure `m`, a text
    /// dimension `d`, and one extra random-typed nullable column.
    fn single_table() -> impl Strategy<Value = Table> {
        exact_coltype().prop_map(|extra_ty| Table {
            name: "t0".into(),
            pk: "id".into(),
            columns: vec![
                Column { name: "id".into(), ty: ColType::Int, nullable: false },
                Column { name: "m".into(), ty: ColType::Numeric, nullable: true },
                Column { name: "d".into(), ty: ColType::Text, nullable: true },
                Column { name: "x".into(), ty: extra_ty, nullable: true },
            ],
        })
    }

    /// Render a literal for a column type (deterministic small domain so joins
    /// and groups actually collide).
    fn literal(ty: ColType, n: i64) -> String {
        match ty {
            ColType::Int | ColType::BigInt => format!("{}", n % 5),
            ColType::Numeric | ColType::Float8 => format!("{}.{}", n % 5, n % 3),
            ColType::Bool => if n % 2 == 0 { "true".into() } else { "false".into() },
            ColType::Text => format!("'g{}'", n % 4),
            ColType::Date => format!("date '2024-01-{:02}'", (n % 27) + 1),
        }
    }

    fn seed_rows(t: &Table, count: usize) -> Vec<Vec<String>> {
        (0..count)
            .map(|i| {
                t.columns
                    .iter()
                    .map(|c| {
                        if c.name == t.pk {
                            format!("{}", i)
                        } else {
                            literal(c.ty, i as i64)
                        }
                    })
                    .collect()
            })
            .collect()
    }

    /// Passthrough: SELECT id, m, d FROM t0; unique key = id.
    fn passthrough_case(t: Table) -> FuzzCase {
        let body = SelectBody {
            rendered_sql: format!("SELECT {pk}, m, d FROM {tbl}", pk = t.pk, tbl = t.name),
        };
        let output_columns = vec![
            Column { name: t.pk.clone(), ty: ColType::Int, nullable: false },
            Column { name: "m".into(), ty: ColType::Numeric, nullable: true },
            Column { name: "d".into(), ty: ColType::Text, nullable: true },
        ];
        let seed = DmlTxn {
            statements: vec![DmlStmt::Insert { table: t.name.clone(), rows: seed_rows(&t, 8) }],
        };
        FuzzCase {
            tables: vec![t.clone()],
            select_body: body,
            unique_columns: vec![t.pk.clone()],
            deferred: false,
            dml: vec![seed],
            output_columns,
        }
    }

    /// Single-table aggregate: SELECT d, SUM(m) AS s, COUNT(*) AS c FROM t0 GROUP BY d.
    fn aggregate_case(t: Table) -> FuzzCase {
        let body = SelectBody {
            rendered_sql: format!(
                "SELECT d, SUM(m) AS s, COUNT(*) AS c FROM {tbl} GROUP BY d",
                tbl = t.name
            ),
        };
        let output_columns = vec![
            Column { name: "d".into(), ty: ColType::Text, nullable: true },
            Column { name: "s".into(), ty: ColType::Numeric, nullable: true },
            Column { name: "c".into(), ty: ColType::BigInt, nullable: false },
        ];
        let seed = DmlTxn {
            statements: vec![DmlStmt::Insert { table: t.name.clone(), rows: seed_rows(&t, 8) }],
        };
        FuzzCase {
            tables: vec![t.clone()],
            select_body: body,
            unique_columns: vec!["d".into()],
            deferred: false,
            dml: vec![seed],
            output_columns,
        }
    }

    pub fn fuzz_case() -> impl Strategy<Value = FuzzCase> {
        single_table().prop_flat_map(|t| {
            prop_oneof![
                Just(passthrough_case(t.clone())),
                Just(aggregate_case(t.clone())),
            ]
        })
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

#[cfg(test)]
mod generate_tests {
    use super::generate::*;
    use proptest::strategy::{Strategy, ValueTree};
    use proptest::test_runner::TestRunner;

    #[test]
    fn generates_a_buildable_case() {
        let mut runner = TestRunner::default();
        let tree = fuzz_case().new_tree(&mut runner).unwrap();
        let case = tree.current();
        assert!(!case.tables.is_empty());
        assert!(case.select_body.rendered_sql.to_uppercase().contains("SELECT"));
        assert!(!case.unique_columns.is_empty());
        assert!(!case.output_columns.is_empty());
    }
}

#[cfg(any(test, feature = "pg_test"))]
pub mod oracle {
    use super::model::*;
    use super::render;
    use pgrx::prelude::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static CASE_SEQ: AtomicU64 = AtomicU64::new(0);

    #[derive(Debug)]
    pub enum Outcome {
        Match,
        #[allow(dead_code)]
        Skip(String),
        Bug(String),
    }

    fn cols_of(case: &FuzzCase, table: &str) -> Vec<String> {
        case.tables
            .iter()
            .find(|t| t.name == table)
            .map(|t| t.columns.iter().map(|c| c.name.clone()).collect())
            .unwrap_or_default()
    }

    fn exact_diff_count(mv: &str, imv: &str) -> i64 {
        let sql = format!(
            "SELECT count(*)::bigint FROM ( \
               (SELECT * FROM {mv} EXCEPT SELECT * FROM {imv}) UNION ALL \
               (SELECT * FROM {imv} EXCEPT SELECT * FROM {mv})) d"
        );
        Spi::get_one::<i64>(&sql)
            .ok()
            .flatten()
            .unwrap_or(-1)
    }

    fn rename_case(case: &FuzzCase, suffix: &str) -> FuzzCase {
        let mut c = case.clone();
        // longest names first to avoid prefix corruption
        let mut names: Vec<String> = case.tables.iter().map(|t| t.name.clone()).collect();
        names.sort_by_key(|n| std::cmp::Reverse(n.len()));
        for old in names {
            let new = format!("{old}{suffix}");
            for t in &mut c.tables {
                if t.name == old {
                    t.name = new.clone();
                }
            }
            c.select_body.rendered_sql =
                c.select_body.rendered_sql.replace(&old, &new);
            for txn in &mut c.dml {
                for stmt in &mut txn.statements {
                    match stmt {
                        DmlStmt::Insert { table, .. }
                        | DmlStmt::Delete { table, .. }
                        | DmlStmt::Update { table, .. }
                        | DmlStmt::Truncate { table } => {
                            if *table == old {
                                *table = new.clone();
                            }
                        }
                    }
                }
            }
        }
        c
    }

    pub fn evaluate(case: &FuzzCase) -> Outcome {
        let seq = CASE_SEQ.fetch_add(1, Ordering::Relaxed);
        let suffix = format!("_fz{seq}");
        let case = rename_case(case, &suffix);

        for t in &case.tables {
            if let Err(_) = Spi::run(&render::create_table_sql(t)) {
                return Outcome::Bug("failed to create base table".to_string());
            }
        }

        let mv = format!("mv{suffix}");
        let imv = format!("imv{suffix}");

        if let Err(_) = Spi::run(&render::create_mv_sql(&mv, &case.select_body)) {
            return Outcome::Bug("failed to create mv".to_string());
        }

        // Try to create a savepoint for isolation, but don't fail if already in a
        // transaction (e.g., #[pg_test] context). The PgTryBuilder will catch
        // exceptions anyway.
        let savepoint_ok = Spi::run(&format!("SAVEPOINT sp{suffix}")).is_ok();

        let keys = case.unique_columns.join(",");
        let mode = if case.deferred { "DEFERRED" } else { "IMMEDIATE" };
        let body = case.select_body.rendered_sql.clone();
        let case_in = case.clone();
        let mv_in = mv.clone();
        let imv_in = imv.clone();
        let suffix_in = suffix.clone();

        let outcome = PgTryBuilder::new(move || {
            let msg = crate::create_reflex_ivm(
                &imv_in,
                &body,
                Some(&keys),
                None,
                Some(mode),
                None,
            );
            if msg.contains(crate::REFLEX_UNSUPPORTED_TAG) {
                return Outcome::Skip(msg.to_string());
            }
            if !msg.starts_with("CREATE REFLEX") {
                return Outcome::Bug(format!("unexpected create return: {msg}"));
            }

            for txn in &case_in.dml {
                for stmt in &txn.statements {
                    let cols = cols_of(&case_in, match stmt {
                        DmlStmt::Insert { table, .. }
                        | DmlStmt::Delete { table, .. }
                        | DmlStmt::Update { table, .. }
                        | DmlStmt::Truncate { table } => table,
                    });
                    let sql = render::dml_sql(stmt, &|_t: &str| cols.clone());
                    let _ = Spi::run(&sql);
                }
            }

            if case_in.deferred {
                for t in &case_in.tables {
                    let _ = Spi::run(&format!("SELECT reflex_flush_deferred('{}')", t.name));
                }
            }

            if let Err(_) = Spi::run(&render::refresh_mv_sql(&mv_in)) {
                return Outcome::Bug("failed to refresh mv".to_string());
            }

            let diff = exact_diff_count(&mv_in, &imv_in);
            if diff == 0 {
                Outcome::Match
            } else {
                Outcome::Bug(format!("contents diverge: {diff} mismatched rows"))
            }
        })
        .catch_others(|_caught| {
            Outcome::Bug("codegen exception (caught)".to_string())
        })
        .execute();

        // Only attempt to release/rollback savepoint if it was created successfully.
        if savepoint_ok {
            match &outcome {
                Outcome::Bug(_) => {
                    let _ = Spi::run(&format!("ROLLBACK TO SAVEPOINT sp{suffix_in}"));
                }
                _ => {
                    let _ = Spi::run(&format!("RELEASE SAVEPOINT sp{suffix_in}"));
                }
            }
        }

        outcome
    }

    pub fn repro_sql(case: &FuzzCase) -> String {
        let mut out = String::new();
        for t in &case.tables {
            out.push_str(&render::create_table_sql(t));
            out.push_str(";\n");
        }
        for txn in &case.dml {
            for stmt in &txn.statements {
                let cols = cols_of(case, match stmt {
                    DmlStmt::Insert { table, .. }
                    | DmlStmt::Delete { table, .. }
                    | DmlStmt::Update { table, .. }
                    | DmlStmt::Truncate { table } => table,
                });
                out.push_str(&render::dml_sql(stmt, &|_t: &str| cols.clone()));
                out.push_str(";\n");
            }
        }
        out.push_str(&format!(
            "SELECT create_reflex_ivm('imv_repro', $body${}$body$, '{}');\n",
            case.select_body.rendered_sql,
            case.unique_columns.join(",")
        ));
        out
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_test]
fn oracle_matches_on_a_simple_generated_case() {
    use proptest::strategy::{Strategy, ValueTree};
    use proptest::test_runner::TestRunner;

    let mut runner = TestRunner::default();
    let case = generate::fuzz_case()
        .new_tree(&mut runner)
        .unwrap()
        .current();
    match oracle::evaluate(&case) {
        oracle::Outcome::Match | oracle::Outcome::Skip(_) => {}
        oracle::Outcome::Bug(msg) => {
            panic!(
                "expected match/skip on simple case, got bug: {msg}\n{}",
                oracle::repro_sql(&case)
            )
        }
    }
}

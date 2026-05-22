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
        #[allow(dead_code)] // rendered + parked; mutation generator does not yet emit TRUNCATE (see Task 13 follow-up)
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

    #[allow(dead_code)] // the oracle inlines REFRESH inside its plpgsql function; kept for parity
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
    /// dimension `d`, a float column `f`, and one extra random-typed nullable column.
    fn single_table() -> impl Strategy<Value = Table> {
        exact_coltype().prop_map(|extra_ty| Table {
            name: "t0".into(),
            pk: "id".into(),
            columns: vec![
                Column { name: "id".into(), ty: ColType::Int, nullable: false },
                Column { name: "m".into(), ty: ColType::Numeric, nullable: true },
                Column { name: "d".into(), ty: ColType::Text, nullable: true },
                Column { name: "f".into(), ty: ColType::Float8, nullable: true },
                Column { name: "x".into(), ty: extra_ty, nullable: true },
            ],
        })
    }

    /// Render a literal for a column type (deterministic small domain so joins
    /// and groups actually collide).
    pub fn literal(ty: ColType, n: i64) -> String {
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

    /// Build a fixed multi-statement mutation for a table. Four statements:
    ///   * INSERT a new row with PK=100. In a two-table LEFT JOIN this is an
    ///     UNMATCHED primary-side insert (no secondary row with fk=100), and in
    ///     DEFERRED mode it is also re-touched by the UPDATE below — exercising
    ///     findings #1 and #2.
    ///   * INSERT a new row with PK=200 whose nullable dimension columns are NULL,
    ///     so an aggregate `GROUP BY <dim>` gains a legitimate NULL group —
    ///     exercising finding #3.
    ///   * UPDATE the measure column where id % 2 = 0 (includes the PK=100 row).
    ///   * DELETE id=0.
    fn build_mutation(t: &Table) -> DmlTxn {
        // Detect which measure column is available: prefer 'm', fall back to 'w', else 'id'.
        let measure = if t.columns.iter().any(|c| c.name == "m") {
            "m"
        } else if t.columns.iter().any(|c| c.name == "w") {
            "w"
        } else {
            "id"
        };

        let mut statements = Vec::new();

        // INSERT: one new row with PK=100 (safe, doesn't collide with seed 0-7).
        let insert_row: Vec<String> = t
            .columns
            .iter()
            .map(|c| {
                if c.name == t.pk {
                    "100".to_string()
                } else {
                    literal(c.ty, 100)
                }
            })
            .collect();
        statements.push(DmlStmt::Insert {
            table: t.name.clone(),
            rows: vec![insert_row],
        });

        // INSERT: a row whose nullable non-measure columns are NULL.
        let null_row: Vec<String> = t
            .columns
            .iter()
            .map(|c| {
                if c.name == t.pk {
                    "200".to_string()
                } else if c.name == measure || !c.nullable {
                    literal(c.ty, 200)
                } else {
                    "NULL".to_string()
                }
            })
            .collect();
        statements.push(DmlStmt::Insert {
            table: t.name.clone(),
            rows: vec![null_row],
        });

        // UPDATE: increment measure column where id % 2 = 0.
        statements.push(DmlStmt::Update {
            table: t.name.clone(),
            set_sql: format!("{measure} = {measure} + 1"),
            where_sql: "id % 2 = 0".into(),
        });

        // DELETE: remove id=0.
        statements.push(DmlStmt::Delete {
            table: t.name.clone(),
            where_sql: "id = 0".into(),
        });

        DmlTxn { statements }
    }

    fn mutation_txn(t: &Table) -> impl Strategy<Value = DmlTxn> {
        Just(build_mutation(t))
    }

    /// Mark a case for DEFERRED incremental maintenance (flushed at end of batch).
    fn deferred(mut case: FuzzCase) -> FuzzCase {
        case.deferred = true;
        case
    }

    /// Helper: append a mutation transaction to a case's DML list.
    fn with_mutation(mut case: FuzzCase, extra: DmlTxn) -> FuzzCase {
        case.dml.push(extra);
        case
    }

    /// Filter predicate variants for filtered cases. Returns (sql_where_clause, some description).
    fn filter_predicate(choice: usize) -> String {
        match choice % 3 {
            0 => "m > 1".to_string(),
            1 => "d <> 'g0'".to_string(),
            _ => "id % 2 = 0".to_string(),
        }
    }

    /// Passthrough: SELECT id, m, d, f FROM t0; unique key = id.
    fn passthrough_case(t: Table) -> FuzzCase {
        let body = SelectBody {
            rendered_sql: format!("SELECT {pk}, m, d, f, x FROM {tbl}", pk = t.pk, tbl = t.name),
        };
        let output_columns = vec![
            Column { name: t.pk.clone(), ty: ColType::Int, nullable: false },
            Column { name: "m".into(), ty: ColType::Numeric, nullable: true },
            Column { name: "d".into(), ty: ColType::Text, nullable: true },
            Column { name: "f".into(), ty: ColType::Float8, nullable: true },
            Column { name: "x".into(), ty: t.columns[4].ty, nullable: true },
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

    /// Passthrough with WHERE filter: SELECT id, m, d FROM t0 WHERE <filter>.
    fn passthrough_filtered_case(t: Table, filter_choice: usize) -> FuzzCase {
        let filter = filter_predicate(filter_choice);
        let body = SelectBody {
            rendered_sql: format!("SELECT {pk}, m, d FROM {tbl} WHERE {filter}", pk = t.pk, tbl = t.name),
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

    /// Single-table aggregate with WHERE filter: SELECT d, SUM(m) AS s, COUNT(*) AS c FROM t0 WHERE <filter> GROUP BY d.
    fn aggregate_filtered_case(t: Table, filter_choice: usize) -> FuzzCase {
        let filter = filter_predicate(filter_choice);
        let body = SelectBody {
            rendered_sql: format!(
                "SELECT d, SUM(m) AS s, COUNT(*) AS c FROM {tbl} WHERE {filter} GROUP BY d",
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

    /// Single-table aggregate with float: SELECT d, SUM(m) AS s, COUNT(*) AS c, AVG(m) AS avg_m, SUM(f) AS sf FROM t0 GROUP BY d.
    fn aggregate_float_case(t: Table) -> FuzzCase {
        let body = SelectBody {
            rendered_sql: format!(
                "SELECT d, SUM(m) AS s, COUNT(*) AS c, AVG(m) AS avg_m, SUM(f) AS sf FROM {tbl} GROUP BY d",
                tbl = t.name
            ),
        };
        let output_columns = vec![
            Column { name: "d".into(), ty: ColType::Text, nullable: true },
            Column { name: "s".into(), ty: ColType::Numeric, nullable: true },
            Column { name: "c".into(), ty: ColType::BigInt, nullable: false },
            Column { name: "avg_m".into(), ty: ColType::Float8, nullable: true },
            Column { name: "sf".into(), ty: ColType::Float8, nullable: true },
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

    /// Single-table aggregate with float and WHERE filter: SELECT d, SUM(m) AS s, COUNT(*) AS c, AVG(m) AS avg_m, SUM(f) AS sf FROM t0 WHERE <filter> GROUP BY d.
    fn aggregate_float_filtered_case(t: Table, filter_choice: usize) -> FuzzCase {
        let filter = filter_predicate(filter_choice);
        let body = SelectBody {
            rendered_sql: format!(
                "SELECT d, SUM(m) AS s, COUNT(*) AS c, AVG(m) AS avg_m, SUM(f) AS sf FROM {tbl} WHERE {filter} GROUP BY d",
                tbl = t.name
            ),
        };
        let output_columns = vec![
            Column { name: "d".into(), ty: ColType::Text, nullable: true },
            Column { name: "s".into(), ty: ColType::Numeric, nullable: true },
            Column { name: "c".into(), ty: ColType::BigInt, nullable: false },
            Column { name: "avg_m".into(), ty: ColType::Float8, nullable: true },
            Column { name: "sf".into(), ty: ColType::Float8, nullable: true },
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

    /// Two-table schema: t0 (id, m, d, f) and t1 (id, fk, w).
    fn two_tables() -> impl Strategy<Value = (Table, Table)> {
        Just((
            Table {
                name: "t0".into(),
                pk: "id".into(),
                columns: vec![
                    Column { name: "id".into(), ty: ColType::Int, nullable: false },
                    Column { name: "m".into(), ty: ColType::Numeric, nullable: true },
                    Column { name: "d".into(), ty: ColType::Text, nullable: true },
                ],
            },
            Table {
                name: "t1".into(),
                pk: "id".into(),
                columns: vec![
                    Column { name: "id".into(), ty: ColType::Int, nullable: false },
                    Column { name: "fk".into(), ty: ColType::Int, nullable: true },
                    Column { name: "w".into(), ty: ColType::Numeric, nullable: true },
                ],
            },
        ))
    }

    fn seed_two(a: &Table, b: &Table) -> DmlTxn {
        DmlTxn {
            statements: vec![
                DmlStmt::Insert { table: a.name.clone(), rows: seed_rows(a, 8) },
                DmlStmt::Insert { table: b.name.clone(), rows: seed_rows(b, 8) },
            ],
        }
    }

    /// Join aggregate: SELECT t0.d, SUM(t0.m) AS s, COUNT(t1.w) AS c
    /// FROM t0 LEFT JOIN t1 ON t1.fk = t0.id GROUP BY t0.d.
    fn join_aggregate_case(a: Table, b: Table) -> FuzzCase {
        let body = SelectBody {
            rendered_sql:
                "SELECT t0.d, SUM(t0.m) AS s, COUNT(t1.w) AS c FROM t0 LEFT JOIN t1 ON t1.fk = t0.id GROUP BY t0.d".into(),
        };
        let output_columns = vec![
            Column { name: "d".into(), ty: ColType::Text, nullable: true },
            Column { name: "s".into(), ty: ColType::Numeric, nullable: true },
            Column { name: "c".into(), ty: ColType::BigInt, nullable: false },
        ];
        FuzzCase {
            tables: vec![a.clone(), b.clone()],
            select_body: body,
            unique_columns: vec!["d".into()],
            deferred: false,
            dml: vec![seed_two(&a, &b)],
            output_columns,
        }
    }

    /// Carried scalar case: SELECT t0.id, SUM(t0.m) AS s, <carried>
    /// FROM t0 LEFT JOIN t1 ON t1.fk = t0.id GROUP BY t0.id.
    /// The carried expression varies by pick % 4.
    fn carried_scalar_case(a: Table, b: Table, pick: usize) -> FuzzCase {
        let (carried_sql, carried_col) = match pick % 4 {
            0 => (
                "COALESCE(SUM(t0.m), 0) AS s0".to_string(),
                Column { name: "s0".into(), ty: ColType::Numeric, nullable: true },
            ),
            1 => (
                "CASE WHEN COUNT(*) > 1 THEN 't' ELSE 'f' END AS lbl".to_string(),
                Column { name: "lbl".into(), ty: ColType::Text, nullable: true },
            ),
            2 => (
                "(t0.id)::text AS idt".to_string(),
                Column { name: "idt".into(), ty: ColType::Text, nullable: true },
            ),
            _ => (
                "EXISTS(SELECT 1 FROM t1 c WHERE c.fk = t0.id AND c.w > 0) AS flag".to_string(),
                Column { name: "flag".into(), ty: ColType::Bool, nullable: true },
            ),
        };
        let body = SelectBody {
            rendered_sql: format!(
                "SELECT t0.id, SUM(t0.m) AS s, {} FROM t0 LEFT JOIN t1 ON t1.fk = t0.id GROUP BY t0.id",
                carried_sql
            ),
        };
        let mut output_columns = vec![
            Column { name: "id".into(), ty: ColType::Int, nullable: false },
            Column { name: "s".into(), ty: ColType::Numeric, nullable: true },
        ];
        output_columns.push(carried_col);
        FuzzCase {
            tables: vec![a.clone(), b.clone()],
            select_body: body,
            unique_columns: vec!["id".into()],
            deferred: false,
            dml: vec![seed_two(&a, &b)],
            output_columns,
        }
    }

    /// CTE-decomposed: WITH agg AS (SELECT fk AS g, SUM(w) AS sw FROM t1 GROUP BY fk)
    /// SELECT t0.id, SUM(t0.m) AS s, a.sw FROM t0 LEFT JOIN agg a ON a.g = t0.id GROUP BY t0.id, a.sw.
    fn cte_decomposed_case(a: Table, b: Table) -> FuzzCase {
        let body = SelectBody {
            rendered_sql:
                "WITH agg AS (SELECT fk AS g, SUM(w) AS sw FROM t1 GROUP BY fk) \
                 SELECT t0.id, SUM(t0.m) AS s, a.sw FROM t0 LEFT JOIN agg a ON a.g = t0.id GROUP BY t0.id, a.sw".into(),
        };
        let output_columns = vec![
            Column { name: "id".into(), ty: ColType::Int, nullable: false },
            Column { name: "s".into(), ty: ColType::Numeric, nullable: true },
            Column { name: "sw".into(), ty: ColType::Numeric, nullable: true },
        ];
        FuzzCase {
            tables: vec![a.clone(), b.clone()],
            select_body: body,
            unique_columns: vec!["id".into()],
            deferred: false,
            dml: vec![seed_two(&a, &b)],
            output_columns,
        }
    }

    pub fn fuzz_case() -> impl Strategy<Value = FuzzCase> {
        prop_oneof![single_table_cases(), join_cases()]
    }

    /// Single-table shapes (passthrough / aggregate / aggregate-with-float, each
    /// unfiltered and filtered) under the multi-statement mutation, in both
    /// IMMEDIATE and DEFERRED maintenance modes.
    fn single_table_cases() -> impl Strategy<Value = FuzzCase> {
        single_table()
            .prop_flat_map(|t| {
                let t2 = t.clone();
                (Just(t), mutation_txn(&t2), any::<usize>(), any::<bool>())
            })
            .prop_flat_map(|(t, mtx, filter_choice, defer)| {
                let mode = move |c: FuzzCase| if defer { deferred(c) } else { c };
                prop_oneof![
                    Just(mode(with_mutation(passthrough_case(t.clone()), mtx.clone()))),
                    Just(mode(with_mutation(aggregate_case(t.clone()), mtx.clone()))),
                    Just(mode(with_mutation(aggregate_float_case(t.clone()), mtx.clone()))),
                    Just(mode(with_mutation(passthrough_filtered_case(t.clone(), filter_choice), mtx.clone()))),
                    Just(mode(with_mutation(aggregate_filtered_case(t.clone(), filter_choice), mtx.clone()))),
                    Just(mode(with_mutation(aggregate_float_filtered_case(t.clone(), filter_choice), mtx.clone()))),
                ]
            })
    }

    /// Two-table LEFT-JOIN shapes (direct join aggregate, carried scalar, and
    /// CTE-decomposed) with a primary-side mutation that inserts an UNMATCHED row
    /// (id=100, no secondary fk match) and a NULL-dimension row.
    fn join_cases() -> impl Strategy<Value = FuzzCase> {
        (two_tables(), any::<usize>()).prop_map(|((a, b), pick)| {
            let mtx = build_mutation(&a);
            match pick % 3 {
                0 => with_mutation(join_aggregate_case(a, b), mtx),
                1 => with_mutation(carried_scalar_case(a.clone(), b, pick), mtx),
                _ => with_mutation(cte_decomposed_case(a, b), mtx),
            }
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

    #[test]
    fn generator_can_emit_join_and_cte_and_carried_shapes() {
        use super::generate::fuzz_case;
        let mut runner = TestRunner::default();
        let (mut saw_join, mut saw_cte, mut saw_carried) = (false, false, false);
        for _ in 0..400 {
            let case = fuzz_case().new_tree(&mut runner).unwrap().current();
            let sql = case.select_body.rendered_sql.to_uppercase();
            saw_join |= sql.contains("JOIN");
            saw_cte |= sql.contains("WITH ");
            saw_carried |= sql.contains("COALESCE") || sql.contains("EXISTS") || sql.contains("CASE");
            if saw_join && saw_cte && saw_carried { break; }
        }
        assert!(saw_join && saw_cte && saw_carried,
            "generator must reach join/cte/carried shapes (join={saw_join} cte={saw_cte} carried={saw_carried})");
    }

    #[test]
    fn generator_emits_multi_statement_single_table_mutations() {
        use super::generate::fuzz_case;
        let mut runner = TestRunner::default();
        let mut saw_multi_stmt = false;
        for _ in 0..100 {
            let case = fuzz_case().new_tree(&mut runner).unwrap().current();
            // Count total statements across all txns: should see at least some with >= 2 statements.
            let total_stmts: usize = case.dml.iter().map(|txn| txn.statements.len()).sum();
            if total_stmts >= 2 {
                saw_multi_stmt = true;
                break;
            }
        }
        assert!(saw_multi_stmt, "generator must emit multi-statement transactions");
    }

    #[test]
    fn generator_reaches_filtered_variants() {
        use super::generate::fuzz_case;
        let mut runner = TestRunner::default();
        let mut filt = false;
        for _ in 0..400 {
            let c = fuzz_case().new_tree(&mut runner).unwrap().current();
            filt |= c.select_body.rendered_sql.to_uppercase().contains("WHERE");
            if filt { break; }
        }
        assert!(filt, "must reach filtered variants with WHERE predicates ({filt})");
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

    fn diff_subquery(mv: &str, imv: &str) -> String {
        format!(
            "( (SELECT * FROM {mv} EXCEPT SELECT * FROM {imv}) UNION ALL \
             (SELECT * FROM {imv} EXCEPT SELECT * FROM {mv}) ) d"
        )
    }

    /// Build the FROM clause that returns rows differing between mv and imv,
    /// exact for non-float columns and within a relative epsilon for float columns.
    /// Used when the case has any float output column.
    ///
    /// A row counts as differing when it has no counterpart on the other side that
    /// agrees on every non-float column (`IS NOT DISTINCT FROM`, so NULL keys and
    /// NULL groups match) and is within 1e-9 relative on every float column. This
    /// uses correlated `NOT EXISTS` rather than a `FULL JOIN ... ON a.k = b.k`
    /// precisely because `=` is not NULL-safe: a NULL group key never satisfies it,
    /// so the old FULL-JOIN form reported the (correct) NULL group as two phantom
    /// unmatched rows.
    pub fn float_diff_from_where(mv: &str, imv: &str, _keys: &[String], cols: &[Column]) -> String {
        let match_pred = |aa: &str, bb: &str| -> String {
            cols.iter()
                .map(|c| {
                    let n = &c.name;
                    if c.ty.is_float() {
                        format!(
                            "(({aa}.\"{n}\" IS NULL AND {bb}.\"{n}\" IS NULL) OR \
                              ({aa}.\"{n}\" IS NOT NULL AND {bb}.\"{n}\" IS NOT NULL AND \
                               abs({aa}.\"{n}\" - {bb}.\"{n}\") <= 1e-9 * \
                               GREATEST(abs({aa}.\"{n}\"), abs({bb}.\"{n}\"), 1)))"
                        )
                    } else {
                        format!("{aa}.\"{n}\" IS NOT DISTINCT FROM {bb}.\"{n}\"")
                    }
                })
                .collect::<Vec<_>>()
                .join(" AND ")
        };
        let col_list = cols
            .iter()
            .map(|c| format!("\"{}\"", c.name))
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            "( SELECT {cl} FROM {mv} a WHERE NOT EXISTS (SELECT 1 FROM {imv} b WHERE {mab}) \
              UNION ALL \
              SELECT {cl} FROM {imv} b WHERE NOT EXISTS (SELECT 1 FROM {mv} a WHERE {mba}) ) d",
            cl = col_list,
            mv = mv,
            imv = imv,
            mab = match_pred("a", "b"),
            mba = match_pred("b", "a"),
        )
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

        // DETERMINISTIC SETUP: Create base tables and MV. These never raise from codegen,
        // so they're safe to run in the outer transaction.
        for t in &case.tables {
            Spi::run(&render::create_table_sql(t))
                .expect("setup ddl: create base table failed");
        }

        let mv = format!("mv{suffix}");
        let imv = format!("imv{suffix}");

        Spi::run(&render::create_mv_sql(&mv, &case.select_body))
            .expect("setup ddl: create mv failed");

        // Build the DML statements for the DO block.
        let mut dml_lines = Vec::new();
        for txn in &case.dml {
            for stmt in &txn.statements {
                let cols = cols_of(&case, match stmt {
                    DmlStmt::Insert { table, .. }
                    | DmlStmt::Delete { table, .. }
                    | DmlStmt::Update { table, .. }
                    | DmlStmt::Truncate { table } => table,
                });
                let sql = render::dml_sql(stmt, &|_t: &str| cols.clone());
                dml_lines.push(format!("    {sql};"));
            }
        }
        let dml_block = dml_lines.join("\n");

        // Build flush lines if deferred.
        let mut flush_lines = Vec::new();
        if case.deferred {
            for t in &case.tables {
                flush_lines.push(format!("    PERFORM reflex_flush_deferred('{}');", t.name));
            }
        }
        let flush_block = flush_lines.join("\n");

        // Build the diff subquery (without the outer SELECT).
        // Use float-tolerant comparison if the case has any float output columns.
        let has_float = case.output_columns.iter().any(|c| c.ty.is_float());
        let diff_from = if has_float {
            float_diff_from_where(&mv, &imv, &case.unique_columns, &case.output_columns)
        } else {
            diff_subquery(&mv, &imv)
        };

        // Construct the DO block as a string. Use $reflexbody$ for body dollar-quoting
        // to avoid collisions with the DO block's outer $$ quotes.
        let keys = case.unique_columns.join(",");
        let mode = if case.deferred { "DEFERRED" } else { "IMMEDIATE" };
        let body = case.select_body.rendered_sql.clone();

        // Build a PL/pgSQL function that encodes the result as JSON for transport across SPI.
        // This avoids pgrx type deserialization issues with RETURNS TABLE.
        let func_name = format!("oracle_func_{}", seq);
        let create_func_sql = format!(
            r#"CREATE OR REPLACE FUNCTION public.{} () RETURNS text AS $func$
DECLARE
  v_msg text;
  v_diff bigint;
  v_status text := 'MATCH';
  v_detail text := '';
BEGIN
  v_msg := create_reflex_ivm('{}', $reflexbody${}$reflexbody$, '{}', NULL, '{}', NULL);
  IF position('{}' in v_msg) > 0 THEN
    v_status := 'SKIP';
    v_detail := v_msg;
  ELSIF v_msg NOT LIKE 'CREATE REFLEX%%' THEN
    v_status := 'BUG';
    v_detail := 'unexpected create return: ' || v_msg;
  ELSE
{}
{}
    REFRESH MATERIALIZED VIEW {};

    SELECT count(*)::bigint INTO v_diff FROM {};

    IF v_diff > 0 THEN
      v_status := 'BUG';
      v_detail := v_diff || ' mismatched rows';
    END IF;
  END IF;

  RETURN v_status || '|||' || v_detail;
EXCEPTION WHEN OTHERS THEN
  RETURN 'BUG' || '|||' || ('codegen exception: ' || SQLERRM);
END;
$func$ LANGUAGE plpgsql;
"#,
            func_name,
            imv,               // create_reflex_ivm arg
            body,              // $reflexbody content
            keys,              // third arg to create_reflex_ivm
            mode,              // fifth arg to create_reflex_ivm
            crate::REFLEX_UNSUPPORTED_TAG,  // position check
            dml_block,         // DML statements
            flush_block,       // flush statements
            mv,                // REFRESH target
            diff_from,         // SELECT FROM (exact or float-tolerant)
        );

        // Create the function.
        if let Err(e) = Spi::run(&create_func_sql) {
            return Outcome::Bug(format!("create function error: {e:?}"));
        }

        // Call the function and parse the result.
        let call_func_sql = format!("SELECT {}();", func_name);
        let outcome = match Spi::get_one::<&str>(&call_func_sql) {
            Ok(Some(result_str)) => {
                let parts: Vec<&str> = result_str.splitn(2, "|||").collect();
                let status = if !parts.is_empty() { parts[0] } else { "UNKNOWN" };
                let detail = if parts.len() > 1 { parts[1].to_string() } else { String::new() };
                match status {
                    "MATCH" => Outcome::Match,
                    "SKIP" => Outcome::Skip(detail),
                    "BUG" => Outcome::Bug(detail),
                    _ => Outcome::Bug(format!("unknown status: {}", status)),
                }
            }
            e => Outcome::Bug(format!("function call error: {:?}", e)),
        };

        // Note: No explicit cleanup of tables/MVs/IMVs here. The test runs in a transaction
        // that rolls back at the end, so all objects are cleaned up automatically.
        // Attempting to drop with CASCADE triggers pg_reflex's drop handlers which can cause
        // side effects. We rely on the outer transaction rollback for cleanup.

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

#[cfg(test)]
mod oracle_unit_tests {
    use super::model::{Column, ColType};
    use super::oracle::float_diff_from_where;

    #[test]
    fn float_compare_uses_relative_epsilon_and_null_safe_match() {
        let cols = vec![
            Column { name: "g".into(), ty: ColType::Int, nullable: false },
            Column { name: "s".into(), ty: ColType::Float8, nullable: true },
            Column { name: "t".into(), ty: ColType::Text, nullable: true },
        ];
        let sql = float_diff_from_where("v_mv", "v_imv", &["g".to_string()], &cols);
        assert!(sql.contains("1e-9"), "must use relative epsilon: {sql}");
        // NULL-safe correlated anti-join, not an equi FULL JOIN (finding #4).
        assert!(sql.contains("NOT EXISTS"), "must use NOT EXISTS anti-join: {sql}");
        assert!(!sql.contains("FULL JOIN"), "must not use a non-NULL-safe FULL JOIN: {sql}");
        assert!(sql.contains("IS NOT DISTINCT FROM"), "non-float cols matched NULL-safe: {sql}");
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_test]
fn oracle_recovery_survives_internal_exception() {
    // Prove that the DO/EXCEPTION pattern leaves the outer transaction clean.
    // We deliberately raise an exception in a DO block, then call evaluate() on
    // a normal case to prove the outer transaction is still usable.
    Spi::run("DO $$ BEGIN RAISE EXCEPTION 'boom'; EXCEPTION WHEN OTHERS THEN NULL; END $$")
        .unwrap();
    // outer txn must still be usable:
    use proptest::strategy::{Strategy, ValueTree};
    use proptest::test_runner::TestRunner;
    let mut runner = TestRunner::default();
    let c = generate::fuzz_case().new_tree(&mut runner).unwrap().current();
    match oracle::evaluate(&c) {
        oracle::Outcome::Match | oracle::Outcome::Skip(_) => {}
        oracle::Outcome::Bug(msg) => {
            panic!("expected Match/Skip after internal exception, got bug: {msg}")
        }
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

fn fuzz_case_count() -> u32 {
    std::env::var("PG_REFLEX_FUZZ_CASES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(64)
}

/// FIXED finding #1 — see docs/fuzz-findings.md. Root cause: the former data-probe
/// marked the LEFT-joined column NOT NULL from a create-time intermediate that
/// happened to be null-free, so MERGE maintenance dropped a later unmatched
/// primary-insert row. Fixed by `infer_not_null_columns`, which promotes a column
/// only when the query structurally guarantees non-NULL.
///
/// A two-table LEFT JOIN aggregate, after inserting a new PRIMARY-side (t0) row
/// that matches NO secondary row, must KEEP that row with the secondary columns NULL.
    #[cfg(any(test, feature = "pg_test"))]
    #[pg_test]
    fn finding_1_leftjoin_unmatched_primary_insert_drops_row() {
        Spi::run("CREATE TABLE f1_t0 (id int primary key, m numeric, d text)").unwrap();
        Spi::run("CREATE TABLE f1_t1 (id int primary key, fk int, w numeric)").unwrap();

        // Seed ONLY matched rows: every t0.id has a matching agg.g (= t1.fk), so
        // the LEFT-joined column `sw` is NULL-free in the create-time intermediate.
        // This is the precondition that makes the data-probe optimization wrongly
        // mark `sw` NOT NULL — the trigger for finding #1. (A seed where some rows
        // are already unmatched at create time would leave `sw` already-NULL and
        // never exercise the bug.)
        Spi::run("INSERT INTO f1_t0 VALUES (0,0.0,'g0'),(1,1.1,'g1'),(2,2.2,'g2')").unwrap();
        Spi::run("INSERT INTO f1_t1 VALUES (10,0,5.0),(11,1,6.0),(12,2,7.0)").unwrap();

        // Create the MV
        let body = "WITH agg AS (SELECT fk AS g, SUM(w) AS sw FROM f1_t1 GROUP BY fk) \
                    SELECT f1_t0.id, SUM(f1_t0.m) AS s, a.sw FROM f1_t0 LEFT JOIN agg a ON a.g = f1_t0.id GROUP BY f1_t0.id, a.sw";
        Spi::run(&format!("CREATE MATERIALIZED VIEW f1_mv AS {body}")).unwrap();

        // Create the IMV
        let r = crate::create_reflex_ivm("f1_imv", body, Some("id"), None, None, None);
        assert!(r.starts_with("CREATE REFLEX"), "IMV creation failed: {r}");

        // Insert a new primary-side row (id=8) that matches NO secondary row →
        // its `sw` must be NULL in the IMV, but a NOT-NULL-marked `sw` would make
        // MERGE maintenance drop the row entirely.
        Spi::run("INSERT INTO f1_t0 (id, m, d) VALUES (8, 3.2, 'g0')").unwrap();

        // Refresh the MV
        Spi::run("REFRESH MATERIALIZED VIEW f1_mv").unwrap();

        // Compare the two views
        let diff = Spi::get_one::<i64>(
            "SELECT count(*)::bigint FROM ( \
               (SELECT * FROM f1_mv EXCEPT SELECT * FROM f1_imv) UNION ALL \
               (SELECT * FROM f1_imv EXCEPT SELECT * FROM f1_mv)) d",
        )
        .unwrap()
        .unwrap();

        // Get the row sets for debugging
        let mv_rows = Spi::get_one::<String>(
            "SELECT string_agg(CAST((id, s, sw) AS text), '; ' ORDER BY id) FROM f1_mv",
        )
        .unwrap()
        .unwrap_or_else(|| "empty".into());

        let imv_rows = Spi::get_one::<String>(
            "SELECT string_agg(CAST((id, s, sw) AS text), '; ' ORDER BY id) FROM f1_imv",
        )
        .unwrap()
        .unwrap_or_else(|| "empty".into());

        assert_eq!(diff, 0, "finding #1: IMV diverged from MV by {diff} rows\nMV: {mv_rows}\nIMV: {imv_rows}");
    }

    /// OPEN finding #2 — see docs/fuzz-findings.md. Remove #[ignore] when fixed.
    ///
    /// A single-table passthrough view with DEFERRED incremental maintenance, after
    /// DML mutations, fails during reflex_flush_deferred() with "duplicate key value
    /// violates unique constraint" error, suggesting the maintenance logic is attempting
    /// to insert or merge rows that would create duplicate key violations.
    #[cfg(any(test, feature = "pg_test"))]
    #[pg_test]
    fn finding_2_deferred_mode_duplicate_key_violation() {
        // Minimal verified repro: in DEFERRED mode, INSERTing a new key and then
        // UPDATEing that SAME key within one deferred batch (before flush) makes the
        // flush MERGE violate the target unique constraint __reflex_uk_*. (A batch of
        // only-updates-of-existing-keys + only-inserts-of-new-keys flushes fine; the
        // trigger is insert+update of the SAME key in one batch.) flush takes the
        // SOURCE table name (f2_t0), not the IMV name.
        Spi::run("CREATE TABLE f2_t0 (id int primary key, m numeric)").unwrap();
        Spi::run("INSERT INTO f2_t0 VALUES (1, 1.0)").unwrap();
        let body = "SELECT id, m FROM f2_t0";
        Spi::run(&format!("CREATE MATERIALIZED VIEW f2_mv AS {body}")).unwrap();
        let r = crate::create_reflex_ivm("f2_imv", body, Some("id"), None, Some("DEFERRED"), None);
        assert!(r.starts_with("CREATE REFLEX"), "IMV creation failed: {r}");

        // One deferred batch: insert id=2, then update id=2.
        Spi::run("INSERT INTO f2_t0 VALUES (2, 5.0)").unwrap();
        Spi::run("UPDATE f2_t0 SET m = m + 1 WHERE id = 2").unwrap();

        // flush must NOT raise duplicate-key; IMV must then match a refreshed MV.
        Spi::run("SELECT reflex_flush_deferred('f2_t0')").unwrap();
        Spi::run("REFRESH MATERIALIZED VIEW f2_mv").unwrap();

        let diff = Spi::get_one::<i64>(
            "SELECT count(*)::bigint FROM ( \
               (SELECT * FROM f2_mv EXCEPT SELECT * FROM f2_imv) UNION ALL \
               (SELECT * FROM f2_imv EXCEPT SELECT * FROM f2_mv)) d",
        )
        .unwrap()
        .unwrap();

        assert_eq!(diff, 0, "finding #2: IMV diverged from MV by {diff} rows after DEFERRED flush");
    }

/// FIXED finding #3 — see docs/fuzz-findings.md. The former data-probe marked a
/// group-by column NOT NULL whenever the create-time data happened to be
/// null-free, even on a plain nullable column with no INNER-join / filter
/// guarantee. A later legitimately-NULL group was then dropped by `=`-matching
/// MERGE maintenance. Fixed by inferring NOT NULL only from query structure.
///
/// `GROUP BY d` on a nullable `d` that is null-free at create time, then inserting
/// NULL-`d` rows, must keep the NULL group in the IMV.
#[cfg(any(test, feature = "pg_test"))]
#[pg_test]
fn finding_3_nullable_groupby_key_drops_null_group() {
    Spi::run("CREATE TABLE g3_t0 (id int primary key, m numeric, d text)").unwrap();
    // Seed with NO nulls in `d`: the precondition that made the old data-probe
    // wrongly mark `d` NOT NULL.
    Spi::run("INSERT INTO g3_t0 VALUES (1,1.0,'a'),(2,2.0,'b'),(3,3.0,'a')").unwrap();
    let body = "SELECT d, SUM(m) AS s FROM g3_t0 GROUP BY d";
    Spi::run(&format!("CREATE MATERIALIZED VIEW g3_mv AS {body}")).unwrap();
    let r = crate::create_reflex_ivm("g3_imv", body, Some("d"), None, None, None);
    assert!(r.starts_with("CREATE REFLEX"), "IMV creation failed: {r}");
    // Introduce a legitimate NULL group.
    Spi::run("INSERT INTO g3_t0 VALUES (4,4.0,NULL),(5,5.0,NULL)").unwrap();
    Spi::run("REFRESH MATERIALIZED VIEW g3_mv").unwrap();
    let diff = Spi::get_one::<i64>(
        "SELECT count(*)::bigint FROM ( \
           (SELECT * FROM g3_mv EXCEPT SELECT * FROM g3_imv) UNION ALL \
           (SELECT * FROM g3_imv EXCEPT SELECT * FROM g3_mv)) d",
    )
    .unwrap()
    .unwrap();
    let mv = Spi::get_one::<String>("SELECT string_agg(CAST((d,s) AS text), '; ' ORDER BY d NULLS LAST) FROM g3_mv").unwrap().unwrap_or_default();
    let imv = Spi::get_one::<String>("SELECT string_agg(CAST((d,s) AS text), '; ' ORDER BY d NULLS LAST) FROM g3_imv").unwrap().unwrap_or_default();
    assert_eq!(diff, 0, "finding #3: nullable-groupby diverged by {diff}\nMV:  {mv}\nIMV: {imv}");
}

/// FIXED finding #4 (fuzz-harness false positive) — see docs/fuzz-findings.md.
/// A filtered float-aggregate with a NULL group is maintained correctly by
/// pg_reflex (exact NULL-safe diff is 0), but the harness's float comparator used
/// a `FULL JOIN ... ON a.k = b.k` that is not NULL-safe, so the NULL group showed
/// as two phantom unmatched rows. Guards both the IMV correctness and the now
/// NULL-safe `float_diff_from_where`.
#[cfg(any(test, feature = "pg_test"))]
#[pg_test]
fn finding_4_filtered_float_aggregate_null_group_diff_safe() {
    Spi::run("CREATE TABLE q4_t0 (id int primary key, m numeric, d text, f float8, x text)").unwrap();
    let body = "SELECT d, SUM(m) AS s, COUNT(*) AS c, AVG(m) AS avg_m, SUM(f) AS sf FROM q4_t0 WHERE id % 2 = 0 GROUP BY d";
    Spi::run(&format!("CREATE MATERIALIZED VIEW q4_mv AS {body}")).unwrap();
    // Create IMV on the EMPTY table (mirrors the fuzzer's incremental path).
    let r = crate::create_reflex_ivm("q4_imv", body, Some("d"), None, None, None);
    assert!(r.starts_with("CREATE REFLEX"), "IMV creation failed: {r}");
    // Apply DML incrementally, including a NULL-group row (id=200, d=NULL).
    Spi::run("INSERT INTO q4_t0 (id,m,d,f,x) VALUES (0,0.0,'g0',0.0,'g0'),(1,1.1,'g1',1.1,'g1'),(2,2.2,'g2',2.2,'g2'),(3,3.0,'g3',3.0,'g3'),(4,4.1,'g0',4.1,'g0'),(5,0.2,'g1',0.2,'g1'),(6,1.0,'g2',1.0,'g2'),(7,2.1,'g3',2.1,'g3')").unwrap();
    Spi::run("INSERT INTO q4_t0 (id,m,d,f,x) VALUES (100,0.1,'g0',0.1,'g0')").unwrap();
    Spi::run("INSERT INTO q4_t0 (id,m,d,f,x) VALUES (200,0.2,NULL,NULL,NULL)").unwrap();
    Spi::run("UPDATE q4_t0 SET m = m + 1 WHERE id % 2 = 0").unwrap();
    Spi::run("DELETE FROM q4_t0 WHERE id = 0").unwrap();
    Spi::run("REFRESH MATERIALIZED VIEW q4_mv").unwrap();

    // pg_reflex is correct: the exact NULL-safe diff is 0.
    let exact_diff = Spi::get_one::<i64>(
        "SELECT count(*)::bigint FROM ((SELECT * FROM q4_mv EXCEPT SELECT * FROM q4_imv) \
         UNION ALL (SELECT * FROM q4_imv EXCEPT SELECT * FROM q4_mv)) d").unwrap().unwrap();
    assert_eq!(exact_diff, 0, "pg_reflex diverged on filtered float aggregate with NULL group");

    // The harness float comparator must also see 0 (it must be NULL-safe).
    let cols = [
        model::Column { name: "d".into(), ty: model::ColType::Text, nullable: true },
        model::Column { name: "s".into(), ty: model::ColType::Numeric, nullable: true },
        model::Column { name: "c".into(), ty: model::ColType::BigInt, nullable: false },
        model::Column { name: "avg_m".into(), ty: model::ColType::Float8, nullable: true },
        model::Column { name: "sf".into(), ty: model::ColType::Float8, nullable: true },
    ];
    let float_diff = Spi::get_one::<i64>(&format!(
        "SELECT count(*)::bigint FROM {}",
        oracle::float_diff_from_where("q4_mv", "q4_imv", &["d".into()], &cols)
    )).unwrap().unwrap();
    assert_eq!(float_diff, 0, "harness float_diff_from_where is not NULL-group-safe");
}

/// Perf-preservation guard for the structural NOT-NULL inference. A
/// catalog-NULLable column made non-NULL by an INNER-join equi-condition (the
/// yse.ivm_sop_forecast_view 405 s shape) MUST still be promoted to NOT NULL so
/// MERGE maintenance keeps `=` matching (index-friendly). If this regresses, the
/// inference lost the INNER-join equi-key case.
#[cfg(any(test, feature = "pg_test"))]
#[pg_test]
fn inner_join_equikey_promoted_not_null() {
    Spi::run("CREATE TABLE ij_dp (id int primary key)").unwrap();
    // dpid is catalog-NULLABLE, but the INNER join below makes it non-NULL.
    Spi::run("CREATE TABLE ij_ss (id int primary key, dpid int, m numeric)").unwrap();
    Spi::run("INSERT INTO ij_dp VALUES (10),(20),(30)").unwrap();
    Spi::run("INSERT INTO ij_ss VALUES (1,10,1.0),(2,20,2.0),(3,10,3.0)").unwrap();
    let body = "SELECT ss.dpid, SUM(ss.m) AS s FROM ij_ss ss \
                INNER JOIN ij_dp dp ON dp.id = ss.dpid GROUP BY ss.dpid";
    let r = crate::create_reflex_ivm("ij_imv", body, Some("dpid"), None, None, None);
    assert!(r.starts_with("CREATE REFLEX"), "IMV creation failed: {r}");

    // The INNER-join equi-key dpid must be inferred NOT NULL.
    let promoted = Spi::get_one::<bool>(
        "SELECT COALESCE((aggregations::jsonb->'not_null_columns') @> '[\"dpid\"]'::jsonb, false) \
         FROM public.__reflex_ivm_reference WHERE name = 'ij_imv'",
    )
    .unwrap()
    .unwrap_or(false);
    assert!(
        promoted,
        "INNER-join equi-key 'dpid' was not inferred NOT NULL — 405s index optimization lost"
    );
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_test]
fn fuzz_differential_exact() {
    use proptest::test_runner::{Config, TestCaseError, TestRunner};

    use std::cell::RefCell;

    let cfg = Config {
        cases: fuzz_case_count(),
        failure_persistence: None,
        ..Config::default()
    };
    let mut runner = TestRunner::new(cfg);
    let first_bug = RefCell::new(None);
    let result = runner.run(&generate::fuzz_case(), |case| match oracle::evaluate(&case) {
        oracle::Outcome::Match | oracle::Outcome::Skip(_) => Ok(()),
        oracle::Outcome::Bug(msg) => {
            if first_bug.borrow().is_none() {
                *first_bug.borrow_mut() = Some((msg.clone(), oracle::repro_sql(&case)));
            }
            Err(TestCaseError::fail(format!(
                "{msg}\n--- minimal repro ---\n{}",
                oracle::repro_sql(&case)
            )))
        }
    });
    if let Err(e) = result {
        if let Some((msg, repro)) = first_bug.into_inner() {
            panic!("differential fuzz found a bug:\n{msg}\n--- minimal repro ---\n{repro}");
        } else {
            panic!("differential fuzz failed with no bug captured. proptest error: {e:?}");
        }
    }
}

/// Bug 1: COALESCE over a joined GROUP BY key. Either it builds and matches
/// the MV, or pg_reflex deliberately rejects it (tagged). It must NOT raise
/// a Postgres exception (the old failure: column "sx" does not exist).
#[cfg(any(test, feature = "pg_test"))]
#[pg_test]
fn regression_bug1_coalesce_over_joined_group_key() {
    use pgrx::prelude::*;
    Spi::run("CREATE TABLE b1_t(g int primary key, v int)").unwrap();
    Spi::run("CREATE TABLE b1_a(g int, x int)").unwrap();
    Spi::run("INSERT INTO b1_t VALUES (1,10),(2,20)").unwrap();
    Spi::run("INSERT INTO b1_a VALUES (1,5),(1,7)").unwrap();
    let body = "WITH agg AS (SELECT g, SUM(x) AS sx FROM b1_a GROUP BY g) \
                SELECT t.g, SUM(t.v) AS s, COALESCE(a.sx, 0) AS sx0 \
                FROM b1_t t LEFT JOIN agg a ON a.g = t.g GROUP BY t.g, a.sx";
    let r = crate::create_reflex_ivm("b1_imv", body, Some("g"), None, None, None);
    assert!(
        r.starts_with("CREATE REFLEX") || r.contains(crate::REFLEX_UNSUPPORTED_TAG),
        "Bug 1 regressed: create must succeed or be cleanly rejected, got: {r}"
    );
}

/// Bug 2: carried EXISTS with a boolean conjunct. Old failure: column ...
/// "is of type numeric but expression is of type boolean".
#[cfg(any(test, feature = "pg_test"))]
#[pg_test]
fn regression_bug2_exists_with_boolean_conjunct() {
    use pgrx::prelude::*;
    Spi::run("CREATE TABLE b2_t(g int primary key, v int)").unwrap();
    Spi::run("CREATE TABLE b2_pt(product_id int, is_active bool)").unwrap();
    Spi::run("INSERT INTO b2_t VALUES (1,10),(2,20)").unwrap();
    Spi::run("INSERT INTO b2_pt VALUES (1,true)").unwrap();
    let body = "SELECT t.g, SUM(t.v) AS s, \
                EXISTS(SELECT 1 FROM b2_pt c WHERE c.product_id = t.g AND c.is_active) AS flag \
                FROM b2_t t GROUP BY t.g";
    let r = crate::create_reflex_ivm("b2_imv", body, Some("g"), None, None, None);
    assert!(
        r.starts_with("CREATE REFLEX") || r.contains(crate::REFLEX_UNSUPPORTED_TAG),
        "Bug 2 regressed: create must succeed or be cleanly rejected, got: {r}"
    );
}

/// Bug 3 (commit 4d1d382): COUNT over a LEFT JOIN — secondary-side
/// incremental maintenance. Build, mutate the secondary side, and assert
/// the IMV matches a refreshed MV.
#[cfg(any(test, feature = "pg_test"))]
#[pg_test]
fn regression_bug3_count_over_left_join_secondary_side() {
    use pgrx::prelude::*;
    Spi::run("CREATE TABLE b3_t(g int primary key, v int)").unwrap();
    Spi::run("CREATE TABLE b3_s(g int, w int)").unwrap();
    Spi::run("INSERT INTO b3_t VALUES (1,10),(2,20),(3,30)").unwrap();
    Spi::run("INSERT INTO b3_s VALUES (1,1),(1,1),(2,1)").unwrap();
    let body = "SELECT t.g, COUNT(s.w) AS c FROM b3_t t \
                LEFT JOIN b3_s s ON s.g = t.g GROUP BY t.g";
    Spi::run(&format!("CREATE MATERIALIZED VIEW b3_mv AS {body}")).unwrap();
    let r = crate::create_reflex_ivm("b3_imv", body, Some("g"), None, None, None);
    assert!(r.starts_with("CREATE REFLEX"), "create failed: {r}");
    // Mutate the secondary (LEFT) side.
    Spi::run("INSERT INTO b3_s VALUES (3,1),(1,1)").unwrap();
    Spi::run("DELETE FROM b3_s WHERE g = 2").unwrap();
    Spi::run("REFRESH MATERIALIZED VIEW b3_mv").unwrap();
    let diff = Spi::get_one::<i64>(
        "SELECT count(*)::bigint FROM ( \
           (SELECT * FROM b3_mv EXCEPT SELECT * FROM b3_imv) UNION ALL \
           (SELECT * FROM b3_imv EXCEPT SELECT * FROM b3_mv)) d",
    )
    .unwrap()
    .unwrap();
    assert_eq!(diff, 0, "Bug 3 regressed: IMV diverged from MV by {diff} rows");
}

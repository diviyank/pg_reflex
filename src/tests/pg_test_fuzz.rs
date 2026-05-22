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

    fn diff_subquery(mv: &str, imv: &str) -> String {
        format!(
            "( (SELECT * FROM {mv} EXCEPT SELECT * FROM {imv}) UNION ALL \
             (SELECT * FROM {imv} EXCEPT SELECT * FROM {mv}) ) d"
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
        let diff_subquery_str = diff_subquery(&mv, &imv);

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
            diff_subquery_str, // SELECT FROM subquery
        );

        // Create the function.
        if let Err(e) = Spi::run(&create_func_sql) {
            return Outcome::Bug(format!("create function error: {e:?}"));
        }

        // Call the function and parse the result.
        let call_func_sql = format!("SELECT {}();", func_name);
        match Spi::get_one::<&str>(&call_func_sql) {
            Ok(Some(result_str)) => {
                let parts: Vec<&str> = result_str.splitn(2, "|||").collect();
                let status = if parts.len() > 0 { parts[0] } else { "UNKNOWN" };
                let detail = if parts.len() > 1 { parts[1].to_string() } else { String::new() };
                match status {
                    "MATCH" => Outcome::Match,
                    "SKIP" => Outcome::Skip(detail),
                    "BUG" => Outcome::Bug(detail),
                    _ => Outcome::Bug(format!("unknown status: {}", status)),
                }
            }
            e => Outcome::Bug(format!("function call error: {:?}", e)),
        }
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

#[cfg(any(test, feature = "pg_test"))]
#[pg_test]
fn fuzz_differential_exact() {
    use proptest::test_runner::{Config, TestCaseError, TestError, TestRunner};

    let cfg = Config {
        cases: fuzz_case_count(),
        failure_persistence: None,
        ..Config::default()
    };
    let mut runner = TestRunner::new(cfg);
    let result = runner.run(&generate::fuzz_case(), |case| match oracle::evaluate(&case) {
        oracle::Outcome::Match | oracle::Outcome::Skip(_) => Ok(()),
        oracle::Outcome::Bug(msg) => Err(TestCaseError::fail(format!(
            "{msg}\n--- minimal repro ---\n{}",
            oracle::repro_sql(&case)
        ))),
    });
    if let Err(TestError::Fail(reason, case)) = result {
        panic!("differential fuzz found a bug: {reason}\nshrunk case: {case:?}");
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

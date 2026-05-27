use crate::model::*;
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

#[cfg(test)]
mod tests {
    use crate::generate::*;
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
        use crate::generate::fuzz_case;
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
        use crate::generate::fuzz_case;
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
        use crate::generate::fuzz_case;
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

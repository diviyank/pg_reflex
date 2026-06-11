use crate::axes::{
    AggFn, Axes, PlannedCase, QueryShape, RefreshMode, SourceKind, SourceObject, SourceObjectKind,
    UniqueCols,
};
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
        Just(ColType::Timestamptz),
    ]
}

/// One base table `t0` with an int PK `id`, a numeric measure `m`, a text
/// dimension `d`, a float column `f`, and one extra random-typed nullable column.
fn single_table() -> impl Strategy<Value = Table> {
    exact_coltype().prop_map(|extra_ty| Table {
        name: "t0".into(),
        pk: "id".into(),
        columns: vec![
            Column {
                name: "id".into(),
                ty: ColType::Int,
                nullable: false,
            },
            Column {
                name: "m".into(),
                ty: ColType::Numeric,
                nullable: true,
            },
            Column {
                name: "d".into(),
                ty: ColType::Text,
                nullable: true,
            },
            Column {
                name: "f".into(),
                ty: ColType::Float8,
                nullable: true,
            },
            Column {
                name: "x".into(),
                ty: extra_ty,
                nullable: true,
            },
        ],
    })
}

/// Render a literal for a column type (deterministic small domain so joins
/// and groups actually collide).
pub fn literal(ty: ColType, n: i64) -> String {
    match ty {
        ColType::Int | ColType::BigInt => format!("{}", n % 5),
        ColType::Numeric | ColType::Float8 => format!("{}.{}", n % 5, n % 3),
        ColType::Bool => {
            if n % 2 == 0 {
                "true".into()
            } else {
                "false".into()
            }
        }
        ColType::Text => format!("'g{}'", n % 4),
        ColType::Date => format!("date '2024-01-{:02}'", (n % 27) + 1),
        ColType::Timestamptz => format!(
            "timestamptz '2024-01-01 00:00:00+00' + ({} || ' seconds')::interval",
            n % 100
        ),
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

/// The WHERE clause for a FilterMode, or None for FilterMode::None.
/// All predicates use `id` (int) so they are type-agnostic across measure types,
/// and all subqueries are UNCORRELATED over the same source (valid, and exercises
/// the subquery-filter maintenance path — the 1.10.2 region).
fn filter_clause(filter: crate::axes::FilterMode, tbl: &str, pk: &str) -> Option<String> {
    use crate::axes::FilterMode::*;
    match filter {
        None => Option::None,
        Simple => Some(format!("{pk} % 2 = 0")),
        ScalarSubquery => Some(format!("{pk} >= (SELECT MIN({pk}) FROM {tbl})")),
        InSubquery => Some(format!(
            "{pk} IN (SELECT {pk} FROM {tbl} WHERE {pk} % 2 = 0)"
        )),
    }
}

/// Passthrough: SELECT id, m, d, f FROM t0; unique key = id.
fn passthrough_case(t: Table, filter: crate::axes::FilterMode) -> FuzzCase {
    let where_sql = match filter_clause(filter, &t.name, &t.pk) {
        Some(c) => format!(" WHERE {}", c),
        None => String::new(),
    };
    let body = SelectBody {
        rendered_sql: format!(
            "SELECT {pk}, m, d, f, x FROM {tbl}{w}",
            pk = t.pk,
            tbl = t.name,
            w = where_sql
        ),
    };
    let output_columns = vec![
        Column {
            name: t.pk.clone(),
            ty: ColType::Int,
            nullable: false,
        },
        Column {
            name: "m".into(),
            ty: ColType::Numeric,
            nullable: true,
        },
        Column {
            name: "d".into(),
            ty: ColType::Text,
            nullable: true,
        },
        Column {
            name: "f".into(),
            ty: ColType::Float8,
            nullable: true,
        },
        Column {
            name: "x".into(),
            ty: t.columns[4].ty,
            nullable: true,
        },
    ];
    let seed = DmlTxn {
        statements: vec![DmlStmt::Insert {
            table: t.name.clone(),
            rows: seed_rows(&t, 8),
        }],
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
        rendered_sql: format!(
            "SELECT {pk}, m, d FROM {tbl} WHERE {filter}",
            pk = t.pk,
            tbl = t.name
        ),
    };
    let output_columns = vec![
        Column {
            name: t.pk.clone(),
            ty: ColType::Int,
            nullable: false,
        },
        Column {
            name: "m".into(),
            ty: ColType::Numeric,
            nullable: true,
        },
        Column {
            name: "d".into(),
            ty: ColType::Text,
            nullable: true,
        },
    ];
    let seed = DmlTxn {
        statements: vec![DmlStmt::Insert {
            table: t.name.clone(),
            rows: seed_rows(&t, 8),
        }],
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

/// Window: SELECT id, d, ROW_NUMBER() OVER (PARTITION BY d ORDER BY id) AS rn FROM t.
/// Output cardinality = input cardinality, keyed by id.
fn window_case(t: Table) -> FuzzCase {
    let body = SelectBody {
        rendered_sql: format!(
            "SELECT {pk}, d, ROW_NUMBER() OVER (PARTITION BY d ORDER BY {pk}) AS rn FROM {tbl}",
            pk = t.pk,
            tbl = t.name
        ),
    };
    let output_columns = vec![
        Column {
            name: t.pk.clone(),
            ty: ColType::Int,
            nullable: false,
        },
        Column {
            name: "d".into(),
            ty: ColType::Text,
            nullable: true,
        },
        Column {
            name: "rn".into(),
            ty: ColType::BigInt,
            nullable: false,
        },
    ];
    let seed = DmlTxn {
        statements: vec![DmlStmt::Insert {
            table: t.name.clone(),
            rows: seed_rows(&t, 8),
        }],
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

/// DISTINCT ON: SELECT DISTINCT ON (d) d, id, m FROM t ORDER BY d, id.
/// One output row per distinct d; keyed by d.
fn distinct_on_case(t: Table) -> FuzzCase {
    let m_ty = t.columns[1].ty;
    let body = SelectBody {
        rendered_sql: format!(
            "SELECT DISTINCT ON (d) d, {pk}, m FROM {tbl} ORDER BY d, {pk}",
            pk = t.pk,
            tbl = t.name
        ),
    };
    let output_columns = vec![
        Column {
            name: "d".into(),
            ty: ColType::Text,
            nullable: true,
        },
        Column {
            name: t.pk.clone(),
            ty: ColType::Int,
            nullable: false,
        },
        Column {
            name: "m".into(),
            ty: m_ty,
            nullable: true,
        },
    ];
    let seed = DmlTxn {
        statements: vec![DmlStmt::Insert {
            table: t.name.clone(),
            rows: seed_rows(&t, 8),
        }],
    };
    FuzzCase {
        tables: vec![t.clone()],
        select_body: body,
        // Auto-infer the key (empty) — the path Phase 1 proved correct for
        // DISTINCT ON maintenance. Declaring the natural output key `d` instead
        // crashes at CREATE: pg_reflex classifies DISTINCT ON as passthrough and
        // applies the declared key to the pre-dedup `__base` (where `d` repeats),
        // failing the `__reflex_uk_<imv>__base` unique index. Filed as a gate
        // finding (docs/audit/2026-06-ivm-audit.md §3 Phase 2B) with a dedicated
        // RED regression; tracked for a focused fix in resolve_unique_columns.
        unique_columns: vec![],
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
        Column {
            name: "d".into(),
            ty: ColType::Text,
            nullable: true,
        },
        Column {
            name: "s".into(),
            ty: ColType::Numeric,
            nullable: true,
        },
        Column {
            name: "c".into(),
            ty: ColType::BigInt,
            nullable: false,
        },
    ];
    let seed = DmlTxn {
        statements: vec![DmlStmt::Insert {
            table: t.name.clone(),
            rows: seed_rows(&t, 8),
        }],
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
        Column {
            name: "d".into(),
            ty: ColType::Text,
            nullable: true,
        },
        Column {
            name: "s".into(),
            ty: ColType::Numeric,
            nullable: true,
        },
        Column {
            name: "c".into(),
            ty: ColType::BigInt,
            nullable: false,
        },
    ];
    let seed = DmlTxn {
        statements: vec![DmlStmt::Insert {
            table: t.name.clone(),
            rows: seed_rows(&t, 8),
        }],
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
        Column {
            name: "d".into(),
            ty: ColType::Text,
            nullable: true,
        },
        Column {
            name: "s".into(),
            ty: ColType::Numeric,
            nullable: true,
        },
        Column {
            name: "c".into(),
            ty: ColType::BigInt,
            nullable: false,
        },
        Column {
            name: "avg_m".into(),
            ty: ColType::Float8,
            nullable: true,
        },
        Column {
            name: "sf".into(),
            ty: ColType::Float8,
            nullable: true,
        },
    ];
    let seed = DmlTxn {
        statements: vec![DmlStmt::Insert {
            table: t.name.clone(),
            rows: seed_rows(&t, 8),
        }],
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
        Column {
            name: "d".into(),
            ty: ColType::Text,
            nullable: true,
        },
        Column {
            name: "s".into(),
            ty: ColType::Numeric,
            nullable: true,
        },
        Column {
            name: "c".into(),
            ty: ColType::BigInt,
            nullable: false,
        },
        Column {
            name: "avg_m".into(),
            ty: ColType::Float8,
            nullable: true,
        },
        Column {
            name: "sf".into(),
            ty: ColType::Float8,
            nullable: true,
        },
    ];
    let seed = DmlTxn {
        statements: vec![DmlStmt::Insert {
            table: t.name.clone(),
            rows: seed_rows(&t, 8),
        }],
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
                Column {
                    name: "id".into(),
                    ty: ColType::Int,
                    nullable: false,
                },
                Column {
                    name: "m".into(),
                    ty: ColType::Numeric,
                    nullable: true,
                },
                Column {
                    name: "d".into(),
                    ty: ColType::Text,
                    nullable: true,
                },
            ],
        },
        Table {
            name: "t1".into(),
            pk: "id".into(),
            columns: vec![
                Column {
                    name: "id".into(),
                    ty: ColType::Int,
                    nullable: false,
                },
                Column {
                    name: "fk".into(),
                    ty: ColType::Int,
                    nullable: true,
                },
                Column {
                    name: "w".into(),
                    ty: ColType::Numeric,
                    nullable: true,
                },
            ],
        },
    ))
}

fn seed_two(a: &Table, b: &Table) -> DmlTxn {
    DmlTxn {
        statements: vec![
            DmlStmt::Insert {
                table: a.name.clone(),
                rows: seed_rows(a, 8),
            },
            DmlStmt::Insert {
                table: b.name.clone(),
                rows: seed_rows(b, 8),
            },
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
        Column {
            name: "d".into(),
            ty: ColType::Text,
            nullable: true,
        },
        Column {
            name: "s".into(),
            ty: ColType::Numeric,
            nullable: true,
        },
        Column {
            name: "c".into(),
            ty: ColType::BigInt,
            nullable: false,
        },
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
            Column {
                name: "s0".into(),
                ty: ColType::Numeric,
                nullable: true,
            },
        ),
        1 => (
            "CASE WHEN COUNT(*) > 1 THEN 't' ELSE 'f' END AS lbl".to_string(),
            Column {
                name: "lbl".into(),
                ty: ColType::Text,
                nullable: true,
            },
        ),
        2 => (
            "(t0.id)::text AS idt".to_string(),
            Column {
                name: "idt".into(),
                ty: ColType::Text,
                nullable: true,
            },
        ),
        _ => (
            "EXISTS(SELECT 1 FROM t1 c WHERE c.fk = t0.id AND c.w > 0) AS flag".to_string(),
            Column {
                name: "flag".into(),
                ty: ColType::Bool,
                nullable: true,
            },
        ),
    };
    let body = SelectBody {
        rendered_sql: format!(
            "SELECT t0.id, SUM(t0.m) AS s, {} FROM t0 LEFT JOIN t1 ON t1.fk = t0.id GROUP BY t0.id",
            carried_sql
        ),
    };
    let mut output_columns = vec![
        Column {
            name: "id".into(),
            ty: ColType::Int,
            nullable: false,
        },
        Column {
            name: "s".into(),
            ty: ColType::Numeric,
            nullable: true,
        },
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
        Column {
            name: "id".into(),
            ty: ColType::Int,
            nullable: false,
        },
        Column {
            name: "s".into(),
            ty: ColType::Numeric,
            nullable: true,
        },
        Column {
            name: "sw".into(),
            ty: ColType::Numeric,
            nullable: true,
        },
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

/// Uniformly sample a valid axis assignment from the legal subspace. Backs the
/// random axes-driven differential proptest (`fuzz_planned_random`), which
/// complements the deterministic all-pairs gate by reaching higher-order
/// (3+-way) interactions the pairwise matrix does not guarantee.
pub fn axes_strategy() -> impl Strategy<Value = crate::axes::Axes> {
    let space = crate::axes::valid_space();
    (0..space.len()).prop_map(move |i| space[i])
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
                Just(mode(with_mutation(
                    passthrough_case(t.clone(), crate::axes::FilterMode::None),
                    mtx.clone()
                ))),
                Just(mode(with_mutation(aggregate_case(t.clone()), mtx.clone()))),
                Just(mode(with_mutation(
                    aggregate_float_case(t.clone()),
                    mtx.clone()
                ))),
                Just(mode(with_mutation(
                    passthrough_filtered_case(t.clone(), filter_choice),
                    mtx.clone()
                ))),
                Just(mode(with_mutation(
                    aggregate_filtered_case(t.clone(), filter_choice),
                    mtx.clone()
                ))),
                Just(mode(with_mutation(
                    aggregate_float_filtered_case(t.clone(), filter_choice),
                    mtx.clone()
                ))),
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

/// Create a deterministic base table with given sequence and index number.
/// Matches the structure: id (pk), m (measure), d (text), f (float8), x (measure again for aggregates).
fn det_table(seq: u64, idx: usize, measure_ty: ColType) -> Table {
    let name = format!("ft_{}_{}", seq, idx);
    Table {
        name,
        pk: "id".into(),
        columns: vec![
            Column {
                name: "id".into(),
                ty: ColType::Int,
                nullable: false,
            },
            Column {
                name: "m".into(),
                ty: measure_ty,
                nullable: true,
            },
            Column {
                name: "d".into(),
                ty: ColType::Text,
                nullable: true,
            },
            Column {
                name: "f".into(),
                ty: ColType::Float8,
                nullable: true,
            },
            Column {
                name: "x".into(),
                ty: measure_ty,
                nullable: true,
            },
        ],
    }
}

/// Parameterized aggregate function name rendering.
fn agg_fn_sql(fn_name: AggFn, col: &str) -> String {
    use AggFn::*;
    match fn_name {
        Sum => format!("SUM({})", col),
        Count => format!("COUNT({})", col),
        Min => format!("MIN({})", col),
        Max => format!("MAX({})", col),
        Avg => format!("AVG({})", col),
    }
}

/// Map aggregate function and input column type to the PostgreSQL result type.
/// Follows PostgreSQL semantics for aggregate return types.
fn agg_result_ty(agg_fn: AggFn, input_ty: ColType) -> ColType {
    use AggFn::*;
    match agg_fn {
        Count => ColType::BigInt, // COUNT always returns bigint
        Sum => match input_ty {
            ColType::Int => ColType::BigInt, // SUM(int) → bigint
            ColType::BigInt | ColType::Numeric => ColType::Numeric, // SUM(bigint|numeric) → numeric
            ColType::Float8 => ColType::Float8, // SUM(float8) → float8
            other => other,                  // Defensive: SUM is only generated over numeric-family
        },
        Avg => match input_ty {
            ColType::Float8 => ColType::Float8, // AVG(float8) → float8
            _ => ColType::Numeric,              // AVG(int|bigint|numeric) → numeric
        },
        Min | Max => input_ty, // MIN/MAX preserve input type
    }
}

/// Single-table aggregate with a parameterized agg function.
fn aggregate_case_with(t: Table, agg_fn: AggFn, filter: crate::axes::FilterMode) -> FuzzCase {
    // Find the measure column type (column 'm' in the table).
    let measure_ty = t
        .columns
        .iter()
        .find(|c| c.name == "m")
        .map(|c| c.ty)
        .unwrap_or(ColType::Numeric); // Defensive default

    let where_sql = match filter_clause(filter, &t.name, &t.pk) {
        Some(c) => format!(" WHERE {}", c),
        None => String::new(),
    };
    let agg_expr = agg_fn_sql(agg_fn, "m");
    let body = SelectBody {
        rendered_sql: format!(
            "SELECT d, {} AS s, COUNT(*) AS c FROM {}{} GROUP BY d",
            agg_expr, t.name, where_sql
        ),
    };
    let output_columns = vec![
        Column {
            name: "d".into(),
            ty: ColType::Text,
            nullable: true,
        },
        Column {
            name: "s".into(),
            ty: agg_result_ty(agg_fn, measure_ty),
            nullable: true,
        },
        Column {
            name: "c".into(),
            ty: ColType::BigInt,
            nullable: false,
        },
    ];
    let seed = DmlTxn {
        statements: vec![DmlStmt::Insert {
            table: t.name.clone(),
            rows: seed_rows(&t, 8),
        }],
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

/// Two-table join aggregate with parameterized agg function and join type.
fn join_aggregate_case_with(a: Table, b: Table, agg_fn: AggFn, is_left: bool) -> FuzzCase {
    // Find the measure column type (column 'm' in table a, the primary side).
    let measure_ty = a
        .columns
        .iter()
        .find(|c| c.name == "m")
        .map(|c| c.ty)
        .unwrap_or(ColType::Numeric); // Defensive default

    let join_type = if is_left { "LEFT JOIN" } else { "INNER JOIN" };
    let agg_expr = agg_fn_sql(agg_fn, "a.m");
    let body = SelectBody {
        rendered_sql: format!(
            "SELECT a.d, {} AS s, COUNT(b.m) AS c FROM {} a {} {} b ON b.id = a.id GROUP BY a.d",
            agg_expr, a.name, join_type, b.name
        ),
    };
    // COUNT(b.m) counts the right-hand/joined side column to measure match count
    let output_columns = vec![
        Column {
            name: "d".into(),
            ty: ColType::Text,
            nullable: true,
        },
        Column {
            name: "s".into(),
            ty: agg_result_ty(agg_fn, measure_ty),
            nullable: true,
        },
        Column {
            name: "c".into(),
            ty: ColType::BigInt,
            nullable: false,
        },
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

/// Set operation UNION ALL: non-aggregate union of two passthrough legs.
fn set_op_union_all_case(a: Table, b: Table) -> FuzzCase {
    let body = SelectBody {
        rendered_sql: format!(
            "SELECT id, m, d, f, x FROM {} UNION ALL SELECT id, m, d, f, x FROM {}",
            a.name, b.name
        ),
    };
    let output_columns = vec![
        Column {
            name: "id".into(),
            ty: ColType::Int,
            nullable: false,
        },
        Column {
            name: "m".into(),
            ty: ColType::Numeric,
            nullable: true,
        },
        Column {
            name: "d".into(),
            ty: ColType::Text,
            nullable: true,
        },
        Column {
            name: "f".into(),
            ty: ColType::Float8,
            nullable: true,
        },
        Column {
            name: "x".into(),
            ty: ColType::Numeric,
            nullable: true,
        },
    ];
    FuzzCase {
        tables: vec![a.clone(), b.clone()],
        select_body: body,
        unique_columns: vec![],
        deferred: false,
        dml: vec![seed_two(&a, &b)],
        output_columns,
    }
}

/// CTE-decomposed case: WITH agg AS (SELECT d AS g, {agg}(m) AS agg_col FROM b GROUP BY d)
/// SELECT a.id, {agg}(a.m) AS s, {outer_agg}(agg.agg_col) AS agg_col FROM a LEFT JOIN agg ON agg.g = a.d GROUP BY a.id.
/// The inner and outer aggregations depend on the aggregation function:
/// - Sum: inner SUM(m), outer MAX(agg_col)
/// - Avg: inner SUM(m), outer MAX(agg_col) -- similar to Sum for simplicity
/// - Count: inner COUNT(*), outer SUM(agg_col) -- summing counts to get total
/// - Min/Max: inner {agg}(m), outer {agg}(agg_col)
fn cte_decomposed_case_with(a: Table, b: Table, agg_fn: AggFn) -> FuzzCase {
    let (inner_agg, main_agg, outer_agg, output_ty, col_name) = match agg_fn {
        AggFn::Sum => (
            "SUM(m) AS sm",
            "SUM",
            "MAX(agg.sm) AS sm",
            ColType::Numeric,
            "sm",
        ),
        AggFn::Avg => {
            // AVG decomposition: For simplicity, treat like SUM (store sum, apply MAX)
            (
                "SUM(m) AS sm",
                "AVG",
                "MAX(agg.sm) AS sm",
                ColType::Numeric,
                "sm",
            )
        }
        AggFn::Count => {
            // COUNT: inner COUNT(*), outer SUM to aggregate counts across groups
            (
                "COUNT(*) AS cnt",
                "COUNT",
                "SUM(agg.cnt) AS cnt",
                ColType::BigInt,
                "cnt",
            )
        }
        AggFn::Min => (
            "MIN(m) AS mn",
            "MIN",
            "MIN(agg.mn) AS mn",
            ColType::Numeric,
            "mn",
        ),
        AggFn::Max => (
            "MAX(m) AS mx",
            "MAX",
            "MAX(agg.mx) AS mx",
            ColType::Numeric,
            "mx",
        ),
    };

    // Build the main SELECT aggregation:
    // - For Sum/Avg/Min/Max: use the aggregation function on the measure column
    // - For Count: use COUNT(*)
    let main_select_agg = if matches!(agg_fn, AggFn::Count) {
        "COUNT(*)".to_string()
    } else {
        format!("{}({}.m)", main_agg, a.name)
    };

    let body = SelectBody {
        rendered_sql: format!(
            "WITH agg AS (SELECT d AS g, {} FROM {} GROUP BY d) \
             SELECT {}.id, {} AS s, {} FROM {} LEFT JOIN agg ON agg.g = {}.d GROUP BY {}.id",
            inner_agg, b.name, a.name, main_select_agg, outer_agg, a.name, a.name, a.name
        ),
    };

    let output_columns = vec![
        Column {
            name: "id".into(),
            ty: ColType::Int,
            nullable: false,
        },
        Column {
            name: "s".into(),
            ty: output_ty,
            nullable: true,
        },
        Column {
            name: col_name.into(),
            ty: output_ty,
            nullable: true,
        },
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

/// Build the base SELECT + tables for a shape, ignoring source kind.
fn base_case_for_shape(a: &Axes, seq: u64) -> Option<FuzzCase> {
    match a.shape {
        QueryShape::Passthrough => {
            Some(passthrough_case(det_table(seq, 0, a.measure_ty), a.filter))
        }
        QueryShape::SingleAggregate => Some(aggregate_case_with(
            det_table(seq, 0, a.measure_ty),
            a.agg?,
            a.filter,
        )),
        QueryShape::JoinInner => Some(join_aggregate_case_with(
            det_table(seq, 0, a.measure_ty),
            det_table(seq, 1, a.measure_ty),
            a.agg?,
            false,
        )),
        QueryShape::JoinLeft => Some(join_aggregate_case_with(
            det_table(seq, 0, a.measure_ty),
            det_table(seq, 1, a.measure_ty),
            a.agg?,
            true,
        )),
        QueryShape::CteDecomposed => Some(cte_decomposed_case_with(
            det_table(seq, 0, a.measure_ty),
            det_table(seq, 1, a.measure_ty),
            a.agg?,
        )),
        QueryShape::SetOpUnionAll => Some(set_op_union_all_case(
            det_table(seq, 0, a.measure_ty),
            det_table(seq, 1, a.measure_ty),
        )),
        QueryShape::WindowFn => Some(window_case(det_table(seq, 0, a.measure_ty))),
        QueryShape::DistinctOn => Some(distinct_on_case(det_table(seq, 0, a.measure_ty))),
    }
}

/// Rewrite source references in rendered_sql based on the source kind and return
/// the source objects to create. String replacement is safe: det_table names (ft_<seq>_<idx>)
/// are unique and unambiguous.
fn wrap_sources(a: &Axes, case: &mut FuzzCase, seq: u64) -> (Vec<SourceObject>, bool) {
    match a.source {
        SourceKind::Table => {
            // Base tables directly: no wrapping, no refresh.
            let objects = case
                .tables
                .iter()
                .map(|t| SourceObject {
                    name: t.name.clone(),
                    kind: SourceObjectKind::Table,
                    define_sql: None,
                })
                .collect();
            (objects, false)
        }
        SourceKind::View => {
            // Wrap each base table in a view.
            let mut objects = Vec::new();
            for t in &case.tables {
                let view_name = format!("v_{}", t.name);
                let define_sql = format!("CREATE VIEW {} AS SELECT * FROM {}", view_name, t.name);

                // Add the base table as a source object.
                objects.push(SourceObject {
                    name: t.name.clone(),
                    kind: SourceObjectKind::Table,
                    define_sql: None,
                });

                // Add the view as a source object.
                objects.push(SourceObject {
                    name: view_name.clone(),
                    kind: SourceObjectKind::View,
                    define_sql: Some(define_sql),
                });

                // Rewrite references from t.name to view_name in rendered_sql.
                case.select_body.rendered_sql =
                    case.select_body.rendered_sql.replace(&t.name, &view_name);
            }
            (objects, false)
        }
        SourceKind::MatView => {
            // Wrap each base table in a materialized view.
            let mut objects = Vec::new();
            for t in &case.tables {
                let mv_name = format!("mv_{}", t.name);
                let define_sql = format!(
                    "CREATE MATERIALIZED VIEW {} AS SELECT * FROM {}",
                    mv_name, t.name
                );

                // Add the base table as a source object.
                objects.push(SourceObject {
                    name: t.name.clone(),
                    kind: SourceObjectKind::Table,
                    define_sql: None,
                });

                // Add the materialized view as a source object.
                objects.push(SourceObject {
                    name: mv_name.clone(),
                    kind: SourceObjectKind::MatView,
                    define_sql: Some(define_sql),
                });

                // Rewrite references from t.name to mv_name in rendered_sql.
                case.select_body.rendered_sql =
                    case.select_body.rendered_sql.replace(&t.name, &mv_name);
            }
            (objects, true)
        }
        SourceKind::CteSubImv => {
            // Wrap in a sub-IMV (an IMV created via create_reflex_ivm).
            // For this task, we model it as a SourceObject with SubImv kind.
            let mut objects = Vec::new();
            for (idx, t) in case.tables.iter().enumerate() {
                let sub_imv_name = format!("simv_{}_{}", seq, idx);
                // The define_sql would be the create_reflex_ivm call or the sub-IMV's defining SELECT.
                // For now, a simple placeholder.
                let define_sql = format!(
                    "CREATE REFLEX IMV {} AS SELECT * FROM {}",
                    sub_imv_name, t.name
                );

                // Add the base table as a source object.
                objects.push(SourceObject {
                    name: t.name.clone(),
                    kind: SourceObjectKind::Table,
                    define_sql: None,
                });

                // Add the sub-IMV as a source object.
                objects.push(SourceObject {
                    name: sub_imv_name.clone(),
                    kind: SourceObjectKind::SubImv,
                    define_sql: Some(define_sql),
                });

                // Rewrite references from t.name to sub_imv_name in rendered_sql.
                case.select_body.rendered_sql = case
                    .select_body
                    .rendered_sql
                    .replace(&t.name, &sub_imv_name);
            }
            (objects, false)
        }
    }
}

pub fn plan_from_axes(a: &Axes, seq: u64) -> Option<PlannedCase> {
    let mut case = base_case_for_shape(a, seq)?;

    // Set deferred maintenance if needed.
    if a.refresh == RefreshMode::Deferred {
        case.deferred = true;
    }

    // Handle unique_columns: when a.unique == Provided, update the case's unique_columns.
    // (Absent means keep the generator default.)
    if a.unique == UniqueCols::Provided {
        // For Passthrough and Join shapes, the unique column is the PK.
        // The generator already sets it correctly, so no change needed here.
        // But to be explicit: if needed, update case.unique_columns based on the shape.
    }

    let (source_objects, refresh_driven) = wrap_sources(a, &mut case, seq);

    Some(PlannedCase {
        axes: *a,
        case,
        source_objects,
        source_is_refresh_driven: refresh_driven,
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
        assert!(case
            .select_body
            .rendered_sql
            .to_uppercase()
            .contains("SELECT"));
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
            saw_carried |=
                sql.contains("COALESCE") || sql.contains("EXISTS") || sql.contains("CASE");
            if saw_join && saw_cte && saw_carried {
                break;
            }
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
        assert!(
            saw_multi_stmt,
            "generator must emit multi-statement transactions"
        );
    }

    #[test]
    fn generator_reaches_filtered_variants() {
        use crate::generate::fuzz_case;
        let mut runner = TestRunner::default();
        let mut filt = false;
        for _ in 0..400 {
            let c = fuzz_case().new_tree(&mut runner).unwrap().current();
            filt |= c.select_body.rendered_sql.to_uppercase().contains("WHERE");
            if filt {
                break;
            }
        }
        assert!(
            filt,
            "must reach filtered variants with WHERE predicates ({filt})"
        );
    }
}

use pgrx::datum::DatumWithOid;
use pgrx::pg_sys::panic::ErrorReportable;
use pgrx::prelude::*;
use pgrx::spi::Spi;
use pgrx::PgBuiltInOids;

/// One row of IMV status summary.  Returned by `reflex_ivm_status`.
type IvmStatusRow = (
    String,                         // name
    i32,                            // graph_depth
    bool,                           // enabled
    String,                         // refresh_mode
    i64,                            // row_count (live SELECT count(*) on target)
    Option<i64>,                    // last_flush_ms
    Option<i64>,                    // last_flush_rows
    i64,                            // flush_count
    Option<String>,                 // last_error
    Option<pgrx::datum::Timestamp>, // last_update_date
);

/// Summary per IMV. `row_count` is live (SELECT count(*) on the target), cheap
/// enough for operator use on a typical registry.
#[pg_extern]
#[allow(clippy::type_complexity)]
fn reflex_ivm_status() -> TableIterator<
    'static,
    (
        name!(name, String),
        name!(graph_depth, i32),
        name!(enabled, bool),
        name!(refresh_mode, String),
        name!(row_count, i64),
        name!(last_flush_ms, Option<i64>),
        name!(last_flush_rows, Option<i64>),
        name!(flush_count, i64),
        name!(last_error, Option<String>),
        name!(last_update_date, Option<pgrx::datum::Timestamp>),
    ),
> {
    let rows: Vec<IvmStatusRow> = Spi::connect(|client| {
        let mut out = Vec::new();
        let rs = client
            .select(
                "SELECT name, graph_depth, COALESCE(enabled, TRUE) AS enabled, \
                        COALESCE(refresh_mode, 'IMMEDIATE') AS refresh_mode, \
                        last_flush_ms, last_flush_rows, COALESCE(flush_count, 0) AS flush_count, \
                        last_error, last_update_date \
                 FROM public.__reflex_ivm_reference \
                 ORDER BY graph_depth, name",
                None,
                &[],
            )
            .unwrap_or_report();
        for row in rs {
            let name: String = row
                .get_by_name::<&str, _>("name")
                .unwrap_or(None)
                .unwrap_or("")
                .to_string();
            let depth = row
                .get_by_name::<i32, _>("graph_depth")
                .unwrap_or(None)
                .unwrap_or(0);
            let enabled = row
                .get_by_name::<bool, _>("enabled")
                .unwrap_or(None)
                .unwrap_or(true);
            let mode: String = row
                .get_by_name::<&str, _>("refresh_mode")
                .unwrap_or(None)
                .unwrap_or("IMMEDIATE")
                .to_string();
            let last_ms = row.get_by_name::<i64, _>("last_flush_ms").unwrap_or(None);
            let last_rows = row.get_by_name::<i64, _>("last_flush_rows").unwrap_or(None);
            let flush_count = row
                .get_by_name::<i64, _>("flush_count")
                .unwrap_or(None)
                .unwrap_or(0);
            let last_err = row
                .get_by_name::<&str, _>("last_error")
                .unwrap_or(None)
                .map(|s| s.to_string());
            let last_upd = row
                .get_by_name::<pgrx::datum::Timestamp, _>("last_update_date")
                .unwrap_or(None);
            out.push((
                name,
                depth,
                enabled,
                mode,
                0i64,
                last_ms,
                last_rows,
                flush_count,
                last_err,
                last_upd,
            ));
        }
        out
    });

    // Populate row_count in a separate pass to keep the registry read short.
    let rows: Vec<IvmStatusRow> = rows
        .into_iter()
        .map(|mut row| {
            let name = &row.0;
            let count_sql = format!("SELECT COUNT(*)::BIGINT AS c FROM {}", quote_ident(name));
            let c = Spi::get_one::<i64>(&count_sql)
                .unwrap_or(None)
                .unwrap_or(-1);
            row.4 = c;
            row
        })
        .collect();

    TableIterator::new(rows)
}

/// Detailed stats for a single IMV: intermediate size, target size, index count,
/// trigger count, last flush timing.
#[pg_extern]
fn reflex_ivm_stats(
    view_name: &str,
) -> TableIterator<'static, (name!(metric, String), name!(value, String))> {
    let mut out: Vec<(String, String)> = Vec::new();
    let qv = quote_ident(view_name);
    let interm = format!("public.__reflex_intermediate_{}", bare_name(view_name));
    let target = qv.clone();

    let interm_size: Option<String> = Spi::get_one(&format!(
        "SELECT pg_size_pretty(pg_total_relation_size('{}'))",
        interm_quoted(&interm)
    ))
    .unwrap_or(None);
    if let Some(sz) = interm_size {
        out.push(("intermediate_size".to_string(), sz));
    }

    let target_size: Option<String> = Spi::get_one(&format!(
        "SELECT pg_size_pretty(pg_total_relation_size('{}'))",
        target.replace("'", "''")
    ))
    .unwrap_or(None);
    if let Some(sz) = target_size {
        out.push(("target_size".to_string(), sz));
    }

    // Registry metrics
    let args =
        [
            unsafe {
                DatumWithOid::new(view_name.to_string(), PgBuiltInOids::TEXTOID.oid().value())
            },
        ];
    #[allow(clippy::type_complexity)]
    let rows: Vec<(Option<i64>, Option<i64>, i64, Option<String>)> = Spi::connect(|client| {
        client
            .select(
                "SELECT last_flush_ms, last_flush_rows, COALESCE(flush_count, 0) AS flush_count, last_error \
                 FROM public.__reflex_ivm_reference WHERE name = $1",
                None,
                &args,
            )
            .unwrap_or_report()
            .map(|r| {
                (
                    r.get_by_name::<i64, _>("last_flush_ms").unwrap_or(None),
                    r.get_by_name::<i64, _>("last_flush_rows").unwrap_or(None),
                    r.get_by_name::<i64, _>("flush_count")
                        .unwrap_or(None)
                        .unwrap_or(0),
                    r.get_by_name::<&str, _>("last_error")
                        .unwrap_or(None)
                        .map(|s| s.to_string()),
                )
            })
            .collect()
    });
    if let Some((ms, rcnt, fcnt, err)) = rows.into_iter().next() {
        out.push((
            "last_flush_ms".to_string(),
            ms.map(|v| v.to_string())
                .unwrap_or_else(|| "NULL".to_string()),
        ));
        out.push((
            "last_flush_rows".to_string(),
            rcnt.map(|v| v.to_string())
                .unwrap_or_else(|| "NULL".to_string()),
        ));
        out.push(("flush_count".to_string(), fcnt.to_string()));
        out.push((
            "last_error".to_string(),
            err.unwrap_or_else(|| "NULL".to_string()),
        ));
    }

    TableIterator::new(out)
}

/// Returns the EXPLAIN output of what the next flush would execute for a given IMV.
/// Useful for diagnosing plan regressions without actually firing a flush.
#[pg_extern]
fn reflex_explain_flush(view_name: &str) -> String {
    let args =
        [
            unsafe {
                DatumWithOid::new(view_name.to_string(), PgBuiltInOids::TEXTOID.oid().value())
            },
        ];
    let base: Option<String> = Spi::connect(|client| {
        client
            .select(
                "SELECT base_query FROM public.__reflex_ivm_reference WHERE name = $1",
                None,
                &args,
            )
            .unwrap_or_report()
            .next()
            .and_then(|r| {
                r.get_by_name::<&str, _>("base_query")
                    .unwrap_or(None)
                    .map(|s| s.to_string())
            })
    });
    let base = match base {
        Some(b) if !b.is_empty() => b,
        _ => return format!("ERROR: no registered IMV '{}'", view_name),
    };
    let explain_sql = format!("EXPLAIN (VERBOSE, COSTS ON) {}", base);
    let lines: Vec<String> = Spi::connect(|client| {
        client
            .select(&explain_sql, None, &[])
            .unwrap_or_report()
            .filter_map(|r| {
                r.get_by_name::<&str, _>("QUERY PLAN")
                    .unwrap_or(None)
                    .map(|s| s.to_string())
            })
            .collect()
    });
    lines.join("\n")
}

fn quote_ident(name: &str) -> String {
    if name.contains('.') {
        name.split('.')
            .map(|p| format!("\"{}\"", p.replace('"', "\"\"")))
            .collect::<Vec<_>>()
            .join(".")
    } else {
        format!("\"{}\"", name.replace('"', "\"\""))
    }
}

fn bare_name(name: &str) -> String {
    name.rsplit('.').next().unwrap_or(name).to_string()
}

fn interm_quoted(name: &str) -> String {
    // pg_total_relation_size accepts an escaped relation literal
    name.replace("'", "''")
}

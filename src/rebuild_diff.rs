//! Rebuilding an IMV target without announcing "everything changed" to the
//! IMVs that read it: a locked keyed (or whole-row) DELETE / UPDATE / INSERT
//! diff written through the root, so dependents' statement triggers receive
//! exactly the rows that changed.

use pgrx::datum::DatumWithOid;
use pgrx::pg_sys::panic::ErrorReportable;
use pgrx::prelude::*;

use crate::query_decomposer::quote_identifier;
use crate::sql_writer::identifier::substitute_identifier_ci;

pub(crate) enum RebuildScope {
    Whole,
    Leaf {
        leaf_qual: String,
        constraint: String,
        partition_columns: Vec<String>,
    },
}

fn text_arg(value: &str) -> DatumWithOid<'static> {
    unsafe { DatumWithOid::new(value.to_string(), PgBuiltInOids::TEXTOID.oid().value()) }
}

fn select_bool(client: &pgrx::spi::SpiClient<'_>, sql: &str, arg: &str) -> bool {
    client
        .select(sql, None, &[text_arg(arg)])
        .unwrap_or_report()
        .first()
        .get_one::<bool>()
        .unwrap_or(None)
        .unwrap_or(false)
}

fn select_texts(client: &pgrx::spi::SpiClient<'_>, sql: &str, arg: &str) -> Vec<String> {
    client
        .select(sql, None, &[text_arg(arg)])
        .unwrap_or_report()
        .filter_map(|row| row.get::<&str>(1).ok().flatten().map(str::to_string))
        .collect()
}

/// Whether a write to the target reaches anything: an enabled, non-internal
/// trigger that fires under the current `session_replication_role`.
pub(crate) fn target_propagates(client: &pgrx::spi::SpiClient<'_>, view_name: &str) -> bool {
    select_bool(
        client,
        "SELECT EXISTS (SELECT 1 FROM pg_trigger t \
           WHERE t.tgrelid = to_regclass($1) AND NOT t.tgisinternal \
             AND (t.tgenabled = 'A' \
                  OR (t.tgenabled = 'O' AND current_setting('session_replication_role') IN ('origin', 'local')) \
                  OR (t.tgenabled = 'R' AND current_setting('session_replication_role') = 'replica')))",
        &quote_identifier(view_name),
    )
}

/// Columns of the narrowest valid, unique, non-partial, plain-column index on the
/// target root, in index order. Empty when there is none.
pub(crate) fn key_columns(client: &pgrx::spi::SpiClient<'_>, view_name: &str) -> Vec<String> {
    select_texts(
        client,
        "WITH idx AS ( \
           SELECT i.indexrelid, i.indkey, i.indnkeyatts \
           FROM pg_index i \
           WHERE i.indrelid = to_regclass($1) AND i.indisunique \
             AND i.indisvalid AND i.indisready \
             AND i.indpred IS NULL AND i.indexprs IS NULL \
           ORDER BY i.indnkeyatts, i.indexrelid LIMIT 1) \
         SELECT a.attname::text FROM idx \
         CROSS JOIN LATERAL unnest(idx.indkey[0:idx.indnkeyatts - 1]) WITH ORDINALITY k(attnum, ord) \
         JOIN pg_attribute a ON a.attrelid = to_regclass($1) AND a.attnum = k.attnum \
         ORDER BY k.ord",
        &quote_identifier(view_name),
    )
}

fn column_names(client: &pgrx::spi::SpiClient<'_>, relation: &str) -> Vec<String> {
    select_texts(
        client,
        "SELECT attname::text FROM pg_attribute \
         WHERE attrelid = to_regclass($1) AND attnum > 0 AND NOT attisdropped ORDER BY attnum",
        relation,
    )
}

fn quoted(col: &str) -> String {
    format!("\"{}\"", col.replace('"', "\"\""))
}

fn qualified_on(alias: &str, constraint: &str, partition_columns: &[String]) -> String {
    partition_columns
        .iter()
        .fold(constraint.to_string(), |acc, col| {
            substitute_identifier_ci(&acc, col, &format!("{alias}.{}", quoted(col)))
        })
}

/// Bring the target (or one leaf of it) to the rows of `rebuild_sql`.
pub(crate) fn rebuild_target_rows(
    client: &mut pgrx::spi::SpiClient<'_>,
    view_name: &str,
    rebuild_sql: &str,
    scope: &RebuildScope,
) {
    let root = quote_identifier(view_name);
    let run = |client: &mut pgrx::spi::SpiClient<'_>, sql: &str| {
        client.update(sql, None, &[]).unwrap_or_report();
    };
    client
        .update(
            "SELECT pg_advisory_xact_lock(hashtext($1), hashtext(reverse($1)))",
            None,
            &[text_arg(view_name)],
        )
        .unwrap_or_report();
    run(client, &format!("LOCK TABLE {root} IN EXCLUSIVE MODE"));
    let caller_float_digits = client
        .select("SELECT current_setting('extra_float_digits')", None, &[])
        .unwrap_or_report()
        .first()
        .get_one::<&str>()
        .unwrap_or(None)
        .unwrap_or("1")
        .to_string();
    run(client, "SELECT set_config('extra_float_digits', '3', true)");

    let staged = client
        .select("SELECT '__reflex_rb_' || substr(md5(random()::text || clock_timestamp()::text), 1, 12)", None, &[])
        .unwrap_or_report()
        .first()
        .get_one::<String>()
        .unwrap_or(None)
        .expect("temp name");
    let (old_rows, scope_filter, scope_on_t) = match scope {
        RebuildScope::Whole => (root.clone(), String::new(), "TRUE".to_string()),
        RebuildScope::Leaf {
            leaf_qual,
            constraint,
            partition_columns,
        } => (
            leaf_qual.clone(),
            format!(" WHERE ({constraint})"),
            format!("({})", qualified_on("t", constraint, partition_columns)),
        ),
    };
    run(
        client,
        &format!(
            "CREATE TEMP TABLE {staged} ON COMMIT DROP AS \
             SELECT * FROM ({rebuild_sql}) __reflex_q{scope_filter}"
        ),
    );

    let target_cols = column_names(client, &root);
    let staged_cols = column_names(client, &format!("pg_temp.{staged}"));
    let keys = key_columns(client, view_name);
    if !keys.is_empty() && target_cols == staged_cols {
        reject_duplicate_keys(client, view_name, &staged, &keys);
        apply_keyed_diff(
            client,
            &root,
            &old_rows,
            &staged,
            &target_cols,
            &keys,
            &scope_on_t,
        );
    } else {
        apply_whole_row_diff(client, &root, &old_rows, &staged, &scope_on_t);
    }

    run(client, &format!("DROP TABLE pg_temp.{staged}"));
    client
        .update(
            "SELECT set_config('extra_float_digits', $1, true)",
            None,
            &[text_arg(&caller_float_digits)],
        )
        .unwrap_or_report();
}

/// A rebuild yielding a key twice means the IMV is already inconsistent: the
/// keyed UPDATE would apply an arbitrary copy, so fail loudly instead.
fn reject_duplicate_keys(
    client: &pgrx::spi::SpiClient<'_>,
    view_name: &str,
    staged: &str,
    keys: &[String],
) {
    let key_list = keys
        .iter()
        .map(|k| quoted(k))
        .collect::<Vec<_>>()
        .join(", ");
    let has_duplicate = !client
        .select(
            &format!(
                "SELECT 1 FROM pg_temp.{staged} GROUP BY {key_list} HAVING count(*) > 1 LIMIT 1"
            ),
            None,
            &[],
        )
        .unwrap_or_report()
        .is_empty();
    if has_duplicate {
        pgrx::error!(
            "pg_reflex: rebuild of '{view_name}' yields duplicate keys ({key_list}); refusing to diff"
        );
    }
}

fn apply_keyed_diff(
    client: &mut pgrx::spi::SpiClient<'_>,
    root: &str,
    old_rows: &str,
    staged: &str,
    columns: &[String],
    keys: &[String],
    scope_on_t: &str,
) {
    let key_match = |a: &str, b: &str| {
        keys.iter()
            .map(|k| format!("{a}.{q} = {b}.{q}", q = quoted(k)))
            .collect::<Vec<_>>()
            .join(" AND ")
    };
    let others: Vec<&String> = columns.iter().filter(|c| !keys.contains(c)).collect();
    let row_of = |alias: &str| {
        format!(
            "ROW({})::text",
            others
                .iter()
                .map(|c| format!("{alias}.{}", quoted(c)))
                .collect::<Vec<_>>()
                .join(", ")
        )
    };
    let stmts = [
        format!(
            "DELETE FROM {root} t WHERE {scope_on_t} AND NOT EXISTS \
             (SELECT 1 FROM pg_temp.{staged} n WHERE {})",
            key_match("n", "t")
        ),
        if others.is_empty() {
            String::new()
        } else {
            format!(
                "UPDATE {root} t SET {} FROM pg_temp.{staged} n \
                 WHERE {} AND {scope_on_t} AND {} IS DISTINCT FROM {}",
                others
                    .iter()
                    .map(|c| format!("{q} = n.{q}", q = quoted(c)))
                    .collect::<Vec<_>>()
                    .join(", "),
                key_match("n", "t"),
                row_of("t"),
                row_of("n")
            )
        },
        format!(
            "INSERT INTO {root} ({cols}) SELECT {ncols} FROM pg_temp.{staged} n \
             WHERE NOT EXISTS (SELECT 1 FROM {old_rows} t WHERE {})",
            key_match("t", "n"),
            cols = columns
                .iter()
                .map(|c| quoted(c))
                .collect::<Vec<_>>()
                .join(", "),
            ncols = columns
                .iter()
                .map(|c| format!("n.{}", quoted(c)))
                .collect::<Vec<_>>()
                .join(", ")
        ),
    ];
    for stmt in stmts.iter().filter(|s| !s.is_empty()) {
        client.update(stmt, None, &[]).unwrap_or_report();
    }
}

fn apply_whole_row_diff(
    client: &mut pgrx::spi::SpiClient<'_>,
    root: &str,
    old_rows: &str,
    staged: &str,
    scope_on_t: &str,
) {
    let numbered = |rel: &str, alias: &str| {
        format!(
            "SELECT {alias}.tableoid AS toid, {alias}.ctid AS tid, ROW({alias}.*)::text AS k, \
                    row_number() OVER (PARTITION BY ROW({alias}.*)::text) AS rn FROM {rel} {alias}"
        )
    };
    let numbered_staged = format!(
        "SELECT y AS r, ROW(y.*)::text AS k, row_number() OVER (PARTITION BY ROW(y.*)::text) AS rn \
         FROM pg_temp.{staged} y"
    );
    let delete = format!(
        "DELETE FROM {root} t USING ( \
           SELECT o.toid, o.tid FROM ({old}) o \
           WHERE NOT EXISTS (SELECT 1 FROM ({numbered_staged}) n WHERE n.k = o.k AND n.rn = o.rn)) d \
         WHERE t.tableoid = d.toid AND t.ctid = d.tid AND {scope_on_t}",
        old = numbered(old_rows, "x")
    );
    let insert = format!(
        "INSERT INTO {root} SELECT (n.r).* FROM ({numbered_staged}) n \
         WHERE NOT EXISTS (SELECT 1 FROM ({old}) o WHERE o.k = n.k AND o.rn = n.rn)",
        old = numbered(old_rows, "x")
    );
    client.update(&delete, None, &[]).unwrap_or_report();
    client.update(&insert, None, &[]).unwrap_or_report();
}

/// SQL entry point for trigger-side full refreshes: diff when the target has
/// dependents, otherwise DELETE + INSERT (safe inside a trigger).
#[pg_extern]
fn reflex_rebuild_target_rows(view_name: &str, rebuild_sql: &str) -> String {
    Spi::connect_mut(|client| {
        if target_propagates(client, view_name) {
            rebuild_target_rows(client, view_name, rebuild_sql, &RebuildScope::Whole);
            "DIFFED".to_string()
        } else {
            let root = quote_identifier(view_name);
            client
                .update(&format!("DELETE FROM {root}"), None, &[])
                .unwrap_or_report();
            client
                .update(&format!("INSERT INTO {root} {rebuild_sql}"), None, &[])
                .unwrap_or_report();
            "REPLACED".to_string()
        }
    })
}

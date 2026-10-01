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

pub(crate) struct KeyColumn {
    name: String,
    nullable: bool,
}

/// Columns of the narrowest valid, unique, non-partial, plain-column index on the
/// target root that admits one row per key, in index order. Empty when there is
/// none. A NULLS DISTINCT index over a nullable column admits several NULL-key
/// rows, so it qualifies only as NULLS NOT DISTINCT or over NOT NULL columns.
/// NULL keys are matched by wrapping them in one-element arrays, which cannot
/// tell a NULL array from an empty one: an index over a nullable array column
/// is not a key.
pub(crate) fn key_columns(client: &pgrx::spi::SpiClient<'_>, view_name: &str) -> Vec<KeyColumn> {
    client
        .select(
            "WITH idx AS ( \
               SELECT i.indexrelid, i.indkey, i.indnkeyatts \
               FROM pg_index i \
               WHERE i.indrelid = to_regclass($1) AND i.indisunique \
                 AND i.indisvalid AND i.indisready \
                 AND i.indpred IS NULL AND i.indexprs IS NULL \
                 AND (i.indnullsnotdistinct OR NOT EXISTS ( \
                       SELECT 1 FROM unnest(i.indkey[0:i.indnkeyatts - 1]) k(attnum) \
                       JOIN pg_attribute na ON na.attrelid = i.indrelid AND na.attnum = k.attnum \
                       WHERE NOT na.attnotnull)) \
                 AND NOT EXISTS ( \
                       SELECT 1 FROM unnest(i.indkey[0:i.indnkeyatts - 1]) k(attnum) \
                       JOIN pg_attribute na ON na.attrelid = i.indrelid AND na.attnum = k.attnum \
                       JOIN pg_type ty ON ty.oid = na.atttypid \
                       WHERE NOT na.attnotnull AND ty.typcategory = 'A') \
               ORDER BY i.indnkeyatts, i.indexrelid LIMIT 1) \
             SELECT a.attname::text, NOT a.attnotnull FROM idx \
             CROSS JOIN LATERAL unnest(idx.indkey[0:idx.indnkeyatts - 1]) WITH ORDINALITY k(attnum, ord) \
             JOIN pg_attribute a ON a.attrelid = to_regclass($1) AND a.attnum = k.attnum \
             ORDER BY k.ord",
            None,
            &[text_arg(&quote_identifier(view_name))],
        )
        .unwrap_or_report()
        .filter_map(|row| {
            Some(KeyColumn {
                name: row.get::<String>(1).ok().flatten()?,
                nullable: row.get::<bool>(2).ok().flatten()?,
            })
        })
        .collect()
}

/// A keyed UPDATE rewrites rows one by one, so two rows swapping values on a
/// second unique (or exclusion) index trip its immediate check mid-statement.
fn has_second_unique_index(client: &pgrx::spi::SpiClient<'_>, view_name: &str) -> bool {
    select_bool(
        client,
        "SELECT EXISTS (SELECT 1 FROM pg_index i \
           WHERE i.indrelid IN (SELECT relid FROM pg_partition_tree(to_regclass($1)) \
                                UNION SELECT to_regclass($1)) \
             AND (i.indisunique OR i.indisexclusion) \
           GROUP BY i.indrelid HAVING count(*) > 1)",
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
    let caller_float_digits = set_local(client, "extra_float_digits", "3");

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
    if !keys.is_empty() && target_cols == staged_cols && !has_second_unique_index(client, view_name)
    {
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
    set_local(client, "extra_float_digits", &caller_float_digits);
}

/// Set a GUC for the rest of the transaction, returning its previous value.
fn set_local(client: &mut pgrx::spi::SpiClient<'_>, name: &str, value: &str) -> String {
    let previous = client
        .select("SELECT current_setting($1)", None, &[text_arg(name)])
        .unwrap_or_report()
        .first()
        .get_one::<String>()
        .unwrap_or(None)
        .unwrap_or_default();
    client
        .update(
            "SELECT set_config($1, $2, true)",
            None,
            &[text_arg(name), text_arg(value)],
        )
        .unwrap_or_report();
    previous
}

/// A rebuild yielding a key twice means the IMV is already inconsistent: the
/// keyed UPDATE would apply an arbitrary copy, so fail loudly instead.
fn reject_duplicate_keys(
    client: &pgrx::spi::SpiClient<'_>,
    view_name: &str,
    staged: &str,
    keys: &[KeyColumn],
) {
    let key_list = keys
        .iter()
        .map(|k| quoted(&k.name))
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

/// Rows whose key holds no NULL are matched with plain `=` (hash / merge
/// joinable, index usable). `=` never matches a NULL, so keys with a NULL get
/// a second match restricted to the NULL-key rows of both sides, comparing
/// nullable columns as one-element arrays: array equality treats NULL elements
/// as equal and stays hash / merge joinable, unlike IS NOT DISTINCT FROM.
fn apply_keyed_diff(
    client: &mut pgrx::spi::SpiClient<'_>,
    root: &str,
    old_rows: &str,
    staged: &str,
    columns: &[String],
    keys: &[KeyColumn],
    scope_on_t: &str,
) {
    let has_nullable_key = keys.iter().any(|k| k.nullable);
    let strict_match = |a: &str, b: &str| {
        keys.iter()
            .map(|k| format!("{a}.{q} = {b}.{q}", q = quoted(&k.name)))
            .collect::<Vec<_>>()
            .join(" AND ")
    };
    let null_key_match = |a: &str, b: &str| {
        let any_null = |alias: &str| {
            keys.iter()
                .filter(|k| k.nullable)
                .map(|k| format!("{alias}.{} IS NULL", quoted(&k.name)))
                .collect::<Vec<_>>()
                .join(" OR ")
        };
        let equal = keys
            .iter()
            .map(|k| {
                let q = quoted(&k.name);
                if k.nullable {
                    format!("ARRAY[{a}.{q}] = ARRAY[{b}.{q}]")
                } else {
                    format!("{a}.{q} = {b}.{q}")
                }
            })
            .collect::<Vec<_>>()
            .join(" AND ");
        format!("({}) AND ({}) AND {equal}", any_null(a), any_null(b))
    };
    let unmatched_in = |rel: &str, alias: &str, other: &str| {
        let strict = format!(
            "NOT EXISTS (SELECT 1 FROM {rel} {alias} WHERE {})",
            strict_match(alias, other)
        );
        if has_nullable_key {
            format!(
                "{strict} AND NOT EXISTS (SELECT 1 FROM {rel} {alias} WHERE {})",
                null_key_match(alias, other)
            )
        } else {
            strict
        }
    };
    let others: Vec<&String> = columns
        .iter()
        .filter(|c| !keys.iter().any(|k| &k.name == *c))
        .collect();
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
    let update_where = |key_match: String| {
        format!(
            "UPDATE {root} t SET {} FROM pg_temp.{staged} n \
             WHERE {key_match} AND {scope_on_t} AND {} IS DISTINCT FROM {}",
            others
                .iter()
                .map(|c| format!("{q} = n.{q}", q = quoted(c)))
                .collect::<Vec<_>>()
                .join(", "),
            row_of("t"),
            row_of("n")
        )
    };
    let mut stmts = vec![format!(
        "DELETE FROM {root} t WHERE {scope_on_t} AND {}",
        unmatched_in(&format!("pg_temp.{staged}"), "n", "t")
    )];
    if !others.is_empty() {
        stmts.push(update_where(strict_match("n", "t")));
        if has_nullable_key {
            stmts.push(update_where(null_key_match("n", "t")));
        }
    }
    stmts.push(format!(
        "INSERT INTO {root} ({cols}) SELECT {ncols} FROM pg_temp.{staged} n WHERE {}",
        unmatched_in(old_rows, "t", "n"),
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
    ));
    // The NULL-key match uses no index, and estimates of how many NULL-key
    // rows there are are often stale: a nested loop would be quadratic.
    let caller_nestloop = has_nullable_key.then(|| set_local(client, "enable_nestloop", "off"));
    for stmt in &stmts {
        client.update(stmt, None, &[]).unwrap_or_report();
    }
    if let Some(caller_nestloop) = caller_nestloop {
        set_local(client, "enable_nestloop", &caller_nestloop);
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

const LEAF_ROWS_SQL: &str = "\
    SELECT COALESCE(sum(CASE WHEN c.reltuples >= 0 THEN c.reltuples \
                             ELSE pg_relation_size(c.oid)::float8 \
                                  / GREATEST(COALESCE((SELECT sum(s.avg_width) FROM pg_stats s \
                                       WHERE s.schemaname = n.nspname AND s.tablename = c.relname), 0) + 24, 32) \
                        END), 0)::float8 \
    FROM pg_partition_tree($1::oid::regclass) t JOIN pg_class c ON c.oid = t.relid \
    JOIN pg_namespace n ON n.oid = c.relnamespace WHERE t.isleaf";

/// For an aggregate IMV the rebuild reads the anchor SOURCE, not the (much
/// smaller) target: map the target or intermediate child to the source child
/// of the same name (`<view>_<src child>` / `__reflex_intermediate_<view>_<src child>`).
fn anchor_source_child_oid(
    client: &pgrx::spi::SpiClient<'_>,
    view_name: &str,
    child: pg_sys::Oid,
) -> Option<pg_sys::Oid> {
    lookup_anchor_source_child(client, view_name, child).unwrap_or_else(|error| {
        debug1!(
            "pg_reflex: sizing {view_name} child by itself, anchor-source lookup failed: {error}"
        );
        None
    })
}

/// `Ok(None)` is the legitimate "size the child itself" (not an aggregate, or
/// no matching source child); `Err` is a failed lookup.
fn lookup_anchor_source_child(
    client: &pgrx::spi::SpiClient<'_>,
    view_name: &str,
    child: pg_sys::Oid,
) -> Result<Option<pg_sys::Oid>, String> {
    use crate::partition::{list_partition_tree, resolve_anchor_source};
    use crate::query_decomposer::split_qualified_name;

    let registry = client
        .select(
            "SELECT depends_on, partition_columns[1] AS part_col, COALESCE(end_query, '') <> '' AS is_aggregate \
             FROM public.__reflex_ivm_reference WHERE name = $1",
            Some(1),
            &[text_arg(view_name)],
        )
        .map_err(|e| e.to_string())?
        .next();
    let Some(registry) = registry else {
        return Ok(None);
    };
    let is_aggregate = registry
        .get_by_name::<bool, _>("is_aggregate")
        .map_err(|e| e.to_string())?
        .unwrap_or(false);
    if !is_aggregate {
        return Ok(None);
    }
    let part_col = registry
        .get_by_name::<String, _>("part_col")
        .map_err(|e| e.to_string())?;
    let depends_on = registry
        .get_by_name::<Vec<String>, _>("depends_on")
        .map_err(|e| e.to_string())?;
    let (Some(part_col), Some(depends_on)) = (part_col, depends_on) else {
        return Ok(None);
    };
    let anchor = resolve_anchor_source(client, &part_col, &depends_on)?;
    let child_name = client
        .select(
            "SELECT relname::text FROM pg_class WHERE oid = $1",
            Some(1),
            &[unsafe { DatumWithOid::new(child, PgBuiltInOids::OIDOID.oid().value()) }],
        )
        .map_err(|e| e.to_string())?
        .next()
        .and_then(|row| row.get::<String>(1).ok().flatten());
    let Some(child_name) = child_name else {
        return Ok(None);
    };
    let (_, view_bare) = split_qualified_name(view_name);
    let Some(source_child) = child_name
        .strip_prefix(&format!("__reflex_intermediate_{view_bare}_"))
        .or_else(|| child_name.strip_prefix(&format!("{view_bare}_")))
    else {
        return Ok(None);
    };
    Ok(list_partition_tree(client, &anchor)
        .into_iter()
        .find(|node| node.bare_name == source_child)
        .map(|node| pg_sys::Oid::from(node.oid)))
}

/// Estimated rows a rebuild of `child` (a target or intermediate partition
/// subtree) reads: summed over its leaves, `reltuples` when analyzed, else
/// file size / row width. Aggregate IMVs are sized by the matching anchor
/// source child.
#[pg_extern]
fn __reflex_rebuild_cost_rows(view_name: &str, child: pg_sys::Oid) -> f64 {
    Spi::connect(|client| {
        let sized = anchor_source_child_oid(client, view_name, child).unwrap_or(child);
        client
            .select(
                LEAF_ROWS_SQL,
                Some(1),
                &[unsafe { DatumWithOid::new(sized, PgBuiltInOids::OIDOID.oid().value()) }],
            )
            .unwrap_or_report()
            .first()
            .get_one::<f64>()
            .unwrap_or(None)
            .unwrap_or(0.0)
    })
}

/// SQL face of `target_propagates`, read by the volume-triggered dispatch.
#[pg_extern]
fn __reflex_target_propagates(view_name: &str) -> bool {
    Spi::connect(|client| target_propagates(client, view_name))
}

//! PostgreSQL fires the statement-level INSERT / UPDATE / DELETE triggers of the
//! table a statement names and of no other table of its partition tree: a
//! statement naming a partition does not reach the triggers of the partitioned
//! source, and a statement naming the source does not reach those of a
//! partition. Each partition of a partitioned source therefore runs the source's
//! three write functions from triggers of its own, so a row is captured once
//! whichever table the statement names.
//!
//! Every table of the tree also carries a BEFORE trigger, the guard. It records
//! in a transaction-local setting that the source was written, which the
//! partition flush needs to know. On a partition it refuses the two statements
//! the write functions cannot serve:
//! - on a partition whose columns are ordered differently than the source's: the
//!   transition tables of its triggers have the partition's column order, and
//!   the write functions read them as rows of the source;
//! - on a table detached from the source, which keeps its triggers until the
//!   detaching transaction commits: its rows are no longer rows of the source,
//!   and the COMMIT-time flush removes from the IMVs the rows it holds then.

use pgrx::prelude::*;

use crate::query_decomposer::{transition_new_table_name, transition_old_table_name};
use crate::schema_builder::{
    member_register_ddl, partition_trigger_name, partitioned_source_trigger_name,
    source_trigger_function_name, source_written_setting,
};
use crate::source_truncate::{is_registered_imv, text_arg};

const GUARD: &str = "guard";
const REORDERED: &str = "reordered";

struct WriteTrigger {
    name: String,
    function: String,
    event: &'static str,
    referencing: String,
}

fn write_triggers_of(source: &str) -> Vec<WriteTrigger> {
    let new_rows = format!("NEW TABLE AS \"{}\"", transition_new_table_name(source));
    let old_rows = format!("OLD TABLE AS \"{}\"", transition_old_table_name(source));
    [
        ("ins", "INSERT", new_rows.clone()),
        ("del", "DELETE", old_rows.clone()),
        ("upd", "UPDATE", format!("{new_rows} {old_rows}")),
    ]
    .into_iter()
    .map(|(op, event, referencing)| WriteTrigger {
        name: partition_trigger_name(source, op),
        function: source_trigger_function_name(source, op),
        event,
        referencing,
    })
    .collect()
}

/// Functions of `source` that triggers on its partitions run.
fn partition_trigger_functions(source: &str) -> Vec<String> {
    write_triggers_of(source)
        .into_iter()
        .map(|trigger| trigger.function)
        .chain([source_trigger_function_name(source, GUARD)])
        .collect()
}

struct TreeTable {
    qualified: String,
    is_source: bool,
    reordered: bool,
    triggers: Vec<String>,
}

fn function_exists(
    client: &pgrx::spi::SpiClient<'_>,
    function: &str,
) -> Result<bool, pgrx::spi::Error> {
    Ok(client
        .select(
            "SELECT 1 FROM pg_proc f JOIN pg_namespace n ON n.oid = f.pronamespace \
             WHERE n.nspname = 'public' AND f.proname = $1",
            None,
            &[text_arg(function)],
        )?
        .next()
        .is_some())
}

/// The tables of the partition tree of `source`, the source first; empty when
/// the source is not a partitioned table.
fn partition_tree_of(
    client: &pgrx::spi::SpiClient<'_>,
    source: &str,
) -> Result<Vec<TreeTable>, pgrx::spi::Error> {
    const COLUMN_ORDER: &str = "ARRAY(SELECT a.attname::text FROM pg_attribute a \
         WHERE a.attrelid = {relation} AND a.attnum > 0 AND NOT a.attisdropped \
         ORDER BY a.attnum)";
    let query = format!(
        "SELECT c.oid::regclass::text AS qualified, \
                c.oid = to_regclass($1) AS is_source, \
                {of_table} IS DISTINCT FROM {of_source} AS reordered, \
                ARRAY(SELECT t.tgname::text FROM pg_trigger t \
                      WHERE t.tgrelid = c.oid AND NOT t.tgisinternal) AS triggers \
         FROM pg_partition_tree(to_regclass($1)) p \
         JOIN pg_class c ON c.oid = p.relid \
         WHERE c.relkind IN ('r', 'p') \
           AND (SELECT s.relkind = 'p' FROM pg_class s WHERE s.oid = to_regclass($1)) \
         ORDER BY p.level, c.oid",
        of_table = COLUMN_ORDER.replace("{relation}", "c.oid"),
        of_source = COLUMN_ORDER.replace("{relation}", "to_regclass($1)"),
    );
    Ok(client
        .select(&query, None, &[text_arg(source)])?
        .filter_map(|row| {
            Some(TreeTable {
                qualified: row
                    .get_by_name::<&str, _>("qualified")
                    .ok()
                    .flatten()?
                    .to_string(),
                is_source: row.get_by_name::<bool, _>("is_source").ok().flatten()?,
                reordered: row.get_by_name::<bool, _>("reordered").ok().flatten()?,
                triggers: row
                    .get_by_name::<Vec<String>, _>("triggers")
                    .ok()
                    .flatten()
                    .unwrap_or_default(),
            })
        })
        .collect())
}

/// (table, trigger) of every trigger running one of `functions`; with
/// `only_outside_the_tree`, those on tables that are neither `source` nor in its
/// partition tree.
fn triggers_running(
    client: &pgrx::spi::SpiClient<'_>,
    source: &str,
    functions: &[String],
    only_outside_the_tree: bool,
) -> Result<Vec<(String, String)>, pgrx::spi::Error> {
    let functions = unsafe {
        pgrx::datum::DatumWithOid::new(
            functions.to_vec(),
            PgBuiltInOids::TEXTARRAYOID.oid().value(),
        )
    };
    let only_outside_the_tree = unsafe {
        pgrx::datum::DatumWithOid::new(only_outside_the_tree, PgBuiltInOids::BOOLOID.oid().value())
    };
    Ok(client
        .select(
            "SELECT t.tgrelid::regclass::text AS qualified, t.tgname::text AS trigger \
             FROM pg_trigger t \
             JOIN pg_proc f ON f.oid = t.tgfoid \
             JOIN pg_namespace n ON n.oid = f.pronamespace \
             WHERE n.nspname = 'public' AND f.proname = ANY($2) AND NOT t.tgisinternal \
               AND (NOT $3 OR (t.tgrelid IS DISTINCT FROM to_regclass($1) \
                               AND t.tgrelid NOT IN \
                                   (SELECT relid FROM pg_partition_tree(to_regclass($1)))))",
            None,
            &[text_arg(source), functions, only_outside_the_tree],
        )?
        .filter_map(|row| {
            Some((
                row.get_by_name::<&str, _>("qualified")
                    .ok()
                    .flatten()?
                    .to_string(),
                row.get_by_name::<&str, _>("trigger")
                    .ok()
                    .flatten()?
                    .to_string(),
            ))
        })
        .collect())
}

fn drop_triggers(
    client: &mut pgrx::spi::SpiClient<'_>,
    triggers: &[(String, String)],
) -> Result<(), pgrx::spi::Error> {
    for (qualified, trigger) in triggers {
        client.update(
            &format!("DROP TRIGGER IF EXISTS \"{trigger}\" ON {qualified}"),
            None,
            &[],
        )?;
    }
    Ok(())
}

fn guard_function_ddl(source: &str) -> String {
    let function = source_trigger_function_name(source, GUARD);
    let on_partition = partition_trigger_name(source, GUARD);
    let on_source = partitioned_source_trigger_name(source);
    let written = source_written_setting(source);
    let source = source.replace('\'', "''");
    format!(
        "CREATE OR REPLACE FUNCTION public.{function}() RETURNS TRIGGER AS $fn$ \
         BEGIN \
           IF TG_NAME = '{on_partition}' THEN \
             IF NOT EXISTS (SELECT 1 FROM pg_partition_ancestors(TG_RELID) a \
                            JOIN pg_trigger t ON t.tgrelid = a.relid::oid \
                            WHERE t.tgname = '{on_source}') THEN \
               RAISE EXCEPTION 'pg_reflex: % on % cannot maintain the incremental views of {source}: the table is no longer a partition of {source} and carries its triggers until the transaction that detached it commits', TG_OP, TG_RELID::regclass \
                 USING HINT = 'Run the statement after that transaction has committed. If it has, run SELECT reflex_rebuild_triggers(''{source}'').'; \
             END IF; \
             IF TG_NARGS > 0 THEN \
               RAISE EXCEPTION 'pg_reflex: % on partition % cannot maintain the incremental views of {source}: the columns of this partition are in a different order than those of {source}', TG_OP, TG_RELID::regclass \
                 USING HINT = 'Address the statement to {source}: it routes the rows to the partition and maintains the views.'; \
             END IF; \
           END IF; \
           PERFORM set_config('{written}', '1', true); \
           RETURN NULL; \
         END; $fn$ LANGUAGE plpgsql"
    )
}

/// Gives every partition of `source` (the spelling its trigger functions were
/// built from) triggers running the write functions of the source, and every
/// table of the tree its guard; removes them from tables that left the tree. A
/// source that is not a partitioned table is left as its create made it.
pub(crate) fn sync_partition_write_triggers(
    client: &mut pgrx::spi::SpiClient<'_>,
    source: &str,
) -> Result<(), pgrx::spi::Error> {
    if source.starts_with('<') || is_registered_imv(client, source)? {
        return Ok(());
    }
    let write_triggers = write_triggers_of(source);
    for trigger in &write_triggers {
        if !function_exists(client, &trigger.function)? {
            return Ok(());
        }
    }
    let tree = partition_tree_of(client, source)?;
    let guard_function = source_trigger_function_name(source, GUARD);
    if !tree.is_empty() {
        client.update(&guard_function_ddl(source), None, &[])?;
        client.update(&member_register_ddl(&guard_function), None, &[])?;
    }
    let guard_on_source = partitioned_source_trigger_name(source);
    let guard_on_partition = partition_trigger_name(source, GUARD);

    for table in &tree {
        let carries = |trigger: &str| table.triggers.iter().any(|name| name == trigger);
        let qualified = &table.qualified;
        let (guard, argument) = match (table.is_source, table.reordered) {
            (true, _) => (&guard_on_source, String::new()),
            (false, false) => (&guard_on_partition, String::new()),
            (false, true) => (&guard_on_partition, format!("'{REORDERED}'")),
        };
        if !carries(guard) {
            client.update(
                &format!(
                    "CREATE TRIGGER \"{guard}\" BEFORE INSERT OR UPDATE OR DELETE ON {qualified} \
                     FOR EACH STATEMENT EXECUTE FUNCTION public.{guard_function}({argument})"
                ),
                None,
                &[],
            )?;
            if table.reordered && !table.is_source {
                warning!(
                    "pg_reflex: the columns of partition {qualified} are in a different order \
                     than those of {source}; INSERT / UPDATE / DELETE addressed to {qualified} \
                     are refused, write through {source}"
                );
            }
        }
        if table.is_source || table.reordered {
            continue;
        }
        for trigger in write_triggers.iter().filter(|t| !carries(&t.name)) {
            client.update(
                &format!(
                    "CREATE TRIGGER \"{}\" AFTER {} ON {qualified} REFERENCING {} \
                     FOR EACH STATEMENT EXECUTE FUNCTION public.{}()",
                    trigger.name, trigger.event, trigger.referencing, trigger.function
                ),
                None,
                &[],
            )?;
        }
    }

    let left_the_tree =
        triggers_running(client, source, &partition_trigger_functions(source), true)?;
    drop_triggers(client, &left_the_tree)
}

/// Removes the triggers `sync_partition_write_triggers` installed for `source`,
/// and the guard function, so the write functions of the source can be dropped.
pub(crate) fn drop_partition_write_triggers(
    client: &mut pgrx::spi::SpiClient<'_>,
    source: &str,
) -> Result<(), pgrx::spi::Error> {
    let installed = triggers_running(client, source, &partition_trigger_functions(source), false)?;
    drop_triggers(client, &installed)?;
    let guard_function = format!("public.{}", source_trigger_function_name(source, GUARD));
    crate::drop_ivm::detach_function_from_extension(client, &guard_function);
    client.update(
        &format!("DROP FUNCTION IF EXISTS {guard_function}()"),
        None,
        &[],
    )?;
    Ok(())
}

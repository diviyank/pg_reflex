//! PostgreSQL fires a statement-level TRUNCATE trigger for the relations the
//! statement truncates and for nothing above them: `TRUNCATE partition` does not
//! reach the trigger of the partitioned source, while `TRUNCATE source` fires the
//! trigger of the source and of every partition. Each relation of a partitioned
//! source therefore carries the source's truncate function twice: BEFORE, to
//! announce itself, and AFTER, where the last announced relation rebuilds the IMVs.

use pgrx::datum::DatumWithOid;
use pgrx::prelude::*;

use crate::query_decomposer::named_relation;
use crate::schema_builder::{
    source_trigger_function_name, source_trigger_name, truncate_announce_trigger_name,
};

pub(crate) fn text_arg(value: &str) -> DatumWithOid<'static> {
    unsafe { DatumWithOid::new(value.to_string(), PgBuiltInOids::TEXTOID.oid().value()) }
}

/// An IMV read as a source is maintained through its root only: pg_reflex
/// truncates and swaps its partitions itself and cascades those rebuilds in code.
pub(crate) fn is_registered_imv(
    client: &pgrx::spi::SpiClient<'_>,
    source: &str,
) -> Result<bool, pgrx::spi::Error> {
    let relation = named_relation(source);
    Ok(client
        .select("SELECT name FROM public.__reflex_ivm_reference", None, &[])?
        .filter_map(|row| row.get_by_name::<&str, _>("name").ok().flatten())
        .any(|name| named_relation(name) == relation))
}

/// Whether a trigger fires in an ordinary session.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum TriggerState {
    Enabled,
    Disabled,
    Missing,
}

impl TriggerState {
    /// From `pg_trigger.tgenabled`; `None` when the table has no such trigger.
    pub(crate) fn of(tgenabled: Option<&str>) -> Self {
        match tgenabled {
            None => TriggerState::Missing,
            Some("O") | Some("A") => TriggerState::Enabled,
            Some(_) => TriggerState::Disabled,
        }
    }
}

/// A trigger pg_reflex keeps on a table of the partition tree of a source.
pub(crate) struct TreeTrigger {
    pub(crate) table: String,
    pub(crate) on_source: bool,
    pub(crate) name: String,
    pub(crate) state: TriggerState,
    pub(crate) create_ddl: String,
}

pub(crate) fn function_exists(
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

/// True when the tables of the partition tree of `source` carry its truncate
/// triggers: an IMV read as a source and a source without truncate function do not.
fn truncate_triggers_are_kept(
    client: &pgrx::spi::SpiClient<'_>,
    source: &str,
) -> Result<bool, pgrx::spi::Error> {
    Ok(!source.starts_with('<')
        && !is_registered_imv(client, source)?
        && function_exists(client, &source_trigger_function_name(source, "trunc"))?)
}

/// The truncate triggers every table of the partition tree of `source` (the
/// spelling its trigger function was built from) is to carry, with their state.
/// Empty for a source that is neither a partition nor a partitioned table: it
/// keeps its single AFTER trigger.
pub(crate) fn truncate_triggers_of_tree(
    client: &pgrx::spi::SpiClient<'_>,
    source: &str,
) -> Result<Vec<TreeTrigger>, pgrx::spi::Error> {
    if !truncate_triggers_are_kept(client, source)? {
        return Ok(Vec::new());
    }
    let function = source_trigger_function_name(source, "trunc");
    let announce = truncate_announce_trigger_name(source);
    let rebuild = source_trigger_name(source, "trunc");
    let mut triggers = Vec::new();
    let tree = client.select(
        "SELECT c.oid::regclass::text AS qualified, \
                c.oid = to_regclass($1) AS is_source, \
                (SELECT t.tgenabled::text FROM pg_trigger t \
                 WHERE t.tgrelid = c.oid AND t.tgname = $2) AS announces, \
                (SELECT t.tgenabled::text FROM pg_trigger t \
                 WHERE t.tgrelid = c.oid AND t.tgname = $3) AS rebuilds \
         FROM pg_partition_tree(to_regclass($1)) p \
         JOIN pg_class c ON c.oid = p.relid \
         WHERE c.relkind IN ('r', 'p') \
         ORDER BY p.level, c.oid",
        None,
        &[text_arg(source), text_arg(&announce), text_arg(&rebuild)],
    )?;
    for row in tree {
        let Some(qualified) = row.get_by_name::<&str, _>("qualified").ok().flatten() else {
            continue;
        };
        let on_source = row
            .get_by_name::<bool, _>("is_source")
            .ok()
            .flatten()
            .unwrap_or(false);
        for (column, trigger, when) in [
            ("announces", &announce, "BEFORE"),
            ("rebuilds", &rebuild, "AFTER"),
        ] {
            triggers.push(TreeTrigger {
                table: qualified.to_string(),
                on_source,
                name: trigger.clone(),
                state: TriggerState::of(row.get_by_name::<&str, _>(column).ok().flatten()),
                create_ddl: format!(
                    "CREATE TRIGGER \"{trigger}\" {when} TRUNCATE ON {qualified} \
                     FOR EACH STATEMENT EXECUTE FUNCTION public.{function}()"
                ),
            });
        }
    }
    Ok(triggers)
}

/// Installs the truncate triggers of `source` (the spelling its trigger function
/// was built from) on every table of its partition tree that lacks them, and
/// removes them from tables that left the tree. A source that is not a
/// partition or a partitioned table keeps its single AFTER trigger.
pub(crate) fn sync_partition_truncate_triggers(
    client: &mut pgrx::spi::SpiClient<'_>,
    source: &str,
) -> Result<(), pgrx::spi::Error> {
    if !truncate_triggers_are_kept(client, source)? {
        return Ok(());
    }
    let function = source_trigger_function_name(source, "trunc");
    for trigger in truncate_triggers_of_tree(client, source)? {
        if trigger.state == TriggerState::Missing {
            client.update(&trigger.create_ddl, None, &[])?;
        }
    }

    let left_the_tree: Vec<(String, String)> = client
        .select(
            "SELECT t.tgrelid::regclass::text AS qualified, t.tgname::text AS trigger \
             FROM pg_trigger t \
             JOIN pg_proc p ON p.oid = t.tgfoid \
             JOIN pg_namespace n ON n.oid = p.pronamespace \
             WHERE n.nspname = 'public' AND p.proname = $2 AND NOT t.tgisinternal \
               AND t.tgrelid IS DISTINCT FROM to_regclass($1) \
               AND t.tgrelid NOT IN (SELECT relid FROM pg_partition_tree(to_regclass($1)))",
            None,
            &[text_arg(source), text_arg(&function)],
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
        .collect();
    for (qualified, trigger) in &left_the_tree {
        client.update(
            &format!("DROP TRIGGER IF EXISTS \"{trigger}\" ON {qualified}"),
            None,
            &[],
        )?;
    }
    Ok(())
}

/// Removes every trigger running the truncate function of `source`, on the source
/// and on each table that carries it, so the function can be dropped.
pub(crate) fn drop_truncate_triggers(
    client: &mut pgrx::spi::SpiClient<'_>,
    source: &str,
) -> Result<(), pgrx::spi::Error> {
    let function = source_trigger_function_name(source, "trunc");
    let installed: Vec<(String, String)> = client
        .select(
            "SELECT t.tgrelid::regclass::text AS qualified, t.tgname::text AS trigger \
             FROM pg_trigger t \
             JOIN pg_proc p ON p.oid = t.tgfoid \
             JOIN pg_namespace n ON n.oid = p.pronamespace \
             WHERE n.nspname = 'public' AND p.proname = $1 AND NOT t.tgisinternal",
            None,
            &[text_arg(&function)],
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
        .collect();
    for (qualified, trigger) in &installed {
        client.update(
            &format!("DROP TRIGGER IF EXISTS \"{trigger}\" ON {qualified}"),
            None,
            &[],
        )?;
    }
    Ok(())
}

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

fn text_arg(value: &str) -> DatumWithOid<'static> {
    unsafe { DatumWithOid::new(value.to_string(), PgBuiltInOids::TEXTOID.oid().value()) }
}

/// An IMV read as a source is maintained through its root only: pg_reflex
/// truncates and swaps its partitions itself and cascades those rebuilds in code.
fn is_registered_imv(
    client: &pgrx::spi::SpiClient<'_>,
    source: &str,
) -> Result<bool, pgrx::spi::Error> {
    let relation = named_relation(source);
    Ok(client
        .select("SELECT name FROM public.__reflex_ivm_reference", None, &[])?
        .filter_map(|row| row.get_by_name::<&str, _>("name").ok().flatten())
        .any(|name| named_relation(name) == relation))
}

struct TruncatedRelation {
    qualified: String,
    announces: bool,
    rebuilds: bool,
}

/// Installs the truncate triggers of `source` (the spelling its trigger function
/// was built from) on every table of its partition tree that lacks them, and
/// removes them from tables that left the tree. A source that is not a
/// partition or a partitioned table keeps its single AFTER trigger.
pub(crate) fn sync_partition_truncate_triggers(
    client: &mut pgrx::spi::SpiClient<'_>,
    source: &str,
) -> Result<(), pgrx::spi::Error> {
    if source.starts_with('<') || is_registered_imv(client, source)? {
        return Ok(());
    }
    let function = source_trigger_function_name(source, "trunc");
    let announce = truncate_announce_trigger_name(source);
    let rebuild = source_trigger_name(source, "trunc");

    let function_exists = client
        .select(
            "SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace \
             WHERE n.nspname = 'public' AND p.proname = $1",
            None,
            &[text_arg(&function)],
        )?
        .next()
        .is_some();
    if !function_exists {
        return Ok(());
    }

    let tree: Vec<TruncatedRelation> = client
        .select(
            "SELECT c.oid::regclass::text AS qualified, \
                    EXISTS (SELECT 1 FROM pg_trigger t \
                            WHERE t.tgrelid = c.oid AND t.tgname = $2) AS announces, \
                    EXISTS (SELECT 1 FROM pg_trigger t \
                            WHERE t.tgrelid = c.oid AND t.tgname = $3) AS rebuilds \
             FROM pg_partition_tree(to_regclass($1)) p \
             JOIN pg_class c ON c.oid = p.relid \
             WHERE c.relkind IN ('r', 'p') \
             ORDER BY p.level, c.oid",
            None,
            &[text_arg(source), text_arg(&announce), text_arg(&rebuild)],
        )?
        .filter_map(|row| {
            Some(TruncatedRelation {
                qualified: row.get_by_name::<&str, _>("qualified").ok().flatten()?.to_string(),
                announces: row.get_by_name::<bool, _>("announces").ok().flatten()?,
                rebuilds: row.get_by_name::<bool, _>("rebuilds").ok().flatten()?,
            })
        })
        .collect();
    for relation in &tree {
        for (present, trigger, when) in [
            (relation.announces, &announce, "BEFORE"),
            (relation.rebuilds, &rebuild, "AFTER"),
        ] {
            if !present {
                client.update(
                    &format!(
                        "CREATE TRIGGER \"{trigger}\" {when} TRUNCATE ON {} \
                         FOR EACH STATEMENT EXECUTE FUNCTION public.{function}()",
                        relation.qualified
                    ),
                    None,
                    &[],
                )?;
            }
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
                row.get_by_name::<&str, _>("qualified").ok().flatten()?.to_string(),
                row.get_by_name::<&str, _>("trigger").ok().flatten()?.to_string(),
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
                row.get_by_name::<&str, _>("qualified").ok().flatten()?.to_string(),
                row.get_by_name::<&str, _>("trigger").ok().flatten()?.to_string(),
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

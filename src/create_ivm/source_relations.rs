//! Relations a definition reads that the statement triggers of pg_reflex cannot
//! follow: a partition, which a write addressed to its partitioned table does not
//! report to; a table of an inheritance hierarchy, which a write addressed to
//! another table of the hierarchy does not report to; and a table whose triggers
//! would carry the names of another table's. A source the definition ignores gets
//! no trigger and is not judged.

use std::ops::ControlFlow;

use pgrx::datum::DatumWithOid;
use pgrx::prelude::*;
use sqlparser::ast::{ObjectName, Query, Statement, Visit, Visitor};

use super::{canonicalize_analysis_sources, partitioned_source_whose_name_needs_quotes};
use crate::query_decomposer::split_qualified_name;
use crate::schema_builder::source_trigger_suffix;
use crate::sql_analyzer::{folded, SqlAnalysis};

/// The CTE names each query level being visited declares, innermost last, and the
/// relations read so far. A CTE body that reads the table its own CTE is named
/// after is taken for the CTE here; it is judged again as the query of its sub-IMV.
#[derive(Default)]
struct RelationsRead {
    cte_names: Vec<Vec<String>>,
    written: Vec<String>,
}

impl Visitor for RelationsRead {
    type Break = ();

    fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<()> {
        let declared = query.with.iter().flat_map(|with| &with.cte_tables);
        self.cte_names
            .push(declared.map(|cte| folded(&cte.alias.name)).collect());
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, _: &Query) -> ControlFlow<()> {
        self.cte_names.pop();
        ControlFlow::Continue(())
    }

    fn pre_visit_relation(&mut self, relation: &ObjectName) -> ControlFlow<()> {
        let names_a_cte = match relation.0.as_slice() {
            [only] => only.as_ident().map(folded).is_some_and(|name| {
                self.cte_names
                    .iter()
                    .flatten()
                    .any(|declared| *declared == name)
            }),
            _ => false,
        };
        let written = relation.to_string();
        if !names_a_cte && !self.written.contains(&written) {
            self.written.push(written);
        }
        ControlFlow::Continue(())
    }
}

/// Every relation `statements` read, in FROM, JOIN, a CTE or a subquery, as written.
fn relations_read(statements: &[Statement]) -> Vec<String> {
    let mut relations = RelationsRead::default();
    for statement in statements {
        let _ = statement.visit(&mut relations);
    }
    relations.written
}

fn text_argument<'a>(value: &str) -> DatumWithOid<'a> {
    unsafe { DatumWithOid::new(value.to_string(), PgBuiltInOids::TEXTOID.oid().value()) }
}

/// Why the first relation of `relations` that is a partition cannot be a source.
/// PostgreSQL fires the statement triggers of the table a statement names, so the
/// triggers of a partition stay silent for a write addressed to an ancestor. The
/// rows of the partition are spelled out as a query over the root where that query
/// is one pg_reflex creates as written and maintains: not under a root whose name
/// needs quotes, and not below a table partitioned on several columns, whose bound
/// is a predicate pg_reflex does not maintain.
fn partition_read_as_a_source(relations: &[String]) -> Option<String> {
    let (relation, root, bound, widest_partition_key) = Spi::connect(|client| {
        relations.iter().find_map(|relation| {
            let row = client
                .select(
                    "SELECT r.oid::regclass::text AS root, \
                            pg_get_partition_constraintdef(c.oid) AS bound, \
                            (SELECT max(p.partnatts)::int \
                               FROM pg_partition_ancestors(c.oid) a \
                               JOIN pg_partitioned_table p ON p.partrelid = a.relid \
                              WHERE a.relid <> c.oid) AS widest_partition_key \
                     FROM pg_class c JOIN pg_class r ON r.oid = pg_partition_root(c.oid) \
                     WHERE c.oid = to_regclass($1) AND c.relispartition",
                    Some(1),
                    &[text_argument(relation)],
                )
                .ok()?
                .next()?;
            Some((
                relation.clone(),
                row.get_by_name::<String, _>("root").ok().flatten()?,
                row.get_by_name::<String, _>("bound").ok().flatten(),
                row.get_by_name::<i32, _>("widest_partition_key")
                    .ok()
                    .flatten(),
            ))
        })
    })?;
    let root_as_registered = registered_spellings(vec![root.clone()]);
    let remedy = if partitioned_source_whose_name_needs_quotes(&root_as_registered).is_some() {
        format!(
            "give the partitioned table and its schema lower-case names that are not keywords \
             (ALTER TABLE ... RENAME TO ...), then read it in place of its partition, with a \
             WHERE on the partition key. The partitioned table is {root}"
        )
    } else if widest_partition_key != Some(1) {
        format!(
            "read the partitioned table itself, FROM {root}, and pass partition_by => \
             ARRAY[]::text[] (7th argument of create_reflex_ivm): pg_reflex does not maintain \
             a WHERE that selects one partition of a table partitioned on several columns"
        )
    } else {
        let rows_of_the_partition = match bound {
            Some(bound) => format!("FROM {root} WHERE {bound}"),
            None => format!("FROM {root}"),
        };
        format!(
            "read the partitioned table and keep the rows of the partition with a WHERE on \
             the partition key: {rows_of_the_partition}"
        )
    };
    Some(format!(
        "a source is a partition, and a write addressed to its partitioned table would not \
         reach the IMV. Fix: {remedy}. The partition is {relation}, of {root}."
    ))
}

/// Why the first relation of `relations` that inherits from a table, or that tables
/// inherit from, cannot be a source: a write addressed to the parent changes the
/// rows of a child, and a read of the parent returns them, without the statement
/// triggers of the other table firing.
fn inheritance_member_read_as_a_source(relations: &[String]) -> Option<String> {
    Spi::connect(|client| {
        relations.iter().find_map(|relation| {
            let row = client
                .select(
                    "SELECT (SELECT i.inhparent::regclass::text FROM pg_inherits i \
                              WHERE i.inhrelid = c.oid ORDER BY i.inhseqno LIMIT 1) AS parent, \
                            (SELECT i.inhrelid::regclass::text FROM pg_inherits i \
                              WHERE i.inhparent = c.oid ORDER BY i.inhrelid LIMIT 1) AS child \
                     FROM pg_class c \
                     WHERE c.oid = to_regclass($1) AND NOT c.relispartition AND c.relkind <> 'p'",
                    Some(1),
                    &[text_argument(relation)],
                )
                .ok()?
                .next()?;
            let parent = row.get_by_name::<String, _>("parent").ok().flatten();
            let child = row.get_by_name::<String, _>("child").ok().flatten();
            let remedy = match (parent, child) {
                (Some(parent), _) => format!(
                    "take the source out of the hierarchy, ALTER TABLE {relation} NO INHERIT \
                     {parent}, for each table it inherits from"
                ),
                (None, Some(child)) => format!(
                    "take its children out of the hierarchy, ALTER TABLE {child} NO INHERIT \
                     {relation}, for each table that inherits from it"
                ),
                (None, None) => return None,
            };
            Some(format!(
                "a source belongs to an inheritance hierarchy (INHERITS), and a write addressed \
                 to another table of the hierarchy would not reach the IMV. Fix: {remedy}. The \
                 source is {relation}."
            ))
        })
    })
}

/// The spelling each of `relations` is registered under as a source.
fn registered_spellings(relations: Vec<String>) -> Vec<String> {
    let mut named = SqlAnalysis {
        sources: relations,
        ..Default::default()
    };
    canonicalize_analysis_sources(&mut named);
    named.sources
}

/// What a relation whose trigger names collide is, which decides who can rename what.
enum CollidingRelation {
    /// A table no IMV reads: renaming it breaks nothing.
    FreeTable,
    /// A table an IMV already reads under this name.
    ReadTable,
    /// The target of the IMV of this name, which pg_reflex writes to by name.
    ImvTarget(String),
    /// A table the IMV being created builds for itself and names after itself.
    OwnSubImv,
}

/// The first of `sources` whose triggers and trigger functions would be named like
/// those of another relation, with that relation and what each of the two is: another
/// source of the same definition, or a source of a registered IMV. Creating the
/// triggers of one replaces the functions of the other.
fn source_sharing_trigger_names(
    sources: &[String],
    view_name: &str,
) -> Option<[(String, CollidingRelation); 2]> {
    Spi::connect(|client| {
        let registered: Vec<String> = client
            .select(
                "SELECT DISTINCT unnest(depends_on) AS source FROM public.__reflex_ivm_reference",
                None,
                &[],
            )
            .ok()?
            .filter_map(|row| row.get_by_name::<String, _>("source").ok().flatten())
            .collect();
        let name_one_relation = |one: &str, other: &str| {
            one == other
                || client
                    .select(
                        "SELECT to_regclass($1) = to_regclass($2) AS same",
                        Some(1),
                        &[text_argument(one), text_argument(other)],
                    )
                    .ok()
                    .and_then(|mut rows| rows.next())
                    .and_then(|row| row.get_by_name::<bool, _>("same").ok().flatten())
                    .unwrap_or(false)
        };
        let (source, other) = sources.iter().find_map(|source| {
            let suffix = source_trigger_suffix(source);
            sources
                .iter()
                .chain(&registered)
                .filter(|other| source_trigger_suffix(other) == suffix)
                .find(|other| !name_one_relation(source, other))
                .map(|other| (source.clone(), other.clone()))
        })?;
        let described = |relation: String| {
            let imv = client
                .select(
                    "SELECT r.name::text AS name, COALESCE(r.is_generated_sub_imv, FALSE) AS generated \
                     FROM public.__reflex_ivm_reference r \
                     WHERE to_regclass(r.name) = to_regclass($1)",
                    Some(1),
                    &[text_argument(&relation)],
                )
                .ok()
                .and_then(|mut rows| rows.next())
                .and_then(|row| {
                    Some((
                        row.get_by_name::<String, _>("name").ok().flatten()?,
                        row.get_by_name::<bool, _>("generated").ok().flatten()?,
                    ))
                });
            let kind = match imv {
                Some((name, true)) if name.starts_with(&format!("{view_name}__")) => {
                    CollidingRelation::OwnSubImv
                }
                Some((name, _)) => CollidingRelation::ImvTarget(name),
                None if registered.contains(&relation) => CollidingRelation::ReadTable,
                None => CollidingRelation::FreeTable,
            };
            (relation, kind)
        };
        Some([described(source), described(other)])
    })
}

/// The change that ends a trigger-name collision without breaking an IMV: a table
/// no IMV reads is renamed; an IMV target is created again under another name,
/// since pg_reflex writes to it by name; a table IMVs read keeps its name while
/// they exist.
fn trigger_name_collision_remedy(colliding: &[(String, CollidingRelation); 2]) -> String {
    let of_kind =
        |wanted: fn(&CollidingRelation) -> bool| colliding.iter().find(|(_, kind)| wanted(kind));
    if of_kind(|kind| matches!(kind, CollidingRelation::OwnSubImv)).is_some() {
        return "give this IMV another name: the table it builds for itself is named after it"
            .to_string();
    }
    if let Some((table, _)) = of_kind(|kind| matches!(kind, CollidingRelation::FreeTable)) {
        return format!(
            "rename the table {table} (ALTER TABLE ... RENAME TO ...), which no IMV reads yet, \
             then create the IMV over the new name"
        );
    }
    if let Some((_, CollidingRelation::ImvTarget(imv))) =
        of_kind(|kind| matches!(kind, CollidingRelation::ImvTarget(_)))
    {
        return format!(
            "drop the IMV {imv} (SELECT drop_reflex_ivm('{imv}')) and create it again under \
             another name, then create this IMV over that name"
        );
    }
    format!(
        "drop the IMVs that read {table}, rename that table, and create them again over the \
         new name",
        table = colliding[0].0
    )
}

fn is_ignored(written: &str, registered: &str, ignored_sources: &[String]) -> bool {
    ignored_sources.iter().any(|ignored| {
        [written, registered].iter().any(|name| {
            ignored.as_str() == *name || ignored.as_str() == split_qualified_name(name).1
        })
    })
}

/// Why pg_reflex cannot follow one of the relations `statements` read, in FROM, JOIN,
/// a CTE or a subquery, and does not ignore, with the change that makes it a source
/// pg_reflex follows.
pub(super) fn source_relation_not_followed(
    statements: &[Statement],
    view_name: &str,
    ignored_sources: &[String],
) -> Option<String> {
    let written = relations_read(statements);
    let registered = registered_spellings(written.clone());
    let (written, registered): (Vec<String>, Vec<String>) = written
        .into_iter()
        .zip(registered)
        .filter(|(written, registered)| !is_ignored(written, registered, ignored_sources))
        .unzip();
    if let Some(reason) = partition_read_as_a_source(&written) {
        return Some(reason);
    }
    if let Some(reason) = inheritance_member_read_as_a_source(&written) {
        return Some(reason);
    }
    let colliding = source_sharing_trigger_names(&registered, view_name)?;
    Some(format!(
        "two relations would get the same pg_reflex trigger names, and the IMVs of one would \
         go unmaintained. Fix: {remedy}. The relations are {one} and {other}: a '.' and a '_' \
         are written alike in the names of the triggers and of their functions.",
        remedy = trigger_name_collision_remedy(&colliding),
        one = colliding[0].0,
        other = colliding[1].0,
    ))
}

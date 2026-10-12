//! Relations a definition reads that the statement triggers of pg_reflex cannot
//! follow: a table whose triggers would carry the names of another table's.

use std::ops::ControlFlow;

use pgrx::datum::DatumWithOid;
use pgrx::prelude::*;
use sqlparser::ast::{ObjectName, Query, Statement, Visit, Visitor};

use super::canonicalize_analysis_sources;
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

/// The spelling each of `relations` is registered under as a source.
fn registered_spellings(relations: Vec<String>) -> Vec<String> {
    let mut named = SqlAnalysis {
        sources: relations,
        ..Default::default()
    };
    canonicalize_analysis_sources(&mut named);
    named.sources
}

/// The first of `sources` whose triggers and trigger functions would be named like
/// those of another relation, with that relation: another source of the same
/// definition, or a source of a registered IMV. Creating the triggers of one replaces
/// the functions of the other. A source no IMV reads yet is named first: it is the
/// one to rename.
fn source_sharing_trigger_names(sources: &[String]) -> Option<(String, String)> {
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
        let read_by_an_imv = |source: &&String| registered.contains(source);
        let new_sources = sources.iter().filter(|source| !read_by_an_imv(source));
        let known_sources = sources.iter().filter(read_by_an_imv);
        new_sources.chain(known_sources).find_map(|source| {
            let suffix = source_trigger_suffix(source);
            sources
                .iter()
                .chain(&registered)
                .filter(|other| source_trigger_suffix(other) == suffix)
                .find(|other| !name_one_relation(source, other))
                .map(|other| (source.clone(), other.clone()))
        })
    })
}

/// Why pg_reflex cannot follow one of the relations `statements` read, in FROM, JOIN,
/// a CTE or a subquery, with the change that makes it a source pg_reflex follows.
pub(super) fn source_relation_not_followed(statements: &[Statement]) -> Option<String> {
    let sources = registered_spellings(relations_read(statements));
    let (source, other) = source_sharing_trigger_names(&sources)?;
    Some(format!(
        "the sources {source} and {other} would get the same pg_reflex trigger names, and the \
         IMVs of one would go unmaintained. Fix: rename {source} (ALTER TABLE ... RENAME TO \
         ...), then create the IMV over the new name. A '.' and a '_' are written alike in \
         the names of the triggers and of their functions."
    ))
}

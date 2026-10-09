//! Two ways of naming a source's columns resolve only against the FROM item as the
//! query wrote it, while pg_reflex follows a source by its own column names and
//! evaluates the WHERE clause of a one-source query against its changed rows:
//! a column renamed by an alias column list (`FROM t AS x(a, b)`), and a
//! reference to the whole row in that WHERE clause (`WHERE t IS NOT NULL`).

use std::ops::ControlFlow;

use pgrx::datum::DatumWithOid;
use pgrx::prelude::*;
use sqlparser::ast::{Expr, ObjectNamePart, Select, SetExpr, Statement, TableFactor};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

fn resolved(written: &sqlparser::ast::Ident) -> String {
    match written.quote_style {
        Some(_) => written.value.clone(),
        None => written.value.to_lowercase(),
    }
}

fn source_columns(client: &pgrx::spi::SpiClient<'_>, source: &str) -> Vec<String> {
    client
        .select(
            "SELECT attname::text AS name FROM pg_attribute \
             WHERE attrelid = to_regclass($1) AND attnum > 0 AND NOT attisdropped \
             ORDER BY attnum",
            None,
            &[unsafe {
                DatumWithOid::new(source.to_string(), PgBuiltInOids::TEXTOID.oid().value())
            }],
        )
        .map(|rows| {
            rows.filter_map(|row| row.get_by_name::<String, _>("name").ok().flatten())
                .collect()
        })
        .unwrap_or_default()
}

/// The unqualified names a clause reads, and whether it names a whole row as
/// `relation.*`.
fn names_read<T: sqlparser::ast::Visit>(clause: &T) -> (Vec<String>, bool) {
    let mut names = Vec::new();
    let mut qualified_row = false;
    let _ = sqlparser::ast::visit_expressions(clause, |expr| {
        match expr {
            Expr::Identifier(name) => names.push(resolved(name)),
            Expr::QualifiedWildcard(..) => qualified_row = true,
            _ => {}
        }
        ControlFlow::<()>::Continue(())
    });
    (names, qualified_row)
}

fn top_level_select(statements: &[Statement]) -> Option<&Select> {
    let [Statement::Query(query)] = statements else {
        return None;
    };
    match query.body.as_ref() {
        SetExpr::Select(select) => Some(select),
        _ => None,
    }
}

/// The first column an alias column list of a table in `select`'s FROM clause
/// renames, as `(alias, stored name, given name)`.
fn renamed_source_column(
    client: &pgrx::spi::SpiClient<'_>,
    select: &Select,
) -> Option<(String, String, String)> {
    select
        .from
        .iter()
        .flat_map(|item| {
            std::iter::once(&item.relation).chain(item.joins.iter().map(|join| &join.relation))
        })
        .find_map(|factor| {
            let TableFactor::Table {
                name,
                alias: Some(alias),
                args: None,
                ..
            } = factor
            else {
                return None;
            };
            let stored = source_columns(client, &name.to_string());
            alias
                .columns
                .iter()
                .map(|column| resolved(&column.name))
                .zip(stored)
                .find(|(given, stored)| given != stored)
                .map(|(given, stored)| (resolved(&alias.name), stored, given))
        })
}

/// Why pg_reflex cannot maintain `sql` as written, with the rewrite that it
/// can; `None` when it can. `only_source` is the source of a one-source query.
pub(super) fn unmaintainable_source_row_reference(
    client: &pgrx::spi::SpiClient<'_>,
    sql: &str,
    only_source: Option<&str>,
) -> Option<String> {
    let statements = Parser::parse_sql(&PostgreSqlDialect {}, sql).ok()?;
    let select = top_level_select(&statements)?;
    if let Some((reference, stored, given)) = renamed_source_column(client, select) {
        return Some(format!(
            "the column alias list of '{reference}' renames its column '{stored}' to '{given}', \
             and pg_reflex follows the columns of a source by their own names. Fix: drop the \
             alias list and rename in the select list, {reference}.{stored} AS {given}."
        ));
    }

    let source = only_source?;
    let [only] = select.from.as_slice() else {
        return None;
    };
    let TableFactor::Table {
        name,
        alias,
        args: None,
        ..
    } = &only.relation
    else {
        return None;
    };
    if !only.joins.is_empty() {
        return None;
    }
    let reference = match alias {
        Some(alias) => resolved(&alias.name),
        None => match name.0.last()? {
            ObjectNamePart::Identifier(relation) => resolved(relation),
            _ => return None,
        },
    };
    let stored = source_columns(client, source);
    if stored.is_empty() {
        return None;
    }
    let (filter_names, filter_reads_qualified_row) = names_read(select.selection.as_ref()?);
    let filter_reads_row = filter_reads_qualified_row
        || (filter_names.contains(&reference) && !stored.contains(&reference));
    filter_reads_row.then(|| {
        format!(
            "the WHERE clause reads '{reference}' as a whole row, which pg_reflex cannot evaluate \
             against the changed rows of '{source}'. Fix: test the columns themselves, e.g. \
             {reference}.<column> IS NOT NULL for each column the test needs."
        )
    })
}

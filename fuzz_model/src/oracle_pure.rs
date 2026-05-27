//! Pure, pgrx-free oracle helpers for differential testing.
//! All string builders and comparison logic; the SPI executor stays in the harness.

use crate::model::*;
use crate::render;
use std::sync::atomic::AtomicU64;

pub static CASE_SEQ: AtomicU64 = AtomicU64::new(0);

#[derive(Debug)]
pub enum Outcome {
    Match,
    #[allow(dead_code)]
    Skip(String),
    Bug(String),
}

/// Extract the list of column names from a FuzzCase for a given table.
pub fn cols_of(case: &FuzzCase, table: &str) -> Vec<String> {
    case.tables
        .iter()
        .find(|t| t.name == table)
        .map(|t| t.columns.iter().map(|c| c.name.clone()).collect())
        .unwrap_or_default()
}

/// Build a subquery that returns rows differing between mv and imv using set operations.
/// Used when the case has no float output columns (exact comparison).
pub fn diff_subquery(mv: &str, imv: &str) -> String {
    format!(
        "( (SELECT * FROM {mv} EXCEPT SELECT * FROM {imv}) UNION ALL \
         (SELECT * FROM {imv} EXCEPT SELECT * FROM {mv}) ) d"
    )
}

/// Build the FROM clause that returns rows differing between mv and imv,
/// exact for non-float columns and within a relative epsilon for float columns.
/// Used when the case has any float output column.
///
/// A row counts as differing when it has no counterpart on the other side that
/// agrees on every non-float column (`IS NOT DISTINCT FROM`, so NULL keys and
/// NULL groups match) and is within 1e-9 relative on every float column. This
/// uses correlated `NOT EXISTS` rather than a `FULL JOIN ... ON a.k = b.k`
/// precisely because `=` is not NULL-safe: a NULL group key never satisfies it,
/// so the old FULL-JOIN form reported the (correct) NULL group as two phantom
/// unmatched rows.
///
/// The 1e-9 relative tolerance absorbs the float8 round-off between the IMV's
/// incremental accumulation (running SUM/AVG via deltas) and the MV's full
/// re-aggregation — orders of magnitude below any genuine divergence, which shows
/// up as a wrong row count or a value off by a real term, not a last-ULP wobble.
pub fn float_diff_from_where(mv: &str, imv: &str, _keys: &[String], cols: &[Column]) -> String {
    let match_pred = |aa: &str, bb: &str| -> String {
        cols.iter()
            .map(|c| {
                let n = &c.name;
                if c.ty.is_float() {
                    format!(
                        "(({aa}.\"{n}\" IS NULL AND {bb}.\"{n}\" IS NULL) OR \
                          ({aa}.\"{n}\" IS NOT NULL AND {bb}.\"{n}\" IS NOT NULL AND \
                           abs({aa}.\"{n}\" - {bb}.\"{n}\") <= 1e-9 * \
                           GREATEST(abs({aa}.\"{n}\"), abs({bb}.\"{n}\"), 1)))"
                    )
                } else {
                    format!("{aa}.\"{n}\" IS NOT DISTINCT FROM {bb}.\"{n}\"")
                }
            })
            .collect::<Vec<_>>()
            .join(" AND ")
    };
    let col_list = cols
        .iter()
        .map(|c| format!("\"{}\"", c.name))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "( SELECT {cl} FROM {mv} a WHERE NOT EXISTS (SELECT 1 FROM {imv} b WHERE {mab}) \
          UNION ALL \
          SELECT {cl} FROM {imv} b WHERE NOT EXISTS (SELECT 1 FROM {mv} a WHERE {mba}) ) d",
        cl = col_list,
        mv = mv,
        imv = imv,
        mab = match_pred("a", "b"),
        mba = match_pred("b", "a"),
    )
}

/// Rename all table names in a FuzzCase by appending a suffix.
/// Used to avoid collisions across many test cases running in the same transaction.
pub fn rename_case(case: &FuzzCase, suffix: &str) -> FuzzCase {
    let mut c = case.clone();
    // longest names first to avoid prefix corruption
    let mut names: Vec<String> = case.tables.iter().map(|t| t.name.clone()).collect();
    names.sort_by_key(|n| std::cmp::Reverse(n.len()));
    for old in names {
        let new = format!("{old}{suffix}");
        for t in &mut c.tables {
            if t.name == old {
                t.name = new.clone();
            }
        }
        c.select_body.rendered_sql =
            c.select_body.rendered_sql.replace(&old, &new);
        for txn in &mut c.dml {
            for stmt in &mut txn.statements {
                match stmt {
                    DmlStmt::Insert { table, .. }
                    | DmlStmt::Delete { table, .. }
                    | DmlStmt::Update { table, .. }
                    | DmlStmt::Truncate { table } => {
                        if *table == old {
                            *table = new.clone();
                        }
                    }
                }
            }
        }
    }
    c
}

/// Build a reproducible SQL script that sets up the case schema, applies DML,
/// and creates an IMV. Useful for manual reproduction of test cases.
pub fn repro_sql(case: &FuzzCase) -> String {
    let mut out = String::new();
    for t in &case.tables {
        out.push_str(&render::create_table_sql(t));
        out.push_str(";\n");
    }
    for txn in &case.dml {
        for stmt in &txn.statements {
            let cols = cols_of(case, match stmt {
                DmlStmt::Insert { table, .. }
                | DmlStmt::Delete { table, .. }
                | DmlStmt::Update { table, .. }
                | DmlStmt::Truncate { table } => table,
            });
            out.push_str(&render::dml_sql(stmt, &|_t: &str| cols.clone()));
            out.push_str(";\n");
        }
    }
    out.push_str(&format!(
        "SELECT create_reflex_ivm('imv_repro', $body${}$body$, '{}');\n",
        case.select_body.rendered_sql,
        case.unique_columns.join(",")
    ));
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn float_compare_uses_relative_epsilon_and_null_safe_match() {
        let cols = vec![
            Column { name: "g".into(), ty: ColType::Int, nullable: false },
            Column { name: "s".into(), ty: ColType::Float8, nullable: true },
            Column { name: "t".into(), ty: ColType::Text, nullable: true },
        ];
        let sql = float_diff_from_where("v_mv", "v_imv", &["g".to_string()], &cols);
        assert!(sql.contains("1e-9"), "must use relative epsilon: {sql}");
        // NULL-safe correlated anti-join, not an equi FULL JOIN (finding #4).
        assert!(sql.contains("NOT EXISTS"), "must use NOT EXISTS anti-join: {sql}");
        assert!(!sql.contains("FULL JOIN"), "must not use a non-NULL-safe FULL JOIN: {sql}");
        assert!(sql.contains("IS NOT DISTINCT FROM"), "non-float cols matched NULL-safe: {sql}");
    }
}

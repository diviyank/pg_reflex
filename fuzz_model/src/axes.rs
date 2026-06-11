//! The orthogonal axes whose 2-way interactions defined the creation-bug class.
//! A validity predicate prunes impossible combinations before generation.

use crate::model::{ColType, FuzzCase};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SourceKind {
    Table,
    View,
    MatView,
    CteSubImv,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum QueryShape {
    Passthrough,
    SingleAggregate,
    JoinInner,
    JoinLeft,
    CteDecomposed,
    SetOpUnionAll,
    WindowFn,
    DistinctOn,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RefreshMode {
    Immediate,
    Deferred,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AggFn {
    Sum,
    Count,
    Min,
    Max,
    Avg,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum UniqueCols {
    Absent,
    Provided,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Lifecycle {
    CreateMutateDrop,
    CascadeDrop,
    Partitioned,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceObjectKind {
    Table,
    View,
    MatView,
    SubImv,
}

#[derive(Debug, Clone)]
pub struct SourceObject {
    pub name: String,
    pub kind: SourceObjectKind,
    /// For View/MatView/SubImv: the SQL the object is defined by.
    pub define_sql: Option<String>,
}

#[derive(Debug, Clone)]
pub struct PlannedCase {
    pub axes: Axes,
    pub case: FuzzCase,
    pub source_objects: Vec<SourceObject>,
    /// True when maintenance is REFRESH-driven (matview source): the front-end
    /// must `REFRESH MATERIALIZED VIEW <src>` + `refresh_imv_depending_on(<src>)`
    /// instead of relying on triggers.
    pub source_is_refresh_driven: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Axes {
    pub source: SourceKind,
    pub shape: QueryShape,
    pub refresh: RefreshMode,
    pub agg: Option<AggFn>,
    pub measure_ty: ColType,
    pub unique: UniqueCols,
    pub lifecycle: Lifecycle,
}

impl ColType {
    /// SUM/AVG operands and the numeric MIN/MAX family.
    pub fn is_numeric_family(self) -> bool {
        matches!(
            self,
            ColType::Int | ColType::BigInt | ColType::Numeric | ColType::Float8
        )
    }
    /// Types MIN/MAX accept (ordered types).
    pub fn is_orderable(self) -> bool {
        !matches!(self, ColType::Bool)
    }
}

pub fn is_valid(a: &Axes) -> bool {
    use AggFn::*;
    use QueryShape::*;
    use SourceKind::*;

    // MatView sources are REFRESH-driven; pg_reflex skips triggers on them, so
    // DEFERRED (trigger-staged) maintenance is meaningless.
    if a.source == MatView && a.refresh == RefreshMode::Deferred {
        return false;
    }

    // Aggregate-fn axis presence must match the shape.
    let shape_is_aggregate = matches!(
        a.shape,
        SingleAggregate | JoinInner | JoinLeft | CteDecomposed
    );
    match (shape_is_aggregate, a.agg.is_some()) {
        (true, false) | (false, true) => return false,
        _ => {}
    }

    // Aggregate-fn / measure-type compatibility.
    if let Some(f) = a.agg {
        match f {
            Sum | Avg => {
                if !a.measure_ty.is_numeric_family() {
                    return false;
                }
            }
            Min | Max => {
                if !a.measure_ty.is_orderable() {
                    return false;
                }
            }
            Count => {} // count(*) accepts any type
        }
    }

    // Explicit unique_columns only meaningful where pg_reflex passes a key through.
    if a.unique == UniqueCols::Provided && !matches!(a.shape, Passthrough | JoinInner | JoinLeft) {
        return false;
    }

    // A CTE sub-IMV source only feeds decomposed/join consumers.
    if a.source == CteSubImv && !matches!(a.shape, CteDecomposed | JoinInner | JoinLeft) {
        return false;
    }

    // SetOpUnionAll is a non-aggregate shape (set-op of passthrough legs).
    if a.shape == SetOpUnionAll && a.agg.is_some() {
        return false;
    }

    true
}

fn all_source() -> Vec<SourceKind> {
    // Gate scope: Table sources only. pg_reflex's supported source for an IMV is a
    // base table; View/MatView/CteSubImv as *sources* are out of scope for the
    // in-backend differential gate (a view source is rejected by codegen, and the
    // matview/sub-IMV source paths are not modeled end-to-end). The `SourceKind`
    // enum and `is_valid` retain the full model so the axis can be re-enabled later
    // without reshaping the design. Note: decomposed query SHAPES (CteDecomposed,
    // SetOpUnionAll) still run over Table sources, so decomposition is still covered.
    vec![SourceKind::Table]
}

fn all_shape() -> Vec<QueryShape> {
    use QueryShape::*;
    vec![
        Passthrough,
        SingleAggregate,
        JoinInner,
        JoinLeft,
        CteDecomposed,
        SetOpUnionAll,
        WindowFn,
        DistinctOn,
    ]
}

fn all_refresh() -> Vec<RefreshMode> {
    vec![RefreshMode::Immediate, RefreshMode::Deferred]
}

fn all_agg() -> Vec<Option<AggFn>> {
    use AggFn::*;
    vec![
        None,
        Some(Sum),
        Some(Count),
        Some(Min),
        Some(Max),
        Some(Avg),
    ]
}

fn all_ty() -> Vec<ColType> {
    use ColType::*;
    vec![Int, Numeric, Timestamptz, Date, Text, Bool]
}

fn all_unique() -> Vec<UniqueCols> {
    vec![UniqueCols::Absent, UniqueCols::Provided]
}

fn all_lifecycle() -> Vec<Lifecycle> {
    vec![
        Lifecycle::CreateMutateDrop,
        Lifecycle::CascadeDrop,
        Lifecycle::Partitioned,
    ]
}

/// The full cartesian product, filtered to the legal subspace.
pub fn valid_space() -> Vec<Axes> {
    let mut out = Vec::new();
    for &source in &all_source() {
        for &shape in &all_shape() {
            for &refresh in &all_refresh() {
                for &agg in &all_agg() {
                    for &measure_ty in &all_ty() {
                        for &unique in &all_unique() {
                            for &lifecycle in &all_lifecycle() {
                                let a = Axes {
                                    source,
                                    shape,
                                    refresh,
                                    agg,
                                    measure_ty,
                                    unique,
                                    lifecycle,
                                };
                                if is_valid(&a) {
                                    out.push(a);
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    out
}

/// A 2-way interaction: two (field-index, value-discriminant) pairs.
type Pair = ((u8, u32), (u8, u32));

fn field_values(a: &Axes) -> [(u8, u32); 7] {
    [
        (0, a.source as u32),
        (1, a.shape as u32),
        (2, a.refresh as u32),
        (
            3,
            match a.agg {
                None => 0,
                Some(f) => 1 + f as u32,
            },
        ),
        (4, a.measure_ty as u32),
        (5, a.unique as u32),
        (6, a.lifecycle as u32),
    ]
}

pub fn required_pairs(space: &[Axes]) -> std::collections::HashSet<Pair> {
    let mut set = std::collections::HashSet::new();
    for a in space {
        let fv = field_values(a);
        for i in 0..fv.len() {
            for j in (i + 1)..fv.len() {
                set.insert((fv[i], fv[j]));
            }
        }
    }
    set
}

/// Deterministic greedy all-pairs: repeatedly pick the valid assignment that
/// covers the most still-uncovered pairs (ties broken by first occurrence).
pub fn pairwise(space: &[Axes]) -> Vec<Axes> {
    let need = required_pairs(space);
    let mut covered: std::collections::HashSet<Pair> = std::collections::HashSet::new();
    let mut chosen: Vec<Axes> = Vec::new();
    while covered.len() < need.len() {
        let mut best: Option<(usize, &Axes)> = None;
        for a in space {
            let fv = field_values(a);
            let mut gain = 0usize;
            for i in 0..fv.len() {
                for j in (i + 1)..fv.len() {
                    if !covered.contains(&(fv[i], fv[j])) {
                        gain += 1;
                    }
                }
            }
            if best.is_none_or(|(g, _)| gain > g) {
                best = Some((gain, a));
            }
        }
        let (gain, a) = best.expect("non-empty space");
        if gain == 0 {
            break;
        }
        let fv = field_values(a);
        for i in 0..fv.len() {
            for j in (i + 1)..fv.len() {
                covered.insert((fv[i], fv[j]));
            }
        }
        chosen.push(*a);
    }
    chosen
}

/// Render an axis assignment to a concrete runnable case. `seq` disambiguates
/// object names across cases sharing a database. Returns None only for an
/// assignment `is_valid` admitted but for which no generator template exists
/// yet (kept explicit so coverage gaps are visible, never silent).
pub fn plan_case(a: &Axes, seq: u64) -> Option<PlannedCase> {
    crate::generate::plan_from_axes(a, seq)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::ColType;

    fn axes(
        source: SourceKind,
        shape: QueryShape,
        refresh: RefreshMode,
        agg: Option<AggFn>,
    ) -> Axes {
        Axes {
            source,
            shape,
            refresh,
            agg,
            measure_ty: ColType::Numeric,
            unique: UniqueCols::Absent,
            lifecycle: Lifecycle::CreateMutateDrop,
        }
    }

    #[test]
    fn axes_is_constructible_and_hashable() {
        let a = Axes {
            source: SourceKind::Table,
            shape: QueryShape::SingleAggregate,
            refresh: RefreshMode::Immediate,
            agg: Some(AggFn::Sum),
            measure_ty: ColType::Numeric,
            unique: UniqueCols::Absent,
            lifecycle: Lifecycle::CreateMutateDrop,
        };
        let mut set = std::collections::HashSet::new();
        set.insert(a);
        assert!(set.contains(&a));
    }

    #[test]
    fn matview_source_cannot_be_deferred() {
        let a = axes(
            SourceKind::MatView,
            QueryShape::Passthrough,
            RefreshMode::Deferred,
            None,
        );
        assert!(!is_valid(&a));
        let b = axes(
            SourceKind::MatView,
            QueryShape::Passthrough,
            RefreshMode::Immediate,
            None,
        );
        assert!(is_valid(&b));
    }

    #[test]
    fn passthrough_has_no_aggregate_fn() {
        let with_agg = axes(
            SourceKind::Table,
            QueryShape::Passthrough,
            RefreshMode::Immediate,
            Some(AggFn::Sum),
        );
        assert!(!is_valid(&with_agg));
        let no_agg = axes(
            SourceKind::Table,
            QueryShape::Passthrough,
            RefreshMode::Immediate,
            None,
        );
        assert!(is_valid(&no_agg));
    }

    #[test]
    fn aggregate_shapes_require_an_agg_fn() {
        let missing = axes(
            SourceKind::Table,
            QueryShape::SingleAggregate,
            RefreshMode::Immediate,
            None,
        );
        assert!(!is_valid(&missing));
    }

    #[test]
    fn minmax_over_bool_is_invalid() {
        let mut a = axes(
            SourceKind::Table,
            QueryShape::SingleAggregate,
            RefreshMode::Immediate,
            Some(AggFn::Min),
        );
        a.measure_ty = ColType::Bool;
        assert!(!is_valid(&a));
    }

    #[test]
    fn sum_avg_require_numeric_family() {
        let mut a = axes(
            SourceKind::Table,
            QueryShape::SingleAggregate,
            RefreshMode::Immediate,
            Some(AggFn::Sum),
        );
        a.measure_ty = ColType::Text;
        assert!(!is_valid(&a));
    }

    #[test]
    fn provided_unique_cols_only_for_passthrough_or_join() {
        let mut agg = axes(
            SourceKind::Table,
            QueryShape::SingleAggregate,
            RefreshMode::Immediate,
            Some(AggFn::Sum),
        );
        agg.unique = UniqueCols::Provided;
        assert!(!is_valid(&agg));
        let mut pass = axes(
            SourceKind::Table,
            QueryShape::Passthrough,
            RefreshMode::Immediate,
            None,
        );
        pass.unique = UniqueCols::Provided;
        assert!(is_valid(&pass));
    }

    #[test]
    fn cte_subimv_source_pairs_only_with_decomposable_shapes() {
        let pass = axes(
            SourceKind::CteSubImv,
            QueryShape::Passthrough,
            RefreshMode::Immediate,
            None,
        );
        assert!(!is_valid(&pass));
        let dec = axes(
            SourceKind::CteSubImv,
            QueryShape::CteDecomposed,
            RefreshMode::Immediate,
            Some(AggFn::Sum),
        );
        assert!(is_valid(&dec));
    }

    #[test]
    fn all_axes_are_all_valid_and_nonempty() {
        let space = valid_space();
        assert!(!space.is_empty());
        assert!(space.iter().all(is_valid));
    }

    #[test]
    fn pairwise_covers_every_valid_2way_interaction() {
        let space = valid_space();
        let chosen = pairwise(&space);
        let needed = required_pairs(&space);
        let covered = required_pairs(&chosen);
        for p in &needed {
            assert!(covered.contains(p), "uncovered pair: {p:?}");
        }
        assert!(chosen.len() < space.len());
    }

    #[test]
    fn pairwise_is_deterministic() {
        assert_eq!(pairwise(&valid_space()), pairwise(&valid_space()));
    }

    #[test]
    fn every_pairwise_case_renders_to_a_planned_case() {
        for a in pairwise(&valid_space()) {
            let pc = plan_case(&a, 0).unwrap_or_else(|| panic!("no plan for {a:?}"));
            assert!(!pc.case.tables.is_empty());
            assert!(!pc.case.select_body.rendered_sql.is_empty());
            assert_eq!(pc.axes, a);
        }
    }

    #[test]
    fn planned_case_marks_matview_source_for_refresh_path() {
        let a = axes(
            SourceKind::MatView,
            QueryShape::SingleAggregate,
            RefreshMode::Immediate,
            Some(AggFn::Sum),
        );
        let pc = plan_case(&a, 7).unwrap();
        assert!(pc.source_is_refresh_driven);
        assert!(pc
            .source_objects
            .iter()
            .any(|o| matches!(o.kind, SourceObjectKind::MatView)));
    }

    #[test]
    fn planned_case_seq_makes_names_unique() {
        let a = axes(
            SourceKind::Table,
            QueryShape::Passthrough,
            RefreshMode::Immediate,
            None,
        );
        let p0 = plan_case(&a, 0).unwrap();
        let p1 = plan_case(&a, 1).unwrap();
        assert_ne!(p0.case.tables[0].name, p1.case.tables[0].name);
    }
}

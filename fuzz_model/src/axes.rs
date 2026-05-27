//! The orthogonal axes whose 2-way interactions defined the creation-bug class.
//! A validity predicate prunes impossible combinations before generation.

use crate::model::ColType;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SourceKind { Table, View, MatView, CteSubImv }

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum QueryShape { Passthrough, SingleAggregate, JoinInner, JoinLeft, CteDecomposed, SetOpUnionAll }

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RefreshMode { Immediate, Deferred }

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AggFn { Sum, Count, Min, Max, Avg }

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum UniqueCols { Absent, Provided }

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Lifecycle { CreateMutateDrop, CascadeDrop, Partitioned }

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
        matches!(self, ColType::Int | ColType::BigInt | ColType::Numeric | ColType::Float8)
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
    let shape_is_aggregate = matches!(a.shape, SingleAggregate | JoinInner | JoinLeft | CteDecomposed);
    match (shape_is_aggregate, a.agg.is_some()) {
        (true, false) | (false, true) => return false,
        _ => {}
    }

    // Aggregate-fn / measure-type compatibility.
    if let Some(f) = a.agg {
        match f {
            Sum | Avg => if !a.measure_ty.is_numeric_family() { return false; },
            Min | Max => if !a.measure_ty.is_orderable() { return false; },
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::ColType;

    fn axes(source: SourceKind, shape: QueryShape, refresh: RefreshMode, agg: Option<AggFn>) -> Axes {
        Axes { source, shape, refresh, agg, measure_ty: ColType::Numeric,
               unique: UniqueCols::Absent, lifecycle: Lifecycle::CreateMutateDrop }
    }

    #[test]
    fn axes_is_constructible_and_hashable() {
        let a = Axes {
            source: SourceKind::Table, shape: QueryShape::SingleAggregate,
            refresh: RefreshMode::Immediate, agg: Some(AggFn::Sum),
            measure_ty: ColType::Numeric, unique: UniqueCols::Absent,
            lifecycle: Lifecycle::CreateMutateDrop,
        };
        let mut set = std::collections::HashSet::new();
        set.insert(a);
        assert!(set.contains(&a));
    }

    #[test]
    fn matview_source_cannot_be_deferred() {
        let a = axes(SourceKind::MatView, QueryShape::Passthrough, RefreshMode::Deferred, None);
        assert!(!is_valid(&a));
        let b = axes(SourceKind::MatView, QueryShape::Passthrough, RefreshMode::Immediate, None);
        assert!(is_valid(&b));
    }

    #[test]
    fn passthrough_has_no_aggregate_fn() {
        let with_agg = axes(SourceKind::Table, QueryShape::Passthrough, RefreshMode::Immediate, Some(AggFn::Sum));
        assert!(!is_valid(&with_agg));
        let no_agg = axes(SourceKind::Table, QueryShape::Passthrough, RefreshMode::Immediate, None);
        assert!(is_valid(&no_agg));
    }

    #[test]
    fn aggregate_shapes_require_an_agg_fn() {
        let missing = axes(SourceKind::Table, QueryShape::SingleAggregate, RefreshMode::Immediate, None);
        assert!(!is_valid(&missing));
    }

    #[test]
    fn minmax_over_bool_is_invalid() {
        let mut a = axes(SourceKind::Table, QueryShape::SingleAggregate, RefreshMode::Immediate, Some(AggFn::Min));
        a.measure_ty = ColType::Bool;
        assert!(!is_valid(&a));
    }

    #[test]
    fn sum_avg_require_numeric_family() {
        let mut a = axes(SourceKind::Table, QueryShape::SingleAggregate, RefreshMode::Immediate, Some(AggFn::Sum));
        a.measure_ty = ColType::Text;
        assert!(!is_valid(&a));
    }

    #[test]
    fn provided_unique_cols_only_for_passthrough_or_join() {
        let mut agg = axes(SourceKind::Table, QueryShape::SingleAggregate, RefreshMode::Immediate, Some(AggFn::Sum));
        agg.unique = UniqueCols::Provided;
        assert!(!is_valid(&agg));
        let mut pass = axes(SourceKind::Table, QueryShape::Passthrough, RefreshMode::Immediate, None);
        pass.unique = UniqueCols::Provided;
        assert!(is_valid(&pass));
    }

    #[test]
    fn cte_subimv_source_pairs_only_with_decomposable_shapes() {
        let pass = axes(SourceKind::CteSubImv, QueryShape::Passthrough, RefreshMode::Immediate, None);
        assert!(!is_valid(&pass));
        let dec = axes(SourceKind::CteSubImv, QueryShape::CteDecomposed, RefreshMode::Immediate, Some(AggFn::Sum));
        assert!(is_valid(&dec));
    }
}

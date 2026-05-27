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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::ColType;

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
}

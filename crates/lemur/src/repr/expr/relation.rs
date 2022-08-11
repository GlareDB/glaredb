use super::ScalarExpr;
use crate::repr::compute::*;

use crate::repr::sort::GroupRanges;
use crate::repr::value::{Row, ValueVec};
use anyhow::{Result};
use serde::{Deserialize, Serialize};

pub type RelationKey = String;

/// Expressions that happen on entire relations.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum RelationExpr {
    Project(Project),
    Aggregate(Aggregate),
    OrderByGroupBy(OrderByGroupBy),
    CrossJoin(CrossJoin),
    NestedLoopJoin(NestedLoopJoin),
    Filter(Filter),
    Values(Values),
    Source(Source),
    Placeholder,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Project {
    pub columns: Vec<ScalarExpr>,
    pub input: Box<RelationExpr>,
}

pub type AggregateFunc = Box<dyn Fn(&ValueVec, &GroupRanges) -> Result<ValueVec> + Send>;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum AggregateOperation {
    First,
    Count,
    Min,
    Max,
    Sum,
    Avg,
}

impl AggregateOperation {
    pub fn try_from_str(s: &str) -> Option<AggregateOperation> {
        Some(match s {
            "count" => AggregateOperation::Count,
            "min" => AggregateOperation::Min,
            "max" => AggregateOperation::Max,
            "sum" => AggregateOperation::Sum,
            "avg" => AggregateOperation::Avg,
            _ => return None,
        })
    }

    pub fn func_for_operation(&self) -> AggregateFunc {
        match self {
            AggregateOperation::First => Box::new(VecUnaryAggregate::first_groups),
            AggregateOperation::Count => Box::new(VecCountAggregate::count_groups),
            AggregateOperation::Min => Box::new(VecUnaryCmpAggregate::min_groups),
            AggregateOperation::Max => Box::new(VecUnaryCmpAggregate::max_groups),
            AggregateOperation::Sum => Box::new(VecNumericAggregate::sum_groups),
            AggregateOperation::Avg => Box::new(VecNumericAggregate::avg_groups),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AggregateExpr {
    pub op: AggregateOperation,
    pub column: usize,
}

impl AggregateExpr {
    pub fn new(op: AggregateOperation, column: usize) -> AggregateExpr {
        AggregateExpr { op, column }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Aggregate {
    pub exprs: Vec<AggregateExpr>,
    pub group_by: Vec<usize>,
    pub input: Box<RelationExpr>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderByGroupBy {
    pub columns: Vec<usize>,
    pub input: Box<RelationExpr>,
    // TODO: Asc/desc,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CrossJoin {
    pub left: Box<RelationExpr>,
    pub right: Box<RelationExpr>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NestedLoopJoin {
    pub predicate: ScalarExpr,
    pub left: Box<RelationExpr>,
    pub right: Box<RelationExpr>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Filter {
    pub predicate: ScalarExpr,
    pub input: Box<RelationExpr>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Values {
    pub rows: Vec<Row>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Source {
    pub source: RelationKey,
    pub filter: Option<ScalarExpr>,
}

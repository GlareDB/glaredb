use crate::catalog::TableReference;
use anyhow::{anyhow, Result};
use lemur::repr::df::groupby::SortOrder;
use lemur::repr::df::Schema;
use lemur::repr::expr::{self, AggregateExpr, RelationExpr, ScalarExpr};
use lemur::repr::value::Row;

/// A logical plan representing a SQL read query.
#[derive(Debug, PartialEq)]
pub enum ReadPlan {
    CrossJoin(CrossJoin),
    Join(Join),
    Sort(Sort),
    Aggregate(Aggregate),
    Project(Project),
    Filter(Filter),
    ScanSource(ScanSource),
    Values(Values),
    Nothing,
}

#[derive(Debug, PartialEq)]
pub struct CrossJoin {
    pub left: Box<ReadPlan>,
    pub right: Box<ReadPlan>,
}

#[derive(Debug, PartialEq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
}

#[derive(Debug, PartialEq)]
pub struct Join {
    pub left: Box<ReadPlan>,
    pub right: Box<ReadPlan>,
    pub join_type: JoinType,
    pub on: ScalarExpr,
}

#[derive(Debug, PartialEq)]
pub struct Sort {
    pub columns: Vec<usize>,
    pub order: SortOrder,
    pub input: Box<ReadPlan>,
}

// TODO: Extend this. How do we want to handle user-defined aggregates?
// TODO: Change group columns to be expressions.
#[derive(Debug, PartialEq)]
pub struct Aggregate {
    pub group_by: Vec<usize>,
    pub funcs: Vec<AggregateExpr>,
    pub input: Box<ReadPlan>,
}

#[derive(Debug, PartialEq)]
pub struct Project {
    pub columns: Vec<ScalarExpr>,
    pub input: Box<ReadPlan>,
}

#[derive(Debug, PartialEq)]
pub struct Filter {
    pub predicate: ScalarExpr,
    pub input: Box<ReadPlan>,
}

#[derive(Debug, PartialEq)]
pub struct ScanSource {
    pub table: TableReference,
    pub filter: Option<ScalarExpr>,
    pub schema: Schema,
}

#[derive(Debug, PartialEq)]
pub struct Values {
    pub rows: Vec<Row>,
}

impl ReadPlan {
    fn replace<F>(&mut self, mut f: F) -> Result<()>
    where
        F: FnMut(Self) -> Result<Self>,
    {
        let temp = std::mem::replace(self, ReadPlan::Nothing);
        *self = f(temp)?;
        Ok(())
    }

    pub fn transform<F1, F2>(mut self, pre: &mut F1, post: &mut F2) -> Result<Self>
    where
        F1: FnMut(Self) -> Result<Self>,
        F2: FnMut(Self) -> Result<Self>,
    {
        self = pre(self)?;
        match &mut self {
            ReadPlan::Nothing | ReadPlan::Values(_) | ReadPlan::ScanSource(_) => (),
            ReadPlan::Sort(Sort { input, .. })
            | ReadPlan::Filter(Filter { input, .. })
            | ReadPlan::Project(Project { input, .. })
            | ReadPlan::Aggregate(Aggregate { input, .. }) => {
                input.replace(|node| node.transform(pre, post))?;
            }
            ReadPlan::CrossJoin(CrossJoin { left, right })
            | ReadPlan::Join(Join { left, right, .. }) => {
                left.replace(|node| node.transform(pre, post))?;
                right.replace(|node| node.transform(pre, post))?;
            }
        }
        self = post(self)?;
        Ok(self)
    }

    pub fn transform_mut<F1, F2>(&mut self, pre: &mut F1, post: &mut F2) -> Result<()>
    where
        F1: FnMut(Self) -> Result<Self>,
        F2: FnMut(Self) -> Result<Self>,
    {
        self.replace(|node| node.transform(pre, post))
    }

    /// Lower self into a lemur relational expression.
    pub fn lower(self) -> Result<RelationExpr> {
        Ok(match self {
            ReadPlan::Nothing => RelationExpr::Placeholder,
            ReadPlan::Values(Values { rows, .. }) => RelationExpr::Values(expr::Values { rows }),
            ReadPlan::ScanSource(ScanSource { table, filter, .. }) => {
                RelationExpr::Source(expr::Source {
                    source: table.to_string(), // TODO: Change relation key to avoid having to make a string.
                    filter,
                })
            }
            ReadPlan::Sort(Sort {
                columns,
                order,
                input,
            }) => RelationExpr::OrderByGroupBy(expr::OrderByGroupBy {
                columns,
                input: Box::new(input.lower()?),
            }),
            ReadPlan::Filter(Filter { predicate, input }) => RelationExpr::Filter(expr::Filter {
                predicate,
                input: Box::new(input.lower()?),
            }),
            ReadPlan::Project(Project { columns, input }) => RelationExpr::Project(expr::Project {
                columns,
                input: Box::new(input.lower()?),
            }),
            ReadPlan::Aggregate(Aggregate {
                group_by,
                funcs,
                input,
            }) => RelationExpr::Aggregate(expr::Aggregate {
                exprs: funcs,
                group_by,
                input: Box::new(input.lower()?),
            }),
            ReadPlan::CrossJoin(CrossJoin { left, right }) => {
                RelationExpr::CrossJoin(expr::CrossJoin {
                    left: Box::new(left.lower()?),
                    right: Box::new(right.lower()?),
                })
            }
            ReadPlan::Join(Join {
                left,
                right,
                join_type,
                on,
            }) => {
                // TODO: Proper join.
                RelationExpr::Filter(expr::Filter {
                    predicate: on,
                    input: Box::new(RelationExpr::CrossJoin(expr::CrossJoin {
                        left: Box::new(left.lower()?),
                        right: Box::new(right.lower()?),
                    })),
                })
            }
        })
    }
}

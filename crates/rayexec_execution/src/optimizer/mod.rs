pub mod join_order;

use crate::{
    optimizer::join_order::JoinOrderRule,
    planner::operator::{
        Aggregate, AnyJoin, CreateTableAs, CrossJoin, EqualityJoin, Filter, Limit, LogicalOperator,
        Order, Projection,
    },
};
use rayexec_error::Result;

#[derive(Debug)]
pub struct Optimizer {}

impl Optimizer {
    pub fn new() -> Self {
        Optimizer {}
    }

    /// Run a logical plan through the optimizer.
    pub fn optimize(&self, plan: LogicalOperator) -> Result<LogicalOperator> {
        let join_order = JoinOrderRule {};
        let optimized = join_order.optimize(plan)?;

        Ok(optimized)
    }
}

pub trait OptimizeRule {
    /// Apply an optimization rule to the logical plan.
    fn optimize(&self, plan: LogicalOperator) -> Result<LogicalOperator>;
}

/// Walk a plan depth first.
///
/// `pre` provides access to children on the way down. `post` provides access to
/// children on the way back up.
pub fn walk_plan<F1, F2>(
    mut plan: LogicalOperator,
    pre: &mut F1,
    post: &mut F2,
) -> Result<LogicalOperator>
where
    F1: FnMut(&mut LogicalOperator) -> Result<()>,
    F2: FnMut(&mut LogicalOperator) -> Result<()>,
{
    pre(&mut plan)?;
    plan = match plan {
        LogicalOperator::Projection(mut p) => {
            pre(&mut p.input)?;
            *p.input = walk_plan(*p.input, pre, post)?;
            post(&mut p.input)?;
            LogicalOperator::Projection(p)
        }
        LogicalOperator::Filter(mut p) => {
            pre(&mut p.input)?;
            *p.input = walk_plan(*p.input, pre, post)?;
            post(&mut p.input)?;
            LogicalOperator::Filter(p)
        }
        LogicalOperator::Aggregate(mut p) => {
            pre(&mut p.input)?;
            *p.input = walk_plan(*p.input, pre, post)?;
            post(&mut p.input)?;
            LogicalOperator::Aggregate(p)
        }
        LogicalOperator::Order(mut p) => {
            pre(&mut p.input)?;
            *p.input = walk_plan(*p.input, pre, post)?;
            post(&mut p.input)?;
            LogicalOperator::Order(p)
        }
        LogicalOperator::Limit(mut p) => {
            pre(&mut p.input)?;
            *p.input = walk_plan(*p.input, pre, post)?;
            post(&mut p.input)?;
            LogicalOperator::Limit(p)
        }
        LogicalOperator::CrossJoin(mut p) => {
            pre(&mut p.left)?;
            *p.left = walk_plan(*p.left, pre, post)?;
            post(&mut p.left)?;

            pre(&mut p.right)?;
            *p.right = walk_plan(*p.right, pre, post)?;
            post(&mut p.right)?;

            LogicalOperator::CrossJoin(p)
        }
        LogicalOperator::AnyJoin(mut p) => {
            pre(&mut p.left)?;
            *p.left = walk_plan(*p.left, pre, post)?;
            post(&mut p.left)?;

            pre(&mut p.right)?;
            *p.right = walk_plan(*p.right, pre, post)?;
            post(&mut p.right)?;

            LogicalOperator::AnyJoin(p)
        }
        LogicalOperator::EqualityJoin(mut p) => {
            pre(&mut p.left)?;
            *p.left = walk_plan(*p.left, pre, post)?;
            post(&mut p.left)?;

            pre(&mut p.right)?;
            *p.right = walk_plan(*p.right, pre, post)?;
            post(&mut p.right)?;

            LogicalOperator::EqualityJoin(p)
        }
        LogicalOperator::CreateTableAs(mut p) => {
            pre(&mut p.input)?;
            *p.input = walk_plan(*p.input, pre, post)?;
            post(&mut p.input)?;
            LogicalOperator::CreateTableAs(p)
        }
        plan @ LogicalOperator::ExpressionList(_)
        | plan @ LogicalOperator::Empty
        | plan @ LogicalOperator::SetVar(_)
        | plan @ LogicalOperator::ShowVar(_)
        | plan @ LogicalOperator::Scan(_) => plan,
    };
    post(&mut plan)?;

    Ok(plan)
}

pub fn walk_plan_pre<F>(plan: LogicalOperator, pre: &mut F) -> Result<LogicalOperator>
where
    F: FnMut(&mut LogicalOperator) -> Result<()>,
{
    let mut post = |_plan: &mut LogicalOperator| Ok(());
    walk_plan(plan, pre, &mut post)
}

pub fn walk_plan_post<F>(plan: LogicalOperator, post: &mut F) -> Result<LogicalOperator>
where
    F: FnMut(&mut LogicalOperator) -> Result<()>,
{
    let mut pre = |_plan: &mut LogicalOperator| Ok(());
    walk_plan(plan, &mut pre, post)
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::planner::operator::LogicalExpression;
    use rayexec_bullet::scalar::OwnedScalarValue;

    // #[test]
    // fn walk_plan_pre_post() {
    //     let plan = LogicalOperator::Projection(Projection {
    //         exprs: Vec::new(),
    //         input: Box::new(LogicalOperator::Filter(Filter {
    //             predicate: LogicalExpression::Literal(OwnedScalarValue::Null),
    //             input: Box::new(LogicalOperator::Empty),
    //         })),
    //     });

    //     let plan = walk_plan(
    //         plan,
    //         &mut |child| {
    //             match child {
    //                 LogicalOperator::Projection(proj) => proj
    //                     .exprs
    //                     .push(LogicalExpression::Literal(OwnedScalarValue::Int8(1))),
    //                 LogicalOperator::Filter(_) => {}
    //                 LogicalOperator::Empty => {}
    //                 other => panic!("unexpected child {other:?}"),
    //             }
    //             Ok(())
    //         },
    //         &mut |child| {
    //             match child {
    //                 LogicalOperator::Projection(proj) => {
    //                     assert_eq!(
    //                         vec![LogicalExpression::Literal(OwnedScalarValue::Int8(1))],
    //                         proj.exprs
    //                     );
    //                     proj.exprs
    //                         .push(LogicalExpression::Literal(OwnedScalarValue::Int8(2)))
    //                 }
    //                 LogicalOperator::Filter(_) => {}
    //                 LogicalOperator::Empty => {}
    //                 other => panic!("unexpected child {other:?}"),
    //             }
    //             Ok(())
    //         },
    //     )
    //     .unwrap();

    //     match plan {
    //         LogicalOperator::Projection(proj) => {
    //             assert_eq!(
    //                 vec![
    //                     LogicalExpression::Literal(OwnedScalarValue::Int8(1)),
    //                     LogicalExpression::Literal(OwnedScalarValue::Int8(2)),
    //                 ],
    //                 proj.exprs
    //             );
    //         }
    //         other => panic!("unexpected root {other:?}"),
    //     }
    // }
}

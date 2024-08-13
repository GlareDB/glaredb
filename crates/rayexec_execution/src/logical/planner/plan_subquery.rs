use crate::{
    expr::scalar::{BinaryOperator, PlannedBinaryOperator},
    functions::aggregate::count::CountNonNullImpl,
    logical::{
        context::QueryContext,
        expr::{LogicalExpression, Subquery},
        operator::{Aggregate, CrossJoin, Limit, LogicalNode, LogicalOperator, Projection},
    },
};
use rayexec_bullet::{datatype::DataType, scalar::OwnedScalarValue};
use rayexec_error::Result;

use super::decorrelate::SubqueryDecorrelator;

/// Logic for flattening and planning subqueries.
#[derive(Debug, Clone, Copy)]
pub struct SubqueryPlanner;

impl SubqueryPlanner {
    pub fn flatten(
        &self,
        context: &mut QueryContext,
        mut plan: LogicalOperator,
    ) -> Result<LogicalOperator> {
        plan.walk_mut_post(&mut |plan| {
            match plan {
                LogicalOperator::Projection(node) => {
                    let proj = node.as_mut();
                    for expr in &mut proj.exprs {
                        self.plan_subquery_expr(context, expr, &mut proj.input)?;
                    }
                }
                LogicalOperator::Aggregate(node) => {
                    let agg = node.as_mut();
                    for expr in &mut agg.aggregates {
                        self.plan_subquery_expr(context, expr, &mut agg.input)?;
                    }
                }
                LogicalOperator::Filter(node) => {
                    let filter = node.as_mut();
                    self.plan_subquery_expr(context, &mut filter.predicate, &mut filter.input)?;
                }
                _other => (),
            };
            Ok(())
        })?;

        Ok(plan)
    }

    /// Plans a subquery expression with a logical operator input.
    ///
    /// Recursively transforms the expression the remove subqueries and place
    /// them in the plan.
    ///
    /// Does nothing if the expression isn't a subquery, or doesn't have a
    /// subquery as a child.
    pub fn plan_subquery_expr(
        &self,
        context: &mut QueryContext,
        expr: &mut LogicalExpression,
        input: &mut LogicalOperator,
    ) -> Result<()> {
        let schema = input.output_schema(&[])?;
        let mut num_cols = schema.types.len();

        expr.walk_mut_post(&mut |expr| {
            if let LogicalExpression::Subquery(subquery) = expr {
                if subquery.is_correlated()? {
                    *expr = SubqueryDecorrelator::default()
                        .plan_correlated(context, subquery, input, num_cols, 1)?;
                } else {
                    *expr = self.plan_uncorrelated(subquery, input, num_cols)?;
                }
                num_cols += 1;
            }
            Ok(())
        })?;

        Ok(())
    }

    /// Plans a single uncorrelated subquery expression.
    ///
    /// The subquery will be flattened into the original input operator, and a
    /// new expression will be returned referencing the flattened result.
    ///
    /// `input_columns` is the number of columns that `input` will originally
    /// produce.
    fn plan_uncorrelated(
        &self,
        subquery: &mut Subquery,
        input: &mut LogicalOperator,
        input_columns: usize,
    ) -> Result<LogicalExpression> {
        let root = subquery.take_root();
        match subquery {
            Subquery::Scalar { .. } => {
                // Normal subquery.
                //
                // Cross join the subquery with the original input, replace
                // the subquery expression with a reference to the new
                // column.
                let column_ref = LogicalExpression::new_column(input_columns);

                // TODO: We should check that the subquery produces one
                // column around here.

                // LIMIT the original subquery to 1
                let subquery = LogicalOperator::Limit(LogicalNode::new(Limit {
                    offset: None,
                    limit: 1,
                    input: root,
                }));

                let orig_input = Box::new(input.take());
                *input = LogicalOperator::CrossJoin(LogicalNode::new(CrossJoin {
                    left: orig_input,
                    right: Box::new(subquery),
                }));

                Ok(column_ref)
            }
            Subquery::Exists { negated, .. } => {
                // Exists subquery.
                //
                // EXISTS -> COUNT(*) == 1
                // NOT EXISTS -> COUNT(*) != 1
                //
                // Cross join with existing input. Replace original subquery expression
                // with reference to new column.

                let expr = LogicalExpression::Binary {
                    op: if *negated {
                        PlannedBinaryOperator {
                            op: BinaryOperator::NotEq,
                            scalar: BinaryOperator::NotEq
                                .scalar_function()
                                .plan_from_datatypes(&[DataType::Int64, DataType::Int64])?,
                        }
                    } else {
                        PlannedBinaryOperator {
                            op: BinaryOperator::Eq,
                            scalar: BinaryOperator::Eq
                                .scalar_function()
                                .plan_from_datatypes(&[DataType::Int64, DataType::Int64])?,
                        }
                    },
                    left: Box::new(LogicalExpression::new_column(input_columns)),
                    right: Box::new(LogicalExpression::Literal(OwnedScalarValue::Int64(1))),
                };

                // COUNT(*) and LIMIT the original query.
                let subquery = LogicalOperator::Aggregate(LogicalNode::new(Aggregate {
                    // TODO: Replace with CountStar once that's in.
                    //
                    // This currently just includes a 'true'
                    // projection that makes the final aggregate
                    // represent COUNT(true).
                    aggregates: vec![LogicalExpression::Aggregate {
                        agg: Box::new(CountNonNullImpl),
                        inputs: vec![LogicalExpression::new_column(0)],
                        filter: None,
                    }],
                    grouping_sets: None,
                    group_exprs: Vec::new(),
                    input: Box::new(LogicalOperator::Limit(LogicalNode::new(Limit {
                        offset: None,
                        limit: 1,
                        input: Box::new(LogicalOperator::Projection(LogicalNode::new(
                            Projection {
                                exprs: vec![LogicalExpression::Literal(OwnedScalarValue::Boolean(
                                    true,
                                ))],
                                input: root,
                            },
                        ))),
                    }))),
                }));

                let orig_input = Box::new(input.take());
                *input = LogicalOperator::CrossJoin(LogicalNode::new(CrossJoin {
                    left: orig_input,
                    right: Box::new(subquery),
                }));

                Ok(expr)
            }
            Subquery::Any { .. } => unimplemented!(),
        }
    }
}

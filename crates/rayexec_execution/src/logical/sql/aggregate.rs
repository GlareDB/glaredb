use std::collections::HashMap;

use crate::logical::{
    context::QueryContext,
    expr::LogicalExpression,
    grouping_set::GroupingSets,
    operator::{Aggregate, LogicalOperator, Projection},
};
use rayexec_error::{RayexecError, Result};
use rayexec_parser::ast;

use super::{binder::Bound, expr::ExpressionContext};

/// Logic for planning aggregates and group bys.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AggregatePlanner;

impl AggregatePlanner {
    /// Extracts and plans aggregates from the select list, returning a new plan
    /// containing the aggregates.
    ///
    /// The select list will be modified to point to the correct column in the
    /// new plan. For example, if we come across an expression representing
    /// `AVG(a)`, the AVG function will be extracted, and the expression will be
    /// modified to point to the result of `AVG(a)`.
    pub fn plan(
        &self,
        context: &mut QueryContext,
        expr_ctx: ExpressionContext,
        select_list: &mut [LogicalExpression],
        alias_map: &HashMap<String, usize>,
        mut plan: LogicalOperator,
        group_by: Option<ast::GroupByNode<Bound>>,
    ) -> Result<LogicalOperator> {
        // Track which select expressions we've modified when extracting
        // aggregates. This prevents will prevent us from try to point a GROUP
        // BY expression to something in the select list if it's already been
        // modified, since that would be incorrect.
        let mut modified = vec![false; select_list.len()];

        // Extract aggs.
        let aggregates = self.extract_aggregates(select_list, &mut modified)?;

        // Starting column index of expressions that will be part of the group
        // by.
        let group_by_exprs_offset = aggregates.len();

        let (group_exprs, grouping_sets) = match group_by {
            Some(group_by) => match group_by {
                ast::GroupByNode::All => unimplemented!(),
                ast::GroupByNode::Exprs { mut exprs } => {
                    let expr = match exprs.len() {
                        1 => exprs.pop().unwrap(),
                        _ => {
                            return Err(RayexecError::new("Invalid number of group by expressions"))
                        }
                    };

                    let (ast_exprs, grouping_sets) = match expr {
                        ast::GroupByExpr::Expr(exprs) => {
                            let grouping_sets = GroupingSets::new_for_group_by(
                                (0..exprs.len())
                                    .map(|i| i + group_by_exprs_offset)
                                    .collect(),
                            );
                            (exprs, grouping_sets)
                        }
                        ast::GroupByExpr::Rollup(exprs) => {
                            let grouping_sets = GroupingSets::new_for_rollup(
                                (0..exprs.len())
                                    .map(|i| i + group_by_exprs_offset)
                                    .collect(),
                            );
                            (exprs, grouping_sets)
                        }
                        ast::GroupByExpr::Cube(exprs) => {
                            let grouping_sets = GroupingSets::new_for_cube(
                                (0..exprs.len())
                                    .map(|i| i + group_by_exprs_offset)
                                    .collect(),
                            );
                            (exprs, grouping_sets)
                        }
                        _ => unimplemented!(),
                    };

                    let exprs = ast_exprs
                        .into_iter()
                        .map(|expr| {
                            expr_ctx.plan_expression_with_select_list(
                                context,
                                alias_map,
                                select_list,
                                expr,
                            )
                        })
                        .collect::<Result<Vec<_>>>()?;

                    // Modify select list to point point to expressions in the
                    // group by as needed.
                    let exprs = self.extract_group_by_exprs(
                        select_list,
                        &mut modified,
                        exprs,
                        group_by_exprs_offset,
                    )?;

                    (exprs, Some(grouping_sets))
                }
            },
            None => (Vec::new(), None),
        };

        if !aggregates.is_empty() || grouping_sets.is_some() {
            plan = self.build_aggregate(aggregates, group_exprs, grouping_sets, plan)?;
        }

        Ok(plan)
    }

    /// Builds an aggregate node in the plan.
    ///
    /// Inputs to the aggregation will be placed in a pre-projection.
    fn build_aggregate(
        &self,
        mut agg_exprs: Vec<LogicalExpression>,
        mut group_exprs: Vec<LogicalExpression>,
        grouping_sets: Option<GroupingSets>,
        current: LogicalOperator,
    ) -> Result<LogicalOperator> {
        let mut projections = Vec::with_capacity(agg_exprs.len());
        for agg_expr in agg_exprs.iter_mut() {
            match agg_expr {
                LogicalExpression::Aggregate { inputs, .. } => {
                    let idx = projections.len();
                    let new_inputs = (0..inputs.len())
                        .map(|i| LogicalExpression::new_column(idx + i))
                        .collect();
                    let mut old = std::mem::replace(inputs, new_inputs);
                    projections.append(&mut old);
                }
                other => {
                    return Err(RayexecError::new(format!(
                        "Unexpected logical expression: {other:?}"
                    )))
                }
            }
        }

        for expr in &mut group_exprs {
            let idx = projections.len();
            let old = std::mem::replace(expr, LogicalExpression::new_column(idx));
            projections.push(old);
        }

        let projection = LogicalOperator::Projection(Projection {
            exprs: projections,
            input: Box::new(current),
        });

        Ok(LogicalOperator::Aggregate(Aggregate {
            aggregates: agg_exprs,
            group_exprs,
            grouping_sets,
            input: Box::new(projection),
        }))
    }

    /// Extract aggregates functions from the logical select list, returning them.
    ///
    /// This will replace the aggregate expression in `exprs` with a column
    /// reference that points to column position in the returned aggregate list.
    ///
    /// This does not change the logical output of `exprs`.
    fn extract_aggregates(
        &self,
        exprs: &mut [LogicalExpression],
        modified: &mut [bool],
    ) -> Result<Vec<LogicalExpression>> {
        let mut aggs = Vec::new();
        for (expr, modified) in exprs.iter_mut().zip(modified.iter_mut()) {
            let mut in_aggregate = false;
            expr.walk_mut_pre(&mut |expr| {
                if expr.is_aggregate() {
                    if in_aggregate {
                        return Err(RayexecError::new("Cannot nest aggregate functions"));
                    }
                    in_aggregate = true;

                    let column_ref = LogicalExpression::new_column(aggs.len());
                    let agg = std::mem::replace(expr, column_ref);
                    *modified = true;
                    aggs.push(agg);
                }
                Ok(())
            })?;
        }

        Ok(aggs)
    }

    /// Extract group by expressions from both the select list, and the
    /// expressions in the GROUP BY clause.
    ///
    /// When a select expression matches exactly one of the group by
    /// expressions, it will be updated with a column reference pointing to that
    /// expression.
    ///
    /// If a select expression has been previously modified, no group by
    /// expressions will ever point to it.
    ///
    /// This does not change the logical output of `select_exprs`.
    fn extract_group_by_exprs(
        &self,
        select_exprs: &mut [LogicalExpression],
        modified: &mut [bool],
        group_by_exprs: Vec<LogicalExpression>,
        offset: usize,
    ) -> Result<Vec<LogicalExpression>> {
        let mut groups = Vec::new();
        for group_by_expr in group_by_exprs {
            // TODO: Select aliases
            //
            // This is valid:
            // SELECT c1 / 100 AS my_alias, SUM(c2) FROM t1 GROUP BY my_alias

            // Extract expressions from the select that equal our group by
            // expression, and replace with a column reference.
            let mut group_added = false;
            for (expr, modified) in select_exprs.iter_mut().zip(modified.iter_mut()) {
                // If this select expression was previously modified, skip it
                // since it'll be pointing to the output of an aggregate.
                if *modified {
                    continue;
                }

                if expr == &group_by_expr {
                    if !group_added {
                        let column_ref = LogicalExpression::new_column(groups.len() + offset);
                        let group = std::mem::replace(expr, column_ref);
                        groups.push(group);
                        group_added = true;
                    } else {
                        // We're already tracking the group. Just find the
                        // existing index, and replace the expression.
                        let group_idx = groups.iter().position(|g| g == expr).unwrap();
                        let column_ref = LogicalExpression::new_column(group_idx + offset);
                        *expr = column_ref;
                    }

                    *modified = true;
                }
            }

            if !group_added {
                // Group not referenced in select. Go ahead and add it.
                groups.push(group_by_expr);
            }
        }

        Ok(groups)
    }
}

#[cfg(test)]
mod tests {
    use crate::functions::aggregate::sum::{SumFloat64Impl, SumImpl};

    use super::*;

    #[test]
    fn extract_aggregates_only_aggregate() {
        let mut selects = vec![LogicalExpression::Aggregate {
            agg: Box::new(SumImpl::Float64(SumFloat64Impl)),
            inputs: vec![LogicalExpression::new_column(0)],
            filter: None,
        }];
        let mut modified = vec![false];

        let out = AggregatePlanner
            .extract_aggregates(&mut selects, &mut modified)
            .unwrap();

        let expect_aggs = vec![LogicalExpression::Aggregate {
            agg: Box::new(SumImpl::Float64(SumFloat64Impl)),
            inputs: vec![LogicalExpression::new_column(0)],
            filter: None,
        }];
        assert_eq!(expect_aggs, out);

        assert_eq!(vec![LogicalExpression::new_column(0)], selects);
        assert_eq!(vec![true], modified);
    }

    #[test]
    fn extract_aggregates_with_other_expressions() {
        let mut selects = vec![
            LogicalExpression::new_column(1),
            LogicalExpression::Aggregate {
                agg: Box::new(SumImpl::Float64(SumFloat64Impl)),
                inputs: vec![LogicalExpression::new_column(0)],
                filter: None,
            },
        ];
        let mut modified = vec![false, false];

        let out = AggregatePlanner
            .extract_aggregates(&mut selects, &mut modified)
            .unwrap();

        let expect_aggs = vec![LogicalExpression::Aggregate {
            agg: Box::new(SumImpl::Float64(SumFloat64Impl)),
            inputs: vec![LogicalExpression::new_column(0)],
            filter: None,
        }];
        assert_eq!(expect_aggs, out);

        let expected_selects = vec![
            LogicalExpression::new_column(1),
            LogicalExpression::new_column(0),
        ];
        assert_eq!(expected_selects, selects);
        assert_eq!(vec![false, true], modified);
    }

    #[test]
    fn extract_group_by_single() {
        let mut selects = vec![
            LogicalExpression::new_column(1),
            LogicalExpression::new_column(0),
        ];
        let mut modified = vec![false, false];

        let group_by = vec![LogicalExpression::new_column(0)];

        let out = AggregatePlanner
            .extract_group_by_exprs(&mut selects, &mut modified, group_by, 2)
            .unwrap();

        let expected_group_by = vec![LogicalExpression::new_column(0)];
        assert_eq!(expected_group_by, out);

        let expected_selects = vec![
            LogicalExpression::new_column(1),
            LogicalExpression::new_column(2),
        ];
        assert_eq!(expected_selects, selects);
        assert_eq!(vec![false, true], modified);
    }

    #[test]
    fn extract_aggregate_and_group_by() {
        // t1(c1, c2)
        //
        // SELECT c1, sum(c2) FROM t1 GROUP BY c1

        let mut selects = vec![
            LogicalExpression::new_column(0),
            LogicalExpression::Aggregate {
                agg: Box::new(SumImpl::Float64(SumFloat64Impl)),
                inputs: vec![LogicalExpression::new_column(1)],
                filter: None,
            },
        ];
        let mut modified = vec![false, false];

        let group_by = vec![LogicalExpression::new_column(0)];

        let got_aggs = AggregatePlanner
            .extract_aggregates(&mut selects, &mut modified)
            .unwrap();
        let got_groups = AggregatePlanner
            .extract_group_by_exprs(&mut selects, &mut modified, group_by, 1)
            .unwrap();

        let expected_aggs = vec![LogicalExpression::Aggregate {
            agg: Box::new(SumImpl::Float64(SumFloat64Impl)),
            inputs: vec![LogicalExpression::new_column(1)],
            filter: None,
        }];
        let expected_groups = vec![LogicalExpression::new_column(0)];
        let expected_selects = vec![
            LogicalExpression::new_column(1),
            LogicalExpression::new_column(0),
        ];
        assert_eq!(expected_aggs, got_aggs);
        assert_eq!(expected_groups, got_groups);
        assert_eq!(expected_selects, selects);
        assert_eq!(vec![true, true], modified);
    }
}

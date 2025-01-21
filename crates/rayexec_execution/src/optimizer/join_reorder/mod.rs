mod edge;
mod graph;
mod statistics;
mod subgraph;

use std::collections::{HashSet, VecDeque};

use graph::Graph;
use rayexec_error::Result;

use super::filter_pushdown::extracted_filter::ExtractedFilter;
use super::filter_pushdown::split::split_conjunction;
use super::OptimizeRule;
use crate::expr::column_expr::ColumnExpr;
use crate::expr::Expression;
use crate::logical::binder::bind_context::BindContext;
use crate::logical::binder::table_list::TableRef;
use crate::logical::logical_join::{ComparisonCondition, JoinType};
use crate::logical::operator::{LogicalNode, LogicalOperator};

/// Reorders joins in the plan.
///
/// Currently just does some reordering or filters + cross joins, but will
/// support switching join sides based on statistics eventually.
#[derive(Debug, Default)]
pub struct JoinReorder {}

impl OptimizeRule for JoinReorder {
    fn optimize(
        &mut self,
        bind_context: &mut BindContext,
        plan: LogicalOperator,
    ) -> Result<LogicalOperator> {
        let mut reorder = InnerJoinReorder::default();
        reorder.reorder(bind_context, plan)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum ReorderableCondition {
    Inner {
        condition: ComparisonCondition,
    },
    Semi {
        conditions: Vec<ComparisonCondition>,
    },
}

impl ReorderableCondition {
    pub fn get_column_refs(&self) -> HashSet<ColumnExpr> {
        match self {
            Self::Inner { condition } => condition
                .left
                .get_column_references()
                .into_iter()
                .chain(condition.right.get_column_references())
                .collect(),
            Self::Semi { conditions } => {
                let mut cols = HashSet::new();
                for condition in conditions {
                    cols.extend(condition.left.get_column_references());
                    cols.extend(condition.right.get_column_references());
                }

                cols
            }
        }
    }

    pub fn get_left_right_table_refs(&self) -> [HashSet<TableRef>; 2] {
        match self {
            Self::Inner { condition } => [
                condition.left.get_table_references(),
                condition.right.get_table_references(),
            ],
            Self::Semi { conditions } => {
                let left_refs = conditions.iter().fold(HashSet::new(), |mut acc, cond| {
                    acc.extend(cond.left.get_table_references());
                    acc
                });
                let right_refs = conditions.iter().fold(HashSet::new(), |mut acc, cond| {
                    acc.extend(cond.right.get_table_references());
                    acc
                });
                [left_refs, right_refs]
            }
        }
    }
}

#[derive(Debug, Default)]
struct InnerJoinReorder {
    /// Extracted conditions from comparison joins.
    conditions: Vec<ReorderableCondition>,
    /// Extracted expressions that cannot be used for inner joins.
    filters: Vec<ExtractedFilter>,
    /// All plans that will be used to build up the join tree.
    child_plans: Vec<LogicalOperator>,
}

impl InnerJoinReorder {
    fn add_expression(&mut self, expr: Expression) {
        let mut split = Vec::new();
        split_conjunction(expr, &mut split);

        for expr in split {
            self.filters.push(ExtractedFilter::from_expr(expr))
        }
    }

    fn reorder(
        &mut self,
        bind_context: &mut BindContext,
        mut root: LogicalOperator,
    ) -> Result<LogicalOperator> {
        // Note that we're not matching on "magic" materialization scans as the
        // normal materialization scan should already handle the reorder within
        // the plan anyways.
        match &root {
            LogicalOperator::MaterializationScan(scan) => {
                // Start a new reorder for this materializations.
                let mut reorder = InnerJoinReorder::default();
                let mut plan = {
                    let mat = bind_context.get_materialization_mut(scan.node.mat)?;
                    std::mem::replace(&mut mat.plan, LogicalOperator::Invalid)
                };
                plan = reorder.reorder(bind_context, plan)?;

                // Since the one or children in the plan might've switched
                // sides, we need to recompute the table refs to ensure they're
                // updated to be the correct order.
                //
                // "magic" materializations don't need to worry about this,
                // since they project out of the materialization (and the column
                // refs don't change).
                let table_refs = plan.get_output_table_refs(bind_context);

                let mat = bind_context.get_materialization_mut(scan.node.mat)?;
                mat.plan = plan;
                mat.table_refs = table_refs;

                let new_scan = scan.clone();

                return Ok(LogicalOperator::MaterializationScan(new_scan));
            }
            LogicalOperator::Filter(_) => {
                self.extract_filters_and_join_children(root)?;
            }
            LogicalOperator::CrossJoin(_) => {
                self.extract_filters_and_join_children(root)?;
            }
            LogicalOperator::ComparisonJoin(join)
                if join.node.join_type == JoinType::Inner
                    || join.node.join_type == JoinType::LeftSemi =>
            {
                self.extract_filters_and_join_children(root)?;
            }
            LogicalOperator::ArbitraryJoin(join) if join.node.join_type == JoinType::Inner => {
                self.extract_filters_and_join_children(root)?;
            }
            _ => {
                // Can't extract at this node, try reordering children and
                // return.
                root.modify_replace_children(&mut |child| {
                    let mut reorder = Self::default();
                    reorder.reorder(bind_context, child)
                })?;
                return Ok(root);
            }
        }

        // Before reordering the join tree at this level, go ahead and reorder
        // nested joins that we're not able to flatten at this level.
        let mut child_plans = Vec::with_capacity(self.child_plans.len());
        for child in self.child_plans.drain(..) {
            let mut reorder = Self::default();
            let child = reorder.reorder(bind_context, child)?;
            child_plans.push(child);
        }

        let graph = Graph::try_new(
            child_plans,
            self.conditions.drain(..),
            self.filters.drain(..),
            bind_context,
        )?;

        let plan = graph.try_build()?;

        Ok(plan)
    }

    fn extract_filters_and_join_children(&mut self, root: LogicalOperator) -> Result<()> {
        assert!(self.filters.is_empty());
        assert!(self.child_plans.is_empty());

        let mut queue: VecDeque<_> = [root].into_iter().collect();

        while let Some(plan) = queue.pop_front() {
            match plan {
                LogicalOperator::Filter(mut filter) => {
                    self.add_expression(filter.node.filter);
                    for child in filter.children.drain(..) {
                        queue.push_back(child);
                    }
                }
                LogicalOperator::CrossJoin(mut join) => {
                    for child in join.children.drain(..) {
                        queue.push_back(child);
                    }
                }
                LogicalOperator::ComparisonJoin(mut join) => {
                    if join.node.join_type == JoinType::Inner {
                        for cond in &join.node.conditions {
                            self.conditions.push(ReorderableCondition::Inner {
                                condition: cond.clone(),
                            })
                        }
                        for child in join.children.drain(..) {
                            queue.push_back(child);
                        }
                    } else if join.node.join_type == JoinType::LeftSemi {
                        // Semi join conditions need to be kept together as
                        // they're not freely reorderable.
                        self.conditions.push(ReorderableCondition::Semi {
                            conditions: join.node.conditions,
                        });

                        for child in join.children.drain(..) {
                            queue.push_back(child);
                        }
                    } else {
                        // Nothing we can do (yet).
                        self.child_plans.push(LogicalOperator::ComparisonJoin(join))
                    }
                }
                LogicalOperator::ArbitraryJoin(mut join) => {
                    if join.node.join_type == JoinType::Inner {
                        self.add_expression(join.node.condition);
                        for child in join.children.drain(..) {
                            queue.push_back(child);
                        }
                    } else {
                        // Nothing we can do (yet).
                        self.child_plans.push(LogicalOperator::ArbitraryJoin(join))
                    }
                }
                other => self.child_plans.push(other),
            }
        }

        Ok(())
    }
}

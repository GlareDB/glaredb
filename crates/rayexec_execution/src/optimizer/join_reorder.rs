use std::collections::{HashSet, VecDeque};

use crate::{
    expr::{self, Expression},
    logical::{
        binder::bind_context::{BindContext, TableRef},
        logical_filter::LogicalFilter,
        logical_join::{JoinType, LogicalComparisonJoin, LogicalCrossJoin},
        operator::{LocationRequirement, LogicalNode, LogicalOperator, Node},
    },
    optimizer::filter_pushdown::condition_extractor::JoinConditionExtractor,
};
use rayexec_error::{RayexecError, Result};

use super::{
    filter_pushdown::{extracted_filter::ExtractedFilter, split::split_conjunction},
    OptimizeRule,
};

/// Reorders joins in the plan.
///
/// Currently just does some reordering or filters + cross joins, but will
/// support switching join sides based on statistics eventually.
#[derive(Debug, Default)]
pub struct JoinReorder {}

impl OptimizeRule for JoinReorder {
    fn optimize(
        &mut self,
        _bind_context: &mut BindContext,
        plan: LogicalOperator,
    ) -> Result<LogicalOperator> {
        let mut reorder = InnerJoinReorder::default();
        reorder.reorder(plan)
    }
}

#[derive(Debug, Default)]
struct InnerJoinReorder {
    filters: Vec<ExtractedFilter>,
    child_plans: Vec<LogicalOperator>,
}

impl InnerJoinReorder {
    // TODO: Duplicated with filter pushdown.
    fn add_filter(&mut self, expr: Expression) {
        let mut split = Vec::new();
        split_conjunction(expr, &mut split);

        self.filters
            .extend(split.into_iter().map(ExtractedFilter::from_expr))
    }

    fn reorder(&mut self, mut root: LogicalOperator) -> Result<LogicalOperator> {
        match &root {
            LogicalOperator::Filter(_) | LogicalOperator::CrossJoin(_) => {
                self.extract_filters_and_join_children(root)?;
            }
            LogicalOperator::ComparisonJoin(join) if join.node.join_type == JoinType::Inner => {
                self.extract_filters_and_join_children(root)?;
            }
            _ => {
                // Can't extract at this node, try reordering children and
                // return.
                root.modify_replace_children(&mut |child| {
                    let mut reorder = Self::default();
                    reorder.reorder(child)
                })?;
                return Ok(root);
            }
        }

        let mut join_tree = JoinTree::new(self.child_plans.drain(..), self.filters.drain(..));
        // Do the magic.
        let plan = join_tree.try_build()?;

        Ok(plan)
    }

    fn extract_filters_and_join_children(&mut self, root: LogicalOperator) -> Result<()> {
        assert!(self.filters.is_empty());
        assert!(self.child_plans.is_empty());

        let mut queue: VecDeque<_> = [root].into_iter().collect();

        while let Some(plan) = queue.pop_front() {
            match plan {
                LogicalOperator::Filter(mut filter) => {
                    self.add_filter(filter.node.filter);
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
                        for condition in join.node.conditions {
                            self.add_filter(condition.into_expression());
                        }
                        for child in join.children.drain(..) {
                            queue.push_back(child);
                        }
                    } else {
                        // Nothing we can do (yet).
                        self.child_plans.push(LogicalOperator::ComparisonJoin(join))
                    }
                }
                other => self.child_plans.push(other),
            }
        }

        Ok(())
    }
}

/// A substree of the query.
#[derive(Debug)]
struct JoinTree {
    nodes: Vec<JoinTreeNode>,
    filters: Vec<ExtractedFilter>,
    constant_filters: Vec<Expression>,
}

/// Represents a single node in the join subtree.
#[derive(Debug, Default)]
struct JoinTreeNode {
    /// If this node is valid.
    ///
    /// As we combine the nodes, into a tree with a filters in the right spot,
    /// some nodes will becoming invalid.
    valid: bool,
    /// All output refs for this node.
    output_refs: HashSet<TableRef>,
    /// The plan making up this node.
    ///
    /// If this is valid, this should never be None.
    plan: Option<LogicalOperator>,
    /// All filters that we know apply to the
    filters: Vec<Expression>,
}

impl JoinTree {
    fn new(
        plans: impl IntoIterator<Item = LogicalOperator>,
        filters: impl IntoIterator<Item = ExtractedFilter>,
    ) -> Self {
        // Initialize all nodes with empty filters and single child.
        let nodes: Vec<_> = plans
            .into_iter()
            .map(|p| JoinTreeNode {
                valid: true,
                output_refs: p.get_output_table_refs().into_iter().collect(),
                plan: Some(p),
                filters: Vec::new(),
            })
            .collect();

        // Collect all filters, then sort by table refs descending. We'll be
        // treating this vec as a stack, and the fewer the table refs, the lower
        // in the tree the filter will go.
        let mut filters: Vec<ExtractedFilter> = filters.into_iter().collect();
        filters.sort_unstable_by(|a, b| (a.tables_refs.len().cmp(&b.tables_refs.len())).reverse());

        JoinTree {
            nodes,
            filters,
            constant_filters: Vec::new(),
        }
    }

    fn try_build(&mut self) -> Result<LogicalOperator> {
        const MAX_COMBINE_STEPS: usize = 64;

        let mut step_count = 0;
        while self.try_combine_step()? {
            if step_count >= MAX_COMBINE_STEPS {
                return Err(RayexecError::new(format!(
                    "Join reorder: combine step count exceeded max: {MAX_COMBINE_STEPS}"
                )));
            }

            step_count += 1;
        }

        assert!(self.filters.is_empty());

        let mut built_nodes = self
            .nodes
            .drain(..)
            .filter_map(|n| if n.valid { Some(n) } else { None });

        let node = match built_nodes.next() {
            Some(plan) => plan,
            None => return Err(RayexecError::new("Join tree has no built nodes")),
        };

        let mut plan = match expr::and(node.filters) {
            Some(filter) => LogicalOperator::Filter(Node {
                node: LogicalFilter { filter },
                location: LocationRequirement::Any,
                children: vec![node.plan.expect("plan to be some")],
            }),
            None => node.plan.expect("plan to be some"),
        };

        // Cross join with other remaining nodes if needed.
        for right in built_nodes {
            let right = match expr::and(right.filters) {
                Some(filter) => LogicalOperator::Filter(Node {
                    node: LogicalFilter { filter },
                    location: LocationRequirement::Any,
                    children: vec![right.plan.expect("plan to be some")],
                }),
                None => right.plan.expect("plan to be some"),
            };

            plan = LogicalOperator::CrossJoin(Node {
                node: LogicalCrossJoin,
                location: LocationRequirement::Any,
                children: vec![plan, right],
            });
        }

        // Apply constant filters if needed.
        if let Some(filter) = expr::and(self.constant_filters.drain(..)) {
            plan = LogicalOperator::Filter(Node {
                node: LogicalFilter { filter },
                location: LocationRequirement::Any,
                children: vec![plan],
            });
        }

        Ok(plan)
    }

    fn try_combine_step(&mut self) -> Result<bool> {
        let filter = match self.filters.pop() {
            Some(filter) => filter,
            None => return Ok(false),
        };

        // Figure out which nodes this filter can possibly apply to.
        let node_indices: Vec<_> = self
            .nodes
            .iter()
            .enumerate()
            .filter_map(|(idx, node)| {
                if !node.valid {
                    return None;
                }

                // Only include nodes that this filter has a reference for.
                if filter.tables_refs.is_disjoint(&node.output_refs) {
                    return None;
                }

                Some(idx)
            })
            .collect();

        match node_indices.len() {
            0 => {
                if !filter.tables_refs.is_empty() {
                    // Shouldn't happen.
                    return Err(RayexecError::new(format!(
                        "Filter does not apply to any nodes in the subtree: {filter:?}"
                    )));
                }

                // Filter does not depend on any plans.
                self.constant_filters.push(filter.filter);
            }
            1 => {
                // Filter applies to just one node in the tree, not a join
                // condition.
                let idx = node_indices[0];
                self.nodes[idx].filters.push(filter.filter);
            }
            2 => {
                // We're referencing two nodes in the tree. Now we combine them
                // into an inner join.
                //
                // These takes will mark the nodes as invalid for the next call
                // (via the default impl)
                let mut left = std::mem::take(&mut self.nodes[node_indices[0]]);
                let mut right = std::mem::take(&mut self.nodes[node_indices[1]]);

                let left_refs: Vec<_> = left.output_refs.iter().copied().collect();
                let right_refs: Vec<_> = right.output_refs.iter().copied().collect();

                let extractor =
                    JoinConditionExtractor::new(&left_refs, &right_refs, JoinType::Inner);
                let mut conditions = extractor.extract(vec![filter.filter])?;

                // Extend node specific filters.
                left.filters.append(&mut conditions.left_filter);
                right.filters.append(&mut conditions.right_filter);

                // Build up left side of join.
                let mut left_plan = left.plan.take().expect("plan to be some");
                if let Some(filter) = expr::and(left.filters) {
                    left_plan = LogicalOperator::Filter(Node {
                        node: LogicalFilter { filter },
                        location: LocationRequirement::Any,
                        children: vec![left_plan],
                    });
                }

                // Build up right side;
                let mut right_plan = right.plan.take().expect("plan to be some");
                if let Some(filter) = expr::and(right.filters) {
                    right_plan = LogicalOperator::Filter(Node {
                        node: LogicalFilter { filter },
                        location: LocationRequirement::Any,
                        children: vec![right_plan],
                    });
                }

                // Now do the join.
                let join = LogicalOperator::ComparisonJoin(Node {
                    node: LogicalComparisonJoin {
                        join_type: JoinType::Inner,
                        conditions: conditions.comparisons,
                    },
                    location: LocationRequirement::Any,
                    children: vec![left_plan, right_plan],
                });

                // Push back new node.
                //
                // Next iteration will now have this node available to pick
                // from.
                self.nodes.push(JoinTreeNode {
                    valid: true,
                    output_refs: join.get_output_table_refs().into_iter().collect(),
                    plan: Some(join),
                    filters: conditions.arbitrary,
                });
            }
            _ => {
                // > 2 nodes.
                //
                // Arbitrarily cross join two of them, and push back the filter
                // to try again.
                let mut left = std::mem::take(&mut self.nodes[node_indices[0]]);
                let mut right = std::mem::take(&mut self.nodes[node_indices[1]]);

                // Build up left side of join.
                let mut left_plan = left.plan.take().expect("plan to be some");
                if let Some(filter) = expr::and(left.filters) {
                    left_plan = LogicalOperator::Filter(Node {
                        node: LogicalFilter { filter },
                        location: LocationRequirement::Any,
                        children: vec![left_plan],
                    });
                }

                // Build up right side;
                let mut right_plan = right.plan.take().expect("plan to be some");
                if let Some(filter) = expr::and(right.filters) {
                    right_plan = LogicalOperator::Filter(Node {
                        node: LogicalFilter { filter },
                        location: LocationRequirement::Any,
                        children: vec![right_plan],
                    });
                }

                let join = LogicalOperator::CrossJoin(Node {
                    node: LogicalCrossJoin,
                    location: LocationRequirement::Any,
                    children: vec![left_plan, right_plan],
                });

                self.nodes.push(JoinTreeNode {
                    valid: true,
                    output_refs: join.get_output_table_refs().into_iter().collect(),
                    plan: Some(join),
                    filters: Vec::new(),
                });

                // Note we push the filter back so that we try again with the
                // same filter on the next iteration.
                //
                // We don't put it in the join tree node since it's still a
                // candidate to be a join condition, just with one of the
                // children being a cross join.
                self.filters.push(filter)
            }
        }

        Ok(true)
    }
}

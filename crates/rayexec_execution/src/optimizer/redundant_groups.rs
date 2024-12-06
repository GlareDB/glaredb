use std::collections::{BTreeSet, HashMap};

use rayexec_error::Result;

use super::OptimizeRule;
use crate::expr::column_expr::ColumnExpr;
use crate::expr::Expression;
use crate::logical::binder::bind_context::BindContext;
use crate::logical::logical_aggregate::LogicalAggregate;
use crate::logical::operator::{LogicalOperator, Node};

/// Remove redundant groupings in the GROUP BY
#[derive(Debug, Default)]
pub struct RemoveRedundantGroups {
    /// Mapping of column expression to a new expression that should be used
    /// instead.
    column_expr_map: HashMap<ColumnExpr, Expression>,
}

impl OptimizeRule for RemoveRedundantGroups {
    fn optimize(
        &mut self,
        bind_context: &mut BindContext,
        plan: LogicalOperator,
    ) -> Result<LogicalOperator> {
        match plan {
            LogicalOperator::Aggregate(agg) => {
                // TODO: Don't know yet.
                if agg.node.grouping_set_table.is_some() {
                    //
                    unimplemented!()
                }

                //
                unimplemented!()
            }
            _ => (),
        }
        unimplemented!()
    }
}

impl RemoveRedundantGroups {
    /// Visit the aggregate node.
    ///
    /// Should not optimize its children, that's handled outside.
    fn visit_aggregate(mut agg: Node<LogicalAggregate>) -> Result<Node<LogicalAggregate>> {
        // TODO: Don't know yet.
        if agg.node.grouping_set_table.is_some() {
            return Ok(agg);
        }

        // Maps a column expr to the group index.
        let mut base_col_map: HashMap<ColumnExpr, usize> = HashMap::new();

        // Find all base columns.
        for (idx, group_expr) in agg.node.group_exprs.iter().enumerate() {
            if let Expression::Column(expr) = group_expr {
                // Only store one group index per column expr.
                if !base_col_map.contains_key(&expr) {
                    base_col_map.insert(*expr, idx);
                }
            }
        }

        // For each group, find if it references any of the extracted columns.
        //
        // None => Cannot be deduplicated.
        // Some(idx) => Can be deduplicated with the group at the given index.
        let mut base_references: Vec<Option<usize>> = vec![None; agg.node.group_exprs.len()];

        for (group_idx, group_expr) in agg.node.group_exprs.iter().enumerate() {
            let col_refs = group_expr.get_column_references();

            // TODO: We should probably also remove groups where len == 0.

            if col_refs.len() == 1 {
                let col_ref = col_refs[0];

                if let Some(base_group_idx) = base_col_map.get(&col_ref) {
                    // If this returns true, we can remove this expression as it's
                    // entirely dependent on the base column.
                    if group_expr.is_const_foldable_with_fixed_column(&col_ref) {
                        base_references[group_idx] = Some(*base_group_idx);
                    }
                }
            }
        }

        if base_references.iter().all(|idx| idx.is_none()) {
            // Nothing to deduplicate.
            return Ok(agg);
        }

        for (to_remove_idx, base_reference) in base_references.into_iter().enumerate() {
            let base_reference = match base_reference {
                Some(base) => base,
                None => continue, // Skip deduplicate.
            };

            // Update grouping sets.
            if let Some(grouping_sets) = &mut agg.node.grouping_sets {
                // Update grouping sets to have it point to the group index that
                // will be remaining after deduplication.
                for grouping_set in grouping_sets {
                    // Point to base group.
                    grouping_set.remove(&to_remove_idx);
                    grouping_set.insert(base_reference);

                    // Now update group indices larger than `group_idx` to shift
                    // down once since this group is being removed.
                    let mut new_grouping_set = BTreeSet::new();

                    for &grouping_idx in grouping_set.iter() {
                        if grouping_idx > to_remove_idx {
                            new_grouping_set.insert(grouping_idx - 1);
                        } else {
                            new_grouping_set.insert(grouping_idx);
                        }
                    }

                    *grouping_set = new_grouping_set;
                }
            }
        }

        unimplemented!()
    }
}

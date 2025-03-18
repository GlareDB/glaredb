use std::collections::{BTreeSet, HashMap};

use glaredb_error::{DbError, Result};

use super::OptimizeRule;
use crate::expr::Expression;
use crate::expr::column_expr::{ColumnExpr, ColumnReference};
use crate::logical::binder::bind_context::BindContext;
use crate::logical::logical_aggregate::LogicalAggregate;
use crate::logical::operator::{LogicalNode, LogicalOperator, Node};

/// Remove redundant groupings in the GROUP BY
#[derive(Debug, Default)]
pub struct RemoveRedundantGroups {
    /// Mapping of column references to a new expression that should be used
    /// instead.
    column_expr_map: HashMap<ColumnReference, Expression>,
}

impl OptimizeRule for RemoveRedundantGroups {
    fn optimize(
        &mut self,
        bind_context: &mut BindContext,
        mut plan: LogicalOperator,
    ) -> Result<LogicalOperator> {
        self.walk_plan(bind_context, &mut plan)?;
        Ok(plan)
    }
}

#[derive(Debug)]
struct RetainedExpression {
    /// The expression we're retaining.
    expr: Expression,
    /// Index of the expression prior to any deduplication.
    original_idx: usize,
    /// Index of the expression after deduplication.
    new_idx: usize,
}

#[derive(Debug)]
struct RemovedExpression {
    /// The expression we're removing.
    expr: Expression,
    /// Index of the expression prior to any deduplication.
    original_idx: usize,
    /// Index of the expression that this expression is considered a duplicated
    /// of, prior to any deduplication.
    base_original_idx: usize,
    /// The column reference that we saw to indicate this is a redundant group.
    column_reference: ColumnReference,
}

impl RemoveRedundantGroups {
    fn walk_plan(
        &mut self,
        bind_context: &mut BindContext,
        plan: &mut LogicalOperator,
    ) -> Result<()> {
        // TODO: There's going to be a bit of duplication for the column pruning
        // rule. Need to figure out a way to handle it all cohesively.
        //
        // Right now this will just handle a projection with the immediate child
        // being an aggregate just to see if this works.
        match plan {
            LogicalOperator::Project(proj) => {
                let mut rule = Self::default();

                proj.modify_replace_children(&mut |mut child| {
                    if let LogicalOperator::Aggregate(agg) = &mut child {
                        rule.visit_aggregate(bind_context, agg)?;
                    }
                    rule.walk_plan(bind_context, &mut child)?;

                    Ok(child)
                })?;

                rule.apply_updated_exprs(proj)?;

                Ok(())
            }
            other => {
                let mut rule = Self::default();
                other.modify_replace_children(&mut |mut child| {
                    rule.walk_plan(bind_context, &mut child)?;
                    Ok(child)
                })?;

                rule.apply_updated_exprs(other)?;

                Ok(())
            }
        }
    }

    fn apply_updated_exprs(&self, plan: &mut impl LogicalNode) -> Result<()> {
        fn inner(expr: &mut Expression, replacements: &HashMap<ColumnReference, Expression>) {
            match expr {
                Expression::Column(col) => {
                    if let Some(replace_expr) = replacements.get(&col.reference) {
                        *expr = replace_expr.clone();
                    }
                }
                other => other
                    .for_each_child_mut(&mut |child| {
                        inner(child, replacements);
                        Ok(())
                    })
                    .expect("replace to not fail"),
            }
        }

        plan.for_each_expr_mut(&mut |expr| {
            inner(expr, &self.column_expr_map);
            Ok(())
        })?;

        Ok(())
    }

    /// Visit the aggregate node.
    ///
    /// Should not optimize its children, that's handled outside.
    fn visit_aggregate(
        &mut self,
        bind_context: &mut BindContext,
        agg: &mut Node<LogicalAggregate>,
    ) -> Result<()> {
        // TODO: Don't know yet.
        if agg.node.grouping_functions_table.is_some() {
            return Ok(());
        }

        let group_table = match agg.node.group_table {
            Some(table) => table,
            None => {
                // No GROUP BY, nothing to do.
                return Ok(());
            }
        };

        // Maps a column expr to the group index.
        let mut base_col_map: HashMap<ColumnReference, usize> = HashMap::new();

        // Find all base columns.
        for (idx, group_expr) in agg.node.group_exprs.iter().enumerate() {
            if let Expression::Column(expr) = group_expr {
                // Only store one group index per column expr.
                base_col_map.entry(expr.reference).or_insert(idx);
            }
        }

        // Expressions we're keeping.
        //
        // This will remain sorted by their original indices.
        let mut retained_exprs: Vec<RetainedExpression> = Vec::new();
        // Expressions we're removing.
        let mut removed_exprs: Vec<RemovedExpression> = Vec::new();

        for (group_idx, group_expr) in agg.node.group_exprs.drain(..).enumerate() {
            let col_refs = group_expr.get_column_references();

            // TODO: We should probably also remove groups where len == 0.

            if col_refs.len() == 1 {
                let col_ref = col_refs[0];

                if let Some(&base_group_idx) = base_col_map.get(&col_ref) {
                    // If this returns true, we can remove this expression as it's
                    // entirely dependent on the base column.
                    if base_group_idx != group_idx
                        && group_expr.is_const_foldable_with_fixed_column(&col_ref)
                    {
                        removed_exprs.push(RemovedExpression {
                            expr: group_expr,
                            original_idx: group_idx,
                            base_original_idx: base_group_idx,
                            column_reference: col_ref,
                        });
                        continue;
                    }
                }
            }

            retained_exprs.push(RetainedExpression {
                expr: group_expr,
                original_idx: group_idx,
                new_idx: group_idx - removed_exprs.len(),
            });
        }

        // Maps original indices to updated indices for groups that we're
        // keeping.
        let original_to_deduplicated: HashMap<usize, usize> = retained_exprs
            .iter()
            .map(|expr| (expr.original_idx, expr.new_idx))
            .collect();

        // Maps original indices for expressions being removed to an updated
        // group expression that we should point to instead.
        let removed_to_deduplicated: HashMap<usize, usize> = removed_exprs
            .iter()
            .map(|expr| {
                let deduped_idx = original_to_deduplicated
                    .get(&expr.base_original_idx)
                    .copied()
                    .ok_or_else(|| {
                        DbError::new(format!(
                            "Missing mapping for group expr at idx {}",
                            expr.base_original_idx
                        ))
                    })?;
                Ok((expr.original_idx, deduped_idx))
            })
            .collect::<Result<_>>()?;

        // Update grouping sets.
        if let Some(grouping_sets) = &mut agg.node.grouping_sets {
            for grouping_set in grouping_sets {
                let mut new_grouping_set = BTreeSet::new();

                // Update expressions we're keeping.
                for (original, updated) in &original_to_deduplicated {
                    if grouping_set.contains(original) {
                        new_grouping_set.insert(*updated);
                    }
                }

                // Updated expression we're removing.
                //
                // This ensures we keep the same number of grouping sets even if
                // we end up with multiple groups that point to the same thing.
                //
                // E.g. `GROUP BY CUBE i, i+1` will have grouping sets that all
                // point to the same `i` column since we'll be keeping the
                // expression for `i`.
                for (original, updated) in &removed_to_deduplicated {
                    if grouping_set.contains(original) {
                        new_grouping_set.insert(*updated);
                    }
                }

                *grouping_set = new_grouping_set;
            }
        }

        // Generate updated expressions for the group exprs we're keeping. These
        // are simple column replacements.
        for retained in &retained_exprs {
            if retained.original_idx == retained.new_idx {
                // No need to update references to this expression, we didn't
                // deduplicate anything before it.
                continue;
            }

            let retained_reference = ColumnReference {
                table_scope: group_table,
                column: retained.new_idx,
            };
            let retained_column = ColumnExpr {
                reference: retained_reference,
                datatype: bind_context.get_column_type(retained_reference)?,
            };

            self.column_expr_map.insert(
                ColumnReference {
                    table_scope: group_table,
                    column: retained.original_idx,
                },
                Expression::Column(retained_column),
            );
        }

        // Generate updated expressions for references to group exprs that we're
        // removing.
        //
        // This is a bit more involved as we keep the entire expression, but
        // replace the column refs to point to retained group exprs. We can do
        // this be cause this expression won't be used in the GROUP BY, but in
        // operators that are referencing the GROUP BY.
        for removed in removed_exprs {
            let updated_retained = original_to_deduplicated
                .get(&removed.base_original_idx)
                .copied()
                .ok_or_else(|| {
                    DbError::new(format!(
                        "Missing mapping for base original idx: {}",
                        removed.base_original_idx
                    ))
                })?;

            let updated_reference = ColumnReference {
                table_scope: group_table,
                column: updated_retained,
            };
            let updated_col = ColumnExpr {
                reference: updated_reference,
                datatype: bind_context.get_column_type(updated_reference)?,
            };

            let expr = removed
                .expr
                .replace_column(removed.column_reference, &updated_col);

            self.column_expr_map.insert(
                ColumnReference {
                    table_scope: group_table,
                    column: removed.original_idx,
                },
                expr,
            );
        }

        // Update bind context.
        let table = bind_context.get_table_mut(group_table)?;
        let mut new_names = Vec::with_capacity(retained_exprs.len());
        let mut new_types = Vec::with_capacity(retained_exprs.len());

        for (idx, (name, datatype)) in table
            .column_names
            .drain(..)
            .zip(table.column_types.drain(..))
            .enumerate()
        {
            if !original_to_deduplicated.contains_key(&idx) {
                // Not a column we're retaining.
                continue;
            }

            new_names.push(name);
            new_types.push(datatype);
        }

        table.column_names = new_names;
        table.column_types = new_types;

        // Place retained expressions back on agg node.
        agg.node.group_exprs = retained_exprs
            .into_iter()
            .map(|retained| retained.expr)
            .collect();

        Ok(())
    }
}

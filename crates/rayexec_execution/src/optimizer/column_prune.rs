use std::collections::{BTreeSet, HashMap};

use rayexec_error::Result;
use tracing::warn;

use crate::{
    expr::{column_expr::ColumnExpr, Expression},
    logical::{
        binder::bind_context::{BindContext, MaterializationRef, TableRef},
        operator::{LogicalNode, LogicalOperator},
    },
};

use super::OptimizeRule;

#[derive(Debug, Default)]
pub struct ColumnPrune {
    /// Materializations we've found. Holds a boolean so we don't try to
    /// recursively collect columns (shouldn't be possible yet).
    materializations: HashMap<MaterializationRef, bool>,
    /// All column exprs we've found in the query.
    column_exprs: HashMap<TableRef, BTreeSet<usize>>,
    /// Remap of old column expression to new column expression.
    column_remap: HashMap<ColumnExpr, ColumnExpr>,
}

impl OptimizeRule for ColumnPrune {
    fn optimize(
        &mut self,
        bind_context: &mut BindContext,
        mut plan: LogicalOperator,
    ) -> Result<LogicalOperator> {
        // Find all columns we're referencing in the query
        self.collect_columns(&plan)?;

        // Update materializations hashmap so we don't recurse.
        for (_, handled) in self.materializations.iter_mut() {
            *handled = true;
        }

        // Handle all referenced materializations.
        let refs: Vec<_> = self.materializations.keys().copied().collect();
        for &mat_ref in &refs {
            let materialization = bind_context.get_materialization(mat_ref)?;
            self.collect_columns(&materialization.plan)?;
        }

        // Update scans.
        self.update_scans(bind_context, &mut plan)?;
        for &mat_ref in &refs {
            let mut orig = {
                let materialization = bind_context.get_materialization_mut(mat_ref)?;
                std::mem::replace(&mut materialization.plan, LogicalOperator::Invalid)
            };
            self.update_scans(bind_context, &mut orig)?;

            let materialization = bind_context.get_materialization_mut(mat_ref)?;
            materialization.plan = orig;
        }

        // Replace references.
        self.replace_references(bind_context, &mut plan)?;
        for &mat_ref in &refs {
            let mut orig = {
                let materialization = bind_context.get_materialization_mut(mat_ref)?;
                std::mem::replace(&mut materialization.plan, LogicalOperator::Invalid)
            };
            self.replace_references(bind_context, &mut orig)?;

            let materialization = bind_context.get_materialization_mut(mat_ref)?;
            materialization.plan = orig;
        }

        Ok(plan)
    }
}

impl ColumnPrune {
    fn collect_columns(&mut self, plan: &LogicalOperator) -> Result<()> {
        match plan {
            LogicalOperator::MaterializationScan(scan) => {
                if *self.materializations.get(&scan.node.mat).unwrap_or(&false) {
                    return Ok(());
                }
                self.materializations.insert(scan.node.mat, false);
            }
            other => {
                other.for_each_expr(&mut |expr| {
                    extract_from_expr(expr, &mut self.column_exprs);
                    Ok(())
                })?;

                for child in plan.children() {
                    self.collect_columns(child)?;
                }
            }
        }

        Ok(())
    }

    fn replace_references(
        &mut self,
        bind_context: &mut BindContext,
        plan: &mut LogicalOperator,
    ) -> Result<()> {
        if let LogicalOperator::MaterializationScan(scan) = plan {
            // TODO: Lotsa caching with the table refs for these.
            let materialization = bind_context.get_materialization_mut(scan.node.mat)?;
            let table_refs = materialization.plan.get_output_table_refs();
            materialization.table_refs = table_refs.clone();
            scan.node.table_refs = table_refs;
        }

        plan.for_each_expr_mut(&mut |expr| {
            replace_column_references(expr, &self.column_remap);
            Ok(())
        })?;

        for child in plan.children_mut() {
            self.replace_references(bind_context, child)?;
        }

        Ok(())
    }

    fn update_scans(
        &mut self,
        bind_context: &mut BindContext,
        plan: &mut LogicalOperator,
    ) -> Result<()> {
        match plan {
            LogicalOperator::Scan(scan) => {
                let cols = match self.column_exprs.remove(&scan.node.table_ref) {
                    Some(cols) => cols,
                    None => {
                        // TODO: I think we could just remove the table?
                        warn!(?scan, "Nothing referencing table");
                        return Ok(());
                    }
                };

                // Check if we're not referencing all columns. If so, we should
                // prune.
                let should_prune = scan.node.projection.iter().any(|col| !cols.contains(col));
                if !should_prune {
                    return Ok(());
                }

                // Prune by creating a new table with the pruned names and
                // types. Create a mapping of original column -> new column.

                let orig = bind_context.get_table(scan.node.table_ref)?;

                let mut pruned_names = Vec::with_capacity(cols.len());
                let mut pruned_types = Vec::with_capacity(cols.len());
                for &col_idx in &cols {
                    pruned_names.push(orig.column_names[col_idx].clone());
                    pruned_types.push(orig.column_types[col_idx].clone());
                }

                let new_ref = bind_context
                    .new_ephemeral_table_with_columns(pruned_types.clone(), pruned_names.clone())?;

                for (new_col, old_col) in cols.iter().copied().enumerate() {
                    self.column_remap.insert(
                        ColumnExpr {
                            table_scope: scan.node.table_ref,
                            column: old_col,
                        },
                        ColumnExpr {
                            table_scope: new_ref,
                            column: new_col,
                        },
                    );
                }

                // Update operator.
                scan.node.did_prune_columns = true;
                scan.node.types = pruned_types;
                scan.node.names = pruned_names;
                scan.node.table_ref = new_ref;
                scan.node.projection = cols.into_iter().collect();

                Ok(())
            }
            other => {
                for child in other.children_mut() {
                    self.update_scans(bind_context, child)?;
                }

                Ok(())
            }
        }
    }
}

fn replace_column_references(expr: &mut Expression, mapping: &HashMap<ColumnExpr, ColumnExpr>) {
    match expr {
        Expression::Column(col) => {
            if let Some(replace) = mapping.get(col).copied() {
                *col = replace;
            }
        }
        other => other
            .for_each_child_mut(&mut |child| {
                replace_column_references(child, mapping);
                Ok(())
            })
            .expect("replace to not fail"),
    }
}

fn extract_from_expr(expr: &Expression, extracted: &mut HashMap<TableRef, BTreeSet<usize>>) {
    match expr {
        Expression::Column(col) => {
            extracted
                .entry(col.table_scope)
                .and_modify(|cols| {
                    cols.insert(col.column);
                })
                .or_insert([col.column].into());
        }
        other => other
            .for_each_child(&mut |child| {
                extract_from_expr(child, extracted);
                Ok(())
            })
            .expect("extract to not fail"),
    }
}

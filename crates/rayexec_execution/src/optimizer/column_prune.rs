use std::collections::{BTreeSet, HashMap, HashSet};

use rayexec_error::{RayexecError, Result};

use super::OptimizeRule;
use crate::expr::column_expr::ColumnExpr;
use crate::expr::Expression;
use crate::logical::binder::bind_context::{BindContext, MaterializationRef};
use crate::logical::logical_project::LogicalProject;
use crate::logical::operator::{LogicalNode, LogicalOperator, Node};

/// Prunes columns from the plan, potentially pushing down projects into scans.
///
/// Note that a previous iteration of this rule assumed that table refs were
/// unique within a plan. That is not the case, particularly with CTEs as they
/// get cloned into the plan during planning without altering any table refs.
/// TPCH query 15 triggered an error due to this, but I was not actually able to
/// easily write a minimal example that could reproduce it.
#[derive(Debug, Default)]
pub struct ColumnPrune {}

impl OptimizeRule for ColumnPrune {
    fn optimize(
        &mut self,
        bind_context: &mut BindContext,
        mut plan: LogicalOperator,
    ) -> Result<LogicalOperator> {
        let mut prune_state = PruneState::new(true);
        prune_state.walk_plan(bind_context, &mut plan)?;
        Ok(plan)
    }
}

/// Walks the plan looking for magic scans referencing a given materializations
/// ref, and extract materialized columns from the scan' projection.
#[derive(Debug)]
struct MagicScanColumnExtractor {
    /// Only look at scans that match this reference.
    mat: MaterializationRef,
    /// Complete set of columns from the underlying materialized plan that the
    /// scan is referencing.
    columns: HashSet<ColumnExpr>,
}

impl MagicScanColumnExtractor {
    fn walk_plan(&mut self, plan: &LogicalOperator) -> Result<()> {
        match plan {
            LogicalOperator::MagicMaterializationScan(scan) if scan.node.mat == self.mat => {
                // Magic scan matches the materialization, get the underlying
                // columns being referenced.
                for proj in &scan.node.projections {
                    extract_column_exprs(proj, &mut self.columns);
                }
            }
            other => {
                // Otherwise just keep looking.
                for child in other.children() {
                    self.walk_plan(child)?
                }
            }
        }
        Ok(())
    }
}

/// Walks the plan to update magic scan projections to have updated column
/// expressions.
#[derive(Debug)]
struct MagicScanColumnReplacer<'a> {
    /// Only look at scans that match this reference.
    mat: MaterializationRef,
    /// Updated expressions mapping original column exprs to new expressions.
    updated: &'a HashMap<ColumnExpr, Expression>,
}

impl MagicScanColumnReplacer<'_> {
    fn walk_plan(&self, plan: &mut LogicalOperator) -> Result<()> {
        match plan {
            LogicalOperator::MagicMaterializationScan(scan) if scan.node.mat == self.mat => {
                // Magic scan matches, replace columns as necessary.
                for proj in &mut scan.node.projections {
                    replace_column_reference(proj, self.updated);
                }
            }
            other => {
                // Otherwise just keep looking.
                for child in other.children_mut() {
                    self.walk_plan(child)?
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
struct PruneState {
    /// Whether or not all columns are implicitly referenced.
    ///
    /// If this is true, then we can't prune any columns.
    implicit_reference: bool,
    /// Column references encountered so far.
    ///
    /// This get's built up as we go down the plan tree.
    current_references: HashSet<ColumnExpr>,
    /// Mapping of old column refs to new expressions that should be used in
    /// place of the old columns.
    updated_expressions: HashMap<ColumnExpr, Expression>,
}

impl PruneState {
    fn new(implicit_reference: bool) -> Self {
        PruneState {
            implicit_reference,
            current_references: HashSet::new(),
            updated_expressions: HashMap::new(),
        }
    }

    /// Create a new prune state that's initialized with column expressions
    /// found in `parent.
    ///
    /// This should be used when walking through operators that don't expose
    /// table refs from child operators (e.g. project).
    fn new_from_parent_node(parent: &impl LogicalNode, implicit_reference: bool) -> Self {
        let mut current_references = HashSet::new();

        parent
            .for_each_expr(&mut |expr| {
                extract_column_exprs(expr, &mut current_references);
                Ok(())
            })
            .expect("extract to not fail");

        PruneState {
            implicit_reference,
            current_references,
            updated_expressions: HashMap::new(),
        }
    }

    /// Replaces and outdated column refs in the plan at this node.
    fn apply_updated_expressions(&self, plan: &mut impl LogicalNode) -> Result<()> {
        plan.for_each_expr_mut(&mut |expr| {
            replace_column_reference(expr, &self.updated_expressions);
            Ok(())
        })
    }

    /// Walk the plan.
    ///
    /// 1. Collect columns in use on the way down.
    /// 2. Reach a node we can't push through, replace projects as necessary.
    /// 3. Replace column references with updated references on the way up.
    fn walk_plan(
        &mut self,
        bind_context: &mut BindContext,
        plan: &mut LogicalOperator,
    ) -> Result<()> {
        // TODO: Implement this. It'd let us remove lateral joins from the plan in cases
        // where only the output of the right side is projected out.
        //
        // E.g. `SELECT u.* FROM my_table t, unnest(t.a) u` would let us remove
        // the left side as it would only be feeding into the `unnest`.
        //
        // // Check if this is a magic join first, as we might be able to remove it
        // // entirely.
        // //
        // // We can remove the join if:
        // //
        // // - We're not referencing anything from the left side in any of the
        // //   parent nodes.
        // // - Join type is INNER
        // if let LogicalOperator::MagicJoin(join) = plan {
        //     if join.node.join_type == JoinType::Inner && !self.implicit_reference {
        //         match join.get_nth_child(0)? {
        //             LogicalOperator::MaterializationScan(scan) => {
        //                 let mat_plan = bind_context.get_materialization_mut(scan.node.mat)?;

        //                 let plan_references_left = self
        //                     .current_references
        //                     .iter()
        //                     .any(|col_expr| mat_plan.table_refs.contains(&col_expr.table_scope));

        //                 if !plan_references_left {
        //                     // We can remove the left! Update the plan the
        //                     // just be the right child and continue pruning.
        //                     let [_left, right] = join.take_two_children_exact()?;
        //                     *plan = right;

        //                     // Decrement scan count. We should be able to
        //                     // remove it entirely if count == 1.
        //                     mat_plan.scan_count -= 1;

        //                     // And now just walk the updated plan.
        //                     self.walk_plan(bind_context, plan)?;
        //                     self.apply_updated_expressions(plan)?;

        //                     return Ok(());
        //                 }
        //             }
        //             other => {
        //                 return Err(RayexecError::new(format!(
        //                     "unexpected left child for magic join: {other:?}"
        //                 )))
        //             }
        //         }
        //     }
        // }

        // Extract columns reference in this plan.
        //
        // Note that this may result in references tracked that we don't care
        // about, for example when at a 'project' node. We'll be creating a new
        // state when walking the child of a project, so these extra references
        // don't matter.
        //
        // The alternative could be to do this more selectively, but doesn't
        // seem worthwhile.
        plan.for_each_expr(&mut |expr| {
            extract_column_exprs(expr, &mut self.current_references);
            Ok(())
        })?;

        // Handle special node logic.
        //
        // This match determines which nodes can have projections pushed down
        // through, and which can't. The default case assumes we can't push down
        // projections.
        match plan {
            LogicalOperator::MagicJoin(join) => {
                // Left child is materialization, right child is normal plan
                // with some number of magic scans.
                //
                // Push down on both sides:
                //
                // 1. Extract all column references to the materialized plan on
                //    the right. This should get us the complete set of column
                //    exprs that are referenced.
                // 2. Prune columns from the materialized left child.
                // 3. Apply any possible updated column expressions to magic
                //    scans on the right.
                // 4. Do normal column pruning on the right.

                // Extract the columns from the right.
                let mut extractor = MagicScanColumnExtractor {
                    mat: join.node.mat_ref,
                    columns: HashSet::new(),
                };
                extractor.walk_plan(join.get_nth_child(1)?)?;

                // Combine extracted columns with currently seen columns.
                self.current_references.extend(&extractor.columns);

                // Now push down into the left child.
                match join.get_nth_child_mut(0)? {
                    LogicalOperator::MaterializationScan(scan) => {
                        let mut mat_plan = bind_context
                            .get_materialization_mut(scan.node.mat)?
                            .plan
                            .take();

                        self.walk_plan(bind_context, &mut mat_plan)?;

                        // Replace materialized plan.
                        let table_refs = mat_plan.get_output_table_refs(bind_context);
                        let mat = bind_context.get_materialization_mut(scan.node.mat)?;
                        mat.table_refs = table_refs;
                        mat.plan = mat_plan;
                    }
                    other => {
                        return Err(RayexecError::new(format!(
                            "unexpected left child for magic join: {other:?}"
                        )))
                    }
                }

                // Now update magic scans as projections might have been
                // inserted/replaced in the materialized plan.
                let replacer = MagicScanColumnReplacer {
                    mat: join.node.mat_ref,
                    updated: &self.updated_expressions,
                };
                replacer.walk_plan(join.get_nth_child_mut(1)?)?;

                // Now just do normal column pruning for the right child.
                self.walk_plan(bind_context, join.get_nth_child_mut(1)?)?;
                self.apply_updated_expressions(join)?;
            }
            LogicalOperator::MagicMaterializationScan(_) => {
                // Nothing to do. Pushdown logic should have happened in the
                // magic join match.
            }
            LogicalOperator::MaterializationScan(_) => {
                // TODO: Normal pruning, all columns implicitly referenced.
            }
            LogicalOperator::Project(project) => {
                // First try to flatten with child projection.
                try_flatten_projection(project)?;

                // Now check if we're actually referencing everything in the
                // projection.
                let proj_references: HashSet<_> = self
                    .current_references
                    .iter()
                    .filter(|col_expr| col_expr.table_scope == project.node.projection_table)
                    .copied()
                    .collect();

                // Special case for if this projection is just a pass through.
                if !self.implicit_reference && projection_is_passthrough(project, bind_context)? {
                    // New reference set we'll pass to child.
                    let mut child_references = HashSet::new();
                    let mut old_references = HashMap::new();

                    for (col_idx, projection) in project.node.projections.iter().enumerate() {
                        let old_column = ColumnExpr {
                            table_scope: project.node.projection_table,
                            column: col_idx,
                        };

                        if !proj_references.contains(&old_column) {
                            // Column not part of expression we're replacing nor
                            // expression we'll want to keep in the child.
                            continue;
                        }

                        let child_col = match projection {
                            Expression::Column(col) => *col,
                            other => {
                                return Err(RayexecError::new(format!(
                                    "Unexpected expression: {other}"
                                )))
                            }
                        };

                        child_references.insert(child_col);

                        // Map projection back to old column reference.
                        old_references.insert(child_col, old_column);
                    }

                    // Replace project plan with its child.
                    let mut child = project.take_one_child_exact()?;

                    let mut child_prune = PruneState {
                        implicit_reference: false,
                        current_references: child_references,
                        updated_expressions: HashMap::new(),
                    };
                    child_prune.walk_plan(bind_context, &mut child)?;

                    // Since we're removing the projection, no need to apply any
                    // changes here, but we'll need to propogate them up.
                    for (child_col, old_col) in old_references {
                        match child_prune.updated_expressions.get(&child_col) {
                            Some(updated) => {
                                // Map old column to updated child column.
                                self.updated_expressions.insert(old_col, updated.clone());
                            }
                            None => {
                                // Child didn't change, map old column to child
                                // column.
                                self.updated_expressions
                                    .insert(old_col, Expression::Column(child_col));
                            }
                        }
                    }

                    // Drop project, replace with child.
                    *plan = child;

                    // And we're done, project no longer part of plan.
                    return Ok(());
                }

                // Only create an updated projection if we're actually pruning
                // columns.
                //
                // If projection references is empty, then I'm not really sure.
                // Just skip for now.
                if !self.implicit_reference
                    && proj_references.len() != project.node.projections.len()
                    && !proj_references.is_empty()
                {
                    let mut new_proj_mapping: Vec<(ColumnExpr, Expression)> =
                        Vec::with_capacity(proj_references.len());

                    for (col_idx, projection) in project.node.projections.iter().enumerate() {
                        let old_column = ColumnExpr {
                            table_scope: project.node.projection_table,
                            column: col_idx,
                        };

                        if !proj_references.contains(&old_column) {
                            // Column not used, omit from the new projection
                            // we're building.
                            continue;
                        }

                        new_proj_mapping.push((old_column, projection.clone()));
                    }

                    // Generate the new table ref.
                    let table_ref =
                        bind_context.clone_to_new_ephemeral_table(project.node.projection_table)?;

                    // Generate the new projection, inserting updated
                    // expressions into the state.
                    let mut new_projections = Vec::with_capacity(new_proj_mapping.len());
                    for (col_idx, (old_column, projection)) in
                        new_proj_mapping.into_iter().enumerate()
                    {
                        new_projections.push(projection);

                        self.updated_expressions.insert(
                            old_column,
                            Expression::Column(ColumnExpr {
                                table_scope: table_ref,
                                column: col_idx,
                            }),
                        );
                    }

                    // Update this node.
                    project.node = LogicalProject {
                        projections: new_projections,
                        projection_table: table_ref,
                    };
                }

                // Now walk children using new prune state.
                let mut child_prune = PruneState::new_from_parent_node(project, false);
                for child in &mut project.children {
                    child_prune.walk_plan(bind_context, child)?;
                }
                child_prune.apply_updated_expressions(project)?;
            }
            LogicalOperator::Scan(scan) => {
                // BTree since we make the guarantee projections are ordered in
                // the scan.
                let mut cols: BTreeSet<_> = self
                    .current_references
                    .iter()
                    .filter_map(|col_expr| {
                        if col_expr.table_scope == scan.node.table_ref {
                            Some(col_expr.column)
                        } else {
                            None
                        }
                    })
                    .collect();

                // If we have an empty column list, then we're likely just
                // checking for the existence of rows. So just always include at
                // least one to make things easy for us.
                if cols.is_empty() {
                    cols.insert(
                        scan.node
                            .projection
                            .first()
                            .copied()
                            .ok_or_else(|| RayexecError::new("Scan references no columns"))?,
                    );
                }

                // Check if we're not referencing all columns. If so, we should
                // prune.
                let should_prune = scan.node.projection.iter().any(|col| !cols.contains(col));
                if !self.implicit_reference && should_prune {
                    // Prune by creating a new table with the pruned names and
                    // types. Create a mapping of original column -> new column.
                    let orig = bind_context.get_table(scan.node.table_ref)?;

                    // We manually pull out the original column name for the
                    // sake of a readable EXPLAIN instead of going with
                    // generated names.
                    let mut pruned_names = Vec::with_capacity(cols.len());
                    let mut pruned_types = Vec::with_capacity(cols.len());
                    for &col_idx in &cols {
                        pruned_names.push(orig.column_names[col_idx].clone());
                        pruned_types.push(orig.column_types[col_idx].clone());
                    }

                    let new_ref = bind_context.new_ephemeral_table_with_columns(
                        pruned_types.clone(),
                        pruned_names.clone(),
                    )?;

                    for (new_col, old_col) in cols.iter().copied().enumerate() {
                        self.updated_expressions.insert(
                            ColumnExpr {
                                table_scope: scan.node.table_ref,
                                column: old_col,
                            },
                            Expression::Column(ColumnExpr {
                                table_scope: new_ref,
                                column: new_col,
                            }),
                        );
                    }

                    // Update operator.
                    scan.node.did_prune_columns = true;
                    scan.node.types = pruned_types;
                    scan.node.names = pruned_names;
                    scan.node.table_ref = new_ref;
                    scan.node.projection = cols.into_iter().collect();
                }
            }
            LogicalOperator::Aggregate(agg) => {
                // Can't push down through aggregate, but we don't need to
                // assume everything is implicitly referenced for the children.
                let mut child_prune = PruneState::new_from_parent_node(agg, false);
                for child in &mut agg.children {
                    child_prune.walk_plan(bind_context, child)?;
                }
                child_prune.apply_updated_expressions(agg)?;
            }
            LogicalOperator::Filter(_) => {
                // Can push through filter.
                for child in plan.children_mut() {
                    self.walk_plan(bind_context, child)?;
                }
                self.apply_updated_expressions(plan)?;
            }
            LogicalOperator::Order(_) => {
                // Can push through order by.
                for child in plan.children_mut() {
                    self.walk_plan(bind_context, child)?;
                }
                self.apply_updated_expressions(plan)?;
            }
            LogicalOperator::Limit(_) => {
                // Can push through limit.
                for child in plan.children_mut() {
                    self.walk_plan(bind_context, child)?;
                }
                self.apply_updated_expressions(plan)?;
            }
            LogicalOperator::CrossJoin(_)
            | LogicalOperator::ComparisonJoin(_)
            | LogicalOperator::ArbitraryJoin(_) => {
                // All joins good to push through.
                for child in plan.children_mut() {
                    self.walk_plan(bind_context, child)?;
                }
                self.apply_updated_expressions(plan)?;
            }
            other => {
                // For all other plans, we take a conservative approach and not
                // push projections down through this node, but instead just
                // start working on the child plan.
                //
                // The child prune state is initialized from expressions at this
                // level.

                let mut child_prune = PruneState::new(true);
                other.for_each_expr(&mut |expr| {
                    extract_column_exprs(expr, &mut child_prune.current_references);
                    Ok(())
                })?;

                for child in other.children_mut() {
                    child_prune.walk_plan(bind_context, child)?;
                }

                // Note we apply from the child prune state since that's what's
                // actually holding the updated expressions that this node
                // should reference.
                child_prune.apply_updated_expressions(other)?;
            }
        }

        Ok(())
    }
}

/// Check if this project is just a simple pass through projection for its
/// child, and not actually needed.
///
/// A project is passthrough if it contains only column expressions with the
/// first expression starting at column 0 and every subsequent expression being
/// incremented by 1 up to num_cols
fn projection_is_passthrough(
    proj: &Node<LogicalProject>,
    bind_context: &BindContext,
) -> Result<bool> {
    let child_ref = match proj
        .get_one_child_exact()?
        .get_output_table_refs(bind_context)
        .first()
    {
        Some(table_ref) => *table_ref,
        None => return Ok(false),
    };

    for (check_idx, expr) in proj.node.projections.iter().enumerate() {
        let col = match expr {
            Expression::Column(col) => col,
            _ => return Ok(false),
        };

        if col.table_scope != child_ref {
            return Ok(false);
        }

        if col.column != check_idx {
            return Ok(false);
        }
    }

    Ok(true)
}

/// Recursively try to flatten this projection into a child projection.
///
/// If the projection's child is not a projection, nothing it done.
///
/// This does not change the table ref of this projection, and all column
/// references that reference this projection remain valid.
fn try_flatten_projection(current: &mut Node<LogicalProject>) -> Result<()> {
    assert_eq!(1, current.children.len());

    if !current.children[0].is_project() {
        // Not a project, nothing to do.
        return Ok(());
    }

    let mut child_projection = match current.take_one_child_exact()? {
        LogicalOperator::Project(project) => project,
        _ => unreachable!("operator has to be a project"),
    };

    // Try flattening child project first.
    try_flatten_projection(&mut child_projection)?;

    // Generate old -> new expression map from the child. We'll walk the parent
    // expression and just replace the old references.
    let expr_map: HashMap<ColumnExpr, Expression> = child_projection
        .node
        .projections
        .into_iter()
        .enumerate()
        .map(|(col_idx, expr)| {
            (
                ColumnExpr {
                    table_scope: child_projection.node.projection_table,
                    column: col_idx,
                },
                expr,
            )
        })
        .collect();

    current.for_each_expr_mut(&mut |expr| {
        replace_column_reference(expr, &expr_map);
        Ok(())
    })?;

    // Set this projection's children the child projection's children.
    current.children = child_projection.children;

    Ok(())
}

/// Replace all column references in the expression map with the associated
/// expression.
fn replace_column_reference(expr: &mut Expression, mapping: &HashMap<ColumnExpr, Expression>) {
    match expr {
        Expression::Column(col) => {
            if let Some(replace) = mapping.get(col) {
                *expr = replace.clone()
            }
        }
        other => other
            .for_each_child_mut(&mut |child| {
                replace_column_reference(child, mapping);
                Ok(())
            })
            .expect("replace to not fail"),
    }
}

fn extract_column_exprs(expr: &Expression, refs: &mut HashSet<ColumnExpr>) {
    match expr {
        Expression::Column(col) => {
            refs.insert(*col);
        }
        other => other
            .for_each_child(&mut |child| {
                extract_column_exprs(child, refs);
                Ok(())
            })
            .expect("extract not to fail"),
    }
}

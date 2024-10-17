use std::collections::{BTreeSet, HashMap, HashSet};

use rayexec_error::{RayexecError, Result};

use super::OptimizeRule;
use crate::expr::column_expr::ColumnExpr;
use crate::expr::Expression;
use crate::logical::binder::bind_context::BindContext;
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
                    let table_ref = bind_context.new_ephemeral_table_from_expressions(
                        "__generated_pruned_projection",
                        new_proj_mapping.iter().map(|(_, expr)| expr),
                    )?;

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
            | LogicalOperator::MagicJoin(_)
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

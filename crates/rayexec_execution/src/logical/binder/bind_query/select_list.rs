use std::collections::HashMap;

use rayexec_error::{RayexecError, Result};
use rayexec_parser::ast::{self};

use super::bind_group_by::BoundGroupBy;
use super::select_expr_expander::ExpandedSelectExpr;
use crate::expr::column_expr::ColumnExpr;
use crate::expr::Expression;
use crate::logical::binder::bind_context::{BindContext, BindScopeRef, TableRef};
use crate::logical::binder::column_binder::DefaultColumnBinder;
use crate::logical::binder::expr_binder::{BaseExpressionBinder, RecursionContext};
use crate::logical::resolver::resolve_context::ResolveContext;
use crate::logical::resolver::ResolvedMeta;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Preprojection {
    pub table: TableRef,
    pub expressions: Vec<Expression>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrunedProjectionTable {
    /// Table containing just column references.
    pub table: TableRef,
    /// Column expressions containing references to the original expanded select
    /// expressions.
    pub expressions: Vec<Expression>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundSelectList {
    /// Optional pruned table to use at the end of select planning.
    ///
    /// Is Some when additional columns are added to the select list for ORDER
    /// BY and GROUP BY. The pruned table serves to remove those from the final
    /// output.
    pub pruned: Option<PrunedProjectionTable>,
    /// Table containing columns for projections
    pub projections_table: TableRef,
    /// Projection expressions. May contain additional expressions for use with
    /// ORDER BY and GROUP BY.
    pub projections: Vec<Expression>,
    /// Number of columns that this select should output.
    ///
    /// If less than length of projections, extra columns need to be omitted.
    pub output_column_count: usize,
    /// Table containing columns for aggregates.
    pub aggregates_table: TableRef,
    /// All extracted aggregates.
    pub aggregates: Vec<Expression>,
    /// Table containing columns for windows.
    pub windows_table: TableRef,
    /// All extracted windows.
    pub windows: Vec<Expression>,
}

#[derive(Debug)]
pub struct SelectList {
    /// The table scope that expressions referencing columns in the select list
    /// should bind to.
    ///
    /// Remains empty during binding.
    pub projections_table: TableRef,
    /// Mapping from explicit user-provided alias to column index in the output.
    pub alias_map: HashMap<String, usize>,
    /// Expanded projections that will be shown in the output.
    pub projections: Vec<Expression>,
    /// Projections that are appended to the right of the output projects.
    ///
    /// This is for appending expressions used for ORDER BY and GROUP BY.
    pub appended: Vec<Expression>,
}

impl SelectList {
    pub fn try_new(
        bind_ref: BindScopeRef,
        bind_context: &mut BindContext,
        resolve_context: &ResolveContext,
        projections: Vec<ExpandedSelectExpr>,
    ) -> Result<Self> {
        let mut alias_map = HashMap::new();

        // Track aliases to allow referencing them in GROUP BY and ORDER BY.
        for (idx, projection) in projections.iter().enumerate() {
            if let Some(alias) = projection.get_alias() {
                alias_map.insert(alias.to_string(), idx);
            }
        }

        // Generate column names from ast expressions.
        let mut names = projections
            .iter()
            .map(|expr| {
                Ok(match expr {
                    ExpandedSelectExpr::Expr { expr, .. } => match expr {
                        ast::Expr::Ident(ident) => ident.as_normalized_string(),
                        ast::Expr::CompoundIdent(idents) => idents
                            .last()
                            .map(|i| i.as_normalized_string())
                            .unwrap_or_else(|| "?column?".to_string()),
                        ast::Expr::Function(func) => {
                            let (func, _) =
                                resolve_context.functions.try_get_bound(func.reference)?;
                            func.name().to_string()
                        }
                        _ => "?column?".to_string(),
                    },
                    ExpandedSelectExpr::Column { name, .. } => name.clone(),
                })
            })
            .collect::<Result<Vec<_>>>()?;

        // Update with user provided aliases
        //
        // TODO: This should be updated in finalize, GROUP BY may reference an
        // unaliased column.
        for (alias, idx) in &alias_map {
            names[*idx] = alias.clone();
        }

        // Bind the expressions.
        let expr_binder = BaseExpressionBinder::new(bind_ref, resolve_context);
        let mut exprs = Vec::with_capacity(projections.len());
        for proj in projections {
            match proj {
                ExpandedSelectExpr::Expr { expr, .. } => {
                    let expr = expr_binder.bind_expression(
                        bind_context,
                        &expr,
                        &mut DefaultColumnBinder,
                        RecursionContext {
                            allow_windows: true,
                            allow_aggregates: true,
                            is_root: true,
                        },
                    )?;
                    exprs.push(expr);
                }
                ExpandedSelectExpr::Column { expr, .. } => {
                    exprs.push(Expression::Column(expr));
                }
            }
        }

        let types = exprs
            .iter()
            .map(|expr| expr.datatype(bind_context))
            .collect::<Result<Vec<_>>>()?;

        // Create table with columns. Now things can bind to the select list if
        // needed (ORDERY BY, GROUP BY).
        let projections_table = bind_context.new_ephemeral_table_with_columns(types, names)?;

        Ok(SelectList {
            projections_table,
            alias_map,
            projections: exprs,
            appended: Vec::new(),
        })
    }

    /// Finalizes the select list, producing a bound variant.
    ///
    /// This will extract aggregates from the list, placing them in their own
    /// table, and add pruning projections if needed.
    pub fn finalize(
        mut self,
        bind_context: &mut BindContext,
        mut group_by: Option<&mut BoundGroupBy>,
    ) -> Result<BoundSelectList> {
        // Extract aggregates and windows into separate tables.
        let aggregates_table = bind_context.new_ephemeral_table()?;
        let windows_table = bind_context.new_ephemeral_table()?;

        let mut aggregates = Vec::new();
        let mut windows = Vec::new();

        for expr in &mut self.projections {
            Self::extract_aggregates(aggregates_table, bind_context, expr, &mut aggregates)?;
            Self::extract_windows(windows_table, bind_context, expr, &mut windows)?;
        }
        for expr in &mut self.appended {
            Self::extract_aggregates(aggregates_table, bind_context, expr, &mut aggregates)?;
            Self::extract_windows(windows_table, bind_context, expr, &mut windows)?;
        }

        // Have projections point to the GROUP BY instead of the other way
        // around.
        if let Some(group_by) = group_by.as_mut() {
            self.update_group_by_dependencies(group_by)?;
        }

        self.verify_column_references(bind_context, aggregates_table, &aggregates, group_by)?;

        // If we had appended column, ensure we have a pruned table that only
        // contains the original projections.
        let pruned_table = if !self.appended.is_empty() {
            let len = self.projections.len();

            // Move appended expressions into the projections list.
            self.projections.append(&mut self.appended);

            let projections_table = bind_context.get_table(self.projections_table)?;
            let pruned_table_ref = bind_context.new_ephemeral_table_with_columns(
                projections_table
                    .column_types
                    .iter()
                    .take(len)
                    .cloned()
                    .collect(),
                projections_table
                    .column_names
                    .iter()
                    .take(len)
                    .cloned()
                    .collect(),
            )?;

            // Project out only expressions in the original select list.
            let expressions = (0..len)
                .map(|idx| {
                    Expression::Column(ColumnExpr {
                        table_scope: self.projections_table,
                        column: idx,
                    })
                })
                .collect();

            Some(PrunedProjectionTable {
                table: pruned_table_ref,
                expressions,
            })
        } else {
            None
        };

        Ok(BoundSelectList {
            pruned: pruned_table,
            projections_table: self.projections_table,
            output_column_count: self.projections.len(),
            projections: self.projections,
            aggregates_table,
            aggregates,
            windows_table,
            windows,
        })
    }

    /// Verify that all column expressions in the project list point to either
    /// an aggregate or group by expression.
    fn verify_column_references(
        &self,
        bind_context: &BindContext,
        agg_table: TableRef,
        aggs: &[Expression],
        group_by: Option<&mut BoundGroupBy>,
    ) -> Result<()> {
        if aggs.is_empty() && group_by.is_none() {
            return Ok(());
        }

        let [t1, t2] = match group_by {
            Some(group_by) => [agg_table, group_by.group_table],
            None => [agg_table, agg_table], // Using agg table twice just to make below logic easier.
        };

        fn inner(
            bind_context: &BindContext,
            expr: &Expression,
            [t1, t2]: [TableRef; 2],
        ) -> Result<()> {
            match expr {
                Expression::Column(col) => {
                    if col.table_scope != t1 && col.table_scope != t2 {
                        let (col_name, _) =
                            bind_context.get_column_info(col.table_scope, col.column)?;
                        return Err(RayexecError::new(format!("Column '{col_name}' must appear in the GROUP BY clause or be used in an aggregate function")));
                    }
                }
                other => other.for_each_child(&mut |child| inner(bind_context, child, [t1, t2]))?,
            }
            Ok(())
        }

        for expr in self.projections.iter().chain(&self.appended) {
            inner(bind_context, expr, [t1, t2])?
        }

        Ok(())
    }

    /// Extracts aggregates from `expression` into `aggregates`.
    fn extract_aggregates(
        aggregates_table: TableRef,
        bind_context: &mut BindContext,
        expression: &mut Expression,
        aggregates: &mut Vec<Expression>,
    ) -> Result<()> {
        if let Expression::Aggregate(agg) = expression {
            // Replace the aggregate in the projections list with a column
            // reference that points to the extracted aggregate.
            let datatype = agg.datatype(bind_context)?;
            let col_idx = bind_context.push_column_for_table(
                aggregates_table,
                "__generated_agg_ref",
                datatype,
            )?;
            let agg = std::mem::replace(
                expression,
                Expression::Column(ColumnExpr {
                    table_scope: aggregates_table,
                    column: col_idx,
                }),
            );

            aggregates.push(agg);
            return Ok(());
        }

        expression.for_each_child_mut(&mut |expr| {
            Self::extract_aggregates(aggregates_table, bind_context, expr, aggregates)
        })?;

        Ok(())
    }

    /// Extracts windows from `expression` into `windows`.
    fn extract_windows(
        windows_table: TableRef,
        bind_context: &mut BindContext,
        expression: &mut Expression,
        windows: &mut Vec<Expression>,
    ) -> Result<()> {
        if let Expression::Window(window) = expression {
            // Replace the window in the projections list with a column
            // reference that points to the extracted aggregate.
            let datatype = window.datatype(bind_context)?;
            let col_idx = bind_context.push_column_for_table(
                windows_table,
                "__generated_window_ref",
                datatype,
            )?;
            let agg = std::mem::replace(
                expression,
                Expression::Column(ColumnExpr {
                    table_scope: windows_table,
                    column: col_idx,
                }),
            );

            windows.push(agg);
            return Ok(());
        }

        expression.for_each_child_mut(&mut |expr| {
            Self::extract_windows(windows_table, bind_context, expr, windows)
        })?;

        Ok(())
    }

    /// Appends an expression to the select list.
    pub fn append_expression(
        &mut self,
        bind_context: &mut BindContext,
        expr: Expression,
    ) -> Result<ColumnExpr> {
        let datatype = expr.datatype(bind_context)?;
        self.appended.push(expr);
        let idx =
            bind_context.push_column_for_table(self.projections_table, "__appended", datatype)?;

        Ok(ColumnExpr {
            table_scope: self.projections_table,
            column: idx,
        })
    }

    /// Try to get a column by a user-provided alias.
    pub fn column_by_user_alias(&self, ident: &ast::Ident) -> Option<ColumnExpr> {
        let name = ident.as_normalized_string();

        // Check user provided alias first.
        if let Some(idx) = self.alias_map.get(&name) {
            return Some(ColumnExpr {
                table_scope: self.projections_table,
                column: *idx,
            });
        }

        None
    }

    /// Try to get a column by column ordinal.
    pub fn column_by_ordinal(
        &self,
        lit: &ast::Literal<ResolvedMeta>,
    ) -> Result<Option<ColumnExpr>> {
        if let ast::Literal::Number(s) = lit {
            let n = s
                .parse::<i64>()
                .map_err(|_| RayexecError::new(format!("Failed to parse '{s}' into a number")))?;
            if n < 1 || n as usize > self.projections.len() {
                return Err(RayexecError::new(format!(
                    "Column out of range, expected 1 - {}",
                    self.projections.len()
                )))?;
            }

            return Ok(Some(ColumnExpr {
                table_scope: self.projections_table,
                column: n as usize - 1,
            }));
        }
        Ok(None)
    }

    /// Updates expressions in the select list and bound group to ensure the
    /// select list depends on columns in the group by, and not the other way
    /// around.
    ///
    /// During GROUP BY binding, the group by may reference columns in the
    /// select via alias or ordinal. We swap the expressions such that the
    /// select list references the group by.
    fn update_group_by_dependencies(&mut self, group_by: &mut BoundGroupBy) -> Result<()> {
        // Update group expressions to be the base for any aliased expressions.
        for (idx, expr) in group_by.expressions.iter_mut().enumerate() {
            if let Expression::Column(col) = expr {
                if col.table_scope == self.projections_table {
                    let proj_expr = self.projections.get_mut(col.column).ok_or_else(|| {
                        RayexecError::new(format!("Missing projection column: {col}"))
                    })?;

                    // Point projection to group by expression, replace group by
                    // expression with original expression.
                    let orig = std::mem::replace(
                        proj_expr,
                        Expression::Column(ColumnExpr {
                            table_scope: group_by.group_table,
                            column: idx,
                        }),
                    );

                    *expr = orig;
                }
            }
        }

        fn update_projection_expr(
            group_by_expr: &Expression,
            group_by_col: ColumnExpr,
            expr: &mut Expression,
        ) -> Result<()> {
            if expr == group_by_expr {
                *expr = Expression::Column(group_by_col);
                return Ok(());
            }

            expr.for_each_child_mut(&mut |child| {
                update_projection_expr(group_by_expr, group_by_col, child)
            })
        }

        // Now update all columns in the select list to references to the GROUP
        // BY expressions.
        for (idx, group_by_expr) in group_by.expressions.iter().enumerate() {
            let group_by_col = ColumnExpr {
                table_scope: group_by.group_table,
                column: idx,
            };

            for expr in self.projections.iter_mut().chain(self.appended.iter_mut()) {
                update_projection_expr(group_by_expr, group_by_col, expr)?;
            }
        }

        Ok(())
    }

    pub fn get_projection(&mut self, idx: usize) -> Result<&Expression> {
        self.projections
            .get(idx)
            .ok_or_else(|| RayexecError::new(format!("Missing projection at index {idx}")))
    }

    pub fn get_projection_mut(&mut self, idx: usize) -> Result<&mut Expression> {
        self.projections
            .get_mut(idx)
            .ok_or_else(|| RayexecError::new(format!("Missing projection at index {idx}")))
    }
}

use rayexec_error::{RayexecError, Result};
use rayexec_parser::ast;

use super::bind_group_by::BoundGroupBy;
use super::bind_select_list::SelectListBinder;
use super::select_list::SelectList;
use crate::expr::column_expr::ColumnExpr;
use crate::expr::Expression;
use crate::logical::binder::bind_context::{BindContext, BindScopeRef, TableRef};
use crate::logical::binder::column_binder::DefaultColumnBinder;
use crate::logical::binder::expr_binder::{BaseExpressionBinder, RecursionContext};
use crate::logical::resolver::resolve_context::ResolveContext;
use crate::logical::resolver::ResolvedMeta;

#[derive(Debug)]
pub struct HavingBinder<'a> {
    current: BindScopeRef,
    resolve_context: &'a ResolveContext,
}

impl<'a> HavingBinder<'a> {
    pub fn new(current: BindScopeRef, resolve_context: &'a ResolveContext) -> Self {
        HavingBinder {
            current,
            resolve_context,
        }
    }

    pub fn bind(
        &mut self,
        bind_context: &mut BindContext,
        select_list: &mut SelectList,
        group_by: Option<&BoundGroupBy>,
        having: ast::Expr<ResolvedMeta>,
    ) -> Result<Expression> {
        let mut expr = BaseExpressionBinder::new(self.current, self.resolve_context)
            .bind_expression(
                bind_context,
                &having,
                &mut DefaultColumnBinder,
                RecursionContext {
                    allow_windows: false,
                    allow_aggregates: true,
                    is_root: true,
                },
            )?;

        // Extract out the aggregates from the expression.
        SelectListBinder::extract_aggregates(
            select_list.aggregates_table,
            bind_context,
            &mut expr,
            &mut select_list.aggregates,
        )?;

        // Update expression to ensure it points to the GROUP BY.
        Self::update_group_by_dependencies(select_list.aggregates_table, &mut expr, group_by)?;

        Ok(expr)
    }

    /// Update the expression to point to the GROUP BY as necessary.
    ///
    /// Errors if we encounter a column expression that doesn't point to the
    /// aggregates table or if the column can't be found in the group by.
    fn update_group_by_dependencies(
        agg_table: TableRef,
        expr: &mut Expression,
        group_by: Option<&BoundGroupBy>,
    ) -> Result<()> {
        match group_by {
            Some(group_by) => {
                fn update_expr(
                    agg_table: TableRef,
                    group_by_expr: &Expression,
                    group_by_col: ColumnExpr,
                    expr: &mut Expression,
                ) -> Result<()> {
                    if expr == group_by_expr {
                        *expr = Expression::Column(group_by_col);
                        return Ok(());
                    }

                    if let Expression::Column(col) = expr {
                        if col.table_scope != agg_table {
                            return Err(RayexecError::new(format!(
                                "'{expr}' contains columns not found in GROUP BY"
                            )));
                        }
                    }

                    expr.for_each_child_mut(&mut |child| {
                        update_expr(agg_table, group_by_expr, group_by_col, child)
                    })
                }

                // Update expression with references to the group by.
                for (idx, group_by_expr) in group_by.expressions.iter().enumerate() {
                    let group_by_col = ColumnExpr {
                        table_scope: group_by.group_table,
                        column: idx,
                    };

                    update_expr(agg_table, group_by_expr, group_by_col, expr)?;
                }

                Ok(())
            }
            None => {
                // Just need to check that the only table ref we have is the one
                // pointing to the aggregates table.
                let refs = expr.get_table_references();
                if refs.is_empty() || (refs.len() == 1 && refs.contains(&agg_table)) {
                    Ok(())
                } else {
                    Err(RayexecError::new(format!(
                        "'{expr}' contains columns not found in GROUP BY"
                    )))
                }
            }
        }
    }
}

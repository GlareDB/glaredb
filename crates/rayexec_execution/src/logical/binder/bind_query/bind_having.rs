use rayexec_error::{RayexecError, Result};
use rayexec_parser::ast;

use super::bind_group_by::BoundGroupBy;
use super::bind_select_list::SelectListBinder;
use super::select_list::{BoundSelectList, SelectList};
use crate::expr::column_expr::{ColumnExpr, ColumnReference};
use crate::expr::Expression;
use crate::logical::binder::bind_context::{BindContext, BindScopeRef};
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
        &self,
        bind_context: &mut BindContext,
        select_list: &mut SelectList,
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
            select_list.grouping_functions_table,
            bind_context,
            &mut expr,
            &mut select_list.aggregates,
            &mut select_list.grouping_set_references,
        )?;

        Ok(expr)
    }

    /// Updates the having expression to ensure all column references point to
    /// either the aggregates table or groups table.
    ///
    /// This should be called after finalizing the select list as we'll be
    /// cloning in expressions from the GROUP BY directly, and they're updating
    /// only after select list finalizing.
    pub fn update_expression_dependencies(
        &self,
        select_list: &BoundSelectList,
        having_expr: &mut Expression,
        group_by: Option<&BoundGroupBy>,
    ) -> Result<()> {
        fn update_expr(
            group_by_expr: &Expression,
            group_by_col: &ColumnExpr,
            expr: &mut Expression,
        ) -> Result<()> {
            if expr == group_by_expr {
                *expr = Expression::Column(group_by_col.clone());
                return Ok(());
            }

            expr.for_each_child_mut(&mut |child| update_expr(group_by_expr, group_by_col, child))
        }

        if let Some(group_by) = group_by {
            // Now update all columns in the select list to references to the GROUP
            // BY expressions.
            for (idx, group_by_expr) in group_by.expressions.iter().enumerate() {
                let group_by_col = ColumnExpr {
                    reference: ColumnReference {
                        table_scope: group_by.group_exprs_table,
                        column: idx,
                    },
                    datatype: group_by_expr.datatype()?,
                };

                update_expr(group_by_expr, &group_by_col, having_expr)?;
            }
        }

        // Verify that we only reference either GROUP BY columns or contain
        // aggregates.
        let mut refs = having_expr.get_table_references();
        refs.remove(&select_list.aggregates_table);
        if let Some(group_by) = group_by {
            refs.remove(&group_by.group_exprs_table);
        }

        if !refs.is_empty() {
            return Err(RayexecError::new(
                "HAVING contains columns not found in the GROUP BY clause",
            ));
        }

        Ok(())
    }
}

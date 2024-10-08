use rayexec_error::Result;
use rayexec_parser::ast;

use super::select_list::SelectList;
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
        &mut self,
        bind_context: &mut BindContext,
        select_list: &mut SelectList,
        having: ast::Expr<ResolvedMeta>,
    ) -> Result<Expression> {
        let expr = BaseExpressionBinder::new(self.current, self.resolve_context).bind_expression(
            bind_context,
            &having,
            &mut DefaultColumnBinder,
            RecursionContext {
                allow_windows: false,
                allow_aggregates: true,
                is_root: true,
            },
        )?;

        // Append expression to projection list, this filter will then reference
        // the appended column.
        let col_expr = select_list.append_expression(bind_context, expr)?;

        Ok(Expression::Column(col_expr))
    }
}

use std::collections::HashMap;

use glaredb_error::Result;
use glaredb_parser::ast;

use super::expr_binder::RecursionContext;
use crate::expr::Expression;
use crate::functions::table::TableFunctionInput;
use crate::logical::binder::bind_context::BindContext;
use crate::logical::binder::column_binder::ErroringColumnBinder;
use crate::logical::binder::expr_binder::BaseExpressionBinder;
use crate::logical::resolver::ResolvedMeta;
use crate::logical::resolver::resolve_context::ResolveContext;
use crate::optimizer::expr_rewrite::ExpressionRewriteRule;
use crate::optimizer::expr_rewrite::const_fold::ConstFold;

#[derive(Debug)]
pub struct ConstantBinder<'a> {
    pub resolve_context: &'a ResolveContext,
}

impl<'a> ConstantBinder<'a> {
    pub fn new(resolve_context: &'a ResolveContext) -> Self {
        ConstantBinder { resolve_context }
    }

    /// Try to bind an AST expression as a constant value.
    pub fn bind_constant_expression(&self, expr: &ast::Expr<ResolvedMeta>) -> Result<Expression> {
        // TODO: Probably want to check that we didn't bind a subquery.
        let mut bind_context = BindContext::new_for_root();
        let expr = BaseExpressionBinder::new(bind_context.root_scope_ref(), self.resolve_context)
            .bind_expression(
            &mut bind_context,
            expr,
            &mut ErroringColumnBinder,
            RecursionContext {
                allow_aggregates: false,
                allow_windows: false,
                is_root: true,
            },
        )?;

        ConstFold::rewrite(expr)
    }

    pub fn bind_constant_function_args(
        &self,
        args: &[ast::FunctionArg<ResolvedMeta>],
    ) -> Result<TableFunctionInput> {
        let mut positional = Vec::new();
        let mut named = HashMap::new();

        for arg in args {
            match arg {
                ast::FunctionArg::Named { name, arg } => {
                    let expr = self.bind_constant_expression(arg)?;
                    named.insert(name.as_normalized_string(), expr);
                }
                ast::FunctionArg::Unnamed { arg } => {
                    let expr = self.bind_constant_expression(arg)?;
                    positional.push(expr);
                }
            }
        }

        Ok(TableFunctionInput { positional, named })
    }
}

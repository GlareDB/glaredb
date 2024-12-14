use std::collections::HashMap;

use rayexec_bullet::scalar::OwnedScalarValue;
use rayexec_error::{RayexecError, Result};
use rayexec_parser::ast;

use super::expr_binder::RecursionContext;
use crate::logical::binder::bind_context::BindContext;
use crate::logical::binder::column_binder::ErroringColumnBinder;
use crate::logical::binder::expr_binder::BaseExpressionBinder;
use crate::logical::resolver::resolve_context::ResolveContext;
use crate::logical::resolver::resolved_table_function::ConstantFunctionArgs;
use crate::logical::resolver::ResolvedMeta;
use crate::optimizer::expr_rewrite::const_fold::ConstFold;
use crate::optimizer::expr_rewrite::ExpressionRewriteRule;

#[derive(Debug)]
pub struct ConstantBinder<'a> {
    pub resolve_context: &'a ResolveContext,
}

impl<'a> ConstantBinder<'a> {
    pub fn new(resolve_context: &'a ResolveContext) -> Self {
        ConstantBinder { resolve_context }
    }

    /// Try to bind an AST expression as a constant value.
    pub fn bind_constant_expression(
        &self,
        expr: &ast::Expr<ResolvedMeta>,
    ) -> Result<OwnedScalarValue> {
        // TODO: Probably want to check that we didn't bind a subquery.
        let mut bind_context = BindContext::new();
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

        let val = ConstFold::rewrite(bind_context.get_table_list(), expr)?.try_into_scalar()?;

        Ok(val)
    }

    pub fn bind_constant_function_args(
        &self,
        args: &[ast::FunctionArg<ResolvedMeta>],
    ) -> Result<ConstantFunctionArgs> {
        let mut positional = Vec::new();
        let mut named = HashMap::new();

        for arg in args {
            match arg {
                ast::FunctionArg::Named { name, arg } => {
                    let expr = self.bind_constant_function_arg_expr(arg)?;
                    named.insert(name.as_normalized_string(), expr);
                }
                ast::FunctionArg::Unnamed { arg } => {
                    let expr = self.bind_constant_function_arg_expr(arg)?;
                    positional.push(expr);
                }
            }
        }

        Ok(ConstantFunctionArgs { positional, named })
    }

    fn bind_constant_function_arg_expr(
        &self,
        arg: &ast::FunctionArgExpr<ResolvedMeta>,
    ) -> Result<OwnedScalarValue> {
        match arg {
            ast::FunctionArgExpr::Expr(expr) => self.bind_constant_expression(expr),
            ast::FunctionArgExpr::Wildcard => Err(RayexecError::new(
                "'*' cannot be used as a constant function argument",
            )),
        }
    }
}

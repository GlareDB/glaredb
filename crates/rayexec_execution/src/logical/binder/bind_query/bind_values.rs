use rayexec_error::{RayexecError, Result};
use rayexec_parser::ast;

use crate::{
    expr::Expression,
    logical::{
        binder::{
            bind_context::{BindContext, BindScopeRef, TableRef},
            column_binder::DefaultColumnBinder,
            expr_binder::{BaseExpressionBinder, RecursionContext},
        },
        resolver::{resolve_context::ResolveContext, ResolvedMeta},
    },
};

#[derive(Debug, Clone, PartialEq)]
pub struct BoundValues {
    pub rows: Vec<Vec<Expression>>,
    pub expressions_table: TableRef,
}

#[derive(Debug)]
pub struct ValuesBinder<'a> {
    pub current: BindScopeRef,
    pub resolve_context: &'a ResolveContext,
}

impl<'a> ValuesBinder<'a> {
    pub fn new(current: BindScopeRef, resolve_context: &'a ResolveContext) -> Self {
        ValuesBinder {
            current,
            resolve_context,
        }
    }

    pub fn bind(
        &self,
        bind_context: &mut BindContext,
        values: ast::Values<ResolvedMeta>,
        _order_by: Option<ast::OrderByModifier<ResolvedMeta>>,
        _limit: ast::LimitModifier<ResolvedMeta>,
    ) -> Result<BoundValues> {
        // TODO: This could theoretically bind expressions as correlated
        // columns. TBD if that's desired.
        let expr_binder = BaseExpressionBinder::new(self.current, self.resolve_context);
        let rows = values
            .rows
            .into_iter()
            .map(|row| {
                expr_binder.bind_expressions(
                    bind_context,
                    &row,
                    &mut DefaultColumnBinder,
                    RecursionContext {
                        allow_windows: false,
                        allow_aggregates: false,
                        is_root: true,
                    },
                )
            })
            .collect::<Result<Vec<Vec<_>>>>()?;

        let first = match rows.first() {
            Some(first) => first,
            None => return Err(RayexecError::new("Empty VALUES statement")),
        };

        let types = first
            .iter()
            .map(|expr| expr.datatype(bind_context))
            .collect::<Result<Vec<_>>>()?;

        let names = (0..first.len())
            .map(|idx| format!("column{}", idx + 1))
            .collect();

        // TODO: What should happen with limit/order by?

        let table_ref = bind_context.push_table(self.current, None, types, names)?;

        Ok(BoundValues {
            rows,
            expressions_table: table_ref,
        })
    }
}

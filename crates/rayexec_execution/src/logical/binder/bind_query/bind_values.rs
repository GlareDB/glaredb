use rayexec_error::{RayexecError, Result};
use rayexec_parser::ast;

use crate::arrays::datatype::DataType;
use crate::expr::cast_expr::CastExpr;
use crate::expr::Expression;
use crate::logical::binder::bind_context::{BindContext, BindScopeRef};
use crate::logical::binder::column_binder::DefaultColumnBinder;
use crate::logical::binder::expr_binder::{BaseExpressionBinder, RecursionContext};
use crate::logical::binder::table_list::TableRef;
use crate::logical::resolver::resolve_context::ResolveContext;
use crate::logical::resolver::ResolvedMeta;

#[derive(Debug, Clone, PartialEq, Eq)]
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
        let mut rows = values
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

        let mut types = match rows.first() {
            Some(first) => first
                .iter()
                .map(|expr| expr.datatype(bind_context.get_table_list()))
                .collect::<Result<Vec<_>>>()?,
            None => return Err(RayexecError::new("Empty VALUES statement")),
        };

        // TODO: Below casting could be a bit more sophisticated by using the
        // implicit cast scoring to find the best types. Currently just searches
        // for null types and replaces those.

        // Find any null types and try to replace them.
        for row in &rows {
            if row.len() != types.len() {
                return Err(RayexecError::new(
                    "All rows in VALUES clause must have the same number of columns",
                ));
            }

            for (expr, datatype) in row.iter().zip(&mut types) {
                if datatype == &DataType::Null {
                    // Replace with current expression type.
                    *datatype = expr.datatype(bind_context.get_table_list())?;
                }
            }
        }

        // Now cast everything to the right type.
        for row in &mut rows {
            for (expr, datatype) in row.iter_mut().zip(&types) {
                if &expr.datatype(bind_context.get_table_list())? != datatype {
                    *expr = Expression::Cast(CastExpr {
                        to: datatype.clone(),
                        expr: Box::new(expr.clone()), // TODO: Could try to take instead of clone.
                    })
                }
            }
        }

        let names = (0..types.len())
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

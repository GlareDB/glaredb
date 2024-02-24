use super::{
    bind_context::{BindContext, Scope},
    operator::{CrossJoin, Empty, LogicalOperator, Order, OrderByExpr, Scan},
};
use crate::{
    expr::{
        binary::{BinaryExpr, BinaryOperator},
        scalar::ScalarValue,
        Expression,
    },
    functions::table::{TableFunction, TableFunctionArgs},
    planner::operator::Filter,
};
use rayexec_error::{RayexecError, Result};
use rayexec_parser::{ast, statement};

pub trait Resolver {
    /// Gets a table function for scanning the table pointed to by `reference`.
    fn resolve_for_table_scan(
        &self,
        reference: &ast::ObjectReference,
    ) -> Result<Box<dyn TableFunction>>;

    /// Resolve a table function by some reference.
    fn resolve_table_function(
        &self,
        reference: &ast::ObjectReference,
    ) -> Result<Box<dyn TableFunction>>;
}

pub struct Planner<R> {
    resolver: R,
}

impl<R: Resolver> Planner<R> {
    pub fn new(resolver: R) -> Self {
        Planner { resolver }
    }

    pub fn plan_statement(
        &self,
        stmt: statement::Statement,
    ) -> Result<(LogicalOperator, BindContext)> {
        match stmt {
            statement::Statement::Query(query) => self.plan_query(query),
            _ => unimplemented!(),
        }
    }

    fn plan_query(&self, query: ast::Query) -> Result<(LogicalOperator, BindContext)> {
        let mut context = BindContext::new();

        // TODO: CTE

        let plan = match query.body {
            ast::QueryBody::Select(select) => self.plan_select(select, &mut context)?,
            _ => unimplemented!(),
        };

        Ok((plan, context))
    }

    fn plan_select(
        &self,
        select: ast::SelectNode,
        context: &mut BindContext,
    ) -> Result<LogicalOperator> {
        // FROM
        let mut plan = self.plan_from_items(select.from, context)?;

        // WHERE
        if let Some(expr) = select.where_expr {
            plan = LogicalOperator::Filter(Filter {
                predicate: self.plan_scalar_expression(expr, context)?,
                input: Box::new(plan),
            });
        }

        // ORDER BY
        if let Some(order_by) = select.order_by {
            let exprs = self.plan_order_by(order_by, context)?;
            plan = LogicalOperator::Order(Order {
                exprs,
                input: Box::new(plan),
            })
        }

        Ok(plan)
    }

    /// Plan the entirety of a FROM clause.
    ///
    /// Each item in the from list will be implicitly cross joined.
    fn plan_from_items(
        &self,
        from: Vec<ast::FromItem>,
        context: &mut BindContext,
    ) -> Result<LogicalOperator> {
        let mut iter = from.into_iter();
        let mut left = match iter.next() {
            Some(item) => self.plan_from_item(item, context)?,
            None => LogicalOperator::Empty(Empty),
        };

        for item in iter {
            // TODO: Scope, lateral
            let right = self.plan_from_item(item, context)?;
            left = LogicalOperator::CrossJoin(CrossJoin {
                left: Box::new(left),
                right: Box::new(right),
            });

            // TODO: Merge scope
        }

        Ok(left)
    }

    /// Plan the logical operator for an item in the FROM clause.
    ///
    /// All table like items encountered (tables, subqueries, etc) will be added
    /// to the scope.
    fn plan_from_item(
        &self,
        from: ast::FromItem,
        context: &mut BindContext,
    ) -> Result<LogicalOperator> {
        match from {
            ast::FromItem::Table {
                name,
                alias,
                column_aliases,
            } => {
                let func = self.resolver.resolve_for_table_scan(&name)?;
                let bound = func.bind(TableFunctionArgs::default())?;

                // Use alias provided by user, otherwise fallback to the base
                // name of the table.
                let alias = alias
                    .map(|ident| ident.to_string())
                    .unwrap_or_else(|| name.base().expect("non-empty reference").to_string());

                let idx = context.add_table(bound, alias)?;

                Ok(LogicalOperator::Scan(Scan {
                    input: idx,
                    projection: None,
                }))
            }
            ast::FromItem::TableFunc {
                name,
                args,
                alias,
                column_aliases,
            } => {
                unimplemented!()
            }
            ast::FromItem::Join { .. } => {
                unimplemented!()
            }
            _ => unimplemented!(),
        }
    }

    /// Plan each expression in the order by list.
    ///
    /// Note that each expression in the list may have independent asc/desc and
    /// nulls first/last options.
    fn plan_order_by(
        &self,
        order_by: ast::OrderByList,
        context: &BindContext,
    ) -> Result<Vec<OrderByExpr>> {
        fn order_by_with_expr_and_opts(
            expr: Expression,
            options: ast::OrderByOptions,
        ) -> OrderByExpr {
            let asc = match options.asc {
                Some(ast::OrderByAscDesc::Descending) => false,
                _ => true,
            };
            let nulls_first = match options.nulls {
                Some(ast::OrderByNulls::First) => true,
                Some(ast::OrderByNulls::Last) => false,
                None => !asc,
            };

            OrderByExpr {
                expr,
                asc,
                nulls_first,
            }
        }

        let exprs = match order_by {
            ast::OrderByList::All { options } => {
                unimplemented!()
            }
            ast::OrderByList::Exprs { exprs } => {
                let mut order_by_exprs = Vec::with_capacity(exprs.len());
                for expr in exprs {
                    let logical = self.plan_scalar_expression(expr.expr, context)?;
                    let order_by_expr = order_by_with_expr_and_opts(logical, expr.options);
                    order_by_exprs.push(order_by_expr);
                }
                order_by_exprs
            }
        };

        Ok(exprs)
    }

    /// Bind an expression.
    fn plan_scalar_expression(&self, expr: ast::Expr, context: &BindContext) -> Result<Expression> {
        Ok(match expr {
            ast::Expr::Literal(lit) => match lit {
                ast::Literal::Number(n) => {
                    if let Ok(n) = n.parse::<i64>() {
                        Expression::Literal(ScalarValue::Int64(n))
                    } else if let Ok(n) = n.parse::<u64>() {
                        Expression::Literal(ScalarValue::UInt64(n))
                    } else if let Ok(n) = n.parse::<f64>() {
                        Expression::Literal(ScalarValue::Float64(n))
                    } else {
                        return Err(RayexecError::new(format!(
                            "Unable to parse {n} as a number"
                        )));
                    }
                }
                ast::Literal::Boolean(b) => Expression::Literal(ScalarValue::Boolean(b)),
                ast::Literal::Null => Expression::Literal(ScalarValue::Null),
                ast::Literal::SingleQuotedString(s) => {
                    Expression::Literal(ScalarValue::Utf8(s.to_string()))
                }
                other => {
                    return Err(RayexecError::new(format!(
                        "Unusupported SQL literal: {other:?}"
                    )))
                }
            },
            ast::Expr::BinaryExpr { left, op, right } => {
                let left = self.plan_scalar_expression(*left, context)?;
                let op = BinaryOperator::try_from(op)?;
                let right = self.plan_scalar_expression(*right, context)?;
                Expression::Binary(BinaryExpr {
                    left: Box::new(left),
                    op,
                    right: Box::new(right),
                })
            }
            _ => unimplemented!(),
        })
    }
}

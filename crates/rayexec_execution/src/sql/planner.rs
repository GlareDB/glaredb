use rayexec_error::{RayexecError, Result};
use rayexec_parser::{ast, parser::parse, statement::Statement};

use crate::expr::{
    binary::{BinaryExpr, BinaryOperator},
    logical::{LogicalExpr, OrderByExpr, UnboundWildcard},
    scalar::ScalarValue,
};
use crate::logical::LogicalPlan;

/// Plan one or more sql statements.
///
/// The generated logical plan will contain unbound nodes requiring further
/// resolution.
#[derive(Debug, Copy, Clone, Default)]
pub struct SqlPlanner;

impl SqlPlanner {
    /// Plans a sql statement into zero or more logical plans.
    pub fn plan(sql: &str) -> Result<Vec<LogicalPlan>> {
        let stmts = parse(sql)?;
        let mut plans = Vec::with_capacity(stmts.len());

        for stmt in stmts {
            let plan = Self::plan_statement(stmt)?;
            plans.push(plan)
        }

        Ok(plans)
    }

    fn plan_statement(stmt: Statement) -> Result<LogicalPlan> {
        match stmt {
            Statement::Query(query) => Self::plan_query(query),
            _ => unimplemented!(),
        }
    }

    fn plan_query(query: ast::Query) -> Result<LogicalPlan> {
        // CTEs
        if let Some(with) = query.with {
            unimplemented!()
        }

        match query.body {
            ast::QueryBody::Select(select) => Self::plan_select_node(select),
            ast::QueryBody::SetExpr { .. } => unimplemented!(),
            ast::QueryBody::Values() => unimplemented!(),
        }
    }

    fn plan_select_node(select: ast::SelectNode) -> Result<LogicalPlan> {
        // Plan FROM
        let mut plan = match select.from.len() {
            0 => LogicalPlan::empty(true),
            _ => {
                let mut tables = select.from.0.into_iter();
                let mut left = Self::plan_table_with_joins(tables.next().unwrap())?;

                for table in tables {
                    let right = Self::plan_table_with_joins(table)?;
                    left = left.cross_join(right)?;
                }

                left
            }
        };

        // Plan WHERE
        if let Some(where_expr) = select.where_expr {
            let expr = Self::plan_expr(where_expr)?;
            plan = plan.filter(expr)?;
        }

        // Plan projection
        let projections = Self::plan_select_list(select.projections)?;
        plan = plan.project(projections)?;

        // TODO: GROUP BY
        // TODO: HAVING

        // DISTINCT
        match select.modifier {
            ast::SelectModifer::None | ast::SelectModifer::All => (), // Nothing to do.
            ast::SelectModifer::Distinct => {
                plan = plan.distinct()?;
            }
            ast::SelectModifer::DistinctOn(exprs) => {
                let exprs = exprs
                    .into_iter()
                    .map(|expr| Self::plan_expr(expr))
                    .collect::<Result<Vec<_>, _>>()?;
                unimplemented!()
            }
        }

        // ORDER BY
        if let Some(list) = select.order_by {
            let exprs = Self::plan_order_by_list(list)?;
            plan = plan.order_by(exprs)?;
        }

        if let Some(limit) = select.limit {
            let expr = Self::plan_expr(limit)?;
            let limit = match expr {
                LogicalExpr::Literal(ScalarValue::Int64(n)) if n >= 0 => n as usize,
                other => {
                    return Err(RayexecError::new(format!(
                        "Invalid expression for LIMIT: {other:?}"
                    )))
                }
            };
            plan = plan.limit(limit)?;
        }

        // TODO: OFFSET

        Ok(plan)
    }

    fn plan_table_with_joins(table: ast::TableWithJoins) -> Result<LogicalPlan> {
        match table.joins.len() {
            0 => Self::plan_table_like(table.table),
            _ => {
                unimplemented!()
            }
        }
    }

    fn plan_order_by_list(list: ast::OrderByList) -> Result<Vec<OrderByExpr>> {
        fn order_by_with_expr_and_opts(
            expr: LogicalExpr,
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

        let exprs = match list {
            ast::OrderByList::All { options } => {
                vec![order_by_with_expr_and_opts(
                    LogicalExpr::UnboundAll,
                    options,
                )]
            }
            ast::OrderByList::Exprs { exprs } => {
                let mut order_by_exprs = Vec::with_capacity(exprs.len());
                for expr in exprs {
                    let logical = Self::plan_expr(expr.expr)?;
                    let order_by_expr = order_by_with_expr_and_opts(logical, expr.options);
                    order_by_exprs.push(order_by_expr);
                }
                order_by_exprs
            }
        };

        Ok(exprs)
    }

    fn plan_select_list(list: ast::SelectList) -> Result<Vec<LogicalExpr>> {
        let mut exprs = Vec::with_capacity(list.0.len());

        for item in list.0 {
            let expr = match item {
                ast::SelectItem::Expr(expr) => Self::plan_expr(expr)?,
                ast::SelectItem::AliasedExpr(_, _) => unimplemented!(),
                ast::SelectItem::Wildcard(wildcard) => {
                    LogicalExpr::UnboundWildcard(UnboundWildcard::Wildcard(wildcard))
                }
                ast::SelectItem::QualifiedWildcard(reference, wildcard) => {
                    LogicalExpr::UnboundWildcard(UnboundWildcard::QualifiedWildcard(
                        reference, wildcard,
                    ))
                }
            };
            exprs.push(expr);
        }

        Ok(exprs)
    }

    fn plan_table_like(table: ast::TableLike) -> Result<LogicalPlan> {
        match table {
            ast::TableLike::Table(reference) => {
                // TODO: Alias
                let plan = LogicalPlan::table(reference);
                Ok(plan)
            }
            ast::TableLike::Function { name, args } => {
                // TODO: Alias & args
                let plan = LogicalPlan::table_func(name);
                Ok(plan)
            }
            ast::TableLike::Derived {} => {
                unimplemented!()
            }
        }
    }

    fn plan_expr(expr: ast::Expr) -> Result<LogicalExpr> {
        Ok(match expr {
            ast::Expr::Ident(ident) => LogicalExpr::UnboundIdent(ident),
            ast::Expr::CompoundIdent(ident) => LogicalExpr::UnboundCompoundIdent(ident),
            ast::Expr::Literal(literal) => match literal {
                ast::Literal::Number(n) => {
                    if let Ok(n) = n.parse::<i64>() {
                        LogicalExpr::Literal(ScalarValue::Int64(n))
                    } else if let Ok(n) = n.parse::<u64>() {
                        LogicalExpr::Literal(ScalarValue::UInt64(n))
                    } else if let Ok(n) = n.parse::<f64>() {
                        LogicalExpr::Literal(ScalarValue::Float64(n))
                    } else {
                        return Err(RayexecError::new(format!(
                            "Unable to parse {n} as a number"
                        )));
                    }
                }
                ast::Literal::Boolean(b) => LogicalExpr::Literal(ScalarValue::Boolean(b)),
                ast::Literal::Null => LogicalExpr::Literal(ScalarValue::Null),
                ast::Literal::SingleQuotedString(s) => {
                    LogicalExpr::Literal(ScalarValue::Utf8(s.to_string()))
                }
                other => {
                    return Err(RayexecError::new(format!(
                        "Unusupported SQL literal: {other:?}"
                    )))
                }
            },
            ast::Expr::BinaryExpr { left, op, right } => {
                let op = BinaryOperator::try_from(op)?;
                let left = Self::plan_expr(*left)?;
                let right = Self::plan_expr(*right)?;
                LogicalExpr::Binary(BinaryExpr {
                    left: Box::new(left),
                    op,
                    right: Box::new(right),
                })
            }
            other => {
                return Err(RayexecError::new(format!(
                    "Unusupported SQL expression: {other:?}"
                )))
            }
        })
    }
}

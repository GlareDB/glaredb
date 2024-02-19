use rayexec_error::{RayexecError, Result};
use rayexec_parser::{
    ast::{self},
    parser::parse,
    statement::Statement,
};

use crate::expr::logical::LogicalExpr;
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
        unimplemented!()
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

        unimplemented!()
    }

    fn plan_table_with_joins(table: ast::TableWithJoins) -> Result<LogicalPlan> {
        match table.joins.len() {
            0 => Self::plan_table_like(table.table),
            _ => {
                unimplemented!()
            }
        }
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
        unimplemented!()
    }
}

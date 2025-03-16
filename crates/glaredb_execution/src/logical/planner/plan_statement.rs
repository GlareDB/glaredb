use glaredb_error::Result;

use super::plan_copy::CopyPlanner;
use super::plan_create_table::CreateTablePlanner;
use super::plan_explain::ExplainPlanner;
use super::plan_insert::InsertPlanner;
use super::plan_query::QueryPlanner;
use crate::logical::binder::bind_attach::{BoundAttach, BoundDetach};
use crate::logical::binder::bind_context::BindContext;
use crate::logical::binder::bind_statement::BoundStatement;
use crate::logical::operator::LogicalOperator;

#[derive(Debug)]
pub struct StatementPlanner;

impl StatementPlanner {
    pub fn plan(
        &self,
        bind_context: &mut BindContext,
        statement: BoundStatement,
    ) -> Result<LogicalOperator> {
        match statement {
            BoundStatement::Query(query) => QueryPlanner.plan(bind_context, query),
            BoundStatement::SetVar(plan) => Ok(LogicalOperator::SetVar(plan)),
            BoundStatement::ShowVar(plan) => Ok(LogicalOperator::ShowVar(plan)),
            BoundStatement::ResetVar(plan) => Ok(LogicalOperator::ResetVar(plan)),
            BoundStatement::Attach(BoundAttach::Database(plan)) => {
                Ok(LogicalOperator::AttachDatabase(plan))
            }
            BoundStatement::Detach(BoundDetach::Database(plan)) => {
                Ok(LogicalOperator::DetachDatabase(plan))
            }
            BoundStatement::Drop(plan) => Ok(LogicalOperator::Drop(plan)),
            BoundStatement::Insert(insert) => InsertPlanner.plan(bind_context, insert),
            BoundStatement::CreateSchema(plan) => Ok(LogicalOperator::CreateSchema(plan)),
            BoundStatement::CreateTable(create) => CreateTablePlanner.plan(bind_context, create),
            BoundStatement::CreateView(create) => Ok(LogicalOperator::CreateView(create)),
            BoundStatement::Describe(plan) => Ok(LogicalOperator::Describe(plan)),
            BoundStatement::Explain(explain) => ExplainPlanner.plan(bind_context, explain),
            BoundStatement::CopyTo(copy_to) => CopyPlanner.plan(bind_context, copy_to),
        }
    }
}

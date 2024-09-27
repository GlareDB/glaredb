use rayexec_error::Result;

use crate::logical::{
    binder::{
        bind_attach::{BoundAttach, BoundDetach},
        bind_context::BindContext,
        bind_statement::BoundStatement,
    },
    operator::LogicalOperator,
};

use super::{
    plan_copy::CopyPlanner, plan_create_table::CreateTablePlanner, plan_explain::ExplainPlanner,
    plan_insert::InsertPlanner, plan_query::QueryPlanner,
};

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

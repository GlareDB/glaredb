use crate::logical::{
    binder::{
        bind_context::BindContext,
        bind_copy::{BoundCopyTo, BoundCopyToSource},
    },
    logical_copy::LogicalCopyTo,
    operator::{LocationRequirement, LogicalOperator, Node},
    planner::{plan_from::FromPlanner, plan_query::QueryPlanner},
};
use rayexec_error::Result;

#[derive(Debug)]
pub struct CopyPlanner;

impl CopyPlanner {
    pub fn plan(
        &self,
        bind_context: &mut BindContext,
        copy_to: BoundCopyTo,
    ) -> Result<LogicalOperator> {
        let source = match copy_to.source {
            BoundCopyToSource::Query(query) => QueryPlanner.plan(bind_context, query)?,
            BoundCopyToSource::Table(table) => FromPlanner.plan(bind_context, table)?,
        };

        // Currently only support copying to local.

        Ok(LogicalOperator::CopyTo(Node {
            node: LogicalCopyTo {
                source_schema: copy_to.source_schema,
                location: copy_to.location,
                copy_to: copy_to.copy_to,
            },
            location: LocationRequirement::ClientLocal,
            children: vec![source],
        }))
    }
}

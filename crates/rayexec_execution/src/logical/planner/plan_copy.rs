use rayexec_error::Result;

use crate::logical::binder::bind_context::BindContext;
use crate::logical::binder::bind_copy::{BoundCopyTo, BoundCopyToSource};
use crate::logical::logical_copy::LogicalCopyTo;
use crate::logical::operator::{LocationRequirement, LogicalOperator, Node};
use crate::logical::planner::plan_from::FromPlanner;
use crate::logical::planner::plan_query::QueryPlanner;
use crate::logical::statistics::StatisticsValue;

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
            estimated_cardinality: StatisticsValue::Unknown,
        }))
    }
}

use glaredb_error::Result;

use crate::logical::binder::bind_context::BindContext;
use crate::logical::binder::bind_create_table::BoundCreateTable;
use crate::logical::logical_create::LogicalCreateTable;
use crate::logical::operator::{LocationRequirement, LogicalOperator, Node};
use crate::logical::planner::plan_query::QueryPlanner;
use crate::statistics::value::StatisticsValue;

#[derive(Debug)]
pub struct CreateTablePlanner;

impl CreateTablePlanner {
    pub fn plan(
        &self,
        bind_context: &mut BindContext,
        create: BoundCreateTable,
    ) -> Result<LogicalOperator> {
        let children = match create.source {
            Some(source) => {
                vec![QueryPlanner.plan(bind_context, source)?]
            }
            _ => Vec::new(),
        };

        Ok(LogicalOperator::CreateTable(Node {
            node: LogicalCreateTable {
                catalog: create.catalog,
                schema: create.schema,
                name: create.name,
                columns: create.columns,
                on_conflict: create.on_conflict,
            },
            location: LocationRequirement::ClientLocal,
            children,
            estimated_cardinality: StatisticsValue::Unknown,
        }))
    }
}

use crate::logical::{
    binder::{bind_context::BindContext, bind_create_table::BoundCreateTable},
    logical_create::LogicalCreateTable,
    operator::{LocationRequirement, LogicalOperator, Node},
    planner::plan_query::QueryPlanner,
};
use rayexec_error::Result;

#[derive(Debug)]
pub struct CreateTablePlanner;

impl CreateTablePlanner {
    pub fn plan(
        &self,
        bind_context: &mut BindContext,
        create: BoundCreateTable,
    ) -> Result<LogicalOperator> {
        let children = if let Some(source) = create.source {
            vec![QueryPlanner.plan(bind_context, source)?]
        } else {
            Vec::new()
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
        }))
    }
}

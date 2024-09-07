use crate::logical::{
    binder::{bind_context::BindContext, bind_insert::BoundInsert},
    logical_insert::LogicalInsert,
    logical_project::LogicalProject,
    operator::{LocationRequirement, LogicalOperator, Node},
};
use rayexec_error::Result;

use super::plan_query::QueryPlanner;

#[derive(Debug)]
pub struct InsertPlanner;

impl InsertPlanner {
    pub fn plan(
        &self,
        bind_context: &mut BindContext,
        insert: BoundInsert,
    ) -> Result<LogicalOperator> {
        let mut source = QueryPlanner.plan(bind_context, insert.source)?;

        if let Some(projections) = insert.projections {
            source = LogicalOperator::Project(Node {
                node: LogicalProject {
                    projections: projections.projections,
                    projection_table: projections.projection_table,
                },
                location: LocationRequirement::Any,
                children: vec![source],
            })
        }

        Ok(LogicalOperator::Insert(Node {
            node: LogicalInsert {
                catalog: insert.table.catalog,
                schema: insert.table.schema,
                table: insert.table.entry,
            },
            location: insert.table_location,
            children: vec![source],
        }))
    }
}

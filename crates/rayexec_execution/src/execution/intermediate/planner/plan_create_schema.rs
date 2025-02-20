
use rayexec_error::{RayexecError, Result};

use super::{IntermediatePipelineBuildState, PipelineIdGen};
use crate::database::create::CreateSchemaInfo;
use crate::execution::operators::create_schema::PhysicalCreateSchema;
use crate::execution::operators::PhysicalOperator;
use crate::logical::logical_create::LogicalCreateSchema;
use crate::logical::operator::Node;

impl IntermediatePipelineBuildState<'_> {
    pub fn plan_create_schema(
        &mut self,
        id_gen: &mut PipelineIdGen,
        create: Node<LogicalCreateSchema>,
    ) -> Result<()> {
        let location = create.location;

        if self.in_progress.is_some() {
            return Err(RayexecError::new("Expected in progress to be None"));
        }

        let operator = PhysicalOperator::CreateSchema(PhysicalCreateSchema::new(
            create.node.catalog,
            CreateSchemaInfo {
                name: create.node.name,
                on_conflict: create.node.on_conflict,
            },
        ));

        unimplemented!()
        // self.in_progress = Some(InProgressPipeline {
        //     id: id_gen.next_pipeline_id(),
        //     operators: vec![operator],
        //     location,
        //     source: PipelineSource::InPipeline,
        // });

        // Ok(())
    }
}

use std::sync::Arc;

use rayexec_error::{RayexecError, Result};

use super::{InProgressPipeline, IntermediatePipelineBuildState, PipelineIdGen};
use crate::database::create::CreateViewInfo;
use crate::execution::intermediate::pipeline::{IntermediateOperator, PipelineSource};
use crate::execution::operators::create_view::PhysicalCreateView;
use crate::execution::operators::PhysicalOperator;
use crate::logical::logical_create::LogicalCreateView;
use crate::logical::operator::Node;

impl IntermediatePipelineBuildState<'_> {
    pub fn plan_create_view(
        &mut self,
        id_gen: &mut PipelineIdGen,
        create: Node<LogicalCreateView>,
    ) -> Result<()> {
        if self.in_progress.is_some() {
            return Err(RayexecError::new("Expected in progress to be None"));
        }

        let operator = IntermediateOperator {
            operator: Arc::new(PhysicalOperator::CreateView(PhysicalCreateView {
                catalog: create.node.catalog,
                schema: create.node.schema,
                info: CreateViewInfo {
                    name: create.node.name,
                    column_aliases: create.node.column_aliases,
                    on_conflict: create.node.on_conflict,
                    query_string: create.node.query_string,
                },
            })),
            partitioning_requirement: Some(1),
        };

        self.in_progress = Some(InProgressPipeline {
            id: id_gen.next_pipeline_id(),
            operators: vec![operator],
            location: create.location,
            source: PipelineSource::InPipeline,
        });

        Ok(())
    }
}

use std::sync::Arc;

use rayexec_error::{RayexecError, Result};

use super::{InProgressPipeline, IntermediatePipelineBuildState, PipelineIdGen};
use crate::execution::intermediate::pipeline::{IntermediateOperator, PipelineSource};
use crate::execution::operators::drop::PhysicalDrop;
use crate::execution::operators::PhysicalOperator;
use crate::logical::logical_drop::LogicalDrop;
use crate::logical::operator::Node;

impl IntermediatePipelineBuildState<'_> {
    pub fn plan_drop(&mut self, id_gen: &mut PipelineIdGen, drop: Node<LogicalDrop>) -> Result<()> {
        let location = drop.location;

        if self.in_progress.is_some() {
            return Err(RayexecError::new("Expected in progress to be None"));
        }

        let operator = IntermediateOperator {
            operator: Arc::new(PhysicalOperator::Drop(PhysicalDrop::new(
                drop.node.catalog,
                drop.node.info,
            ))),
            partitioning_requirement: Some(1),
        };

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

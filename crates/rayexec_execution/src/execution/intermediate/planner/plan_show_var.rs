use std::sync::Arc;

use rayexec_error::{RayexecError, Result};

use super::{InProgressPipeline, IntermediatePipelineBuildState, PipelineIdGen};
use crate::arrays::array::Array;
use crate::arrays::batch::Batch;
use crate::execution::intermediate::pipeline::{IntermediateOperator, PipelineSource};
use crate::execution::operators::values::PhysicalValues;
use crate::execution::operators::PhysicalOperator;
use crate::logical::logical_set::LogicalShowVar;
use crate::logical::operator::Node;

impl IntermediatePipelineBuildState<'_> {
    pub fn plan_show_var(
        &mut self,
        id_gen: &mut PipelineIdGen,
        show: Node<LogicalShowVar>,
    ) -> Result<()> {
        let location = show.location;
        let show = show.into_inner();

        if self.in_progress.is_some() {
            return Err(RayexecError::new("Expected in progress to be None"));
        }

        let operator = IntermediateOperator {
            operator: Arc::new(PhysicalOperator::Values(PhysicalValues::new(vec![
                Batch::try_from_arrays([Array::from_iter([show.value.to_string().as_str()])])?,
            ]))),
            partitioning_requirement: Some(1),
        };

        self.in_progress = Some(InProgressPipeline {
            id: id_gen.next_pipeline_id(),
            operators: vec![operator],
            location,
            source: PipelineSource::InPipeline,
        });

        Ok(())
    }
}

use std::sync::Arc;

use rayexec_error::{RayexecError, Result};

use super::{InProgressPipeline, IntermediatePipelineBuildState, PipelineIdGen};
use crate::arrays::array::Array;
use crate::arrays::batch::Batch;
use crate::execution::intermediate::pipeline::{IntermediateOperator, PipelineSource};
use crate::execution::operators::values::PhysicalValues;
use crate::execution::operators::PhysicalOperator;
use crate::logical::logical_describe::LogicalDescribe;
use crate::logical::operator::Node;

impl IntermediatePipelineBuildState<'_> {
    pub fn plan_describe(
        &mut self,
        id_gen: &mut PipelineIdGen,
        describe: Node<LogicalDescribe>,
    ) -> Result<()> {
        let location = describe.location;

        if self.in_progress.is_some() {
            return Err(RayexecError::new("Expected in progress to be None"));
        }

        let names = Array::from_iter(describe.node.schema.iter().map(|f| f.name.as_str()));
        let datatypes =
            Array::from_iter(describe.node.schema.iter().map(|f| f.datatype.to_string()));
        let batch = Batch::try_from_arrays(vec![names, datatypes])?;

        unimplemented!()
        // let operator = IntermediateOperator {
        //     operator: Arc::new(PhysicalOperator::Values(PhysicalValues::new(vec![batch]))),
        //     partitioning_requirement: Some(1),
        // };

        // self.in_progress = Some(InProgressPipeline {
        //     id: id_gen.next_pipeline_id(),
        //     operators: vec![operator],
        //     location,
        //     source: PipelineSource::InPipeline,
        // });

        // Ok(())
    }
}

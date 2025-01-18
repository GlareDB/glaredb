use std::sync::Arc;

use rayexec_error::Result;

use super::{IntermediatePipelineBuildState, Materializations, PipelineIdGen};
use crate::execution::intermediate::pipeline::IntermediateOperator;
use crate::execution::operators::copy_to::CopyToOperation;
use crate::execution::operators::sink::PhysicalSink;
use crate::execution::operators::PhysicalOperator;
use crate::logical::logical_copy::LogicalCopyTo;
use crate::logical::operator::Node;

impl IntermediatePipelineBuildState<'_> {
    pub fn plan_copy_to(
        &mut self,
        id_gen: &mut PipelineIdGen,
        materializations: &mut Materializations,
        mut copy_to: Node<LogicalCopyTo>,
    ) -> Result<()> {
        let location = copy_to.location;
        let source = copy_to.take_one_child_exact()?;

        self.walk(materializations, id_gen, source)?;

        let operator = IntermediateOperator {
            operator: Arc::new(PhysicalOperator::CopyTo(PhysicalSink::new(
                CopyToOperation {
                    copy_to: copy_to.node.copy_to,
                    location: copy_to.node.location,
                    schema: copy_to.node.source_schema,
                },
            ))),
            // This should be temporary until there's a better understanding of
            // how we want to handle parallel writes.
            partitioning_requirement: Some(1),
        };

        self.push_intermediate_operator(operator, location, id_gen)?;

        Ok(())
    }
}

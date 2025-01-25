
use rayexec_error::Result;

use super::{IntermediatePipelineBuildState, Materializations, PipelineIdGen};
use crate::execution::operators::limit::PhysicalLimit;
use crate::execution::operators::PhysicalOperator;
use crate::logical::logical_limit::LogicalLimit;
use crate::logical::operator::Node;

impl IntermediatePipelineBuildState<'_> {
    pub fn plan_limit(
        &mut self,
        id_gen: &mut PipelineIdGen,
        materializations: &mut Materializations,
        mut limit: Node<LogicalLimit>,
    ) -> Result<()> {
        let location = limit.location;
        let input = limit.take_one_child_exact()?;

        self.walk(materializations, id_gen, input)?;

        // This is a global limit, ensure this operator is only receiving a
        // single input partition.
        let operator = PhysicalOperator::Limit(PhysicalLimit {
            limit: limit.node.limit,
            offset: limit.node.offset,
        });

        self.push_intermediate_operator(operator, location, id_gen)?;

        Ok(())
    }
}

use std::sync::Arc;

use rayexec_error::{Result, ResultExt};

use super::{IntermediatePipelineBuildState, Materializations, PipelineIdGen};
use crate::execution::intermediate::pipeline::IntermediateOperator;
use crate::execution::operators::table_inout::PhysicalTableInOut;
use crate::execution::operators::PhysicalOperator;
use crate::logical::logical_inout::LogicalInOut;
use crate::logical::operator::{LogicalNode, Node};

impl IntermediatePipelineBuildState<'_> {
    pub fn plan_inout(
        &mut self,
        id_gen: &mut PipelineIdGen,
        materializations: &mut Materializations,
        mut inout: Node<LogicalInOut>,
    ) -> Result<()> {
        let input = inout.take_one_child_exact()?;
        let input_refs = input.get_output_table_refs(self.bind_context);
        self.walk(materializations, id_gen, input)?;

        let function_inputs = self
            .expr_planner
            .plan_scalars(&input_refs, &inout.node.function.positional_inputs)
            .context("Failed to plan input expressions for table inout")?;

        let projected_outputs = self
            .expr_planner
            .plan_scalars(&input_refs, &inout.node.projected_outputs)
            .context("Failed to plan additional output expressions for table inout")?;

        let operator = IntermediateOperator {
            operator: Arc::new(PhysicalOperator::TableInOut(PhysicalTableInOut {
                function: inout.node.function,
                function_inputs,
                projected_outputs,
            })),
            partitioning_requirement: None,
        };

        self.push_intermediate_operator(operator, inout.location, id_gen)?;

        Ok(())
    }
}

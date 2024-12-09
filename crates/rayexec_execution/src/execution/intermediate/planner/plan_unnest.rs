use std::sync::Arc;

use rayexec_error::{Result, ResultExt};

use super::{IntermediatePipelineBuildState, Materializations, PipelineIdGen};
use crate::execution::intermediate::pipeline::IntermediateOperator;
use crate::execution::operators::unnest::PhysicalUnnest;
use crate::execution::operators::PhysicalOperator;
use crate::logical::logical_unnest::LogicalUnnest;
use crate::logical::operator::{LogicalNode, Node};

impl<'a> IntermediatePipelineBuildState<'a> {
    pub fn plan_unnest(
        &mut self,
        id_gen: &mut PipelineIdGen,
        materializations: &mut Materializations,
        mut unnest: Node<LogicalUnnest>,
    ) -> Result<()> {
        let location = unnest.location;

        let input = unnest.take_one_child_exact()?;
        let input_refs = input.get_output_table_refs(self.bind_context);
        self.walk(materializations, id_gen, input)?;

        let project_expressions = self
            .expr_planner
            .plan_scalars(&input_refs, &unnest.node.project_expressions)
            .context("Failed to plan project expressions for unnest")?;

        let unnest_expressions = self
            .expr_planner
            .plan_scalars(&input_refs, &unnest.node.unnest_expressions)
            .context("Failed to plan unnest expressions for unnest")?;

        let operator = IntermediateOperator {
            operator: Arc::new(PhysicalOperator::Unnest(PhysicalUnnest {
                project_expressions,
                unnest_expressions,
            })),
            partitioning_requirement: None,
        };

        self.push_intermediate_operator(operator, location, id_gen)?;

        Ok(())
    }
}

use std::sync::Arc;

use rayexec_error::{Result, ResultExt};

use super::{IntermediatePipelineBuildState, Materializations, PipelineIdGen};
use crate::execution::intermediate::pipeline::IntermediateOperator;
use crate::execution::operators::project::PhysicalProject;
use crate::execution::operators::simple::SimpleOperator;
use crate::execution::operators::PhysicalOperator;
use crate::logical::logical_project::LogicalProject;
use crate::logical::operator::{LogicalNode, Node};

impl IntermediatePipelineBuildState<'_> {
    pub fn plan_project(
        &mut self,
        id_gen: &mut PipelineIdGen,
        materializations: &mut Materializations,
        mut project: Node<LogicalProject>,
    ) -> Result<()> {
        let location = project.location;

        let input = project.take_one_child_exact()?;
        let input_refs = input.get_output_table_refs(self.bind_context);
        self.walk(materializations, id_gen, input)?;

        let projections = self
            .expr_planner
            .plan_scalars(&input_refs, &project.node.projections)
            .context("Failed to plan expressions for projection")?;

        let operator = IntermediateOperator {
            operator: Arc::new(PhysicalOperator::Project(PhysicalProject { projections })),
            partitioning_requirement: None,
        };

        self.push_intermediate_operator(operator, location, id_gen)?;

        Ok(())
    }
}

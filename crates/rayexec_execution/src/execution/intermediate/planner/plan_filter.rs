use rayexec_error::{Result, ResultExt};

use super::{IntermediatePipelineBuildState, Materializations, PipelineIdGen};
use crate::execution::operators::filter::PhysicalFilter;
use crate::execution::operators::PhysicalOperator;
use crate::logical::logical_filter::LogicalFilter;
use crate::logical::operator::{LogicalNode, Node};

impl IntermediatePipelineBuildState<'_> {
    pub fn plan_filter(
        &mut self,
        id_gen: &mut PipelineIdGen,
        materializations: &mut Materializations,
        mut filter: Node<LogicalFilter>,
    ) -> Result<()> {
        let location = filter.location;

        let input = filter.take_one_child_exact()?;
        let input_refs = input.get_output_table_refs(self.bind_context);
        self.walk(materializations, id_gen, input)?;

        // let predicate = self
        //     .expr_planner
        //     .plan_scalar(&input_refs, &filter.node.filter)
        //     .context("Failed to plan expressions for filter")?;

        // let operator = PhysicalOperator::Filter(PhysicalFilter { predicate });
        // self.push_intermediate_operator(operator, location, id_gen)?;

        // Ok(())
        unimplemented!()
    }
}

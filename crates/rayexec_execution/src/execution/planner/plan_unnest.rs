use rayexec_error::{Result, ResultExt};

use super::OperatorPlanState;
use crate::logical::logical_unnest::LogicalUnnest;
use crate::logical::operator::{LogicalNode, Node};

impl OperatorPlanState<'_> {
    pub fn plan_unnest(&mut self, mut unnest: Node<LogicalUnnest>) -> Result<()> {
        let location = unnest.location;

        let input = unnest.take_one_child_exact()?;
        let input_refs = input.get_output_table_refs(self.bind_context);
        // self.walk(materializations, input)?;

        // let project_expressions = self
        //     .expr_planner
        //     .plan_scalars(&input_refs, &unnest.node.project_expressions)
        //     .context("Failed to plan project expressions for unnest")?;

        unimplemented!()
        // let unnest_expressions = self
        //     .expr_planner
        //     .plan_scalars(&input_refs, &unnest.node.unnest_expressions)
        //     .context("Failed to plan unnest expressions for unnest")?;

        // let operator = PhysicalOperator::Unnest(PhysicalUnnest {
        //     project_expressions,
        //     unnest_expressions,
        // });
        // self.push_intermediate_operator(operator, location)?;

        // Ok(())
    }
}

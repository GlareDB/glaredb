use glaredb_error::{Result, ResultExt};

use super::OperatorPlanState;
use crate::execution::operators::project::PhysicalProject;
use crate::execution::operators::{PlannedOperator, PlannedOperatorWithChildren};
use crate::logical::logical_project::LogicalProject;
use crate::logical::operator::{LogicalNode, Node};

impl OperatorPlanState<'_> {
    pub fn plan_project(
        &mut self,
        mut project: Node<LogicalProject>,
    ) -> Result<PlannedOperatorWithChildren> {
        let _location = project.location;

        let input = project.take_one_child_exact()?;
        let input_refs = input.get_output_table_refs(self.bind_context);
        let input = self.plan(input)?;

        let projections = self
            .expr_planner
            .plan_scalars(&input_refs, &project.node.projections)
            .context("Failed to plan expressions for projection")?;

        let operator = PhysicalProject::new(projections);
        let planned = PlannedOperator::new_execute(operator);

        Ok(PlannedOperatorWithChildren {
            operator: planned,
            children: vec![input],
        })
    }
}

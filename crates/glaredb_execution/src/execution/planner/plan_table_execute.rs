use glaredb_error::{Result, ResultExt};

use super::OperatorPlanState;
use crate::execution::operators::project::PhysicalProject;
use crate::execution::operators::table_execute::PhysicalTableExecute;
use crate::execution::operators::{PlannedOperator, PlannedOperatorWithChildren};
use crate::expr::physical::column_expr::PhysicalColumnExpr;
use crate::logical::logical_inout::LogicalTableExecute;
use crate::logical::operator::{LogicalNode, Node};

impl OperatorPlanState<'_> {
    pub fn plan_table_execute(
        &mut self,
        mut inout: Node<LogicalTableExecute>,
    ) -> Result<PlannedOperatorWithChildren> {
        let input = inout.take_one_child_exact()?;
        let input_refs = input.get_output_table_refs(self.bind_context);
        let child = self.plan(input)?;

        // TODO: Currently assumes positional inputs are the only ones we care
        // about.
        let function_inputs = self
            .expr_planner
            .plan_scalars(
                &input_refs,
                &inout.node.function.bind_state.input.positional,
            )
            .context("Failed to plan input expressions for table inout")?;

        let projected_outputs = self
            .expr_planner
            .plan_scalars(&input_refs, &inout.node.projected_outputs)
            .context("Failed to plan additional output expressions for table inout")?;

        // Generate column expressions for additional projections.
        let additional_projections: Vec<_> = projected_outputs
            .iter()
            .enumerate()
            .map(|(idx, expr)| PhysicalColumnExpr {
                idx: idx + function_inputs.len(),
                datatype: expr.datatype(),
            })
            .collect();

        let projections = function_inputs.into_iter().chain(projected_outputs);

        // Project function inputs first.
        let child = PlannedOperatorWithChildren {
            operator: PlannedOperator::new_execute(
                self.id_gen.next(),
                PhysicalProject::new(projections),
            ),
            children: vec![child],
        };

        let input_types = child.operator.call_output_types();

        let operator =
            PhysicalTableExecute::new(inout.node.function, additional_projections, input_types);

        Ok(PlannedOperatorWithChildren {
            operator: PlannedOperator::new_execute(self.id_gen.next(), operator),
            children: vec![child],
        })
    }
}

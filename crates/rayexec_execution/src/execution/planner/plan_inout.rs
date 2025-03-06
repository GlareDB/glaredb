use rayexec_error::{Result, ResultExt};

use super::{Materializations, OperatorPlanState};
use crate::execution::operators::project::PhysicalProject;
use crate::execution::operators::table_inout::PhysicalTableInOut;
use crate::execution::operators::PhysicalOperator;
use crate::expr::physical::column_expr::PhysicalColumnExpr;
use crate::logical::logical_inout::LogicalTableExecute;
use crate::logical::operator::{LogicalNode, Node};

impl OperatorPlanState<'_> {
    pub fn plan_inout(
        &mut self,
        materializations: &mut Materializations,
        mut inout: Node<LogicalTableExecute>,
    ) -> Result<()> {
        unimplemented!()
        // let input = inout.take_one_child_exact()?;
        // let input_refs = input.get_output_table_refs(self.bind_context);
        // self.walk(materializations, input)?;

        // let function_inputs = self
        //     .expr_planner
        //     .plan_scalars(&input_refs, &inout.node.function.positional)
        //     .context("Failed to plan input expressions for table inout")?;

        // let projected_outputs = self
        //     .expr_planner
        //     .plan_scalars(&input_refs, &inout.node.projected_outputs)
        //     .context("Failed to plan additional output expressions for table inout")?;

        // let input_types: Vec<_> = function_inputs
        //     .iter()
        //     .chain(projected_outputs.iter())
        //     .map(|expr| expr.datatype())
        //     .collect();

        // let projected_inputs: Vec<_> = projected_outputs
        //     .iter()
        //     .enumerate()
        //     .map(|(idx, expr)| PhysicalColumnExpr {
        //         idx: idx + function_inputs.len(),
        //         datatype: expr.datatype(),
        //     })
        //     .collect();

        // let projections = function_inputs
        //     .into_iter()
        //     .chain(projected_outputs.into_iter());

        // // Project function inputs first.
        // self.push_intermediate_operator(
        //     PhysicalOperator::Project(PhysicalProject::new(projections)),
        //     inout.location,
        // )?;

        // // Push inout
        // self.push_intermediate_operator(
        //     PhysicalOperator::TableInOut(PhysicalTableInOut {
        //         function: inout.node.function,
        //         input_types,
        //         projected_inputs,
        //     }),
        //     inout.location,
        // )?;

        // Ok(())
    }
}

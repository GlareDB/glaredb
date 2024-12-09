use std::sync::Arc;

use rayexec_error::{not_implemented, Result};

use super::{IntermediatePipelineBuildState, Materializations, PipelineIdGen};
use crate::execution::intermediate::pipeline::IntermediateOperator;
use crate::execution::operators::hash_aggregate::PhysicalHashAggregate;
use crate::execution::operators::union::PhysicalUnion;
use crate::execution::operators::PhysicalOperator;
use crate::logical::logical_setop::{LogicalSetop, SetOpKind};
use crate::logical::operator::Node;

impl IntermediatePipelineBuildState<'_> {
    pub fn plan_set_operation(
        &mut self,
        id_gen: &mut PipelineIdGen,
        materializations: &mut Materializations,
        mut setop: Node<LogicalSetop>,
    ) -> Result<()> {
        let location = setop.location;

        let [left, right] = setop.take_two_children_exact()?;
        let top = left;
        let bottom = right;

        // Continue building left/top.
        self.walk(materializations, id_gen, top)?;

        // Create new pipelines for bottom.
        let mut bottom_builder =
            IntermediatePipelineBuildState::new(self.config, self.bind_context);
        bottom_builder.walk(materializations, id_gen, bottom)?;
        self.local_group
            .merge_from_other(&mut bottom_builder.local_group);
        self.remote_group
            .merge_from_other(&mut bottom_builder.remote_group);

        let bottom_in_progress = bottom_builder.take_in_progress_pipeline()?;

        match setop.node.kind {
            SetOpKind::Union => {
                let operator = IntermediateOperator {
                    operator: Arc::new(PhysicalOperator::Union(PhysicalUnion)),
                    partitioning_requirement: None,
                };

                self.push_intermediate_operator(operator, location, id_gen)?;

                // The union operator is the "sink" for the bottom pipeline.
                self.push_as_child_pipeline(bottom_in_progress, 1)?;
            }
            other => not_implemented!("set op {other}"),
        }

        // Make output distinct by grouping on all columns. No output
        // aggregates, so the output schema remains the same.
        if !setop.node.all {
            let output_types = self
                .bind_context
                .get_table(setop.node.table_ref)?
                .column_types
                .clone();

            let grouping_sets = vec![(0..output_types.len()).collect()];

            let operator =
                IntermediateOperator {
                    operator: Arc::new(PhysicalOperator::HashAggregate(
                        PhysicalHashAggregate::new(Vec::new(), grouping_sets, Vec::new()),
                    )),
                    partitioning_requirement: None,
                };

            self.push_intermediate_operator(operator, location, id_gen)?;
        }

        Ok(())
    }
}

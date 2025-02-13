use std::collections::BTreeSet;

use rayexec_error::Result;

use super::{IntermediatePipelineBuildState, Materializations, PipelineIdGen};
use crate::execution::operators::hash_aggregate::PhysicalHashAggregate;
use crate::execution::operators::project::PhysicalProject;
use crate::execution::operators::PhysicalOperator;
use crate::logical::logical_distinct::LogicalDistinct;
use crate::logical::operator::{LogicalNode, Node};

impl IntermediatePipelineBuildState<'_> {
    pub fn plan_distinct(
        &mut self,
        id_gen: &mut PipelineIdGen,
        materializations: &mut Materializations,
        mut distinct: Node<LogicalDistinct>,
    ) -> Result<()> {
        let input = distinct.take_one_child_exact()?;
        let input_refs = input.get_output_table_refs(self.bind_context);
        self.walk(materializations, id_gen, input)?;

        // Create group expressions from the distinct.
        let group_types = distinct
            .node
            .on
            .iter()
            .map(|expr| expr.datatype(self.bind_context.get_table_list()))
            .collect::<Result<Vec<_>>>()?;
        let group_exprs = self
            .expr_planner
            .plan_scalars(&input_refs, &distinct.node.on)?;

        self.push_intermediate_operator(
            PhysicalOperator::Project(PhysicalProject::new(group_exprs)),
            distinct.location,
            id_gen,
        )?;

        let grouping_sets: Vec<BTreeSet<usize>> = vec![(0..group_types.len()).collect()];

        unimplemented!()
        // self.push_intermediate_operator(
        //     PhysicalOperator::HashAggregate(PhysicalHashAggregate::new(
        //         Vec::new(),
        //         grouping_sets,
        //         Vec::new(),
        //     )),
        //     distinct.location,
        //     id_gen,
        // )?;

        // Ok(())
    }
}

use std::sync::Arc;

use rayexec_error::Result;

use super::{InProgressPipeline, IntermediatePipelineBuildState, Materializations, PipelineIdGen};
use crate::execution::intermediate::pipeline::{
    IntermediateOperator,
    IntermediatePipeline,
    PipelineSink,
    PipelineSource,
};
use crate::execution::operators::PhysicalOperator;
use crate::logical::logical_order::LogicalOrder;
use crate::logical::operator::{LocationRequirement, LogicalNode, Node};

impl IntermediatePipelineBuildState<'_> {
    pub fn plan_sort(
        &mut self,
        id_gen: &mut PipelineIdGen,
        materializations: &mut Materializations,
        mut order: Node<LogicalOrder>,
    ) -> Result<()> {
        let location = order.location;

        let input = order.take_one_child_exact()?;
        let input_refs = input.get_output_table_refs(self.bind_context);
        self.walk(materializations, id_gen, input)?;

        let exprs = self
            .expr_planner
            .plan_sorts(&input_refs, &order.node.exprs)?;

        unimplemented!()
        // // Partition-local sorting.
        // let operator = IntermediateOperator {
        //     operator: Arc::new(PhysicalOperator::LocalSort(PhysicalScatterSort::new(
        //         exprs.clone(),
        //     ))),
        //     partitioning_requirement: None,
        // };
        // self.push_intermediate_operator(operator, location, id_gen)?;

        // // Global sorting.
        // let operator = IntermediateOperator {
        //     operator: Arc::new(PhysicalOperator::MergeSorted(PhysicalGatherSort::new(
        //         exprs,
        //     ))),
        //     partitioning_requirement: None,
        // };
        // self.push_intermediate_operator(operator, location, id_gen)?;

        // // Global sorting accepts n-partitions, but produces only a single
        // // partition. We finish the current pipeline

        // let in_progress = self.take_in_progress_pipeline()?;
        // self.in_progress = Some(InProgressPipeline {
        //     id: id_gen.next_pipeline_id(),
        //     operators: Vec::new(),
        //     location,
        //     source: PipelineSource::OtherPipeline {
        //         pipeline: in_progress.id,
        //         partitioning_requirement: Some(1),
        //     },
        // });

        // let pipeline = IntermediatePipeline {
        //     id: in_progress.id,
        //     sink: PipelineSink::InPipeline,
        //     source: in_progress.source,
        //     operators: in_progress.operators,
        // };
        // // TODO: This should not be happening here.
        // // https://github.com/GlareDB/glaredb/issues/3352
        // match location {
        //     LocationRequirement::ClientLocal => {
        //         self.local_group.pipelines.insert(pipeline.id, pipeline);
        //     }
        //     LocationRequirement::Remote => {
        //         self.remote_group.pipelines.insert(pipeline.id, pipeline);
        //     }
        //     LocationRequirement::Any => {
        //         // TODO
        //         self.local_group.pipelines.insert(pipeline.id, pipeline);
        //     }
        // }

        // Ok(())
    }
}

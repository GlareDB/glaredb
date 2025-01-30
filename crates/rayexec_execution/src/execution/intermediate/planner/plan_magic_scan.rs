
use rayexec_error::Result;

use super::{IntermediatePipelineBuildState, Materializations, PipelineIdGen};
use crate::logical::logical_materialization::LogicalMagicMaterializationScan;
use crate::logical::operator::Node;

impl IntermediatePipelineBuildState<'_> {
    pub fn plan_magic_materialize_scan(
        &mut self,
        id_gen: &mut PipelineIdGen,
        materializations: &mut Materializations,
        scan: Node<LogicalMagicMaterializationScan>,
    ) -> Result<()> {
        // if !materializations
        //     .local
        //     .materializations
        //     .contains_key(&scan.node.mat)
        // {
        //     return Err(RayexecError::new(format!(
        //         "Missing materialization for ref: {}",
        //         scan.node.mat
        //     )));
        // }

        // if self.in_progress.is_some() {
        //     return Err(RayexecError::new(
        //         "Expected in progress to be None for materialization scan",
        //     ));
        // }

        unimplemented!()
        // // Initialize in-progress with no operators, but scan source being this
        // // materialization.
        // self.in_progress = Some(InProgressPipeline {
        //     id: id_gen.next_pipeline_id(),
        //     operators: Vec::new(),
        //     location: LocationRequirement::ClientLocal, // Currently only support local.
        //     source: PipelineSource::Materialization {
        //         mat_ref: scan.node.mat,
        //     },
        // });

        // // Plan the projection out of the materialization.
        // let materialized_refs = &self
        //     .bind_context
        //     .get_materialization(scan.node.mat)?
        //     .table_refs;
        // let projections = self
        //     .expr_planner
        //     .plan_scalars(materialized_refs, &scan.node.projections)
        //     .context("Failed to plan projections out of materialization")?;
        // let operator = IntermediateOperator {
        //     operator: Arc::new(PhysicalOperator::Project(PhysicalProject { projections })),
        //     partitioning_requirement: None,
        // };

        // // TODO: Distinct the projection.

        // self.push_intermediate_operator(operator, scan.location, id_gen)?;

        // Ok(())
    }
}

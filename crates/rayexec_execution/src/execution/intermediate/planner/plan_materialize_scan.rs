use rayexec_error::{RayexecError, Result};

use super::{InProgressPipeline, IntermediatePipelineBuildState, Materializations, PipelineIdGen};
use crate::execution::intermediate::pipeline::PipelineSource;
use crate::logical::logical_materialization::LogicalMaterializationScan;
use crate::logical::operator::{LocationRequirement, Node};

impl IntermediatePipelineBuildState<'_> {
    pub fn plan_materialize_scan(
        &mut self,
        id_gen: &mut PipelineIdGen,
        materializations: &mut Materializations,
        scan: Node<LogicalMaterializationScan>,
    ) -> Result<()> {
        if !materializations
            .local
            .materializations
            .contains_key(&scan.node.mat)
        {
            return Err(RayexecError::new(format!(
                "Missing materialization for ref: {}",
                scan.node.mat
            )));
        }

        if self.in_progress.is_some() {
            return Err(RayexecError::new(
                "Expected in progress to be None for materialization scan",
            ));
        }

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

        // Ok(())
    }
}

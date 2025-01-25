
use rayexec_error::{RayexecError, Result};

use super::{IntermediatePipelineBuildState, PipelineIdGen};
use crate::execution::operators::empty::PhysicalEmpty;
use crate::execution::operators::PhysicalOperator;
use crate::logical::logical_empty::LogicalEmpty;
use crate::logical::operator::Node;

impl IntermediatePipelineBuildState<'_> {
    pub fn plan_empty(
        &mut self,
        id_gen: &mut PipelineIdGen,
        empty: Node<LogicalEmpty>,
    ) -> Result<()> {
        // "Empty" is a source of data by virtue of emitting a batch consisting
        // of no columns and 1 row.
        //
        // This enables expression evualtion to work without needing to special
        // case a query without a FROM clause. E.g. `SELECT 1+1` would execute
        // the expression `1+1` with the input being the batch with 1 row and no
        // columns.
        //
        // Because this this batch is really just to drive execution on an
        // expression with no input, we just hard the partitions for this
        // pipeline to 1.
        if self.in_progress.is_some() {
            return Err(RayexecError::new("Expected in progress to be None"));
        }

        // This has a partitioning requirement of 1 since it's only used to
        // drive output of a query that contains no FROM (typically just a
        // simple projection).
        let operator = PhysicalOperator::Empty(PhysicalEmpty);

        unimplemented!()
        // self.in_progress = Some(InProgressPipeline {
        //     id: id_gen.next_pipeline_id(),
        //     operators: vec![operator],
        //     location: empty.location,
        //     source: PipelineSource::InPipeline,
        // });

        // Ok(())
    }
}

use crate::execution::intermediate::pipeline::IntermediatePipelineId;
use super::partition_pipeline::ExecutablePartitionPipeline;
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};

/// A pipeline represents execution across a sequence of operators.
///
/// Pipelines are made of multiple partition pipelines, where all partition
/// pipelines are doing the same work across the same operators, just in a
/// different partition.
#[derive(Debug)]
pub struct ExecutablePipeline {
    /// ID of this pipeline. Unique to the query graph.
    ///
    /// Informational only.
    #[allow(dead_code)]
    pub(crate) pipeline_id: IntermediatePipelineId,
    /// Parition pipelines that make up this pipeline.
    pub(crate) partitions: Vec<ExecutablePartitionPipeline>,
}

impl ExecutablePipeline {
    pub fn into_partition_pipeline_iter(self) -> impl Iterator<Item = ExecutablePartitionPipeline> {
        self.partitions.into_iter()
    }
}

impl Explainable for ExecutablePipeline {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new(format!("Pipeline {}", self.pipeline_id))
    }
}

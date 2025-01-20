use std::collections::BTreeMap;
use std::fmt;
use std::time::Duration;

use super::partition_pipeline::ExecutablePartitionPipeline;
use crate::execution::intermediate::pipeline::IntermediatePipelineId;
use crate::explain::context_display::ContextDisplayMode;
use crate::explain::explainable::ExplainConfig;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExecutionProfileData {
    /// Profile data for all pipelines in this query.
    pub pipelines: BTreeMap<IntermediatePipelineId, PipelineProfileData>,
}

impl ExecutionProfileData {
    pub fn add_partition_data(&mut self, partition: &ExecutablePartitionPipeline) {
        let pipeline_data = self.pipelines.entry(partition.pipeline_id()).or_default();

        unimplemented!()
        // let partition_data = PartitionPipelineProfileData {
        //     operators: partition
        //         .operators()
        //         .iter()
        //         .map(|op| op.profile_data().clone())
        //         .collect(),
        //     explain_strings: partition
        //         .operators()
        //         .iter()
        //         .map(|op| {
        //             op.physical_operator()
        //                 .explain_entry(ExplainConfig {
        //                     context_mode: ContextDisplayMode::Raw,
        //                     verbose: false,
        //                 })
        //                 .to_string()
        //         })
        //         .collect(),
        // };

        // pipeline_data
        //     .partitions
        //     .insert(partition.partition(), partition_data);
    }
}

impl fmt::Display for ExecutionProfileData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (id, pipeline) in &self.pipelines {
            writeln!(f, "Pipeline {id:?}")?;

            for (id, partition) in &pipeline.partitions {
                writeln!(f, "  Partition {id}")?;

                #[allow(clippy::write_literal)]
                writeln!(
                    f,
                    "    [{:>2}]  {:>8}  {:>8}  {:>16}  {}",
                    "Op", "Read", "Emitted", "Elapsed (micro)", "Explain",
                )?;

                for (idx, (operator, explain)) in partition
                    .operators
                    .iter()
                    .zip(&partition.explain_strings)
                    .enumerate()
                {
                    writeln!(
                        f,
                        "    [{:>2}]  {:>8}  {:>8}  {:>16}  {}",
                        idx,
                        operator.rows_read,
                        operator.rows_emitted,
                        operator.elapsed.as_micros(),
                        explain,
                    )?;
                }
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PipelineProfileData {
    /// Profile data for all partitions in this pipeline.
    ///
    /// Keyed by the partition number within the pipeline.
    pub partitions: BTreeMap<usize, PartitionPipelineProfileData>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionPipelineProfileData {
    /// Profile data for all operators in this partition pipeline.
    pub operators: Vec<OperatorProfileData>,
    // TODO: This is here just to help debug. Evetually I want to just be able
    // to line up this data with the original plan using the pipeline/partition
    // ids.
    pub explain_strings: Vec<String>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct OperatorProfileData {
    /// Number of rows read into the operator.
    pub rows_read: usize,
    /// Number of rows produced by the operator.
    pub rows_emitted: usize,
    /// Elapsed time while activley executing this operator.
    pub elapsed: Duration,
}

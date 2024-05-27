pub mod planner;
pub mod explain;
pub mod sink;

use super::pipeline::{PartitionPipeline, Pipeline};

#[derive(Debug)]
pub struct QueryGraph {
    /// All pipelines that make up this query.
    pipelines: Vec<Pipeline>,
}

impl QueryGraph {
    pub fn into_partition_pipeline_iter(self) -> impl Iterator<Item = PartitionPipeline> {
        self.pipelines
            .into_iter()
            .flat_map(|pipeline| pipeline.into_partition_pipeline_iter())
    }
}

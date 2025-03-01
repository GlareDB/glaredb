use std::fmt;

use crate::execution::operators::{PhysicalOperator, PlannedOperator};

/// ID of a single intermediate pipeline.
///
/// Unique within a query.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct IntermediatePipelineId(pub usize);

impl fmt::Display for IntermediatePipelineId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PipelineSink {
    /// Pipeline sink is already part of the pipeline, no special handling
    /// needed.
    InPipeline,
    /// Pipeline sink is an operator in some other pipeline.
    OtherPipeline {
        id: IntermediatePipelineId,
        operator_idx: usize,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PipelineSource {
    /// Pipeline source is already part of the pipeline.
    InPipeline,
    /// Pipeline source is an operator in some other pipeline.
    ///
    /// This currently always point to a materialization.
    OtherPipeline {
        id: IntermediatePipelineId,
        operator_idx: usize,
    },
}

#[derive(Debug)]
pub struct IntermediatePipeline {
    pub(crate) id: IntermediatePipelineId,
    pub(crate) source: PipelineSource,
    pub(crate) sink: PipelineSink,
    pub(crate) operators: Vec<PlannedOperator>,
}

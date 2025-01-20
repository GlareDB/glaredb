use std::collections::HashMap;
use std::sync::Arc;

use rayexec_error::{OptionExt, Result};
use rayexec_proto::ProtoConv;
use uuid::Uuid;

use crate::database::DatabaseContext;
use crate::execution::operators::PhysicalOperator;
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::logical::binder::bind_context::MaterializationRef;
use crate::proto::DatabaseProtoConv;

/// ID of a single intermediate pipeline.
///
/// Unique within a query.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct IntermediatePipelineId(pub usize);

#[derive(Debug)]
pub enum PipelineSink {
    /// Pipeline sink is already part of the pipeline, no special handling
    /// needed.
    InPipeline,
    /// Pipeline sink is an operator in some other pipeline.
    OtherPipeline {
        id: IntermediatePipelineId,
        operator: usize,
    },
}

#[derive(Debug)]
pub struct IntermediatePipeline {
    pub(crate) sink: PipelineSink,
    pub(crate) id: IntermediatePipelineId,
    pub(crate) operators: Vec<PhysicalOperator>,
}

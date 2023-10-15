use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::record_batch::RecordBatch;
use std::sync::Arc;
use std::task::Poll;
use std::{fmt::Debug, task::Context};

use super::{DistExecError, Result};

pub trait Pipeline: Source + Sink {}

impl<P: Source + Sink> Pipeline for P {}

pub trait Source: Send + Sync + Debug {
    fn output_partitions(&self) -> usize;
    fn poll_partition(
        &self,
        cx: &mut Context<'_>,
        partition: usize,
    ) -> Poll<Option<Result<RecordBatch>>>;
}

pub trait Sink: Send + Sync + Debug {
    fn push(&self, input: RecordBatch, child: usize, partition: usize) -> Result<()>;
    fn finish(&self, child: usize, partition: usize) -> Result<()>;
}

pub trait ErrorSink: Send + Sync + Debug {
    fn push_error(&self, err: DistExecError, partition: usize) -> Result<()>;
}

/// A complete query pipeline representing a single execution plan.
#[derive(Debug)]
pub struct QueryPipeline {
    /// Output schema.
    pub schema: Arc<Schema>,

    /// Number of output partitions.
    pub output_partitions: usize,

    /// Intermediate pipelines for this query.
    pub stages: Vec<PipelineStage>,

    /// Output sink for the final result stream.
    pub sink: Arc<dyn Sink>,

    /// Output errors.
    pub error_sink: Arc<dyn ErrorSink>,
}

/// A part of a full query pipeline.
///
/// Represents a unit of work that should send its results to some output.
#[derive(Debug, Clone)]
pub struct PipelineStage {
    pub source: Arc<dyn Source>,
    pub output: Arc<dyn Sink>,
}

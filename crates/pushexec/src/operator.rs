use crate::errors::{PushExecError, Result};
use datafusion::{
    arrow::record_batch::RecordBatch,
    physical_plan::{metrics, repartition::BatchPartitioner, Partitioning},
};
use std::{
    pin::Pin,
    task::{Context, Poll, Waker},
};

pub struct Pipeline {
    pub sink: BoxedSink,
    pub operators: Vec<BoxedOperator>,
    pub source: Vec<BoxedSource>,
}

pub type BoxedOperator = Pin<Box<dyn Operator>>;

pub trait Operator: Sink + Source {}

pub type BoxedSink = Pin<Box<dyn Sink>>;

/// Accepts partitioned data.
pub trait Sink: Send {
    /// Push a partition to the sink.
    fn push_partition(&self, input: RecordBatch, partition: usize) -> Result<()>;

    fn input_partitions(&self) -> usize;

    /// Mark a partition as finished.
    fn finish(&self, partition: usize) -> Result<()>;
}

pub type BoxedSource = Pin<Box<dyn Source>>;

pub trait Source: Send {
    fn output_partitions(&self) -> usize;

    /// Poll for a partition.
    fn poll_partition(
        &self,
        cx: &mut Context,
        partition: usize,
    ) -> Poll<Option<Result<RecordBatch>>>;
}

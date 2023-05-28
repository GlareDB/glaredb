use crate::errors::{PushExecError, Result};
use datafusion::{
    arrow::record_batch::RecordBatch,
    physical_plan::{metrics, repartition::BatchPartitioner, Partitioning},
};
use std::{
    pin::Pin,
    task::{Context, Poll, Waker},
};

pub trait Pipeline: Sink + Source {}

#[derive(Debug, Clone, Copy)]
pub struct PushPartitionId {
    /// Index of the partition.
    pub idx: usize,
    /// The child pipeline that this partition is coming from.
    pub child: usize,
}

/// Accepts partitioned data.
pub trait Sink: Send {
    /// Push a partition to the sink.
    fn push_partition(&self, input: RecordBatch, partition: PushPartitionId) -> Result<()>;

    /// Mark a partition as finished.
    fn finish(&self, partition: PushPartitionId) -> Result<()>;
}

pub trait Source: Send {
    fn output_partitions(&self) -> usize;

    /// Poll for a partition.
    fn poll_partition(
        &self,
        cx: &mut Context,
        partition: usize,
    ) -> Poll<Option<Result<RecordBatch>>>;
}

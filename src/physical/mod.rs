//! Physical plans.

pub mod buffer;
pub mod filter;
pub mod hash_aggregate;
pub mod hash_join;
pub mod order;
pub mod projection;
pub mod ungrouped_aggregate;

#[cfg(test)]
mod test_util;

use arrow_array::RecordBatch;
use std::fmt::{self, Debug};
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::errors::Result;

pub trait Source: Sync + Send + Debug {
    /// Return the number of partitions this source outputs.
    fn output_partitions(&self) -> usize;

    /// Poll for the next batch for a partition.
    fn poll_partition(
        &self,
        cx: &mut Context<'_>,
        partition: usize,
    ) -> Poll<Option<Result<RecordBatch>>>;
}

pub trait Sink: Sync + Send + Debug {
    /// Push a partition batch to the sink.
    ///
    /// Child indicates which of the children of the pipeline are pushing to the
    /// sink. Most sinks accept only a single child, but sinks like hash join
    /// accept two children (0 -> left, 1 -> right).
    fn push(&self, input: RecordBatch, child: usize, partition: usize) -> Result<()>;

    /// Mark the partition as finished for the specific child.
    fn finish(&self, child: usize, partition: usize) -> Result<()>;
}

pub trait Operator: Source + Sink {}

pub struct Pipeline {
    /// Destiantion for all resulting record batches.
    destination: Box<dyn Sink>,

    /// Intermediate operators for the pipeline.
    operators: Vec<Arc<dyn Operator>>,

    /// Data sources for the pipeline.
    sources: Vec<Box<dyn Source>>,
}

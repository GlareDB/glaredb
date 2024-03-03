pub mod buffer;
pub mod cross_join;
pub mod empty_source;
pub mod filter;
pub mod hash_aggregate;
pub mod hash_join;
pub mod nested_loop_join;
pub mod order;
pub mod projection;
pub mod ungrouped_aggregate;
pub mod values;

mod util;

#[cfg(test)]
mod test_util;

use rayexec_error::Result;
use std::fmt::Debug;
use std::task::{Context, Poll};

use crate::types::batch::DataBatch;

pub trait Sink: Sync + Send + Debug {
    /// Number of input partitions this sink can handle.
    fn input_partitions(&self) -> usize;

    fn poll_ready(&self, cx: &mut Context, partition: usize) -> Poll<()>;

    fn push(&self, input: DataBatch, partition: usize) -> Result<()>;

    fn finish(&self, partition: usize) -> Result<()>;
}

pub trait Source: Sync + Send + Debug {
    /// Number of output partitions this source can produce.
    fn output_partitions(&self) -> usize;

    fn poll_next(&self, cx: &mut Context, partition: usize) -> Poll<Option<Result<DataBatch>>>;
}

pub trait PhysicalOperator: Sync + Send + Debug {
    /// Execute this operator on an input batch.
    fn execute(&self, input: DataBatch) -> Result<DataBatch>;
}

pub trait Source2: Sync + Send + Debug {
    /// Return the number of partitions this source outputs.
    fn output_partitions(&self) -> usize;

    /// Poll for the next batch for a partition.
    fn poll_partition(
        &self,
        cx: &mut Context<'_>,
        partition: usize,
    ) -> Poll<Option<Result<DataBatch>>>;
}

pub trait Sink2: Sync + Send + Debug {
    /// Poll to make sure this sink is ready to accept a batch for the given
    /// partition and child.
    ///
    /// This should be called before calling `push` to ensure that the operator
    /// is ready for input.
    ///
    /// This provides two things:
    ///
    /// - Pipeline backpressure to avoid things piling up in an operator.
    /// - Provide a mechanism for joins to prevent accepting input from the
    ///   probe side before the build phase is complete.
    fn poll_push_ready(&self, child: usize, partition: usize) -> Poll<()> {
        Poll::Ready(())
    }

    /// Push a partition batch to the sink.
    ///
    /// Child indicates which of the children of the pipeline are pushing to the
    /// sink. Most sinks accept only a single child, but sinks like hash join
    /// accept two children (0 -> left, 1 -> right).
    fn push(&self, input: DataBatch, child: usize, partition: usize) -> Result<()>;

    /// Mark the partition as finished for the specific child.
    fn finish(&self, child: usize, partition: usize) -> Result<()>;
}

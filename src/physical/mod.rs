//! Physical plans.

pub mod buffer;
pub mod filter;
pub mod hash_join;
pub mod projection;

use arrow_array::RecordBatch;
use std::fmt::{self, Debug};
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
    fn push(&self, input: RecordBatch, child: usize, partition: usize) -> Result<()>;
    fn finish(&self, child: usize, partition: usize) -> Result<()>;
}

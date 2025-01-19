use std::fmt::Debug;
use std::task::Context;

use rayexec_error::Result;

use crate::arrays::batch::Batch;
use crate::database::DatabaseContext;
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PollPull {
    /// Batch was pulled, keep pulling for more batches.
    HasMore,
    /// More work needs to be done to pull the batch.
    ///
    /// The same output batch will be provided on the next poll.
    Pending,
    /// Source is exhausted.
    ///
    /// Output batch will contain meaningful data.
    Exhausted,
}

pub trait SourceOperation: Debug + Send + Sync + Explainable {
    /// Create partition sources for this operation.
    ///
    /// This must return the exact number of sources for partitions.
    ///
    /// Called exactly once during physical planning.
    fn create_partition_sources(
        &mut self,
        context: &DatabaseContext,
        batch_size: usize,
        partitions: usize,
    ) -> Result<Vec<Box<dyn PartitionSource>>>;
}

pub trait PartitionSource: Debug + Send {
    /// Pull batches from this source.
    ///
    /// `output` will already be reset for writing. The source should write its
    /// data to `output` which will be pushed to the upstream operators.
    fn poll_pull(&mut self, cx: &mut Context, output: &mut Batch) -> Result<PollPull>;
}

impl SourceOperation for Box<dyn SourceOperation> {
    fn create_partition_sources(
        &mut self,
        context: &DatabaseContext,
        batch_size: usize,
        partitions: usize,
    ) -> Result<Vec<Box<dyn PartitionSource>>> {
        self.as_mut()
            .create_partition_sources(context, batch_size, partitions)
    }
}

impl Explainable for Box<dyn SourceOperation> {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        self.as_ref().explain_entry(conf)
    }
}

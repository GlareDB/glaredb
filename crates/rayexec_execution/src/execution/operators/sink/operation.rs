use std::fmt::Debug;
use std::task::Context;

use rayexec_error::Result;

use crate::arrays::batch::Batch;
use crate::database::DatabaseContext;
use crate::execution::operators::PollFinalize;
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PollPush {
    /// Batch was pushed, ready for a new input batch.
    Pushed,
    /// More work needs to be done to finishing pushing this batch.
    ///
    /// The same input batch will be provided on the next poll.
    Pending,
}

/// Describes and operation for asynchronously pushing batches somewhere.
pub trait SinkOperation: Debug + Send + Sync + Explainable {
    /// Create partition sinks for this operation.
    ///
    /// `partititions` will follow the partitioning requirement if set.
    fn create_partition_sinks(
        &mut self,
        context: &DatabaseContext,
        partitions: usize,
    ) -> Result<Vec<Box<dyn PartitionSink>>>;

    /// Optional partitiong requirment for this sink.
    ///
    /// The default implementaion does not set a requirement, and the number of partitions
    /// used with be whatever the value is for execution.
    fn partitioning_requirement(&self) -> Option<usize> {
        None
    }
}

pub trait PartitionSink: Debug + Send {
    /// Push a batch to this sink.
    fn poll_push(&mut self, cx: &mut Context, input: &mut Batch) -> Result<PollPush>;

    /// Finalize this sink.
    ///
    /// Called once only after all batches have been pushed. If there's any
    /// pending work that needs to happen (flushing), it should happen here.
    /// Once this returns, the sink is complete.
    ///
    /// This should not return `NeedsDrain`.
    fn poll_finalize(&mut self, cx: &mut Context) -> Result<PollFinalize>;
}

impl SinkOperation for Box<dyn SinkOperation> {
    fn create_partition_sinks(
        &mut self,
        context: &DatabaseContext,
        partitions: usize,
    ) -> Result<Vec<Box<dyn PartitionSink>>> {
        self.as_mut().create_partition_sinks(context, partitions)
    }

    fn partitioning_requirement(&self) -> Option<usize> {
        self.as_ref().partitioning_requirement()
    }
}

impl Explainable for Box<dyn SinkOperation> {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        self.as_ref().explain_entry(conf)
    }
}

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

pub trait SourceOperation: Debug + Send + Sync + Explainable + 'static {
    /// Create partition sources for this operation.
    ///
    /// This should try to return scans for efficiently reading the source. The
    /// number of scans should be at most `partitions`, but may be less. If
    /// less, then the source operator may try to internally distribute the
    /// batch results.
    ///
    /// Called exactly once during physical planning.
    fn create_partition_sources(
        &mut self,
        context: &DatabaseContext,
        projections: &Projections,
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
        projections: &Projections,
        batch_size: usize,
        partitions: usize,
    ) -> Result<Vec<Box<dyn PartitionSource>>> {
        self.as_mut()
            .create_partition_sources(context, projections, batch_size, partitions)
    }
}

impl Explainable for Box<dyn SourceOperation> {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        self.as_ref().explain_entry(conf)
    }
}

/// Scan projections.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Projections {
    /// Column indices to project out of the scan.
    pub column_indices: Vec<usize>,
}

impl Projections {
    pub fn new(columns: impl IntoIterator<Item = usize>) -> Self {
        Projections {
            column_indices: columns.into_iter().collect(),
        }
    }

    pub fn num_columns(&self) -> usize {
        self.column_indices.len()
    }
}

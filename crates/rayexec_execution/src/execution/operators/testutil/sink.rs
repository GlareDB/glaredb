use std::task::Context;

use rayexec_error::Result;

use crate::arrays::batch::Batch;
use crate::database::DatabaseContext;
use crate::execution::operators::sink::operation::{PartitionSink, PollPush, SinkOperation};
use crate::execution::operators::PollFinalize;
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};

/// Sink operation that collects all input batches.
#[derive(Debug)]
pub struct CollectingSinkOperation;

impl SinkOperation for CollectingSinkOperation {
    fn create_partition_sinks(
        &self,
        _context: &DatabaseContext,
        num_sinks: usize,
    ) -> Result<Vec<Box<dyn PartitionSink>>> {
        let sinks = (0..num_sinks)
            .map(|_| {
                Box::new(PartitionCollectingSink {
                    batches: Vec::new(),
                }) as _
            })
            .collect();

        Ok(sinks)
    }
}

impl Explainable for CollectingSinkOperation {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("CollectingSink")
    }
}

#[derive(Debug)]
pub struct PartitionCollectingSink {
    pub batches: Vec<Batch>,
}

impl PartitionSink for PartitionCollectingSink {
    fn poll_push(&mut self, _cx: &mut Context, input: &mut Batch) -> Result<PollPush> {
        let batch = Batch::try_new_from_other(input)?;
        self.batches.push(batch);

        Ok(PollPush::Pushed)
    }

    fn poll_finalize(&mut self, _cx: &mut Context) -> Result<PollFinalize> {
        Ok(PollFinalize::Finalized)
    }
}

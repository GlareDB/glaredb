use std::task::Context;

use rayexec_error::{RayexecError, Result};

use crate::arrays::batch::Batch;
use crate::database::DatabaseContext;
use crate::execution::operators::source::operation::{PartitionSource, PollPull, SourceOperation};
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};

/// Source implementation that returns static batches.
///
/// The batches that get returned for a partition is determined via:
/// `batch_idx % partition_idx == 0`.
#[derive(Debug)]
pub struct BatchesSource {
    pub batches: Vec<Batch>,
}

impl SourceOperation for BatchesSource {
    fn create_partition_sources(
        &mut self,
        _context: &DatabaseContext,
        batch_size: usize,
        partitions: usize,
    ) -> Result<Vec<Box<dyn PartitionSource>>> {
        let mut part_batches: Vec<_> = (0..partitions).map(|_| Vec::new()).collect();

        for (batch_idx, batch) in self.batches.iter_mut().enumerate() {
            let part_idx = batch_idx % partitions;
            let new_batch = Batch::new_from_other(batch)?;

            if new_batch.num_rows() > batch_size {
                return Err(RayexecError::new("Test batch num rows exceeds batch size"));
            }

            part_batches[part_idx].push(new_batch);
        }

        let part_states = part_batches
            .into_iter()
            .map(|batches| {
                Box::new(PartitionBatchSource {
                    curr_idx: 0,
                    batches,
                }) as _
            })
            .collect();

        Ok(part_states)
    }
}

impl Explainable for BatchesSource {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("BatchesSource")
    }
}

#[derive(Debug)]
pub struct PartitionBatchSource {
    curr_idx: usize,
    batches: Vec<Batch>,
}

impl PartitionSource for PartitionBatchSource {
    fn poll_pull(&mut self, _cx: &mut Context, output: &mut Batch) -> Result<PollPull> {
        if self.batches.len() == 0 {
            output.set_num_rows(0)?;
            return Ok(PollPull::Exhausted);
        }

        assert!(self.curr_idx < self.batches.len());

        output.clone_from_other(&mut self.batches[self.curr_idx])?;
        self.curr_idx += 1;

        if self.curr_idx >= self.batches.len() {
            Ok(PollPull::Exhausted)
        } else {
            Ok(PollPull::HasMore)
        }
    }
}

use super::{Sink, Source};
use crate::physical::plans::util::hash::{build_hashes, partition_for_hash};
use crate::physical::plans::util::take::take_indexes;
use crate::planner::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::types::batch::{ColumnHash, DataBatch};
use arrow_array::UInt64Array;
use parking_lot::Mutex;
use rayexec_error::{RayexecError, Result};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Waker};

#[derive(Debug)]
pub struct PhysicalHashRepartition {
    /// States for the output partitions.
    // TODO: Probably just replace these with channels.
    output_states: Vec<Mutex<OutputPartitionState>>,

    /// Number of input partition.
    input_partitions: usize,

    /// Number of input partitions that are still sending input.
    remaining_inputs: AtomicUsize,

    /// Column indexes to hash.
    columns: Vec<usize>,
    // TODO: Approx buffered atomic for back pressure. Maybe
}

#[derive(Debug)]
struct OutputPartitionState {
    /// If this partition's inputs are finished.
    finished: bool,

    /// Pending waker on the pull side.
    pending: Option<Waker>,

    /// Batches with their hashes computed.
    batches: VecDeque<DataBatch>,
}

impl PhysicalHashRepartition {
    pub fn new(input_partitions: usize, output_partitions: usize, columns: Vec<usize>) -> Self {
        let output_states = (0..output_partitions)
            .map(|_| {
                Mutex::new(OutputPartitionState {
                    finished: false,
                    pending: None,
                    batches: VecDeque::new(),
                })
            })
            .collect();

        PhysicalHashRepartition {
            output_states,
            input_partitions,
            remaining_inputs: AtomicUsize::new(input_partitions),
            columns,
        }
    }
}

impl Source for PhysicalHashRepartition {
    fn output_partitions(&self) -> usize {
        self.output_states.len()
    }

    fn poll_next(&self, cx: &mut Context, partition: usize) -> Poll<Option<Result<DataBatch>>> {
        let mut state = self.output_states[partition].lock();
        match state.batches.pop_front() {
            Some(batch) => Poll::Ready(Some(Ok(batch))),
            None => {
                if state.finished {
                    Poll::Ready(None)
                } else {
                    state.pending = Some(cx.waker().clone());
                    Poll::Pending
                }
            }
        }
    }
}

impl Sink for PhysicalHashRepartition {
    fn input_partitions(&self) -> usize {
        self.input_partitions
    }

    fn poll_ready(&self, _cx: &mut Context, _partition: usize) -> Poll<()> {
        // TODO: Idk
        Poll::Ready(())
    }

    fn push(&self, input: DataBatch, _partition: usize) -> Result<()> {
        // TODO: Maybe don't allocate this for every input partition.
        let mut hashes = Vec::with_capacity(input.num_rows());
        let arrs: Vec<_> = self
            .columns
            .iter()
            .map(|idx| input.column(*idx).unwrap())
            .collect();
        build_hashes(&arrs, &mut hashes)?;

        let partitions = self.output_partitions();

        // Per-partition row indexes.
        let mut row_indexes: Vec<Vec<usize>> = (0..partitions)
            .map(|_partition| Vec::with_capacity(input.num_rows() / partitions))
            .collect();

        for (row_idx, hash) in hashes.iter().enumerate() {
            row_indexes[partition_for_hash(*hash, partitions)].push(row_idx);
        }

        for (partition_idx, partition_rows) in row_indexes.into_iter().enumerate() {
            // Get the hashes corresponding to the rows for this output
            // partition.
            let batch_hashes = take_indexes(&hashes, &partition_rows);
            let column_hash = ColumnHash::new(&self.columns, batch_hashes);

            // Because arrow
            let partition_rows =
                UInt64Array::from_iter(partition_rows.into_iter().map(|idx| idx as u64));

            // Get the rows for this batch.
            let cols = input
                .columns()
                .iter()
                .map(|col| arrow::compute::take(col.as_ref(), &partition_rows, None))
                .collect::<Result<Vec<_>, _>>()?;

            let output_batch = DataBatch::try_new_with_column_hash(cols, column_hash)?;

            let mut output_state = self.output_states[partition_idx].lock();
            output_state.batches.push_back(output_batch);

            if let Some(waker) = output_state.pending.take() {
                waker.wake();
            }
        }

        Ok(())
    }

    fn finish(&self, _partition: usize) -> Result<()> {
        let prev = self.remaining_inputs.fetch_add(1, Ordering::SeqCst);
        if prev == 1 {
            // If we're the last partition to finish, go ahead and mark all the
            // outputs as finished and wake up any pending wakers.
            let output_states = self.output_states.iter().map(|s| s.lock());
            for mut state in output_states {
                assert!(!state.finished);
                state.finished = true;
                if let Some(waker) = state.pending.take() {
                    waker.wake();
                }
            }
        }
        Ok(())
    }
}

impl Explainable for PhysicalHashRepartition {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("PhysicalHashRepartition")
    }
}

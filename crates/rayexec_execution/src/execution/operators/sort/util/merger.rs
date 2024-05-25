use rayexec_bullet::batch::Batch;
use rayexec_error::{RayexecError, Result};
use std::{
    cmp::{Ordering, Reverse},
    collections::BinaryHeap,
};

use super::{accumulator::IndicesAccumulator, sorted_batch::RowReference};

#[derive(Debug)]
pub enum MergeResult {
    /// We have a merged batch.
    ///
    /// Nothing else needed before the next call to `try_merge`.
    Batch(Batch),

    /// Need to push a new batch for the input at the given index.
    ///
    /// `push_batch_for_input` should be called before the next call to
    /// `try_merge`.
    NeedsInput(usize),

    /// No more outputs will be produced.
    Exhausted,
}

#[derive(Debug)]
pub enum IterState<I> {
    /// Normal state, we just need to iterate.
    Iterator(I),

    /// Input is finished, we don't need to account for this iterator anymore.
    Finished,
}

/// Merge k inputs into totally sorted outputs.
#[derive(Debug)]
pub struct KWayMerger<I> {
    /// Accumulator for the interleave indices.
    acc: IndicesAccumulator,

    /// Heap containing the heads of all batches we're sorting.
    ///
    /// This heap contains at most one row reference for each batch. This row
    /// reference indicates the "head" of the sorted batch. When a row reference
    /// is popped, the next row reference for that same batch should be pushed
    /// onto the heap.
    heap: BinaryHeap<Reverse<InputRowReference>>,

    /// Iterators for getting row references. This iterator should return rows
    /// in order.
    ///
    /// Indexed by input idx.
    ///
    /// None indicates no more batches for the input.
    row_reference_iters: Vec<IterState<I>>,

    /// If we're needing additional input.
    ///
    /// This is for debugging only.
    needs_input: bool,
}

impl<I> KWayMerger<I>
where
    I: Iterator<Item = RowReference>,
{
    /// Initialize the merger with the provided inputs.
    ///
    /// The vec contains (iter, first_batch) pairs which will be used to
    /// inialize the heap.
    ///
    /// The initial heap will be created from the first element of each
    /// iterator. If an input is never expected to produce references, its iter
    /// state should be Finished and the batch should be None.
    pub fn try_new(inputs: Vec<(Option<Batch>, IterState<I>)>) -> Result<Self> {
        let mut heap = BinaryHeap::new();
        let mut iters = Vec::with_capacity(inputs.len());
        let mut acc = IndicesAccumulator::new(inputs.len());

        for (input_idx, (mut first_batch, iter)) in inputs.into_iter().enumerate() {
            match iter {
                IterState::Iterator(mut iter) => match iter.next() {
                    Some(reference) => {
                        let batch = match first_batch.take() {
                            Some(batch) => batch,
                            None => {
                                return Err(RayexecError::new(
                                    "Expected first batch to be Some if iter exists",
                                ))
                            }
                        };
                        acc.push_input_batch(input_idx, batch);
                        heap.push(Reverse(InputRowReference {
                            input_idx,
                            row_reference: reference,
                        }));
                        iters.push(IterState::Iterator(iter));
                    }
                    None => {
                        return Err(RayexecError::new(
                            "Expected iterators to produce at least one row reference",
                        ))
                    }
                },
                IterState::Finished => {
                    if first_batch.is_some() {
                        return Err(RayexecError::new(
                            "Expected first batch to be None if iter produces no row references",
                        ));
                    }
                    iters.push(IterState::Finished);
                    // Continue, we'll just never receive inputs at this index.
                    //
                    // This would happen in the case of a global sort where 1 or
                    // more partitions don't produce sorted batches (i.e. not
                    // enough batches to repartition evenly).
                }
            }
        }

        Ok(KWayMerger {
            acc,
            heap,
            row_reference_iters: iters,
            needs_input: false,
        })
    }

    pub fn num_inputs(&self) -> usize {
        self.row_reference_iters.len()
    }

    /// Push a batch and iterator for an input.
    pub fn push_batch_for_input(&mut self, input: usize, batch: Batch, mut iter: I) -> Result<()> {
        assert!(self.needs_input);

        self.needs_input = false;
        self.acc.push_input_batch(input, batch);

        let row_reference = match iter.next() {
            Some(reference) => reference,
            None => return Err(RayexecError::new("Unexpected empty iterator")),
        };
        self.heap.push(Reverse(InputRowReference {
            input_idx: input,
            row_reference,
        }));

        self.row_reference_iters[input] = IterState::Iterator(iter);

        Ok(())
    }

    /// Marks an input as finished.
    ///
    /// During merge, there will be no attempts to continue to read rows for
    /// this partition.
    pub fn input_finished(&mut self, input: usize) {
        assert!(self.needs_input);

        self.needs_input = false;
        self.row_reference_iters[input] = IterState::Finished;
    }

    /// Try to merge the inputs, attempting to create a batch of size
    /// `batch_size`.
    ///
    /// If one of the inputs runs out of rows, the index of the input will be
    /// returned. `push_batch_for_input` or `input_finished` should be called
    /// before trying to continue the merge.
    pub fn try_merge(&mut self, batch_size: usize) -> Result<MergeResult> {
        let remaining = batch_size - self.acc.len();

        assert!(!self.needs_input, "additional input needed");

        for _ in 0..remaining {
            // TODO: If the heap only contains a single row reference, we know
            // that there's only one batch we'll be pulling from. We should just
            // short circuit in that case.

            let reference = match self.heap.pop() {
                Some(r) => r,
                None => break, // Heap empty, we're done. Break and try to build.
            };

            self.acc
                .append_row_to_indices(reference.0.input_idx, reference.0.row_reference.row_idx);

            match &mut self.row_reference_iters[reference.0.input_idx] {
                IterState::Iterator(iter) => match iter.next() {
                    Some(row_reference) => self.heap.push(Reverse(InputRowReference {
                        input_idx: reference.0.input_idx,
                        row_reference,
                    })),
                    None => {
                        self.needs_input = true;
                        return Ok(MergeResult::NeedsInput(reference.0.input_idx));
                    }
                },
                IterState::Finished => (), // Just continue. No more batches from this input.
            }
        }

        match self.acc.build()? {
            Some(batch) => Ok(MergeResult::Batch(batch)),
            None => Ok(MergeResult::Exhausted),
        }
    }
}

/// A row reference with an additional input index which allows us to map the
/// row reference to some batch.
///
/// `Eq` and `Ord` delegate to the row reference.
#[derive(Debug)]
struct InputRowReference {
    input_idx: usize,
    row_reference: RowReference,
}

impl PartialEq for InputRowReference {
    fn eq(&self, other: &Self) -> bool {
        self.row_reference == other.row_reference
    }
}

impl Eq for InputRowReference {}

impl PartialOrd for InputRowReference {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.row_reference.partial_cmp(&other.row_reference)
    }
}

impl Ord for InputRowReference {
    fn cmp(&self, other: &Self) -> Ordering {
        self.row_reference.cmp(&other.row_reference)
    }
}

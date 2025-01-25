use std::sync::Arc;

use rayexec_error::{RayexecError, Result};

use crate::arrays::array::buffer_manager::NopBufferManager;
use crate::arrays::array::Array;
use crate::arrays::batch::Batch;
use crate::arrays::compute::interleave::interleave;

/// Tracks the state per input into the merge.
#[derive(Debug, Clone)]
struct InputState {
    /// Index of the batch.
    batch_idx: usize,
}

/// Accumulate fill mapping indices from multiple inputs to produce a sorted
/// batch output.
#[derive(Debug)]
pub struct IndicesAccumulator {
    /// Batches we're using for the build.
    batches: Vec<(usize, Batch)>,
    /// States for each input we're reading from.
    states: Vec<InputState>,
    /// Interleave indices referencing the stored batches.
    indices: Vec<(usize, usize)>,
}

impl IndicesAccumulator {
    pub fn new(num_inputs: usize) -> Self {
        IndicesAccumulator {
            batches: Vec::new(),
            states: (0..num_inputs)
                .map(|_| InputState { batch_idx: 0 })
                .collect(),
            indices: Vec::new(),
        }
    }

    /// Push a new batch for an input.
    ///
    /// The inputs's state will be updated to point to the beginning of this
    /// batch (making any previous batches pushed for this input unreachable).
    pub fn push_input_batch(&mut self, input: usize, batch: Batch) {
        let idx = self.batches.len();
        self.batches.push((input, batch));
        self.states[input] = InputState { batch_idx: idx };
    }

    /// Appends a row to interleave indices using the current state of the
    /// provided input.
    pub fn append_row_to_indices(&mut self, input: usize, row: usize) {
        let state = &mut self.states[input];
        self.indices.push((state.batch_idx, row));
    }

    pub fn len(&self) -> usize {
        self.indices.len()
    }

    /// Build a batch from the accumulated interleave indices.
    ///
    /// Internally drops batches that will no longer be part of the output.
    // TODO: Output batch should be passed in.
    pub fn build(&mut self) -> Result<Option<Batch>> {
        if self.indices.is_empty() {
            return Ok(None);
        }

        // If we have indices, we should have at least one batch.
        let num_cols = self.num_columns()?;

        let merged = (0..num_cols)
            .map(|col_idx| {
                let cols: Vec<_> = self
                    .batches
                    .iter()
                    .map(|(_, batch)| batch.array(col_idx).expect("column to exist"))
                    .collect();

                let datatype = cols.first().expect("at least one array").datatype().clone();
                let mut out =
                    Array::try_new(&NopBufferManager, datatype, self.indices.len())?;

                interleave(&cols, &self.indices, &mut out)?;

                Ok(out)
            })
            .collect::<Result<Vec<_>>>()?;
        self.indices.clear();

        let batch = Batch::try_from_arrays(merged)?;

        // Drops batches that are no longer reachable (won't be contributing to
        // the output).
        let mut retained = 0;
        let mut curr_idx = 0;
        self.batches.retain(|(input, _batch)| {
            let state = &mut self.states[*input];
            let latest = state.batch_idx == curr_idx;
            curr_idx += 1;

            if latest {
                // Keep batch, adjust batch index to point to the new position.
                state.batch_idx = retained;
                retained += 1;
                true
            } else {
                // Drop batch...
                false
            }
        });

        Ok(Some(batch))
    }

    /// Return the number of columns in the ouput.
    ///
    /// Errors if there's no buffered batches.
    fn num_columns(&self) -> Result<usize> {
        match self.batches.first() {
            Some((_, b)) => Ok(b.num_arrays()),
            None => Err(RayexecError::new("Cannot get number of columns")),
        }
    }
}

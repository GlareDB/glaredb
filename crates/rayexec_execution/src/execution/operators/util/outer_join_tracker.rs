use std::sync::Arc;

use crate::arrays::array::{Array, ArrayData};
use crate::arrays::batch::Batch;
use crate::arrays::bitmap::Bitmap;
use crate::arrays::datatype::DataType;
use crate::arrays::selection::SelectionVector;
use rayexec_error::Result;

/// Bitmaps corresponding to rows in the batches collected on the left side of
/// the join.
///
/// During probing, bitmaps will be updated to mark rows as having been visited.
///
/// Each partition (thread) will have its own set of bitmaps that will then be
/// merged at the end to produce a final set of bitmaps. This final set of
/// bitmaps will be used to determine which rows we need to emit in the case of
/// LEFT joins.
#[derive(Debug, Clone)]
pub struct LeftOuterJoinTracker {
    bitmaps: Vec<Bitmap>,
}

impl LeftOuterJoinTracker {
    pub fn new_for_batches(batches: &[Batch]) -> Self {
        let bitmaps = batches
            .iter()
            .map(|b| Bitmap::new_with_all_false(b.num_rows()))
            .collect();

        LeftOuterJoinTracker { bitmaps }
    }

    pub fn merge_from(&mut self, other: &LeftOuterJoinTracker) {
        debug_assert_eq!(self.bitmaps.len(), other.bitmaps.len());

        for (a, b) in self.bitmaps.iter_mut().zip(other.bitmaps.iter()) {
            a.bit_or_mut(b).expect("both bitmaps to be the same length");
        }
    }

    pub fn mark_rows_visited_for_batch(
        &mut self,
        batch_idx: usize,
        visited_rows: impl IntoIterator<Item = usize>,
    ) {
        let bitmap = self.bitmaps.get_mut(batch_idx).expect("bitmap to exist");
        for row in visited_rows {
            bitmap.set_unchecked(row, true);
        }
    }
}

/// Global drain state for draining remaining rows from the left batches.
#[derive(Debug)]
pub struct LeftOuterJoinDrainState {
    tracker: LeftOuterJoinTracker,
    /// All batches from the left side.
    batches: Vec<Batch>,
    /// Types for the right side of the join. Used to create the (typed) null
    /// columns for left rows that weren't visited.
    right_types: Vec<DataType>,
    /// Current batch we're draining.
    batch_idx: usize,
    /// How many batches to skip on each iteration.
    ///
    /// This should be set to the number of partitions draining, and lets us
    /// have multiple drains at the same time as each partition will visit
    /// different sets of batches.
    skip: usize,
}

impl LeftOuterJoinDrainState {
    pub fn new(
        start_idx: usize,
        skip: usize,
        tracker: LeftOuterJoinTracker,
        batches: Vec<Batch>,
        right_types: Vec<DataType>,
    ) -> Self {
        LeftOuterJoinDrainState {
            tracker,
            batches,
            right_types,
            batch_idx: start_idx,
            skip,
        }
    }

    /// Drains the next batch from the left, and appends a boolean column
    /// representing which rows were visited.
    pub fn drain_mark_next(&mut self) -> Result<Option<Batch>> {
        let batch = match self.batches.get(self.batch_idx) {
            Some(batch) => batch,
            None => return Ok(None),
        };
        let bitmap = self
            .tracker
            .bitmaps
            .get(self.batch_idx)
            .expect("bitmap to exist");
        self.batch_idx += self.skip;

        let cols = batch
            .columns()
            .iter()
            .cloned()
            .chain([Array::new_with_array_data(
                DataType::Boolean,
                ArrayData::Boolean(Arc::new(bitmap.clone().into())),
            )]);

        let batch = Batch::try_new(cols)?;

        Ok(Some(batch))
    }

    /// Drain the next batch.
    ///
    /// This will filter out rows that have been visited, and join the remaining
    /// rows will null columns on the right.
    pub fn drain_next(&mut self) -> Result<Option<Batch>> {
        loop {
            let batch = match self.batches.get(self.batch_idx) {
                Some(batch) => batch,
                None => return Ok(None),
            };
            let bitmap = self
                .tracker
                .bitmaps
                .get(self.batch_idx)
                .expect("bitmap to exist");
            self.batch_idx += self.skip;

            // TODO: Don't clone. Also might make sense to have the bitmap logic
            // flipped to avoid the negate here (we're already doing that for RIGHT
            // joins).
            let mut bitmap = bitmap.clone();
            bitmap.bit_negate();

            // Create a selection for just the unvisited rows in the left batch.
            let selection = SelectionVector::from_iter(bitmap.index_iter());
            let num_rows = selection.num_rows();

            if num_rows == 0 {
                // Try the next batch.
                continue;
            }

            let left_cols = batch.select(Arc::new(selection)).into_arrays();
            let right_cols = self
                .right_types
                .iter()
                .map(|datatype| Array::new_typed_null_array(datatype.clone(), num_rows))
                .collect::<Result<Vec<_>>>()?;

            let batch = Batch::try_new(left_cols.into_iter().chain(right_cols))?;

            return Ok(Some(batch));
        }
    }

    pub fn drain_semi_next(&mut self) -> Result<Option<Batch>> {
        loop {
            let batch = match self.batches.get(self.batch_idx) {
                Some(batch) => batch,
                None => return Ok(None),
            };
            let bitmap = self
                .tracker
                .bitmaps
                .get(self.batch_idx)
                .expect("bitmap to exist");
            self.batch_idx += self.skip;

            // Create a selection for just the visited rows in the left batch.
            let selection = SelectionVector::from_iter(bitmap.index_iter());
            let num_rows = selection.num_rows();

            if num_rows == 0 {
                // Try the next batch.
                continue;
            }

            let left_cols = batch.select(Arc::new(selection)).into_arrays();
            let right_cols = self
                .right_types
                .iter()
                .map(|datatype| Array::new_typed_null_array(datatype.clone(), num_rows))
                .collect::<Result<Vec<_>>>()?;

            let batch = Batch::try_new(left_cols.into_iter().chain(right_cols))?;

            return Ok(Some(batch));
        }
    }
}

/// Track visited rows on the right side of a join.
///
/// This tracker should be created per batch on the right side. No global state
/// is required.
#[derive(Debug)]
pub struct RightOuterJoinTracker {
    /// Bitmap indicating which rows we haven't visited.
    pub unvisited: Bitmap,
}

impl RightOuterJoinTracker {
    /// Create a new tracker for the provided batch.
    pub fn new_for_batch(batch: &Batch) -> Self {
        RightOuterJoinTracker {
            unvisited: Bitmap::new_with_all_true(batch.num_rows()),
        }
    }

    /// Mark the given row indices as visited.
    pub fn mark_rows_visited(&mut self, visited_rows: impl IntoIterator<Item = usize>) {
        for idx in visited_rows {
            self.unvisited.set_unchecked(idx, false);
        }
    }

    /// Return a batch with unvisited rows in the batch.
    ///
    /// `right` should be the batch used to create this tracker.
    ///
    /// `left_types` is used to create typed null columns for the left side of
    /// the batch.
    ///
    /// Returns None if all row on the right were visited.
    pub fn into_unvisited(self, left_types: &[DataType], right: &Batch) -> Result<Option<Batch>> {
        let selection = SelectionVector::from_iter(self.unvisited.index_iter());
        let num_rows = selection.num_rows();
        if num_rows == 0 {
            return Ok(None);
        }

        let right_cols = right.select(Arc::new(selection)).into_arrays();

        let left_null_cols = left_types
            .iter()
            .map(|datatype| Array::new_typed_null_array(datatype.clone(), num_rows))
            .collect::<Result<Vec<_>>>()?;

        let batch = Batch::try_new(left_null_cols.into_iter().chain(right_cols))?;

        Ok(Some(batch))
    }
}

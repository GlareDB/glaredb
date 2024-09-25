use std::sync::Arc;

use rayexec_bullet::{
    array::{Array, BooleanArray},
    batch::Batch,
    bitmap::Bitmap,
    compute::{self, filter::filter},
    datatype::DataType,
};
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
            .map(|b| Bitmap::all_false(b.num_rows()))
            .collect();

        LeftOuterJoinTracker { bitmaps }
    }

    pub fn num_batches(&self) -> usize {
        self.bitmaps.len()
    }

    pub fn merge_from(&mut self, other: &LeftOuterJoinTracker) {
        debug_assert_eq!(self.bitmaps.len(), other.bitmaps.len());

        for (a, b) in self.bitmaps.iter_mut().zip(other.bitmaps.iter()) {
            a.bit_or_mut(b).expect("both bitmaps to be the same length");
        }
    }

    pub fn mark_rows_visited_for_batch(&mut self, batch_idx: usize, rows: &[usize]) {
        let bitmap = self.bitmaps.get_mut(batch_idx).expect("bitmap to exist");
        for row in rows {
            bitmap.set(*row, true);
        }
    }
}

/// Global drain state for draining remaining rows from the left batches.
#[derive(Debug)]
pub struct LeftOuterJoinDrainState {
    tracker: LeftOuterJoinTracker,
    /// All batches from the left side.
    batches: Vec<Batch>,
    left_types: Vec<DataType>,
    right_types: Vec<DataType>,
    /// Current batch we're draining.
    batch_idx: usize,
}

impl LeftOuterJoinDrainState {
    pub fn new(
        tracker: LeftOuterJoinTracker,
        batches: Vec<Batch>,
        left_types: Vec<DataType>,
        right_types: Vec<DataType>,
    ) -> Self {
        LeftOuterJoinDrainState {
            tracker,
            batches,
            left_types,
            right_types,
            batch_idx: 0,
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
        self.batch_idx += 1;

        let cols = batch
            .columns()
            .iter()
            .cloned()
            .chain([Arc::new(Array::Boolean(BooleanArray::new(
                bitmap.clone(),
                None,
            )))]);

        let batch = Batch::try_new(cols)?;

        Ok(Some(batch))
    }

    /// Drain the next batch.
    ///
    /// This will filter out rows that have been visited, and join the remaining
    /// rows will null columns on the right.
    pub fn drain_next(&mut self) -> Result<Option<Batch>> {
        let batch = match self.batches.get(self.batch_idx) {
            Some(batch) => batch,
            None => return Ok(None),
        };
        let bitmap = self
            .tracker
            .bitmaps
            .get(self.batch_idx)
            .expect("bitmap to exist");
        self.batch_idx += 1;

        let num_rows = bitmap.len() - bitmap.count_trues();

        // TODO: Don't clone. Also might make sense to have the bitmap logic
        // flipped to avoid the negate here (we're already doing that for RIGHT
        // joins).
        let mut bitmap = bitmap.clone();
        bitmap.bit_negate();

        // TODO: We could just skip this batch.
        if num_rows == 0 {
            let cols = self
                .left_types
                .iter()
                .chain(self.right_types.iter())
                .map(|t| Array::new_nulls(t, 0));
            let batch = Batch::try_new(cols)?;

            return Ok(Some(batch));
        }

        let left_cols = batch
            .columns()
            .iter()
            .map(|c| filter(c, &bitmap))
            .collect::<Result<Vec<_>>>()?;

        let right_cols = self
            .right_types
            .iter()
            .map(|t| Array::new_nulls(t, num_rows));

        let batch = Batch::try_new(left_cols.into_iter().chain(right_cols))?;

        Ok(Some(batch))
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
            unvisited: Bitmap::all_true(batch.num_rows()),
        }
    }

    /// Mark the given row indices as visited.
    pub fn mark_rows_visited(&mut self, rows: &[usize]) {
        for &idx in rows {
            self.unvisited.set(idx, false);
        }
    }

    /// Return a batch with unvisited rows in the batch.
    ///
    /// `right` should be the batch used to create this tracker.
    ///
    /// `left_types` is used to create typed null columns for the left side of
    /// the batch.
    pub fn into_unvisited(self, left_types: &[DataType], right: &Batch) -> Result<Batch> {
        let unvisited_count = self.unvisited.count_trues();

        let right_unvisited = right
            .columns()
            .iter()
            .map(|a| compute::filter::filter(a, &self.unvisited))
            .collect::<Result<Vec<_>>>()?;

        let left_null_cols = left_types
            .iter()
            .map(|t| Array::new_nulls(t, unvisited_count));

        Batch::try_new(left_null_cols.chain(right_unvisited))
    }
}

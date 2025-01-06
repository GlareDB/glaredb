use std::cmp::Ordering;
use std::fmt;
use std::sync::Arc;

use crate::arrays::batch::Batch2;
use crate::arrays::row::encoding::{ComparableRow, ComparableRows};

/// A batch that's been physically sorted.
///
/// Note that constructing this will not check that the batch is actually
/// sorted.
#[derive(Debug)]
pub struct PhysicallySortedBatch {
    /// The sorted batch.
    pub batch: Batch2,

    /// The sorted keys.
    pub keys: ComparableRows,
}

impl PhysicallySortedBatch {
    pub fn into_batch_and_iter(self) -> (Batch2, SortedKeysIter) {
        let iter = SortedKeysIter {
            row_idx: 0,
            keys: Arc::new(self.keys),
        };

        (self.batch, iter)
    }
}

#[derive(Debug)]
pub struct SortedKeysIter {
    row_idx: usize,
    keys: Arc<ComparableRows>,
}

impl Iterator for SortedKeysIter {
    type Item = RowReference;

    fn next(&mut self) -> Option<Self::Item> {
        if self.row_idx >= self.keys.num_rows() {
            return None;
        }
        let row_idx = self.row_idx;
        self.row_idx += 1;

        Some(RowReference {
            rows: self.keys.clone(),
            row_idx,
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.keys.num_rows() - self.row_idx;
        (len, Some(len))
    }
}

/// A logically sorted batch.
///
/// This doens't store a sorted batch itself, but instead stores row indices
/// which would result in a sorted batch.
///
/// Note that constructing this will not check that the indices actually lead to
/// a sorted batch.
#[derive(Debug)]
pub struct IndexSortedBatch {
    /// Indices of rows in sort order.
    pub sort_indices: Vec<usize>,
    /// Unsorted keys for the batch.
    pub keys: ComparableRows,
    /// The original unsorted batch.
    pub batch: Batch2,
}

impl IndexSortedBatch {
    pub fn into_batch_and_iter(self) -> (Batch2, SortedIndicesIter) {
        let iter = SortedIndicesIter {
            indices: self.sort_indices,
            idx: 0,
            keys: Arc::new(self.keys),
        };

        (self.batch, iter)
    }
}

#[derive(Debug)]
pub struct SortedIndicesIter {
    indices: Vec<usize>,
    idx: usize,
    keys: Arc<ComparableRows>,
}

impl Iterator for SortedIndicesIter {
    type Item = RowReference;

    fn next(&mut self) -> Option<Self::Item> {
        if self.idx >= self.indices.len() {
            return None;
        }

        let row_idx = self.indices[self.idx];
        self.idx += 1;

        Some(RowReference {
            rows: self.keys.clone(),
            row_idx,
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.indices.len() - self.idx;
        (len, Some(len))
    }
}

/// A reference to row in a sorted batch.
///
/// The `Ord` and `Eq` implementations only takes into account the row key, and
/// not the batch index or row index. This lets us shove these references into a
/// heap containing references to multiple batches, letting us getting the total
/// order of all batches.
pub struct RowReference {
    /// Index of the row inside the batch this reference is for.
    pub row_idx: usize,

    /// Reference to the comparable rows.
    pub rows: Arc<ComparableRows>,
}

impl RowReference {
    fn row(&self) -> ComparableRow {
        self.rows.row(self.row_idx).expect("row to exist")
    }
}

impl PartialEq for RowReference {
    fn eq(&self, other: &Self) -> bool {
        self.row() == other.row()
    }
}

impl Eq for RowReference {}

impl PartialOrd for RowReference {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RowReference {
    fn cmp(&self, other: &Self) -> Ordering {
        self.row().cmp(&other.row())
    }
}

impl fmt::Debug for RowReference {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RowReference")
            .field("row_idx", &self.row_idx)
            .field("row", &self.row())
            .finish()
    }
}

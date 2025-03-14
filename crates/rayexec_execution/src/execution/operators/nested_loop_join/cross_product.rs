use rayexec_error::Result;
use stdutil::iter::IntoExactSizeIterator;

use crate::arrays::batch::Batch;
use crate::arrays::cache::NopCache;
use crate::arrays::collection::concurrent::{
    ColumnCollectionScanState,
    ConcurrentColumnCollection,
};
use crate::arrays::datatype::DataType;
use crate::storage::projections::Projections;

// TODO: `batch` and `projections` are pub so we can reuse them when draining
// the collection for left outer.
#[derive(Debug)]
pub struct CrossProductState {
    /// Scan state for column collection containing the left-side data.
    ///
    /// Gets reset once we've scanned the end of the collection, indicating we
    /// need a new input batch for the right side (and we'll start scanning from
    /// the beginning again).
    scan_state: Option<ColumnCollectionScanState>,
    /// Batch containing data from the left side.
    pub batch: Batch,
    /// Current row relative to the batch we're processing.
    batch_row_idx: usize,
    /// Current row relative to the entire collection.
    ///
    /// Used to set matches for the left side.
    collection_row_idx: usize,
    /// Projections out of the column collection. Should project all columns.
    pub projections: Projections,
}

impl CrossProductState {
    pub fn new(left_datatypes: impl IntoExactSizeIterator<Item = DataType>) -> Result<Self> {
        // TODO: Create batch without needing to allocate.
        let batch = Batch::new(left_datatypes, 1)?;
        let projections = Projections::new(0..batch.arrays.len());

        Ok(CrossProductState {
            scan_state: None,
            batch,
            batch_row_idx: 0,
            collection_row_idx: 0,
            projections,
        })
    }

    /// Get the current scan offset into the collection.
    pub fn collection_scan_offset(&self) -> Option<usize> {
        self.scan_state
            .as_ref()
            .map(|state| state.relative_scan_offset() + self.batch_row_idx)
    }

    /// Try to scan the next cross-product into `output`.
    ///
    /// Returns a bool indicating if we did write to the output. If true is
    /// returned, then the same right-side batch should be provided again to get
    /// the next output batch.
    ///
    /// If this returns false, then a new right-side batch should be provided.
    /// Internally keeps state to reset the left-side scan to the beginning when
    /// `false` is returned.
    pub fn scan_next(
        &mut self,
        left: &ConcurrentColumnCollection,
        right: &mut Batch,
        output: &mut Batch,
    ) -> Result<bool> {
        let scan_state = match self.scan_state.as_mut() {
            Some(state) => {
                self.batch_row_idx += 1;
                state
            }
            None => {
                self.batch_row_idx = 0;
                self.scan_state = Some(left.init_scan_state());
                self.scan_state.as_mut().unwrap()
            }
        };

        if self.batch_row_idx >= self.batch.num_rows() {
            // Scan next batch.
            self.batch_row_idx = 0;
            let count = left.scan(&self.projections, scan_state, &mut self.batch)?;
            if count == 0 {
                // We're done.
                self.scan_state = None;
                output.set_num_rows(0)?;
                return Ok(false);
            }
        }

        // Get current row from the left, extend out to number of rows we're
        // cross joining on the right.
        for idx in 0..self.batch.arrays.len() {
            let left = &mut self.batch.arrays[idx];
            let dest = &mut output.arrays[idx];

            dest.clone_constant_from(left, self.batch_row_idx, right.num_rows(), &mut NopCache)?;
        }

        // Now just move the right array over.
        for idx in 0..right.arrays.len() {
            let offset = self.batch.arrays.len();
            let right = &mut right.arrays[idx];
            let dest = &mut output.arrays[idx + offset];

            dest.clone_from_other(right, &mut NopCache)?;
        }

        output.set_num_rows(right.num_rows)?;

        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrays::datatype::DataType;
    use crate::generate_batch;
    use crate::testutil::arrays::assert_batches_eq;

    #[test]
    fn simple() {
        let collection = ConcurrentColumnCollection::new([DataType::Int32, DataType::Utf8], 16, 16);
        let mut append_state = collection.init_append_state();

        let mut input = generate_batch!([1, 2], ["a", "b"]);
        collection
            .append_batch(&mut append_state, &mut input)
            .unwrap();
        collection.flush(&mut append_state).unwrap();

        let mut cross_state = CrossProductState::new([DataType::Int32, DataType::Utf8]).unwrap();

        let mut right = generate_batch!([4.5, 6.0]);
        let mut output =
            Batch::new([DataType::Int32, DataType::Utf8, DataType::Float64], 16).unwrap();

        let did_write_out = cross_state
            .scan_next(&collection, &mut right, &mut output)
            .unwrap();
        assert!(did_write_out);

        let expected = generate_batch!([1, 1], ["a", "a"], [4.5, 6.0]);
        assert_batches_eq(&expected, &output);
        assert_eq!(Some(0), cross_state.collection_scan_offset());

        let did_write_out = cross_state
            .scan_next(&collection, &mut right, &mut output)
            .unwrap();
        assert!(did_write_out);

        let expected = generate_batch!([2, 2], ["b", "b"], [4.5, 6.0]);
        assert_batches_eq(&expected, &output);
        assert_eq!(Some(1), cross_state.collection_scan_offset());

        let did_write_out = cross_state
            .scan_next(&collection, &mut right, &mut output)
            .unwrap();
        assert!(!did_write_out);
        assert_eq!(0, output.num_rows);
        assert_eq!(None, cross_state.collection_scan_offset());

        // Now begin scanning with a new right-side batch, we should begin
        // scanning from the beginning again.
        let mut right = generate_batch!([7.5, 8.0]);

        let did_write_out = cross_state
            .scan_next(&collection, &mut right, &mut output)
            .unwrap();
        assert!(did_write_out);

        let expected = generate_batch!([1, 1], ["a", "a"], [7.5, 8.0]);
        assert_batches_eq(&expected, &output);
        assert_eq!(Some(0), cross_state.collection_scan_offset());

        let did_write_out = cross_state
            .scan_next(&collection, &mut right, &mut output)
            .unwrap();
        assert!(did_write_out);

        let expected = generate_batch!([2, 2], ["b", "b"], [7.5, 8.0]);
        assert_batches_eq(&expected, &output);
        assert_eq!(Some(1), cross_state.collection_scan_offset());
    }
}

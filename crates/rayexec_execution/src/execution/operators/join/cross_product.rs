use std::sync::Arc;

use rayexec_error::Result;

use crate::arrays::array::buffer_manager::NopBufferManager;
use crate::arrays::batch::Batch;
use crate::arrays::cache::NopCache;
use crate::execution::operators::materialize::column_collection::ColumnCollection;

#[derive(Debug)]
pub struct CrossProductState {
    collection: Arc<ColumnCollection>,
    batch_idx: usize,
    row_idx: usize,
}

impl CrossProductState {
    pub fn new(collection: Arc<ColumnCollection>) -> Self {
        CrossProductState {
            collection,
            batch_idx: 0,
            row_idx: 0,
        }
    }

    pub fn collection(&self) -> &ColumnCollection {
        self.collection.as_ref()
    }

    /// Tries to load the next row from the batch collection into output.
    ///
    /// `output` should have already been reset for write.
    ///
    /// If we ran out of rows to reference in the batch collection, Ok(false)
    /// will be returned indicating we need to use a new input batch.
    pub fn try_set_next_row(&mut self, input: &mut Batch, output: &mut Batch) -> Result<bool> {
        debug_assert_eq!(
            input.arrays.len() + self.collection.num_columns(),
            output.arrays.len()
        );

        if self.batch_idx >= self.collection.num_batches() {
            return Ok(false);
        }

        let batch = self
            .collection
            .get_batch(self.batch_idx)
            .expect("batch to exist");
        debug_assert!(self.row_idx < batch.num_rows());

        let manager = NopBufferManager;

        // Set constant reference to single row on left side.
        for (idx, collected_array) in batch.arrays.iter().enumerate() {
            let value = collected_array.get_value(self.row_idx)?;
            output.arrays[idx].set_value(0, &value)?;
            output.arrays[idx].select(&manager, std::iter::repeat(0).take(input.num_rows()))?;
        }

        // Reference columns from right as-is
        let col_offset = self.collection.num_columns();
        for (idx, input_array) in input.arrays.iter_mut().enumerate() {
            output.arrays[idx + col_offset].clone_from_other(input_array, &mut NopCache)?;
        }

        output.set_num_rows(input.num_rows())?;

        self.row_idx += 1;
        if self.row_idx >= batch.num_rows() {
            // Move to next batch in collection. Next call to this function
            // will ensure this batch actually exists.
            self.batch_idx += 1;
            self.row_idx = 0;
        }

        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use stdutil::iter::TryFromExactSizeIterator;

    use super::*;
    use crate::arrays::array::Array;
    use crate::arrays::datatype::DataType;
    use crate::testutil::arrays::assert_batches_eq;

    #[test]
    fn cross_product_single_collected_batch() {
        let mut collection = ColumnCollection::new([DataType::Utf8], 2);
        let batches = [Batch::from_arrays([Array::try_from_iter(["a", "b"]).unwrap()]).unwrap()];

        collection.append_many(&batches).unwrap();

        let mut cross_product = CrossProductState::new(Arc::new(collection));
        let mut input = Batch::from_arrays([Array::try_from_iter([1, 2]).unwrap()]).unwrap();
        let mut out = Batch::new([DataType::Utf8, DataType::Int32], 2).unwrap();

        let expected1 = Batch::from_arrays([
            Array::try_from_iter(["a", "a"]).unwrap(),
            Array::try_from_iter([1, 2]).unwrap(),
        ])
        .unwrap();

        let did_write = cross_product
            .try_set_next_row(&mut input, &mut out)
            .unwrap();
        assert!(did_write);
        assert_batches_eq(&expected1, &out);

        let expected2 = Batch::from_arrays([
            Array::try_from_iter(["b", "b"]).unwrap(),
            Array::try_from_iter([1, 2]).unwrap(),
        ])
        .unwrap();

        out.reset_for_write().unwrap();
        let did_write = cross_product
            .try_set_next_row(&mut input, &mut out)
            .unwrap();
        assert!(did_write);
        assert_batches_eq(&expected2, &out);

        let did_write = cross_product
            .try_set_next_row(&mut input, &mut out)
            .unwrap();
        assert!(!did_write);
    }
}

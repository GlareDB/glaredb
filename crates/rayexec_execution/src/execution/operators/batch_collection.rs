use rayexec_error::{RayexecError, Result};

use crate::arrays::array::exp::Array;
use crate::arrays::batch_exp::Batch;
use crate::arrays::buffer::buffer_manager::NopBufferManager;
use crate::arrays::datatype::DataType;

#[derive(Debug)]
pub struct BatchCollection {
    /// Datatypes of the arrays we're storing.
    datatypes: Vec<DataType>,
    /// All blocks making up this collection.
    blocks: Vec<BatchCollectionBlock>,
}

impl BatchCollection {}

#[derive(Debug)]
pub struct BatchCollectionBlock {
    /// Number of rows we're currently storing in this block.
    row_count: usize,
    /// Max number of rows this block store.
    capacity: usize,
    /// Arrays making up this block.
    arrays: Vec<Array>,
}

impl BatchCollectionBlock {
    pub fn new(datatypes: &[DataType], capacity: usize) -> Result<Self> {
        let arrays = datatypes
            .iter()
            .map(|datatype| Array::new(&NopBufferManager, datatype.clone(), capacity))
            .collect::<Result<Vec<_>>>()?;

        Ok(BatchCollectionBlock {
            row_count: 0,
            capacity,
            arrays,
        })
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn set_row_count(&mut self, count: usize) -> Result<()> {
        if count > self.capacity {
            return Err(RayexecError::new("Row count would exceed capacity"));
        }
        self.row_count = count;
        Ok(())
    }

    pub fn row_count(&self) -> usize {
        self.row_count
    }

    pub fn arrays(&self) -> &[Array] {
        &self.arrays
    }

    pub fn has_capacity_for_rows(&self, additional: usize) -> bool {
        self.row_count + additional < self.capacity
    }

    /// Appends a batch to this block.
    pub fn append_batch_data(&mut self, batch: &Batch) -> Result<()> {
        let total_num_rows = self.row_count + batch.num_rows();
        if total_num_rows > self.capacity {
            return Err(
                RayexecError::new("New row count for batch block would exceed capacity")
                    .with_field("new_row_count", total_num_rows)
                    .with_field("capacity", self.capacity),
            );
        }

        if self.arrays.len() != batch.arrays().len() {
            return Err(RayexecError::new("Array length mismatch"));
        }

        for (from, to) in batch.arrays.iter().zip(self.arrays.iter_mut()) {
            // [0..batch_num_rows) => [self_row_count..)
            let mapping =
                (0..batch.num_rows()).zip(self.row_count..(self.row_count + batch.num_rows()));
            from.copy_rows(mapping, to)?;
        }

        self.row_count += batch.num_rows();

        Ok(())
    }

    /// Copies a single row from another block.
    pub fn copy_row_from_other(
        &mut self,
        dest_row: usize,
        source: &BatchCollectionBlock,
        source_row: usize,
    ) -> Result<()> {
        if self.arrays.len() != source.arrays.len() {
            return Err(RayexecError::new(
                "Number of arrays in self and other differ",
            ));
        }

        for (from, to) in source.arrays().iter().zip(self.arrays.iter_mut()) {
            let mapping = [(source_row, dest_row)];
            from.copy_rows(mapping, to)?;
        }

        Ok(())
    }

    pub fn into_batch(self) -> Result<Batch> {
        let mut batch = Batch::try_from_arrays(self.arrays, false)?;
        batch.set_num_rows(self.row_count)?;

        Ok(batch)
    }
}

#[cfg(test)]
mod tests {
    use stdutil::iter::TryFromExactSizeIterator;

    use super::*;
    use crate::arrays::testutil::assert_batches_eq;

    #[test]
    fn block_append_i32() {
        let mut block = BatchCollectionBlock::new(&[DataType::Int32], 4096).unwrap();

        let array1 = Array::try_from_iter([4, 5, 6]).unwrap();
        let array2 = Array::try_from_iter([7, 8]).unwrap();
        let array3 = Array::try_from_iter([9, 10, 11]).unwrap();

        let batch1 = Batch::try_from_arrays([array1], true).unwrap();
        let batch2 = Batch::try_from_arrays([array2], true).unwrap();
        let batch3 = Batch::try_from_arrays([array3], true).unwrap();

        block.append_batch_data(&batch1).unwrap();
        block.append_batch_data(&batch2).unwrap();
        block.append_batch_data(&batch3).unwrap();

        let out = block.into_batch().unwrap();

        let expected = Batch::try_from_arrays(
            [Array::try_from_iter([4, 5, 6, 7, 8, 9, 10, 11]).unwrap()],
            true,
        )
        .unwrap();

        assert_batches_eq(&expected, &out);
    }

    #[test]
    fn block_append_i32_dictionary() {
        let mut block = BatchCollectionBlock::new(&[DataType::Int32], 4096).unwrap();

        let mut array = Array::try_from_iter([4, 5, 6]).unwrap();
        // '[4, 4, 6, 6, 5, 5]'
        array.select(&NopBufferManager, [0, 0, 2, 2, 1, 1]).unwrap();

        let batch = Batch::try_from_arrays([array], true).unwrap();
        block.append_batch_data(&batch).unwrap();

        assert_eq!(6, block.row_count());

        let out = block.into_batch().unwrap();

        let expected =
            Batch::try_from_arrays([Array::try_from_iter([4, 4, 6, 6, 5, 5]).unwrap()], true)
                .unwrap();

        assert_batches_eq(&expected, &out);
    }

    #[test]
    fn block_copy_row_i32_string() {
        let mut block1 =
            BatchCollectionBlock::new(&[DataType::Int32, DataType::Utf8], 4096).unwrap();
        let mut block2 =
            BatchCollectionBlock::new(&[DataType::Int32, DataType::Utf8], 4096).unwrap();

        block1
            .append_batch_data(
                &Batch::try_from_arrays(
                    [
                        Array::try_from_iter([4, 5, 6]).unwrap(),
                        Array::try_from_iter(["a", "b", "c"]).unwrap(),
                    ],
                    true,
                )
                .unwrap(),
            )
            .unwrap();

        block2
            .append_batch_data(
                &Batch::try_from_arrays(
                    [
                        Array::try_from_iter([7, 8]).unwrap(),
                        Array::try_from_iter(["dog", "cat"]).unwrap(),
                    ],
                    true,
                )
                .unwrap(),
            )
            .unwrap();

        block1.copy_row_from_other(1, &block2, 0).unwrap();

        let out = block1.into_batch().unwrap();
        let expected = Batch::try_from_arrays(
            [
                Array::try_from_iter([4, 7, 6]).unwrap(),
                Array::try_from_iter(["a", "dog", "c"]).unwrap(),
            ],
            true,
        )
        .unwrap();

        assert_batches_eq(&expected, &out);
    }
}

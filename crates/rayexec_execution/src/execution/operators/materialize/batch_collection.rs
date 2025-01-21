use rayexec_error::{RayexecError, Result};

use crate::arrays::batch::Batch;
use crate::arrays::datatype::DataType;

#[derive(Debug)]
pub struct BatchCollection {
    /// Column types.
    types: Vec<DataType>,
    /// Batches that make up the collection.
    ///
    /// The last batch is always the one we try to append new data to.
    batches: Vec<Batch>,
    /// Capacity used for creating new buffer batches.
    batch_capacity: usize,
    /// Current row count.
    row_count: usize,
}

impl BatchCollection {
    /// Create a collection for storing columns of the given types.
    ///
    /// `batch_capacity` determines the capacity for each batch within this
    /// collection.
    pub fn new(types: impl IntoIterator<Item = DataType>, batch_capacity: usize) -> Self {
        BatchCollection {
            types: types.into_iter().collect(),
            batches: Vec::new(),
            batch_capacity,
            row_count: 0,
        }
    }

    pub fn num_columns(&self) -> usize {
        self.types.len()
    }

    pub fn row_count(&self) -> usize {
        self.row_count
    }

    pub fn num_batches(&self) -> usize {
        self.batches.len()
    }

    pub fn get_batch(&self, idx: usize) -> Option<&Batch> {
        self.batches.get(idx)
    }

    /// Appends a batch to this collection.
    pub fn append(&mut self, batch: &Batch) -> Result<()> {
        // Ensure we have at least one batch to use.
        if self.batches.is_empty() {
            let batch = Batch::try_new(self.types.clone(), self.batch_capacity)?;
            self.batches.push(batch);
        }

        let mut copy_offset = 0; // Offset to begin copying from in the input.
        let mut rows_remaining = batch.num_rows();

        while rows_remaining != 0 {
            let buffer = self.batches.last_mut().expect("at least one batch");
            let copy_count = usize::min(buffer.capacity - buffer.num_rows, rows_remaining);

            // Generate mapping for this "slice" of the input batch.
            let mapping = (copy_offset..(copy_offset + copy_count))
                .zip(buffer.num_rows..(buffer.num_rows + copy_count));
            batch.copy_rows(mapping, buffer)?;
            buffer.set_num_rows(buffer.num_rows + copy_count)?;

            copy_offset += copy_count;
            rows_remaining -= copy_count;
            self.row_count += copy_count;

            if rows_remaining > 0 {
                // Didn't fit everything into this batch, create a new batch to use.
                let batch = Batch::try_new(self.types.clone(), self.batch_capacity)?;
                self.batches.push(batch);
            }
        }

        Ok(())
    }

    pub fn append_many<'a>(&mut self, batches: impl IntoIterator<Item = &'a Batch>) -> Result<()> {
        for batch in batches {
            self.append(batch)?;
        }
        Ok(())
    }

    /// Merges anothe collection into this one.
    ///
    /// This will append all batches from `other` the end of the current list of
    /// batches.
    pub fn merge(&mut self, other: BatchCollection) -> Result<()> {
        if self.types != other.types {
            return Err(RayexecError::new(
                "Attempted to merge two batch collections with different types",
            ));
        }

        // TODO: Check capacity value is the same?

        self.batches.extend(other.batches);
        self.row_count += other.row_count;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use stdutil::iter::TryFromExactSizeIterator;

    use super::*;
    use crate::arrays::array::Array;
    use crate::arrays::testutil::assert_batches_eq;

    #[test]
    fn append_fits_in_single_buffer() {
        let mut collection = BatchCollection::new([DataType::Int32], 16);

        let input = Batch::try_from_arrays([Array::try_from_iter([1, 2, 3]).unwrap()]).unwrap();
        collection.append(&input).unwrap();
        assert_eq!(1, collection.num_batches());
        assert_eq!(3, collection.row_count());

        let expected = Batch::try_from_arrays([Array::try_from_iter([1, 2, 3]).unwrap()]).unwrap();
        assert_batches_eq(&expected, collection.get_batch(0).unwrap());
    }

    #[test]
    fn append_exceeds_single_buffer() {
        let mut collection = BatchCollection::new([DataType::Int32], 16);

        let input = Batch::try_from_arrays([Array::try_from_iter(0..17_i32).unwrap()]).unwrap();
        collection.append(&input).unwrap();
        assert_eq!(2, collection.num_batches());
        assert_eq!(17, collection.row_count());

        let expected1 = Batch::try_from_arrays([Array::try_from_iter(0..16_i32).unwrap()]).unwrap();
        assert_batches_eq(&expected1, collection.get_batch(0).unwrap());

        let expected2 = Batch::try_from_arrays([Array::try_from_iter([16]).unwrap()]).unwrap();
        assert_batches_eq(&expected2, collection.get_batch(1).unwrap());
    }

    #[test]
    fn append_requireds_many_buffers() {
        let mut collection = BatchCollection::new([DataType::Int32], 16);

        let input = Batch::try_from_arrays([Array::try_from_iter(0..33_i32).unwrap()]).unwrap();
        collection.append(&input).unwrap();
        assert_eq!(3, collection.num_batches());
        assert_eq!(33, collection.row_count());

        let expected1 = Batch::try_from_arrays([Array::try_from_iter(0..16_i32).unwrap()]).unwrap();
        assert_batches_eq(&expected1, collection.get_batch(0).unwrap());

        let expected2 =
            Batch::try_from_arrays([Array::try_from_iter(16..32_i32).unwrap()]).unwrap();
        assert_batches_eq(&expected2, collection.get_batch(1).unwrap());

        let expected3 = Batch::try_from_arrays([Array::try_from_iter([32]).unwrap()]).unwrap();
        assert_batches_eq(&expected3, collection.get_batch(2).unwrap());
    }
}

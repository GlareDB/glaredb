use iterutil::exact_size::IntoExactSizeIterator;
use rayexec_error::{RayexecError, Result};

use crate::arrays::array::Array;
use crate::arrays::batch::Batch;
use crate::arrays::buffer::addressable::{AddressableStorage, MutableAddressableStorage};
use crate::arrays::buffer::physical_type::{
    MutablePhysicalStorage,
    PhysicalI32,
    PhysicalI8,
    PhysicalType,
    PhysicalUtf8,
};
use crate::arrays::buffer_manager::BufferManager;
use crate::arrays::datatype::DataType;

#[derive(Debug)]
pub struct BatchCollection<B: BufferManager> {
    /// Datatypes of the arrays we're storing.
    datatypes: Vec<DataType>,
    /// All blocks making up this collection.
    blocks: Vec<BatchCollectionBlock<B>>,
}

impl<B> BatchCollection<B> where B: BufferManager {}

#[derive(Debug)]
pub struct BatchCollectionBlock<B: BufferManager> {
    /// Number of rows we're currently storing in this block.
    row_count: usize,
    /// Max number of rows this block store.
    capacity: usize,
    /// Arrays making up this block.
    arrays: Vec<Array<B>>,
}

impl<B> BatchCollectionBlock<B>
where
    B: BufferManager,
{
    pub fn new(manager: &B, datatypes: &[DataType], capacity: usize) -> Result<Self> {
        let arrays = datatypes
            .iter()
            .map(|datatype| Array::new(manager, datatype.clone(), capacity))
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

    pub fn row_count(&self) -> usize {
        self.row_count
    }

    pub fn arrays(&self) -> &[Array<B>] {
        &self.arrays
    }

    pub fn append_batch_data(&mut self, batch: &Batch<B>) -> Result<()> {
        let total_num_rows = self.row_count + batch.num_rows();
        if total_num_rows > self.capacity {
            return Err(RayexecError::new("New row count for batch block would exceed capacity")
                .with_field("new_row_count", total_num_rows)
                .with_field("capacity", self.capacity));
        }

        if self.arrays.len() != batch.arrays().len() {
            return Err(RayexecError::new("Array length mismatch"));
        }

        for (from, to) in batch.arrays.iter().zip(self.arrays.iter_mut()) {
            // [0..batch_num_rows) => [self_row_count..)
            let mapping = (0..batch.num_rows()).zip(self.row_count..(self.row_count + batch.num_rows()));

            match to.datatype.physical_type() {
                PhysicalType::Int8 => append_copy::<PhysicalI8, _>(from, mapping, to)?,
                PhysicalType::Int32 => append_copy::<PhysicalI32, _>(from, mapping, to)?,
                PhysicalType::Utf8 => append_copy::<PhysicalUtf8, _>(from, mapping, to)?,
                _ => unimplemented!(),
            }
        }

        self.row_count += batch.num_rows();

        Ok(())
    }
}

fn append_copy<S, B>(
    from: &Array<B>,
    mapping: impl IntoExactSizeIterator<Item = (usize, usize)>,
    to: &mut Array<B>,
) -> Result<()>
where
    S: MutablePhysicalStorage,
    B: BufferManager,
{
    let from_flat = from.flat_view()?;
    let from_storage = S::get_storage(from_flat.array_buffer)?;

    let to_data = to.data.try_as_mut()?;
    let mut to_storage = S::get_storage_mut(to_data)?;

    if from_flat.validity.all_valid() && to.validity.all_valid() {
        for (from_idx, to_idx) in mapping.into_iter() {
            let from_idx = from_flat.selection.get(from_idx).unwrap();
            let v = from_storage.get(from_idx).unwrap();
            to_storage.put(to_idx, v);
        }
    } else {
        for (from_idx, to_idx) in mapping.into_iter() {
            let from_idx = from_flat.selection.get(from_idx).unwrap();
            if from_flat.validity.is_valid(from_idx) {
                let v = from_storage.get(from_idx).unwrap();
                to_storage.put(to_idx, v);
            } else {
                to.validity.set_invalid(to_idx);
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrays::buffer::{Int32Builder, StringViewBufferBuilder};
    use crate::arrays::buffer_manager::NopBufferManager;
    use crate::arrays::executor::scalar::unary::UnaryExecutor;

    #[test]
    fn append_i32() {
        let mut block = BatchCollectionBlock::new(&NopBufferManager, &[DataType::Int32], 4096).unwrap();

        let array1 = Array::new_with_buffer(DataType::Int32, Int32Builder::from_iter([4, 5, 6]).unwrap());
        let array2 = Array::new_with_buffer(DataType::Int32, Int32Builder::from_iter([7, 8]).unwrap());
        let array3 = Array::new_with_buffer(DataType::Int32, Int32Builder::from_iter([9, 10, 11]).unwrap());

        let batch1 = Batch::from_arrays([array1], true).unwrap();
        let batch2 = Batch::from_arrays([array2], true).unwrap();
        let batch3 = Batch::from_arrays([array3], true).unwrap();

        block.append_batch_data(&batch1).unwrap();
        block.append_batch_data(&batch2).unwrap();
        block.append_batch_data(&batch3).unwrap();

        assert_eq!(8, block.row_count());

        let mut out = [0, 0, 0, 0, 0, 0, 0, 0];
        UnaryExecutor::for_each_flat::<PhysicalI32, _>(block.arrays()[0].flat_view().unwrap(), 0..8, |idx, v| {
            out[idx] = v.copied().unwrap();
        })
        .unwrap();

        assert_eq!(&[4, 5, 6, 7, 8, 9, 10, 11], &out);
    }

    #[test]
    fn append_i32_dictionary() {
        let mut block = BatchCollectionBlock::new(&NopBufferManager, &[DataType::Int32], 4096).unwrap();

        let mut array = Array::new_with_buffer(DataType::Int32, Int32Builder::from_iter([4, 5, 6]).unwrap());
        array.select(&NopBufferManager, [0, 0, 2, 2, 1, 1]).unwrap();

        let batch = Batch::from_arrays([array], true).unwrap();
        block.append_batch_data(&batch).unwrap();

        assert_eq!(6, block.row_count());

        let mut out = vec![0; 6];
        UnaryExecutor::for_each_flat::<PhysicalI32, _>(block.arrays()[0].flat_view().unwrap(), 0..6, |idx, v| {
            out[idx] = v.copied().unwrap();
        })
        .unwrap();

        assert_eq!(vec![4, 4, 6, 6, 5, 5], out);
    }

    #[test]
    fn append_string() {
        let mut block = BatchCollectionBlock::new(&NopBufferManager, &[DataType::Utf8], 4096).unwrap();

        let array1 = Array::new_with_buffer(
            DataType::Utf8,
            StringViewBufferBuilder::from_iter(["a", "bb", "ccc"]).unwrap(),
        );
        let array2 = Array::new_with_buffer(DataType::Utf8, StringViewBufferBuilder::from_iter(["d", "ee"]).unwrap());
        let array3 = Array::new_with_buffer(
            DataType::Utf8,
            StringViewBufferBuilder::from_iter(["f", "gg", "hhh"]).unwrap(),
        );

        let batch1 = Batch::from_arrays([array1], true).unwrap();
        let batch2 = Batch::from_arrays([array2], true).unwrap();
        let batch3 = Batch::from_arrays([array3], true).unwrap();

        block.append_batch_data(&batch1).unwrap();
        block.append_batch_data(&batch2).unwrap();
        block.append_batch_data(&batch3).unwrap();

        assert_eq!(8, block.row_count());

        let mut out = vec![String::new(); 8];
        UnaryExecutor::for_each_flat::<PhysicalUtf8, _>(block.arrays()[0].flat_view().unwrap(), 0..8, |idx, v| {
            out[idx] = v.map(|s| s.to_string()).unwrap();
        })
        .unwrap();

        assert_eq!(
            vec![
                "a".to_string(),
                "bb".to_string(),
                "ccc".to_string(),
                "d".to_string(),
                "ee".to_string(),
                "f".to_string(),
                "gg".to_string(),
                "hhh".to_string(),
            ],
            out,
        );
    }
}
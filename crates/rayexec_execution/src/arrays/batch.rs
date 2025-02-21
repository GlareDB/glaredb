use std::fmt::Debug;

use rayexec_error::{RayexecError, Result};
use stdutil::iter::IntoExactSizeIterator;

use super::array::selection::Selection;
use super::cache::{BufferCache, MaybeCache, NopCache};
use super::datatype::DataType;
use super::scalar::ScalarValue;
use crate::arrays::array::Array;
use crate::buffer::buffer_manager::NopBufferManager;

/// A batch of owned same-length arrays.
#[derive(Debug)]
pub struct Batch {
    /// Arrays making up the batch.
    ///
    /// All arrays must have the same capacity (underlying length).
    pub(crate) arrays: Vec<Array>,
    /// Number of logical rows in the batch.
    ///
    /// Equal to or less than capacity when batch contains at least one array.
    /// If the batch contains no arrays, number of rows can be arbitarily set.
    ///
    /// This allows "resizing" batches without needed to resize the underlying
    /// arrays, allowing for buffer reuse.
    pub(crate) num_rows: usize,
    /// Cache for this batch.
    ///
    /// If a batch is being written to, then this cache must be configured with
    /// appropriate buffer cache.
    ///
    /// If the batch is only ever going to reference shared arrays, then we can
    /// omit the caches.
    pub(crate) cache: Option<BufferCache>,
}

impl Batch {
    pub const fn empty() -> Self {
        Batch {
            arrays: Vec::new(),
            num_rows: 0,
            cache: None,
        }
    }

    pub fn empty_with_num_rows(num_rows: usize) -> Self {
        Batch {
            arrays: Vec::new(),
            num_rows,
            cache: None,
        }
    }

    /// Create a batch by initializing arrays for the given datatypes.
    ///
    /// Each array will be initialized to hold `capacity` rows.
    pub fn new(
        datatypes: impl IntoExactSizeIterator<Item = DataType>,
        capacity: usize,
    ) -> Result<Self> {
        let datatypes = datatypes.into_exact_size_iter();
        let mut arrays = Vec::with_capacity(datatypes.len());

        for datatype in datatypes {
            let array = Array::new(&NopBufferManager, datatype, capacity)?;
            arrays.push(array)
        }

        let cache = BufferCache::new(&NopBufferManager, capacity, arrays.len());

        Ok(Batch {
            arrays,
            num_rows: 0,
            cache: Some(cache),
        })
    }

    /// Try to create a new batch using the arrays from the other batch.
    pub fn new_from_other(other: &mut Self) -> Result<Self> {
        let arrays = other
            .arrays_mut()
            .iter_mut()
            .map(|arr| Array::new_from_other(&NopBufferManager, arr))
            .collect::<Result<Vec<_>>>()?;

        Ok(Batch {
            arrays,
            num_rows: other.num_rows,
            cache: None,
        })
    }

    /// Create a new batch from some number of arrays.
    ///
    /// All arrays should have the same logical length.
    ///
    /// The initial number of rows the batch will report will equal the capacity
    /// of the arrays. `set_num_rows` should be used if the logical number of
    /// rows is less than capacity.
    pub fn from_arrays(arrays: impl IntoIterator<Item = Array>) -> Result<Self> {
        let arrays: Vec<_> = arrays.into_iter().collect();
        let num_rows = match arrays.first() {
            Some(arr) => arr.logical_len(),
            None => {
                return Ok(Batch {
                    arrays: Vec::new(),
                    num_rows: 0,
                    cache: None,
                })
            }
        };

        for array in &arrays {
            if array.logical_len() != num_rows {
                return Err(RayexecError::new(
                    "Attempted to create batch from arrays with different lengths",
                )
                .with_field("expected", num_rows)
                .with_field("got", array.logical_len()));
            }
        }

        Ok(Batch {
            arrays,
            num_rows,
            cache: None,
        })
    }

    /// Try to clone arrays from another batch into self.
    ///
    /// If we have a cache configured for this batch, we'll attempt to cache the
    /// current array buffers for later reuse.
    pub fn clone_from_other(&mut self, other: &mut Self) -> Result<()> {
        check_num_arrays(self, other)?;
        match &mut self.cache {
            Some(cache) => {
                debug_assert_eq!(self.arrays.len(), cache.cached.len());
                for (src, (dest, dest_cache)) in other
                    .arrays
                    .iter_mut()
                    .zip(self.arrays.iter_mut().zip(&mut cache.cached))
                {
                    dest.clone_from_other(src, dest_cache)?;
                }
            }
            None => {
                for (src, dest) in other.arrays.iter_mut().zip(&mut self.arrays) {
                    dest.clone_from_other(src, &mut NopCache)?;
                }
            }
        }

        self.num_rows = other.num_rows;

        Ok(())
    }

    /// Swap a pair of arrays between two batches.
    ///
    /// This does not alter the cache for either batch.
    pub fn swap_arrays(
        &mut self,
        own_idx: usize,
        (other, other_idx): (&mut Self, usize),
    ) -> Result<()> {
        self.arrays[own_idx].swap(&mut other.arrays[other_idx])
    }

    /// Clones an array from the other batch into this batch.
    ///
    /// Tries to cache the existing array for this batch.
    pub fn clone_array_from(
        &mut self,
        own_idx: usize,
        (other, other_idx): (&mut Self, usize),
    ) -> Result<()> {
        match &mut self.cache {
            Some(cache) => self.arrays[own_idx]
                .clone_from_other(&mut other.arrays[other_idx], &mut cache.cached[own_idx]),
            None => {
                self.arrays[own_idx].clone_from_other(&mut other.arrays[other_idx], &mut NopCache)
            }
        }
    }

    /// Sets a constant value for an array.
    pub fn set_constant_value(&mut self, arr_idx: usize, val: ScalarValue) -> Result<()> {
        let arr = &mut self.arrays[arr_idx];
        let new_arr = Array::new_constant(&NopBufferManager, &val, self.num_rows)?;
        let old = std::mem::replace(arr, new_arr);

        if let Some(cache) = &mut self.cache {
            cache.cached[arr_idx].maybe_cache(old.data);
        }

        Ok(())
    }

    /// Try to clone an row from another batch into this batch.
    ///
    /// `num_rows` determines the how many times the row will be logically
    /// repeated in the batch.
    ///
    /// If we have a cache configured, arrays buffers will attempt to be cached.
    pub fn clone_row_from_other(
        &mut self,
        other: &mut Self,
        row: usize,
        num_rows: usize,
    ) -> Result<()> {
        check_num_arrays(self, other)?;
        match &mut self.cache {
            Some(cache) => {
                debug_assert_eq!(self.arrays.len(), cache.cached.len());
                for ((src, src_cache), dest) in
                    (other.arrays.iter_mut().zip(&mut cache.cached)).zip(&mut self.arrays)
                {
                    dest.clone_constant_from(src, row, num_rows, src_cache)?;
                }
            }
            None => {
                for (src, dest) in other.arrays.iter_mut().zip(&mut self.arrays) {
                    dest.clone_constant_from(src, row, num_rows, &mut NopCache)?;
                }
            }
        }

        self.num_rows = num_rows;

        Ok(())
    }

    /// Selects rows from the batch based on `selection`.
    pub fn select(
        &mut self,
        selection: impl IntoExactSizeIterator<Item = usize> + Clone,
    ) -> Result<()> {
        let num_rows = selection.clone().into_exact_size_iter().len();
        for arr in &mut self.arrays {
            let selection = selection.clone();
            arr.select(&NopBufferManager, selection)?;
        }
        self.set_num_rows(num_rows)?;

        Ok(())
    }

    /// Reset all arrays in the batch for writes.
    ///
    /// This errors if we don't have a buffer cache configured for this batch.
    pub fn reset_for_write(&mut self) -> Result<()> {
        match &mut self.cache {
            Some(cache) => {
                cache.reset_arrays(&mut self.arrays)?;
                self.num_rows = 0;
                Ok(())
            }
            None => Err(RayexecError::new(
                "No buffer cache configured for batch, cannot reset for write",
            )),
        }
    }

    /// Copy rows from this batch to another batch.
    ///
    /// `mapping` provides (from, to) pairs for how to copy the rows.
    pub fn copy_rows<I>(&self, mapping: I, dest: &mut Self) -> Result<()>
    where
        I: IntoIterator<Item = (usize, usize)> + Clone,
    {
        if self.arrays.len() != dest.arrays.len() {
            return Err(RayexecError::new(
                "Attempted to copy rows to another batch with invalid number of columns",
            ));
        }

        for (from, to) in self.arrays.iter().zip(dest.arrays.iter_mut()) {
            let mapping = mapping.clone();
            from.copy_rows(mapping, to)?;
        }

        Ok(())
    }

    /// Appends a batch to the end of self.
    ///
    /// Errors if this batch doesn't have enough capacity to append the other
    /// batch.
    pub fn append(&mut self, other: &Batch) -> Result<()> {
        let capacity = self.write_capacity()?;
        if self.num_rows() + other.num_rows() > capacity {
            return Err(
                RayexecError::new("Batch doesn't have sufficient capacity for append")
                    .with_field("self_rows", self.num_rows())
                    .with_field("other_rows", other.num_rows())
                    .with_field("self_capacity", capacity),
            );
        }

        for (from, to) in other.arrays.iter().zip(self.arrays.iter_mut()) {
            // [0..batch_num_rows) => [self_row_count..)
            let mapping =
                (0..other.num_rows()).zip(self.num_rows..(self.num_rows + other.num_rows()));
            from.copy_rows(mapping, to)?;
        }

        self.num_rows += other.num_rows;

        Ok(())
    }

    pub fn array(&self, idx: usize) -> Option<&Array> {
        self.arrays.get(idx)
    }

    pub fn arrays(&self) -> &[Array] {
        &self.arrays
    }

    pub fn arrays_mut(&mut self) -> &mut [Array] {
        &mut self.arrays
    }

    pub fn num_arrays(&self) -> usize {
        self.arrays.len()
    }

    pub fn into_arrays(self) -> Vec<Array> {
        self.arrays
    }

    /// Helper for returning a pretty formatted table for the batch.
    ///
    /// This should only be used during debugging.
    #[cfg(debug_assertions)]
    #[allow(unused)]
    pub fn debug_table(&self) -> super::format::pretty::table::PrettyTable {
        use crate::arrays::field::{Field, Schema};
        use crate::arrays::format::pretty::table::PrettyTable;

        let schema =
            Schema::new(self.arrays.iter().enumerate().map(|(idx, array)| {
                Field::new(format!("array{idx}"), array.datatype().clone(), true)
            }));

        PrettyTable::try_new(&schema, &[self], 100, None)
            .expect("to be able to create pretty table")
    }
}

impl Batch {
    /// Get the write capacity for this batch.
    ///
    /// Errors if we don't have a buffer cache configured for this batchs.
    pub fn write_capacity(&self) -> Result<usize> {
        match &self.cache {
            Some(cache) => Ok(cache.capacity()),
            None => Err(RayexecError::new(
                "Batch doesn't have a buffer cache and cannot be written to",
            )),
        }
    }

    /// Returns a selection that selects rows [0, num_rows).
    pub fn selection<'a>(&self) -> Selection<'a> {
        Selection::Linear {
            start: 0,
            len: self.num_rows,
        }
    }

    pub fn num_rows(&self) -> usize {
        self.num_rows
    }

    /// Sets the logical number of rows for the batch.
    pub fn set_num_rows(&mut self, rows: usize) -> Result<()> {
        self.num_rows = rows;
        Ok(())
    }
}

/// Check that two batches have the same number of arrays.
fn check_num_arrays(b1: &Batch, b2: &Batch) -> Result<()> {
    if b1.arrays.len() != b2.arrays.len() {
        return Err(RayexecError::new("Batches have different number of arrays")
            .with_field("batch1", b1.arrays.len())
            .with_field("batch2", b2.arrays.len()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use stdutil::iter::TryFromExactSizeIterator;

    use super::*;
    use crate::testutil::arrays::{assert_batches_eq, generate_batch};

    #[test]
    fn new_from_other() {
        let mut batch = Batch::from_arrays([
            Array::try_from_iter([1, 2, 3, 4]).unwrap(),
            Array::try_from_iter(["a", "b", "c", "d"]).unwrap(),
        ])
        .unwrap();

        let new_batch = Batch::new_from_other(&mut batch).unwrap();

        let expected = Batch::from_arrays([
            Array::try_from_iter([1, 2, 3, 4]).unwrap(),
            Array::try_from_iter(["a", "b", "c", "d"]).unwrap(),
        ])
        .unwrap();

        assert_batches_eq(&expected, &batch);
        assert_batches_eq(&expected, &new_batch);
    }

    #[test]
    fn append_batch_simple() {
        let mut batch = Batch::new([DataType::Int32, DataType::Utf8], 1024).unwrap();

        let append1 = Batch::from_arrays([
            Array::try_from_iter([1, 2, 3]).unwrap(),
            Array::try_from_iter(["a", "b", "c"]).unwrap(),
        ])
        .unwrap();
        batch.append(&append1).unwrap();

        let append2 = Batch::from_arrays([
            Array::try_from_iter([4, 5, 6]).unwrap(),
            Array::try_from_iter(["d", "e", "f"]).unwrap(),
        ])
        .unwrap();
        batch.append(&append2).unwrap();

        let expected = Batch::from_arrays([
            Array::try_from_iter([1, 2, 3, 4, 5, 6]).unwrap(),
            Array::try_from_iter(["a", "b", "c", "d", "e", "f"]).unwrap(),
        ])
        .unwrap();

        assert_batches_eq(&expected, &batch);
    }

    #[test]
    fn clone_from_simple() {
        let mut batch1 = Batch::from_arrays([
            Array::try_from_iter([1, 2, 3, 4]).unwrap(),
            Array::try_from_iter(["a", "b", "c", "d"]).unwrap(),
        ])
        .unwrap();

        let mut batch2 = Batch::new([DataType::Int32, DataType::Utf8], 4).unwrap();

        batch2.clone_from_other(&mut batch1).unwrap();

        let expected = Batch::from_arrays([
            Array::try_from_iter([1, 2, 3, 4]).unwrap(),
            Array::try_from_iter(["a", "b", "c", "d"]).unwrap(),
        ])
        .unwrap();

        assert_batches_eq(&expected, &batch1);
        assert_batches_eq(&expected, &batch2);
    }

    #[test]
    fn set_constant_val_simple() {
        let mut batch = generate_batch!([1, 2, 3, 4], ["a", "b", "c", "d"]);
        batch
            .set_constant_value(1, ScalarValue::Utf8("dog".into()))
            .unwrap();

        let expected = generate_batch!([1, 2, 3, 4], ["dog", "dog", "dog", "dog"]);
        assert_batches_eq(&expected, &batch);
    }
}

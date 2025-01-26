use std::sync::Arc;

use rayexec_error::{RayexecError, Result};
use stdutil::iter::IntoExactSizeIterator;

use super::array::buffer_manager::{BufferManager, NopBufferManager};
use super::array::cache::CachingAllocator;
use super::array::selection::Selection;
use super::cache::BufferCache;
use super::datatype::DataType;
use crate::arrays::array::Array;
use crate::arrays::cache::NopCache;
use crate::arrays::row::ScalarRow;
use crate::arrays::selection::SelectionVector;

/// A batch of same-length arrays.
#[derive(Debug)]
pub struct Batch<B: BufferManager = NopBufferManager> {
    /// Arrays making up the batch.
    ///
    /// All arrays must have the same capacity (underlying length).
    pub(crate) arrays: Vec<Array<B>>,
    /// Number of logical rows in the batch.
    ///
    /// Equal to or less than capacity when batch contains at least one array.
    /// If the batch contains no arrays, number of rows can be arbitarily set.
    ///
    /// This allows "resizing" batches without needed to resize the underlying
    /// arrays, allowing for buffer reuse.
    pub(crate) num_rows: usize,
    /// Capacity (in number of rows) of the batch.
    ///
    /// This should match the capacity of the arrays. If there are zero arrays
    /// in the batch, this should be zero.
    pub(crate) capacity: usize,
    /// Cache for this batch.
    ///
    /// If a batch is being written to, then this cache must be configured with
    /// appropriate buffer cache.
    ///
    /// If the batch is only ever going to reference shared arrays, then we can
    /// omit the caches.
    pub(crate) cache: Option<BufferCache<B>>,
}

impl Batch {
    pub const fn empty() -> Self {
        Batch {
            arrays: Vec::new(),
            num_rows: 0,
            capacity: 0,
            cache: None,
        }
    }

    pub fn empty_with_num_rows(num_rows: usize) -> Self {
        Batch {
            arrays: Vec::new(),
            num_rows,
            capacity: 0,
            cache: None,
        }
    }

    /// Create a batch by initializing arrays for the given datatypes.
    ///
    /// Each array will be initialized to hold `capacity` rows.
    pub fn try_new(
        datatypes: impl IntoExactSizeIterator<Item = DataType>,
        capacity: usize,
    ) -> Result<Self> {
        let datatypes = datatypes.into_exact_size_iter();
        let mut arrays = Vec::with_capacity(datatypes.len());

        for datatype in datatypes {
            let array = Array::try_new(&NopBufferManager, datatype, capacity)?;
            arrays.push(array)
        }

        let cache = BufferCache::new(NopBufferManager, capacity, arrays.len());

        Ok(Batch {
            arrays,
            num_rows: 0,
            capacity,
            cache: Some(cache),
        })
    }

    /// Try to create a new batch using the arrays from the other batch.
    pub fn try_new_from_other(other: &mut Self) -> Result<Self> {
        let arrays = other
            .arrays_mut()
            .iter_mut()
            .map(|arr| Array::try_new_from_other(&NopBufferManager, arr))
            .collect::<Result<Vec<_>>>()?;

        Ok(Batch {
            arrays,
            num_rows: other.num_rows,
            capacity: other.capacity,
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
    pub fn try_from_arrays(arrays: impl IntoIterator<Item = Array>) -> Result<Self> {
        let arrays: Vec<_> = arrays.into_iter().collect();
        let capacity = match arrays.first() {
            Some(arr) => arr.capacity(),
            None => {
                return Ok(Batch {
                    arrays: Vec::new(),
                    num_rows: 0,
                    capacity: 0,
                    cache: None,
                })
            }
        };

        for array in &arrays {
            if array.capacity() != capacity {
                return Err(RayexecError::new(
                    "Attempted to create batch from arrays with different capacities",
                )
                .with_field("expected", capacity)
                .with_field("got", array.capacity()));
            }
        }

        Ok(Batch {
            arrays,
            num_rows: capacity,
            capacity,
            cache: None,
        })
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
    ///
    /// Errors if `rows` is greater than the capacity of the batch.
    pub fn set_num_rows(&mut self, rows: usize) -> Result<()> {
        // TODO: Need to solidify what capacity should be with dictionaries.
        // if rows > self.capacity {
        //     return Err(RayexecError::new("Number of rows exceeds capacity")
        //         .with_field("capacity", self.capacity)
        //         .with_field("requested_num_rows", rows));
        // }
        self.num_rows = rows;

        Ok(())
    }

    /// Try to clone arrays from another batch into self.
    ///
    /// If we have a cache configured for this batch, we'll attempt to cache the
    /// current array buffers for later reuse.
    pub fn try_clone_from_other(&mut self, other: &mut Self) -> Result<()> {
        check_num_arrays(self, other)?;
        match &mut self.cache {
            Some(cache) => {
                debug_assert_eq!(self.arrays.len(), cache.cached.len());
                for ((src, src_cache), dest) in
                    (other.arrays.iter_mut().zip(&mut cache.cached)).zip(&mut self.arrays)
                {
                    dest.try_clone_from_other_with_cache(src, src_cache)?;
                }
            }
            None => {
                for (src, dest) in other.arrays.iter_mut().zip(&mut self.arrays) {
                    dest.try_clone_from_other(src)?;
                }
            }
        }

        self.capacity = other.capacity;
        self.num_rows = other.num_rows;

        Ok(())
    }

    /// Try to clone an row from another batch into this batch.
    ///
    /// `num_rows` determines the how many times the row will be logically
    /// repeated in the batch.
    ///
    /// If we have a cache configured, arrays buffers will attempt to be cached.
    pub fn try_clone_row_from_other(
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
                    dest.try_clone_constant_from_other_with_cache(src, row, num_rows, src_cache)?;
                }
            }
            None => {
                for (src, dest) in other.arrays.iter_mut().zip(&mut self.arrays) {
                    dest.try_clone_constant_from_other(src, row, num_rows)?;
                }
            }
        }

        self.capacity = num_rows; // TODO: What should this be?
        self.num_rows = num_rows;

        Ok(())
    }

    /// Selects rows from the batch based on `selection`.
    pub fn select(&mut self, selection: Selection) -> Result<()> {
        for arr in &mut self.arrays {
            arr.select(&NopBufferManager, selection)?;
        }
        self.set_num_rows(selection.len())?;

        Ok(())
    }

    /// Reset all arrays in the batch for writes.
    ///
    /// This errors if we don't have a buffer cache configured for this batch.
    pub fn reset_for_write(&mut self) -> Result<()> {
        match &mut self.cache {
            Some(cache) => {
                cache.reset_arrays(&mut self.arrays)?;
                self.capacity = cache.capacity();
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
        if self.num_rows() + other.num_rows() > self.capacity {
            return Err(
                RayexecError::new("Batch doesn't have sufficient capacity for append")
                    .with_field("self_rows", self.num_rows())
                    .with_field("other_rows", other.num_rows())
                    .with_field("self_capacity", self.capacity),
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

    #[deprecated]
    pub fn project(self, indices: &[usize]) -> Self {
        let cols = self
            .arrays
            .into_iter()
            .enumerate()
            .filter_map(|(idx, arr)| {
                if indices.contains(&idx) {
                    Some(arr)
                } else {
                    None
                }
            })
            .collect();

        Batch {
            arrays: cols,
            num_rows: self.num_rows,
            capacity: self.capacity,
            cache: None,
        }
    }

    // TODO: Remove
    /// Selects rows in the batch.
    ///
    /// This accepts an Arc selection as it'll be cloned for each array in the
    /// batch.
    #[deprecated]
    pub fn select_old(&self, selection: Arc<SelectionVector>) -> Batch {
        unimplemented!()
        // let cols = self
        //     .arrays
        //     .iter()
        //     .map(|c| {
        //         let mut col = c.clone();
        //         col.select_mut2(selection.clone());
        //         col
        //     })
        //     .collect();

        // Batch {
        //     arrays: cols,
        //     num_rows: selection.as_ref().num_rows(),
        //     capacity: selection.as_ref().num_rows(),
        // }
    }

    /// Get the row at some index.
    #[deprecated]
    pub fn row(&self, idx: usize) -> Option<ScalarRow> {
        unimplemented!()
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

/// Check that two batches have the same number of arrays.
fn check_num_arrays<B: BufferManager>(b1: &Batch<B>, b2: &Batch<B>) -> Result<()> {
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
    use crate::arrays::testutil::assert_batches_eq;

    #[test]
    fn new_from_other() {
        let mut batch = Batch::try_from_arrays([
            Array::try_from_iter([1, 2, 3, 4]).unwrap(),
            Array::try_from_iter(["a", "b", "c", "d"]).unwrap(),
        ])
        .unwrap();

        let new_batch = Batch::try_new_from_other(&mut batch).unwrap();

        let expected = Batch::try_from_arrays([
            Array::try_from_iter([1, 2, 3, 4]).unwrap(),
            Array::try_from_iter(["a", "b", "c", "d"]).unwrap(),
        ])
        .unwrap();

        assert_batches_eq(&expected, &batch);
        assert_batches_eq(&expected, &new_batch);
    }

    #[test]
    fn append_batch_simple() {
        let mut batch = Batch::try_new([DataType::Int32, DataType::Utf8], 1024).unwrap();

        let append1 = Batch::try_from_arrays([
            Array::try_from_iter([1, 2, 3]).unwrap(),
            Array::try_from_iter(["a", "b", "c"]).unwrap(),
        ])
        .unwrap();
        batch.append(&append1).unwrap();

        let append2 = Batch::try_from_arrays([
            Array::try_from_iter([4, 5, 6]).unwrap(),
            Array::try_from_iter(["d", "e", "f"]).unwrap(),
        ])
        .unwrap();
        batch.append(&append2).unwrap();

        let expected = Batch::try_from_arrays([
            Array::try_from_iter([1, 2, 3, 4, 5, 6]).unwrap(),
            Array::try_from_iter(["a", "b", "c", "d", "e", "f"]).unwrap(),
        ])
        .unwrap();

        assert_batches_eq(&expected, &batch);
    }

    #[test]
    fn clone_from_simple() {
        let mut batch1 = Batch::try_from_arrays([
            Array::try_from_iter([1, 2, 3, 4]).unwrap(),
            Array::try_from_iter(["a", "b", "c", "d"]).unwrap(),
        ])
        .unwrap();

        let mut batch2 = Batch::try_new([DataType::Int32, DataType::Utf8], 4).unwrap();

        batch2.try_clone_from_other(&mut batch1).unwrap();

        let expected = Batch::try_from_arrays([
            Array::try_from_iter([1, 2, 3, 4]).unwrap(),
            Array::try_from_iter(["a", "b", "c", "d"]).unwrap(),
        ])
        .unwrap();

        assert_batches_eq(&expected, &batch1);
        assert_batches_eq(&expected, &batch2);
    }

    // #[test]
    // fn clone_row_from_simple() {
    //     let mut batch1 = Batch::try_from_arrays([
    //         Array::try_from_iter([1, 2, 3, 4]).unwrap(),
    //         Array::try_from_iter(["a", "b", "c", "d"]).unwrap(),
    //     ])
    //     .unwrap();

    //     let mut batch2 = Batch::try_new([DataType::Int32, DataType::Utf8], 4).unwrap();

    //     batch2.try_clone_row_from(&mut batch1, 2, 3).unwrap();

    //     let expected = Batch::try_from_arrays([
    //         Array::try_from_iter([3, 3, 3]).unwrap(),
    //         Array::try_from_iter(["c", "c", "c"]).unwrap(),
    //     ])
    //     .unwrap();

    //     assert_batches_eq(&expected, &batch2);
    // }
}

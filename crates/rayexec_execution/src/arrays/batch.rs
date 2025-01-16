use std::sync::Arc;

use rayexec_error::{RayexecError, Result};

use super::array::buffer_manager::NopBufferManager;
use super::array::selection::Selection;
use super::datatype::DataType;
use crate::arrays::array::Array;
use crate::arrays::row::ScalarRow;
use crate::arrays::selection::SelectionVector;

/// A batch of same-length arrays.
#[derive(Debug, Clone, PartialEq)]
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
    /// Capacity (in number of rows) of the batch.
    ///
    /// This should match the capacity of the arrays. If there are zero arrays
    /// in the batch, this should be zero.
    pub(crate) capacity: usize,
}

impl Batch {
    pub const fn empty() -> Self {
        Batch {
            arrays: Vec::new(),
            num_rows: 0,
            capacity: 0,
        }
    }

    pub fn empty_with_num_rows(num_rows: usize) -> Self {
        Batch {
            arrays: Vec::new(),
            num_rows,
            capacity: 0,
        }
    }

    /// Create a batch by initializing arrays for the given datatypes.
    ///
    /// Each array will be initialized to hold `capacity` rows.
    pub fn try_new(
        datatypes: impl stdutil::iter::IntoExactSizeIterator<Item = DataType>,
        capacity: usize,
    ) -> Result<Self> {
        let datatypes = datatypes.into_iter();
        let mut arrays = Vec::with_capacity(datatypes.len());

        for datatype in datatypes {
            let array = Array::try_new(&Arc::new(NopBufferManager), datatype, capacity)?;
            arrays.push(array)
        }

        Ok(Batch {
            arrays,
            num_rows: 0,
            capacity,
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

    /// Selects rows from the batch based on `selection`.
    pub fn select(&mut self, selection: Selection) -> Result<()> {
        for arr in &mut self.arrays {
            arr.select(&Arc::new(NopBufferManager), selection)?;
        }
        self.set_num_rows(selection.len())?;

        Ok(())
    }

    /// Reset all arrays in the batch for writes.
    pub fn reset_for_write(&mut self) -> Result<()> {
        for arr in &mut self.arrays {
            arr.reset_for_write(&Arc::new(NopBufferManager))?;
        }
        Ok(())
    }

    /// Copy rows from this batch to another batch.
    ///
    /// `mapping` provides (from, to) pairs for how to copy the rows.
    pub fn copy_rows(&self, mapping: &[(usize, usize)], dest: &mut Self) -> Result<()> {
        if self.arrays.len() != dest.arrays.len() {
            return Err(RayexecError::new(
                "Attempted to copy rows to another batch with invalid number of columns",
            ));
        }

        for (from, to) in self.arrays.iter().zip(dest.arrays.iter_mut()) {
            from.copy_rows(mapping.iter().copied(), to)?;
        }

        Ok(())
    }

    /// Clones another batch into self.
    pub fn try_clone_from(&mut self, other: &mut Self) -> Result<()> {
        if self.arrays.len() != other.arrays.len() {
            return Err(RayexecError::new(
                "Attempted to clone from batch with different number of arrays",
            )
            .with_field("self", self.arrays.len())
            .with_field("other", other.arrays.len()));
        }

        for (own, other) in self.arrays.iter_mut().zip(other.arrays.iter_mut()) {
            own.try_clone_from(&Arc::new(NopBufferManager), other)?;
        }

        self.set_num_rows(other.num_rows())?;

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
        }
    }

    // TODO: Remove
    /// Selects rows in the batch.
    ///
    /// This accepts an Arc selection as it'll be cloned for each array in the
    /// batch.
    #[deprecated]
    pub fn select_old(&self, selection: Arc<SelectionVector>) -> Batch {
        let cols = self
            .arrays
            .iter()
            .map(|c| {
                let mut col = c.clone();
                col.select_mut2(selection.clone());
                col
            })
            .collect();

        Batch {
            arrays: cols,
            num_rows: selection.as_ref().num_rows(),
            capacity: selection.as_ref().num_rows(),
        }
    }

    /// Get the row at some index.
    #[deprecated]
    pub fn row(&self, idx: usize) -> Option<ScalarRow> {
        if idx >= self.num_rows {
            return None;
        }

        // Non-zero number of rows, but no actual columns. Just return an empty
        // row.
        if self.arrays.is_empty() {
            return Some(ScalarRow::empty());
        }

        let row = self
            .arrays
            .iter()
            .map(|col| col.logical_value(idx).unwrap());

        Some(ScalarRow::from_iter(row))
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

#[cfg(test)]
mod tests {
    use stdutil::iter::TryFromExactSizeIterator;

    use super::*;
    use crate::arrays::testutil::assert_batches_eq;

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

        batch2.try_clone_from(&mut batch1).unwrap();

        let expected = Batch::try_from_arrays([
            Array::try_from_iter([1, 2, 3, 4]).unwrap(),
            Array::try_from_iter(["a", "b", "c", "d"]).unwrap(),
        ])
        .unwrap();

        assert_batches_eq(&expected, &batch1);
        assert_batches_eq(&expected, &batch2);
    }
}

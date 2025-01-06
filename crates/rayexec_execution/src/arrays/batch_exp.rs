use rayexec_error::{RayexecError, Result};
use stdutil::iter::IntoExactSizeIterator;

use super::array::exp::Array;
use super::array::selection::Selection;
use super::buffer::buffer_manager::{BufferManager, NopBufferManager};
use super::datatype::DataType;

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
}

impl<B> Batch<B>
where
    B: BufferManager,
{
    /// Create an empty batch with zero rows.
    pub const fn empty() -> Self {
        Self::empty_with_num_rows(0)
    }

    /// Create an empty batch with some number of rows.
    pub const fn empty_with_num_rows(num_rows: usize) -> Self {
        Batch {
            arrays: Vec::new(),
            num_rows,
            capacity: 0,
        }
    }

    /// Create a batch by initializing arrays for the given datatypes.
    ///
    /// Each array will be initialized to hold `capacity` rows.
    pub fn new(
        manager: &B,
        datatypes: impl IntoExactSizeIterator<Item = DataType>,
        capacity: usize,
    ) -> Result<Self> {
        let datatypes = datatypes.into_iter();
        let mut arrays = Vec::with_capacity(datatypes.len());

        for datatype in datatypes {
            let array = Array::new(manager, datatype, capacity)?;
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
    /// All arrays are expected to have the same capacity.
    ///
    /// `row_eq_cap` indicates if the logical cardinality of the batch should
    /// equal the capacity of the arrays. If false, the logical cardinality will
    /// be set to zero.
    pub(crate) fn try_from_arrays(
        arrays: impl IntoIterator<Item = Array<B>>,
        rows_eq_cap: bool,
    ) -> Result<Self> {
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
            num_rows: if rows_eq_cap { capacity } else { 0 },
            capacity,
        })
    }

    pub fn clone_from(&mut self, manager: &B, other: &mut Self) -> Result<()> {
        if self.arrays.len() != other.arrays.len() {
            return Err(RayexecError::new(
                "Attempted to clone from other batch with different number of arrays",
            ));
        }

        for (a, b) in self.arrays.iter_mut().zip(other.arrays.iter_mut()) {
            a.clone_from(manager, b)?;
        }

        self.set_num_rows(other.num_rows())?;

        Ok(())
    }

    pub fn select(&mut self, manager: &B, selection: &[usize]) -> Result<()> {
        for arr in &mut self.arrays {
            arr.select(manager, selection.iter().copied())?;
        }

        self.set_num_rows(selection.len())?;

        Ok(())
    }

    pub fn num_rows(&self) -> usize {
        self.num_rows
    }

    pub fn set_num_rows(&mut self, rows: usize) -> Result<()> {
        if rows > self.capacity {
            return Err(RayexecError::new("Number of rows exceeds capacity")
                .with_field("capacity", self.capacity)
                .with_field("requested_num_rows", rows));
        }
        self.num_rows = rows;

        Ok(())
    }

    /// Returns a selection that selects rows [0, num_rows).
    pub fn selection<'a>(&self) -> Selection<'a> {
        Selection::Linear { len: self.num_rows }
    }

    pub fn arrays(&self) -> &[Array<B>] {
        &self.arrays
    }

    pub fn arrays_mut(&mut self) -> &mut [Array<B>] {
        &mut self.arrays
    }
}

#[cfg(test)]
mod tests {
    use stdutil::iter::TryFromExactSizeIterator;

    use super::*;

    #[test]
    fn from_arrays_all_same_len() {
        let a = Array::try_from_iter([3, 4, 5]).unwrap();
        let b = Array::try_from_iter(["a", "b", "c"]).unwrap();

        let batch = Batch::try_from_arrays([a, b], true).unwrap();

        assert_eq!(3, batch.selection().len());
    }
}

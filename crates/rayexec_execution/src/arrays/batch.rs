use iterutil::exact_size::IntoExactSizeIterator;
use rayexec_error::{not_implemented, RayexecError, Result};

use super::array::Array;
use super::buffer::physical_type::{PhysicalI32, PhysicalI8, PhysicalType, PhysicalUtf8};
use super::buffer::string_view::StringViewHeap;
use super::buffer::ArrayBuffer;
use super::buffer_manager::{BufferManager, NopBufferManager};
use super::datatype::DataType;
use super::flat_array::FlatSelection;

/// Collection of arrays with equal capacity.
#[derive(Debug)]
pub struct Batch<B: BufferManager = NopBufferManager> {
    pub(crate) arrays: Vec<Array<B>>,
    pub(crate) num_rows: usize,
}

impl<B> Batch<B>
where
    B: BufferManager,
{
    pub const fn empty() -> Self {
        Batch {
            arrays: Vec::new(),
            num_rows: 0,
        }
    }

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
        })
    }

    /// Create a new batch from some number of arrays.
    ///
    /// All arrays are expected to have the same capacity.
    ///
    /// `row_eq_cap` indicates if the logical cardinality of the batch should
    /// equal the capacity of the arrays. If false, the logical cardinality will
    /// be set to zero.
    pub(crate) fn from_arrays(
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
        })
    }

    pub(crate) fn push_array(&mut self, array: Array<B>) -> Result<()> {
        // TODO: Check cap <= current num rows
        let cap = match self.arrays.first() {
            Some(arr) => arr.capacity(),
            None => {
                self.arrays.push(array);
                return Ok(());
            }
        };

        if array.capacity() != cap {
            return Err(
                RayexecError::new("Attempted to push array with different capacity")
                    .with_field("expected", cap)
                    .with_field("got", array.capacity()),
            );
        }

        self.arrays.push(array);

        Ok(())
    }

    pub(crate) fn set_num_rows(&mut self, num_rows: usize) -> Result<()> {
        let cap = match self.arrays.first() {
            Some(arr) => arr.capacity(),
            None => {
                // Allow setting arbitrary number of rows for batch with no
                // arrays.
                self.num_rows = num_rows;
                return Ok(());
            }
        };

        if num_rows > cap {
            return Err(RayexecError::new("num_rows exceeds capacity")
                .with_field("requested_num_rows", num_rows)
                .with_field("capacity", cap));
        }
        self.num_rows = num_rows;
        Ok(())
    }

    pub fn num_rows(&self) -> usize {
        self.num_rows
    }

    /// Generate a selection that will select all rows in order for this batch.
    pub fn generate_selection(&self) -> FlatSelection {
        FlatSelection::linear(self.num_rows)
    }

    pub fn arrays(&self) -> &[Array<B>] {
        &self.arrays
    }

    pub fn arrays_mut(&mut self) -> &mut [Array<B>] {
        &mut self.arrays
    }

    pub fn get_array(&self, idx: usize) -> Result<&Array<B>> {
        self.get_array_opt(idx)
            .ok_or_else(|| RayexecError::new("Missing array").with_field("idx", idx))
    }

    pub fn get_array_opt(&self, idx: usize) -> Option<&Array<B>> {
        self.arrays.get(idx)
    }

    pub fn get_array_mut(&mut self, idx: usize) -> Result<&mut Array<B>> {
        self.get_array_mut_opt(idx)
            .ok_or_else(|| RayexecError::new("Missing array").with_field("idx", idx))
    }

    pub fn get_array_mut_opt(&mut self, idx: usize) -> Option<&mut Array<B>> {
        self.arrays.get_mut(idx)
    }
}

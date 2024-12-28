use iterutil::IntoExactSizeIterator;
use rayexec_error::Result;

use super::array::exp::Array;
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

    pub fn arrays(&self) -> &[Array<B>] {
        &self.arrays
    }

    pub fn arrays_mut(&mut self) -> &mut [Array<B>] {
        &mut self.arrays
    }
}

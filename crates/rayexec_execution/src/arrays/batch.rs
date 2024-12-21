use iterutil::exact_size::IntoExactSizeIterator;
use rayexec_error::{not_implemented, RayexecError, Result};

use super::array::Array;
use super::buffer::physical_type::{PhysicalI32, PhysicalI8, PhysicalType, PhysicalUtf8};
use super::buffer::string_view::StringViewHeap;
use super::buffer::ArrayBuffer;
use super::buffer_manager::{BufferManager, NopBufferManager};
use super::datatype::DataType;

/// Collection of same length arrays.
#[derive(Debug)]
pub struct Batch<B: BufferManager = NopBufferManager> {
    pub(crate) arrays: Vec<Array<B>>,
    pub(crate) capacity: usize,
    pub(crate) num_rows: usize,
}

impl<B> Batch<B>
where
    B: BufferManager,
{
    pub fn new(
        manager: &B,
        datatypes: impl IntoExactSizeIterator<Item = DataType>,
        capacity: usize,
    ) -> Result<Self> {
        let datatypes = datatypes.into_iter();
        let mut arrays = Vec::with_capacity(datatypes.len());

        for datatype in datatypes {
            let buffer = init_array_buffer(manager, &datatype, capacity)?;
            let array = Array::new(datatype, buffer);
            arrays.push(array)
        }

        Ok(Batch {
            arrays,
            capacity,
            num_rows: 0,
        })
    }

    pub(crate) fn from_arrays(arrays: impl IntoIterator<Item = Array<B>>) -> Result<Self> {
        let arrays: Vec<_> = arrays.into_iter().collect();
        let capacity = match arrays.first() {
            Some(arr) => arr.capacity(),
            None => {
                return Ok(Batch {
                    arrays: Vec::new(),
                    capacity: 0,
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
            capacity,
            num_rows: 0,
        })
    }

    pub(crate) fn push_array(&mut self, array: Array<B>) -> Result<()> {
        if array.capacity() != self.capacity {
            return Err(
                RayexecError::new("Attempted to push array with different capacity")
                    .with_field("expected", self.capacity)
                    .with_field("got", array.capacity()),
            );
        }

        self.arrays.push(array);

        Ok(())
    }

    pub(crate) fn set_num_rows(&mut self, num_rows: usize) -> Result<()> {
        if num_rows > self.capacity {
            return Err(RayexecError::new("num_rows exceeds capacity")
                .with_field("requested_num_rows", num_rows)
                .with_field("capacity", self.capacity));
        }
        self.num_rows = num_rows;
        Ok(())
    }

    pub fn num_rows(&self) -> usize {
        self.num_rows
    }

    pub fn capacity(&self) -> usize {
        self.capacity
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

fn init_array_buffer<B>(manager: &B, datatype: &DataType, cap: usize) -> Result<ArrayBuffer<B>>
where
    B: BufferManager,
{
    match datatype.physical_type() {
        PhysicalType::Int8 => ArrayBuffer::with_capacity::<PhysicalI8>(manager, cap),
        PhysicalType::Int32 => ArrayBuffer::with_capacity::<PhysicalI32>(manager, cap),
        PhysicalType::Utf8 => {
            let heap = StringViewHeap::new();
            ArrayBuffer::with_len_and_child_buffer::<PhysicalUtf8>(manager, cap, heap)
        }
        other => not_implemented!("init array buffer: {other}"),
    }
}

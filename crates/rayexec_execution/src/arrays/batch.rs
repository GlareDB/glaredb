use iterutil::exact_size::IntoExactSizeIterator;
use rayexec_error::{not_implemented, RayexecError, Result};

use super::array::Array;
use super::buffer::physical_type::{PhysicalI32, PhysicalI8, PhysicalType, PhysicalUtf8};
use super::buffer::string_view::StringViewHeap;
use super::buffer::ArrayBuffer;
use super::buffer_manager::{BufferManager, NopBufferManager};
use super::datatype::DataType;

/// Collection of same length arrays.
pub struct Batch<B: BufferManager = NopBufferManager> {
    pub(crate) arrays: Vec<BatchArray<B>>,
    pub(crate) capacity: usize,
    pub(crate) len: usize,
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
            arrays.push(BatchArray::Owned(array))
        }

        Ok(Batch {
            arrays,
            capacity,
            len: 0,
        })
    }

    pub fn get_array(&self, idx: usize) -> Result<&Array<B>> {
        self.get_array_opt(idx)
            .ok_or_else(|| RayexecError::new("Missing array").with_field("idx", idx))
    }

    pub fn get_array_opt(&self, idx: usize) -> Option<&Array<B>> {
        self.arrays.get(idx).map(|a| a.as_ref())
    }

    pub fn get_array_mut(&mut self, idx: usize) -> Result<&mut Array<B>> {
        unimplemented!()
        // self.get_array_mut_opt(idx)
        //     .ok_or_else(|| RayexecError::new("Missing array").with_field("idx", idx))
    }

    pub fn get_array_mut_opt(&mut self, idx: usize) -> Result<Option<&mut Array<B>>> {
        self.arrays.get_mut(idx).map(|a| a.try_as_mut()).transpose()
    }

    pub fn num_rows(&self) -> usize {
        self.len
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

#[derive(Debug)]
pub enum BatchArray<B: BufferManager> {
    Managed(B::CowPtr<Array<B>>),
    Owned(Array<B>),
}

impl<B> BatchArray<B>
where
    B: BufferManager,
{
    pub fn try_as_mut(&mut self) -> Result<&mut Array<B>> {
        match self {
            Self::Managed(_) => Err(RayexecError::new(
                "Mut references from managed arrays not yet supported",
            )),
            Self::Owned(array) => Ok(array),
        }
    }
}

impl<B> AsRef<Array<B>> for BatchArray<B>
where
    B: BufferManager,
{
    fn as_ref(&self) -> &Array<B> {
        match self {
            Self::Managed(m) => m.as_ref(),
            Self::Owned(array) => array,
        }
    }
}

fn init_array_buffer<B>(manager: &B, datatype: &DataType, cap: usize) -> Result<ArrayBuffer<B>>
where
    B: BufferManager,
{
    match datatype.physical_type() {
        PhysicalType::Int8 => ArrayBuffer::with_len::<PhysicalI8>(manager, cap),
        PhysicalType::Int32 => ArrayBuffer::with_len::<PhysicalI32>(manager, cap),
        PhysicalType::Utf8 => {
            let heap = StringViewHeap::new();
            ArrayBuffer::with_len_and_child_buffer::<PhysicalUtf8>(manager, cap, heap)
        }
        other => not_implemented!("init array buffer: {other}"),
    }
}

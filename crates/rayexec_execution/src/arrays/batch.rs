use std::ops::Deref;

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
            arrays.push(BatchArray::owned(array))
        }

        Ok(Batch {
            arrays,
            capacity,
            len: 0,
        })
    }

    pub fn get_array(&self, idx: usize) -> Result<&BatchArray<B>> {
        self.get_array_opt(idx)
            .ok_or_else(|| RayexecError::new("Missing array").with_field("idx", idx))
    }

    pub fn get_array_opt(&self, idx: usize) -> Option<&BatchArray<B>> {
        self.arrays.get(idx)
    }

    pub fn get_array_mut(&mut self, idx: usize) -> Result<&mut BatchArray<B>> {
        self.get_array_mut_opt(idx)
            .ok_or_else(|| RayexecError::new("Missing array").with_field("idx", idx))
    }

    pub fn get_array_mut_opt(&mut self, idx: usize) -> Option<&mut BatchArray<B>> {
        self.arrays.get_mut(idx)
    }

    pub fn num_rows(&self) -> usize {
        self.len
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

#[derive(Debug)]
pub struct BatchArray<B: BufferManager> {
    inner: BatchArrayInner<B>,
}

#[derive(Debug)]
enum BatchArrayInner<B: BufferManager> {
    Managed(B::CowPtr<Array<B>>),
    Owned(Array<B>),
    Uninit,
}

impl<B> BatchArray<B>
where
    B: BufferManager,
{
    pub fn owned(array: Array<B>) -> Self {
        BatchArray {
            inner: BatchArrayInner::Owned(array),
        }
    }

    pub fn is_managed(&self) -> bool {
        matches!(self.inner, BatchArrayInner::Managed(_))
    }

    pub fn is_owned(&self) -> bool {
        matches!(self.inner, BatchArrayInner::Owned(_))
    }

    /// Try to make the array managed by the buffer manager.
    ///
    /// Does nothing if the array is already managed.
    ///
    /// Returns an error if the array cannot be made to be managed. The array is
    /// still valid (and remains in the 'owned' state).
    ///
    /// A cloned pointer to the newly managed array will be returned.
    pub fn make_managed(&mut self, manager: &B) -> Result<B::CowPtr<Array<B>>> {
        match &mut self.inner {
            BatchArrayInner::Managed(m) => Ok(m.clone()), // Already managed.
            BatchArrayInner::Owned(_) => {
                let orig = std::mem::replace(&mut self.inner, BatchArrayInner::Uninit);
                let array = match orig {
                    BatchArrayInner::Owned(array) => array,
                    _ => unreachable!("variant already checked"),
                };

                match manager.make_cow(array) {
                    Ok(managed) => {
                        self.inner = BatchArrayInner::Managed(managed);
                        match &self.inner {
                            BatchArrayInner::Managed(m) => Ok(m.clone()),
                            _ => unreachable!("variant just set"),
                        }
                    }
                    Err(orig) => {
                        // Manager rejected it, put it back as owned and return
                        // an error.
                        self.inner = BatchArrayInner::Owned(orig);
                        Err(RayexecError::new("Failed to make batch array managed"))
                    }
                }
            }
            BatchArrayInner::Uninit => panic!("array in uninit state"),
        }
    }

    pub fn try_as_mut(&mut self) -> Result<&mut Array<B>> {
        match &mut self.inner {
            BatchArrayInner::Managed(_) => Err(RayexecError::new(
                "Mut references from managed arrays not yet supported",
            )),
            BatchArrayInner::Owned(array) => Ok(array),
            BatchArrayInner::Uninit => panic!("array in uninit state"),
        }
    }
}

impl<B> AsRef<Array<B>> for BatchArray<B>
where
    B: BufferManager,
{
    fn as_ref(&self) -> &Array<B> {
        match &self.inner {
            BatchArrayInner::Managed(m) => m.as_ref(),
            BatchArrayInner::Owned(array) => array,
            BatchArrayInner::Uninit => panic!("array in uninit state"),
        }
    }
}

impl<B> Deref for BatchArray<B>
where
    B: BufferManager,
{
    type Target = Array<B>;

    fn deref(&self) -> &Self::Target {
        BatchArray::as_ref(&self)
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

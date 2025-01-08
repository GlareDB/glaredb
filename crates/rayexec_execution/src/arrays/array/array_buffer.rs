use std::marker::PhantomData;

use rayexec_error::{RayexecError, Result};

use super::buffer_manager::BufferManager;
use super::physical_type::{PhysicalStorage, PhysicalType};
use super::raw::RawBuffer;

/// Buffer for arrays.
///
/// Buffers are able to hold a fixed number of elements in the primary buffer.
/// Some types make use of secondary buffers for additional data. In such cases,
/// the primary buffer may hold things like metadata or offsets depending on the
/// type.
#[derive(Debug)]
pub struct ArrayBuffer<B: BufferManager> {
    /// Physical type of the buffer.
    physical_type: PhysicalType,
    /// The primary data buffer.
    ///
    /// For primitive buffers, this will just contain the primitives themselves.
    /// Other buffers like string buffers will store the metadata here.
    primary: RawBuffer<B>,
    /// Secondary buffer if needed for the buffer type.
    secondary: Box<SecondaryBuffer<B>>,
}

impl<B> ArrayBuffer<B>
where
    B: BufferManager,
{
    /// Create an array buffer with the given capacity for the primary data
    /// buffer.
    ///
    /// The secondary buffer will be initialized to None.
    pub(crate) fn with_primary_capacity<S: PhysicalStorage>(
        manager: &B,
        capacity: usize,
    ) -> Result<Self> {
        unimplemented!()
        // let primary = RawBufferParts::try_new::<S::PrimaryBufferType>(manager, capacity)?;

        // Ok(ArrayBuffer {
        //     physical_type: S::PHYSICAL_TYPE,
        //     primary,
        //     secondary: Box::new(SecondaryBuffer::None),
        // })
    }

    pub fn try_as_slice<S: PhysicalStorage>(&self) -> Result<&[S::PrimaryBufferType]> {
        unimplemented!()
        // self.check_type(S::PHYSICAL_TYPE)?;
        // let slice = unsafe { self.primary.as_slice::<S::PrimaryBufferType>() };

        // Ok(slice)
    }

    pub fn try_as_slice_mut<S: PhysicalStorage>(&mut self) -> Result<&mut [S::PrimaryBufferType]> {
        unimplemented!()
        // self.check_type(S::PHYSICAL_TYPE)?;
        // let slice = unsafe { self.primary.as_slice_mut::<S::PrimaryBufferType>() };

        // Ok(slice)
    }
}

#[derive(Debug)]
pub enum SecondaryBuffer<B: BufferManager> {
    // StringViewHeap(StringViewHeap),
    Dictionary(DictionaryBuffer<B>),
    List(ListBuffer<B>),
    None,
}

impl<B> SecondaryBuffer<B>
where
    B: BufferManager,
{
    pub fn get_list(&self) -> Result<&ListBuffer<B>> {
        match self {
            Self::List(l) => Ok(l),
            _ => Err(RayexecError::new("Expected list buffer")),
        }
    }

    pub fn get_list_mut(&mut self) -> Result<&mut ListBuffer<B>> {
        match self {
            Self::List(l) => Ok(l),
            _ => Err(RayexecError::new("Expected list buffer")),
        }
    }
}

#[derive(Debug)]
pub struct DictionaryBuffer<B: BufferManager> {
    // pub(crate) validity: Validity,
    // pub(crate) buffer: ArrayData<B>,
    _b: PhantomData<B>,
}

impl<B> DictionaryBuffer<B>
where
    B: BufferManager,
{
    // pub fn new(buffer: ArrayData<B>, validity: Validity) -> Self {
    //     debug_assert_eq!(buffer.capacity(), validity.len());
    //     DictionaryBuffer { buffer, validity }
    // }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ListItemMetadata {
    pub offset: i32,
    pub len: i32,
}

#[derive(Debug)]
pub struct ListBuffer<B: BufferManager> {
    /// Number of "filled" entries in the child array.
    ///
    /// This differs from the child's capacity as we need to be able
    /// incrementally push back values.
    ///
    /// This is only looked at when writing values to the child array. Reads can
    /// ignore this as all required info is in the entry metadata.
    pub(crate) entries: usize,
    // pub(crate) child: Array<B>,
    _b: PhantomData<B>,
}

impl<B> ListBuffer<B>
where
    B: BufferManager,
{
    // pub fn new(child: Array<B>) -> Self {
    //     ListBuffer { entries: 0, child }
    // }
}

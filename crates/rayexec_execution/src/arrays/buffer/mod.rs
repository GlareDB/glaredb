pub mod buffer_manager;
pub mod physical_type;
pub mod string_view;

mod raw;

use buffer_manager::{BufferManager, NopBufferManager};
use physical_type::{PhysicalStorage, PhysicalType};
use raw::RawBufferParts;
use rayexec_error::{RayexecError, Result};
use string_view::{
    StringViewAddressable,
    StringViewAddressableMut,
    StringViewHeap,
    StringViewMetadataUnion,
};

use super::array::array_data::ArrayData;
use super::array::validity::Validity;

#[derive(Debug)]
pub struct ArrayBuffer<B: BufferManager = NopBufferManager> {
    /// Physical type of the buffer.
    physical_type: PhysicalType,
    /// The primary data buffer.
    ///
    /// For primitive buffers, this will just contain the primitives themselves.
    /// Other buffers like string buffers will store the metadata here.
    primary: RawBufferParts<B>,
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
        let primary = RawBufferParts::try_new::<S::PrimaryBufferType>(manager, capacity)?;

        Ok(ArrayBuffer {
            physical_type: S::PHYSICAL_TYPE,
            primary,
            secondary: Box::new(SecondaryBuffer::None),
        })
    }

    pub(crate) fn put_secondary_buffer(&mut self, secondary: SecondaryBuffer<B>) {
        self.secondary = Box::new(secondary)
    }

    pub fn capacity(&self) -> usize {
        self.primary.len
    }

    pub fn physical_type(&self) -> PhysicalType {
        self.physical_type
    }

    pub fn try_as_slice<S: PhysicalStorage>(&self) -> Result<&[S::PrimaryBufferType]> {
        self.check_type(S::PHYSICAL_TYPE)?;
        let slice = unsafe { self.primary.as_slice::<S::PrimaryBufferType>() };

        Ok(slice)
    }

    pub fn try_as_slice_mut<S: PhysicalStorage>(&mut self) -> Result<&mut [S::PrimaryBufferType]> {
        self.check_type(S::PHYSICAL_TYPE)?;
        let slice = unsafe { self.primary.as_slice_mut::<S::PrimaryBufferType>() };

        Ok(slice)
    }

    pub fn get_secondary(&self) -> &SecondaryBuffer<B> {
        &self.secondary
    }

    pub fn get_secondary_mut(&mut self) -> &mut SecondaryBuffer<B> {
        &mut self.secondary
    }

    pub fn try_as_string_view_addressable(&self) -> Result<StringViewAddressable> {
        self.check_type(PhysicalType::Utf8)?;

        let metadata = unsafe { self.primary.as_slice::<StringViewMetadataUnion>() };
        let heap = match self.secondary.as_ref() {
            SecondaryBuffer::StringViewHeap(heap) => heap,
            _ => return Err(RayexecError::new("Missing string heap")),
        };

        Ok(StringViewAddressable { metadata, heap })
    }

    pub fn try_as_string_view_addressable_mut(&mut self) -> Result<StringViewAddressableMut> {
        self.check_type(PhysicalType::Utf8)?;

        let metadata = unsafe { self.primary.as_slice_mut::<StringViewMetadataUnion>() };
        let heap = match self.secondary.as_mut() {
            SecondaryBuffer::StringViewHeap(heap) => heap,
            _ => return Err(RayexecError::new("Missing string heap")),
        };

        Ok(StringViewAddressableMut { metadata, heap })
    }

    fn check_type(&self, want: PhysicalType) -> Result<()> {
        if want != self.physical_type {
            return Err(RayexecError::new("Physical types don't match")
                .with_field("have", self.physical_type)
                .with_field("want", want));
        }

        Ok(())
    }
}

impl<B: BufferManager> Drop for ArrayBuffer<B> {
    fn drop(&mut self) {
        let ptr = self.primary.ptr;

        let len = self.primary.len * self.physical_type.primary_buffer_mem_size();
        let cap = self.primary.cap * self.physical_type.primary_buffer_mem_size();

        let vec = unsafe { Vec::from_raw_parts(ptr, len, cap) };
        std::mem::drop(vec);

        // self.primary.reservation.free()
    }
}

#[derive(Debug)]
pub enum SecondaryBuffer<B: BufferManager> {
    StringViewHeap(StringViewHeap),
    Dictionary(DictionaryBuffer<B>),
    None,
}

#[derive(Debug)]
pub struct DictionaryBuffer<B: BufferManager> {
    pub(crate) validity: Validity,
    pub(crate) buffer: ArrayData<B>,
}

impl<B> DictionaryBuffer<B>
where
    B: BufferManager,
{
    pub fn new(buffer: ArrayData<B>, validity: Validity) -> Self {
        debug_assert_eq!(buffer.capacity(), validity.len());
        DictionaryBuffer { buffer, validity }
    }
}

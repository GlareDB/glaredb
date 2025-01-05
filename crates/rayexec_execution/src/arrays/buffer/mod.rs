pub mod any;
pub mod buffer_manager;
pub mod physical_type;
pub mod string_view;

mod raw;

use any::AnyAddressable;
use buffer_manager::{BufferManager, NopBufferManager};
use fmtutil::IntoDisplayableSlice;
use physical_type::{PhysicalStorage, PhysicalType};
use raw::RawBufferParts;
use rayexec_error::{RayexecError, Result};
use string_view::{
    BinaryViewAddressable,
    BinaryViewAddressableMut,
    StringViewAddressable,
    StringViewAddressableMut,
    StringViewHeap,
    StringViewMetadataUnion,
};

use super::array::array_data::ArrayData;
use super::array::exp::Array;
use super::array::validity::Validity;

/// Buffer for arrays.
///
/// Buffers are able to hold a fixed number of elements in the primary buffer.
/// Some types make use of secondary buffers for additional data. In such cases,
/// the primary buffer may hold things like metadata or offsets depending on the
/// type.
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

    pub fn as_any_addressable(&self) -> AnyAddressable<B> {
        AnyAddressable { buffer: self }
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

    pub fn try_as_binary_view_addressable(&self) -> Result<BinaryViewAddressable> {
        self.check_type_one_of(&[PhysicalType::Utf8, PhysicalType::Binary])?;

        let metadata = unsafe { self.primary.as_slice::<StringViewMetadataUnion>() };
        let heap = match self.secondary.as_ref() {
            SecondaryBuffer::StringViewHeap(heap) => heap,
            _ => return Err(RayexecError::new("Missing string heap")),
        };

        Ok(BinaryViewAddressable { metadata, heap })
    }

    pub fn try_as_binary_view_addressable_mut(&mut self) -> Result<BinaryViewAddressableMut> {
        // Note that unlike the non-mut version of this function, we only allow
        // physical binary types here. For reads, treating strings as binary is
        // completely fine, but allowing writing raw binary to a logical string
        // array could lead to invalid utf8.
        self.check_type(PhysicalType::Binary)?;

        let metadata = unsafe { self.primary.as_slice_mut::<StringViewMetadataUnion>() };
        let heap = match self.secondary.as_mut() {
            SecondaryBuffer::StringViewHeap(heap) => heap,
            _ => return Err(RayexecError::new("Missing string heap")),
        };

        Ok(BinaryViewAddressableMut { metadata, heap })
    }

    /// Resize the primary buffer to be able to hold `capacity` elements.
    pub fn resize_primary<S: PhysicalStorage>(
        &mut self,
        manager: &B,
        capacity: usize,
    ) -> Result<()> {
        self.check_type(S::PHYSICAL_TYPE)?;

        unsafe {
            self.primary
                .resize::<S::PrimaryBufferType>(manager, capacity)
        }
    }

    /// Ensure the primary buffer can hold `capacity` elements.
    ///
    /// Does nothing if the primary buffer already has enough capacity.
    pub fn reserve_primary<S: PhysicalStorage>(
        &mut self,
        manager: &B,
        capacity: usize,
    ) -> Result<()> {
        self.check_type(S::PHYSICAL_TYPE)?;

        if self.capacity() >= capacity {
            return Ok(());
        }

        self.resize_primary::<S>(manager, capacity)
    }

    /// Checks that the physical type of this buffer matches `want`.
    fn check_type(&self, want: PhysicalType) -> Result<()> {
        if want != self.physical_type {
            return Err(RayexecError::new("Physical types don't match")
                .with_field("have", self.physical_type)
                .with_field("want", want));
        }

        Ok(())
    }

    fn check_type_one_of(&self, oneof: &[PhysicalType]) -> Result<()> {
        if !oneof.contains(&self.physical_type) {
            return Err(
                RayexecError::new("Physical type not one of requested types")
                    .with_field("have", self.physical_type)
                    .with_field("oneof", oneof.display_as_list().to_string()),
            );
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
    pub(crate) child: Array<B>,
}

impl<B> ListBuffer<B>
where
    B: BufferManager,
{
    pub fn new(child: Array<B>) -> Self {
        ListBuffer { entries: 0, child }
    }
}

#[cfg(test)]
mod tests {
    use physical_type::PhysicalI32;

    use super::*;

    #[test]
    fn resize_primitive_increase_size() {
        let mut buffer =
            ArrayBuffer::with_primary_capacity::<PhysicalI32>(&NopBufferManager, 4).unwrap();

        let s = buffer.try_as_slice::<PhysicalI32>().unwrap();
        assert_eq!(4, s.len());

        buffer
            .resize_primary::<PhysicalI32>(&NopBufferManager, 8)
            .unwrap();

        let s = buffer.try_as_slice_mut::<PhysicalI32>().unwrap();
        assert_eq!(8, s.len());

        // Sanity check, make sure we can write to it.
        s.iter_mut().for_each(|v| *v = 12);

        assert_eq!(vec![12; 8].as_slice(), s);
    }

    #[test]
    fn resize_primitive_decrease_size() {
        let mut buffer =
            ArrayBuffer::with_primary_capacity::<PhysicalI32>(&NopBufferManager, 4).unwrap();

        let s = buffer.try_as_slice::<PhysicalI32>().unwrap();
        assert_eq!(4, s.len());

        buffer
            .resize_primary::<PhysicalI32>(&NopBufferManager, 2)
            .unwrap();

        let s = buffer.try_as_slice_mut::<PhysicalI32>().unwrap();
        assert_eq!(2, s.len());

        // Sanity check, make sure we can write to it.
        s.iter_mut().for_each(|v| *v = 12);

        assert_eq!(vec![12; 2].as_slice(), s);
    }
}

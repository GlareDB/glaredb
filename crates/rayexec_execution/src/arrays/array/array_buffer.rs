use std::marker::PhantomData;
use std::sync::Arc;

use fmtutil::IntoDisplayableSlice;
use rayexec_error::{RayexecError, Result};

use super::buffer_manager::BufferManager;
use super::physical_type::{PhysicalStorage, PhysicalType};
use super::raw::RawBuffer;
use super::string_view::{
    BinaryViewAddressable,
    BinaryViewAddressableMut,
    StringViewAddressable,
    StringViewAddressableMut,
    StringViewHeap,
    StringViewMetadataUnion,
};
use super::Array;

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
    #[allow(dead_code)]
    pub(crate) fn with_primary_capacity<S: PhysicalStorage>(
        manager: &Arc<B>,
        capacity: usize,
    ) -> Result<Self> {
        let primary = RawBuffer::try_with_capacity::<S::PrimaryBufferType>(manager, capacity)?;

        Ok(ArrayBuffer {
            physical_type: S::PHYSICAL_TYPE,
            primary,
            secondary: Box::new(SecondaryBuffer::None),
        })
    }

    #[allow(dead_code)]
    pub(crate) fn put_secondary_buffer(&mut self, secondary: SecondaryBuffer<B>) {
        self.secondary = Box::new(secondary)
    }

    pub const fn primary_capacity(&self) -> usize {
        self.primary.reservation.size() / self.physical_type.primary_buffer_mem_size()
    }

    pub fn get_secondary(&self) -> &SecondaryBuffer<B> {
        &self.secondary
    }

    pub fn get_secondary_mut(&mut self) -> &mut SecondaryBuffer<B> {
        &mut self.secondary
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

    /// Ensure the primary buffer can hold `capacity` elements.
    ///
    /// Does nothing if the primary buffer already has enough capacity.
    // TODO: Do we need to enable shrinking buffers? Experiment branch had that,
    // but I don't believe it was actually needed anywhere.
    pub fn reserve_primary<S: PhysicalStorage>(&mut self, capacity: usize) -> Result<()> {
        self.check_type(S::PHYSICAL_TYPE)?;

        let s = self.try_as_slice::<S>()?;
        if s.len() >= capacity {
            // Already have the capacity needed.
            return Ok(());
        }

        let additional = capacity - s.len();
        unsafe { self.primary.reserve::<S::PrimaryBufferType>(additional)? };

        Ok(())
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

    use super::*;
    use crate::arrays::array::buffer_manager::NopBufferManager;
    use crate::arrays::array::physical_type::{
        Addressable,
        AddressableMut,
        PhysicalI32,
        PhysicalUtf8,
    };

    #[test]
    fn reserve_primitive() {
        let mut buffer =
            ArrayBuffer::with_primary_capacity::<PhysicalI32>(&Arc::new(NopBufferManager), 4)
                .unwrap();
        assert_eq!(4, buffer.primary_capacity());

        let s = buffer.try_as_slice::<PhysicalI32>().unwrap();
        assert_eq!(4, s.len());

        buffer.reserve_primary::<PhysicalI32>(8).unwrap();
        assert_eq!(8, buffer.primary_capacity());

        let s = buffer.try_as_slice_mut::<PhysicalI32>().unwrap();
        assert_eq!(8, s.len());

        // Sanity check, make sure we can write to it.
        s.iter_mut().for_each(|v| *v = 12);

        assert_eq!(vec![12; 8].as_slice(), s);
    }

    #[test]
    fn as_string_view() {
        let mut buffer =
            ArrayBuffer::with_primary_capacity::<PhysicalUtf8>(&Arc::new(NopBufferManager), 4)
                .unwrap();
        buffer.put_secondary_buffer(SecondaryBuffer::StringViewHeap(StringViewHeap::new()));

        let mut s = buffer.try_as_string_view_addressable_mut().unwrap();
        s.put(0, "dog");
        s.put(1, "cat");
        s.put(2, "dogcatdogcatdogcat");
        s.put(3, "");

        let s = buffer.try_as_string_view_addressable().unwrap();
        assert_eq!("dog", s.get(0).unwrap());
        assert_eq!("cat", s.get(1).unwrap());
        assert_eq!("dogcatdogcatdogcat", s.get(2).unwrap());
        assert_eq!("", s.get(3).unwrap());
    }
}

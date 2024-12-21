pub mod addressable;
pub mod any;
pub mod dictionary;
pub mod list;
pub mod physical_type;
pub mod string_view;
pub mod struct_buffer;

use std::marker::PhantomData;

use dictionary::DictionaryBuffer;
use iterutil::exact_size::IntoExactSizeIterator;
use list::{ListBuffer, ListItemMetadata};
use physical_type::{
    PhysicalI32,
    PhysicalI8,
    PhysicalList,
    PhysicalStorage,
    PhysicalStruct,
    PhysicalType,
    PhysicalUntypedNull,
    PhysicalUtf8,
};
use rayexec_error::{not_implemented, RayexecError, Result};
use string_view::{
    StringViewHeap,
    StringViewMetadataUnion,
    StringViewStorage,
    StringViewStorageMut,
};
use struct_buffer::{StructBuffer, StructItemMetadata};

use super::array::Array;
use super::buffer_manager::{BufferManager, NopBufferManager, Reservation};
use super::datatype::DataType;

#[derive(Debug)]
pub struct ArrayBuffer<B: BufferManager = NopBufferManager> {
    /// The physical type of the buffer.
    physical_type: PhysicalType,
    /// The primary data buffer.
    primary: RawBufferParts<B>,
    /// Extra buffers for non-primitive data types (varlen, lists, etc)
    secondary: Box<SecondaryBuffers<B>>,
}

impl<B> ArrayBuffer<B>
where
    B: BufferManager,
{
    /// Create a new buffer with the given capacity.
    pub fn with_capacity<S: PhysicalStorage>(manager: &B, capacity: usize) -> Result<Self> {
        let data = RawBufferParts::try_new::<S::PrimaryBufferType>(manager, capacity)?;

        Ok(ArrayBuffer {
            physical_type: S::PHYSICAL_TYPE,
            primary: data,
            secondary: Box::new(SecondaryBuffers::None),
        })
    }

    pub fn with_len_and_child_buffer<S: PhysicalStorage>(
        manager: &B,
        len: usize,
        child: impl Into<SecondaryBuffers<B>>,
    ) -> Result<Self> {
        let data = RawBufferParts::try_new::<S::PrimaryBufferType>(manager, len)?;

        Ok(ArrayBuffer {
            physical_type: S::PHYSICAL_TYPE,
            primary: data,
            secondary: Box::new(child.into()),
        })
    }

    pub fn physical_type(&self) -> PhysicalType {
        self.physical_type
    }

    /// Returns the length of the primary buffer.
    ///
    /// The length of the primary buffer indicates the number of top-level
    /// entries this buffer is holding.
    ///
    /// Note that secondary buffers may not be empty even if `len == 0` and so
    /// this shouldn't be used for memory tracking.
    pub fn capacity(&self) -> usize {
        self.primary.len
    }

    pub fn secondary_buffers(&self) -> &SecondaryBuffers<B> {
        self.secondary.as_ref()
    }

    pub fn secondary_buffers_mut(&mut self) -> &mut SecondaryBuffers<B> {
        self.secondary.as_mut()
    }

    pub fn try_as_slice<S: PhysicalStorage>(&self) -> Result<&[S::PrimaryBufferType]> {
        if S::PHYSICAL_TYPE != self.physical_type {
            return Err(
                RayexecError::new("Attempted to cast buffer to wrong physical type")
                    .with_field("expected_type", self.physical_type)
                    .with_field("requested_type", S::PHYSICAL_TYPE),
            );
        }

        let data = unsafe { self.primary.as_slice::<S::PrimaryBufferType>() };

        Ok(data)
    }

    pub fn try_as_slice_mut<S: PhysicalStorage>(&mut self) -> Result<&mut [S::PrimaryBufferType]> {
        if S::PHYSICAL_TYPE != self.physical_type {
            return Err(
                RayexecError::new("Attempted to cast buffer to wrong physical type")
                    .with_field("expected_type", self.physical_type)
                    .with_field("requested_type", S::PHYSICAL_TYPE),
            );
        }

        let data = unsafe { self.primary.as_slice_mut::<S::PrimaryBufferType>() };

        Ok(data)
    }

    pub fn try_as_string_view_storage(&self) -> Result<StringViewStorage<'_>> {
        let metadata = self.try_as_slice::<PhysicalUtf8>()?;

        match self.secondary.as_ref() {
            SecondaryBuffers::StringViewHeap(heap) => Ok(StringViewStorage { metadata, heap }),
            _ => Err(RayexecError::new("Missing string heap")),
        }
    }

    pub fn try_as_string_view_storage_mut(&mut self) -> Result<StringViewStorageMut<'_>> {
        // TODO: Duplicated, but let's us take each field mutably.
        if PhysicalUtf8::PHYSICAL_TYPE != self.physical_type {
            return Err(
                RayexecError::new("Attempted to cast buffer to wrong physical type")
                    .with_field("expected_type", self.physical_type)
                    .with_field("requested_type", PhysicalUtf8::PHYSICAL_TYPE),
            );
        }

        let metadata = unsafe { self.primary.as_slice_mut::<StringViewMetadataUnion>() };

        match self.secondary.as_mut() {
            SecondaryBuffers::StringViewHeap(heap) => Ok(StringViewStorageMut { metadata, heap }),
            _ => Err(RayexecError::new("Missing string heap")),
        }
    }

    pub fn resize<S: PhysicalStorage>(&mut self, manager: &B, len: usize) -> Result<()> {
        if S::PHYSICAL_TYPE != self.physical_type {
            return Err(
                RayexecError::new("Attempted to resize buffer using wrong physical type")
                    .with_field("expected_type", self.physical_type)
                    .with_field("requested_type", S::PHYSICAL_TYPE),
            );
        }

        unsafe { self.primary.resize::<S::PrimaryBufferType>(manager, len) }
    }

    /// Appends data from another buffer into this buffer.
    pub fn append_from<S: PhysicalStorage>(
        &mut self,
        manager: &B,
        other: &ArrayBuffer,
    ) -> Result<()> {
        if !self.secondary.is_none() {
            return Err(RayexecError::new(
                "Appending secondary buffers not yet supported",
            ));
        }

        let orig_len = self.capacity();
        let new_len = self.capacity() + other.capacity();

        // Ensure we have the right type for other before trying to resize self.
        let other = other.try_as_slice::<S>()?;

        // Resize self to new size.
        self.resize::<S>(manager, new_len)?;

        // Now copy everything over.
        let this = self.try_as_slice_mut::<S>()?;
        let new_this = &mut this[orig_len..];
        new_this.copy_from_slice(other);

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
pub enum SecondaryBuffers<B: BufferManager> {
    StringViewHeap(StringViewHeap),
    List(ListBuffer<B>),
    Struct(StructBuffer<B>),
    Dictionary(DictionaryBuffer<B>),
    None,
}

impl<B> SecondaryBuffers<B>
where
    B: BufferManager,
{
    pub fn is_none(&self) -> bool {
        matches!(self, SecondaryBuffers::None)
    }

    pub fn try_as_struct_buffer(&self) -> Result<&StructBuffer<B>> {
        match self {
            Self::Struct(buf) => Ok(buf),
            _ => Err(RayexecError::new("Not a struct buffer")),
        }
    }

    pub fn try_as_list_buffer(&self) -> Result<&ListBuffer<B>> {
        match self {
            Self::List(buf) => Ok(buf),
            _ => Err(RayexecError::new("Not a list buffer")),
        }
    }

    pub fn try_as_dictionary_buffer(&self) -> Result<&DictionaryBuffer<B>> {
        match self {
            Self::Dictionary(buf) => Ok(buf),
            _ => Err(RayexecError::new("Not a dictionary buffer")),
        }
    }
}

impl<B: BufferManager> From<StringViewHeap> for SecondaryBuffers<B> {
    fn from(value: StringViewHeap) -> Self {
        SecondaryBuffers::StringViewHeap(value)
    }
}

impl<B: BufferManager> From<ListBuffer<B>> for SecondaryBuffers<B> {
    fn from(value: ListBuffer<B>) -> Self {
        SecondaryBuffers::List(value)
    }
}

impl<B: BufferManager> From<StructBuffer<B>> for SecondaryBuffers<B> {
    fn from(value: StructBuffer<B>) -> Self {
        SecondaryBuffers::Struct(value)
    }
}

#[derive(Debug)]
struct RawBufferParts<B: BufferManager> {
    /// Memory reservation for this buffer.
    reservation: B::Reservation,
    /// Raw pointer to start of vec.
    ptr: *mut u8,
    /// Number of elements `T` in the vec, not bytes.
    len: usize,
    /// Capacity of vec (`T` not bytes).
    cap: usize,
}

impl<B: BufferManager> RawBufferParts<B> {
    fn try_new<T: Default + Copy>(manager: &B, len: usize) -> Result<Self> {
        // Note that `vec!` may over-allocate, so we track that too.
        //
        // See <https://doc.rust-lang.org/std/vec/struct.Vec.html#guarantees>
        // > vec![x; n], vec![a, b, c, d], and Vec::with_capacity(n), will all
        // > produce a Vec with at least the requested capacity.
        let alloc_size = len * std::mem::size_of::<T>();
        let mut reservation = manager.reserve_external(alloc_size)?;

        let mut data: Vec<T> = vec![T::default(); len];

        let ptr = data.as_mut_ptr();
        let len = data.len();
        let cap = data.capacity();

        let additional = (cap * std::mem::size_of::<T>()) - alloc_size;
        if additional > 0 {
            let additional = manager.reserve_external(additional)?;
            reservation = reservation.combine(additional);
        }

        std::mem::forget(data);

        Ok(RawBufferParts {
            reservation,
            ptr: ptr.cast(),
            len,
            cap,
        })
    }

    unsafe fn as_slice<T>(&self) -> &[T] {
        std::slice::from_raw_parts(self.ptr.cast::<T>().cast_const(), self.len)
    }

    unsafe fn as_slice_mut<T>(&mut self) -> &mut [T] {
        std::slice::from_raw_parts_mut(self.ptr.cast::<T>(), self.len)
    }

    unsafe fn resize<T: Default + Copy>(&mut self, manager: &B, len: usize) -> Result<()> {
        if self.len == 0 {
            // Special case when length is zero.
            //
            // We want to enable the use case where we initialize the buffer to
            // nothing (null) and later append to it. However, the `T` that we
            // pass in here might have a different alignment which wouldn't be
            // safe.
            //
            // By just creating a new buffer, we can avoid that issue.
            let new_self = Self::try_new::<T>(manager, len)?;
            *self = new_self;
            return Ok(());
        }

        debug_assert_eq!(self.ptr as usize % std::mem::size_of::<T>(), 0);

        let mut data: Vec<T> = Vec::from_raw_parts(self.ptr.cast(), self.len, self.cap);

        // TODO: Reservation stuff.

        data.resize(len, T::default());

        self.ptr = data.as_mut_ptr().cast();
        self.len = data.len();
        self.cap = data.capacity();

        std::mem::forget(data);

        Ok(())
    }
}

pub type Int8Builder = PrimBufferBuilder<PhysicalI8>;
pub type Int32Builder = PrimBufferBuilder<PhysicalI32>;

#[derive(Debug)]
pub struct PrimBufferBuilder<S: PhysicalStorage> {
    _s: PhantomData<S>,
}

impl<S: PhysicalStorage> PrimBufferBuilder<S> {
    pub fn from_iter<I>(iter: I) -> Result<ArrayBuffer>
    where
        I: IntoExactSizeIterator<Item = S::PrimaryBufferType>,
    {
        let iter = iter.into_iter();
        let mut data =
            RawBufferParts::try_new::<S::PrimaryBufferType>(&NopBufferManager, iter.len())?;

        let data_slice = unsafe { data.as_slice_mut() };
        for (idx, val) in iter.enumerate() {
            data_slice[idx] = val;
        }

        Ok(ArrayBuffer {
            physical_type: S::PHYSICAL_TYPE,
            primary: data,
            secondary: Box::new(SecondaryBuffers::None),
        })
    }
}

#[derive(Debug)]
pub struct StringViewBufferBuilder;

impl StringViewBufferBuilder {
    pub fn from_iter<A, I>(iter: I) -> Result<ArrayBuffer>
    where
        A: AsRef<str>,
        I: IntoExactSizeIterator<Item = A>,
    {
        let iter = iter.into_iter();
        let mut data =
            RawBufferParts::try_new::<StringViewMetadataUnion>(&NopBufferManager, iter.len())?;

        let mut heap = StringViewHeap::new();

        let data_slice = unsafe { data.as_slice_mut() };
        for (idx, val) in iter.enumerate() {
            let metadata = heap.push_bytes(val.as_ref().as_bytes());
            data_slice[idx] = metadata;
        }

        Ok(ArrayBuffer {
            physical_type: PhysicalUtf8::PHYSICAL_TYPE,
            primary: data,
            secondary: Box::new(SecondaryBuffers::StringViewHeap(heap)),
        })
    }
}

#[derive(Debug)]
pub struct ListBufferBuilder;

impl ListBufferBuilder {
    pub fn from_iter<I>(child_datatype: DataType, iter: I) -> Result<ArrayBuffer>
    where
        I: IntoExactSizeIterator<Item = ArrayBuffer>,
    {
        let mut iter = iter.into_iter();

        let mut data = RawBufferParts::try_new::<ListItemMetadata>(&NopBufferManager, iter.len())?;

        // Init child buffer with first array buffer.
        let mut child_buf = match iter.next() {
            Some(buf) => buf,
            None => {
                // We have a list array, but no lists (rows == 0).
                return Ok(ArrayBuffer {
                    physical_type: PhysicalList::PHYSICAL_TYPE,
                    primary: data,
                    secondary: Box::new(SecondaryBuffers::None),
                });
            }
        };

        // Track first list.
        let parent = unsafe { data.as_slice_mut() };
        parent[0] = ListItemMetadata {
            offset: 0,
            len: child_buf.capacity() as i32,
        };

        // Now iter all remaining array buffers, append the actual data to the
        // child buf and tracking the metadata in the parent buf.
        for (idx, child) in iter.enumerate() {
            // +1 since we already have the first entry.
            parent[idx + 1] = ListItemMetadata {
                offset: child_buf.capacity() as i32,
                len: child.capacity() as i32,
            };

            // TODO: Move this out.
            match child_buf.physical_type {
                PhysicalType::UntypedNull => {
                    child_buf.append_from::<PhysicalUntypedNull>(&NopBufferManager, &child)?
                }
                PhysicalType::Int8 => {
                    child_buf.append_from::<PhysicalI8>(&NopBufferManager, &child)?
                }
                PhysicalType::Int32 => {
                    child_buf.append_from::<PhysicalI32>(&NopBufferManager, &child)?
                }
                PhysicalType::Utf8 => {
                    child_buf.append_from::<PhysicalUtf8>(&NopBufferManager, &child)?
                }
                other => not_implemented!("append from {other}"),
            }
        }

        let child_array = Array::new_with_buffer(child_datatype, child_buf);

        Ok(ArrayBuffer {
            physical_type: PhysicalList::PHYSICAL_TYPE,
            primary: data,
            secondary: Box::new(SecondaryBuffers::List(ListBuffer::new(child_array))),
        })
    }
}

#[derive(Debug)]
pub struct StructBufferBuilder;

impl StructBufferBuilder {
    pub fn from_arrays(arrays: impl IntoIterator<Item = Array>) -> Result<ArrayBuffer> {
        let children: Vec<_> = arrays.into_iter().collect();

        let len = match children.first() {
            Some(child) => child.capacity(),
            None => 0,
        };

        let data = RawBufferParts::try_new::<StructItemMetadata>(&NopBufferManager, len)?;

        for child in &children {
            if child.capacity() != len {
                return Err(RayexecError::new("Struct buffer has incorrect length")
                    .with_field("want", len)
                    .with_field("have", child.capacity()));
            }
        }

        Ok(ArrayBuffer {
            physical_type: PhysicalStruct::PHYSICAL_TYPE,
            primary: data,
            secondary: Box::new(SecondaryBuffers::Struct(StructBuffer { children })),
        })
    }
}

#[cfg(test)]
mod tests {
    use addressable::AddressableStorage;
    use physical_type::PhysicalI32;

    use super::*;

    // #[test]
    // fn reserve_and_drop() {
    //     let tracker = AtomicReservationTracker::default();

    //     let buffer = ArrayBuffer::with_len::<PhysicalI32>(&tracker, 4).unwrap();

    //     let total_reserved = tracker.total_reserved();
    //     assert!(
    //         total_reserved >= 16,
    //         "expected at least 16 bytes reserved, got {total_reserved}"
    //     );

    //     std::mem::drop(buffer);

    //     let total_reserved = tracker.total_reserved();
    //     assert_eq!(0, total_reserved);
    // }

    #[test]
    fn new_from_prim_iter() {
        let buf = PrimBufferBuilder::<PhysicalI32>::from_iter([4, 5, 6]).unwrap();
        let slice = buf.try_as_slice::<PhysicalI32>().unwrap();

        assert_eq!(&[4, 5, 6], slice)
    }

    #[test]
    fn new_from_strings_iter() {
        let buf =
            StringViewBufferBuilder::from_iter(["a", "bb", "ccc", "ddddddddddddddd"]).unwrap();
        let view_buf = buf.try_as_string_view_storage().unwrap();

        assert_eq!("a", view_buf.get(0).unwrap());
        assert_eq!("bb", view_buf.get(1).unwrap());
        assert_eq!("ccc", view_buf.get(2).unwrap());
        assert_eq!("ddddddddddddddd", view_buf.get(3).unwrap());
    }

    #[test]
    fn append_same_prim_type() {
        let mut a = PrimBufferBuilder::<PhysicalI32>::from_iter([4, 5, 6]).unwrap();
        let b = PrimBufferBuilder::<PhysicalI32>::from_iter([7, 8]).unwrap();

        a.append_from::<PhysicalI32>(&NopBufferManager, &b).unwrap();

        let a_slice = a.try_as_slice::<PhysicalI32>().unwrap();
        assert_eq!(&[4, 5, 6, 7, 8], a_slice);
    }
}

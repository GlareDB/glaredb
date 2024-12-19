pub mod addressable;
pub mod physical_type;
pub mod reservation;
pub mod string_view;

use std::marker::PhantomData;

use physical_type::{PhysicalI32, PhysicalStorage, PhysicalType, PhysicalUtf8};
use rayexec_error::{RayexecError, Result};
use reservation::{NopReservationTracker, Reservation, ReservationTracker};
use string_view::{StringViewBuffer, StringViewBufferMut, StringViewHeap, StringViewMetadataUnion};

use crate::compute::util::{FromExactSizedIterator, IntoExactSizedIterator};
use crate::executor::physical_type::PhysicalI8;

#[derive(Debug)]
pub struct ArrayBuffer<R: ReservationTracker = NopReservationTracker> {
    /// The physical type of the buffer.
    physical_type: PhysicalType,
    /// The underlying data for the buffer.
    ///
    /// Stored as raw parts then converted to a slice on access.
    data: RawBufferParts<R>,
    /// Child buffers for extra data.
    child: ChildBuffer,
}

impl<R> ArrayBuffer<R>
where
    R: ReservationTracker,
{
    /// Create a new buffer with the given len.
    pub fn with_len<S: PhysicalStorage>(tracker: &R, len: usize) -> Result<Self> {
        let data = RawBufferParts::try_new::<S::PrimaryBufferType>(tracker, len)?;

        Ok(ArrayBuffer {
            physical_type: S::PHYSICAL_TYPE,
            data,
            child: ChildBuffer::None,
        })
    }

    pub fn with_len_and_child_buffer<S: PhysicalStorage>(
        tracker: &R,
        len: usize,
        child: impl Into<ChildBuffer>,
    ) -> Result<Self> {
        let data = RawBufferParts::try_new::<S::PrimaryBufferType>(tracker, len)?;

        Ok(ArrayBuffer {
            physical_type: S::PHYSICAL_TYPE,
            data,
            child: child.into(),
        })
    }

    pub fn len(&self) -> usize {
        self.data.len
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn try_as_slice<S: PhysicalStorage>(&self) -> Result<&[S::PrimaryBufferType]> {
        if S::PHYSICAL_TYPE != self.physical_type {
            return Err(
                RayexecError::new("Attempted to cast buffer to wrong physical type")
                    .with_field("expected_type", self.physical_type)
                    .with_field("requested_type", S::PHYSICAL_TYPE),
            );
        }

        let data = unsafe { self.data.as_slice::<S::PrimaryBufferType>() };

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

        let data = unsafe { self.data.as_slice_mut::<S::PrimaryBufferType>() };

        Ok(data)
    }

    pub fn try_as_string_view_buffer(&self) -> Result<StringViewBuffer<'_>> {
        let metadata = self.try_as_slice::<PhysicalUtf8>()?;

        match &self.child {
            ChildBuffer::StringViewHeap(heap) => Ok(StringViewBuffer { metadata, heap }),
            _ => Err(RayexecError::new("Missing string heap")),
        }
    }

    pub fn try_as_string_view_buffer_mut(&mut self) -> Result<StringViewBufferMut<'_>> {
        // TODO: Duplicated, but let's us take each field mutably.
        if PhysicalUtf8::PHYSICAL_TYPE != self.physical_type {
            return Err(
                RayexecError::new("Attempted to cast buffer to wrong physical type")
                    .with_field("expected_type", self.physical_type)
                    .with_field("requested_type", PhysicalUtf8::PHYSICAL_TYPE),
            );
        }

        let metadata = unsafe { self.data.as_slice_mut::<StringViewMetadataUnion>() };

        match &mut self.child {
            ChildBuffer::StringViewHeap(heap) => Ok(StringViewBufferMut { metadata, heap }),
            _ => Err(RayexecError::new("Missing string heap")),
        }
    }
}

impl<R: ReservationTracker> Drop for ArrayBuffer<R> {
    fn drop(&mut self) {
        let ptr = self.data.ptr;

        let len = self.data.len * self.physical_type.primary_buffer_mem_size();
        let cap = self.data.cap * self.physical_type.primary_buffer_mem_size();

        let vec = unsafe { Vec::from_raw_parts(ptr, len, cap) };
        std::mem::drop(vec);

        self.data.reservation.free()
    }
}

#[derive(Debug)]
pub enum ChildBuffer {
    StringViewHeap(StringViewHeap),
    None,
}

impl From<StringViewHeap> for ChildBuffer {
    fn from(value: StringViewHeap) -> Self {
        ChildBuffer::StringViewHeap(value)
    }
}

#[derive(Debug)]
struct RawBufferParts<R: ReservationTracker> {
    /// Memory reservation for this buffer.
    reservation: R::Reservation,
    /// Raw pointer to start of vec.
    ptr: *mut u8,
    /// Number of elements `T` in the vec, not bytes.
    len: usize,
    /// Capacity of vec (`T` not bytes).
    cap: usize,
}

impl<R: ReservationTracker> RawBufferParts<R> {
    fn try_new<T: Default + Copy>(tracker: &R, len: usize) -> Result<Self> {
        // Note that `vec!` may over-allocate, so we track that too.
        //
        // See <https://doc.rust-lang.org/std/vec/struct.Vec.html#guarantees>
        // > vec![x; n], vec![a, b, c, d], and Vec::with_capacity(n), will all
        // > produce a Vec with at least the requested capacity.
        let alloc_size = len * std::mem::size_of::<T>();
        let mut reservation = tracker.reserve(alloc_size)?;

        let mut data: Vec<T> = vec![T::default(); len];

        let ptr = data.as_mut_ptr();
        let len = data.len();
        let cap = data.capacity();

        let additional = (cap * std::mem::size_of::<T>()) - alloc_size;
        if additional > 0 {
            let additional = tracker.reserve(additional)?;
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
        I: IntoExactSizedIterator<Item = S::PrimaryBufferType>,
    {
        let iter = iter.into_iter();
        let mut data =
            RawBufferParts::try_new::<S::PrimaryBufferType>(&NopReservationTracker, iter.len())?;

        let data_slice = unsafe { data.as_slice_mut() };
        for (idx, val) in iter.enumerate() {
            data_slice[idx] = val;
        }

        Ok(ArrayBuffer {
            physical_type: S::PHYSICAL_TYPE,
            data,
            child: ChildBuffer::None,
        })
    }
}

#[derive(Debug)]
pub struct StringViewBufferBuilder;

impl StringViewBufferBuilder {
    pub fn from_iter<A, I>(iter: I) -> Result<ArrayBuffer>
    where
        A: AsRef<str>,
        I: IntoExactSizedIterator<Item = A>,
    {
        let iter = iter.into_iter();
        let mut data =
            RawBufferParts::try_new::<StringViewMetadataUnion>(&NopReservationTracker, iter.len())?;

        let mut heap = StringViewHeap::new();

        let data_slice = unsafe { data.as_slice_mut() };
        for (idx, val) in iter.enumerate() {
            let metadata = heap.push_bytes(val.as_ref().as_bytes());
            data_slice[idx] = metadata;
        }

        Ok(ArrayBuffer {
            physical_type: PhysicalUtf8::PHYSICAL_TYPE,
            data,
            child: ChildBuffer::StringViewHeap(heap),
        })
    }
}

#[cfg(test)]
mod tests {
    use addressable::AddressableStorage;
    use physical_type::PhysicalI32;
    use reservation::AtomicReservationTracker;

    use super::*;

    #[test]
    fn reserve_and_drop() {
        let tracker = AtomicReservationTracker::default();

        let buffer = ArrayBuffer::with_len::<PhysicalI32>(&tracker, 4).unwrap();

        let total_reserved = tracker.total_reserved();
        assert!(
            total_reserved >= 16,
            "expected at least 16 bytes reserved, got {total_reserved}"
        );

        std::mem::drop(buffer);

        let total_reserved = tracker.total_reserved();
        assert_eq!(0, total_reserved);
    }

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
        let view_buf = buf.try_as_string_view_buffer().unwrap();

        assert_eq!("a", view_buf.get(0).unwrap());
        assert_eq!("bb", view_buf.get(1).unwrap());
        assert_eq!("ccc", view_buf.get(2).unwrap());
        assert_eq!("ddddddddddddddd", view_buf.get(3).unwrap());
    }
}

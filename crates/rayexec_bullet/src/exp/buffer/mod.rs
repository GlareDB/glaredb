pub mod addressable;
pub mod physical_type;
pub mod reservation;
pub mod string_view;

use physical_type::{PhysicalStorage, PhysicalType, PhysicalUtf8};
use rayexec_error::{RayexecError, Result};
use reservation::{NopReservationTracker, Reservation, ReservationTracker};
use string_view::{StringViewBuffer, StringViewHeap};

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

#[derive(Debug)]
pub enum ChildBuffer {
    StringViewHeap(StringViewHeap),
    None,
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
        child: ChildBuffer,
    ) -> Result<Self> {
        let data = RawBufferParts::try_new::<S::PrimaryBufferType>(tracker, len)?;

        Ok(ArrayBuffer {
            physical_type: S::PHYSICAL_TYPE,
            data,
            child,
        })
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
}

impl<R: ReservationTracker> Drop for ArrayBuffer<R> {
    fn drop(&mut self) {
        let ptr = self.data.ptr;

        let len = self.data.len * self.physical_type.buffer_mem_size();
        let cap = self.data.cap * self.physical_type.buffer_mem_size();

        let vec = unsafe { Vec::from_raw_parts(ptr, len, cap) };
        std::mem::drop(vec);

        self.data.reservation.free()
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

#[cfg(test)]
mod tests {
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
}

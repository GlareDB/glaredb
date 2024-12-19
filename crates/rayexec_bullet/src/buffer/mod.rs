pub mod addressable;
pub mod string_view;

use physical_type::{PhysicalStorage, PhysicalType, PhysicalUtf8};
use rayexec_error::{RayexecError, Result};
use string_view::{StringViewBuffer, StringViewHeap};

mod physical_type;

#[derive(Debug)]
pub struct ArrayBuffer {
    /// The physical type of the buffer.
    physical_type: PhysicalType,
    /// The underlying data for the buffer.
    ///
    /// Stored as raw parts then converted to a slice on access.
    data: RawBufferParts,
    /// Child buffers for extra data.
    child: ChildBuffer,
}

#[derive(Debug)]
pub enum ChildBuffer {
    StringViewHeap(StringViewHeap),
    None,
}

impl ArrayBuffer {
    /// Create a new buffer with the given len.
    pub fn with_len<S: PhysicalStorage>(len: usize) -> Self {
        let data = RawBufferParts::new::<S::PrimaryBufferType>(len);
        ArrayBuffer {
            physical_type: S::PHYSICAL_TYPE,
            data,
            child: ChildBuffer::None,
        }
    }

    pub fn with_len_and_child_buffer<S: PhysicalStorage>(len: usize, child: ChildBuffer) -> Self {
        let data = RawBufferParts::new::<S::PrimaryBufferType>(len);
        ArrayBuffer {
            physical_type: S::PHYSICAL_TYPE,
            data,
            child,
        }
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

impl Drop for ArrayBuffer {
    fn drop(&mut self) {
        let ptr = self.data.ptr;

        let len = self.data.len * self.physical_type.buffer_mem_size();
        let cap = self.data.cap * self.physical_type.buffer_mem_size();

        let vec = unsafe { Vec::from_raw_parts(ptr, len, cap) };
        std::mem::drop(vec);
    }
}

#[derive(Debug, Clone, Copy)]
struct RawBufferParts {
    ptr: *mut u8,
    len: usize,
    cap: usize,
}

impl RawBufferParts {
    fn new<T: Default + Copy>(len: usize) -> Self {
        let mut data: Vec<T> = vec![T::default(); len];

        let ptr = data.as_mut_ptr();
        let len = data.len();
        let cap = data.capacity();

        std::mem::forget(data);

        RawBufferParts {
            ptr: ptr.cast(),
            len,
            cap,
        }
    }

    unsafe fn as_slice<T>(&self) -> &[T] {
        std::slice::from_raw_parts(self.ptr.cast::<T>().cast_const(), self.len)
    }

    unsafe fn as_slice_mut<T>(&mut self) -> &mut [T] {
        std::slice::from_raw_parts_mut(self.ptr.cast::<T>(), self.len)
    }
}

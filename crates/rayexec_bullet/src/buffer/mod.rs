use physical_type::{PhysicalStorage, PhysicalType};
use rayexec_error::{RayexecError, Result};

mod physical_type;

#[derive(Debug)]
pub struct ArrayBuffer {
    /// The physical type of the buffer.
    physical_type: PhysicalType,
    /// The underlying data for the buffer.
    ///
    /// Stored as raw parts then converted to a slice on access.
    data: RawBufferParts,
}

impl ArrayBuffer {
    /// Create a new buffer with the given len.
    pub fn with_len<S: PhysicalStorage>(len: usize) -> Self {
        let data = RawBufferParts::new::<S::BufferType>(len);
        ArrayBuffer {
            physical_type: S::PHYSICAL_TYPE,
            data,
        }
    }

    pub fn try_as_slice<S: PhysicalStorage>(&self) -> Result<&[S::BufferType]> {
        if S::PHYSICAL_TYPE != self.physical_type {
            return Err(
                RayexecError::new("Attempted to cast buffer to wrong physical type")
                    .with_field("expected_type", self.physical_type)
                    .with_field("requested_type", S::PHYSICAL_TYPE),
            );
        }

        let data = unsafe { self.data.as_slice::<S::BufferType>() };

        Ok(data)
    }

    pub fn try_as_slice_mut<S: PhysicalStorage>(&mut self) -> Result<&mut [S::BufferType]> {
        if S::PHYSICAL_TYPE != self.physical_type {
            return Err(
                RayexecError::new("Attempted to cast buffer to wrong physical type")
                    .with_field("expected_type", self.physical_type)
                    .with_field("requested_type", S::PHYSICAL_TYPE),
            );
        }

        let data = unsafe { self.data.as_slice_mut::<S::BufferType>() };

        Ok(data)
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

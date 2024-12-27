pub mod buffer_manager;
pub mod physical_type;
pub mod string_view;

mod raw;

use buffer_manager::{BufferManager, NopBufferManager};
use raw::RawBufferParts;
use rayexec_error::Result;
use string_view::StringViewHeap;

use super::executor::physical_type::{PhysicalStorage, PhysicalType};

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
        unimplemented!()
    }
}

#[derive(Debug)]
pub enum SecondaryBuffer<B: BufferManager> {
    StringViewHeap(StringViewHeap),
    Temp(B),
    None,
}

impl<B: BufferManager> Drop for ArrayBuffer<B> {
    fn drop(&mut self) {
        let ptr = self.primary.ptr;

        unimplemented!()
        // let len = self.primary.len * self.physical_type.primary_buffer_mem_size();
        // let cap = self.primary.cap * self.physical_type.primary_buffer_mem_size();

        // let vec = unsafe { Vec::from_raw_parts(ptr, len, cap) };
        // std::mem::drop(vec);

        // self.primary.reservation.free()
    }
}

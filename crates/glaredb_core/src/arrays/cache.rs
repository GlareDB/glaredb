use glaredb_error::Result;

use super::array::Array;
use super::array::array_buffer::AnyArrayBuffer;
use crate::arrays::array::validity::Validity;
use crate::buffer::buffer_manager::{BufferManager, RawBufferManager};

/// Maybe cache a buffer.
pub trait MaybeCache {
    fn maybe_cache(&mut self, buffer: AnyArrayBuffer);
}

impl<M> MaybeCache for Option<M>
where
    M: MaybeCache,
{
    fn maybe_cache(&mut self, buffer: AnyArrayBuffer) {
        match self {
            Some(cache) => cache.maybe_cache(buffer),
            None => NopCache.maybe_cache(buffer),
        }
    }
}

/// Implementation of `MaybeCache` that always drops the buffer.
#[derive(Debug, Clone, Copy)]
pub struct NopCache;

impl MaybeCache for NopCache {
    fn maybe_cache(&mut self, _buffer: AnyArrayBuffer) {
        // Just drop...
    }
}

// TODO: Rename to BufferAllocator that can happen to reuse buffers.
#[derive(Debug)]
pub struct BufferCache {
    pub(crate) manager: RawBufferManager,
    /// Capacity of all writable arrays in the batch.
    ///
    /// Newly allocated buffers for an array will use this as their initial
    /// capacity.
    pub(crate) capacity: usize,
}

impl BufferCache {
    pub fn new(manager: &impl BufferManager, capacity: usize, _num_arrays: usize) -> Self {
        BufferCache {
            manager: RawBufferManager::from_buffer_manager(manager),
            capacity,
        }
    }

    pub const fn capacity(&self) -> usize {
        self.capacity
    }

    /// Resets all arrays to make them writeable.
    ///
    /// This will attempt to reuse a cached buffer for each array, allocating
    /// new buffers if a cached one isn't available.
    ///
    /// Arrays will all be the same capacity, and all validities reset.
    pub fn reset_arrays(&mut self, arrays: &mut [Array]) -> Result<()> {
        for array in arrays {
            Self::reset_array(&self.manager, array, self.capacity)?;
        }

        Ok(())
    }

    /// Resets a single array, allocating a new buffer if necessary.
    fn reset_array(manager: &RawBufferManager, array: &mut Array, cap: usize) -> Result<()> {
        // TODO: Possibly check ref count.
        // TODO: Check if shared, if not and its the correct capacity, then we can return early.

        let data = AnyArrayBuffer::new_for_datatype(manager, &array.datatype, cap)?;

        array.data = data;
        array.validity = Validity::new_all_valid(cap);

        Ok(())
    }
}

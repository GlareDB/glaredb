use rayexec_error::Result;
use stdutil::convert::TryAsMut;

use super::array::array_buffer::{ArrayBuffer, ArrayBufferType, ScalarBuffer, StringBuffer};
use super::array::buffer_manager::BufferManager;
use super::array::Array;
use crate::arrays::array::physical_type::PhysicalType;
use crate::arrays::array::validity::Validity;

/// Maybe cache a buffer.
pub trait MaybeCache<B: BufferManager> {
    fn maybe_cache(&mut self, buffer: ArrayBuffer<B>);
}

impl<M, B> MaybeCache<B> for Option<M>
where
    M: MaybeCache<B>,
    B: BufferManager,
{
    fn maybe_cache(&mut self, buffer: ArrayBuffer<B>) {
        match self {
            Some(cache) => cache.maybe_cache(buffer),
            None => NopCache.maybe_cache(buffer),
        }
    }
}

/// Implementation of `MaybeCache` that always drops the buffer.
#[derive(Debug, Clone, Copy)]
pub struct NopCache;

impl<B> MaybeCache<B> for NopCache
where
    B: BufferManager,
{
    fn maybe_cache(&mut self, _buffer: ArrayBuffer<B>) {
        // Just drop...
    }
}

#[derive(Debug)]
pub struct BufferCache<B: BufferManager> {
    pub(crate) manager: B,
    /// Contains an optionally cached buffer for each array in a batch.
    pub(crate) cached: Vec<Cached<B>>,
    /// Capacity of all writable arrays in the batch.
    ///
    /// Newly allocated buffers for an array will use this as their initial
    /// capacity.
    pub(crate) capacity: usize,
}

impl<B> BufferCache<B>
where
    B: BufferManager,
{
    pub fn new(manager: B, capacity: usize, num_arrays: usize) -> Self {
        let cached = (0..num_arrays).map(|_| Cached::None).collect();

        BufferCache {
            manager,
            cached,
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
    pub fn reset_arrays(&mut self, arrays: &mut [Array<B>]) -> Result<()> {
        assert_eq!(arrays.len(), self.cached.len());

        for (cached, array) in self.cached.iter_mut().zip(arrays) {
            Self::reset_array(&self.manager, array, cached, self.capacity)?;
        }

        Ok(())
    }

    /// Resets a single array, allocating a new buffer if necessary.
    fn reset_array(
        manager: &B,
        array: &mut Array<B>,
        cached: &mut Cached<B>,
        cap: usize,
    ) -> Result<()> {
        // TODO: Possibly check ref count.
        // TODO: Check if shared, if not and its the correct capacity, then we can return early.

        let cached = std::mem::replace(cached, Cached::None);
        let buffer = match cached {
            Cached::Scalar(scalar) => {
                debug_assert_eq!(scalar.physical_type, array.datatype.physical_type());
                ArrayBuffer::new(scalar)
            }
            Cached::String(string) => {
                debug_assert!(
                    array.datatype.physical_type() == PhysicalType::Utf8
                        || array.datatype.physical_type() == PhysicalType::Binary
                );
                ArrayBuffer::new(string)
            }
            Cached::None => {
                // Need to allocate new buffer.
                ArrayBuffer::try_new_for_datatype(manager, &array.datatype, cap)?
            }
        };

        assert_eq!(cap, buffer.logical_len());

        array.data = buffer;
        array.validity = Validity::new_all_valid(cap);

        Ok(())
    }
}

/// Contains a possibly cached buffer for a single array.
#[derive(Debug)]
pub enum Cached<B: BufferManager> {
    Scalar(ScalarBuffer<B>),
    String(StringBuffer<B>),
    None,
}

impl<B> Cached<B>
where
    B: BufferManager,
{
    pub fn has_cached_buffer(&self) -> bool {
        !matches!(self, Cached::None)
    }
}

impl<B> MaybeCache<B> for Cached<B>
where
    B: BufferManager,
{
    /// Maybe cache the provided buffer.
    ///
    /// The buffer will be dropped if:
    ///
    /// - we already have a buffer in the cache.
    /// - the buffer is not a type we can cache (yet).
    /// - the buffer contains a shared component.
    fn maybe_cache(&mut self, buffer: ArrayBuffer<B>) {
        if self.has_cached_buffer() {
            // Already have buffer cached.
            return;
        }

        // TODO: Possibly check ref count.

        match buffer.into_inner() {
            ArrayBufferType::Scalar(buf) => {
                if buf.raw.is_owned() {
                    *self = Cached::Scalar(buf)
                }
            }
            ArrayBufferType::String(mut buf) => {
                if buf.metadata.is_owned() && buf.buffer.is_owned() {
                    let heap = buf.buffer.try_as_mut().expect("heap to be owned");
                    heap.clear();

                    // TODO: Zero out metadatas?

                    *self = Cached::String(buf)
                }
            }
            ArrayBufferType::Constant(constant) => {
                // Possibly peel off constant row selection.
                self.maybe_cache(*constant.child_buffer);
            }
            ArrayBufferType::Dictionary(dict) => {
                // Possibly peel off selection.
                self.maybe_cache(*dict.child_buffer);
            }
            _ => {
                // Just drop, not a buffer type we can cache yet.
            }
        }
    }
}

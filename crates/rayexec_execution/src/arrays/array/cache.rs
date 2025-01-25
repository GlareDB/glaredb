use rayexec_error::Result;
use stdutil::convert::TryAsMut;

use super::array_buffer::{ScalarBuffer, StringBuffer};
use super::buffer_manager::BufferManager;
use super::{Array, ArrayBufferType};
use crate::arrays::array::physical_type::PhysicalType;
use crate::arrays::array::ArrayBuffer;

/// Contains a possibly cached buffer for a single array.
#[derive(Debug)]
pub struct BufferCache<B: BufferManager> {
    /// Contains the (optional) cached buffer.
    cached: Cached<B>,
    /// Capacity of the cached buffer.
    ///
    /// If we don't actually have a cached buffer, this capacity will be used
    /// during allocation of a new buffer.
    capacity: usize,
}

#[derive(Debug)]
pub enum Cached<B: BufferManager> {
    Scalar(ScalarBuffer<B>),
    String(StringBuffer<B>),
    None,
}

impl<B> BufferCache<B>
where
    B: BufferManager,
{
    /// Creates a new buffer cache with an initially empty buffer.
    ///
    /// `capacity` is used when allocating new buffers for arrays if we don't
    /// have a cached buffer.
    pub const fn new(capacity: usize) -> Self {
        BufferCache {
            cached: Cached::None,
            capacity,
        }
    }

    pub fn has_cached_buffer(&self) -> bool {
        !matches!(self.cached, Cached::None)
    }

    /// Maybe cache the provided buffer.
    ///
    /// The buffer will be dropped if:
    ///
    /// - we already have a buffer in the cache.
    /// - the buffer is not a type we can cache (yet).
    /// - the buffer contains a shared component.
    pub fn maybe_cache(&mut self, buffer: ArrayBuffer<B>) {
        if self.has_cached_buffer() {
            // Already have buffer cached.
            return;
        }

        // TODO: Possibly check ref count.

        match buffer.into_inner() {
            ArrayBufferType::Scalar(buf) => {
                if buf.raw.is_owned() {
                    self.cached = Cached::Scalar(buf)
                }
            }
            ArrayBufferType::String(mut buf) => {
                if buf.metadata.is_owned() && buf.heap.is_owned() {
                    let heap = buf.heap.try_as_mut().expect("heap to be owned");
                    heap.clear();

                    // TODO: Zero out metadatas?

                    self.cached = Cached::String(buf)
                }
            }
            _ => {
                // Just drop, not a buffer type we can check yet.
            }
        }
    }

    /// Try to move a cached buffer into the array. If we don't have a cached
    /// buffer available, then allocate a new one.
    pub fn take_buffer_or_allocate(&mut self, manager: &B, array: &mut Array<B>) -> Result<()> {
        // TODO: Possibly check ref count.

        let cached = std::mem::replace(&mut self.cached, Cached::None);
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
                ArrayBuffer::try_new_for_datatype(manager, &array.datatype, self.capacity)?
            }
        };

        array.data = buffer;

        Ok(())
    }
}

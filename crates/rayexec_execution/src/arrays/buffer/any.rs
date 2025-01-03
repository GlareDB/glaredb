use super::buffer_manager::BufferManager;
use super::physical_type::Addressable;
use super::ArrayBuffer;

/// Representation of the existence of a value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AnyValue;

/// Wrapper around an array buffer for providing `AddressableStorage`
/// functionality for any array buffer type.
///
/// This is used when the values themselves don't matter, only that they exist.
#[derive(Debug)]
pub struct AnyAddressable<'a, B: BufferManager> {
    pub(crate) buffer: &'a ArrayBuffer<B>,
}

impl<'a, B> Addressable for AnyAddressable<'a, B>
where
    B: BufferManager,
{
    type T = AnyValue;

    fn len(&self) -> usize {
        self.buffer.capacity()
    }

    fn get(&self, idx: usize) -> Option<&Self::T> {
        if idx < self.buffer.capacity() {
            Some(&AnyValue)
        } else {
            None
        }
    }
}

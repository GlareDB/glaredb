use super::ArrayBuffer;
use crate::arrays::buffer_manager::BufferManager;
use crate::arrays::validity::Validity;

#[derive(Debug)]
pub struct DictionaryBuffer<B: BufferManager> {
    pub(crate) validity: Validity,
    pub(crate) buffer: ArrayBuffer<B>,
}

impl<B> DictionaryBuffer<B>
where
    B: BufferManager,
{
    pub fn new(buffer: ArrayBuffer<B>, validity: Validity) -> Self {
        debug_assert_eq!(buffer.len(), validity.len());
        DictionaryBuffer { buffer, validity }
    }
}

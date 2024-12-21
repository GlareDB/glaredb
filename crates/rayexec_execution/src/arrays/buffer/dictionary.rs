use crate::arrays::array::ArrayData;
use crate::arrays::buffer_manager::BufferManager;
use crate::arrays::validity::Validity;

#[derive(Debug)]
pub struct DictionaryBuffer<B: BufferManager> {
    pub(crate) validity: Validity,
    pub(crate) buffer: ArrayData<B>,
}

impl<B> DictionaryBuffer<B>
where
    B: BufferManager,
{
    pub fn new(buffer: ArrayData<B>, validity: Validity) -> Self {
        debug_assert_eq!(buffer.capacity(), validity.len());
        DictionaryBuffer { buffer, validity }
    }
}

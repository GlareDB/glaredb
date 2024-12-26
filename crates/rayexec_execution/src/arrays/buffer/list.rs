use crate::arrays::array::Array;
use crate::arrays::buffer_manager::BufferManager;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ListItemMetadata {
    pub offset: i32,
    pub len: i32,
}

#[derive(Debug)]
pub struct ListBuffer<B: BufferManager> {
    pub(crate) child: Array<B>,
}

impl<B> ListBuffer<B>
where
    B: BufferManager,
{
    pub fn new(child: Array<B>) -> Self {
        ListBuffer { child }
    }
}

use super::array_buffer::ArrayBuffer;
use super::selection::Selection;

/// Describes how we should iterate over the buffer during execution.
#[derive(Debug)]
pub enum ExecutionFormat<'a, B: ArrayBuffer> {
    /// No transformations needed to read the buffer, read from the first index
    /// to the last.
    NoTransformation(&'a B),
    /// We have a selection on the buffer. Read the selection to determine how
    /// we should iterate over the buffer.
    Selection(SelectionFormat<'a, B>),
}

#[derive(Debug)]
pub struct SelectionFormat<'a, B: ArrayBuffer> {
    /// Selection indices for the buffer.
    pub(crate) selection: Selection<'a>,
    /// The buffer itself.
    pub(crate) buffer: &'a B,
}

impl<'a, B> SelectionFormat<'a, B>
where
    B: ArrayBuffer,
{
    pub fn logical_len(&self) -> usize {
        self.selection.len()
    }
}

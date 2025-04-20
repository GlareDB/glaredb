use super::array_buffer::ArrayBuffer;
use super::selection::Selection;

/// Describes how we should iterate over the buffer during execution.
#[derive(Debug)]
pub enum ExecutionFormat<'a, B: ArrayBuffer> {
    /// No transformations needed to read the buffer, read from the first index
    /// to the last.
    Flat(&'a B),
    /// We have a selection on the buffer. Read the selection to determine how
    /// we should iterate over the buffer.
    Selection(SelectionFormat<'a, B>),
}

impl<'a, B> ExecutionFormat<'a, B>
where
    B: ArrayBuffer,
{
    pub const fn is_selection(&self) -> bool {
        matches!(self, ExecutionFormat::Selection(_))
    }
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
    pub fn flat(buffer: &'a B) -> Self {
        SelectionFormat {
            selection: Selection::linear(0, buffer.logical_len()),
            buffer,
        }
    }

    pub fn logical_len(&self) -> usize {
        self.selection.len()
    }
}

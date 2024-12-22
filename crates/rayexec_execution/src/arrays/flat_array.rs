use rayexec_error::Result;

use super::array::Array;
use super::buffer::physical_type::PhysicalDictionary;
use super::buffer::ArrayBuffer;
use super::buffer_manager::{BufferManager, NopBufferManager};
use super::validity::Validity;

/// A view on top of normal arrays flattening some parts of the nested
/// structure.
#[derive(Debug)]
pub struct FlatArrayView<'a, B: BufferManager = NopBufferManager> {
    pub(crate) validity: &'a Validity,
    pub(crate) array_buffer: &'a ArrayBuffer<B>,
    pub(crate) selection: FlatSelection<'a>,
}

impl<'a, B> FlatArrayView<'a, B>
where
    B: BufferManager,
{
    pub fn from_array(array: &'a Array<B>) -> Result<Self> {
        if array.is_dictionary() {
            let selection = array.data.try_as_slice::<PhysicalDictionary>()?;
            let dict_buffer = array.data.secondary_buffers().try_as_dictionary_buffer()?;

            Ok(FlatArrayView {
                validity: &dict_buffer.validity,
                array_buffer: &dict_buffer.buffer,
                selection: FlatSelection::selection(selection),
            })
        } else {
            Ok(FlatArrayView {
                validity: &array.validity,
                array_buffer: &array.data,
                selection: FlatSelection::linear(array.capacity()),
            })
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum FlatSelection<'a> {
    /// Constant selection.
    ///
    /// All indices point to the same location.
    Constant { len: usize, loc: usize },
    /// Represents a linear selection.
    ///
    /// '0..len'
    Linear { len: usize },
    /// Represents the true location to use for some index.
    Selection(&'a [usize]),
}

impl<'a> FlatSelection<'a> {
    pub fn constant(len: usize, loc: usize) -> Self {
        Self::Constant { len, loc }
    }

    pub fn linear(len: usize) -> Self {
        Self::Linear { len }
    }

    pub fn selection(sel: &'a [usize]) -> Self {
        Self::Selection(sel)
    }

    pub fn is_linear(&self) -> bool {
        matches!(self, FlatSelection::Linear { .. })
    }

    pub fn iter(&self) -> FlatSelectionIter {
        FlatSelectionIter { idx: 0, sel: *self }
    }

    pub fn len(&self) -> usize {
        match self {
            Self::Constant { len, .. } => *len,
            Self::Linear { len } => *len,
            Self::Selection(sel) => sel.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub fn get(&self, idx: usize) -> Option<usize> {
        match self {
            Self::Constant { len, loc } => {
                if idx >= *len {
                    None
                } else {
                    Some(*loc)
                }
            }
            Self::Linear { len } => {
                if idx >= *len {
                    None
                } else {
                    Some(idx)
                }
            }
            Self::Selection(sel) => sel.get(idx).copied(),
        }
    }
}

impl<'a> IntoIterator for FlatSelection<'a> {
    type Item = usize;
    type IntoIter = FlatSelectionIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        FlatSelectionIter { idx: 0, sel: self }
    }
}

#[derive(Debug)]
pub struct FlatSelectionIter<'a> {
    idx: usize,
    sel: FlatSelection<'a>,
}

impl<'a> Iterator for FlatSelectionIter<'a> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        if self.idx >= self.sel.len() {
            return None;
        }

        let v = match self.sel {
            FlatSelection::Constant { loc, .. } => loc,
            FlatSelection::Linear { .. } => self.idx,
            FlatSelection::Selection(sel) => sel[self.idx],
        };

        self.idx += 1;

        Some(v)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let rem = self.sel.len() - self.idx;
        (rem, Some(rem))
    }
}

impl<'a> ExactSizeIterator for FlatSelectionIter<'a> {}

use rayexec_error::Result;

use super::array::Array;
use super::buffer::physical_type::PhysicalDictionary;
use super::buffer::reservation::{NopReservationTracker, ReservationTracker};
use super::buffer::ArrayBuffer;
use super::validity::Validity;

/// A view on top of normal arrays flattening some parts of the nested
/// structure.
#[derive(Debug)]
pub struct FlatArrayView<'a, R: ReservationTracker = NopReservationTracker> {
    pub(crate) validity: &'a Validity,
    pub(crate) array_buffer: &'a ArrayBuffer<R>,
    pub(crate) selection: FlatSelection<'a>,
}

impl<'a, R> FlatArrayView<'a, R>
where
    R: ReservationTracker,
{
    pub fn from_array(array: &'a Array<R>) -> Result<Self> {
        if array.is_dictionary() {
            let selection = array.buffer.try_as_slice::<PhysicalDictionary>()?;
            let dict_buffer = array
                .buffer
                .secondary_buffers()
                .try_as_dictionary_buffer()?;

            Ok(FlatArrayView {
                validity: &dict_buffer.validity,
                array_buffer: &dict_buffer.buffer,
                selection: FlatSelection::selection(selection),
            })
        } else {
            Ok(FlatArrayView {
                validity: &array.validity,
                array_buffer: &array.buffer,
                selection: FlatSelection::linear(array.len()),
            })
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum FlatSelection<'a> {
    /// Represents a linear selection.
    ///
    /// '0..len'
    Linear(usize),
    /// Represents the true location to use for some index.
    Selection(&'a [usize]),
}

impl<'a> FlatSelection<'a> {
    pub fn linear(len: usize) -> Self {
        Self::Linear(len)
    }

    pub fn selection(sel: &'a [usize]) -> Self {
        Self::Selection(sel)
    }

    pub fn iter(&self) -> FlatSelectionIter {
        FlatSelectionIter { idx: 0, sel: *self }
    }

    pub fn len(&self) -> usize {
        match self {
            Self::Linear(len) => *len,
            Self::Selection(sel) => sel.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub fn get(&self, idx: usize) -> Option<usize> {
        match self {
            Self::Linear(len) => {
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
            FlatSelection::Linear(_) => self.idx,
            FlatSelection::Selection(sel) => sel[self.idx],
        };

        self.idx += 1;

        Some(v)
    }
}

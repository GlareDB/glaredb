use rayexec_error::Result;

use super::array_buffer::ArrayBuffer;
use super::buffer_manager::{BufferManager, NopBufferManager};
use super::selection::Selection;
use super::validity::Validity;
use super::Array;
use crate::arrays::array::array_buffer::ArrayBufferType;

/// A view on top of normal arrays flattening some parts of the nested
/// structure.
#[derive(Debug)]
pub struct FlattenedArray<'a, B: BufferManager = NopBufferManager> {
    pub(crate) validity: &'a Validity,
    pub(crate) array_buffer: &'a ArrayBuffer<B>,
    pub(crate) selection: Selection<'a>,
}

impl<'a, B> FlattenedArray<'a, B>
where
    B: BufferManager,
{
    pub fn from_array(array: &'a Array<B>) -> Result<Self> {
        match array.data.as_ref() {
            ArrayBufferType::Dictionary(dict) => Ok(FlattenedArray {
                validity: &array.validity,
                array_buffer: &dict.child_buffer,
                selection: Selection::slice(dict.selection.as_slice()),
            }),
            ArrayBufferType::Constant(constant) => Ok(FlattenedArray {
                validity: &array.validity,
                array_buffer: &constant.child_buffer,
                selection: Selection::constant(constant.len, constant.row_reference),
            }),
            _ => {
                // Everything else just stays as-is.
                Ok(FlattenedArray {
                    validity: &array.validity,
                    array_buffer: &array.data,
                    selection: Selection::linear(0, array.validity.len()),
                })
            }
        }
    }

    pub fn logical_len(&self) -> usize {
        self.selection.len()
    }
}

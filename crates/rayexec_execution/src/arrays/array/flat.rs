use rayexec_error::{RayexecError, Result};

use super::exp::Array;
use super::selection::Selection;
use super::validity::Validity;
use crate::arrays::buffer::buffer_manager::{BufferManager, NopBufferManager};
use crate::arrays::buffer::physical_type::PhysicalDictionary;
use crate::arrays::buffer::{ArrayBuffer, SecondaryBuffer};

/// A view on top of normal arrays flattening some parts of the nested
/// structure.
#[derive(Debug)]
pub struct FlatArrayView<'a, B: BufferManager = NopBufferManager> {
    pub(crate) validity: &'a Validity,
    pub(crate) array_buffer: &'a ArrayBuffer<B>,
    pub(crate) selection: Selection<'a>,
}

impl<'a, B> FlatArrayView<'a, B>
where
    B: BufferManager,
{
    pub fn from_array(array: &'a Array<B>) -> Result<Self> {
        if array.is_dictionary() {
            let selection = array.data.try_as_slice::<PhysicalDictionary>()?;
            let dict_buffer = match array.data.get_secondary() {
                SecondaryBuffer::Dictionary(dict) => dict,
                _ => {
                    return Err(RayexecError::new(
                        "Secondary buffer not a dictionary buffer",
                    ))
                }
            };

            Ok(FlatArrayView {
                validity: &dict_buffer.validity,
                array_buffer: &dict_buffer.buffer,
                selection: Selection::selection(selection),
            })
        } else {
            Ok(FlatArrayView {
                validity: &array.validity,
                array_buffer: &array.data,
                selection: Selection::linear(array.capacity()),
            })
        }
    }

    pub fn logical_len(&self) -> usize {
        self.selection.len()
    }
}

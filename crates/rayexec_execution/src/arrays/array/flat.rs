use rayexec_error::{RayexecError, Result};

use super::array_buffer::{ArrayBuffer, SecondaryBuffer};
use super::buffer_manager::{BufferManager, NopBufferManager};
use super::physical_type::PhysicalDictionary;
use super::selection::Selection;
use super::validity::Validity;
use super::Array;

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
        let data = &array.next.as_ref().unwrap().data;
        if array.is_dictionary() {
            let selection = data.try_as_slice::<PhysicalDictionary>()?;

            let dict_buffer = match data.get_secondary() {
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
                selection: Selection::slice(selection),
            })
        } else {
            let validity = &array.next.as_ref().unwrap().validity;

            Ok(FlatArrayView {
                validity,
                array_buffer: data,
                selection: Selection::linear(array.capacity()),
            })
        }
    }

    pub fn logical_len(&self) -> usize {
        self.selection.len()
    }
}

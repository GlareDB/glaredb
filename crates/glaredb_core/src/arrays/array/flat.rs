use glaredb_error::Result;

use super::Array;
use super::array_buffer::{ArrayBuffer, ArrayBuffer2};
use super::physical_type::PhysicalType;
use super::selection::Selection;
use super::validity::Validity;
use crate::arrays::array::array_buffer::ArrayBufferType2;

#[derive(Debug)]
pub struct FlattenedBuffer<'a, B: ArrayBuffer> {
    /// Order of indices to read.
    ///
    /// Determines the logical length of the buffer.
    pub(crate) selection: Selection<'a>,
    /// The array buffer itself.
    pub(crate) buffer: &'a B,
}

impl<'a, B> FlattenedBuffer<'a, B>
where
    B: ArrayBuffer,
{
    pub fn logical_len(&self) -> usize {
        self.selection.len()
    }
}

/// A view on top of normal arrays flattening some parts of the nested
/// structure.
#[derive(Debug)]
pub struct FlattenedArray<'a> {
    pub(crate) validity: &'a Validity,
    pub(crate) array_buffer: &'a ArrayBuffer2,
    pub(crate) selection: Selection<'a>,
}

impl<'a> FlattenedArray<'a> {
    /// Create a new flattend array with a linear selection.
    ///
    /// This is useful for when we have borrowed data from an array.
    ///
    /// The logical length of the buffer and the length of the validity must be
    /// the same.
    pub fn from_buffer_and_validity(
        buffer: &'a ArrayBuffer2,
        validity: &'a Validity,
    ) -> Result<Self> {
        debug_assert_eq!(buffer.logical_len(), validity.len());
        match buffer.as_ref() {
            ArrayBufferType2::Dictionary(dict) => Ok(FlattenedArray {
                validity,
                array_buffer: &dict.child_buffer,
                selection: Selection::slice(dict.selection.as_slice()),
            }),
            ArrayBufferType2::Constant(constant) => Ok(FlattenedArray {
                validity,
                array_buffer: &constant.child_buffer,
                selection: Selection::constant(constant.len, constant.row_reference),
            }),
            _ => {
                // Everything else just stays as-is.
                Ok(FlattenedArray {
                    validity,
                    array_buffer: buffer,
                    selection: Selection::linear(0, validity.len()),
                })
            }
        }
    }

    /// Create a flattend array from a normal array.
    ///
    /// This will pull up any selections from child buffers.
    pub fn from_array(array: &'a Array) -> Result<Self> {
        unimplemented!()
        // Self::from_buffer_and_validity(&array.data, &array.validity)
    }

    pub fn physical_type(&self) -> PhysicalType {
        self.array_buffer.physical_type()
    }

    /// Get the logical length of the array based on the selection.
    pub fn logical_len(&self) -> usize {
        self.selection.len()
    }
}

use rayexec_error::{RayexecError, Result};

use super::buffer::addressable::AddressableStorage;
use super::buffer::dictionary::DictionaryBuffer;
use super::buffer::physical_type::{PhysicalDictionary, PhysicalStorage, PhysicalType};
use super::buffer::reservation::{NopReservationTracker, ReservationTracker};
use super::buffer::{ArrayBuffer, SecondaryBuffers};
use super::validity::Validity;
use crate::compute::util::IntoExactSizedIterator;
use crate::datatype::DataType;

#[derive(Debug)]
pub struct Array<R: ReservationTracker = NopReservationTracker> {
    /// Data type of the array.
    pub(crate) datatype: DataType,
    pub(crate) validity: Validity,
    /// Buffer containing the underlying array data.
    pub(crate) buffer: ArrayBuffer<R>,
}

impl<R> Array<R>
where
    R: ReservationTracker,
{
    pub fn new(datatype: DataType, buffer: ArrayBuffer<R>) -> Self {
        let validity = Validity::new_all_valid(buffer.len());
        Array {
            datatype,
            validity,
            buffer,
        }
    }

    pub fn new_with_validity(
        datatype: DataType,
        buffer: ArrayBuffer<R>,
        validity: Validity,
    ) -> Result<Self> {
        if validity.len() != buffer.len() {
            return Err(
                RayexecError::new("Validty length does not match buffer length")
                    .with_field("validity_len", validity.len())
                    .with_field("buffer_len", buffer.len()),
            );
        }

        Ok(Array {
            datatype,
            validity,
            buffer,
        })
    }

    pub fn validity(&self) -> &Validity {
        &self.validity
    }

    pub fn buffer(&self) -> &ArrayBuffer<R> {
        &self.buffer
    }

    pub fn buffer_mut(&mut self) -> &mut ArrayBuffer<R> {
        &mut self.buffer
    }

    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Selects indice from the array.
    ///
    /// This will convert the underlying array buffer into a dictionary buffer.
    pub fn select(
        &mut self,
        tracker: &R,
        selection: impl IntoExactSizedIterator<Item = usize>,
    ) -> Result<()> {
        if self.is_dictionary() {
            // Already dictionary, select the selection.
            let sel = selection.into_iter();
            let mut new_buf = ArrayBuffer::with_len::<PhysicalDictionary>(tracker, sel.len())?;

            let old_sel = self.buffer.try_as_slice::<PhysicalDictionary>()?;
            let new_sel = new_buf.try_as_slice_mut::<PhysicalDictionary>()?;

            for (sel_idx, sel_buf) in sel.zip(new_sel) {
                let idx = old_sel[sel_idx];
                *sel_buf = idx;
            }

            // Now swap the secondary buffers, the dictionary buffer will now be
            // on `new_buf`.
            std::mem::swap(
                self.buffer.secondary_buffers_mut(),
                new_buf.secondary_buffers_mut(),
            );

            // And set the new buf, old buf gets dropped.
            self.buffer = new_buf;

            return Ok(());
        }

        let sel = selection.into_iter();
        let mut new_buf = ArrayBuffer::with_len::<PhysicalDictionary>(tracker, sel.len())?;

        let new_buf_slice = new_buf.try_as_slice_mut::<PhysicalDictionary>()?;

        // Set all selection indices in the new array buffer.
        for (sel_idx, sel_buf) in sel.zip(new_buf_slice) {
            *sel_buf = sel_idx
        }

        // TODO: Probably verify selection all in bounds.

        // Now replace the original buffer, and put the original buffer in the
        // secondary buffer.
        let orig_validity =
            std::mem::replace(&mut self.validity, Validity::new_all_valid(new_buf.len()));
        let orig_buffer = std::mem::replace(&mut self.buffer, new_buf);
        *self.buffer.secondary_buffers_mut() =
            SecondaryBuffers::Dictionary(DictionaryBuffer::new(orig_buffer, orig_validity));

        Ok(())
    }

    /// If this array is a dictionary array.
    pub fn is_dictionary(&self) -> bool {
        self.buffer.physical_type() == PhysicalType::Dictionary
    }

    pub fn get_dictionary_buffer(&self) -> Option<&DictionaryBuffer<R>> {
        match self.buffer.secondary_buffers() {
            SecondaryBuffers::Dictionary(buf) => Some(buf),
            _ => None,
        }
    }
}

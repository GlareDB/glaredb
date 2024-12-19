use super::buffer::reservation::{NopReservation, NopReservationTracker, ReservationTracker};
use super::buffer::ArrayBuffer;
use super::validity::Validity;
use crate::bitmap::Bitmap;
use crate::datatype::DataType;
use crate::shared_or_owned::SharedOrOwned;

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

    pub fn validity(&self) -> &Validity {
        &self.validity
    }

    pub fn buffer(&self) -> &ArrayBuffer<R> {
        &self.buffer
    }

    pub fn buffer_mut(&mut self) -> &mut ArrayBuffer<R> {
        &mut self.buffer
    }
}

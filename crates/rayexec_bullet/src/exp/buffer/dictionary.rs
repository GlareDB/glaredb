use super::reservation::ReservationTracker;
use super::ArrayBuffer;
use crate::exp::validity::Validity;

#[derive(Debug)]
pub struct DictionaryBuffer<R: ReservationTracker> {
    pub(crate) validity: Validity,
    pub(crate) buffer: ArrayBuffer<R>,
}

impl<R> DictionaryBuffer<R>
where
    R: ReservationTracker,
{
    pub fn new(buffer: ArrayBuffer<R>, validity: Validity) -> Self {
        debug_assert_eq!(buffer.len(), validity.len());
        DictionaryBuffer { buffer, validity }
    }
}

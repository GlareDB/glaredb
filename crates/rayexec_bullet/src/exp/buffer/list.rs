use super::reservation::{NopReservationTracker, ReservationTracker};
use crate::exp::array::Array;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ListItemMetadata {
    pub offset: i32,
    pub len: i32,
}

#[derive(Debug)]
pub struct ListBuffer<R: ReservationTracker> {
    child: Array<R>,
}

impl<R> ListBuffer<R>
where
    R: ReservationTracker,
{
    pub fn new(child: Array<R>) -> Self {
        ListBuffer { child }
    }
}

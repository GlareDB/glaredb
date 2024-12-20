use super::reservation::ReservationTracker;
use crate::exp::array::Array;

/// Zero-sized type that gets stored in the primary buffer.
///
/// Since all data exists in the child buffers, using a zero-sized type for the
/// primary buffer lets us continue to track length uniformly with the other
/// types.
#[derive(Debug, Clone, Default, Copy, PartialEq, Eq)]
pub struct StructItemMetadata;

#[derive(Debug)]
pub struct StructBuffer<R: ReservationTracker> {
    /// Children of equal size length making up the struct.
    pub(crate) children: Vec<Array<R>>,
}

use glaredb_core::arrays::array::physical_type::{
    AddressableMut,
    MutableScalarStorage,
    PhysicalI64,
};

use super::{ReaderErrorState, ValueReader};
use crate::column::read_buffer::ReadCursor;

/// Julian day 2440588 corresponds to Unix epoch (1970-01-01)
const UNIX_EPOCH_JULIAN: u32 = 2_440_588;

const MILLIS_PER_DAY: i64 = 86_400_000;
const MICROS_PER_DAY: i64 = MILLIS_PER_DAY * 1_000;
const NANOS_PER_DAY: i64 = MICROS_PER_DAY * 1_000;

/// Interprets an Int96 as Timestamp(ns).
///
/// - First 64 bits (8 bytes): nanoseconds since midnight
/// - Next 32 bits (4 bytes): Julian day
#[derive(Debug, Clone, Copy, Default)]
pub struct Int96TsReader;

impl ValueReader for Int96TsReader {
    type Storage = PhysicalI64;

    unsafe fn read_next_unchecked(
        &mut self,
        data: &mut ReadCursor,
        out_idx: usize,
        out: &mut <Self::Storage as MutableScalarStorage>::AddressableMut<'_>,
        _error_state: &mut ReaderErrorState,
    ) {
        let nanos = unsafe { data.read_next_unchecked::<i64>() };
        let julian = unsafe { data.read_next_unchecked::<u32>() };
        let days_since = julian - UNIX_EPOCH_JULIAN;
        let full_nanos = (days_since as i64 * NANOS_PER_DAY) + nanos;

        out.put(out_idx, &full_nanos);
    }

    unsafe fn skip_unchecked(
        &mut self,
        data: &mut ReadCursor,
        _error_state: &mut ReaderErrorState,
    ) {
        unsafe {
            data.skip_bytes_unchecked(12);
        }
    }
}

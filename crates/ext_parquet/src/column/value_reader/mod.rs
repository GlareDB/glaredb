pub mod bool;
pub mod int96;
pub mod primitive;
pub mod varlen;

use std::fmt::Debug;

use glaredb_core::arrays::array::physical_type::MutableScalarStorage;
use glaredb_core::storage::scan_filter::PhysicalScanFilter;
use glaredb_error::{DbError, Result};

use super::row_group_pruner::{PlainType, RowGroupPruner};
use crate::column::read_buffer::ReadCursor;

/// Reads values from the read buffer, and writes them to addressable storage.
///
/// The buffers passed to this read are exact sized, and the index to write
/// values to should always be in bounds relative to the provided storage. This
/// should never error.
///
/// A new value reader is created every time a new page is loaded (via
/// `Default`).
///
/// # Safety
///
/// For fixed-sized data, the caller should guarantee that we have the correct
/// amount of bytes in the buffer. Implementations should read directly from the
/// buffer without length checks.
///
/// For varlen data with interleaved lengths, the checked methods on the buffer
/// should be used, and any failures should be written to the error state.
// TODO: Add Fixed/Variable associated constant.
pub trait ValueReader: Default + Debug + Sync + Send {
    type Storage: MutableScalarStorage;
    type PlainType: PlainType;

    /// Read the next value in the buffer, writing it the mutable storage at the
    /// given index.
    unsafe fn read_next_unchecked(
        &mut self,
        data: &mut ReadCursor,
        out_idx: usize,
        out: &mut <Self::Storage as MutableScalarStorage>::AddressableMut<'_>,
        error_state: &mut ReaderErrorState,
    );

    /// Skip the next value in the buffer.
    unsafe fn skip_unchecked(&mut self, data: &mut ReadCursor, error_state: &mut ReaderErrorState);
}

#[derive(Debug, Default)]
pub struct ReaderErrorState {
    error: Option<DbError>,
}

impl ReaderErrorState {
    pub fn set_error_fn<F>(&mut self, error_fn: F)
    where
        F: FnOnce() -> DbError,
    {
        if self.error.is_none() {
            self.error = Some(error_fn())
        }
    }

    pub fn into_result(self) -> Result<()> {
        match self.error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

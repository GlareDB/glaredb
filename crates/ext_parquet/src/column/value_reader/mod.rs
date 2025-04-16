pub mod bool;
pub mod int96;
pub mod primitive;
pub mod varlen;

use std::fmt::Debug;

use glaredb_core::arrays::array::physical_type::MutableScalarStorage;

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
/// The caller must guarantee that we have sufficient data in the buffer to read
/// a complete value.
pub trait ValueReader: Default + Debug + Sync + Send {
    type Storage: MutableScalarStorage;

    /// Read the next value in the buffer, writing it the mutable storage at the
    /// given index.
    unsafe fn read_next_unchecked(
        &mut self,
        data: &mut ReadCursor,
        out_idx: usize,
        out: &mut <Self::Storage as MutableScalarStorage>::AddressableMut<'_>,
    );

    /// Skip the next value in the buffer.
    unsafe fn skip_unchecked(&mut self, data: &mut ReadCursor);
}

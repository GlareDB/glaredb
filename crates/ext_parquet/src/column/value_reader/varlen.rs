use glaredb_core::arrays::array::physical_type::{
    AddressableMut, MutableScalarStorage, PhysicalBinary, PhysicalUtf8,
};
use glaredb_error::DbError;

use super::{ReaderErrorState, ValueReader};
use crate::column::read_buffer::ReadCursor;
use crate::column::row_group_pruner::PlainTypeByteArray;

/// Value reader for reading variable length strings and byte arrays.
#[derive(Debug, Clone, Copy, Default)]
pub struct BinaryValueReader;

impl ValueReader for BinaryValueReader {
    type Storage = PhysicalBinary;
    type PlainType = PlainTypeByteArray;

    unsafe fn read_next_unchecked(
        &mut self,
        data: &mut ReadCursor,
        out_idx: usize,
        out: &mut <Self::Storage as MutableScalarStorage>::AddressableMut<'_>,
        error_state: &mut ReaderErrorState,
    ) {
        let bs = data
            .read_next::<u32>()
            .and_then(|len| data.read_bytes(len as usize));

        match bs {
            Some(bs) => out.put(out_idx, bs),
            None => error_state.set_error_fn(|| DbError::new("Not enough bytes to read")),
        }
    }

    unsafe fn skip_unchecked(&mut self, data: &mut ReadCursor, error_state: &mut ReaderErrorState) {
        let did_skip = data
            .read_next::<u32>()
            .map(|len| data.skip_bytes(len as usize))
            .unwrap_or(false);
        if !did_skip {
            error_state.set_error_fn(|| DbError::new("Not enough bytes to skip"));
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct Utf8ValueReader;

impl ValueReader for Utf8ValueReader {
    type Storage = PhysicalUtf8;
    type PlainType = PlainTypeByteArray;

    unsafe fn read_next_unchecked(
        &mut self,
        data: &mut ReadCursor,
        out_idx: usize,
        out: &mut <Self::Storage as MutableScalarStorage>::AddressableMut<'_>,
        error_state: &mut ReaderErrorState,
    ) {
        let bs = data
            .read_next::<u32>()
            .and_then(|len| data.read_bytes(len as usize));

        match bs {
            Some(bs) => match std::str::from_utf8(bs) {
                Ok(s) => out.put(out_idx, s),
                Err(err) => error_state.set_error_fn(move || {
                    DbError::with_source("Failed to read valid utf8 bytes", Box::new(err))
                }),
            },
            None => error_state.set_error_fn(|| DbError::new("Not enough bytes to read")),
        }
    }

    unsafe fn skip_unchecked(&mut self, data: &mut ReadCursor, error_state: &mut ReaderErrorState) {
        let did_skip = data
            .read_next::<u32>()
            .map(|len| data.skip_bytes(len as usize))
            .unwrap_or(false);
        if !did_skip {
            error_state.set_error_fn(|| DbError::new("Not enough bytes to skip"));
        }
    }
}

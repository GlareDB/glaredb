use arrow::ffi;
use bytes::{Buf, BufMut};
use datafusion::arrow::array::ArrayData;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use std::io::{self, Read, Write};
use std::ops::Deref;
use std::sync::Arc;

#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    #[error("invalid buffer length: {0}")]
    InvalidBufferLen(usize),
    #[error("invalid marker")]
    InvalidMarker,
    #[error(transparent)]
    Io(#[from] io::Error),
}

/// Marker indicating the start of a record batch.
pub const RECORD_BATCH_MARKER: [u8; 4] = [b'G', b'L', b'A', b'R'];

/// Number of bytes taken up for the header.
pub const HEADER_SIZE: usize = 8;

/// Read a batches length in bytes.
pub fn read_batch_len(buf: &[u8]) -> Result<usize, CodecError> {
    if buf.len() < HEADER_SIZE {
        return CodecError::InvalidBufferLen(buf.len());
    }
    if buf[0..4] != RECORD_BATCH_MARKER[..] {
        return CodecError::InvalidMarker;
    }

    // I wouldn't say defaulting to native endian is fantastic, but it follows
    // the semantics of `BufMut` when writing.
    let len = u32::from_ne_bytes(buf[4..8].try_into().unwrap());

    Ok(len as usize)
}

pub fn write_record_batch<B: BufMut>(mut buf: B, batch: &RecordBatch) -> Result<(), CodecError> {
    buf.put(&RECORD_BATCH_MARKER[..]);

    unimplemented!()
}

fn write_schema<B: BufMut>(mut buf: B, schema: &SchemaRef) -> Result<(), CodecError> {
    unimplemented!()
}

// fn array_data_as_bytes(data: &ArrayData) -> &[u8] {
//     unsafe { std::slice::from_raw_parts(data.as_ptr(), data.len()) }
// }

fn write_array_data<B: BufMut>(buf: &mut B, data: &ArrayData) -> Result<(), CodecError> {
    unimplemented!()
}

//! # Inference
//!
//! Steps:
//!
//! - Infer column delimiters, number of fields per record
//!
//! Can probably use the `Decoder` with differently configured csv readers that
//! repeatedly called on a small sample until we get a configuration that looks
//! reasonable (consistent number of fields across all records in the sample).
//!
//! - Infer types
//!
//! Try to parse into candidate types, starting at the second record in the
//! sample.
//!
//! - Header inferrence
//!
//! Determine if there's a header by trying to parse the first record into the
//! inferred types from the previous step. If it differs, assume a header.
use std::fmt;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures::stream::BoxStream;
use futures::StreamExt;
use rayexec_error::{RayexecError, Result, ResultExt};
use rayexec_execution::arrays::array::physical_type::{
    AddressableMut,
    MutableScalarStorage,
    PhysicalBool,
    PhysicalF64,
    PhysicalI64,
    PhysicalUtf8,
    ScalarStorage,
};
use rayexec_execution::arrays::array::Array;
use rayexec_execution::arrays::batch::Batch;
use rayexec_execution::arrays::compute::cast::parse::{
    BoolParser,
    Float64Parser,
    Int64Parser,
    Parser,
};
use rayexec_execution::arrays::datatype::{DataType, TimeUnit, TimestampTypeMeta};
use rayexec_execution::arrays::field::{Field, Schema};
use rayexec_execution::execution::operators::source::operation::{PollPull, Projections};
use rayexec_io::exp::AsyncReadStream;
use rayexec_io::FileSource;
use serde::{Deserialize, Serialize};

use crate::decoder::{ByteRecords, CsvDecoder, DecoderResult};
use crate::dialect::DialectOptions;
use crate::schema::CsvSchema;

#[derive(Debug)]
pub struct CsvReader {
    /// Schema we've inferred or otherwise been provided.
    schema: Schema,
    /// If we should skip the header record.
    ///
    /// Gets set to false after reading the first record.
    skip_header: bool,
    /// Reusable read buffer.
    read_buf: Vec<u8>,
    /// Buffered decoded records.
    output: ByteRecords,
    decoder: CsvDecoder,
    /// Source stream.
    stream: Pin<Box<dyn AsyncReadStream>>,
    projections: Projections,
    /// Current write state.
    write_state: WriteState,
}

#[derive(Debug, Clone, Copy)]
struct WriteState {
    /// If we need to pull from the stream for the next set of records to
    /// decode.
    needs_pull: bool,
    /// If the stream is exhausted.
    stream_exhausted: bool,
    /// Offset into the decoded records we should write to the batch.
    record_offset: usize,
    /// Current offset to the batch we're writing to.
    ///
    /// This lets us fill the batch completely before allowing it to be sent to
    /// the next operator.
    batch_write_offset: usize,
}

impl CsvReader {
    pub fn new(
        mut reader: impl FileSource,
        csv_schema: CsvSchema,
        dialect: DialectOptions,
    ) -> Self {
        unimplemented!()
        // let stream = AsyncCsvStream {
        //     schema: csv_schema.schema,
        //     skip_header: csv_schema.has_header,
        //     stream: reader.read_stream(),
        //     decoder_state: DecoderState::default(),
        //     decoder: CsvDecoder::new(dialect),
        //     buf: None,
        //     buf_offset: 0,
        //     decoding_finished: false,
        // };

        // AsyncCsvReader { stream }
    }

    pub async fn read_next(&mut self) -> Result<Option<Batch>> {
        unimplemented!()
    }

    /// Pulls the next batch by decoding the stream.
    pub fn poll_pull(&mut self, cx: &mut Context, output: &mut Batch) -> Result<PollPull> {
        let output_capacity = output.write_capacity()?;

        loop {
            if self.write_state.needs_pull {
                match self.stream.as_mut().poll_read(cx, &mut self.read_buf)? {
                    Poll::Ready(Some(count)) => {
                        // We got bytes, send to decoder.
                        let _ = self
                            .decoder
                            .decode(&self.read_buf[0..count], &mut self.output);
                        // Continue on. We'll be writing to the output batch.
                        self.write_state.needs_pull = false;
                    }
                    Poll::Ready(None) => {
                        // Stream is exhausted, writing any remaining records to
                        // the batch.
                        self.write_state.needs_pull = false;
                        self.write_state.stream_exhausted = true;
                        // Continue on...
                    }
                    Poll::Pending => return Ok(PollPull::Pending),
                }
            }

            let rem_cap = output_capacity - self.write_state.batch_write_offset;
            let rem_records = self.output.num_records() - self.write_state.record_offset;
            let count = usize::min(rem_cap, rem_records);

            self.write_batch(
                self.write_state.record_offset,
                output,
                self.write_state.batch_write_offset,
                count,
            )?;

            self.write_state.record_offset += count;
            self.write_state.batch_write_offset += count;

            if rem_records == count {
                // Need more records.
                self.write_state.needs_pull = true;
                self.write_state.record_offset = 0;

                // Unless the stream is exhausted, then we're done.
                if self.write_state.stream_exhausted {
                    let final_count = self.write_state.batch_write_offset + count;
                    output.set_num_rows(final_count)?;

                    // TODO: Verify that we don't have any partial records.

                    return Ok(PollPull::Exhausted);
                }
            }

            if rem_cap == count {
                // Emit batch, need a new one.
                self.write_state.batch_write_offset = 0;
                return Ok(PollPull::HasMore);
            }
        }
    }

    fn write_batch(
        &self,
        records_offset: usize,
        batch: &mut Batch,
        write_offset: usize,
        count: usize,
    ) -> Result<()> {
        for (out_idx, &col_idx) in self.projections.column_indices.iter().enumerate() {
            let array = &mut batch.arrays_mut()[out_idx];
            match array.datatype() {
                DataType::Boolean => self.write_primitive::<PhysicalBool, _>(
                    records_offset,
                    col_idx,
                    array,
                    write_offset,
                    count,
                    BoolParser,
                )?,
                DataType::Int64 => self.write_primitive::<PhysicalI64, _>(
                    records_offset,
                    col_idx,
                    array,
                    write_offset,
                    count,
                    Int64Parser::new(),
                )?,
                DataType::Float64 => self.write_primitive::<PhysicalF64, _>(
                    records_offset,
                    col_idx,
                    array,
                    write_offset,
                    count,
                    Float64Parser::new(),
                )?,
                DataType::Utf8 => {
                    self.write_string(records_offset, col_idx, array, write_offset, count)?
                }
                other => {
                    return Err(RayexecError::new("Unhandled datatype for csv scanning")
                        .with_field("datatype", other.clone()))
                }
            }
        }

        Ok(())
    }

    fn write_primitive<S, P>(
        &self,
        records_offset: usize,
        field_idx: usize,
        array: &mut Array,
        write_offset: usize,
        count: usize,
        mut parser: P,
    ) -> Result<()>
    where
        S: MutableScalarStorage,
        S::StorageType: Sized,
        P: Parser<Type = S::StorageType>,
    {
        let mut output = S::get_addressable_mut(array.data_mut())?;

        for idx in 0..count {
            let record_idx = idx + records_offset;
            let write_idx = idx + write_offset;

            // TODO: Allow indexing directly to field instead of having to go
            // through record.

            let record = self.output.get_record(record_idx);
            let field = record.field(field_idx);
            let field = std::str::from_utf8(field).context("failed to parse field as utf8")?;

            let v = parser
                .parse(field)
                .ok_or_else(|| RayexecError::new("Failed to parse '{field}'"))?;

            output.put(write_idx, &v);
        }

        Ok(())
    }

    fn write_string(
        &self,
        records_offset: usize,
        field_idx: usize,
        array: &mut Array,
        write_offset: usize,
        count: usize,
    ) -> Result<()> {
        let mut output = PhysicalUtf8::get_addressable_mut(array.data_mut())?;

        for idx in 0..count {
            let record_idx = idx + records_offset;
            let write_idx = idx + write_offset;

            // TODO: Allow indexing directly to field instead of having to go
            // through record.

            let record = self.output.get_record(record_idx);
            let field = record.field(field_idx);
            let field = std::str::from_utf8(field).context("failed to parse field as utf8")?;

            output.put(write_idx, field);
        }

        Ok(())
    }
}

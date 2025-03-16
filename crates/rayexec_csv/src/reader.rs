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
use std::task::{Context, Poll};

use rayexec_error::{RayexecError, Result, ResultExt};
use rayexec_execution::arrays::array::physical_type::{
    AddressableMut,
    MutableScalarStorage,
    PhysicalBool,
    PhysicalF64,
    PhysicalI64,
    PhysicalUtf8,
};
use rayexec_execution::arrays::array::Array;
use rayexec_execution::arrays::batch::Batch;
use rayexec_execution::arrays::compute::cast::parse::{
    BoolParser,
    Float64Parser,
    Int64Parser,
    Parser,
};
use rayexec_execution::arrays::datatype::DataType;
use rayexec_execution::execution::operators::PollPull;
use rayexec_execution::io::file::AsyncReadStream;
use rayexec_execution::storage::projections::Projections;

use crate::decoder::{ByteRecords, CsvDecoder};

#[derive(Debug)]
pub struct CsvReader {
    /// Reusable read buffer.
    read_buf: Vec<u8>,
    /// Buffered decoded records.
    output: ByteRecords,
    decoder: CsvDecoder,
    /// Source stream.
    stream: Box<dyn AsyncReadStream>,
    projections: Projections,
    /// Current write state.
    write_state: WriteState,
}

#[derive(Debug, Clone, Copy)]
struct WriteState {
    /// Offset into to the records buffer to continue reading from.
    read_record_offset: usize,
    /// Current offset to the batch we're writing to.
    batch_write_offset: usize,
}

impl CsvReader {
    pub fn new(
        stream: Box<dyn AsyncReadStream>,
        skip_header: bool,
        projections: Projections,
        read_buf: Vec<u8>,
        decoder: CsvDecoder,
        output: ByteRecords,
    ) -> Self {
        CsvReader {
            read_buf,
            output,
            decoder,
            stream,
            projections,
            write_state: WriteState {
                read_record_offset: if skip_header { 1 } else { 0 },
                batch_write_offset: 0,
            },
        }
    }

    /// Pulls the next batch by decoding the stream.
    pub fn poll_pull(&mut self, cx: &mut Context, output: &mut Batch) -> Result<PollPull> {
        let out_cap = output.write_capacity()?;

        loop {
            // Try to write before polling again.
            if self.output.num_records() >= self.write_state.read_record_offset {
                // Remaining capacity of the output batch.
                let rem_cap = out_cap - self.write_state.batch_write_offset;
                // Records yet to be read from the decode buffer.
                let rem_records = self.output.num_records() - self.write_state.read_record_offset;

                let count = usize::min(rem_cap, rem_records);

                // Write the records to the output batch.
                self.write_batch(
                    self.write_state.read_record_offset,
                    output,
                    self.write_state.batch_write_offset,
                    count,
                )?;

                self.write_state.read_record_offset += count;
                self.write_state.batch_write_offset += count;

                // Update batch num rows to reflect the records we just
                // wrote to it.
                output.set_num_rows(self.write_state.batch_write_offset)?;

                if count == rem_records {
                    // We've exhausted the completed records in the decode
                    // state, clear them out.
                    self.output.clear_completed();
                    self.write_state.read_record_offset = 0;
                }

                if count == rem_cap {
                    // We filled up the batch. Signal we need a new one.
                    self.write_state.batch_write_offset = 0;
                    return Ok(PollPull::HasMore);
                }
            }

            match self.stream.as_mut().poll_read(cx, &mut self.read_buf)? {
                Poll::Ready(Some(count)) => {
                    // We got bytes, send to decoder.
                    let _ = self
                        .decoder
                        .decode(&self.read_buf[0..count], &mut self.output);

                    // Continue...
                }
                Poll::Ready(None) => {
                    // Stream is exhausted, we would've written all records to
                    // the batch already.
                    return Ok(PollPull::Exhausted);
                }
                Poll::Pending => return Ok(PollPull::Pending),
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
        self.projections
            .for_each_column(batch, &mut |col_idx, array| {
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
                Ok(())
            })?;

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

#[cfg(test)]
mod tests {
    use rayexec_execution::buffer::buffer_manager::NopBufferManager;
    use rayexec_execution::generate_batch;
    use rayexec_execution::io::file::FileSource;
    use rayexec_execution::io::memory::MemoryFileSource;
    use rayexec_execution::testutil::arrays::assert_batches_eq;
    use rayexec_execution::util::task::noop_context;

    use super::*;
    use crate::dialect::DialectOptions;

    fn make_stream(bytes: impl AsRef<[u8]>) -> Box<dyn AsyncReadStream> {
        let mut source = MemoryFileSource::new(&NopBufferManager, bytes).unwrap();
        let stream = source.read();
        Box::new(stream)
    }

    #[test]
    fn default_dialect_no_skip_header_complete_read() {
        let input = r#"mario,9.5,8000
wario,10.0,950
yoshi,4.5,10000
"#;
        let stream = make_stream(input);
        let decoder = CsvDecoder::new(DialectOptions::default());
        let output = ByteRecords::with_buffer_capacity(16);
        let mut reader = CsvReader::new(
            stream,
            false,
            Projections::new([0, 1, 2]),
            vec![0; 256],
            decoder,
            output,
        );

        let mut batch =
            Batch::new([DataType::Utf8, DataType::Float64, DataType::Int64], 16).unwrap();
        let poll = reader.poll_pull(&mut noop_context(), &mut batch).unwrap();
        assert_eq!(PollPull::Exhausted, poll);

        let expected = generate_batch!(
            ["mario", "wario", "yoshi"],
            [9.5, 10.0, 4.5],
            [8000_i64, 950, 10000]
        );
        assert_batches_eq(&expected, &batch);
    }

    #[test]
    fn default_dialect_no_skip_header_complete_read_small_read_buffer() {
        // Same as above, but with a small read buffer requiring multiple polls
        // to the stream.
        let input = r#"mario,9.5,8000
wario,10.0,950
yoshi,4.5,10000
"#;
        let stream = make_stream(input);
        let decoder = CsvDecoder::new(DialectOptions::default());
        let output = ByteRecords::with_buffer_capacity(16);
        let mut reader = CsvReader::new(
            stream,
            false,
            Projections::new([0, 1, 2]),
            vec![0; 16],
            decoder,
            output,
        );

        let mut batch =
            Batch::new([DataType::Utf8, DataType::Float64, DataType::Int64], 16).unwrap();
        let poll = reader.poll_pull(&mut noop_context(), &mut batch).unwrap();
        assert_eq!(PollPull::Exhausted, poll);

        let expected = generate_batch!(
            ["mario", "wario", "yoshi"],
            [9.5, 10.0, 4.5],
            [8000_i64, 950, 10000]
        );
        assert_batches_eq(&expected, &batch);
    }

    #[test]
    fn default_dialect_no_skip_header_large_read_buf_small_output_batch() {
        // Batch can't hold all records, requiring multiple pulls.
        let input = r#"mario,9.5,8000
wario,10.0,950
yoshi,4.5,10000
"#;
        let stream = make_stream(input);
        let decoder = CsvDecoder::new(DialectOptions::default());
        let output = ByteRecords::with_buffer_capacity(16);
        let mut reader = CsvReader::new(
            stream,
            false,
            Projections::new([0, 1, 2]),
            vec![0; 256],
            decoder,
            output,
        );

        let mut batch =
            Batch::new([DataType::Utf8, DataType::Float64, DataType::Int64], 2).unwrap();
        let poll = reader.poll_pull(&mut noop_context(), &mut batch).unwrap();
        assert_eq!(PollPull::HasMore, poll);

        let expected = generate_batch!(["mario", "wario"], [9.5, 10.0], [8000_i64, 950]);
        assert_batches_eq(&expected, &batch);

        // Pull again.
        let poll = reader.poll_pull(&mut noop_context(), &mut batch).unwrap();
        assert_eq!(PollPull::Exhausted, poll);

        println!("{}", batch.debug_table());

        let expected = generate_batch!(["yoshi"], [4.5], [10000_i64]);
        assert_batches_eq(&expected, &batch);
    }

    #[test]
    fn default_dialect_no_skip_header_small_read_buf_small_output_batch() {
        // Same as above, but with small read buffer.
        let input = r#"mario,9.5,8000
wario,10.0,950
yoshi,4.5,10000
"#;
        let stream = make_stream(input);
        let decoder = CsvDecoder::new(DialectOptions::default());
        let output = ByteRecords::with_buffer_capacity(16);
        let mut reader = CsvReader::new(
            stream,
            false,
            Projections::new([0, 1, 2]),
            vec![0; 16],
            decoder,
            output,
        );

        let mut batch =
            Batch::new([DataType::Utf8, DataType::Float64, DataType::Int64], 2).unwrap();
        let poll = reader.poll_pull(&mut noop_context(), &mut batch).unwrap();
        assert_eq!(PollPull::HasMore, poll);

        let expected = generate_batch!(["mario", "wario"], [9.5, 10.0], [8000_i64, 950]);
        assert_batches_eq(&expected, &batch);

        // Pull again.
        let poll = reader.poll_pull(&mut noop_context(), &mut batch).unwrap();
        assert_eq!(PollPull::Exhausted, poll);

        let expected = generate_batch!(["yoshi"], [4.5], [10000_i64]);
        assert_batches_eq(&expected, &batch);
    }

    #[test]
    fn default_dialect_skip_header_complete_read() {
        let input = r#"string,float,int
mario,9.5,8000
wario,10.0,950
yoshi,4.5,10000
"#;
        let stream = make_stream(input);
        let decoder = CsvDecoder::new(DialectOptions::default());
        let output = ByteRecords::with_buffer_capacity(16);
        let mut reader = CsvReader::new(
            stream,
            true,
            Projections::new([0, 1, 2]),
            vec![0; 256],
            decoder,
            output,
        );

        let mut batch =
            Batch::new([DataType::Utf8, DataType::Float64, DataType::Int64], 16).unwrap();
        let poll = reader.poll_pull(&mut noop_context(), &mut batch).unwrap();
        assert_eq!(PollPull::Exhausted, poll);

        let expected = generate_batch!(
            ["mario", "wario", "yoshi"],
            [9.5, 10.0, 4.5],
            [8000_i64, 950, 10000]
        );
        assert_batches_eq(&expected, &batch);
    }
}

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

use glaredb_core::arrays::array::Array;
use glaredb_core::arrays::array::array_buffer::ArrayBuffer;
use glaredb_core::arrays::array::physical_type::{
    AddressableMut,
    MutableScalarStorage,
    PhysicalBool,
    PhysicalF64,
    PhysicalI64,
    PhysicalUtf8,
};
use glaredb_core::arrays::batch::Batch;
use glaredb_core::arrays::datatype::DataTypeId;
use glaredb_core::execution::operators::PollPull;
use glaredb_core::functions::cast::parse::{BoolParser, Float64Parser, Int64Parser, Parser};
use glaredb_core::runtime::filesystem::AnyFile;
use glaredb_core::runtime::filesystem::file_provider::MultiFileProvider;
use glaredb_core::storage::projections::{ProjectedColumn, Projections};
use glaredb_error::{DbError, Result, ResultExt};

use crate::decoder::{ByteRecords, CsvDecoder};

#[derive(Debug)]
pub struct CsvReader {
    /// Source file.
    ///
    /// This may be None if we need to reset the reader to read from a new file.
    file: Option<AnyFile>,
    /// If we should skip the header of the file.
    skip_header: bool,
    /// Reusable read buffer.
    read_buf: Vec<u8>,
    /// Buffered decoded records.
    records: ByteRecords,
    decoder: CsvDecoder,
    projections: Projections,
    /// Current state of the reader.
    state: ReaderState,
    /// Total number of rows we've emitted so far for this file.
    ///
    /// Used for row id.
    current_count: i64,
}

#[derive(Debug)]
enum ReaderState {
    /// We're reading from the file stream.
    Reading {
        /// If we should skip the first record when flipping to `Flushing`.
        skip_first: bool,
    },
    Flushing {
        /// Offset into the decoded record buffer we should read from.
        record_offset: usize,
        /// If this state change was from the stream being exhausted.
        stream_exhausted: bool,
    },
    /// We've exhausted the stream, no more records will be produced.
    Exhausted,
}

impl CsvReader {
    pub fn new(
        skip_header: bool,
        projections: Projections,
        read_buf: Vec<u8>,
        decoder: CsvDecoder,
        records: ByteRecords,
    ) -> Self {
        CsvReader {
            file: None,
            skip_header,
            read_buf,
            records,
            decoder,
            projections,
            state: ReaderState::Reading {
                skip_first: skip_header,
            },
            current_count: 0,
        }
    }

    /// Prepares the reader to begin reading from a new file.
    ///
    /// Resets internal state as needed. `poll_pull` can immediately be called.
    pub fn prepare(&mut self, file: AnyFile) {
        self.file = Some(file);
        self.records.clear_all(); // TODO: Would this ever have a partial record?
        self.state = ReaderState::Reading {
            skip_first: self.skip_header,
        };
        self.current_count = 0;
    }

    /// Pulls the next batch by decoding the stream.
    pub fn poll_pull(&mut self, cx: &mut Context, output: &mut Batch) -> Result<PollPull> {
        let out_cap = output.write_capacity()?;
        debug_assert_ne!(0, out_cap);

        let file = match self.file.as_mut() {
            Some(file) => file,
            None => {
                return Err(DbError::new(
                    "Attempted to pull from CSV reader without preparing a file",
                ));
            }
        };

        loop {
            match self.state {
                ReaderState::Reading { skip_first } => {
                    // Read from the file stream.
                    match file.call_poll_read(cx, &mut self.read_buf)? {
                        Poll::Ready(0) => {
                            // Exhausted the stream, flush out any decoded
                            // records.
                            self.state = ReaderState::Flushing {
                                record_offset: if skip_first { 1 } else { 0 },
                                stream_exhausted: true,
                            }
                            // Continue...
                        }
                        Poll::Ready(n) => {
                            // Push read bytes to decoder.
                            let _ = self.decoder.decode(&self.read_buf[0..n], &mut self.records);
                            if self.records.num_records() >= out_cap {
                                // We have enough records to read from.
                                self.state = ReaderState::Flushing {
                                    record_offset: if skip_first { 1 } else { 0 },
                                    stream_exhausted: false,
                                }
                            }

                            // Continue...
                        }
                        Poll::Pending => return Ok(PollPull::Pending),
                    }
                }
                ReaderState::Flushing {
                    record_offset,
                    stream_exhausted,
                } => {
                    if record_offset >= self.records.num_records() {
                        // We've processed all decoded records. Should
                        // we read more, or are we done?
                        if stream_exhausted {
                            // We're done,
                            self.state = ReaderState::Exhausted;
                        } else {
                            // We should read more.

                            // Clear the current set of decoded records.
                            self.records.clear_completed();
                            debug_assert_eq!(0, self.records.num_records());

                            self.state = ReaderState::Reading {
                                // Always false since we should have already
                                // skipped the header.
                                skip_first: false,
                            };
                        }
                        continue;
                    }

                    let remaining = self.records.num_records() - record_offset;
                    let write_count = usize::min(remaining, out_cap);

                    // Write the decoded records to the output batch.
                    self.write_batch(record_offset, output, 0, write_count)?;

                    // Update state for the next poll.
                    self.state = ReaderState::Flushing {
                        record_offset: record_offset + write_count,
                        stream_exhausted,
                    };

                    // Update current count for this file.
                    self.current_count += write_count as i64;

                    // Return what we have, come back for more.
                    output.set_num_rows(write_count)?;
                    return Ok(PollPull::HasMore);
                }
                ReaderState::Exhausted => {
                    // We're done.
                    output.set_num_rows(0)?;
                    return Ok(PollPull::Exhausted);
                }
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
            .for_each_column(batch, &mut |col_idx, array| match col_idx {
                ProjectedColumn::Data(col_idx) => {
                    match array.datatype().id() {
                        DataTypeId::Boolean => self.write_primitive::<PhysicalBool, _>(
                            records_offset,
                            col_idx,
                            array,
                            write_offset,
                            count,
                            BoolParser,
                        )?,
                        DataTypeId::Int64 => self.write_primitive::<PhysicalI64, _>(
                            records_offset,
                            col_idx,
                            array,
                            write_offset,
                            count,
                            Int64Parser::new(),
                        )?,
                        DataTypeId::Float64 => self.write_primitive::<PhysicalF64, _>(
                            records_offset,
                            col_idx,
                            array,
                            write_offset,
                            count,
                            Float64Parser::new(),
                        )?,
                        DataTypeId::Utf8 => {
                            self.write_string(records_offset, col_idx, array, write_offset, count)?
                        }
                        other => {
                            return Err(DbError::new("Unhandled datatype for csv scanning")
                                .with_field("datatype", other));
                        }
                    }
                    Ok(())
                }
                ProjectedColumn::Metadata(MultiFileProvider::META_PROJECTION_FILENAME) => {
                    let file = self
                        .file
                        .as_ref()
                        .expect("file to be Some when writing projections");

                    let data = PhysicalUtf8::buffer_downcast_mut(array.data_mut())?;
                    let indices = write_offset..(write_offset + count);
                    data.put_duplicated(file.call_path().as_bytes(), indices)?;

                    Ok(())
                }
                ProjectedColumn::Metadata(MultiFileProvider::META_PROJECTION_ROWID) => {
                    let data = PhysicalI64::buffer_downcast_mut(array.data_mut())?;
                    let row_ids = &mut data.as_slice_mut()[write_offset..(write_offset + count)];
                    for (idx, row_id) in row_ids.iter_mut().enumerate() {
                        *row_id = self.current_count + idx as i64;
                    }

                    Ok(())
                }
                other => panic!("invalid projection: {other:?}"),
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
        let (data, validity) = array.data_and_validity_mut();
        let mut output = S::get_addressable_mut(data)?;

        for idx in 0..count {
            let record_idx = idx + records_offset;
            let write_idx = idx + write_offset;

            // TODO: Allow indexing directly to field instead of having to go
            // through record.

            let record = self.records.get_record(record_idx);
            let field = record.field(field_idx);
            let field = std::str::from_utf8(field).context("failed to parse field as utf8")?;

            if field.is_empty() {
                // Empty fields are NULL
                validity.set_invalid(write_idx);
            } else {
                let v = parser.parse(field).ok_or_else(|| {
                    DbError::new(format!("Failed to parse '{field}' as {}", S::PHYSICAL_TYPE)) // TODO: Type id
                })?;
                output.put(write_idx, &v);
            }
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
        let (data, validity) = array.data_and_validity_mut();
        let mut output = PhysicalUtf8::get_addressable_mut(data)?;

        for idx in 0..count {
            let record_idx = idx + records_offset;
            let write_idx = idx + write_offset;

            // TODO: Allow indexing directly to field instead of having to go
            // through record.

            let record = self.records.get_record(record_idx);
            let field = record.field(field_idx);
            let field = std::str::from_utf8(field).context("failed to parse field as utf8")?;

            if field.is_empty() {
                // Empty fields are NULL
                validity.set_invalid(write_idx);
            } else {
                output.put(write_idx, field);
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use glaredb_core::arrays::datatype::DataType;
    use glaredb_core::buffer::buffer_manager::DefaultBufferManager;
    use glaredb_core::generate_batch;
    use glaredb_core::runtime::filesystem::memory::MemoryFileHandle;
    use glaredb_core::testutil::arrays::assert_batches_eq;
    use glaredb_core::util::task::noop_context;

    use super::*;
    use crate::dialect::DialectOptions;

    fn make_file(bytes: impl AsRef<[u8]>) -> AnyFile {
        let file = MemoryFileHandle::from_bytes(&DefaultBufferManager, bytes).unwrap();
        AnyFile::from_file(file)
    }

    #[test]
    fn default_dialect_no_skip_header_complete_read() {
        let input = r#"mario,9.5,8000
wario,10.0,950
yoshi,4.5,10000
"#;
        let file = make_file(input);
        let decoder = CsvDecoder::new(DialectOptions::default());
        let output = ByteRecords::with_buffer_capacity(16);
        let mut reader = CsvReader::new(
            false,
            Projections::new([0, 1, 2]),
            vec![0; 256],
            decoder,
            output,
        );
        reader.prepare(file);

        let mut batch = Batch::new(
            [DataType::utf8(), DataType::float64(), DataType::int64()],
            16,
        )
        .unwrap();
        let poll = reader.poll_pull(&mut noop_context(), &mut batch).unwrap();
        assert_eq!(PollPull::HasMore, poll);

        let expected = generate_batch!(
            ["mario", "wario", "yoshi"],
            [9.5, 10.0, 4.5],
            [8000_i64, 950, 10000]
        );
        assert_batches_eq(&expected, &batch);

        let poll = reader.poll_pull(&mut noop_context(), &mut batch).unwrap();
        assert_eq!(PollPull::Exhausted, poll);
        assert_eq!(0, batch.num_rows());
    }

    #[test]
    fn default_dialect_no_skip_header_complete_read_small_read_buffer() {
        // Same as above, but with a small read buffer requiring multiple polls
        // to the stream.
        let input = r#"mario,9.5,8000
wario,10.0,950
yoshi,4.5,10000
"#;
        let file = make_file(input);
        let decoder = CsvDecoder::new(DialectOptions::default());
        let output = ByteRecords::with_buffer_capacity(16);
        let mut reader = CsvReader::new(
            false,
            Projections::new([0, 1, 2]),
            vec![0; 16],
            decoder,
            output,
        );
        reader.prepare(file);

        let mut batch = Batch::new(
            [DataType::utf8(), DataType::float64(), DataType::int64()],
            16,
        )
        .unwrap();
        let poll = reader.poll_pull(&mut noop_context(), &mut batch).unwrap();
        assert_eq!(PollPull::HasMore, poll);

        let expected = generate_batch!(
            ["mario", "wario", "yoshi"],
            [9.5, 10.0, 4.5],
            [8000_i64, 950, 10000]
        );
        assert_batches_eq(&expected, &batch);

        let poll = reader.poll_pull(&mut noop_context(), &mut batch).unwrap();
        assert_eq!(PollPull::Exhausted, poll);
        assert_eq!(0, batch.num_rows());
    }

    #[test]
    fn default_dialect_no_skip_header_large_read_buf_small_output_batch() {
        // Batch can't hold all records, requiring multiple pulls.
        let input = r#"mario,9.5,8000
wario,10.0,950
yoshi,4.5,10000
"#;
        let file = make_file(input);
        let decoder = CsvDecoder::new(DialectOptions::default());
        let output = ByteRecords::with_buffer_capacity(16);
        let mut reader = CsvReader::new(
            false,
            Projections::new([0, 1, 2]),
            vec![0; 256],
            decoder,
            output,
        );
        reader.prepare(file);

        let mut batch = Batch::new(
            [DataType::utf8(), DataType::float64(), DataType::int64()],
            2,
        )
        .unwrap();
        let poll = reader.poll_pull(&mut noop_context(), &mut batch).unwrap();
        assert_eq!(PollPull::HasMore, poll);

        let expected = generate_batch!(["mario", "wario"], [9.5, 10.0], [8000_i64, 950]);
        assert_batches_eq(&expected, &batch);

        // Pull again.
        let poll = reader.poll_pull(&mut noop_context(), &mut batch).unwrap();
        assert_eq!(PollPull::HasMore, poll);

        println!("{}", batch.debug_table());

        let expected = generate_batch!(["yoshi"], [4.5], [10000_i64]);
        assert_batches_eq(&expected, &batch);

        let poll = reader.poll_pull(&mut noop_context(), &mut batch).unwrap();
        assert_eq!(PollPull::Exhausted, poll);
        assert_eq!(0, batch.num_rows());
    }

    #[test]
    fn default_dialect_no_skip_header_small_read_buf_small_output_batch() {
        // Same as above, but with small read buffer.
        let input = r#"mario,9.5,8000
wario,10.0,950
yoshi,4.5,10000
"#;
        let file = make_file(input);
        let decoder = CsvDecoder::new(DialectOptions::default());
        let output = ByteRecords::with_buffer_capacity(16);
        let mut reader = CsvReader::new(
            false,
            Projections::new([0, 1, 2]),
            vec![0; 16],
            decoder,
            output,
        );
        reader.prepare(file);

        let mut batch = Batch::new(
            [DataType::utf8(), DataType::float64(), DataType::int64()],
            2,
        )
        .unwrap();
        let poll = reader.poll_pull(&mut noop_context(), &mut batch).unwrap();
        assert_eq!(PollPull::HasMore, poll);

        let expected = generate_batch!(["mario", "wario"], [9.5, 10.0], [8000_i64, 950]);
        assert_batches_eq(&expected, &batch);

        // Pull again.
        let poll = reader.poll_pull(&mut noop_context(), &mut batch).unwrap();
        assert_eq!(PollPull::HasMore, poll);

        let expected = generate_batch!(["yoshi"], [4.5], [10000_i64]);
        assert_batches_eq(&expected, &batch);

        let poll = reader.poll_pull(&mut noop_context(), &mut batch).unwrap();
        assert_eq!(PollPull::Exhausted, poll);
        assert_eq!(0, batch.num_rows());
    }

    #[test]
    fn default_dialect_skip_header_complete_read() {
        let input = r#"string,float,int
mario,9.5,8000
wario,10.0,950
yoshi,4.5,10000
"#;
        let file = make_file(input);
        let decoder = CsvDecoder::new(DialectOptions::default());
        let output = ByteRecords::with_buffer_capacity(16);
        let mut reader = CsvReader::new(
            true,
            Projections::new([0, 1, 2]),
            vec![0; 256],
            decoder,
            output,
        );
        reader.prepare(file);

        let mut batch = Batch::new(
            [DataType::utf8(), DataType::float64(), DataType::int64()],
            16,
        )
        .unwrap();
        let poll = reader.poll_pull(&mut noop_context(), &mut batch).unwrap();
        assert_eq!(PollPull::HasMore, poll);

        let expected = generate_batch!(
            ["mario", "wario", "yoshi"],
            [9.5, 10.0, 4.5],
            [8000_i64, 950, 10000]
        );
        assert_batches_eq(&expected, &batch);

        let poll = reader.poll_pull(&mut noop_context(), &mut batch).unwrap();
        assert_eq!(PollPull::Exhausted, poll);
        assert_eq!(0, batch.num_rows());
    }
}

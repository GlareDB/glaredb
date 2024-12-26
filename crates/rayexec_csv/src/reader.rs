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

use bytes::Bytes;
use futures::stream::BoxStream;
use futures::StreamExt;
use rayexec_error::{RayexecError, Result};
use rayexec_execution::arrays::array::{Array, ArrayData};
use rayexec_execution::arrays::batch::Batch;
use rayexec_execution::arrays::bitmap::Bitmap;
use rayexec_execution::arrays::compute::cast::parse::{
    BoolParser,
    Float64Parser,
    Int64Parser,
    Parser,
};
use rayexec_execution::arrays::datatype::{DataType, TimeUnit, TimestampTypeMeta};
use rayexec_execution::arrays::executor::builder::{ArrayDataBuffer, GermanVarlenBuffer};
use rayexec_execution::arrays::field::{Field, Schema};
use rayexec_execution::arrays::storage::{BooleanStorage, PrimitiveStorage};
use rayexec_io::FileSource;
use serde::{Deserialize, Serialize};

use crate::decoder::{CompletedRecords, CsvDecoder, DecoderResult, DecoderState};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DialectOptions {
    /// Delimiter character.
    pub delimiter: u8,

    /// Quote character.
    pub quote: u8,
}

impl Default for DialectOptions {
    fn default() -> Self {
        DialectOptions {
            delimiter: b',',
            quote: b'"',
        }
    }
}

impl DialectOptions {
    /// Try to infer which csv options to use based on some number of records
    /// from a csv source.
    pub fn infer_from_sample(sample_bytes: &[u8]) -> Result<Self> {
        // Best dialect chosen so far alongside number of fields decoded.
        let mut best: (Option<Self>, usize) = (None, 0);

        let mut state = DecoderState::default();

        for dialect in Self::dialects() {
            let mut decoder = CsvDecoder::new(*dialect);

            match decoder.decode(sample_bytes, &mut state) {
                Ok(DecoderResult::InputExhuasted) | Ok(DecoderResult::Finished) => {
                    let decoded_fields = state.num_fields().unwrap_or(0);
                    let completed_records = state.num_records();

                    // To be considered the best dialect:
                    //
                    // - Should decode at least 2 records.
                    // - Should read the entirety of the input (checked by match).
                    // - Should have decoded more number of fields than previous best.
                    if completed_records >= 2 && decoded_fields > best.1 {
                        best = (Some(*dialect), decoded_fields)
                    }

                    // Don't have enough info, try next dialect.
                }
                Ok(DecoderResult::BufferFull { .. }) => {
                    // Means we migh have attempted to read the entirety of the
                    // input as once field.
                }
                Err(_e) => {
                    // Assume all errors indicate inconsistent number of fields
                    // in record.
                    //
                    // Continue to next dialect.
                }
            }

            state.reset();
        }

        match best.0 {
            Some(best) => Ok(best),
            None => Err(RayexecError::new(
                "Unable to infer csv dialect from provided sample",
            )),
        }
    }

    pub fn csv_core_reader(&self) -> csv_core::Reader {
        csv_core::ReaderBuilder::new()
            .delimiter(self.delimiter)
            .quote(self.quote)
            .build()
    }

    /// Dialects used when attempting to infer options for a csv file.
    const fn dialects() -> &'static [Self] {
        &[
            DialectOptions {
                delimiter: b',',
                quote: b'"',
            },
            DialectOptions {
                delimiter: b'|',
                quote: b'"',
            },
            DialectOptions {
                delimiter: b';',
                quote: b'"',
            },
            DialectOptions {
                delimiter: b'\t',
                quote: b'"',
            },
            DialectOptions {
                delimiter: b',',
                quote: b'\'',
            },
            DialectOptions {
                delimiter: b'|',
                quote: b'\'',
            },
            DialectOptions {
                delimiter: b';',
                quote: b'\'',
            },
            DialectOptions {
                delimiter: b'\t',
                quote: b'\'',
            },
        ]
    }
}

/// Candidate types used when trying to infer the types for a file.
///
/// Variants are ordered from the narrowest to the widest type allowing for
/// comparisons.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum CandidateType {
    /// Boolean type, strictest.
    Boolean,
    /// Int64 candidate type.
    Int64,
    /// Float64 candidate type.
    Float64,
    /// Timestamp candidate type.
    Timestamp,
    /// Utf8 type, this should be able to encompass any field.
    Utf8,
}

impl CandidateType {
    const fn as_datatype(&self) -> DataType {
        match self {
            Self::Boolean => DataType::Boolean,
            Self::Int64 => DataType::Int64,
            Self::Float64 => DataType::Float64,
            Self::Timestamp => DataType::Timestamp(TimestampTypeMeta::new(TimeUnit::Microsecond)),
            Self::Utf8 => DataType::Utf8,
        }
    }

    /// Check if this candidate type is valid for some input.
    fn is_valid(&self, input: &str) -> bool {
        match self {
            Self::Boolean => BoolParser.parse(input).is_some(),
            Self::Int64 => Int64Parser::new().parse(input).is_some(),
            Self::Float64 => Float64Parser::new().parse(input).is_some(),
            Self::Timestamp => false, // TODO
            Self::Utf8 => true,
        }
    }

    /// Update this candidate type based on some string input.
    fn update_from_input(&mut self, input: &str) {
        match self {
            Self::Boolean => {
                if BoolParser.parse(input).is_none() {
                    *self = Self::Int64;
                    self.update_from_input(input)
                }
            }
            Self::Int64 => {
                if Int64Parser::new().parse(input).is_none() {
                    *self = Self::Float64;
                    self.update_from_input(input)
                }
            }
            Self::Float64 => {
                if Float64Parser::new().parse(input).is_none() {
                    *self = Self::Timestamp;
                    self.update_from_input(input)
                }
            }
            Self::Timestamp => {
                // TODO: Check
                *self = Self::Utf8;
            }
            Self::Utf8 => (), // Nothing to do, already the widest.
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CsvSchema {
    /// All fields in the the csv input.
    pub schema: Schema,

    /// Whether or not the input has a header line.
    pub has_header: bool,
}

impl CsvSchema {
    /// Create a new schema using gnerated names.
    pub fn new_with_generated_names(types: Vec<DataType>) -> Self {
        let schema = Schema::new(types.into_iter().enumerate().map(|(idx, typ)| Field {
            name: format!("column{idx}"),
            datatype: typ,
            nullable: true,
        }));

        CsvSchema {
            schema,
            has_header: false,
        }
    }

    /// Try to infer the schema for a csv input based on some number of input
    /// records.
    pub fn infer_from_records(records: CompletedRecords) -> Result<Self> {
        if records.num_completed() == 0 {
            return Err(RayexecError::new(
                "Unable to infer CSV schema with no records",
            ));
        }

        let num_fields = match records.num_fields() {
            Some(n) => n,
            None => return Err(RayexecError::new("Unknown number of fields")),
        };

        // Start with most restrictive.
        let mut candidates = vec![CandidateType::Boolean; num_fields];

        // Skip first record since it may be a header.
        for record in records.iter().skip(1) {
            for (candidate, field) in candidates.iter_mut().zip(record.iter()) {
                candidate.update_from_input(field?);
            }
        }

        // Now test the candidates against the possible header. If any of the
        // candidates fails, we assume the record is a header.
        let has_header = records
            .get_record(0)
            .ok_or_else(|| RayexecError::new("missing record 0"))?
            .iter()
            .zip(candidates.iter())
            .any(|(field, candidate)| !candidate.is_valid(field.unwrap_or_default()));

        let fields: Vec<_> = if has_header {
            // Use the names from the header.
            records
                .get_record(0)
                .expect("record 0 to exist")
                .iter()
                .zip(candidates.into_iter())
                .map(|(name, candidate)| {
                    Ok(Field {
                        name: name?.to_string(),
                        datatype: candidate.as_datatype(),
                        nullable: true,
                    })
                })
                .collect::<Result<Vec<_>>>()?
        } else {
            // Generate field names.
            candidates
                .into_iter()
                .enumerate()
                .map(|(idx, candidate)| Field {
                    name: format!("column{idx}"),
                    datatype: candidate.as_datatype(),
                    nullable: true,
                })
                .collect()
        };

        Ok(CsvSchema {
            schema: Schema::new(fields),
            has_header,
        })
    }
}

pub struct AsyncCsvReader {
    stream: AsyncCsvStream,
}

impl AsyncCsvReader {
    pub fn new(
        mut reader: impl FileSource,
        csv_schema: CsvSchema,
        dialect: DialectOptions,
    ) -> Self {
        let stream = AsyncCsvStream {
            schema: csv_schema.schema,
            skip_header: csv_schema.has_header,
            stream: reader.read_stream(),
            decoder_state: DecoderState::default(),
            decoder: CsvDecoder::new(dialect),
            buf: None,
            buf_offset: 0,
            decoding_finished: false,
        };

        AsyncCsvReader { stream }
    }

    pub async fn read_next(&mut self) -> Result<Option<Batch>> {
        self.stream.next_batch().await
    }
}

impl fmt::Debug for AsyncCsvReader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AsyncCsvReader").finish_non_exhaustive()
    }
}

struct AsyncCsvStream {
    /// Schema we've inferred or otherwise been provided.
    schema: Schema,

    /// If we should skip the header record.
    skip_header: bool,

    /// Inner stream for getting bytes.
    stream: BoxStream<'static, Result<Bytes>>,

    /// Decoder that we push bytes to.
    decoder: CsvDecoder,

    /// Decoder state updated on every push to the decoder.
    decoder_state: DecoderState,

    /// If we read from the stream and push to the decoder, but the decoder's
    /// buffer is full, we store the stream's buffer here so the next iteration
    /// can continue where it left off.
    buf: Option<Bytes>,

    /// Offset into the above buffer where we should resume reading.
    buf_offset: usize,

    /// If the decoder said we're finished.
    ///
    /// This is used to allow us to determine if we should push an empty buffer
    /// to the decoder as csv_core expects an empty buffer when reading is
    /// complete. One we've pushed that buffer, this gets set to true and we
    /// build the remaining batch.
    decoding_finished: bool,
}

impl AsyncCsvStream {
    async fn next_batch(&mut self) -> Result<Option<Batch>> {
        loop {
            let (buf, offset) = match self.buf.take() {
                Some(buf) => (buf, self.buf_offset),
                None => match self.stream.next().await {
                    Some(buf) => (buf?, 0),
                    None => {
                        if self.decoding_finished {
                            // We're done, we've already passed an empty buffer
                            // to the decoder.
                            return Ok(None);
                        }
                        // Provide an empty buffer. csv_core expects and empty
                        // buffer to signal end of reading.
                        (Bytes::new(), 0)
                    }
                },
            };

            match self
                .decoder
                .decode(&buf[offset..], &mut self.decoder_state)?
            {
                DecoderResult::Finished => {
                    self.decoding_finished = true;
                    // Continue on with using the decoded results.
                }
                DecoderResult::InputExhuasted => continue, // To next iteration of outer loop.
                DecoderResult::BufferFull { input_offset } => {
                    // Need to flush out buffer. Store for later use.
                    self.buf = Some(buf);
                    self.buf_offset = input_offset;
                }
            }

            let num_fields = match self.decoder_state.num_fields() {
                Some(num) => num,
                None => return Err(RayexecError::new("First record exceeded buffer size")),
            };

            if num_fields != self.schema.fields.len() {
                return Err(RayexecError::new(format!(
                    "Number of fields read does not match schema, got {}, expected {}",
                    num_fields,
                    self.schema.fields.len()
                )));
            }

            let completed = self.decoder_state.completed_records();
            if completed.num_completed() == 0 {
                return Err(RayexecError::new(
                    "CSV record too large, exceeds buffer size",
                ));
            }

            let batch = Self::build_batch(completed, &self.schema, self.skip_header)?;
            self.skip_header = false;

            self.decoder_state.clear_completed();

            return Ok(Some(batch));
        }
    }

    fn build_batch(
        completed: CompletedRecords,
        schema: &Schema,
        skip_header: bool,
    ) -> Result<Batch> {
        let skip_records = if skip_header { 1 } else { 0 };

        let mut arrs = Vec::with_capacity(schema.fields.len());
        for (idx, field) in schema.fields.iter().enumerate() {
            let arr = match &field.datatype {
                DataType::Boolean => Self::build_boolean(&completed, idx, skip_records)?,
                DataType::Int64 => Self::build_primitive(
                    &field.datatype,
                    &completed,
                    idx,
                    skip_records,
                    Int64Parser::new(),
                )?,
                DataType::Float64 => Self::build_primitive(
                    &field.datatype,
                    &completed,
                    idx,
                    skip_records,
                    Float64Parser::new(),
                )?,
                DataType::Utf8 => Self::build_utf8(&completed, idx, skip_records)?,
                other => return Err(RayexecError::new(format!("Unhandled data type: {other}"))),
            };

            arrs.push(arr);
        }

        Batch::try_new(arrs)
    }

    fn build_boolean(
        completed: &CompletedRecords,
        field_idx: usize,
        skip_records: usize,
    ) -> Result<Array> {
        let mut values = Bitmap::with_capacity(completed.num_completed());
        let mut validity = Bitmap::with_capacity(completed.num_completed());

        for record in completed.iter().skip(skip_records) {
            let field = record.get_field(field_idx)?;
            if field.is_empty() {
                values.push(false);
                validity.push(false);
            } else {
                values.push(BoolParser.parse(field).ok_or_else(|| {
                    RayexecError::new(format!("Failed to parse '{field}' into a boolean"))
                })?);
                validity.push(true);
            }
        }

        Ok(Array::new_with_validity_and_array_data(
            DataType::Boolean,
            validity,
            BooleanStorage::from(values),
        ))
    }

    fn build_primitive<T, P>(
        datatype: &DataType,
        completed: &CompletedRecords,
        field_idx: usize,
        skip_records: usize,
        mut parser: P,
    ) -> Result<Array>
    where
        T: Default,
        P: Parser<Type = T>,
        PrimitiveStorage<T>: Into<ArrayData>,
    {
        let mut values = Vec::with_capacity(completed.num_completed());
        let mut validity = Bitmap::with_capacity(completed.num_completed());

        for record in completed.iter().skip(skip_records) {
            let field = record.get_field(field_idx)?;
            if field.is_empty() {
                values.push(T::default());
                validity.push(false);
            } else {
                values.push(
                    parser
                        .parse(field)
                        .ok_or_else(|| RayexecError::new(format!("Failed to parse '{field}'")))?,
                );
                validity.push(true);
            }
        }

        Ok(Array::new_with_validity_and_array_data(
            datatype.clone(),
            validity,
            PrimitiveStorage::from(values),
        ))
    }

    fn build_utf8(
        completed: &CompletedRecords,
        field_idx: usize,
        skip_records: usize,
    ) -> Result<Array> {
        let mut values = GermanVarlenBuffer::with_len(completed.num_completed() - skip_records);
        let mut validity = Bitmap::with_capacity(completed.num_completed());

        for (idx, record) in completed.iter().skip(skip_records).enumerate() {
            let field = record.get_field(field_idx)?;
            if field.is_empty() {
                validity.push(false);
            } else {
                values.put(idx, field);
                validity.push(true);
            }
        }

        Ok(Array::new_with_validity_and_array_data(
            DataType::Utf8,
            validity,
            values.into_data(),
        ))
    }
}

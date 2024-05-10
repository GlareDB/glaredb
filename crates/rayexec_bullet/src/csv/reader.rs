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

use std::str::FromStr;

use super::decode::{DecodedRecords, Decoder};
use crate::{
    array::{Array, BooleanArray, PrimitiveArray, Utf8Array},
    bitmap::Bitmap,
    field::{DataType, Field, Schema},
};
use rayexec_error::{RayexecError, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

        for dialect in Self::dialects() {
            let reader = dialect.csv_core_reader();
            let mut decoder = Decoder::new(reader, None);

            match decoder.decode(sample_bytes) {
                Ok(r) => {
                    let decoded_fields = decoder.num_fields().unwrap_or(0);

                    // To be considered the best dialect:
                    //
                    // - Should decode at least 2 records.
                    // - Should read the entirety of the input.
                    // - Should have decoded more number of fields than previous best.
                    if r.completed >= 2
                        && r.input_offset == sample_bytes.len()
                        && decoded_fields > best.1
                    {
                        best = (Some(*dialect), decoded_fields)
                    }

                    // Don't have enough info, try next dialect.
                }
                Err(_e) => {
                    // Assume all errors indicate inconsistent number of fields
                    // in record.
                    //
                    // Continue to next dialect.
                }
            }
        }

        match best.0 {
            Some(best) => Ok(best),
            None => Err(RayexecError::new(
                "Unable to infer csv dialect from provided sample",
            )),
        }
    }

    /// Create a decoder for the dialect.
    pub fn decoder(&self) -> Decoder {
        Decoder::new(self.csv_core_reader(), None)
    }

    /// Create a csv core reader from these options.
    fn csv_core_reader(&self) -> csv_core::Reader {
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
// TODO: Date/time when we have them. Thos would fall between float and utf8.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum CandidateType {
    /// Boolean type, strictest.
    Boolean,
    /// Int64 candidate type.
    Int64,
    /// Float64 candidate type.
    Float64,
    /// Utf8 type, this should be able to encompass any field.
    Utf8,
}

impl CandidateType {
    const fn as_datatype(&self) -> DataType {
        match self {
            Self::Boolean => DataType::Boolean,
            Self::Int64 => DataType::Int64,
            Self::Float64 => DataType::Float64,
            Self::Utf8 => DataType::Utf8,
        }
    }

    /// Check if this candidate type is valid for some input.
    fn is_valid(&self, input: &str) -> bool {
        match self {
            Self::Boolean => input.parse::<bool>().is_ok(),
            Self::Int64 => input.parse::<i64>().is_ok(),
            Self::Float64 => input.parse::<f64>().is_ok(),
            Self::Utf8 => true,
        }
    }

    /// Update this candidate type based on some string input.
    fn update_from_input(&mut self, input: &str) {
        // TODO: What's the performance of parse vs regex?
        match self {
            Self::Boolean => {
                if input.parse::<bool>().is_err() {
                    if input.parse::<i64>().is_ok() {
                        *self = Self::Int64;
                        return;
                    }
                    if input.parse::<f64>().is_ok() {
                        *self = Self::Float64;
                        return;
                    }
                    *self = Self::Utf8;
                }
            }
            Self::Int64 => {
                if input.parse::<i64>().is_err() {
                    if input.parse::<f64>().is_ok() {
                        *self = Self::Float64;
                        return;
                    }
                    *self = Self::Utf8;
                }
            }
            Self::Float64 => {
                if input.parse::<f64>().is_err() {
                    *self = Self::Utf8;
                }
            }
            Self::Utf8 => (), // Nothing to do, already the widest.
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CsvSchema {
    /// All fields in the the csv input.
    pub fields: Vec<Field>,

    /// Whether or not the input has a header line.
    pub has_header: bool,
}

impl CsvSchema {
    /// Create a new schema using gnerated names.
    pub fn new_with_generated_names(types: Vec<DataType>) -> Self {
        let fields = types
            .into_iter()
            .enumerate()
            .map(|(idx, typ)| Field {
                name: format!("column{idx}"),
                datatype: typ,
                nullable: true,
            })
            .collect();

        CsvSchema {
            fields,
            has_header: false,
        }
    }

    pub fn into_schema(self) -> Schema {
        Schema {
            fields: self.fields,
        }
    }

    /// Try to infer the schema for a csv input based on some number of input
    /// records.
    pub fn infer_from_records(records: DecodedRecords) -> Result<Self> {
        if records.num_records() == 0 {
            return Err(RayexecError::new(
                "Unable to infer CSV schema with no records",
            ));
        }

        // Start with most restrictive.
        let mut candidates = vec![CandidateType::Boolean; records.num_fields()];

        // Skip first record since it may be a header.
        for record in records.iter().skip(1) {
            for (candidate, field) in candidates.iter_mut().zip(record.iter()) {
                candidate.update_from_input(field);
            }
        }

        // Now test the candidates against the possible header. If any of the
        // candidates fails, we assume the record is a header.
        let has_header = records
            .get_record(0)
            .iter()
            .zip(candidates.iter())
            .any(|(field, candidate)| !candidate.is_valid(field));

        let fields: Vec<_> = if has_header {
            // Use the names from the header.
            records
                .get_record(0)
                .iter()
                .zip(candidates.into_iter())
                .map(|(name, candidate)| Field {
                    name: name.to_string(),
                    datatype: candidate.as_datatype(),
                    nullable: true,
                })
                .collect()
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

        Ok(CsvSchema { fields, has_header })
    }
}

#[derive(Debug)]
pub struct TypedDecoder {
    projection: Vec<usize>,

    /// Types to convert fields to.
    types: Vec<DataType>,

    /// Underlying decoder.
    decoder: Decoder,

    /// Whether or not we need to skip the header.
    skip_header: bool,
}

impl TypedDecoder {
    pub fn new(dialect: DialectOptions, schema: &CsvSchema) -> Self {
        let types: Vec<_> = schema.fields.iter().map(|f| f.datatype.clone()).collect();
        // TODO: Creating a decoder here potentially throws away some work we've
        // already done.
        let decoder = Decoder::new(dialect.csv_core_reader(), Some(types.len()));

        TypedDecoder {
            projection: (0..types.len()).collect(),
            types,
            decoder,
            skip_header: schema.has_header,
        }
    }

    pub fn decode(&mut self, input: &[u8]) -> Result<usize> {
        let result = self.decoder.decode(input)?;
        // TODO: Double check me.
        assert_eq!(result.input_offset, input.len());
        Ok(result.completed)
    }

    /// Flush out records into arrays.
    ///
    /// This will skip the header if required.
    pub fn flush(&mut self) -> Result<Vec<Array>> {
        let skip = if self.skip_header {
            self.skip_header = false;
            1
        } else {
            0
        };

        self.flush_skip(skip)
    }

    /// Flush out all records into arrays.
    ///
    /// `skip_records` indicates how many records to skip at the beginning. The
    /// skipped records will not be parsed.
    pub fn flush_skip(&mut self, skip_records: usize) -> Result<Vec<Array>> {
        // TODO: This will error on partial records. We'll need to handle that
        // _somewhere_ but I'm not exactly sure where yet.
        let records = self.decoder.flush()?;

        let array = self
            .projection
            .iter()
            .map(|idx| {
                let typ = &self.types[*idx];
                Ok(match typ {
                    DataType::Boolean => {
                        let mut bits = Bitmap::default();
                        for record in records.iter().skip(skip_records) {
                            // TODO: Nulls
                            let field = record.get_field(*idx);
                            let b: bool = field.parse().map_err(|_e| {
                                RayexecError::new(format!("Failed to parse '{field}' into a bool"))
                            })?;
                            bits.push(b);
                        }

                        Array::Boolean(BooleanArray::new_with_values(bits))
                    }
                    DataType::Int8 => {
                        Array::Int8(Self::build_primitive(&records, *idx, skip_records)?)
                    }
                    DataType::Int16 => {
                        Array::Int16(Self::build_primitive(&records, *idx, skip_records)?)
                    }
                    DataType::Int32 => {
                        Array::Int32(Self::build_primitive(&records, *idx, skip_records)?)
                    }
                    DataType::Int64 => {
                        Array::Int64(Self::build_primitive(&records, *idx, skip_records)?)
                    }
                    DataType::UInt8 => {
                        Array::UInt8(Self::build_primitive(&records, *idx, skip_records)?)
                    }
                    DataType::UInt16 => {
                        Array::UInt16(Self::build_primitive(&records, *idx, skip_records)?)
                    }
                    DataType::UInt32 => {
                        Array::UInt32(Self::build_primitive(&records, *idx, skip_records)?)
                    }
                    DataType::UInt64 => {
                        Array::UInt64(Self::build_primitive(&records, *idx, skip_records)?)
                    }
                    DataType::Float32 => {
                        Array::Float32(Self::build_primitive(&records, *idx, skip_records)?)
                    }
                    DataType::Float64 => {
                        Array::Float64(Self::build_primitive(&records, *idx, skip_records)?)
                    }
                    DataType::Utf8 => {
                        // TODO: Nulls
                        let iter = records
                            .iter()
                            .skip(skip_records)
                            .map(|record| record.get_field(*idx));
                        Array::Utf8(Utf8Array::from_iter(iter))
                    }
                    DataType::LargeUtf8 => {
                        // TODO: Nulls
                        let iter = records
                            .iter()
                            .skip(skip_records)
                            .map(|record| record.get_field(*idx));
                        Array::Utf8(Utf8Array::from_iter(iter))
                    }
                    other => {
                        return Err(RayexecError::new(format!("Unhandled data type: {other:?}")))
                    }
                })
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(array)
    }

    fn build_primitive<T: FromStr>(
        records: &DecodedRecords,
        field: usize,
        skip: usize,
    ) -> Result<PrimitiveArray<T>> {
        let mut values = Vec::with_capacity(records.num_records());

        for record in records.iter().skip(skip) {
            // TODO: Nulls
            let field = record.get_field(field);
            let val: T = field
                .parse()
                .map_err(|_e| RayexecError::new(format!("failed to parse '{field}'")))?;
            values.push(val);
        }

        Ok(PrimitiveArray::from(values))
    }
}

#[cfg(test)]
mod tests {
    use crate::array::{Float64Array, Int64Array};

    use super::*;

    #[test]
    fn dialect_infer_ok() {
        struct TestCase {
            csv: String,
            expected: DialectOptions,
        }

        let test_cases = [
            // Simple
            TestCase {
                csv: [
                    "a,b,c\n", //
                    "d,f,g\n", //
                    "h,i,j\n",
                ]
                .join(""),
                expected: DialectOptions {
                    delimiter: b',',
                    quote: b'"',
                },
            },
            // Quotes (")
            TestCase {
                csv: [
                    "a,b,c\n",                //
                    "d,\"hello, world\",g\n", //
                    "h,i,j\n",
                ]
                .join(""),
                expected: DialectOptions {
                    delimiter: b',',
                    quote: b'"',
                },
            },
            // Alt delimiter
            TestCase {
                csv: [
                    "a|b|c\n", //
                    "d|f|g\n", //
                    "h|i|j\n",
                ]
                .join(""),
                expected: DialectOptions {
                    delimiter: b'|',
                    quote: b'"',
                },
            },
            // Quotes (') (note ambiguous)
            TestCase {
                csv: [
                    "a,b,c\n",             //
                    "d,'hello world',g\n", //
                    "h,i,j\n",
                ]
                .join(""),
                expected: DialectOptions {
                    delimiter: b',',
                    quote: b'"',
                },
            },
            // Quotes (')
            TestCase {
                csv: [
                    "a,b,c\n",              //
                    "d,'hello, world',g\n", //
                    "h,i,j\n",
                ]
                .join(""),
                expected: DialectOptions {
                    delimiter: b',',
                    quote: b'\'',
                },
            },
            // Partial record, last line cut off.
            TestCase {
                csv: "a,b,c\nd,e,f\ng,".to_string(),
                expected: DialectOptions {
                    delimiter: b',',
                    quote: b'\"',
                },
            },
        ];

        for tc in test_cases {
            let bs = tc.csv.as_bytes();
            let got = DialectOptions::infer_from_sample(bs).unwrap();
            assert_eq!(tc.expected, got);
        }
    }

    #[test]
    fn typed_decode_ok() {
        struct TestCase {
            csv: String,
            dialect: DialectOptions,
            schema: CsvSchema,
            expected: Vec<Array>,
        }

        let test_cases = [
            // Simple
            TestCase {
                csv: [
                    "a,1,5.0\n", //
                    "b,2,5.5\n", //
                    "c,3,6.0\n",
                ]
                .join(""),
                dialect: DialectOptions::default(),
                schema: CsvSchema::new_with_generated_names(vec![
                    DataType::Utf8,
                    DataType::Int64,
                    DataType::Float64,
                ]),
                expected: vec![
                    Array::Utf8(Utf8Array::from_iter(["a", "b", "c"])),
                    Array::Int64(Int64Array::from_iter([1, 2, 3])),
                    Array::Float64(Float64Array::from_iter([5.0, 5.5, 6.0])),
                ],
            },
            // Numbers in source, string as type
            TestCase {
                csv: [
                    "a,11\n",  //
                    "b,222\n", //
                    "c,3333\n",
                ]
                .join(""),
                dialect: DialectOptions::default(),
                schema: CsvSchema::new_with_generated_names(vec![DataType::Utf8, DataType::Utf8]),
                expected: vec![
                    Array::Utf8(Utf8Array::from_iter(["a", "b", "c"])),
                    Array::Utf8(Utf8Array::from_iter(["11", "222", "3333"])),
                ],
            },
        ];

        for tc in test_cases {
            let bs = tc.csv.as_bytes();
            let mut typed = TypedDecoder::new(tc.dialect, &tc.schema);

            typed.decode(bs).unwrap();
            let got = typed.flush_skip(0).unwrap();

            assert_eq!(tc.expected, got);
        }
    }

    #[test]
    fn typed_decode_skip_records() {
        struct TestCase {
            csv: String,
            skip: usize,
            dialect: DialectOptions,
            schema: CsvSchema,
            expected: Vec<Array>,
        }

        let test_cases = [
            // Simple
            TestCase {
                csv: [
                    "a,1,5.0\n", //
                    "b,2,5.5\n", //
                    "c,3,6.0\n",
                ]
                .join(""),
                skip: 1,
                dialect: DialectOptions::default(),
                schema: CsvSchema::new_with_generated_names(vec![
                    DataType::Utf8,
                    DataType::Int64,
                    DataType::Float64,
                ]),
                expected: vec![
                    Array::Utf8(Utf8Array::from_iter(["b", "c"])),
                    Array::Int64(Int64Array::from_iter([2, 3])),
                    Array::Float64(Float64Array::from_iter([5.5, 6.0])),
                ],
            },
            // Header (different types than values in the record)
            TestCase {
                csv: [
                    "column1, column2\n", //
                    "a,11\n",             //
                    "b,222\n",            //
                    "c,3333\n",
                ]
                .join(""),
                skip: 1,
                dialect: DialectOptions::default(),
                schema: CsvSchema::new_with_generated_names(vec![DataType::Utf8, DataType::Utf8]),
                expected: vec![
                    Array::Utf8(Utf8Array::from_iter(["a", "b", "c"])),
                    Array::Utf8(Utf8Array::from_iter(["11", "222", "3333"])),
                ],
            },
        ];

        for tc in test_cases {
            let bs = tc.csv.as_bytes();
            let mut typed = TypedDecoder::new(tc.dialect, &tc.schema);

            typed.decode(bs).unwrap();
            let got = typed.flush_skip(tc.skip).unwrap();

            assert_eq!(tc.expected, got);
        }
    }

    #[test]
    fn typed_decoder_skip_header() {
        // First set of lines, first line being the header.
        let csv_lines1 = [
            "col1,col2,col3\n", //
            "a,b,true\n",       //
            "d,e,false\n",
        ];

        let schema = CsvSchema {
            fields: vec![
                Field::new("col1", DataType::Utf8, true),
                Field::new("col2", DataType::Utf8, true),
                Field::new("col3", DataType::Boolean, true),
            ],
            has_header: true,
        };

        let mut typed = TypedDecoder::new(DialectOptions::default(), &schema);

        for line in csv_lines1 {
            let decoded = typed.decode(line.as_bytes()).unwrap();
            assert_eq!(1, decoded);
        }

        let expected1 = vec![
            Array::Utf8(Utf8Array::from_iter(["a", "d"])),
            Array::Utf8(Utf8Array::from_iter(["b", "e"])),
            Array::Boolean(BooleanArray::from_iter([true, false])),
        ];

        let got1 = typed.flush().unwrap();
        assert_eq!(expected1, got1);

        // Try decoding more lines.
        let csv_lines2 = [
            "f,g,false\n", //
            "h,i,true\n",
        ];

        for line in csv_lines2 {
            let decoded = typed.decode(line.as_bytes()).unwrap();
            assert_eq!(1, decoded);
        }

        let expected2 = vec![
            Array::Utf8(Utf8Array::from_iter(["f", "h"])),
            Array::Utf8(Utf8Array::from_iter(["g", "i"])),
            Array::Boolean(BooleanArray::from_iter([false, true])),
        ];

        let got2 = typed.flush().unwrap();
        assert_eq!(expected2, got2);
    }

    #[test]
    fn csv_schema_infer_ok() {
        struct TestCase {
            csv: String,
            dialect: DialectOptions,
            expected: CsvSchema,
        }

        let test_cases = [
            // No header, all strings,
            TestCase {
                csv: [
                    "a,b,c\n", //
                    "d,e,f\n",
                ]
                .join(""),
                dialect: DialectOptions::default(),
                expected: CsvSchema {
                    fields: vec![
                        Field::new("column0", DataType::Utf8, true),
                        Field::new("column1", DataType::Utf8, true),
                        Field::new("column2", DataType::Utf8, true),
                    ],
                    has_header: false,
                },
            },
            // No header, mixed types
            TestCase {
                csv: [
                    "true,1.1,c\n", //
                    "false,8.0,f\n",
                ]
                .join(""),
                dialect: DialectOptions::default(),
                expected: CsvSchema {
                    fields: vec![
                        Field::new("column0", DataType::Boolean, true),
                        Field::new("column1", DataType::Float64, true),
                        Field::new("column2", DataType::Utf8, true),
                    ],
                    has_header: false,
                },
            },
            // Header, mixed types
            TestCase {
                csv: [
                    "my_col_1,my_col_2,my_col_3\n",
                    "true,1.1,c\n", //
                    "false,8.0,f\n",
                ]
                .join(""),
                dialect: DialectOptions::default(),
                expected: CsvSchema {
                    fields: vec![
                        Field::new("my_col_1", DataType::Boolean, true),
                        Field::new("my_col_2", DataType::Float64, true),
                        Field::new("my_col_3", DataType::Utf8, true),
                    ],
                    has_header: true,
                },
            },
        ];

        for tc in test_cases {
            let reader = tc.dialect.csv_core_reader();
            let mut decoder = Decoder::new(reader, None);
            decoder.decode(tc.csv.as_bytes()).unwrap();
            let records = decoder.flush().unwrap();

            let got = CsvSchema::infer_from_records(records).unwrap();
            assert_eq!(tc.expected, got);
        }
    }
}

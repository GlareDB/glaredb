use glaredb_core::functions::cast::parse::{BoolParser, Float64Parser, Int64Parser, Parser};
use glaredb_core::arrays::datatype::{DataType, TimeUnit, TimestampTypeMeta};
use glaredb_core::arrays::field::{ColumnSchema, Field};
use glaredb_error::{DbError, Result, ResultExt};

use crate::decoder::ByteRecords;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CsvSchema {
    /// All fields in the the csv input.
    pub schema: ColumnSchema,
    /// Whether or not the input has a header line.
    pub has_header: bool,
}

impl CsvSchema {
    /// Create a new schema using gnerated names.
    pub fn new_with_generated_names(types: impl IntoIterator<Item = DataType>) -> Self {
        let schema = ColumnSchema::new(types.into_iter().enumerate().map(|(idx, typ)| Field {
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
    pub fn infer_from_records(records: &ByteRecords) -> Result<Self> {
        if records.num_records() == 0 {
            return Err(DbError::new("Unable to infer CSV schema with no records"));
        }

        let num_fields = records.get_record(0).num_fields();

        // Start with most restrictive.
        let mut candidates = vec![CandidateType::Boolean; num_fields];

        // Skip first record since it may be a header.
        for record in records.iter_records().skip(1) {
            for (candidate, field) in candidates.iter_mut().zip(record.iter_fields()) {
                let field = std::str::from_utf8(field).context("failed to read field as utf8")?;
                candidate.update_from_input(field);
            }
        }

        // Now test the candidates against the possible header. If any of the
        // candidates fails, we assume the record is a header.
        let has_header = records
            .get_record(0)
            .iter_fields()
            .zip(candidates.iter())
            .any(|(field, candidate)| {
                !candidate.is_valid(std::str::from_utf8(field).unwrap_or_default())
            });

        let fields: Vec<_> = if has_header {
            // Use the names from the header.
            records
                .get_record(0)
                .iter_fields()
                .zip(candidates.into_iter())
                .map(|(name, candidate)| {
                    let name =
                        std::str::from_utf8(name).context("failed to read header field as utf8")?;
                    Ok(Field {
                        name: name.to_string(),
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
            schema: ColumnSchema::new(fields),
            has_header,
        })
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decoder::CsvDecoder;
    use crate::dialect::DialectOptions;

    /// Helper for making byte records from some bytes.
    ///
    /// This uses the default csv dialect.
    fn make_records(bs: impl AsRef<[u8]>) -> ByteRecords {
        let mut decoder = CsvDecoder::new(DialectOptions::default());
        let mut records = ByteRecords::with_buffer_capacity(16);
        decoder.decode(bs.as_ref(), &mut records);
        records
    }

    #[test]
    fn infer_all_utf8() {
        let records = make_records(
            r#"a,b,c
d,e,f
x,y,z
"#,
        );

        let schema = CsvSchema::infer_from_records(&records).unwrap();
        assert!(!schema.has_header);

        let expected = ColumnSchema::new([
            Field::new("column0", DataType::Utf8, true),
            Field::new("column1", DataType::Utf8, true),
            Field::new("column2", DataType::Utf8, true),
        ]);
        assert_eq!(expected, schema.schema);
    }

    #[test]
    fn infer_utf8_float_int() {
        let records = make_records(
            r#"a,4.0,80
d,5,90
x,5.5,100
"#,
        );

        let schema = CsvSchema::infer_from_records(&records).unwrap();
        assert!(!schema.has_header);

        let expected = ColumnSchema::new([
            Field::new("column0", DataType::Utf8, true),
            Field::new("column1", DataType::Float64, true),
            Field::new("column2", DataType::Int64, true),
        ]);
        assert_eq!(expected, schema.schema);
    }

    #[test]
    fn infer_utf8_float_int_with_header() {
        let records = make_records(
            r#"c1,c2,c3
a,4.0,80
d,5,90
x,5.5,100
"#,
        );

        let schema = CsvSchema::infer_from_records(&records).unwrap();
        assert!(schema.has_header);

        let expected = ColumnSchema::new([
            Field::new("c1", DataType::Utf8, true),
            Field::new("c2", DataType::Float64, true),
            Field::new("c3", DataType::Int64, true),
        ]);
        assert_eq!(expected, schema.schema);
    }

    #[test]
    fn infer_float_special_cases() {
        let records = make_records(
            r#"c1,c2,c3
a,Inf,80
d,-Inf,90
x,NaN,100
"#,
        );

        let schema = CsvSchema::infer_from_records(&records).unwrap();
        assert!(schema.has_header);

        let expected = ColumnSchema::new([
            Field::new("c1", DataType::Utf8, true),
            Field::new("c2", DataType::Float64, true),
            Field::new("c3", DataType::Int64, true),
        ]);
        assert_eq!(expected, schema.schema);
    }
}

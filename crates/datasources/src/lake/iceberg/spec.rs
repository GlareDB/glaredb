use crate::lake::iceberg::errors::{IcebergError, Result};
use datafusion::arrow::datatypes::{DataType, TimeUnit};

#[derive(Debug, Clone, Copy)]
pub enum FormatVersion {
    V1,
    V2,
}

#[derive(Debug, Clone, Copy)]
pub enum FileFormat {
    Parquet,
    Orc,
    Avro,
}

// boolean	True or false
// int	32-bit signed integers	Can promote to long
// long	64-bit signed integers
// float	32-bit IEEE 754 floating point	Can promote to double
// double	64-bit IEEE 754 floating point
// decimal(P,S)	Fixed-point decimal; precision P, scale S	Scale is fixed [1], precision must be 38 or less
// date	Calendar date without timezone or time
// time	Time of day without date, timezone	Microsecond precision [2]
// timestamp	Timestamp without timezone	Microsecond precision [2]
// timestamptz	Timestamp with timezone	Stored as UTC [2]
// string	Arbitrary-length character sequences	Encoded with UTF-8 [3]
// uuid	Universally unique identifiers	Should use 16-byte fixed
// fixed(L)	Fixed-length byte array of length L
// binary

#[derive(Debug, Clone, Copy)]
pub enum PrimitiveType {
    Boolean,
    Int,
    Long,
    Float,
    Double,
    Decimal { p: u8, s: u8 },
    Date,
    Time,
    Timestamp,
    TimestampTz,
    String,
    Uuid,
    Fixed(usize),
    Binary,
}

impl TryFrom<PrimitiveType> for DataType {
    type Error = IcebergError;

    fn try_from(value: PrimitiveType) -> Result<Self> {
        Ok(match value {
            PrimitiveType::Boolean => DataType::Boolean,
            PrimitiveType::Int => DataType::Int32,
            PrimitiveType::Long => DataType::Int64,
            PrimitiveType::Float => DataType::Float32,
            PrimitiveType::Double => DataType::Float64,
            PrimitiveType::Decimal { p, s } => DataType::Decimal128(p, s as i8),
            PrimitiveType::Date => DataType::Date32,
            PrimitiveType::Time => unimplemented!(),
            PrimitiveType::Timestamp => DataType::Timestamp(TimeUnit::Microsecond, None),
            PrimitiveType::String => DataType::Utf8,
            _ => unimplemented!(),
        })
    }
}

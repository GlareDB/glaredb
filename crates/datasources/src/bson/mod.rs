pub mod builder;
pub mod errors;
pub mod schema;
pub mod stream;
pub mod table;

use datafusion::arrow::array::cast::as_string_array;
use datafusion::arrow::array::types::{
    Date32Type,
    Date64Type,
    Decimal128Type,
    DurationMicrosecondType,
    DurationMillisecondType,
    DurationNanosecondType,
    DurationSecondType,
    Float16Type,
    Float32Type,
    Float64Type,
    GenericBinaryType,
    Int16Type,
    Int32Type,
    Int64Type,
    Int8Type,
    IntervalDayTimeType,
    IntervalYearMonthType,
    Time32MillisecondType,
    Time32SecondType,
    Time64MicrosecondType,
    Time64NanosecondType,
    TimestampMicrosecondType,
    TimestampMillisecondType,
    TimestampSecondType,
    UInt16Type,
    UInt32Type,
    UInt64Type,
    UInt8Type,
};
use datafusion::arrow::array::{Array, AsArray, StructArray};
use datafusion::arrow::datatypes::{DataType, Fields, IntervalUnit, TimeUnit};
use datafusion::arrow::error::ArrowError;

pub struct BsonBatchConverter {
    batch: StructArray,
    schema: Vec<String>,
    row: usize,
    started: bool,
    columns: Vec<Vec<bson::Bson>>,
}

impl BsonBatchConverter {
    pub fn new(batch: StructArray, fields: Fields) -> Self {
        let mut field_names = Vec::with_capacity(fields.len());
        for field in &fields {
            field_names.push(field.name().to_owned())
        }

        Self {
            batch: batch.clone(),
            schema: field_names,
            row: 0,
            started: false,
            columns: Vec::with_capacity(batch.num_columns()),
        }
    }

    fn setup(&mut self) -> Result<(), ArrowError> {
        for col in self.batch.columns().iter() {
            self.columns
                .push(array_to_bson(col).map_err(|e| ArrowError::from_external_error(Box::new(e)))?)
        }
        self.started = true;
        Ok(())
    }
}

impl Iterator for BsonBatchConverter {
    type Item = Result<bson::Document, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.started {
            if let Err(e) = self.setup() {
                return Some(Err(e));
            }
        }

        if self.row >= self.batch.len() {
            return None;
        }

        let mut doc = bson::Document::new();
        for (i, field) in self.schema.iter().enumerate() {
            doc.insert(field.to_string(), self.columns[i][self.row].to_owned());
        }

        self.row += 1;
        Some(Ok(doc))
    }
}

pub fn array_to_bson(array: &dyn Array) -> Result<Vec<bson::Bson>, ArrowError> {
    let mut out = Vec::<bson::Bson>::with_capacity(array.len());
    let dt = array.data_type().to_owned();
    match dt {
        DataType::Int8 => array
            .as_primitive::<Int8Type>()
            .iter()
            .for_each(|val| out.push(bson::Bson::Int32(val.unwrap_or_default() as i32))),
        DataType::Int16 => array
            .as_primitive::<Int16Type>()
            .iter()
            .for_each(|val| out.push(bson::Bson::Int32(val.unwrap_or_default() as i32))),
        DataType::Int32 => array
            .as_primitive::<Int32Type>()
            .iter()
            .for_each(|val| out.push(bson::Bson::Int32(val.unwrap_or_default()))),
        DataType::Int64 => array
            .as_primitive::<Int64Type>()
            .iter()
            .for_each(|val| out.push(bson::Bson::Int64(val.unwrap_or_default()))),
        DataType::UInt8 => array
            .as_primitive::<UInt8Type>()
            .iter()
            .for_each(|val| out.push(bson::Bson::Int32(val.unwrap_or_default() as i32))),
        DataType::UInt16 => array
            .as_primitive::<UInt16Type>()
            .iter()
            .for_each(|val| out.push(bson::Bson::Int32(val.unwrap_or_default() as i32))),
        DataType::UInt32 => array
            .as_primitive::<UInt32Type>()
            .iter()
            .for_each(|val| out.push(bson::Bson::Int64(val.unwrap_or_default() as i64))),
        DataType::UInt64 => array
            .as_primitive::<UInt64Type>()
            .iter()
            .for_each(|val| out.push(bson::Bson::Int64(val.unwrap_or_default() as i64))),
        DataType::Utf8 | DataType::LargeUtf8 => {
            as_string_array(array).iter().for_each(|val| match val {
                Some(v) => out.push(bson::Bson::String(v.to_string())),
                None => out.push(bson::Bson::Null),
            })
        }
        DataType::Float16 => array
            .as_primitive::<Float16Type>()
            .iter()
            .for_each(|val| out.push(bson::Bson::Double(val.unwrap_or_default().to_f64()))),
        DataType::Float32 => array
            .as_primitive::<Float32Type>()
            .iter()
            .for_each(|val| out.push(bson::Bson::Double(val.unwrap_or_default() as f64))),
        DataType::Float64 => array
            .as_primitive::<Float64Type>()
            .iter()
            .for_each(|val| out.push(bson::Bson::Double(val.unwrap_or_default()))),
        DataType::Decimal128(_, _) => array
            .as_primitive::<Decimal128Type>()
            .iter()
            // TODO: this is probably not correct:
            .for_each(|val| {
                out.push(bson::ser::to_bson(&val.unwrap_or_default()).expect("decimal128"))
            }),
        DataType::Null => {
            for _ in 0..array.len() {
                out.push(bson::Bson::Null)
            }
        }
        DataType::Boolean => array
            .as_boolean()
            .iter()
            .for_each(|val| out.push(bson::Bson::Boolean(val.unwrap_or_default()))),
        DataType::FixedSizeBinary(_) => array.as_fixed_size_binary().iter().for_each(|val| {
            out.push(bson::Bson::Binary(bson::Binary {
                subtype: bson::spec::BinarySubtype::Generic,
                bytes: val.unwrap_or_default().to_vec(),
            }))
        }),
        DataType::Binary => array
            .as_bytes::<GenericBinaryType<i32>>()
            .iter()
            .for_each(|val| {
                out.push(bson::Bson::Binary(bson::Binary {
                    subtype: bson::spec::BinarySubtype::Generic,
                    bytes: val.unwrap_or_default().to_vec(),
                }))
            }),
        DataType::LargeBinary => {
            array
                .as_bytes::<GenericBinaryType<i64>>()
                .iter()
                .for_each(|val| {
                    out.push(bson::Bson::Binary(bson::Binary {
                        subtype: bson::spec::BinarySubtype::Generic,
                        bytes: val.unwrap_or_default().to_vec(),
                    }))
                })
        }
        DataType::Date32 => array
            .as_primitive::<Date32Type>()
            .iter()
            .for_each(|val| out.push(bson::Bson::Int32(val.unwrap_or_default()))),
        DataType::Date64 => array
            .as_primitive::<Date64Type>()
            .iter()
            .for_each(|val| out.push(bson::Bson::Int64(val.unwrap_or_default()))),
        DataType::Interval(IntervalUnit::DayTime) => array
            .as_primitive::<IntervalDayTimeType>()
            .iter()
            .for_each(|val| out.push(bson::Bson::Int64(val.unwrap_or_default()))),
        DataType::Interval(IntervalUnit::YearMonth) => array
            .as_primitive::<IntervalYearMonthType>()
            .iter()
            .for_each(|val| out.push(bson::Bson::Int32(val.unwrap_or_default()))),
        DataType::Interval(IntervalUnit::MonthDayNano) => {
            return Err(ArrowError::CastError(
                "calendar type is not representable in BSON".to_string(),
            ))
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => array
            .as_primitive::<TimestampMillisecondType>()
            .iter()
            .for_each(|val| {
                out.push(bson::Bson::DateTime(bson::datetime::DateTime::from_millis(
                    val.unwrap_or_default(),
                )))
            }),
        DataType::Timestamp(TimeUnit::Second, _) => array
            .as_primitive::<TimestampSecondType>()
            .iter()
            .for_each(|val| {
                out.push(bson::Bson::DateTime(bson::datetime::DateTime::from_millis(
                    val.unwrap_or_default() * 1000,
                )))
            }),
        DataType::Timestamp(TimeUnit::Microsecond, _) => array
            .as_primitive::<TimestampMicrosecondType>()
            .iter()
            .for_each(|val| {
                out.push(bson::Bson::DateTime(bson::datetime::DateTime::from_millis(
                    val.unwrap_or_default() / 100,
                )))
            }),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => array
            .as_primitive::<TimestampMicrosecondType>()
            .iter()
            .for_each(|val| {
                out.push(bson::Bson::DateTime(bson::datetime::DateTime::from_millis(
                    val.unwrap_or_default() / 100_000,
                )))
            }),
        DataType::Time32(TimeUnit::Second) => array
            .as_primitive::<Time32SecondType>()
            .iter()
            .for_each(|val| out.push(bson::Bson::Int32(val.unwrap_or_default()))),
        DataType::Time32(TimeUnit::Millisecond) => array
            .as_primitive::<Time32MillisecondType>()
            .iter()
            .for_each(|val| out.push(bson::Bson::Int32(val.unwrap_or_default()))),
        DataType::Time32(TimeUnit::Nanosecond)
        | DataType::Time32(TimeUnit::Microsecond)
        | DataType::Time64(TimeUnit::Second)
        | DataType::Time64(TimeUnit::Millisecond) => {
            return Err(ArrowError::CastError(
                "unreasonable time value conversion BSON".to_string(),
            ))
        }
        DataType::Time64(TimeUnit::Microsecond) => array
            .as_primitive::<Time64MicrosecondType>()
            .iter()
            .for_each(|val| out.push(bson::Bson::Int64(val.unwrap_or_default()))),
        DataType::Time64(TimeUnit::Nanosecond) => array
            .as_primitive::<Time64NanosecondType>()
            .iter()
            .for_each(|val| out.push(bson::Bson::Int64(val.unwrap_or_default()))),
        DataType::Duration(TimeUnit::Second) => array
            .as_primitive::<DurationSecondType>()
            .iter()
            .for_each(|val| out.push(bson::Bson::Int64(val.unwrap_or_default()))),
        DataType::Duration(TimeUnit::Millisecond) => array
            .as_primitive::<DurationMillisecondType>()
            .iter()
            .for_each(|val| out.push(bson::Bson::Int64(val.unwrap_or_default()))),
        DataType::Duration(TimeUnit::Microsecond) => array
            .as_primitive::<DurationMicrosecondType>()
            .iter()
            .for_each(|val| out.push(bson::Bson::Int64(val.unwrap_or_default()))),
        DataType::Duration(TimeUnit::Nanosecond) => array
            .as_primitive::<DurationNanosecondType>()
            .iter()
            .for_each(|val| out.push(bson::Bson::Int64(val.unwrap_or_default()))),
        DataType::List(_) | DataType::FixedSizeList(_, _) | DataType::LargeList(_) => {
            out.push(bson::Bson::Array(array_to_bson(array)?))
        }
        DataType::Struct(fields) => {
            let converter = BsonBatchConverter::new(array.as_struct().to_owned(), fields);

            for doc in converter {
                out.push(bson::Bson::Document(doc?))
            }
        }
        DataType::Map(_, _) => {
            let struct_array = array.as_map().entries();
            let converter =
                BsonBatchConverter::new(struct_array.to_owned(), struct_array.fields().to_owned());

            for doc in converter {
                out.push(bson::Bson::Document(doc?))
            }
        }
        DataType::Dictionary(_, _) => out.push(bson::Bson::Array(array_to_bson(
            array.as_any_dictionary().values(),
        )?)),
        DataType::Decimal256(_, _) | DataType::RunEndEncoded(_, _) | DataType::Union(_, _) => {
            return Err(ArrowError::CastError(
                "type is not representable in BSON".to_string(),
            ))
        }
    };
    Ok(out)
}

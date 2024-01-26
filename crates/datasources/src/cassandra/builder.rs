use chrono::{DateTime, NaiveTime, Utc};
use datafusion::arrow::array::{
    ArrayBuilder,
    Date64Builder,
    DurationNanosecondBuilder,
    Int64Builder,
    ListBuilder,
};

use super::{
    Any,
    Arc,
    ArrayRef,
    CqlValue,
    DataType,
    Float32Builder,
    Float64Builder,
    Int16Builder,
    Int32Builder,
    Int8Builder,
    StringBuilder,
    TimeUnit,
    TimestampMillisecondBuilder,
};
#[derive(Debug)]
pub(super) enum CqlValueArrayBuilder {
    Date64(Date64Builder),
    Duration(DurationNanosecondBuilder),
    Float32(Float32Builder),
    Float64(Float64Builder),
    Int8(Int8Builder),
    Int16(Int16Builder),
    Int32(Int32Builder),
    Int64(Int64Builder),
    Timestamp(TimestampMillisecondBuilder),
    Utf8(StringBuilder),
    List(Box<ListBuilder<Self>>),
}

impl ArrayBuilder for CqlValueArrayBuilder {
    fn len(&self) -> usize {
        match self {
            CqlValueArrayBuilder::Date64(b) => b.len(),
            CqlValueArrayBuilder::Duration(b) => b.len(),
            CqlValueArrayBuilder::Float32(b) => b.len(),
            CqlValueArrayBuilder::Float64(b) => b.len(),
            CqlValueArrayBuilder::Int8(b) => b.len(),
            CqlValueArrayBuilder::Int16(b) => b.len(),
            CqlValueArrayBuilder::Int32(b) => b.len(),
            CqlValueArrayBuilder::Int64(b) => b.len(),
            CqlValueArrayBuilder::Timestamp(b) => b.len(),
            CqlValueArrayBuilder::Utf8(b) => b.len(),
            CqlValueArrayBuilder::List(b) => b.len(),
        }
    }

    fn finish(&mut self) -> ArrayRef {
        match self {
            CqlValueArrayBuilder::Date64(b) => Arc::new(b.finish()),
            CqlValueArrayBuilder::Duration(b) => Arc::new(b.finish()),
            CqlValueArrayBuilder::Float32(b) => Arc::new(b.finish()),
            CqlValueArrayBuilder::Float64(b) => Arc::new(b.finish()),
            CqlValueArrayBuilder::Int8(b) => Arc::new(b.finish()),
            CqlValueArrayBuilder::Int16(b) => Arc::new(b.finish()),
            CqlValueArrayBuilder::Int32(b) => Arc::new(b.finish()),
            CqlValueArrayBuilder::Int64(b) => Arc::new(b.finish()),
            CqlValueArrayBuilder::Timestamp(b) => Arc::new(b.finish()),
            CqlValueArrayBuilder::Utf8(b) => Arc::new(b.finish()),
            CqlValueArrayBuilder::List(b) => Arc::new(b.finish()),
        }
    }

    fn finish_cloned(&self) -> ArrayRef {
        match self {
            CqlValueArrayBuilder::Date64(b) => Arc::new(b.finish_cloned()),
            CqlValueArrayBuilder::Duration(b) => Arc::new(b.finish_cloned()),
            CqlValueArrayBuilder::Float32(b) => Arc::new(b.finish_cloned()),
            CqlValueArrayBuilder::Float64(b) => Arc::new(b.finish_cloned()),
            CqlValueArrayBuilder::Int8(b) => Arc::new(b.finish_cloned()),
            CqlValueArrayBuilder::Int16(b) => Arc::new(b.finish_cloned()),
            CqlValueArrayBuilder::Int32(b) => Arc::new(b.finish_cloned()),
            CqlValueArrayBuilder::Int64(b) => Arc::new(b.finish_cloned()),
            CqlValueArrayBuilder::Timestamp(b) => Arc::new(b.finish_cloned()),
            CqlValueArrayBuilder::Utf8(b) => Arc::new(b.finish_cloned()),
            CqlValueArrayBuilder::List(b) => Arc::new(b.finish_cloned()),
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn into_box_any(self: Box<Self>) -> Box<dyn Any> {
        Box::new(self)
    }
}

impl CqlValueArrayBuilder {
    pub(super) fn new(dtype: &DataType) -> Self {
        match dtype {
            DataType::Utf8 => CqlValueArrayBuilder::Utf8(StringBuilder::new()),
            DataType::Date64 => CqlValueArrayBuilder::Date64(Date64Builder::new()),
            DataType::Float32 => CqlValueArrayBuilder::Float32(Float32Builder::new()),
            DataType::Float64 => CqlValueArrayBuilder::Float64(Float64Builder::new()),
            DataType::Duration(TimeUnit::Nanosecond) => {
                CqlValueArrayBuilder::Duration(DurationNanosecondBuilder::new())
            }
            DataType::Timestamp(TimeUnit::Millisecond, None) => {
                CqlValueArrayBuilder::Timestamp(TimestampMillisecondBuilder::new())
            }
            DataType::Int8 => CqlValueArrayBuilder::Int8(Int8Builder::new()),
            DataType::Int16 => CqlValueArrayBuilder::Int16(Int16Builder::new()),
            DataType::Int32 => CqlValueArrayBuilder::Int32(Int32Builder::new()),
            DataType::Int64 => CqlValueArrayBuilder::Int64(Int64Builder::new()),
            DataType::List(fld) => {
                let inner = Self::new(fld.data_type());
                let builder = ListBuilder::new(inner);
                let builder = Box::new(builder);
                CqlValueArrayBuilder::List(builder)
            }
            _ => unreachable!(
                "the casting in `try_convert_dtype` makes the above an exhaustive match"
            ),
        }
    }

    pub(super) fn append_option(&mut self, value: Option<CqlValue>) {
        match value {
            None => self.append_null(),
            Some(value) => self.append_value(value),
        }
    }
    pub(super) fn append_null(&mut self) {
        match self {
            CqlValueArrayBuilder::Utf8(b) => b.append_null(),
            CqlValueArrayBuilder::Date64(b) => b.append_null(),
            CqlValueArrayBuilder::Float64(b) => b.append_null(),
            CqlValueArrayBuilder::Duration(b) => b.append_null(),
            CqlValueArrayBuilder::Float32(b) => b.append_null(),
            CqlValueArrayBuilder::Int32(b) => b.append_null(),
            CqlValueArrayBuilder::Timestamp(b) => b.append_null(),
            CqlValueArrayBuilder::Int16(b) => b.append_null(),
            CqlValueArrayBuilder::Int8(b) => b.append_null(),
            CqlValueArrayBuilder::Int64(b) => b.append_null(),
            CqlValueArrayBuilder::List(b) => b.append_null(),
        }
    }

    fn append_value(&mut self, value: CqlValue) {
        use CqlValueArrayBuilder::*;
        match (self, value) {
            (Utf8(builder), CqlValue::Ascii(value)) => builder.append_value(value),
            (Utf8(builder), CqlValue::Text(value)) => builder.append_value(value),
            (Utf8(builder), CqlValue::Uuid(value)) => builder.append_value(value.to_string()),
            (Float64(builder), CqlValue::Double(value)) => builder.append_value(value),

            (Duration(builder), CqlValue::Duration(value)) => {
                if value.months != 0 {
                    unimplemented!("months are yet not supported")
                }
                const NANOS_PER_SECOND: i64 = 1_000_000_000;
                const SECONDS_PER_DAY: i64 = 86_400; // 24 * 60 * 60
                let nanos = value.nanoseconds;
                let nano_days = value.days as i64 * NANOS_PER_SECOND * SECONDS_PER_DAY;
                let nanos_full = nano_days + nanos;
                builder.append_value(nanos_full)
            }
            (Float32(builder), CqlValue::Float(value)) => builder.append_value(value),
            (Int32(builder), CqlValue::Int(value)) => builder.append_value(value),
            (Timestamp(builder), CqlValue::Timestamp(value)) => builder.append_value(value.0),
            (Int16(builder), CqlValue::SmallInt(value)) => builder.append_value(value),
            (Int8(builder), CqlValue::TinyInt(value)) => builder.append_value(value),
            (Int64(builder), CqlValue::BigInt(value)) => builder.append_value(value),
            (Date64(builder), CqlValue::Date(value)) => {
                let days_since_unix_epoch = value.0 as i64 - (1 << 31);
                let duration_since_unix_epoch = chrono::Duration::days(days_since_unix_epoch);
                let unix_epoch = chrono::NaiveDate::from_yo_opt(1970, 1).unwrap();
                let date = unix_epoch + duration_since_unix_epoch;
                let time = NaiveTime::from_hms_opt(0, 0, 0).unwrap(); // Midnight
                let datetime = date.and_time(time);
                let timestamp =
                    DateTime::<Utc>::from_naive_utc_and_offset(datetime, Utc).timestamp_millis();

                builder.append_value(timestamp)
            }
            (List(builder), CqlValue::List(values)) => {
                for value in values {
                    builder.values().append_value(value)
                }
                builder.append(true)
            }
            (_, _) => {
                unreachable!("Schema is already checked. This indicates a bug")
            }
        }
    }
}

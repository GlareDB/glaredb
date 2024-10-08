use chrono::{DateTime, Datelike, NaiveDate, Timelike, Utc};
use rayexec_error::{not_implemented, RayexecError, Result};

use crate::{
    array::Array,
    datatype::{DataType, DecimalTypeMeta, TimeUnit},
    executor::{
        builder::{ArrayBuilder, PrimitiveBuffer},
        physical_type::{PhysicalI32, PhysicalI64},
        scalar::UnaryExecutor,
    },
    scalar::decimal::{Decimal64Type, DecimalType},
};

pub const EPOCH_NAIVE_DATE: NaiveDate = match NaiveDate::from_ymd_opt(1970, 1, 1) {
    Some(date) => date,
    _ => unreachable!(),
};

pub const EPOCH_DAYS_FROM_CE: i32 = 719_163;

pub const SECONDS_IN_DAY: i64 = 86_400;

/// Date parts that can be extracted for date and time values.
///
/// Follows Postgres conventions: <https://www.postgresql.org/docs/current/functions-datetime.html#FUNCTIONS-DATETIME-EXTRACT>
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatePart {
    /// Year divided by 100.
    Century,
    /// For timestamps, day of the month. For intervals, number of days.
    Day,
    /// Year divided by 10.
    Decade,
    /// Day of week, Sunday = 0, Saturday = 6.
    DayOfWeek,
    /// Day of year, 1 through 366
    DayOfYear,
    /// Number of seconds since unix epoch.
    Epoch,
    /// Hour field, 0-23
    Hour,
    /// Iso day of week, Monday = 1, Sunday = 7.
    IsoDayOfWeek,
    /// Fucked.
    IsoYear,
    /// Who is Julian?
    Julian,
    /// The seconds field, in microseconds.
    Microseconds,
    Millenium,
    /// The seconds field, in milliseconds.
    Milliseconds,
    /// The minute field, 0-59
    Minute,
    /// For timestamps, the month number (1-12). For intervals, number of months
    /// modulo 12 (0-11).
    Month,
    /// The quarter in a year, 1-4
    Quarter,
    /// The seconds field, including fractionals.
    Second,
    Timezone,
    TimezoneHour,
    TimezoneMinute,
    /// ISO week number.
    Week,
    /// The year field.
    Year,
}

/// Extracts a date part from the array.
///
/// The results should be decimal representing the part extracted, and should
/// use the Decimal64 default precision and scale.
pub fn extract_date_part(part: DatePart, arr: &Array) -> Result<Array> {
    let datatype = arr.datatype();
    match datatype {
        DataType::Date32 => match part {
            DatePart::Microseconds => date32_extract_with_fn(arr, extract_microseconds),
            DatePart::Milliseconds => date32_extract_with_fn(arr, extract_milliseconds),
            DatePart::Second => date32_extract_with_fn(arr, extract_seconds),
            DatePart::Minute => date32_extract_with_fn(arr, extract_minute),
            DatePart::DayOfWeek => date32_extract_with_fn(arr, extract_day_of_week),
            DatePart::IsoDayOfWeek => date32_extract_with_fn(arr, extract_iso_day_of_week),
            DatePart::Day => date32_extract_with_fn(arr, extract_day),
            DatePart::Month => date32_extract_with_fn(arr, extract_month),
            DatePart::Quarter => date32_extract_with_fn(arr, extract_quarter),
            DatePart::Year => date32_extract_with_fn(arr, extract_year),
            other => not_implemented!("Extract {other:?} from {datatype}"),
        },
        DataType::Date64 => match part {
            DatePart::Microseconds => date64_extract_with_fn(arr, extract_microseconds),
            DatePart::Milliseconds => date64_extract_with_fn(arr, extract_milliseconds),
            DatePart::Second => date64_extract_with_fn(arr, extract_seconds),
            DatePart::Minute => date64_extract_with_fn(arr, extract_minute),
            DatePart::DayOfWeek => date64_extract_with_fn(arr, extract_day_of_week),
            DatePart::IsoDayOfWeek => date64_extract_with_fn(arr, extract_iso_day_of_week),
            DatePart::Day => date64_extract_with_fn(arr, extract_day),
            DatePart::Month => date64_extract_with_fn(arr, extract_month),
            DatePart::Quarter => date64_extract_with_fn(arr, extract_quarter),
            DatePart::Year => date64_extract_with_fn(arr, extract_year),
            other => not_implemented!("Extract {other:?} from {datatype}"),
        },
        DataType::Timestamp(m) => match part {
            DatePart::Microseconds => timestamp_extract_with_fn(m.unit, arr, extract_microseconds),
            DatePart::Milliseconds => timestamp_extract_with_fn(m.unit, arr, extract_milliseconds),
            DatePart::Second => timestamp_extract_with_fn(m.unit, arr, extract_seconds),
            DatePart::Minute => timestamp_extract_with_fn(m.unit, arr, extract_minute),
            DatePart::DayOfWeek => timestamp_extract_with_fn(m.unit, arr, extract_day_of_week),
            DatePart::IsoDayOfWeek => {
                timestamp_extract_with_fn(m.unit, arr, extract_iso_day_of_week)
            }
            DatePart::Day => timestamp_extract_with_fn(m.unit, arr, extract_day),
            DatePart::Month => timestamp_extract_with_fn(m.unit, arr, extract_month),
            DatePart::Quarter => timestamp_extract_with_fn(m.unit, arr, extract_quarter),
            DatePart::Year => timestamp_extract_with_fn(m.unit, arr, extract_year),
            other => not_implemented!("Extract {other:?} from {datatype}"),
        },
        other => Err(RayexecError::new(format!(
            "Unable to extract date part for array with data type {other}"
        ))),
    }
}

fn timestamp_extract_with_fn<F>(unit: TimeUnit, arr: &Array, f: F) -> Result<Array>
where
    F: Fn(DateTime<Utc>) -> i64,
{
    match unit {
        TimeUnit::Second => timestamp_extract_with_fn_and_datetime_builder(arr, f, |val| {
            DateTime::from_timestamp(val, 0).unwrap_or_default()
        }),
        TimeUnit::Millisecond => timestamp_extract_with_fn_and_datetime_builder(arr, f, |val| {
            DateTime::from_timestamp_millis(val).unwrap_or_default()
        }),
        TimeUnit::Microsecond => timestamp_extract_with_fn_and_datetime_builder(arr, f, |val| {
            DateTime::from_timestamp_micros(val).unwrap_or_default()
        }),
        TimeUnit::Nanosecond => timestamp_extract_with_fn_and_datetime_builder(arr, f, |val| {
            DateTime::from_timestamp_nanos(val)
        }),
    }
}

fn timestamp_extract_with_fn_and_datetime_builder<F, B>(
    arr: &Array,
    f: F,
    builder: B,
) -> Result<Array>
where
    B: Fn(i64) -> DateTime<Utc>,
    F: Fn(DateTime<Utc>) -> i64,
{
    UnaryExecutor::execute::<PhysicalI64, _, _>(
        arr,
        ArrayBuilder {
            datatype: DataType::Decimal64(DecimalTypeMeta {
                precision: Decimal64Type::MAX_PRECISION,
                scale: Decimal64Type::DEFAULT_SCALE,
            }),
            buffer: PrimitiveBuffer::with_len(arr.logical_len()),
        },
        |val, buf| {
            let date = builder(val);
            buf.put(&f(date))
        },
    )
}

fn date32_extract_with_fn<F>(arr: &Array, f: F) -> Result<Array>
where
    F: Fn(DateTime<Utc>) -> i64,
{
    UnaryExecutor::execute::<PhysicalI32, _, _>(
        arr,
        ArrayBuilder {
            datatype: DataType::Decimal64(DecimalTypeMeta {
                precision: Decimal64Type::MAX_PRECISION,
                scale: Decimal64Type::DEFAULT_SCALE,
            }),
            buffer: PrimitiveBuffer::with_len(arr.logical_len()),
        },
        |val, buf| {
            // TODO: Can this actually fail?
            let date = DateTime::from_timestamp(val as i64 * SECONDS_IN_DAY, 0).unwrap_or_default();
            buf.put(&f(date))
        },
    )
}

fn date64_extract_with_fn<F>(arr: &Array, f: F) -> Result<Array>
where
    F: Fn(DateTime<Utc>) -> i64,
{
    UnaryExecutor::execute::<PhysicalI64, _, _>(
        arr,
        ArrayBuilder {
            datatype: DataType::Decimal64(DecimalTypeMeta {
                precision: Decimal64Type::MAX_PRECISION,
                scale: Decimal64Type::DEFAULT_SCALE,
            }),
            buffer: PrimitiveBuffer::with_len(arr.logical_len()),
        },
        |val, buf| {
            // TODO: Can this actually fail?
            let date = DateTime::from_timestamp_millis(val).unwrap_or_default();
            buf.put(&f(date))
        },
    )
}

/// Scale to use when computing a whole integer value.
///
/// This is needed since all 'extract' functions assume the output is a
/// Decimal64. So we need to scale the underlying int so that it's not
/// interpreted as a fractional.
const WHOLE_INT_SCALE: i64 = i64::pow(10, Decimal64Type::DEFAULT_SCALE as u32);

fn extract_quarter<T: Datelike + Timelike>(val: T) -> i64 {
    ((val.month0() / 3) + 1) as i64 * WHOLE_INT_SCALE
}

fn extract_year<T: Datelike + Timelike>(val: T) -> i64 {
    (val.year() as i64) * WHOLE_INT_SCALE
}

fn extract_month<T: Datelike + Timelike>(val: T) -> i64 {
    (val.month() as i64) * WHOLE_INT_SCALE
}

fn extract_day<T: Datelike + Timelike>(val: T) -> i64 {
    (val.day() as i64) * WHOLE_INT_SCALE
}

fn extract_day_of_week<T: Datelike + Timelike>(val: T) -> i64 {
    ((val.weekday().number_from_sunday() as i64) - 1) * WHOLE_INT_SCALE
}

fn extract_iso_day_of_week<T: Datelike + Timelike>(val: T) -> i64 {
    (val.weekday().number_from_monday() as i64) * WHOLE_INT_SCALE
}

fn extract_minute<T: Datelike + Timelike>(val: T) -> i64 {
    (val.minute() as i64) * WHOLE_INT_SCALE
}

/// Extracts seconds, including fractional.
///
/// Assumes Decimal64Type::DEFAULT_SCALE = 3
fn extract_seconds<T: Datelike + Timelike>(val: T) -> i64 {
    (val.nanosecond() / 1_000_000) as i64
}

/// Extracts milliseconds, including fractional.
///
/// Assumes Decimal64Type::DEFAULT_SCALE = 3
fn extract_milliseconds<T: Datelike + Timelike>(val: T) -> i64 {
    (val.nanosecond() / 1_000) as i64
}

/// Extracts microseconds, including fractional.
///
/// Assumes Decimal64Type::DEFAULT_SCALE = 3
fn extract_microseconds<T: Datelike + Timelike>(val: T) -> i64 {
    val.nanosecond() as i64
}

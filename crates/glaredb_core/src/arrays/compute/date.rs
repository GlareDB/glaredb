use chrono::{DateTime, Datelike, NaiveDate, Timelike, Utc};
use glaredb_error::{DbError, Result, not_implemented};

use crate::arrays::array::Array;
use crate::arrays::array::physical_type::{PhysicalI32, PhysicalI64};
use crate::arrays::datatype::{DataType, TimeUnit};
use crate::arrays::executor::OutBuffer;
use crate::arrays::executor::scalar::UnaryExecutor;
use crate::arrays::scalar::decimal::{Decimal64Type, DecimalType};
use crate::util::iter::IntoExactSizeIterator;

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
pub fn extract_date_part(
    part: DatePart,
    arr: &Array,
    sel: impl IntoExactSizeIterator<Item = usize>,
    out: &mut Array,
) -> Result<()> {
    let datatype = arr.datatype();
    match datatype {
        DataType::Date32 => match part {
            DatePart::Microseconds => date32_extract_with_fn(arr, sel, extract_microseconds, out),
            DatePart::Milliseconds => date32_extract_with_fn(arr, sel, extract_milliseconds, out),
            DatePart::Second => date32_extract_with_fn(arr, sel, extract_seconds, out),
            DatePart::Minute => date32_extract_with_fn(arr, sel, extract_minute, out),
            DatePart::DayOfWeek => date32_extract_with_fn(arr, sel, extract_day_of_week, out),
            DatePart::IsoDayOfWeek => {
                date32_extract_with_fn(arr, sel, extract_iso_day_of_week, out)
            }
            DatePart::Day => date32_extract_with_fn(arr, sel, extract_day, out),
            DatePart::Month => date32_extract_with_fn(arr, sel, extract_month, out),
            DatePart::Quarter => date32_extract_with_fn(arr, sel, extract_quarter, out),
            DatePart::Year => date32_extract_with_fn(arr, sel, extract_year, out),
            other => not_implemented!("Extract {other:?} from {datatype}"),
        },
        DataType::Date64 => match part {
            DatePart::Microseconds => date64_extract_with_fn(arr, sel, extract_microseconds, out),
            DatePart::Milliseconds => date64_extract_with_fn(arr, sel, extract_milliseconds, out),
            DatePart::Second => date64_extract_with_fn(arr, sel, extract_seconds, out),
            DatePart::Minute => date64_extract_with_fn(arr, sel, extract_minute, out),
            DatePart::DayOfWeek => date64_extract_with_fn(arr, sel, extract_day_of_week, out),
            DatePart::IsoDayOfWeek => {
                date64_extract_with_fn(arr, sel, extract_iso_day_of_week, out)
            }
            DatePart::Day => date64_extract_with_fn(arr, sel, extract_day, out),
            DatePart::Month => date64_extract_with_fn(arr, sel, extract_month, out),
            DatePart::Quarter => date64_extract_with_fn(arr, sel, extract_quarter, out),
            DatePart::Year => date64_extract_with_fn(arr, sel, extract_year, out),
            other => not_implemented!("Extract {other:?} from {datatype}"),
        },
        DataType::Timestamp(m) => match part {
            DatePart::Microseconds => {
                timestamp_extract_with_fn(m.unit, arr, sel, extract_microseconds, out)
            }
            DatePart::Milliseconds => {
                timestamp_extract_with_fn(m.unit, arr, sel, extract_milliseconds, out)
            }
            DatePart::Second => timestamp_extract_with_fn(m.unit, arr, sel, extract_seconds, out),
            DatePart::Minute => timestamp_extract_with_fn(m.unit, arr, sel, extract_minute, out),
            DatePart::DayOfWeek => {
                timestamp_extract_with_fn(m.unit, arr, sel, extract_day_of_week, out)
            }
            DatePart::IsoDayOfWeek => {
                timestamp_extract_with_fn(m.unit, arr, sel, extract_iso_day_of_week, out)
            }
            DatePart::Day => timestamp_extract_with_fn(m.unit, arr, sel, extract_day, out),
            DatePart::Month => timestamp_extract_with_fn(m.unit, arr, sel, extract_month, out),
            DatePart::Quarter => timestamp_extract_with_fn(m.unit, arr, sel, extract_quarter, out),
            DatePart::Year => timestamp_extract_with_fn(m.unit, arr, sel, extract_year, out),
            other => not_implemented!("Extract {other:?} from {datatype}"),
        },
        other => Err(DbError::new(format!(
            "Unable to extract date part for array with data type {other}"
        ))),
    }
}

fn timestamp_extract_with_fn<F>(
    unit: TimeUnit,
    arr: &Array,
    sel: impl IntoExactSizeIterator<Item = usize>,
    f: F,
    out: &mut Array,
) -> Result<()>
where
    F: Fn(DateTime<Utc>) -> i64,
{
    match unit {
        TimeUnit::Second => timestamp_extract_with_fn_and_datetime_builder(
            arr,
            sel,
            f,
            |val| DateTime::from_timestamp(val, 0).unwrap_or_default(),
            out,
        ),
        TimeUnit::Millisecond => timestamp_extract_with_fn_and_datetime_builder(
            arr,
            sel,
            f,
            |val| DateTime::from_timestamp_millis(val).unwrap_or_default(),
            out,
        ),
        TimeUnit::Microsecond => timestamp_extract_with_fn_and_datetime_builder(
            arr,
            sel,
            f,
            |val| DateTime::from_timestamp_micros(val).unwrap_or_default(),
            out,
        ),
        TimeUnit::Nanosecond => timestamp_extract_with_fn_and_datetime_builder(
            arr,
            sel,
            f,
            DateTime::from_timestamp_nanos,
            out,
        ),
    }
}

fn timestamp_extract_with_fn_and_datetime_builder<F, B>(
    arr: &Array,
    sel: impl IntoExactSizeIterator<Item = usize>,
    f: F,
    builder: B,
    out: &mut Array,
) -> Result<()>
where
    B: Fn(i64) -> DateTime<Utc>,
    F: Fn(DateTime<Utc>) -> i64,
{
    UnaryExecutor::execute::<PhysicalI64, PhysicalI64, _>(
        arr,
        sel,
        OutBuffer::from_array(out)?,
        |&val, buf| {
            let date = builder(val);
            buf.put(&f(date))
        },
    )
}

fn date32_extract_with_fn<F>(
    arr: &Array,
    sel: impl IntoExactSizeIterator<Item = usize>,
    f: F,
    out: &mut Array,
) -> Result<()>
where
    F: Fn(DateTime<Utc>) -> i64,
{
    UnaryExecutor::execute::<PhysicalI32, PhysicalI64, _>(
        arr,
        sel,
        OutBuffer::from_array(out)?,
        |&val, buf| {
            // TODO: Can this actually fail?
            let date = DateTime::from_timestamp(val as i64 * SECONDS_IN_DAY, 0).unwrap_or_default();
            buf.put(&f(date))
        },
    )
}

fn date64_extract_with_fn<F>(
    arr: &Array,
    sel: impl IntoExactSizeIterator<Item = usize>,
    f: F,
    out: &mut Array,
) -> Result<()>
where
    F: Fn(DateTime<Utc>) -> i64,
{
    UnaryExecutor::execute::<PhysicalI64, PhysicalI64, _>(
        arr,
        sel,
        OutBuffer::from_array(out)?,
        |&val, buf| {
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

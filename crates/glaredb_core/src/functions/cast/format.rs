//! Utilities for writing values into strings (and other buffers).
use std::fmt::{self, Display};
use std::marker::PhantomData;

use chrono::{DateTime, Utc};
use half::f16;

use crate::arrays::compute::date::SECONDS_IN_DAY;
use crate::arrays::scalar::decimal::DecimalPrimitive;
use crate::arrays::scalar::interval::Interval;

/// Logic for formatting and writing a type to a buffer.
pub trait Formatter {
    /// Type we're formatting.
    type Type;

    /// Write the value to the buffer.
    fn write<W: fmt::Write>(&self, val: &Self::Type, buf: &mut W) -> fmt::Result;
}

/// Formatter that uses the type's `Display` implmentation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DisplayFormatter<T: Display> {
    _type: PhantomData<T>,
}

impl<T: Display> Formatter for DisplayFormatter<T> {
    type Type = T;
    fn write<W: fmt::Write>(&self, val: &Self::Type, buf: &mut W) -> fmt::Result {
        write!(buf, "{val}")
    }
}

pub type BoolFormatter = DisplayFormatter<bool>;
pub type Int8Formatter = DisplayFormatter<i8>;
pub type Int16Formatter = DisplayFormatter<i16>;
pub type Int32Formatter = DisplayFormatter<i32>;
pub type Int64Formatter = DisplayFormatter<i64>;
pub type Int128Formatter = DisplayFormatter<i128>;
pub type UInt8Formatter = DisplayFormatter<u8>;
pub type UInt16Formatter = DisplayFormatter<u16>;
pub type UInt32Formatter = DisplayFormatter<u32>;
pub type UInt64Formatter = DisplayFormatter<u64>;
pub type UInt128Formatter = DisplayFormatter<u128>;
pub type Float16Formatter = DisplayFormatter<f16>;
pub type Float32Formatter = DisplayFormatter<f32>;
pub type Float64Formatter = DisplayFormatter<f64>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecimalFormatter<T: DecimalPrimitive> {
    precision: u8,
    scale: i8,
    /// Mask to use to get digits before and after the decimal point.
    mask: T,
}

pub type Decimal64Formatter = DecimalFormatter<i64>;
pub type Decimal128Formatter = DecimalFormatter<i128>;

impl<T: DecimalPrimitive> DecimalFormatter<T> {
    pub fn new(precision: u8, scale: i8) -> Self {
        let mask = T::pow(T::from(10).unwrap(), scale.unsigned_abs() as _);

        DecimalFormatter {
            precision,
            scale,
            mask,
        }
    }
}

impl<T> Formatter for DecimalFormatter<T>
where
    T: DecimalPrimitive,
{
    type Type = T;
    fn write<W: fmt::Write>(&self, val: &Self::Type, buf: &mut W) -> fmt::Result {
        let mut val = *val;

        if val.is_negative() {
            val = -val;
            write!(buf, "-")?;
        }

        let (int_digits, decimal_digits) = if self.scale < 0 {
            // Negative scale, never has anything to the right of the decimal
            // point.
            (val * self.mask, T::zero())
        } else {
            (val / self.mask, val % self.mask)
        };

        if int_digits.is_zero() {
            write!(buf, "0")?;
        } else {
            write!(buf, "{int_digits}")?;
        }

        if self.scale <= 0 {
            // Nothing left to do.
            return Ok(());
        }

        let pad = self.scale as usize; // We just checked that this is positive.
        write!(buf, ".{decimal_digits:0>pad$}")?;

        Ok(())
    }
}

/// Trait for converting an i64 to a Chrono DateTime;
pub trait DateTimeFromTimestamp {
    fn from(val: i64) -> Option<DateTime<Utc>>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DateTimeFromSeconds;
impl DateTimeFromTimestamp for DateTimeFromSeconds {
    fn from(val: i64) -> Option<DateTime<Utc>> {
        DateTime::from_timestamp(val, 0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DateTimeFromMilliseconds;
impl DateTimeFromTimestamp for DateTimeFromMilliseconds {
    fn from(val: i64) -> Option<DateTime<Utc>> {
        DateTime::from_timestamp_millis(val)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DateTimeFromMicroseconds;
impl DateTimeFromTimestamp for DateTimeFromMicroseconds {
    fn from(val: i64) -> Option<DateTime<Utc>> {
        DateTime::from_timestamp_micros(val)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DateTimeFromNanoseconds;
impl DateTimeFromTimestamp for DateTimeFromNanoseconds {
    fn from(val: i64) -> Option<DateTime<Utc>> {
        Some(DateTime::from_timestamp_nanos(val))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TimestampFormatter<T: DateTimeFromTimestamp> {
    _type: PhantomData<T>,
}

pub type TimestampSecondsFormatter = TimestampFormatter<DateTimeFromSeconds>;
pub type TimestampMillisecondsFormatter = TimestampFormatter<DateTimeFromMilliseconds>;
pub type TimestampMicrosecondsFormatter = TimestampFormatter<DateTimeFromMicroseconds>;
pub type TimestampNanosecondsFormatter = TimestampFormatter<DateTimeFromNanoseconds>;

impl<T: DateTimeFromTimestamp> Formatter for TimestampFormatter<T> {
    type Type = i64;
    fn write<W: fmt::Write>(&self, val: &Self::Type, buf: &mut W) -> fmt::Result {
        let datetime = T::from(*val).ok_or(fmt::Error)?;
        write!(buf, "{datetime}")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Date32Formatter;

impl Formatter for Date32Formatter {
    type Type = i32;
    fn write<W: fmt::Write>(&self, val: &Self::Type, buf: &mut W) -> fmt::Result {
        let datetime =
            DateTime::from_timestamp((*val as i64) * SECONDS_IN_DAY, 0).ok_or(fmt::Error)?;
        write!(buf, "{}", datetime.format("%Y-%m-%d"))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Date64Formatter;

impl Formatter for Date64Formatter {
    type Type = i64;
    fn write<W: fmt::Write>(&self, val: &Self::Type, buf: &mut W) -> fmt::Result {
        let datetime = DateTime::from_timestamp_millis(*val).ok_or(fmt::Error)?;
        write!(buf, "{datetime}")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IntervalFormatter;

impl Formatter for IntervalFormatter {
    type Type = Interval;
    fn write<W: fmt::Write>(&self, val: &Self::Type, buf: &mut W) -> fmt::Result {
        let years = val.months / 12;
        let months = val.months % 12;

        let days = val.days;

        let mut nanos = val.nanos;
        let hours = nanos / Interval::NANOSECONDS_IN_HOUR;
        nanos %= Interval::NANOSECONDS_IN_HOUR;
        let minutes = nanos / Interval::NANOSECONDS_IN_MINUTE;
        nanos %= Interval::NANOSECONDS_IN_MINUTE;
        let seconds = nanos / Interval::NANOSECONDS_IN_SECOND;
        nanos %= Interval::NANOSECONDS_IN_SECOND;
        let millis = nanos / Interval::NANOSECONDS_IN_MILLISECOND;

        let mut pad = false;

        if years > 0 {
            write!(buf, "{years} year")?;
            if years > 1 {
                write!(buf, "s")?;
            }
            pad = true;
        }

        if months > 0 {
            if pad {
                write!(buf, " ")?;
            }

            write!(buf, "{months} mon")?;
            if months > 1 {
                write!(buf, "s")?;
            }
            pad = true;
        }

        if days > 0 {
            if pad {
                write!(buf, " ")?;
            }

            write!(buf, "{days} day")?;
            if days > 1 {
                write!(buf, "s")?;
            }
            pad = true;
        }

        // Only write the "time" portion if anything is non-zero.
        if (hours + minutes + seconds + millis) != 0 {
            if pad {
                write!(buf, " ")?;
            }

            write!(buf, "{hours:02}:{minutes:02}:{seconds:02}")?;

            if millis > 0 {
                write!(buf, ".{millis:.6}")?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decimal_positive_scale() {
        let formatter = Decimal64Formatter::new(6, 3);
        let mut buf = String::new();
        formatter.write(&123450, &mut buf).unwrap();
        assert_eq!("123.450", buf);

        let mut buf = String::new();
        formatter.write(&123, &mut buf).unwrap();
        assert_eq!("0.123", buf);

        let mut buf = String::new();
        formatter.write(&12, &mut buf).unwrap();
        assert_eq!("0.012", buf);
    }

    #[test]
    fn decimal_negative_scale() {
        let formatter = Decimal64Formatter::new(6, -3);
        let mut buf = String::new();
        formatter.write(&123450, &mut buf).unwrap();
        assert_eq!("123450000", buf);

        let mut buf = String::new();
        formatter.write(&23, &mut buf).unwrap();
        assert_eq!("23000", buf);
    }

    #[test]
    fn decimal_zero_scale() {
        let formatter = Decimal64Formatter::new(6, 0);
        let mut buf = String::new();
        formatter.write(&123450, &mut buf).unwrap();
        assert_eq!("123450", buf);

        let mut buf = String::new();
        formatter.write(&23, &mut buf).unwrap();
        assert_eq!("23", buf);
    }

    #[test]
    fn decimal_negative_value() {
        let formatter = Decimal64Formatter::new(3, 2);
        let mut buf = String::new();
        formatter.write(&-49, &mut buf).unwrap();
        assert_eq!("-0.49", buf);
    }

    #[test]
    fn decimal_negative_value_with_left_padding_in_decimal() {
        let formatter = Decimal64Formatter::new(4, 3);
        let mut buf = String::new();
        formatter.write(&-49, &mut buf).unwrap();
        assert_eq!("-0.049", buf);
    }

    #[test]
    fn decimal_negative_value_with_non_zero_int() {
        let formatter = Decimal64Formatter::new(5, 3);
        let mut buf = String::new();
        formatter.write(&-24049, &mut buf).unwrap();
        assert_eq!("-24.049", buf);
    }

    #[test]
    fn decimal_negative_value_with_trailing_zero_in_decimal() {
        let formatter = Decimal64Formatter::new(4, 3);
        let mut buf = String::new();
        formatter.write(&-490, &mut buf).unwrap();
        assert_eq!("-0.490", buf);
    }

    #[test]
    fn decimal_zero_right_of_decimal() {
        let formatter = Decimal64Formatter::new(4, 1);
        let mut buf = String::new();
        formatter.write(&30, &mut buf).unwrap();
        assert_eq!("3.0", buf);
    }

    #[test]
    fn date32_basic() {
        let formatter = Date32Formatter;
        let mut buf = String::new();
        formatter.write(&8319, &mut buf).unwrap();
        assert_eq!("1992-10-11", buf);
    }

    #[test]
    fn interval() {
        let interval = Interval {
            months: (12 * 178) + 3,
            days: 0,
            nanos: 0,
        };
        let mut buf = String::new();
        IntervalFormatter.write(&interval, &mut buf).unwrap();
        assert_eq!("178 years 3 mons", buf);

        let interval = Interval {
            months: (12 * 178) + 3,
            days: 2,
            nanos: 0,
        };
        let mut buf = String::new();
        IntervalFormatter.write(&interval, &mut buf).unwrap();
        assert_eq!("178 years 3 mons 2 days", buf);

        let interval = Interval {
            months: 0,
            days: 0,
            nanos: 3 * Interval::NANOSECONDS_IN_HOUR,
        };
        let mut buf = String::new();
        IntervalFormatter.write(&interval, &mut buf).unwrap();
        assert_eq!("03:00:00", buf);

        let interval = Interval {
            months: 0,
            days: 0,
            nanos: (3 * Interval::NANOSECONDS_IN_HOUR) + (24 * Interval::NANOSECONDS_IN_SECOND),
        };
        let mut buf = String::new();
        IntervalFormatter.write(&interval, &mut buf).unwrap();
        assert_eq!("03:00:24", buf);

        let interval = Interval {
            months: 0,
            days: 0,
            nanos: (3 * Interval::NANOSECONDS_IN_HOUR)
                + (24 * Interval::NANOSECONDS_IN_SECOND)
                + (982 * Interval::NANOSECONDS_IN_MILLISECOND),
        };
        let mut buf = String::new();
        IntervalFormatter.write(&interval, &mut buf).unwrap();
        assert_eq!("03:00:24.982", buf);

        let interval = Interval {
            months: 14,
            days: 11,
            nanos: (3 * Interval::NANOSECONDS_IN_HOUR)
                + (24 * Interval::NANOSECONDS_IN_SECOND)
                + (982 * Interval::NANOSECONDS_IN_MILLISECOND),
        };
        let mut buf = String::new();
        IntervalFormatter.write(&interval, &mut buf).unwrap();
        assert_eq!("1 year 2 mons 11 days 03:00:24.982", buf);
    }
}

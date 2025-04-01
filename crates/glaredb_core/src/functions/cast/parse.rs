//! Parsing related utilities for casting from a string to other types.
use std::fmt::Write;
use std::marker::PhantomData;
use std::str::FromStr;

use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, Utc};
use half::f16;
use num_traits::PrimInt;

use crate::arrays::compute::date::EPOCH_DAYS_FROM_CE;
use crate::arrays::scalar::interval::Interval;

/// Logic for parsing a string into some type.
pub trait Parser {
    /// The type we'll be producing.
    type Type;

    /// Parse a string into `Type`, returning None if the parse cannot be done.
    fn parse(&mut self, s: &str) -> Option<Self::Type>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BoolParser;

impl Parser for BoolParser {
    type Type = bool;
    fn parse(&mut self, s: &str) -> Option<Self::Type> {
        match s {
            "t" | "true" | "TRUE" | "T" => Some(true),
            "f" | "false" | "FALSE" | "F" => Some(false),
            _ => None,
        }
    }
}

/// Parser that uses the stdlib `FromStr` trait.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct FromStrParser<T: FromStr> {
    _type: PhantomData<T>,
}

impl<T: FromStr> FromStrParser<T> {
    pub const fn new() -> Self {
        FromStrParser { _type: PhantomData }
    }
}

impl<T: FromStr> Parser for FromStrParser<T> {
    type Type = T;
    fn parse(&mut self, s: &str) -> Option<Self::Type> {
        T::from_str(s).ok()
    }
}

pub type Int8Parser = FromStrParser<i8>;
pub type Int16Parser = FromStrParser<i16>;
pub type Int32Parser = FromStrParser<i32>;
pub type Int64Parser = FromStrParser<i64>;
pub type Int128Parser = FromStrParser<i128>;
pub type UInt8Parser = FromStrParser<u8>;
pub type UInt16Parser = FromStrParser<u16>;
pub type UInt32Parser = FromStrParser<u32>;
pub type UInt64Parser = FromStrParser<u64>;
pub type UInt128Parser = FromStrParser<u128>;
pub type Float16Parser = FromStrParser<f16>;
pub type Float32Parser = FromStrParser<f32>;
pub type Float64Parser = FromStrParser<f64>;

/// Parse a string date into a number of days since epoch.
///
/// Example formats:
///
/// '1992-10-11'
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Date32Parser;

impl Parser for Date32Parser {
    type Type = i32;
    fn parse(&mut self, s: &str) -> Option<Self::Type> {
        let date = NaiveDate::from_str(s).ok()?;
        Some(date.num_days_from_ce() - EPOCH_DAYS_FROM_CE)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DecimalParser<T: PrimInt> {
    precision: u8,
    scale: i8,
    _type: PhantomData<T>,
}

pub type Decimal64Parser = DecimalParser<i64>;
pub type Decimal128Parser = DecimalParser<i128>;

impl<T: PrimInt> DecimalParser<T> {
    pub fn new(precision: u8, scale: i8) -> Self {
        DecimalParser {
            precision,
            scale,
            _type: PhantomData,
        }
    }
}

impl<T: PrimInt> Parser for DecimalParser<T> {
    type Type = T;
    fn parse(&mut self, s: &str) -> Option<Self::Type> {
        let bs = s.as_bytes();
        let (neg, bs) = match bs.first() {
            Some(b'-') => (true, &bs[1..]),
            Some(b'+') => (false, &bs[1..]),
            _ => (false, bs),
        };

        let mut val = T::zero();
        let mut digits: u8 = 0; // Total number of digits.
        let mut decimals: i8 = 0; // Digits to right of decimal point.

        let ten = T::from(10).unwrap();

        let mut iter = bs.iter();

        // Leading digits.
        for b in iter.by_ref() {
            match b {
                b'0'..=b'9' => {
                    // Leading zero.
                    if digits == 0 && *b == b'0' {
                        continue;
                    }
                    digits += 1;
                    val = val.mul(ten);
                    val = val.add(T::from(b - b'0').unwrap());
                }
                b'.' => {
                    break;
                }
                _ => return None,
            }
        }

        // Digits after decimal.
        for b in iter {
            match b {
                b'0'..=b'9' => {
                    if decimals == self.scale {
                        continue;
                    }

                    decimals += 1;
                    digits += 1;
                    val = val.mul(ten);
                    val = val.add(T::from(b - b'0').unwrap());
                }
                b'e' | b'E' => return None,
                _ => return None,
            }
        }

        if self.scale < 0 {
            digits -= self.scale.unsigned_abs();
            let exp = self.scale.unsigned_abs() as u32;
            val = val.div(ten.pow(exp));
        }

        if digits > self.precision {
            return None;
        }

        if decimals < self.scale {
            let exp = (self.scale - decimals) as u32;
            val = val.mul(ten.pow(exp));
        }

        if neg {
            val = T::zero().sub(val);
        }

        Some(val)
    }
}

const NANOSECONDS_IN_SECONDS: f64 = 1_000_000_000.0;

#[derive(Debug, Clone, Copy)]
enum IntervalUnit {
    Millenium,
    Century,
    Decade,
    Year,
    Month,
    Week,
    Day,
    Hour,
    Minute,
    Second,
    Millisecond,
    Microsecond,
    Nanosecond,
}

impl FromStr for IntervalUnit {
    type Err = ();
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "millenium" | "milleniums" => Self::Millenium,
            "century" | "centuries" => Self::Century,
            "decade" | "decades" => Self::Decade,
            "year" | "years" => Self::Year,
            "month" | "months" => Self::Month,
            "week" | "weeks" => Self::Week,
            "day" | "days" => Self::Day,
            "hour" | "hours" => Self::Hour,
            "minute" | "minutes" | "min" | "mins" => Self::Minute,
            "second" | "seconds" | "sec" | "secs" => Self::Second,
            "millisecond" | "milliseconds" => Self::Millisecond,
            "microsecond" | "microseconds" => Self::Microsecond,
            "nanosecond" | "nanoseconds" => Self::Nanosecond,
            _ => return Err(()),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct IntervalParser {
    buf: String,
}

impl Parser for IntervalParser {
    type Type = Interval;
    fn parse(&mut self, s: &str) -> Option<Self::Type> {
        self.buf.clear();
        self.buf.write_str(s).ok()?;
        self.buf.make_ascii_lowercase();
        let s = &self.buf;

        let mut fields: Vec<_> = s.split_whitespace().collect();

        // If there's only one "field", assume it's seconds.
        if fields.len() == 1 {
            let sec = fields.pop().unwrap().parse::<f64>().ok()?;
            let nanos = sec * NANOSECONDS_IN_SECONDS;
            return Some(Interval {
                months: 0,
                days: 0,
                nanos: nanos.round() as i64,
            });
        }

        if fields.len() % 2 != 0 {
            return None;
        }

        let mut interval = Interval {
            months: 0,
            days: 0,
            nanos: 0,
        };

        // TODO: Duplicate unit check

        for field in fields.chunks(2) {
            let quantity = field[0].parse::<f64>().ok()?;
            let unit = IntervalUnit::from_str(field[1]).ok()?;

            // TODO: Move the inner logic to methods on Interval.
            match unit {
                IntervalUnit::Millenium => {
                    let mill = quantity.floor();
                    let centuries = ((quantity - mill) * 10.0) as i32;
                    interval.add_millenium(mill as i32);
                    interval.add_centuries(centuries);
                }
                IntervalUnit::Century => {
                    let centuries = quantity.floor();
                    let decades = ((quantity - centuries) * 10.0) as i32;
                    interval.add_centuries(centuries as i32);
                    interval.add_decades(decades);
                }
                IntervalUnit::Decade => {
                    let decades = quantity.floor();
                    let years = ((quantity - decades) * 10.0) as i32;
                    interval.add_decades(decades as i32);
                    interval.add_years(years);
                }
                IntervalUnit::Year => {
                    let years = quantity.floor();
                    let months = ((quantity - years) * 12.0) as i32;
                    interval.add_years(years as i32);
                    interval.add_months(months);
                }
                IntervalUnit::Month => {
                    let months = quantity.floor();
                    let days =
                        ((quantity - months) * Interval::ASSUMED_DAYS_IN_MONTH as f64) as i32;
                    interval.add_months(months as i32);
                    interval.add_days(days);
                }
                IntervalUnit::Week => {
                    let weeks = quantity.floor();
                    let days = ((quantity - weeks) * 7.0) as i32;
                    interval.add_days(weeks as i32 * 7);
                    interval.add_days(days);
                }
                IntervalUnit::Day => {
                    let days = quantity.floor();
                    let hours = ((quantity - days) * Interval::ASSUMED_HOURS_IN_DAY as f64) as i32;
                    interval.add_days(days as i32);
                    interval.add_hours(hours as i64);
                }
                IntervalUnit::Hour => {
                    let hours = quantity.floor();
                    let minutes = ((quantity - hours) * 60.0) as i64;
                    interval.add_hours(hours as i64);
                    interval.add_minutes(minutes);
                }
                IntervalUnit::Minute => {
                    let minutes = quantity.floor();
                    let secs = ((quantity - minutes) * 60.0) as i64;
                    interval.add_minutes(minutes as i64);
                    interval.add_seconds(secs);
                }
                IntervalUnit::Second => {
                    let secs = quantity.floor();
                    let ms = ((quantity - secs) * 1000.0) as i64;
                    interval.add_seconds(secs as i64);
                    interval.add_milliseconds(ms);
                }
                IntervalUnit::Millisecond => {
                    let ms = quantity.floor();
                    let us = ((quantity - ms) * 1000.0) as i64;
                    interval.add_milliseconds(ms as i64);
                    interval.add_microseconds(us);
                }
                IntervalUnit::Microsecond => {
                    let us = quantity.floor();
                    let ns = ((quantity - us) * 1000.0) as i64;
                    interval.add_microseconds(us as i64);
                    interval.nanos += ns;
                }
                IntervalUnit::Nanosecond => {
                    let ns = quantity.round() as i64;
                    interval.nanos += ns;
                }
            }
        }

        Some(interval)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrays::compute::date::EPOCH_NAIVE_DATE;

    #[test]
    fn epoch_days() {
        assert_eq!(EPOCH_DAYS_FROM_CE, EPOCH_NAIVE_DATE.num_days_from_ce());
    }

    #[test]
    fn test_parse_date32() {
        assert_eq!(8319, Date32Parser.parse("1992-10-11").unwrap());
        assert_eq!(-1, Date32Parser.parse("1969-12-31").unwrap());
    }

    #[test]
    fn parse_decimal() {
        // Can parse
        assert_eq!(123, Decimal64Parser::new(5, 1).parse("12.3").unwrap());
        assert_eq!(12, Decimal64Parser::new(5, 0).parse("12.3").unwrap());
        assert_eq!(1230, Decimal64Parser::new(5, 1).parse("123").unwrap());
        assert_eq!(-1230, Decimal64Parser::new(5, 1).parse("-123").unwrap());
        assert_eq!(1230, Decimal64Parser::new(5, 2).parse("12.3").unwrap());
        assert_eq!(123, Decimal64Parser::new(3, 1).parse("12.3").unwrap());
        assert_eq!(123, Decimal64Parser::new(3, 0).parse("123.4").unwrap());
        assert_eq!(-1230, Decimal64Parser::new(5, 2).parse("-12.3").unwrap());
        assert_eq!(123, Decimal64Parser::new(5, -2).parse("12300").unwrap());

        // Can't parse
        assert_eq!(None, Decimal64Parser::new(5, 1).parse("1four2.3"));
        assert_eq!(None, Decimal64Parser::new(5, 1).parse("12.3a"));
        assert_eq!(None, Decimal64Parser::new(3, 1).parse("123.4")); // "overflow"
    }

    #[test]
    fn parse_intervals() {
        let expected = Interval {
            months: 0,
            days: 0,
            nanos: 2_500_000_000,
        };
        assert_eq!(expected, IntervalParser::default().parse("2.5").unwrap());

        let expected = Interval {
            months: 0,
            days: 1,
            nanos: 3 * Interval::NANOSECONDS_IN_HOUR,
        };
        assert_eq!(
            expected,
            IntervalParser::default().parse("1 day 3 hours").unwrap()
        );

        let expected = Interval {
            months: 0,
            days: 1,
            nanos: 12 * Interval::NANOSECONDS_IN_HOUR,
        };
        assert_eq!(
            expected,
            IntervalParser::default().parse("1.5 days").unwrap()
        );

        let expected = Interval {
            months: 0,
            days: 1,
            nanos: 14 * Interval::NANOSECONDS_IN_HOUR,
        };
        assert_eq!(
            expected,
            IntervalParser::default().parse("1.5 days 2 hours").unwrap()
        );
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimestampParser {
    pub unit: crate::arrays::datatype::TimeUnit,
}

impl Parser for TimestampParser {
    type Type = i64;
    fn parse(&mut self, s: &str) -> Option<Self::Type> {
        if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
            match self.unit {
                crate::arrays::datatype::TimeUnit::Second => Some(dt.timestamp()),
                crate::arrays::datatype::TimeUnit::Millisecond => Some(dt.timestamp_millis()),
                crate::arrays::datatype::TimeUnit::Microsecond => Some(dt.timestamp_micros()),
                crate::arrays::datatype::TimeUnit::Nanosecond => {
                    Some(dt.timestamp_nanos_opt().unwrap())
                }
            }
        } else if let Ok(dt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
            let dt = DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc);
            match self.unit {
                crate::arrays::datatype::TimeUnit::Second => Some(dt.timestamp()),
                crate::arrays::datatype::TimeUnit::Millisecond => Some(dt.timestamp_millis()),
                crate::arrays::datatype::TimeUnit::Microsecond => Some(dt.timestamp_micros()),
                crate::arrays::datatype::TimeUnit::Nanosecond => {
                    Some(dt.timestamp_nanos_opt().unwrap())
                }
            }
        } else if let Ok(dt) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
            let dt =
                DateTime::<Utc>::from_naive_utc_and_offset(dt.and_hms_opt(0, 0, 0).unwrap(), Utc);
            match self.unit {
                crate::arrays::datatype::TimeUnit::Second => Some(dt.timestamp()),
                crate::arrays::datatype::TimeUnit::Millisecond => Some(dt.timestamp_millis()),
                crate::arrays::datatype::TimeUnit::Microsecond => Some(dt.timestamp_micros()),
                crate::arrays::datatype::TimeUnit::Nanosecond => {
                    Some(dt.timestamp_nanos_opt().unwrap())
                }
            }
        } else if let Ok(unix_timestamp) = s.parse::<i64>() {
            let dt = DateTime::<Utc>::from_timestamp(unix_timestamp, 0).unwrap();
            match self.unit {
                crate::arrays::datatype::TimeUnit::Second => Some(dt.timestamp()),
                crate::arrays::datatype::TimeUnit::Millisecond => Some(dt.timestamp_millis()),
                crate::arrays::datatype::TimeUnit::Microsecond => Some(dt.timestamp_micros()),
                crate::arrays::datatype::TimeUnit::Nanosecond => {
                    Some(dt.timestamp_nanos_opt().unwrap())
                }
            }
        } else {
            None
        }
    }
}

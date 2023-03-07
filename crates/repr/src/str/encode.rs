use chrono::{Datelike, Timelike};
use dtoa::{Buffer as DtoaBuffer, Float as DtoaFloat};
use num_traits::{Float as NumFloat, PrimInt as NumInt};
use std::fmt::{Display, Write};

use crate::error::{ReprError, Result};

macro_rules! put_fmt {
    ($dst:expr, $($arg:tt)*) => {
        write!($dst, $($arg)*).map_err(|e| ReprError::from(e))
    };
}

/// Encode bool as a string.
pub fn encode_bool<B: Write>(buf: &mut B, v: bool) -> Result<()> {
    if v {
        put_fmt!(buf, "t")
    } else {
        put_fmt!(buf, "f")
    }
}

/// Encode integral value as a string.
pub fn encode_int<B, I>(buf: &mut B, v: I) -> Result<()>
where
    B: Write,
    I: NumInt + Display,
{
    put_fmt!(buf, "{v}")
}

/// Encode floating point value as a string.
pub fn encode_float<B, F>(buf: &mut B, v: F) -> Result<()>
where
    B: Write,
    F: NumFloat + DtoaFloat,
{
    if v.is_nan() {
        return put_fmt!(buf, "NaN");
    }

    if v.is_infinite() {
        if v.is_sign_negative() {
            return put_fmt!(buf, "-Infinity");
        } else {
            return put_fmt!(buf, "Infinity");
        }
    }

    if v.is_zero() && v.is_sign_negative() {
        return put_fmt!(buf, "-0");
    }

    // We don't use the standard library to get the formatting for floats.
    // Postgres uses the C style "%g" formatting, which states that:
    //
    //     The double argument is converted in style f or e (or F or E
    //     for G conversions).  The precision specifies the number of
    //     significant digits.  If the precision is missing, 6 digits are
    //     given; if the precision is zero, it is treated as 1.  Style e
    //     is used if the exponent from its conversion is less than -4 or
    //     greater than or equal to the precision.  Trailing zeros are
    //     removed from the fractional part of the result; a decimal
    //     point appears only if it is followed by at least one digit.
    //
    // Dtoa is the closest we have to this in Rust. Maybe in future we can look
    // at using the libc bindings to get it working exactly!

    // Creating this buffer is very cheap, nothing to worry.
    let mut dtoa_buf = DtoaBuffer::new();

    let v = dtoa_buf
        .format_finite(v)
        // Remove any trailing ".0" so as to get whole numbers in case of when
        // fractional part is 0.
        .trim_end_matches(".0");

    let mut iter = v.chars().peekable();
    while let Some(c) = iter.next() {
        buf.write_char(c)?;

        if c == 'e' {
            // Postgres represents exponential values as "1e+9" and "1e-9".
            // Adding a '+' sign in the case of positive exponent since the
            // library doesn't (it returns "1e9").
            if iter.peek() != Some(&'-') {
                buf.write_char('+')?;
            }
        }
    }

    Ok(())
}

/// Encode a string value.
pub fn encode_string<B, S>(buf: &mut B, v: S) -> Result<()>
where
    B: Write,
    S: Display,
{
    put_fmt!(buf, "{}", v)
}

/// Encode a binary value as a hex string prefixed by "\x".
pub fn encode_binary<B, V>(buf: &mut B, v: V) -> Result<()>
where
    B: Write,
    V: AsRef<[u8]>,
{
    put_fmt!(buf, "\\x")?;
    put_binary(buf, v.as_ref())
}

/// Encode a binary value as a hex string prefixed by "0x".
pub fn encode_binary_mysql<B, V>(buf: &mut B, v: V) -> Result<()>
where
    B: Write,
    V: AsRef<[u8]>,
{
    put_fmt!(buf, "0x")?;
    put_binary(buf, v.as_ref())
}

fn put_binary<B>(buf: &mut B, v: &[u8]) -> Result<()>
where
    B: Write,
{
    for i in v.iter() {
        // Pad buffer with a '0' to represent single digits (04, 0a...)
        if i < &16 {
            buf.write_char('0')?;
        }
        put_fmt!(buf, "{:x}", i)?;
    }
    Ok(())
}

fn put_micros<B, T>(buf: &mut B, v: &T) -> Result<()>
where
    B: Write,
    T: Timelike,
{
    let nanos = v.nanosecond() as i64;
    // Add 500 ns to let flooring integer division round the time to nearest microsecond
    let nanos = nanos + 500;
    let mut micros = nanos / 1_000;
    if micros > 0 {
        // Remove the trailing zeros from microseconds.
        let mut width = 6;
        while micros % 10 == 0 {
            width -= 1;
            micros /= 10;
        }
        put_fmt!(buf, ".{micros:0width$}")?;
    }
    Ok(())
}

fn put_year_ce<B: Write>(buf: &mut B, is_ad: bool) -> Result<()> {
    if !is_ad {
        put_fmt!(buf, " BC")?;
    }
    Ok(())
}

/// Returns true if year is AD else false.
fn put_only_date<B, T>(buf: &mut B, v: &T) -> Result<bool>
where
    B: Write,
    T: Datelike,
{
    let (is_ad, year) = v.year_ce();
    let (month, day) = (v.month(), v.day());
    put_fmt!(buf, "{year}-{month:02}-{day:02}")?;
    Ok(is_ad)
}

/// Encode a time value as a string.
pub fn encode_time<B, T>(buf: &mut B, v: &T, tz: bool) -> Result<()>
where
    B: Write,
    T: Timelike,
{
    let (hour, minute, second) = (v.hour(), v.minute(), v.second());
    put_fmt!(buf, "{hour:02}:{minute:02}:{second:02}")?;
    put_micros(buf, v)?;
    if tz {
        put_fmt!(buf, "+00")?;
    }
    Ok(())
}

/// Encode a date value as a string.
pub fn encode_date<B, T>(buf: &mut B, v: &T) -> Result<()>
where
    B: Write,
    T: Datelike,
{
    let is_ad = put_only_date(buf, v)?;
    put_year_ce(buf, is_ad)
}

/// Encode a UTC timestamp as a string. Setting tz = true will print the
/// timezone information as offset (+00).
pub fn encode_utc_timestamp<B, T>(buf: &mut B, v: &T, tz: bool) -> Result<()>
where
    B: Write,
    T: Timelike + Datelike,
{
    let is_ad = put_only_date(buf, v)?;
    // Add a space between date and time.
    buf.write_char(' ')?;
    encode_time(buf, v, tz)?;
    put_year_ce(buf, is_ad)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use chrono::{NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Utc};

    use super::*;

    macro_rules! assert_encode {
        ($fn:ident, $val:expr, $str:expr) => {{
            let mut s = String::new();
            $fn(&mut s, $val).unwrap();
            let e: &str = $str.as_ref();
            assert_eq!(&s, e);
        }};

        ($fn:ident, $val:expr, $opt:expr, $str:expr) => {{
            let mut s = String::new();
            $fn(&mut s, $val, $opt).unwrap();
            assert_eq!(s, $str.to_string());
        }};
    }

    #[test]
    fn test_encode() {
        assert_encode!(encode_bool, true, "t");
        assert_encode!(encode_bool, false, "f");

        assert_encode!(encode_int, 123_i8, "123");
        assert_encode!(encode_int, -123_i8, "-123");
        assert_encode!(encode_int, 1234_i16, "1234");
        assert_encode!(encode_int, -1234_i16, "-1234");
        assert_encode!(encode_int, 654321_i32, "654321");
        assert_encode!(encode_int, -654321_i32, "-654321");
        assert_encode!(encode_int, 1234567890_i64, "1234567890");
        assert_encode!(encode_int, -1234567890_i64, "-1234567890");

        assert_encode!(encode_float, 123.456_f32, "123.456");
        assert_encode!(encode_float, -123.456_f32, "-123.456");
        assert_encode!(encode_float, 0.000000001_f32, "1e-9");
        assert_encode!(encode_float, 1234000000000000000000.0_f32, "1.234e+21");
        assert_encode!(encode_float, f32::NAN, "NaN");
        assert_encode!(encode_float, f32::INFINITY, "Infinity");
        assert_encode!(encode_float, f32::NEG_INFINITY, "-Infinity");
        assert_encode!(encode_float, -0.0_f32, "-0");
        assert_encode!(encode_float, 0.0_f32, "0");
        assert_encode!(encode_float, 123.0456789_f64, "123.0456789");
        assert_encode!(encode_float, 0.000000001_f64, "1e-9");
        assert_encode!(encode_float, 1234000000000000000000.0_f64, "1.234e+21");
        assert_encode!(encode_float, -123.0456789_f64, "-123.0456789");
        assert_encode!(encode_float, f64::NAN, "NaN");
        assert_encode!(encode_float, f64::INFINITY, "Infinity");
        assert_encode!(encode_float, f64::NEG_INFINITY, "-Infinity");
        assert_encode!(encode_float, -0.0_f64, "-0");
        assert_encode!(encode_float, 0.0_f64, "0");

        assert_encode!(encode_string, "abcdefghij", "abcdefghij");

        assert_encode!(encode_binary, &[23, 13, 255, 0, 130], "\\x170dff0082");

        assert_encode!(encode_binary_mysql, &[23, 13, 255, 0, 130], "0x170dff0082");

        let nt = NaiveDateTime::from_timestamp_opt(938689324, 0).unwrap();
        assert_encode!(
            encode_utc_timestamp,
            &nt,
            false,
            nt.format("%Y-%m-%d %H:%M:%S")
        );

        let nt = NaiveDateTime::from_timestamp_opt(938689324, 123567).unwrap();
        assert_encode!(
            encode_utc_timestamp,
            &nt,
            false,
            format!("{}.000124", nt.format("%Y-%m-%d %H:%M:%S"))
        );

        let nt = NaiveDateTime::from_timestamp_opt(938689324, 123_400_000).unwrap();
        assert_encode!(
            encode_utc_timestamp,
            &nt,
            false,
            format!("{}.1234", nt.format("%Y-%m-%d %H:%M:%S"))
        );

        let nt = NaiveDateTime::from_timestamp_opt(-197199051, 0).unwrap();
        assert_encode!(
            encode_utc_timestamp,
            &nt,
            false,
            nt.format("%Y-%m-%d %H:%M:%S")
        );

        let nt = NaiveDateTime::from_timestamp_opt(-62143593684, 0).unwrap();
        assert_encode!(
            encode_utc_timestamp,
            &nt,
            false,
            format!("1-{} BC", nt.format("%m-%d %H:%M:%S"))
        );

        let dt = Utc.timestamp_opt(938689324, 0).unwrap();
        assert_encode!(
            encode_utc_timestamp,
            &dt,
            true,
            format!("{}+00", dt.format("%Y-%m-%d %H:%M:%S"))
        );

        let dt = Utc.timestamp_opt(938689324, 123567).unwrap();
        assert_encode!(
            encode_utc_timestamp,
            &dt,
            true,
            format!("{}.000124+00", dt.format("%Y-%m-%d %H:%M:%S"))
        );

        let dt = Utc.timestamp_opt(938689324, 123_400_000).unwrap();
        assert_encode!(
            encode_utc_timestamp,
            &dt,
            true,
            format!("{}.1234+00", dt.format("%Y-%m-%d %H:%M:%S"))
        );

        let dt = Utc.timestamp_opt(-197199051, 0).unwrap();
        assert_encode!(
            encode_utc_timestamp,
            &dt,
            true,
            format!("{}+00", dt.format("%Y-%m-%d %H:%M:%S"))
        );

        let dt = Utc.timestamp_opt(-62143593684, 0).unwrap();
        assert_encode!(
            encode_utc_timestamp,
            &dt,
            true,
            format!("1-{}+00 BC", dt.format("%m-%d %H:%M:%S"))
        );

        let nt = NaiveTime::from_hms_nano_opt(16, 32, 4, 0).unwrap();
        assert_encode!(encode_time, &nt, false, "16:32:04");

        let nt = NaiveTime::from_hms_nano_opt(16, 32, 4, 123567).unwrap();
        assert_encode!(encode_time, &nt, false, "16:32:04.000124");

        let nt = NaiveTime::from_hms_nano_opt(16, 32, 4, 123_400_000).unwrap();
        assert_encode!(encode_time, &nt, false, "16:32:04.1234");

        let nt = NaiveTime::from_hms_nano_opt(16, 32, 4, 0).unwrap();
        assert_encode!(encode_time, &nt, true, "16:32:04+00");

        let nt = NaiveTime::from_hms_nano_opt(16, 32, 4, 123567).unwrap();
        assert_encode!(encode_time, &nt, true, "16:32:04.000124+00");

        let nt = NaiveTime::from_hms_nano_opt(16, 32, 4, 123_400_000).unwrap();
        assert_encode!(encode_time, &nt, true, "16:32:04.1234+00");

        let nd = NaiveDate::from_ymd_opt(1999, 9, 30).unwrap();
        assert_encode!(encode_date, &nd, "1999-09-30");

        let nd = NaiveDate::from_ymd_opt(1963, 10, 2).unwrap();
        assert_encode!(encode_date, &nd, "1963-10-02");

        let nd = NaiveDate::from_ymd_opt(0, 9, 30).unwrap();
        assert_encode!(encode_date, &nd, "1-09-30 BC");
    }
}

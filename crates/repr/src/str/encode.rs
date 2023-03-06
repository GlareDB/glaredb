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

pub fn encode_bool<B: Write>(buf: &mut B, v: bool) -> Result<()> {
    if v {
        put_fmt!(buf, "t")
    } else {
        put_fmt!(buf, "f")
    }
}

pub fn encode_int<B, I>(buf: &mut B, v: I) -> Result<()>
where
    B: Write,
    I: NumInt + Display,
{
    put_fmt!(buf, "{v}")
}

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

pub fn encode_string<B, S>(buf: &mut B, v: S) -> Result<()>
where
    B: Write,
    S: Display,
{
    put_fmt!(buf, "{}", v)
}

pub fn encode_binary<B, V>(buf: &mut B, v: V) -> Result<()>
where
    B: Write,
    V: AsRef<[u8]>,
{
    put_fmt!(buf, "\\x")?;
    put_binary(buf, v.as_ref())
}

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

pub fn encode_date<B, T>(buf: &mut B, v: &T) -> Result<()>
where
    B: Write,
    T: Datelike,
{
    let is_ad = put_only_date(buf, v)?;
    put_year_ce(buf, is_ad)
}

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

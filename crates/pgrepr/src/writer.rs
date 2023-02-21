use crate::error::{PgReprError, Result};
use bytes::{BufMut, BytesMut};
use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike, Utc};
use dtoa::{Buffer as DtoaBuffer, Float as DtoaFloat};
use num_traits::Float as NumFloat;
use std::fmt::Write;
use tokio_postgres::types::{IsNull, ToSql, Type as PgType};

macro_rules! put_fmt {
    ($dst:expr, $($arg:tt)*) => {
        write!($dst, $($arg)*).map_err(|e| PgReprError::from(e))
    };
}

/// Writer defines the interface for the different kinds of values that can be
/// encoded as a postgres type.
pub(crate) trait Writer {
    fn write_bool(buf: &mut BytesMut, v: bool) -> Result<()>;

    fn write_int2(buf: &mut BytesMut, v: i16) -> Result<()>;
    fn write_int4(buf: &mut BytesMut, v: i32) -> Result<()>;
    fn write_int8(buf: &mut BytesMut, v: i64) -> Result<()>;

    fn write_float4(buf: &mut BytesMut, v: f32) -> Result<()>;
    fn write_float8(buf: &mut BytesMut, v: f64) -> Result<()>;

    fn write_text(buf: &mut BytesMut, v: String) -> Result<()>;
    fn write_bytea(buf: &mut BytesMut, v: Vec<u8>) -> Result<()>;

    fn write_timestamp(buf: &mut BytesMut, v: NaiveDateTime) -> Result<()>;
    fn write_timestamptz(buf: &mut BytesMut, v: DateTime<Utc>) -> Result<()>;
    fn write_time(buf: &mut BytesMut, v: NaiveTime) -> Result<()>;
    fn write_date(buf: &mut BytesMut, v: NaiveDate) -> Result<()>;
}

#[derive(Debug)]
pub(crate) struct TextWriter;

impl Writer for TextWriter {
    fn write_bool(buf: &mut BytesMut, v: bool) -> Result<()> {
        let v = if v { 't' } else { 'f' };
        put_fmt!(buf, "{v}")
    }

    fn write_int2(buf: &mut BytesMut, v: i16) -> Result<()> {
        put_fmt!(buf, "{v}")
    }

    fn write_int4(buf: &mut BytesMut, v: i32) -> Result<()> {
        put_fmt!(buf, "{v}")
    }

    fn write_int8(buf: &mut BytesMut, v: i64) -> Result<()> {
        put_fmt!(buf, "{v}")
    }

    fn write_float4(buf: &mut BytesMut, v: f32) -> Result<()> {
        put_float_text(buf, v)
    }

    fn write_float8(buf: &mut BytesMut, v: f64) -> Result<()> {
        put_float_text(buf, v)
    }

    fn write_text(buf: &mut BytesMut, v: String) -> Result<()> {
        put_fmt!(buf, "{v}")
    }

    fn write_bytea(buf: &mut BytesMut, v: Vec<u8>) -> Result<()> {
        put_fmt!(buf, "\\x")?;
        for i in v.into_iter() {
            // Pad buffer with a '0' to represent single digits (04, 0a...)
            if i < 16 {
                buf.put_u8(b'0');
            }
            put_fmt!(buf, "{:x}", i)?;
        }
        Ok(())
    }

    fn write_timestamp(buf: &mut BytesMut, v: NaiveDateTime) -> Result<()> {
        put_utc_timestamp_text(buf, v, false)
    }

    fn write_timestamptz(buf: &mut BytesMut, v: DateTime<Utc>) -> Result<()> {
        put_utc_timestamp_text(buf, v, true)
    }

    fn write_time(buf: &mut BytesMut, v: NaiveTime) -> Result<()> {
        put_time_text(buf, &v, false)
    }

    fn write_date(buf: &mut BytesMut, v: NaiveDate) -> Result<()> {
        let is_ad = put_only_date_text(buf, &v)?;
        put_year_ce_text(buf, is_ad)
    }
}

#[derive(Debug)]
pub(crate) struct BinaryWriter;

macro_rules! put_to_sql {
    ($buf:ident, $pgtype:ident, $v:ident) => {
        match $v.to_sql(&PgType::$pgtype, $buf).map_err(|e| {
            PgReprError::InternalError(format!(
                "cannot encode value={:?} as {}: {e}",
                $v,
                &PgType::$pgtype,
            ))
        })? {
            IsNull::Yes => unreachable!("nulls should not be encoded here"),
            _ => Ok(()),
        }
    };
}

impl Writer for BinaryWriter {
    fn write_bool(buf: &mut BytesMut, v: bool) -> Result<()> {
        put_to_sql!(buf, BOOL, v)
    }

    fn write_int2(buf: &mut BytesMut, v: i16) -> Result<()> {
        put_to_sql!(buf, INT2, v)
    }

    fn write_int4(buf: &mut BytesMut, v: i32) -> Result<()> {
        put_to_sql!(buf, INT4, v)
    }

    fn write_int8(buf: &mut BytesMut, v: i64) -> Result<()> {
        put_to_sql!(buf, INT8, v)
    }

    fn write_float4(buf: &mut BytesMut, v: f32) -> Result<()> {
        put_to_sql!(buf, FLOAT4, v)
    }

    fn write_float8(buf: &mut BytesMut, v: f64) -> Result<()> {
        put_to_sql!(buf, FLOAT8, v)
    }

    fn write_text(buf: &mut BytesMut, v: String) -> Result<()> {
        put_to_sql!(buf, TEXT, v)
    }

    fn write_bytea(buf: &mut BytesMut, v: Vec<u8>) -> Result<()> {
        put_to_sql!(buf, BYTEA, v)
    }

    fn write_timestamp(buf: &mut BytesMut, v: NaiveDateTime) -> Result<()> {
        put_to_sql!(buf, TIMESTAMP, v)
    }

    fn write_timestamptz(buf: &mut BytesMut, v: DateTime<Utc>) -> Result<()> {
        put_to_sql!(buf, TIMESTAMPTZ, v)
    }

    fn write_time(buf: &mut BytesMut, v: NaiveTime) -> Result<()> {
        put_to_sql!(buf, TIME, v)
    }

    fn write_date(buf: &mut BytesMut, v: NaiveDate) -> Result<()> {
        put_to_sql!(buf, DATE, v)
    }
}

fn put_float_text<F>(buf: &mut BytesMut, v: F) -> Result<()>
where
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

    // Creating this buffer is very cheap, nothing to worry.
    let mut dtoa_buf = DtoaBuffer::new();
    let mut v = dtoa_buf.format_finite(v);
    if let Some(s) = v.strip_suffix(".0") {
        v = s;
    }
    let mut iter = v.chars().peekable();
    while let Some(c) = iter.next() {
        buf.put_u8(c as u8);
        if c == 'e' && iter.peek() != Some(&'-') {
            buf.put_u8(b'+');
        }
    }
    Ok(())
}

trait TimeLike: Timelike {
    fn timestamp_subsec_micros(&self) -> u32;
}

macro_rules! impl_datetimelike_for {
    ($t:ty) => {
        impl TimeLike for $t {
            fn timestamp_subsec_micros(&self) -> u32 {
                self.timestamp_subsec_micros()
            }
        }
    };
}

impl_datetimelike_for! {NaiveDateTime}
impl_datetimelike_for! {DateTime<Utc>}

impl TimeLike for NaiveTime {
    fn timestamp_subsec_micros(&self) -> u32 {
        self.nanosecond() / 1_000
    }
}

fn put_micros_text<T>(buf: &mut BytesMut, v: &T) -> Result<()>
where
    T: Timelike,
{
    // Remove the trailing zeros from microseconds.
    let nanos = v.nanosecond();
    let mut micros = nanos / 1_000;
    if nanos % 1_000 >= 500 {
        micros += 1;
    }
    if micros > 0 {
        let mut width = 6;
        while micros % 10 == 0 {
            width -= 1;
            micros /= 10;
        }
        put_fmt!(buf, ".{micros:0width$}")?;
    }
    Ok(())
}

fn put_year_ce_text(buf: &mut BytesMut, is_ad: bool) -> Result<()> {
    if !is_ad {
        put_fmt!(buf, " BC")?;
    }
    Ok(())
}

/// Returns true if year is AD else false.
fn put_only_date_text<T>(buf: &mut BytesMut, v: &T) -> Result<bool>
where
    T: Datelike,
{
    let (is_ad, year) = v.year_ce();
    let (month, day) = (v.month(), v.day());
    put_fmt!(buf, "{year}-{month:02}-{day:02}")?;
    Ok(is_ad)
}

fn put_time_text<T>(buf: &mut BytesMut, v: &T, tz: bool) -> Result<()>
where
    T: TimeLike,
{
    let (hour, minute, second) = (v.hour(), v.minute(), v.second());
    put_fmt!(buf, "{hour:02}:{minute:02}:{second:02}")?;
    put_micros_text(buf, v)?;
    if tz {
        put_fmt!(buf, "+00")?;
    }
    Ok(())
}

fn put_utc_timestamp_text<T>(buf: &mut BytesMut, v: T, tz: bool) -> Result<()>
where
    T: TimeLike + Datelike,
{
    let is_ad = put_only_date_text(buf, &v)?;
    // Add a space between date and time.
    buf.put_u8(b' ');
    put_time_text(buf, &v, tz)?;
    put_year_ce_text(buf, is_ad)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::writer::{BinaryWriter, TextWriter, Writer};
    use bytes::BytesMut;
    use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};

    fn assert_buf(buf: &BytesMut, val: &[u8]) {
        let slice = buf.as_ref();
        assert_eq!(
            val,
            &slice[0..buf.len()],
            "\nExpected: {}\nGot: {}",
            &String::from_utf8_lossy(val),
            &String::from_utf8_lossy(slice)
        );
        assert_eq!(buf.len(), val.len());
    }

    #[test]
    fn test_text_writer() {
        type Writer = TextWriter;

        let mut buf = BytesMut::new();
        let buf = &mut buf;

        buf.clear();
        Writer::write_bool(buf, true).unwrap();
        assert_buf(buf, b"t");

        buf.clear();
        Writer::write_bool(buf, false).unwrap();
        assert_buf(buf, b"f");

        buf.clear();
        Writer::write_int2(buf, 1234).unwrap();
        assert_buf(buf, b"1234");

        buf.clear();
        Writer::write_int2(buf, -1234).unwrap();
        assert_buf(buf, b"-1234");

        buf.clear();
        Writer::write_int4(buf, 654321).unwrap();
        assert_buf(buf, b"654321");

        buf.clear();
        Writer::write_int4(buf, -654321).unwrap();
        assert_buf(buf, b"-654321");

        buf.clear();
        Writer::write_int8(buf, 1234567890).unwrap();
        assert_buf(buf, b"1234567890");

        buf.clear();
        Writer::write_int8(buf, -1234567890).unwrap();
        assert_buf(buf, b"-1234567890");

        buf.clear();
        Writer::write_float4(buf, 123.456).unwrap();
        assert_buf(buf, b"123.456");

        buf.clear();
        Writer::write_float4(buf, -123.456).unwrap();
        assert_buf(buf, b"-123.456");

        buf.clear();
        Writer::write_float4(buf, 0.000000001).unwrap();
        assert_buf(buf, b"1e-9");

        buf.clear();
        Writer::write_float4(buf, 1234000000000000000000.0).unwrap();
        assert_buf(buf, b"1.234e+21");

        buf.clear();
        Writer::write_float4(buf, f32::NAN).unwrap();
        assert_buf(buf, b"NaN");

        buf.clear();
        Writer::write_float4(buf, f32::INFINITY).unwrap();
        assert_buf(buf, b"Infinity");

        buf.clear();
        Writer::write_float4(buf, f32::NEG_INFINITY).unwrap();
        assert_buf(buf, b"-Infinity");

        buf.clear();
        Writer::write_float4(buf, -0.0).unwrap();
        assert_buf(buf, b"-0");

        buf.clear();
        Writer::write_float4(buf, 0.0).unwrap();
        assert_buf(buf, b"0");

        buf.clear();
        Writer::write_float8(buf, 123.0456789).unwrap();
        assert_buf(buf, b"123.0456789");

        buf.clear();
        Writer::write_float8(buf, 0.000000001).unwrap();
        assert_buf(buf, b"1e-9");

        buf.clear();
        Writer::write_float8(buf, 1234000000000000000000.0).unwrap();
        assert_buf(buf, b"1.234e+21");

        buf.clear();
        Writer::write_float8(buf, -123.0456789).unwrap();
        assert_buf(buf, b"-123.0456789");

        buf.clear();
        Writer::write_float8(buf, f64::NAN).unwrap();
        assert_buf(buf, b"NaN");

        buf.clear();
        Writer::write_float8(buf, f64::INFINITY).unwrap();
        assert_buf(buf, b"Infinity");

        buf.clear();
        Writer::write_float8(buf, f64::NEG_INFINITY).unwrap();
        assert_buf(buf, b"-Infinity");

        buf.clear();
        Writer::write_float8(buf, -0.0).unwrap();
        assert_buf(buf, b"-0");

        buf.clear();
        Writer::write_float8(buf, 0.0).unwrap();
        assert_buf(buf, b"0");

        buf.clear();
        Writer::write_text(buf, "abcdefghij".to_string()).unwrap();
        assert_buf(buf, b"abcdefghij");

        buf.clear();
        Writer::write_bytea(buf, [23, 13, 255, 0, 130].to_vec()).unwrap();
        assert_buf(buf, b"\\x170dff0082");

        buf.clear();
        let nt = NaiveDateTime::from_timestamp_opt(938689324, 0).unwrap();
        Writer::write_timestamp(buf, nt).unwrap();
        assert_buf(
            buf,
            format!("{}", nt.format("%Y-%m-%d %H:%M:%S")).as_bytes(),
        );

        buf.clear();
        let nt = NaiveDateTime::from_timestamp_opt(938689324, 123567).unwrap();
        Writer::write_timestamp(buf, nt).unwrap();
        assert_buf(
            buf,
            format!("{}.000124", nt.format("%Y-%m-%d %H:%M:%S")).as_bytes(),
        );

        buf.clear();
        let nt = NaiveDateTime::from_timestamp_opt(938689324, 123_400_000).unwrap();
        Writer::write_timestamp(buf, nt).unwrap();
        assert_buf(
            buf,
            format!("{}.1234", nt.format("%Y-%m-%d %H:%M:%S")).as_bytes(),
        );

        buf.clear();
        let nt = NaiveDateTime::from_timestamp_opt(-62143593684, 0).unwrap();
        Writer::write_timestamp(buf, nt).unwrap();
        assert_buf(
            buf,
            format!("1-{} BC", nt.format("%m-%d %H:%M:%S")).as_bytes(),
        );

        buf.clear();
        let dt = DateTime::<Utc>::from_utc(
            NaiveDateTime::from_timestamp_opt(938689324, 0).unwrap(),
            Utc,
        );
        Writer::write_timestamptz(buf, dt).unwrap();
        assert_buf(
            buf,
            format!("{}+00", dt.format("%Y-%m-%d %H:%M:%S")).as_bytes(),
        );

        buf.clear();
        let dt = DateTime::<Utc>::from_utc(
            NaiveDateTime::from_timestamp_opt(938689324, 123567).unwrap(),
            Utc,
        );
        Writer::write_timestamptz(buf, dt).unwrap();
        assert_buf(
            buf,
            format!("{}.000124+00", dt.format("%Y-%m-%d %H:%M:%S")).as_bytes(),
        );

        buf.clear();
        let dt = DateTime::<Utc>::from_utc(
            NaiveDateTime::from_timestamp_opt(938689324, 123_400_000).unwrap(),
            Utc,
        );
        Writer::write_timestamptz(buf, dt).unwrap();
        assert_buf(
            buf,
            format!("{}.1234+00", dt.format("%Y-%m-%d %H:%M:%S")).as_bytes(),
        );

        buf.clear();
        let dt = DateTime::<Utc>::from_utc(
            NaiveDateTime::from_timestamp_opt(-62143593684, 0).unwrap(),
            Utc,
        );
        Writer::write_timestamptz(buf, dt).unwrap();
        assert_buf(
            buf,
            format!("1-{}+00 BC", dt.format("%m-%d %H:%M:%S")).as_bytes(),
        );

        buf.clear();
        let nt = NaiveTime::from_hms_nano_opt(16, 32, 4, 0).unwrap();
        Writer::write_time(buf, nt).unwrap();
        assert_buf(buf, b"16:32:04");

        buf.clear();
        let nt = NaiveTime::from_hms_nano_opt(16, 32, 4, 123567).unwrap();
        Writer::write_time(buf, nt).unwrap();
        assert_buf(buf, b"16:32:04.000124");

        buf.clear();
        let nt = NaiveTime::from_hms_nano_opt(16, 32, 4, 123_400_000).unwrap();
        Writer::write_time(buf, nt).unwrap();
        assert_buf(buf, b"16:32:04.1234");

        buf.clear();
        let nd = NaiveDate::from_ymd_opt(1999, 9, 30).unwrap();
        Writer::write_date(buf, nd).unwrap();
        assert_buf(buf, b"1999-09-30");

        buf.clear();
        let nd = NaiveDate::from_ymd_opt(0, 9, 30).unwrap();
        Writer::write_date(buf, nd).unwrap();
        assert_buf(buf, b"1-09-30 BC");
    }

    #[test]
    fn test_binary_writer() {
        type Writer = BinaryWriter;

        let mut buf = BytesMut::new();
        let buf = &mut buf;

        buf.clear();
        Writer::write_bool(buf, true).unwrap();
        assert_buf(buf, &[1]);

        buf.clear();
        Writer::write_bool(buf, false).unwrap();
        assert_buf(buf, &[0]);

        buf.clear();
        Writer::write_int2(buf, 1234).unwrap();
        assert_buf(buf, 1234_i16.to_be_bytes().as_ref());

        buf.clear();
        Writer::write_int4(buf, 654321).unwrap();
        assert_buf(buf, 654321_i32.to_be_bytes().as_ref());

        buf.clear();
        Writer::write_int8(buf, 1234567890).unwrap();
        assert_buf(buf, 1234567890_i64.to_be_bytes().as_ref());

        buf.clear();
        Writer::write_float4(buf, 123.456).unwrap();
        assert_buf(buf, 123.456_f32.to_be_bytes().as_ref());

        buf.clear();
        Writer::write_float8(buf, 123.0456789).unwrap();
        assert_buf(buf, 123.0456789_f64.to_be_bytes().as_ref());

        buf.clear();
        Writer::write_text(buf, "abcdefghij".to_string()).unwrap();
        assert_buf(buf, b"abcdefghij");

        buf.clear();
        Writer::write_bytea(buf, [23, 13, 255, 0, 130].to_vec()).unwrap();
        assert_buf(buf, &[23, 13, 255, 0, 130]);

        buf.clear();
        let nt = NaiveDateTime::from_timestamp_opt(938689324, 123567).unwrap();
        Writer::write_timestamp(buf, nt).unwrap();
        // Microseconds since Jan 1, 2000
        assert_buf(buf, (-7_995_475_999_876_i64).to_be_bytes().as_ref());

        buf.clear();
        let dt = DateTime::<Utc>::from_utc(nt, Utc);
        Writer::write_timestamptz(buf, dt).unwrap();
        assert_buf(buf, (-7_995_475_999_876_i64).to_be_bytes().as_ref());

        buf.clear();
        let nt = NaiveTime::from_hms_micro_opt(16, 32, 4, 1234).unwrap();
        Writer::write_time(buf, nt).unwrap();
        // Microseconds since mid-night
        assert_buf(buf, 59_524_001_234_i64.to_be_bytes().as_ref());

        buf.clear();
        let nd = NaiveDate::from_ymd_opt(1999, 9, 30).unwrap();
        Writer::write_date(buf, nd).unwrap();
        // Days since Jan 1, 2000
        assert_buf(buf, (-93_i32).to_be_bytes().as_ref());
    }
}

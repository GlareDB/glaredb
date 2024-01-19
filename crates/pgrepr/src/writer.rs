use std::fmt::Display;

use bytes::BytesMut;
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use chrono_tz::Tz;
use decimal::Decimal128;
use repr::str::encode::*;
use tokio_postgres::types::{IsNull, ToSql, Type as PgType};

use crate::error::{PgReprError, Result};

/// Writer defines the interface for the different kinds of values that can be
/// encoded as a postgres type.
pub trait Writer {
    fn write_bool(buf: &mut BytesMut, v: bool) -> Result<()>;

    fn write_int2(buf: &mut BytesMut, v: i16) -> Result<()>;
    fn write_int4(buf: &mut BytesMut, v: i32) -> Result<()>;
    fn write_int8(buf: &mut BytesMut, v: i64) -> Result<()>;

    fn write_float4(buf: &mut BytesMut, v: f32) -> Result<()>;
    fn write_float8(buf: &mut BytesMut, v: f64) -> Result<()>;

    fn write_text(buf: &mut BytesMut, v: &str) -> Result<()>;
    fn write_bytea(buf: &mut BytesMut, v: &[u8]) -> Result<()>;

    fn write_timestamp(buf: &mut BytesMut, v: &NaiveDateTime) -> Result<()>;
    fn write_timestamptz(buf: &mut BytesMut, v: &DateTime<Tz>) -> Result<()>;
    fn write_time(buf: &mut BytesMut, v: &NaiveTime) -> Result<()>;
    fn write_date(buf: &mut BytesMut, v: &NaiveDate) -> Result<()>;

    fn write_decimal(buf: &mut BytesMut, v: &Decimal128) -> Result<()>;

    fn write_any<T: Display>(buf: &mut BytesMut, v: &T) -> Result<()> {
        encode_string(buf, v)?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct TextWriter;

impl Writer for TextWriter {
    fn write_bool(buf: &mut BytesMut, v: bool) -> Result<()> {
        encode_bool(buf, v)?;
        Ok(())
    }

    fn write_int2(buf: &mut BytesMut, v: i16) -> Result<()> {
        encode_int(buf, v)?;
        Ok(())
    }

    fn write_int4(buf: &mut BytesMut, v: i32) -> Result<()> {
        encode_int(buf, v)?;
        Ok(())
    }

    fn write_int8(buf: &mut BytesMut, v: i64) -> Result<()> {
        encode_int(buf, v)?;
        Ok(())
    }

    fn write_float4(buf: &mut BytesMut, v: f32) -> Result<()> {
        encode_float(buf, v)?;
        Ok(())
    }

    fn write_float8(buf: &mut BytesMut, v: f64) -> Result<()> {
        encode_float(buf, v)?;
        Ok(())
    }

    fn write_text(buf: &mut BytesMut, v: &str) -> Result<()> {
        encode_string(buf, v)?;
        Ok(())
    }

    fn write_bytea(buf: &mut BytesMut, v: &[u8]) -> Result<()> {
        encode_binary(buf, v)?;
        Ok(())
    }

    fn write_timestamp(buf: &mut BytesMut, v: &NaiveDateTime) -> Result<()> {
        encode_utc_timestamp(buf, v, false)?;
        Ok(())
    }

    fn write_timestamptz(buf: &mut BytesMut, v: &DateTime<Tz>) -> Result<()> {
        encode_utc_timestamp(buf, v, true)?;
        Ok(())
    }

    fn write_time(buf: &mut BytesMut, v: &NaiveTime) -> Result<()> {
        encode_time(buf, v, false)?;
        Ok(())
    }

    fn write_date(buf: &mut BytesMut, v: &NaiveDate) -> Result<()> {
        encode_date(buf, v)?;
        Ok(())
    }

    fn write_decimal(buf: &mut BytesMut, v: &Decimal128) -> Result<()> {
        encode_decimal(buf, v)?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct BinaryWriter;

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

    fn write_text(buf: &mut BytesMut, v: &str) -> Result<()> {
        put_to_sql!(buf, TEXT, v)
    }

    fn write_bytea(buf: &mut BytesMut, v: &[u8]) -> Result<()> {
        put_to_sql!(buf, BYTEA, v)
    }

    fn write_timestamp(buf: &mut BytesMut, v: &NaiveDateTime) -> Result<()> {
        put_to_sql!(buf, TIMESTAMP, v)
    }

    fn write_timestamptz(buf: &mut BytesMut, v: &DateTime<Tz>) -> Result<()> {
        let utc_date_time = DateTime::<Utc>::from_naive_utc_and_offset(v.naive_utc(), Utc);
        put_to_sql!(buf, TIMESTAMPTZ, utc_date_time)
    }

    fn write_time(buf: &mut BytesMut, v: &NaiveTime) -> Result<()> {
        put_to_sql!(buf, TIME, v)
    }

    fn write_date(buf: &mut BytesMut, v: &NaiveDate) -> Result<()> {
        put_to_sql!(buf, DATE, v)
    }

    fn write_decimal(_buf: &mut BytesMut, _v: &Decimal128) -> Result<()> {
        Err(PgReprError::InternalError(
            "cannot encode decimal (numeric) value into PG binary".to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use chrono_tz::UTC;

    use super::*;

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
        Writer::write_text(buf, "abcdefghij").unwrap();
        assert_buf(buf, b"abcdefghij");

        buf.clear();
        Writer::write_bytea(buf, &[23, 13, 255, 0, 130]).unwrap();
        assert_buf(buf, b"\\x170dff0082");

        buf.clear();
        let nt = NaiveDateTime::from_timestamp_opt(938689324, 0).unwrap();
        Writer::write_timestamp(buf, &nt).unwrap();
        assert_buf(
            buf,
            format!("{}", nt.format("%Y-%m-%d %H:%M:%S")).as_bytes(),
        );

        buf.clear();
        let nt = NaiveDateTime::from_timestamp_opt(938689324, 123567).unwrap();
        Writer::write_timestamp(buf, &nt).unwrap();
        assert_buf(
            buf,
            format!("{}.000124", nt.format("%Y-%m-%d %H:%M:%S")).as_bytes(),
        );

        buf.clear();
        let nt = NaiveDateTime::from_timestamp_opt(938689324, 123_400_000).unwrap();
        Writer::write_timestamp(buf, &nt).unwrap();
        assert_buf(
            buf,
            format!("{}.1234", nt.format("%Y-%m-%d %H:%M:%S")).as_bytes(),
        );

        buf.clear();
        let nt = NaiveDateTime::from_timestamp_opt(-197199051, 0).unwrap();
        Writer::write_timestamp(buf, &nt).unwrap();
        assert_buf(
            buf,
            format!("{}", nt.format("%Y-%m-%d %H:%M:%S")).as_bytes(),
        );

        buf.clear();
        let nt = NaiveDateTime::from_timestamp_opt(-62143593684, 0).unwrap();
        Writer::write_timestamp(buf, &nt).unwrap();
        assert_buf(
            buf,
            format!("1-{} BC", nt.format("%m-%d %H:%M:%S")).as_bytes(),
        );

        buf.clear();
        let dt = UTC.timestamp_opt(938689324, 0).unwrap();
        Writer::write_timestamptz(buf, &dt).unwrap();
        assert_buf(
            buf,
            format!("{}+00", dt.format("%Y-%m-%d %H:%M:%S")).as_bytes(),
        );

        buf.clear();
        let dt = UTC.timestamp_opt(938689324, 123567).unwrap();
        Writer::write_timestamptz(buf, &dt).unwrap();
        assert_buf(
            buf,
            format!("{}.000124+00", dt.format("%Y-%m-%d %H:%M:%S")).as_bytes(),
        );

        buf.clear();
        let dt = UTC.timestamp_opt(938689324, 123_400_000).unwrap();
        Writer::write_timestamptz(buf, &dt).unwrap();
        assert_buf(
            buf,
            format!("{}.1234+00", dt.format("%Y-%m-%d %H:%M:%S")).as_bytes(),
        );

        buf.clear();
        let dt = UTC.timestamp_opt(-197199051, 0).unwrap();
        Writer::write_timestamptz(buf, &dt).unwrap();
        assert_buf(
            buf,
            format!("{}+00", dt.format("%Y-%m-%d %H:%M:%S")).as_bytes(),
        );

        buf.clear();
        let dt = UTC.timestamp_opt(-62143593684, 0).unwrap();
        Writer::write_timestamptz(buf, &dt).unwrap();
        assert_buf(
            buf,
            format!("1-{}+00 BC", dt.format("%m-%d %H:%M:%S")).as_bytes(),
        );

        buf.clear();
        let nt = NaiveTime::from_hms_nano_opt(16, 32, 4, 0).unwrap();
        Writer::write_time(buf, &nt).unwrap();
        assert_buf(buf, b"16:32:04");

        buf.clear();
        let nt = NaiveTime::from_hms_nano_opt(16, 32, 4, 123567).unwrap();
        Writer::write_time(buf, &nt).unwrap();
        assert_buf(buf, b"16:32:04.000124");

        buf.clear();
        let nt = NaiveTime::from_hms_nano_opt(16, 32, 4, 123_400_000).unwrap();
        Writer::write_time(buf, &nt).unwrap();
        assert_buf(buf, b"16:32:04.1234");

        buf.clear();
        let nd = NaiveDate::from_ymd_opt(1999, 9, 30).unwrap();
        Writer::write_date(buf, &nd).unwrap();
        assert_buf(buf, b"1999-09-30");

        buf.clear();
        let nd = NaiveDate::from_ymd_opt(1963, 10, 2).unwrap();
        Writer::write_date(buf, &nd).unwrap();
        assert_buf(buf, b"1963-10-02");

        buf.clear();
        let nd = NaiveDate::from_ymd_opt(0, 9, 30).unwrap();
        Writer::write_date(buf, &nd).unwrap();
        assert_buf(buf, b"1-09-30 BC");

        buf.clear();
        let decimal = Decimal128::new(3950123456, 6).unwrap();
        Writer::write_decimal(buf, &decimal).unwrap();
        assert_buf(buf, b"3950.123456");
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
        Writer::write_text(buf, "abcdefghij").unwrap();
        assert_buf(buf, b"abcdefghij");

        buf.clear();
        Writer::write_bytea(buf, &[23, 13, 255, 0, 130]).unwrap();
        assert_buf(buf, &[23, 13, 255, 0, 130]);

        buf.clear();
        let nt = NaiveDateTime::from_timestamp_opt(938689324, 123567).unwrap();
        Writer::write_timestamp(buf, &nt).unwrap();
        // Microseconds since Jan 1, 2000
        assert_buf(buf, (-7_995_475_999_876_i64).to_be_bytes().as_ref());

        buf.clear();
        let dt = UTC.timestamp_opt(938689324, 123567).unwrap();
        Writer::write_timestamptz(buf, &dt).unwrap();
        assert_buf(buf, (-7_995_475_999_876_i64).to_be_bytes().as_ref());

        buf.clear();
        let nt = NaiveTime::from_hms_micro_opt(16, 32, 4, 1234).unwrap();
        Writer::write_time(buf, &nt).unwrap();
        // Microseconds since mid-night
        assert_buf(buf, 59_524_001_234_i64.to_be_bytes().as_ref());

        buf.clear();
        let nd = NaiveDate::from_ymd_opt(1999, 9, 30).unwrap();
        Writer::write_date(buf, &nd).unwrap();
        // Days since Jan 1, 2000
        assert_buf(buf, (-93_i32).to_be_bytes().as_ref());

        // buf.clear();
        // let decimal = Decimal128::new(3950123456, 6).unwrap();
        // Writer::write_decimal(buf, &decimal).unwrap();
        // assert_buf(buf, &[0, 3, 0, 0, 0, 0, 0, 6, 15, 110, 4, 210, 21, 224]);
    }
}

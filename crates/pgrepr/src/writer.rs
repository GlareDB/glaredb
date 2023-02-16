use crate::error::{PgReprError, Result};
use bytes::BytesMut;
use num_traits::Float as FloatTrait;
use std::fmt::{Display, Write};
use tokio_postgres::types::{IsNull, ToSql, Type as PgType};

macro_rules! put_fmt {
    ($dst:expr, $($arg:tt)*) => {
        write!($dst, $($arg)*).map_err(|e| e.into())
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
        put_float(buf, v)
    }

    fn write_float8(buf: &mut BytesMut, v: f64) -> Result<()> {
        put_float(buf, v)
    }

    fn write_text(buf: &mut BytesMut, v: String) -> Result<()> {
        put_fmt!(buf, "{v}")
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
        // Rust guarantees a bool to be 0 if false and 1 if true when casted to
        // an integer. See: https://doc.rust-lang.org/std/primitive.bool.html
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
}

fn put_float<F>(buf: &mut BytesMut, v: F) -> Result<()>
where
    F: Display + FloatTrait,
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

    // TODO: Postgres displays large exponents as "1e+10" and "1e-10". The
    // standard library prints all the decimals.
    put_fmt!(buf, "{v}")
}

#[cfg(test)]
mod tests {
    use crate::writer::{BinaryWriter, TextWriter, Writer};
    use bytes::BytesMut;

    fn assert_buf(buf: &BytesMut, val: &[u8]) {
        assert_eq!(buf.len(), val.len());
        let slice = buf.as_ref();
        assert_eq!(val, &slice[0..buf.len()]);
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
    }
}

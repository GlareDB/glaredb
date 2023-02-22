use crate::error::{PgReprError, Result};
use bytes::Bytes;
use num_traits::Float as FloatTrait;
use std::fmt;
use std::fmt::{Display, Write};
use std::str::FromStr;
use tokio_postgres::types::{IsNull, ToSql, Type as PgType};

/// Reader defines the interface for the different kinds of values that can be
/// decoded as a postgres type.
pub(crate) trait Reader {
    fn read_bool(buf: &[u8]) -> Result<bool>;

    fn read_int2(buf: &[u8]) -> Result<i16>;
    fn read_int4(buf: &[u8]) -> Result<i32>;
    fn read_int8(buf: &[u8]) -> Result<i64>;
    fn read_float4(buf: &[u8]) -> Result<f32>;
    fn read_float8(buf: &[u8]) -> Result<f64>;

    fn read_text(buf: &[u8]) -> Result<String>;
}

#[derive(Debug)]
pub(crate) struct TextReader;

impl TextReader {
    fn parse<E: std::error::Error + Sync + Send + 'static, F: FromStr<Err = E>>(
        buf: &[u8],
    ) -> Result<F> {
        Ok(std::str::from_utf8(buf)?
            .parse::<F>()
            .map_err(|e| PgReprError::ParseError(Box::new(e)))?)
    }
}

impl Reader for TextReader {
    fn read_bool(buf: &[u8]) -> Result<bool> {
        TextReader::parse::<_, SqlBool>(buf).map(|b| b.0)
    }

    fn read_int2(buf: &[u8]) -> Result<i16> {
        TextReader::parse(buf)
    }

    fn read_int4(buf: &[u8]) -> Result<i32> {
        TextReader::parse(buf)
    }

    fn read_int8(buf: &[u8]) -> Result<i64> {
        TextReader::parse(buf)
    }

    fn read_float4(buf: &[u8]) -> Result<f32> {
        TextReader::parse(buf)
    }

    fn read_float8(buf: &[u8]) -> Result<f64> {
        TextReader::parse(buf)
    }

    fn read_text(buf: &[u8]) -> Result<String> {
        TextReader::parse(buf) // TODO: Cstring?
    }
}

#[derive(Debug, thiserror::Error)]
#[error("String was not 't', 'true', 'f', or 'false'")]
struct ParseSqlBoolError;

struct SqlBool(bool);

impl FromStr for SqlBool {
    type Err = ParseSqlBoolError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "true" | "t" => Ok(SqlBool(true)),
            "false" | "f" => Ok(SqlBool(false)),
            _ => Err(ParseSqlBoolError),
        }
    }
}

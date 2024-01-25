use crate::error::{PgReprError, Result};
use std::str::FromStr;

/// Reader defines the interface for the different kinds of values that can be
/// decoded as a postgres type.
pub trait Reader {
    fn read_bool(buf: &[u8]) -> Result<bool>;

    fn read_int2(buf: &[u8]) -> Result<i16>;
    fn read_int4(buf: &[u8]) -> Result<i32>;
    fn read_int8(buf: &[u8]) -> Result<i64>;
    fn read_float4(buf: &[u8]) -> Result<f32>;
    fn read_float8(buf: &[u8]) -> Result<f64>;

    fn read_text(buf: &[u8]) -> Result<String>;
}

#[derive(Debug)]
pub struct TextReader;

impl TextReader {
    fn parse<E: std::error::Error + Sync + Send + 'static, F: FromStr<Err = E>>(
        buf: &[u8],
    ) -> Result<F> {
        std::str::from_utf8(buf)?
            .parse::<F>()
            .map_err(|e| PgReprError::ParseError(Box::new(e)))
    }
}

impl Reader for TextReader {
    fn read_bool(buf: &[u8]) -> Result<bool> {
        Self::parse::<_, SqlBool>(buf).map(|b| b.0)
    }

    fn read_int2(buf: &[u8]) -> Result<i16> {
        Self::parse(buf)
    }

    fn read_int4(buf: &[u8]) -> Result<i32> {
        Self::parse(buf)
    }

    fn read_int8(buf: &[u8]) -> Result<i64> {
        Self::parse(buf)
    }

    fn read_float4(buf: &[u8]) -> Result<f32> {
        Self::parse(buf)
    }

    fn read_float8(buf: &[u8]) -> Result<f64> {
        Self::parse(buf)
    }

    fn read_text(buf: &[u8]) -> Result<String> {
        Self::parse(buf)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_sql_bool() {
        let v = TextReader::read_bool("t".as_bytes()).unwrap();
        assert!(v);

        let v = TextReader::read_bool("true".as_bytes()).unwrap();
        assert!(v);

        let v = TextReader::read_bool("f".as_bytes()).unwrap();
        assert!(!v);

        let v = TextReader::read_bool("false".as_bytes()).unwrap();
        assert!(!v);

        let _ = TextReader::read_bool("none".as_bytes()).unwrap_err();
    }
}

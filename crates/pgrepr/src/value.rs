use crate::error::{internal, Result};
use crate::types::Type;
use postgres_types::FromSql;

/// A Postgres value
#[derive(Debug, Clone)]
pub enum Value {
    Float8(f64),
    Int8(i64),
    VarChar(String),
}

/// The formatting type for an encoded value.
#[derive(Debug, Clone, Copy)]
pub enum Format {
    Text,
    Binary,
}

impl Value {
    pub fn decode(format: Format, ty: Type, buf: &[u8]) -> Result<Self> {
        match format {
            Format::Binary => Self::decode_binary(ty, buf),
            Format::Text => Self::decode_text(ty, buf),
        }
    }

    fn decode_text(ty: Type, buf: &[u8]) -> Result<Self> {
        Ok(match ty {
            Type::Float8 => Value::Float8(parse_float8(buf)?),
            Type::Int8 => Value::Int8(parse_int8(buf)?),
            Type::VarChar => Value::VarChar(parse_varchar(buf)?),
        })
    }

    fn decode_binary(ty: Type, buf: &[u8]) -> Result<Self> {
        let res = match ty {
            Type::Float8 => f64::from_sql(ty.as_pg_type(), buf).map(Value::Float8),
            Type::Int8 => i64::from_sql(ty.as_pg_type(), buf).map(Value::Int8),
            Type::VarChar => String::from_sql(ty.as_pg_type(), buf).map(Value::VarChar),
        };

        match res {
            Ok(v) => Ok(v),
            Err(e) => Err(internal!("failed to decode binary value: {}", e)),
        }
    }
}

/// Parse a float8 from a string.
fn parse_float8(buf: &[u8]) -> Result<f64> {
    lexical_core::parse(buf).map_err(|e| internal!("failed to parse float8: {}", e))
}

/// Parse an int8 from a string.
fn parse_int8(buf: &[u8]) -> Result<i64> {
    lexical_core::parse(buf).map_err(|e| internal!("failed to parse int8: {}", e))
}

/// Parse a varchar from a string.
fn parse_varchar(buf: &[u8]) -> Result<String> {
    String::from_utf8(buf.to_vec()).map_err(|e| internal!("failed to parse varchar: {}", e))
}

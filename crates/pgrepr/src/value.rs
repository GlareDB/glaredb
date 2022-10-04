use crate::error::{internal, Result};
use crate::types::Type;
use postgres_types::FromSql;

/*
 * TODO:
 *
 * 1. Pass BindOpts between functions to reduce changes to function signatures
 * 2. Add support for int8 text
 * 3. Add support for int8 Binary
 * 4. Add support for varchar text
 * 5. Add support for varchar Binary
 * 6. Add support for float8 text
 * 7. Add support for float8 Binary
 */

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
            other => unimplemented!("decode text value for {:?}", other),
        })
    }

    fn decode_binary(ty: Type, buf: &[u8]) -> Result<Self> {
        let res = match ty {
            Type::Float8 => f64::from_sql(ty.as_pg_type(), buf).map(Value::Float8),
            other => unimplemented!("decode binary value for {:?}", other),
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

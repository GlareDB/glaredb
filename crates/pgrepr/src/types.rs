use crate::value::Value;
use postgres_types::Type as PgType;

#[derive(Debug, thiserror::Error)]
pub enum TypeError {
    #[error("cannot convert value type to pg type: {0:?}")]
    NonconvertibleType(Value),

    #[error("invalid oid: {0}")]
    InvalidOid(i32),
}

/// A Postgres type.
#[derive(Clone, Copy, Debug)]
pub enum Type {
    Int8,
    Float8,
    VarChar,
}

impl Type {
    pub fn oid(&self) -> i32 {
        self.as_pg_type().oid() as i32 // TODO: Why does `postgres_types` return a u32?
    }

    /// Return the type modifier.
    pub fn type_mod(&self) -> i32 {
        -1 // TODO: Actually get the modifer, -1 (no modifier) works fine for now.
    }

    /// Returns the size in bytes.
    ///
    /// Variable length types return a negative value (as per the Postgres
    /// frontend/backend protocol).
    pub fn type_size(&self) -> i16 {
        match self {
            Type::Int8 => 8,
            Type::Float8 => 8,
            Type::VarChar => -1,
        }
    }

    pub(crate) fn as_pg_type(&self) -> &'static PgType {
        match self {
            Type::Int8 => &PgType::INT8,
            Type::Float8 => &PgType::FLOAT8,
            Type::VarChar => &PgType::VARCHAR,
        }
    }

    /// Convert a Postgres type integer to a `Type`.
    /// To see the types look at the Postgres catalog https://github.com/postgres/postgres/blob/27b77ecf9f4d5be211900eda54d8155ada50d696/src/include/catalog/pg_type.dat
    pub fn from_oid(oid: i32) -> Result<Self, TypeError> {
        match oid {
            20 => Ok(Type::Int8),
            701 => Ok(Type::Float8),
            1043 => Ok(Type::VarChar),
            _ => Err(TypeError::InvalidOid(oid)),
        }
    }
}

impl TryFrom<Value> for Type {
    type Error = TypeError;
    fn try_from(value: Value) -> Result<Self, Self::Error> {
        Ok(match value {
            Value::Int8(_) => Type::Int8,
            Value::Float8(_) => Type::Float8,
            Value::VarChar(_) => Type::VarChar,
        })
    }
}

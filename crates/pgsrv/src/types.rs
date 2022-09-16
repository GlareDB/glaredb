use lemur::repr::value::ValueType;
use postgres_types::Type as PgType;

#[derive(Debug, thiserror::Error)]
pub enum TypeError {
    #[error("cannot convert value type to pg type: {0:?}")]
    NonconvertibleType(ValueType),
}

#[derive(Debug)]
pub enum Type {
    Bool,
    Int2,
    Int4,
    Float4,
    Text,
    Bytea,
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
            Type::Bool => 1,
            Type::Int2 => 2,
            Type::Int4 => 4,
            Type::Float4 => 4,
            Type::Text => -1,
            Type::Bytea => -1,
        }
    }

    fn as_pg_type(&self) -> &'static PgType {
        match self {
            Type::Bool => &PgType::BOOL,
            Type::Int2 => &PgType::INT2,
            Type::Int4 => &PgType::INT4,
            Type::Float4 => &PgType::FLOAT4,
            Type::Text => &PgType::TEXT,
            Type::Bytea => &PgType::BYTEA,
        }
    }
}

impl TryFrom<ValueType> for Type {
    type Error = TypeError;
    fn try_from(value: ValueType) -> Result<Self, Self::Error> {
        Ok(match value {
            ValueType::Bool => Type::Bool,
            ValueType::Int8 => Type::Int2,
            ValueType::Int32 => Type::Int4,
            ValueType::Float32 => Type::Float4,
            ValueType::Utf8 => Type::Text,
            ValueType::Binary => Type::Bytea,
            other => return Err(TypeError::NonconvertibleType(other)),
        })
    }
}

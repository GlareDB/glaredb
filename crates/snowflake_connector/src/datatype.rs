use std::{fmt::Display, str::FromStr};

use serde::{de::Visitor, Deserialize, Serialize};

use crate::errors::SnowflakeError;

#[derive(Debug, Clone, Copy)]
pub enum SnowflakeDataType {
    Any,
    Array,
    Binary,
    Boolean,
    Char,
    Date,
    Fixed,
    Number,
    Object,
    Real,
    Text,
    Time,
    Timestamp,
    TimestampLtz,
    TimestampNtz,
    TimestampTz,
    Variant,
}

impl SnowflakeDataType {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Any => "ANY",
            Self::Array => "ARRAY",
            Self::Binary => "BINARY",
            Self::Boolean => "BOOLEAN",
            Self::Char => "CHAR",
            Self::Date => "DATE",
            Self::Fixed => "FIXED",
            Self::Number => "NUMBER",
            Self::Object => "OBJECT",
            Self::Real => "REAL",
            Self::Text => "TEXT",
            Self::Time => "TIME",
            Self::Timestamp => "TIMESTAMP",
            Self::TimestampLtz => "TIMESTAMP_LTZ",
            Self::TimestampNtz => "TIMESTAMP_NTZ",
            Self::TimestampTz => "TIMESTAMP_TZ",
            Self::Variant => "VARIANT",
        }
    }
}

impl FromStr for SnowflakeDataType {
    type Err = SnowflakeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let ty = match s {
            "any" | "ANY" => Self::Any,
            "array" | "ARRAY" => Self::Array,
            "binary" | "BINARY" => Self::Binary,
            "boolean" | "BOOLEAN" => Self::Boolean,
            "char" | "CHAR" => Self::Char,
            "date" | "DATE" => Self::Date,
            "fixed" | "FIXED" => Self::Fixed,
            "number" | "NUMBER" => Self::Number,
            "object" | "OBJECT" => Self::Object,
            "real" | "REAL" => Self::Real,
            "text" | "TEXT" => Self::Text,
            "time" | "TIME" => Self::Time,
            "timestamp" | "TIMESTAMP" => Self::Timestamp,
            "timestamp_ltz" | "TIMESTAMP_LTZ" => Self::TimestampLtz,
            "timestamp_ntz" | "TIMESTAMP_NTZ" => Self::TimestampNtz,
            "timestamp_tz" | "TIMESTAMP_TZ" => Self::TimestampTz,
            "variant" | "VARIANT" => Self::Variant,
            s => return Err(SnowflakeError::InvalidSnowflakeDataType(s.to_owned())),
        };
        Ok(ty)
    }
}

impl Display for SnowflakeDataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl Serialize for SnowflakeDataType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

struct SnowflakeDataTypeVisitor;

impl<'de> Visitor<'de> for SnowflakeDataTypeVisitor {
    type Value = SnowflakeDataType;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("snowflake data type")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        v.parse()
            .map_err(|_e| serde::de::Error::invalid_value(serde::de::Unexpected::Str(v), &self))
    }
}

impl<'de> Deserialize<'de> for SnowflakeDataType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_str(SnowflakeDataTypeVisitor)
    }
}

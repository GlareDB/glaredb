use std::str::FromStr;
use std::sync::Arc;

use datafusion::arrow::datatypes::{
    DataType,
    Field as ArrowField,
    Schema as ArrowSchema,
    TimeUnit,
};
use once_cell::sync::Lazy;
use regex::Regex;
use serde::{de, Deserialize, Deserializer};

use crate::lake::iceberg::errors::{IcebergError, Result};

/// Primitive types supported in iceberg tables.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrimitiveType {
    Boolean,
    Int,
    Long,
    Float,
    Double,
    Decimal { p: u8, s: u8 },
    Date,
    Time,
    Timestamp,
    Timestamptz,
    String,
    Uuid,
    Fixed(usize),
    Binary,
}

impl TryFrom<PrimitiveType> for DataType {
    type Error = IcebergError;

    fn try_from(value: PrimitiveType) -> Result<Self> {
        Ok(match value {
            PrimitiveType::Boolean => DataType::Boolean,
            PrimitiveType::Int => DataType::Int32,
            PrimitiveType::Long => DataType::Int64,
            PrimitiveType::Float => DataType::Float32,
            PrimitiveType::Double => DataType::Float64,
            PrimitiveType::Decimal { p, s } => DataType::Decimal128(p, s as i8),
            PrimitiveType::Date => DataType::Date32,
            PrimitiveType::Time => DataType::Timestamp(TimeUnit::Microsecond, None), /* TODO: Possibly `Time32` instead? */
            PrimitiveType::Timestamp => DataType::Timestamp(TimeUnit::Microsecond, None),
            PrimitiveType::Timestamptz => DataType::Timestamp(TimeUnit::Microsecond, None),
            PrimitiveType::String => DataType::Utf8,
            PrimitiveType::Uuid => DataType::Utf8,
            PrimitiveType::Fixed(l) => DataType::FixedSizeBinary(l as i32),
            PrimitiveType::Binary => DataType::Binary,
        })
    }
}

impl<'de> Deserialize<'de> for PrimitiveType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s: &str = Deserialize::deserialize(deserializer)?;
        PrimitiveType::from_str(s).map_err(de::Error::custom)
    }
}

impl FromStr for PrimitiveType {
    type Err = IcebergError;

    fn from_str(s: &str) -> Result<Self> {
        Ok(match s {
            "boolean" => PrimitiveType::Boolean,
            "int" => PrimitiveType::Int,
            "long" => PrimitiveType::Long,
            "float" => PrimitiveType::Float,
            "double" => PrimitiveType::Double,
            "date" => PrimitiveType::Date,
            "time" => PrimitiveType::Time,
            "timestamp" => PrimitiveType::Timestamp,
            "timestamptz" => PrimitiveType::Timestamptz,
            "string" => PrimitiveType::String,
            "uuid" => PrimitiveType::Uuid,
            "binary" => PrimitiveType::Binary,
            other if other.starts_with("decimal") => {
                // Regex that matches:
                // decimal(15, 2)
                // decimal(15,2)
                static DECIMAL_RE: Lazy<Regex> =
                    Lazy::new(|| Regex::new(r"^decimal\((?P<p>\d+),\s?(?P<s>\d+)\)$").unwrap());

                let captures = DECIMAL_RE.captures(other).ok_or_else(|| {
                    IcebergError::DataInvalid(format!("Invalid decimal type: {other}"))
                })?;

                let p: u8 = captures
                    .name("p")
                    .ok_or_else(|| {
                        IcebergError::DataInvalid(format!("Invalid decimal type: {other}"))
                    })?
                    .as_str()
                    .parse()
                    .map_err(|e| {
                        IcebergError::DataInvalid(format!("Decimal precision not a u8: {e}"))
                    })?;

                let s: u8 = captures
                    .name("s")
                    .ok_or_else(|| {
                        IcebergError::DataInvalid(format!("Invalid decimal type: {other}"))
                    })?
                    .as_str()
                    .parse()
                    .map_err(|e| {
                        IcebergError::DataInvalid(format!("Decimal scale not a u8: {e}"))
                    })?;

                PrimitiveType::Decimal { p, s }
            }
            other if other.starts_with("fixed") => {
                // Regex that matches:
                // fixed[16]
                static FIXED_RE: Lazy<Regex> =
                    Lazy::new(|| Regex::new(r"^fixed\[(?P<l>\d+)\]$").unwrap());

                let captures = FIXED_RE.captures(other).ok_or_else(|| {
                    IcebergError::DataInvalid(format!("Invalid fixed type: {other}"))
                })?;

                let l: usize = captures
                    .name("l")
                    .ok_or_else(|| {
                        IcebergError::DataInvalid(format!("Invalid fixed type: {other}"))
                    })?
                    .as_str()
                    .parse()
                    .map_err(|e| {
                        IcebergError::DataInvalid(format!("Fixed length not a usize: {e}"))
                    })?;

                PrimitiveType::Fixed(l)
            }
            other => return Err(IcebergError::DataInvalid(format!("Invalid type: {other}"))),
        })
    }
}

/// Union between primitive and nested types.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum AnyType {
    Primitive(PrimitiveType),
    List(ListType),
    Struct(StructType),
    Map(MapType),
}

impl TryFrom<&AnyType> for DataType {
    type Error = IcebergError;

    fn try_from(value: &AnyType) -> Result<Self> {
        Ok(match value {
            AnyType::Primitive(t) => (*t).try_into()?,
            AnyType::List(t) => t.try_into()?,
            AnyType::Struct(t) => t.try_into()?,
            AnyType::Map(t) => t.try_into()?,
        })
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
#[serde(tag = "type", rename = "list")]
pub struct ListType {
    pub element_id: i32,
    pub element_required: bool,
    pub element: Box<AnyType>,
}

impl TryFrom<&ListType> for DataType {
    type Error = IcebergError;

    fn try_from(value: &ListType) -> Result<Self> {
        let field = ArrowField::new(
            "item",
            value.element.as_ref().try_into()?,
            !value.element_required,
        );
        Ok(DataType::List(Arc::new(field)))
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
#[serde(tag = "type", rename = "map")]
pub struct MapType {
    pub key_id: i32,
    pub key: Box<AnyType>,
    pub value_id: i32,
    pub value_required: bool,
    pub value: Box<AnyType>,
}

impl TryFrom<&MapType> for DataType {
    type Error = IcebergError;

    fn try_from(value: &MapType) -> Result<Self> {
        let key_field = ArrowField::new("key", value.key.as_ref().try_into()?, false);
        let val_field = ArrowField::new(
            "value",
            value.value.as_ref().try_into()?,
            value.value_required,
        );
        let field = ArrowField::new_struct("entryies", vec![key_field, val_field], false);

        let typ = DataType::Map(Arc::new(field), false);

        Ok(typ)
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
#[serde(tag = "type", rename = "struct")]
pub struct StructType {
    pub fields: Vec<StructField>,
}

impl TryFrom<&StructType> for DataType {
    type Error = IcebergError;

    fn try_from(value: &StructType) -> Result<Self> {
        let fields = value
            .fields
            .iter()
            .map(|f| {
                let typ = &f.r#type;
                Ok(ArrowField::new(&f.name, typ.try_into()?, !f.required))
            })
            .collect::<Result<Vec<_>>>()?;

        let typ = DataType::Struct(fields.into());
        Ok(typ)
    }
}

/// Fields on a struct.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub struct StructField {
    pub id: i32,
    pub name: String,
    pub required: bool,
    pub r#type: AnyType,
    pub doc: Option<String>,
    /// JSON serialized initial value for the field.
    pub initial_default: Option<String>, // TODO
    /// JSON serialized write default value for the field.
    pub write_default: Option<String>, // TODO
}

impl StructField {
    pub fn to_arrow_field(&self) -> Result<ArrowField> {
        let typ = &self.r#type;
        Ok(ArrowField::new(&self.name, typ.try_into()?, !self.required))
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Schema {
    pub schema_id: i32,
    pub identifier_field_ids: Option<Vec<i32>>,
    pub fields: Vec<StructField>,
}

impl Schema {
    pub fn to_arrow_schema(&self) -> Result<ArrowSchema> {
        let fields = self
            .fields
            .iter()
            .map(|f| f.to_arrow_field())
            .collect::<Result<Vec<_>>>()?;
        Ok(ArrowSchema::new(fields))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deserialize_long_field() {
        let json = r#"
            {
              "id" : 1,
              "name" : "l_orderkey",
              "required" : false,
              "type" : "long"
            }"#;

        let deserialized: StructField = serde_json::from_str(json).unwrap();
        let expected = StructField {
            id: 1,
            name: "l_orderkey".to_string(),
            required: false,
            r#type: AnyType::Primitive(PrimitiveType::Long),
            doc: None,
            initial_default: None,
            write_default: None,
        };
        assert_eq!(expected, deserialized);
    }

    #[test]
    fn test_deserialize_decimal_field() {
        let json = r#"
            {
              "id" : 6,
              "name" : "l_extendedprice",
              "required" : false,
              "type" : "decimal(15, 2)"
            }"#;

        let deserialized: StructField = serde_json::from_str(json).unwrap();
        let expected = StructField {
            id: 6,
            name: "l_extendedprice".to_string(),
            required: false,
            r#type: AnyType::Primitive(PrimitiveType::Decimal { p: 15, s: 2 }),
            doc: None,
            initial_default: None,
            write_default: None,
        };
        assert_eq!(expected, deserialized);
    }

    #[test]
    fn test_deserialize_decimal_no_space_field() {
        // See lack of space after comma in decimal type.
        let json = r#"
            {
              "id" : 6,
              "name" : "l_extendedprice",
              "required" : false,
              "type" : "decimal(15,2)"
            }"#;

        let deserialized: StructField = serde_json::from_str(json).unwrap();
        let expected = StructField {
            id: 6,
            name: "l_extendedprice".to_string(),
            required: false,
            r#type: AnyType::Primitive(PrimitiveType::Decimal { p: 15, s: 2 }),
            doc: None,
            initial_default: None,
            write_default: None,
        };
        assert_eq!(expected, deserialized);
    }

    #[test]
    fn test_deserialize_list_field() {
        let json = r#"
            {
              "id" : 6,
              "name" : "l_extendedprice",
              "required" : false,
              "type" : {
                "type": "list",
                "element-id": 3,
                "element-required": true,
                "element": "string"
              }
            }"#;

        let deserialized: StructField = serde_json::from_str(json).unwrap();
        let expected = StructField {
            id: 6,
            name: "l_extendedprice".to_string(),
            required: false,
            r#type: AnyType::List(ListType {
                element_required: true,
                element_id: 3,
                element: Box::new(AnyType::Primitive(PrimitiveType::String)),
            }),
            doc: None,
            initial_default: None,
            write_default: None,
        };
        assert_eq!(expected, deserialized);
    }

    #[test]
    fn test_deserialize_struct_field() {
        let json = r#"
            {
              "id" : 6,
              "name" : "l_extendedprice",
              "required" : false,
              "type" : {
                "type": "struct",
                "fields": [ {
                  "id": 1,
                  "name": "id",
                  "required": true,
                  "type": "uuid",
                  "initial-default": "0db3e2a8-9d1d-42b9-aa7b-74ebe558dceb",
                  "write-default": "ec5911be-b0a7-458c-8438-c9a3e53cffae"
                }, {
                  "id": 2,
                  "name": "data",
                  "required": false,
                  "type": "string"
                } ]
              }
            }"#;

        let deserialized: StructField = serde_json::from_str(json).unwrap();
        let expected = StructField {
            id: 6,
            name: "l_extendedprice".to_string(),
            required: false,
            r#type: AnyType::Struct(StructType {
                fields: vec![
                    StructField {
                        id: 1,
                        name: "id".to_string(),
                        required: true,
                        r#type: AnyType::Primitive(PrimitiveType::Uuid),
                        initial_default: Some("0db3e2a8-9d1d-42b9-aa7b-74ebe558dceb".to_string()),
                        write_default: Some("ec5911be-b0a7-458c-8438-c9a3e53cffae".to_string()),
                        doc: None,
                    },
                    StructField {
                        id: 2,
                        name: "data".to_string(),
                        required: false,
                        r#type: AnyType::Primitive(PrimitiveType::String),
                        initial_default: None,
                        write_default: None,
                        doc: None,
                    },
                ],
            }),
            doc: None,
            initial_default: None,
            write_default: None,
        };
        assert_eq!(expected, deserialized);
    }

    #[test]
    fn test_deserialize_map_field() {
        let json = r#"
            {
              "id" : 6,
              "name" : "l_extendedprice",
              "required" : false,
              "type" : {
                "type": "map",
                "key-id": 4,
                "key": "string",
                "value-id": 5,
                "value-required": false,
                "value": "double"
              }
            }"#;

        let deserialized: StructField = serde_json::from_str(json).unwrap();
        let expected = StructField {
            id: 6,
            name: "l_extendedprice".to_string(),
            required: false,
            r#type: AnyType::Map(MapType {
                key_id: 4,
                key: Box::new(AnyType::Primitive(PrimitiveType::String)),
                value: Box::new(AnyType::Primitive(PrimitiveType::Double)),
                value_id: 5,
                value_required: false,
            }),
            doc: None,
            initial_default: None,
            write_default: None,
        };
        assert_eq!(expected, deserialized);
    }
}

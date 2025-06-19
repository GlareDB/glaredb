use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged, rename_all = "camelCase")]
pub enum SchemaType {
    Primitive(PrimitiveType),
    Struct(Box<StructType>),
    Array(Box<ArrayType>),
    Map(Box<MapType>),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StructType {
    pub fields: Vec<StructField>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ArrayType {
    pub element_type: SchemaType,
    pub contains_null: bool,
}

// TODO: Spec example contains a 'valueContainsNull' field, but it's not
// actually written in the spec. Determine if that's a field we can rely on.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MapType {
    pub key_type: SchemaType,
    pub value_type: SchemaType,
}

// TODO: Timestamp timezone
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum PrimitiveType {
    String,
    Long,
    Integer,
    Short,
    Byte,
    Float,
    Double,
    Decimal,
    Boolean,
    Binary,
    Date,
    Timestamp,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StructField {
    pub name: String,
    #[serde(rename = "type")]
    pub typ: SchemaType,
    pub nullable: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn struct_field_primitive() {
        let input = r#"
        {
          "name" : "d",
          "type" : "integer",
          "nullable" : false,
          "metadata" : { }
        }
        "#;

        let field: StructField = serde_json::from_str(input).unwrap();
        let expected = StructField {
            name: "d".to_string(),
            typ: SchemaType::Primitive(PrimitiveType::Integer),
            nullable: false,
        };

        assert_eq!(expected, field);
    }

    #[test]
    fn struct_field_array() {
        let input = r#"
        {
          "name" : "c",
          "type" : {
            "type" : "array",
            "elementType" : "integer",
            "containsNull" : false
          },
          "nullable" : true,
          "metadata" : { }
        }
        "#;

        let field: StructField = serde_json::from_str(input).unwrap();
        let expected = StructField {
            name: "c".to_string(),
            typ: SchemaType::Array(Box::new(ArrayType {
                element_type: SchemaType::Primitive(PrimitiveType::Integer),
                contains_null: false,
            })),
            nullable: true,
        };

        assert_eq!(expected, field);
    }

    #[test]
    fn struct_field_map() {
        let input = r#"
        {
          "name" : "f",
          "type" : {
            "type" : "map",
            "keyType" : "string",
            "valueType" : "string",
            "valueContainsNull" : true
          },
          "nullable" : true,
          "metadata" : { }
        }
        "#;

        let field: StructField = serde_json::from_str(input).unwrap();
        let expected = StructField {
            name: "f".to_string(),
            typ: SchemaType::Map(Box::new(MapType {
                key_type: SchemaType::Primitive(PrimitiveType::String),
                value_type: SchemaType::Primitive(PrimitiveType::String),
            })),
            nullable: true,
        };

        assert_eq!(expected, field);
    }
}

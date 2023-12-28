use std::sync::Arc;

use datafusion::arrow::{
    array::{ArrayRef, BooleanBuilder, Float64Builder, Int64Builder, StringBuilder, UInt64Builder},
    datatypes::DataType,
    error::ArrowError,
};
use serde_json::Value;

pub fn create_child_builder(
    dt: &DataType,
    capacity: usize,
) -> Result<ArrayBuilderVariant, ArrowError> {
    let builder = match dt {
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            ArrayBuilderVariant::Int64(Int64Builder::with_capacity(capacity))
        }
        DataType::Float16 | DataType::Float32 | DataType::Float64 => {
            ArrayBuilderVariant::Float64(Float64Builder::with_capacity(capacity))
        }
        DataType::Utf8 => {
            ArrayBuilderVariant::String(StringBuilder::with_capacity(capacity, capacity * 16))
        }
        DataType::UInt16 | DataType::UInt8 | DataType::UInt32 | DataType::UInt64 => {
            ArrayBuilderVariant::UInt64(UInt64Builder::with_capacity(capacity))
        }
        DataType::Boolean => ArrayBuilderVariant::Boolean(BooleanBuilder::with_capacity(capacity)),
        // DataType::Struct(fields) => ArrayBuilderVariant::StructBuilder(
        //     StructBuilder::from_fields(fields.clone(), capacity),
        //     fields.clone(),
        // ),
        _ => {
            return Err(ArrowError::JsonError(
                "This type is not supported yet".to_string(),
            ))
        }
    };
    Ok(builder)
}

pub enum ArrayBuilderVariant {
    Int64(Int64Builder),
    Float64(Float64Builder),
    String(StringBuilder),
    UInt64(UInt64Builder),
    Boolean(BooleanBuilder),
    // StructBuilder(StructBuilder, Fields),
}

impl ArrayBuilderVariant {
    pub fn append_value(&mut self, value: &Value) -> Result<(), ArrowError> {
        match self {
            ArrayBuilderVariant::Int64(builder) => {
                if let Some(val) = value.as_i64() {
                    builder.append_value(val);
                } else {
                    builder.append_null();
                }
            }
            ArrayBuilderVariant::Float64(builder) => {
                if let Some(val) = value.as_f64() {
                    builder.append_value(val);
                } else {
                    builder.append_null();
                }
            }
            ArrayBuilderVariant::String(builder) => {
                if let Some(val) = value.as_str() {
                    builder.append_value(val);
                } else {
                    builder.append_null();
                }
            }
            ArrayBuilderVariant::UInt64(builder) => {
                if let Some(val) = value.as_u64() {
                    builder.append_value(val);
                } else {
                    builder.append_null();
                }
            }
            ArrayBuilderVariant::Boolean(builder) => {
                if let Some(val) = value.as_bool() {
                    builder.append_value(val);
                } else {
                    builder.append_null();
                }
            } // ArrayBuilderVariant::StructBuilder(builder, fields) => {
              //     let mut builders = Vec::with_capacity(fields.len());
              //     for field in fields.clone().into_iter() {
              //         builders.push(make_builder(field.data_type(), fields.len()));
              //     }
              //     // Assuming 'value' is a JSON object for a struct
              //     if let Value::Object(values) = value {
              //         // Iterate over the fields and append values
              //         for i in 0..builder.num_fields() {
              //             let arr_builder = match builders.get(i) {
              //                 Some(b) => b,
              //                 None => return Err(ArrowError::JsonError("Type definition for nested object not found".to_string()))
              //             };
              //             let field_builder = builder.field_builder::<arr_builder>(i).unwrap();
              //             // You need to determine how to extract and append values
              //             // based on the field type and JSON structure
              //             // This is a simplified example
              //             let field_value = values.get(field_builder.name()).unwrap();
              //             // Recursively call append_value for the nested builder
              //             // You need a way to convert field_builder into ArrayBuilderVariant
              //         }
              //         builder.append(true)?; // Append the struct value (true for valid)
              //     } else {
              //         builder.append(false)?; // Append null if the value is not an object
              //     }
              // }
        }
        Ok(())
    }

    pub fn finish(self) -> Result<ArrayRef, ArrowError> {
        Ok(match self {
            ArrayBuilderVariant::Int64(mut builder) => Arc::new(builder.finish()),
            ArrayBuilderVariant::Float64(mut builder) => Arc::new(builder.finish()),
            ArrayBuilderVariant::String(mut builder) => Arc::new(builder.finish()),
            ArrayBuilderVariant::UInt64(mut builder) => Arc::new(builder.finish()),
            ArrayBuilderVariant::Boolean(mut builder) => Arc::new(builder.finish()),
            // ArrayBuilderVariant::StructBuilder(mut builder, _) => Arc::new(builder.finish()),
            // ArrayBuilderVariant::ListBuilder(mut builder) => Arc::new(builder.finish()),
        })
    }
}

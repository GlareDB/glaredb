
use std::sync::Arc;

use datafusion::arrow::{array::{Int64Builder, Float64Builder, StringBuilder, ArrayRef}, error::ArrowError};
use serde_json::Value;

pub enum ArrayBuilderVariant {
    Int64Builder(Int64Builder),
    Float64Builder(Float64Builder),
    StringBuilder(StringBuilder),
}

impl ArrayBuilderVariant {
      pub fn append_value(&mut self, value: &Value) -> Result<(), ArrowError> {
        match self {
            ArrayBuilderVariant::Int64Builder(builder) => {
                if let Some(val) = value.as_i64() {
                    builder.append_value(val);
                } else {
                    builder.append_null();
                }
            }
            ArrayBuilderVariant::Float64Builder(builder) => {
                if let Some(val) = value.as_f64() {
                    builder.append_value(val);
                } else {
                    builder.append_null();
                }
            }
            ArrayBuilderVariant::StringBuilder(builder) => {
                if let Some(val) = value.as_str() {
                    builder.append_value(val);
                } else {
                    builder.append_null();
                }
            }
            // Handle other types
        }
        Ok(())
    }

    pub fn finish(self) -> Result<ArrayRef, ArrowError> {
        Ok(match self {
            ArrayBuilderVariant::Int64Builder(mut builder) => Arc::new(builder.finish()),
            ArrayBuilderVariant::Float64Builder(mut builder) => Arc::new(builder.finish()),
            ArrayBuilderVariant::StringBuilder(mut builder) => Arc::new(builder.finish()),
            // Finish other types
        })
    }
}
use std::{any::Any, iter, sync::Arc};

use datafusion::arrow::{
    array::{
        Array, ArrayBuilder, ArrayData, ArrayDataBuilder, ArrayRef, BooleanBufferBuilder,
        BooleanBuilder, BufferBuilder, Float64Builder, GenericListArray, Int64Builder, ListBuilder,
        StringBuilder, StructBuilder, UInt64Builder,
    },
    buffer::NullBuffer,
    datatypes::{DataType, FieldRef, Fields, SchemaRef},
    error::ArrowError,
    record_batch::RecordBatch,
};
use serde_json::Value;

pub fn create_child_builder(dt: &DataType) -> Result<ArrayBuilderVariant, ArrowError> {
    let builder = match dt {
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            ArrayBuilderVariant::Int64(Int64Builder::new())
        }
        DataType::Float16 | DataType::Float32 | DataType::Float64 => {
            ArrayBuilderVariant::Float64(Float64Builder::new())
        }
        DataType::Utf8 => ArrayBuilderVariant::String(StringBuilder::new()),
        DataType::UInt16 | DataType::UInt8 | DataType::UInt32 | DataType::UInt64 => {
            ArrayBuilderVariant::UInt64(UInt64Builder::new())
        }
        DataType::Boolean => ArrayBuilderVariant::Boolean(BooleanBuilder::new()),
        DataType::List(inner) => {
            let value_builders = create_child_builder(inner.data_type())?;
            ArrayBuilderVariant::List(ListBuilder::new(Box::new(value_builders)))
        }
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

#[derive(Debug)]
pub enum ArrayBuilderVariant {
    Int64(Int64Builder),
    Float64(Float64Builder),
    String(StringBuilder),
    UInt64(UInt64Builder),
    Boolean(BooleanBuilder),
    List(ListBuilder<Box<ArrayBuilderVariant>>),
    // StructBuilder(StructBuilder, Fields),
}

impl ArrayBuilderVariant {
    fn len(&self) -> usize {
        match &self {
            ArrayBuilderVariant::Int64(builder) => builder.len(),
            ArrayBuilderVariant::Float64(builder) => builder.len(),
            ArrayBuilderVariant::String(builder) => builder.len(),
            ArrayBuilderVariant::UInt64(builder) => builder.len(),
            ArrayBuilderVariant::Boolean(builder) => builder.len(),
            ArrayBuilderVariant::List(builder) => builder.len(),
        }
    }

    fn as_any(&self) -> &dyn Any {
        match &self {
            ArrayBuilderVariant::Int64(builder) => builder,
            ArrayBuilderVariant::Float64(builder) => builder,
            ArrayBuilderVariant::String(builder) => builder,
            ArrayBuilderVariant::UInt64(builder) => builder,
            ArrayBuilderVariant::Boolean(builder) => builder,
            ArrayBuilderVariant::List(builder) => builder,
        }
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        match self {
            ArrayBuilderVariant::Int64(builder) => builder,
            ArrayBuilderVariant::Float64(builder) => builder,
            ArrayBuilderVariant::String(builder) => builder,
            ArrayBuilderVariant::UInt64(builder) => builder,
            ArrayBuilderVariant::Boolean(builder) => builder,
            ArrayBuilderVariant::List(builder) => builder,
        }
    }

    pub fn try_finish(&mut self) -> ArrayRef {
        match self {
            ArrayBuilderVariant::Int64(ref mut builder) => Arc::new(builder.finish()),
            ArrayBuilderVariant::Float64(ref mut builder) => Arc::new(builder.finish()),
            ArrayBuilderVariant::String(ref mut builder) => Arc::new(builder.finish()),
            ArrayBuilderVariant::UInt64(ref mut builder) => Arc::new(builder.finish()),
            ArrayBuilderVariant::Boolean(ref mut builder) => Arc::new(builder.finish()),
            ArrayBuilderVariant::List(ref mut builder) => Arc::new(builder.finish()),
        }
    }

    fn finish_cloned(&self) -> ArrayRef {
        match self {
            ArrayBuilderVariant::Int64(builder) => Arc::new(builder.finish_cloned()),
            ArrayBuilderVariant::Float64(builder) => Arc::new(builder.finish_cloned()),
            ArrayBuilderVariant::String(builder) => Arc::new(builder.finish_cloned()),
            ArrayBuilderVariant::UInt64(builder) => Arc::new(builder.finish_cloned()),
            ArrayBuilderVariant::Boolean(builder) => Arc::new(builder.finish_cloned()),
            ArrayBuilderVariant::List(builder) => Arc::new(builder.finish_cloned()),
        }
    }

    fn append_to_builder(builder: &mut ArrayBuilderVariant, value: &Value) {
        match builder {
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
            }

            ArrayBuilderVariant::List(builder) => {
                if let Some(val_list) = value.as_array() {
                    for val in val_list {
                        // Recursively append each value in the list
                        // This requires a function to handle appending a `Value` to a `Box<ArrayBuilderVariant>`
                        let value_builder = builder.values();
                        ArrayBuilderVariant::append_to_builder(value_builder, val);
                    }
                    // Finalize the list element
                    builder.append(true);
                } else {
                    // Append a null list if the value is not a list
                    builder.append(false);
                }
            }
        }
    }

    pub fn append_value(&mut self, value: &Value) -> Result<(), ArrowError> {
        // ArrayBuilderVariant::StructBuilder(builder, fields) => {
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
        let builder = self;
        Self::append_to_builder(builder, value);
        Ok(())
    }
}

impl ArrayBuilder for ArrayBuilderVariant {
    fn len(&self) -> usize {
        self.len()
    }

    fn finish(&mut self) -> ArrayRef {
        self.try_finish()
    }

    fn finish_cloned(&self) -> ArrayRef {
        self.finish_cloned()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self.as_any()
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self.as_any_mut()
    }

    fn into_box_any(self: Box<Self>) -> Box<dyn std::any::Any> {
        self
    }
}

impl ArrayBuilder for Box<ArrayBuilderVariant> {
    fn len(&self) -> usize {
        self.as_ref().len()
    }

    fn finish(&mut self) -> ArrayRef {
        (**self).try_finish()
    }

    fn finish_cloned(&self) -> ArrayRef {
        self.as_ref().finish_cloned()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self.as_ref().as_any()
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        (**self).as_any_mut()
    }

    fn into_box_any(self: Box<Self>) -> Box<dyn std::any::Any> {
        self
    }
}

impl Extend<Option<Value>> for ArrayBuilderVariant {
    #[inline]
    fn extend<T: IntoIterator<Item = Option<Value>>>(&mut self, iter: T) {
        for v in iter {
            if let Some(val) = v {
                let _ = self.append_value(&val);
            }
        }
    }
}

// TODO; use limit from passed in max_size_limit argument
pub fn json_values_to_record_batch(
    rows: &[Value],
    schema: SchemaRef,
    size: usize,
) -> Result<RecordBatch, ArrowError> {
    let fields = schema.fields().iter().take(size);
    let mut columns: Vec<Arc<dyn Array>> = Vec::with_capacity(fields.len());
    for field in fields {
        let col: Arc<dyn Array> = match field.data_type() {
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                let mut arr = Int64Builder::new();
                for row in rows.iter() {
                    let val: Option<i64> = row[field.name()].as_i64();
                    arr.append_option(val);
                }
                Arc::new(arr.finish())
            }
            DataType::Float16 | DataType::Float32 | DataType::Float64 => {
                let mut arr = Float64Builder::new();
                for row in rows.iter() {
                    let val: Option<f64> = row[field.name()].as_f64();
                    arr.append_option(val);
                }
                Arc::new(arr.finish())
            }
            DataType::UInt16 | DataType::UInt8 | DataType::UInt32 | DataType::UInt64 => {
                let mut arr = UInt64Builder::new();
                for row in rows.iter() {
                    let val: Option<u64> = row[field.name()].as_u64();
                    arr.append_option(val);
                }
                Arc::new(arr.finish())
            }
            DataType::Boolean => {
                let mut arr = BooleanBuilder::new();
                for row in rows.iter() {
                    let val: Option<bool> = row[field.name()].as_bool();
                    arr.append_option(val);
                }
                Arc::new(arr.finish())
            }
            DataType::Utf8 => {
                // Assumes an average of 16 bytes per item.
                let mut arr = StringBuilder::with_capacity(rows.len(), rows.len() * 16);
                for row in rows.iter() {
                    let val: Option<&str> = row[field.name()].as_str();
                    arr.append_option(val);
                }
                Arc::new(arr.finish())
            }
            // TODO work on structs with unequal lengths, i.e merge the schemas
            DataType::Struct(fields) => {
                let mut builder = StructBuilder::from_fields(fields.clone(), fields.len());

                for row in rows {
                    let row_value = &row[field.name()];
                    build_struct(&mut builder, row_value, fields);
                }

                Arc::new(builder.finish())
            }
            DataType::List(arr_field) => {
                let child_arrs = build_list(rows, arr_field.clone(), field)?;
                let arr: GenericListArray<i32> = GenericListArray::from(child_arrs);
                Arc::new(arr) as ArrayRef
            }

            // TODO
            // 1. add timestamp and date conversions; (serde does not suppoert that though)
            other => {
                return Err(ArrowError::CastError(format!(
                    "Failed to convert {:#?}",
                    other
                )))
            }
        };

        columns.push(col);
    }
    let batch = RecordBatch::try_new(schema, columns)?;
    Ok(batch)
}

// TODO:
// 1. change fn name
// 2. Remove the "unwraps" and handle errors properly
fn build_struct(builder: &mut StructBuilder, val: &Value, fields: &Fields) {
    for (idx, struct_field) in fields.into_iter().enumerate() {
        match &val[struct_field.name()] {
            Value::String(s) => {
                builder
                    .field_builder::<StringBuilder>(idx)
                    .unwrap()
                    .append_value(s);
            }
            Value::Number(n) => {
                // TODO handle u64 and i64. Do that by matching on n with the Number enum
                builder
                    .field_builder::<Int64Builder>(idx)
                    .unwrap()
                    .append_value(n.as_i64().unwrap());
            }
            Value::Bool(b) => {
                builder
                    .field_builder::<BooleanBuilder>(idx)
                    .unwrap()
                    .append_value(*b);
            }
            Value::Null => builder.append(false),
            Value::Object(inner) => {
                let inner_builder = builder.field_builder::<StructBuilder>(idx).unwrap();
                let inner_fields = iter::once(struct_field.clone()).collect::<Fields>();
                build_struct(inner_builder, &Value::Object(inner.clone()), &inner_fields);
            }
            // Value::Array(inner) => {
            //     let inner_builder = builder
            //         .field_builder::<ListBuilder<ArrayBuilderVariant>>(idx)
            //         .unwrap();
            //     let arr = build_list(&inner, struct_field.clone(), struct_field).unwrap();
            //     // let arr: GenericListArray<i32> = GenericListArray::from(arr);
            //     inner_builder.append_value(arr.child_data());
            // }
            _ => unimplemented!(),
        };
        builder.append(true);
    }
}

// TODO: change fn name
fn build_list(
    rows: &[Value],
    arr_field: FieldRef,
    field: &FieldRef,
) -> Result<ArrayData, ArrowError> {
    let mut nulls = arr_field
        .is_nullable()
        .then(|| BooleanBufferBuilder::new(rows.len()));
    let mut offsets = vec![0];

    // TODO look into using arrow's "make_builder" macro
    let mut child_builder = create_child_builder(arr_field.data_type())?;

    for row in rows.iter() {
        match (row.as_object().unwrap().get(field.name()), nulls.as_mut()) {
            (Some(val), None) => {
                let value_arrays = val.as_array().unwrap();
                for r in value_arrays.iter() {
                    child_builder.append_value(r)?;
                }

                let last_offset = *offsets.last().unwrap();
                offsets.push(last_offset + value_arrays.len() as i32);
            }
            (Some(val), Some(nulls)) => {
                nulls.append(true);
                let value_arrays = val.as_array().unwrap();
                for r in value_arrays.iter() {
                    child_builder.append_value(r)?;
                }

                let last_offset = *offsets.last().unwrap();
                offsets.push(last_offset + value_arrays.len() as i32);
            }
            (None, Some(nulls)) => nulls.append(false),
            _ => {
                return Err(ArrowError::JsonError(format!(
                    "Invalid key for {:#?}",
                    field.name()
                )))
            }
        }
    }

    let child_array = child_builder.try_finish();

    let mut offset_buffer = BufferBuilder::<i32>::new(rows.len() + 1);
    offset_buffer.append_slice(&offsets);

    let nulls = nulls.as_mut().map(|x| NullBuffer::new(x.finish()));
    let data = ArrayDataBuilder::new(field.data_type().clone())
        .len(rows.len())
        .nulls(nulls)
        .add_buffer(offset_buffer.finish())
        .child_data(vec![child_array.to_data().clone()]);
    data.build()
}

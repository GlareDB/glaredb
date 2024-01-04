use std::{
    any::TypeId,
    borrow::{Borrow, BorrowMut},
    collections::HashMap,
    sync::Arc,
};

use datafusion::arrow::{
    array::{
        builder, make_builder, new_empty_array, Array, ArrayBuilder, ArrayRef, BooleanArray,
        BooleanBuilder, Float16Builder, Float32Builder, Float64Builder, GenericListBuilder,
        Int16Builder, Int32Array, Int32Builder, Int64Builder, Int8Builder, ListArray, ListBuilder,
        NullBuilder, OffsetSizeTrait, PrimitiveArray, PrimitiveBuilder, StringBuilder,
        UInt16Builder, UInt32Builder, UInt64Builder, UInt8Array, UInt8Builder,
    },
    datatypes::{
        ArrowPrimitiveType, DataType, Field, FieldRef, Float64Type, GenericStringType, Int32Type,
        Int64Type, Int8Type, SchemaRef, UInt64Type,
    },
    error::ArrowError,
    record_batch::RecordBatch,
};
use serde_json::Value;

use super::builder::{ArrayBuilderVariant, create_child_builder};

fn allocate_array(f: &Field) -> Result<Box<dyn ArrayBuilder>, ArrowError> {
    let builder: Box<dyn ArrayBuilder> = match f.data_type() {
        DataType::Null => Box::new(NullBuilder::new()),
        DataType::Int8 => Box::new(Int8Builder::new()),
        DataType::Int16 => Box::new(Int16Builder::new()),
        DataType::Int32 => Box::new(Int32Builder::new()),
        DataType::Int64 => Box::new(Int64Builder::new()),
        DataType::UInt8 => Box::new(UInt8Builder::new()),
        DataType::UInt16 => Box::new(UInt16Builder::new()),
        DataType::UInt32 => Box::new(UInt32Builder::new()),
        DataType::UInt64 => Box::new(UInt64Builder::new()),
        DataType::Float16 => Box::new(Float16Builder::new()),
        DataType::Float32 => Box::new(Float32Builder::new()),
        DataType::Float64 => Box::new(Float64Builder::new()),
        DataType::Boolean => Box::new(BooleanBuilder::new()),
        DataType::Utf8 => Box::new(StringBuilder::new()),
        DataType::List(inner) => {
            let value_builders = create_child_builder(inner.data_type())?;
            Box::new(ListBuilder::new(value_builders))
        }
        other => {
            return Err(ArrowError::JsonError(format!(
                "{:#?} type not supported",
                other
            )))
        }
    };
    Ok(builder)
}

fn generic_deserialize_into<'a, A: Borrow<Value>, M: 'static>(
    target: &mut Box<dyn ArrayBuilder>,
    rows: &[A],
    deserialize_into: fn(&mut M, &[A]) -> (),
) {
    deserialize_into(&mut target.as_any_mut().downcast_mut::<M>().unwrap(), rows);
}

fn deserialize_primitive_into<'a, A: Borrow<Value>, T: ArrowPrimitiveType>(
    target: &mut Box<dyn ArrayBuilder>,
    rows: &[A],
    deserialize_into: fn(&mut PrimitiveBuilder<T>, &[A]) -> (),
) {
    generic_deserialize_into(target, rows, deserialize_into)
}

fn deserialize_into(target: &mut Box<dyn ArrayBuilder>, rows: &[Value], dt: &DataType) {
    match dt {
        DataType::Boolean => generic_deserialize_into(target, rows, deserialize_boolean_into),
        DataType::Float32 | DataType::Float64 => {
            deserialize_primitive_into::<_, Float64Type>(target, rows, deserialize_float_into)
        }
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            deserialize_primitive_into::<_, Int64Type>(target, rows, deserialize_int_into)
        }
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
            deserialize_primitive_into::<_, UInt64Type>(target, rows, deserialize_unsigned_int_into)
        }
        DataType::Utf8 => generic_deserialize_into(target, rows, deserialize_utf8_into),
        DataType::LargeUtf8 => generic_deserialize_into(target, rows, deserialize_utf8_into),
        // DataType::FixedSizeList(field, _) => {
        //     generic_deserialize_into(target, rows, deserialize_fixed_size_list_into)
        // }
        DataType::List(field) => deserialize_list_into(
            target
                .as_any_mut()
                .downcast_mut::<ListBuilder<ArrayBuilderVariant>>()
                .unwrap(),
            rows,
            Field::new_list(field.name(), field.clone(), field.is_nullable()),
        ),
        _ => {
            todo!()
        }
    }
}

pub fn json_values_to_record_batch(
    json: &Value,
    schema: SchemaRef,
) -> Result<RecordBatch, ArrowError> {
    let mut results = schema
        .fields
        .iter()
        .map(|f| (f.name(), (f.data_type(), allocate_array(&f).unwrap())))
        .collect::<HashMap<_, _>>();

    match json {
        Value::Array(rows) => {
            for row in rows.iter() {
                match row {
                    Value::Object(record) => {
                        for (key, value) in record.iter() {
                            let (dt, arr) = results.get_mut(key).ok_or_else(|| {
                                ArrowError::JsonError(format!("unexpected key: '{key}'"))
                            })?;
                            deserialize_into(arr, &[value.clone()], dt);
                        }
                    }
                    _ => {
                        return Err(ArrowError::JsonError(
                            "each row must be an Object".to_string(),
                        ))
                    }
                }
            }
        }
        _ => {
            return Err(ArrowError::JsonError(
                "outer type must be an Array".to_string(),
            ))
        }
    }

    let columns: Vec<ArrayRef> = results
        .values_mut()
        .map(|(_, builder)| builder.finish())
        .collect();

    let batch = RecordBatch::try_new(schema, columns)?;
    Ok(batch)
}

fn deserialize_boolean_into<'a, A: Borrow<Value>>(target: &mut BooleanBuilder, rows: &[A]) {
    for row in rows {
        match row.borrow() {
            Value::Bool(v) => target.append_value(*v),
            _ => target.append_null(),
        }
    }
}

fn deserialize_float_into<'a, T: ArrowPrimitiveType<Native = f64>, A: Borrow<Value>>(
    target: &mut PrimitiveBuilder<T>,
    rows: &[A],
) {
    for row in rows {
        match row.borrow() {
            Value::Number(number) => target.append_option(number.as_f64()),
            _ => target.append_null(),
        }
    }
}

fn deserialize_int_into<'a, T: ArrowPrimitiveType<Native = i64>, A: Borrow<Value>>(
    target: &mut PrimitiveBuilder<T>,
    rows: &[A],
) {
    for row in rows {
        match row.borrow() {
            Value::Number(number) => target.append_option(number.as_i64()),
            _ => target.append_null(),
        }
    }
}

fn deserialize_unsigned_int_into<'a, T: ArrowPrimitiveType<Native = u64>, A: Borrow<Value>>(
    target: &mut PrimitiveBuilder<T>,
    rows: &[A],
) {
    for row in rows {
        match row.borrow() {
            Value::Number(number) => target.append_option(number.as_u64()),
            _ => target.append_null(),
        }
    }
}

fn deserialize_utf8_into<'a, A: Borrow<Value>>(target: &mut StringBuilder, rows: &[A]) {
    for row in rows {
        match row.borrow() {
            Value::String(v) => target.append_option(Some(v)),
            Value::Number(number) => {
                let n = number.to_string();
                target.append_value(n)
            }
            Value::Bool(v) => target.append_option(Some(if *v { "true" } else { "false" })),
            _ => target.append_null(),
        }
    }
}

fn deserialize_list_into<'a, O: OffsetSizeTrait, A: Borrow<Value>>(
    target: &mut GenericListBuilder<O, ArrayBuilderVariant>,
    rows: &[A],
    field: Field,
) {
    let empty = Vec::<Value>::new();
    let inner: Vec<Value> = rows
        .iter()
        .flat_map(|row| match row.borrow() {
            Value::Array(value) => value.iter(),
            _ => empty.iter(),
        })
        .cloned()
        .collect();

    let mut values = allocate_array(&field).unwrap();
    deserialize_into(&mut values, &inner, field.data_type());
}


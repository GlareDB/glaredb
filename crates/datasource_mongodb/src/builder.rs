use crate::errors::{MongoError, Result};
use bitvec::{order::Lsb0, vec::BitVec};
use datafusion::arrow::array::{
    Array, ArrayBuilder, ArrayRef, BinaryBuilder, BooleanBuilder, Decimal128Builder,
    Float64Builder, Int32Builder, Int64Builder, StringBuilder, StructArray,
    TimestampMicrosecondBuilder, TimestampMillisecondBuilder,
};
use datafusion::arrow::datatypes::{DataType, Field, Fields, TimeUnit};
use mongodb::bson::{RawBsonRef, RawDocument};
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

/// Similar to arrow's `StructBuilder`, but specific for "shredding" bson
/// records.
///
/// Failures to append either a record or null will put the builder in an
/// undefined state.
pub struct RecordStructBuilder {
    fields: Fields,
    builders: Vec<Box<dyn ArrayBuilder>>,
    field_index: HashMap<String, usize>,
}

impl RecordStructBuilder {
    pub fn new_with_capacity(fields: Fields, capacity: usize) -> Result<RecordStructBuilder> {
        let builders = column_builders_for_fields(fields.clone(), capacity)?;
        Self::new_with_builders(fields, builders)
    }

    pub fn new_with_builders(
        fields: Fields,
        builders: Vec<Box<dyn ArrayBuilder>>,
    ) -> Result<RecordStructBuilder> {
        if fields.len() != builders.len() {
            return Err(MongoError::InvalidArgsForRecordStructBuilder);
        }
        if builders.is_empty() {
            return Err(MongoError::InvalidArgsForRecordStructBuilder);
        }

        let mut field_index = HashMap::with_capacity(fields.len());
        for (idx, field) in fields.iter().enumerate() {
            field_index.insert(field.name().clone(), idx);
        }

        Ok(RecordStructBuilder {
            fields,
            builders,
            field_index,
        })
    }

    pub fn append_nulls(&mut self) -> Result<()> {
        for (builder, field) in self.builders.iter_mut().zip(self.fields.iter()) {
            append_null(field.data_type(), builder.as_mut())?;
        }
        Ok(())
    }

    pub fn append_record(&mut self, doc: &RawDocument) -> Result<()> {
        let mut cols_set: BitVec<u8, Lsb0> = BitVec::repeat(false, self.fields.len());

        for iter_result in doc {
            match iter_result {
                Ok((key, val)) => {
                    let idx = *self
                        .field_index
                        .get(key)
                        .ok_or_else(|| MongoError::ColumnNotInInferredSchema(key.to_string()))?;

                    if *cols_set.get(idx).unwrap() {
                        println!("DUPLICATE SET: {}, {:?}", key, doc);
                    }

                    // Add value to columns.
                    let typ = self.fields.get(idx).unwrap().data_type(); // Programmer error if data type doesn't exist.
                    let col = self.builders.get_mut(idx).unwrap(); // Programmer error if this doesn't exist.
                    append_value(val, typ, col.as_mut())?;

                    // Track which columns we've added values to.
                    cols_set.set(idx, true);
                }
                Err(_) => return Err(MongoError::FailedToReadRawBsonDocument),
            }
        }

        // Append nulls to all columns not included in the doc.
        for (idx, did_set) in cols_set.iter().enumerate() {
            if !did_set {
                // Add nulls...
                let typ = self.fields.get(idx).unwrap().data_type(); // Programmer error if data type doesn't exist.
                let col = self.builders.get_mut(idx).unwrap(); // Programmer error if column doesn't exist.
                append_null(typ, col.as_mut())?;
            }
        }

        Ok(())
    }

    pub fn into_fields_and_builders(self) -> (Fields, Vec<Box<dyn ArrayBuilder>>) {
        (self.fields, self.builders)
    }
}

impl ArrayBuilder for RecordStructBuilder {
    fn len(&self) -> usize {
        self.builders.get(0).unwrap().len()
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn finish(&mut self) -> ArrayRef {
        let fields = std::mem::take(&mut self.fields);
        let builders = std::mem::take(&mut self.builders);
        let arrays = builders.into_iter().map(|mut b| b.finish());

        let pairs: Vec<(Field, Arc<dyn Array>)> = fields
            .into_iter()
            .map(|f| f.as_ref().clone())
            .zip(arrays)
            .collect();

        let array: StructArray = pairs.into();

        Arc::new(array)
    }

    fn finish_cloned(&self) -> ArrayRef {
        let fields = self.fields.clone();
        let arrays: Vec<Arc<dyn Array>> = self.builders.iter().map(|b| b.finish_cloned()).collect();

        let pairs: Vec<(Field, Arc<dyn Array>)> = fields
            .into_iter()
            .map(|f| f.as_ref().clone())
            .zip(arrays)
            .collect();

        let array: StructArray = pairs.into();

        Arc::new(array)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn into_box_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

/// Macro for generating code for downcasting and appending a value.
macro_rules! append_scalar {
    ($builder:ty, $col:expr, $v:expr) => {
        $col.as_any_mut()
            .downcast_mut::<$builder>()
            .unwrap()
            .append_value($v)
    };
}

/// Append a value to a column.
///
/// Errors if the value is of an unsupported type.
///
/// Panics if the array builder is not the expected type. This would indicated a
/// programmer error.
fn append_value(val: RawBsonRef, typ: &DataType, col: &mut dyn ArrayBuilder) -> Result<()> {
    // So robust
    match (val, typ) {
        // Boolean
        (RawBsonRef::Boolean(v), DataType::Boolean) => {
            append_scalar!(BooleanBuilder, col, v)
        }
        (RawBsonRef::Boolean(v), DataType::Utf8) => {
            append_scalar!(StringBuilder, col, v.to_string())
        }

        // Double
        (RawBsonRef::Double(v), DataType::Int32) => append_scalar!(Int32Builder, col, v as i32),
        (RawBsonRef::Double(v), DataType::Int64) => append_scalar!(Int64Builder, col, v as i64),
        (RawBsonRef::Double(v), DataType::Float64) => append_scalar!(Float64Builder, col, v),
        (RawBsonRef::Double(v), DataType::Utf8) => {
            append_scalar!(StringBuilder, col, v.to_string())
        }

        // Int32
        (RawBsonRef::Int32(v), DataType::Int32) => append_scalar!(Int32Builder, col, v),
        (RawBsonRef::Int32(v), DataType::Int64) => append_scalar!(Int64Builder, col, v as i64),
        (RawBsonRef::Int32(v), DataType::Float64) => append_scalar!(Float64Builder, col, v as f64),
        (RawBsonRef::Int32(v), DataType::Utf8) => {
            append_scalar!(StringBuilder, col, v.to_string())
        }

        // Int64
        (RawBsonRef::Int64(v), DataType::Int32) => append_scalar!(Int32Builder, col, v as i32),
        (RawBsonRef::Int64(v), DataType::Int64) => append_scalar!(Int64Builder, col, v),
        (RawBsonRef::Int64(v), DataType::Float64) => append_scalar!(Float64Builder, col, v as f64),
        (RawBsonRef::Int64(v), DataType::Utf8) => {
            append_scalar!(StringBuilder, col, v.to_string())
        }

        // String
        (RawBsonRef::String(v), DataType::Boolean) => {
            append_scalar!(BooleanBuilder, col, v.parse().unwrap_or_default())
        }
        (RawBsonRef::String(v), DataType::Int32) => {
            append_scalar!(Int32Builder, col, v.parse().unwrap_or_default())
        }
        (RawBsonRef::String(v), DataType::Int64) => {
            append_scalar!(Int64Builder, col, v.parse().unwrap_or_default())
        }
        (RawBsonRef::String(v), DataType::Float64) => {
            append_scalar!(Float64Builder, col, v.parse().unwrap_or_default())
        }
        (RawBsonRef::String(v), DataType::Utf8) => {
            append_scalar!(StringBuilder, col, v)
        }

        // Binary
        (RawBsonRef::Binary(v), DataType::Binary) => append_scalar!(BinaryBuilder, col, v.bytes),

        // Object id
        (RawBsonRef::ObjectId(v), DataType::Utf8) => {
            append_scalar!(StringBuilder, col, v.to_string())
        }

        // Timestamp
        (RawBsonRef::Timestamp(v), DataType::Timestamp(TimeUnit::Microsecond, _)) => {
            append_scalar!(TimestampMicrosecondBuilder, col, v.time as i64) // TODO: Possibly change to nanosecond.
        }

        // Datetime
        (RawBsonRef::DateTime(v), DataType::Timestamp(TimeUnit::Microsecond, _)) => {
            append_scalar!(
                TimestampMicrosecondBuilder, // TODO: Possibly change to nanosecond.
                col,
                v.timestamp_millis()
            )
        }

        // Document
        (RawBsonRef::Document(nested), DataType::Struct(_)) => {
            let builder = col
                .as_any_mut()
                .downcast_mut::<RecordStructBuilder>()
                .unwrap();
            builder.append_record(nested)?;
        }

        // Array
        (RawBsonRef::Array(arr), DataType::Utf8) => {
            // TODO: Proper types.
            let s = arr
                .into_iter()
                .map(|r| r.map(|v| format!("{:?}", v)).unwrap_or_default())
                .collect::<Vec<_>>()
                .join(", ");
            append_scalar!(StringBuilder, col, format!("[{}]", s))
        }

        // Decimal128
        (RawBsonRef::Decimal128(v), DataType::Decimal128(_, _)) => col
            .as_any_mut()
            .downcast_mut::<Decimal128Builder>()
            .unwrap()
            .append_value(i128::from_le_bytes(v.bytes())),

        (bson_ref, dt) => {
            return Err(MongoError::UnhandledElementType(
                bson_ref.element_type(),
                dt.clone(),
            ))
        }
    }
    Ok(())
}

/// Append a null value to the array build.
///
/// Panics if the array builder is not the correct type for the provided data
/// type.
fn append_null(typ: &DataType, col: &mut dyn ArrayBuilder) -> Result<()> {
    match typ {
        &DataType::Boolean => col
            .as_any_mut()
            .downcast_mut::<BooleanBuilder>()
            .unwrap()
            .append_null(),
        &DataType::Int32 => col
            .as_any_mut()
            .downcast_mut::<Int32Builder>()
            .unwrap()
            .append_null(),
        &DataType::Int64 => col
            .as_any_mut()
            .downcast_mut::<Int64Builder>()
            .unwrap()
            .append_null(),
        &DataType::Float64 => col
            .as_any_mut()
            .downcast_mut::<Float64Builder>()
            .unwrap()
            .append_null(),
        &DataType::Timestamp(_, _) => col
            .as_any_mut()
            .downcast_mut::<TimestampMillisecondBuilder>() // TODO: Possibly change to nanosecond.
            .unwrap()
            .append_null(),
        &DataType::Utf8 => col
            .as_any_mut()
            .downcast_mut::<StringBuilder>()
            .unwrap()
            .append_null(),
        &DataType::Binary => col
            .as_any_mut()
            .downcast_mut::<BinaryBuilder>()
            .unwrap()
            .append_null(),
        &DataType::Struct(_) => col
            .as_any_mut()
            .downcast_mut::<RecordStructBuilder>()
            .unwrap()
            .append_nulls()?,
        &DataType::Decimal128(_, _) => col
            .as_any_mut()
            .downcast_mut::<Decimal128Builder>()
            .unwrap()
            .append_null(),
        other => return Err(MongoError::UnexpectedDataTypeForBuilder(other.clone())),
    }
    Ok(())
}

fn column_builders_for_fields(
    fields: Fields,
    capacity: usize,
) -> Result<Vec<Box<dyn ArrayBuilder>>> {
    let mut cols = Vec::with_capacity(capacity);

    for field in fields.into_iter() {
        let col: Box<dyn ArrayBuilder> = match field.data_type() {
            DataType::Boolean => Box::new(BooleanBuilder::with_capacity(capacity)),
            DataType::Int32 => Box::new(Int32Builder::with_capacity(capacity)),
            DataType::Int64 => Box::new(Int64Builder::with_capacity(capacity)),
            DataType::Float64 => Box::new(Float64Builder::with_capacity(capacity)),
            DataType::Timestamp(_, _) => {
                Box::new(TimestampMicrosecondBuilder::with_capacity(capacity)) // TODO: Possibly change to nanosecond.
            }
            DataType::Utf8 => Box::new(StringBuilder::with_capacity(capacity, 10)), // TODO: Can collect avg when inferring schema.
            DataType::Binary => Box::new(BinaryBuilder::with_capacity(capacity, 10)), // TODO: Can collect avg when inferring schema.
            DataType::Decimal128(_, _) => Box::new(Decimal128Builder::with_capacity(capacity)), // TODO: Can collect avg when inferring schema.
            DataType::Struct(fields) => {
                let nested = column_builders_for_fields(fields.clone(), capacity)?;
                Box::new(RecordStructBuilder::new_with_builders(
                    fields.clone(),
                    nested,
                )?)
            }
            other => return Err(MongoError::UnexpectedDataTypeForBuilder(other.clone())),
        };

        cols.push(col);
    }

    Ok(cols)
}

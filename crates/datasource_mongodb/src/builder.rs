use crate::errors::{MongoError, Result};
use async_stream::stream;
use bitvec::{order::Lsb0, vec::BitVec};
use datafusion::arrow::array::{
    Array, ArrayBuilder, ArrayRef, BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder,
    Float32Builder, Float64Builder, Int16Builder, Int32Builder, Int64Builder, Int8Builder,
    ListBuilder, StringBuilder, StructArray, StructBuilder, Time64MicrosecondBuilder,
    TimestampMicrosecondBuilder, TimestampMillisecondBuilder,
};
use datafusion::arrow::datatypes::{
    DataType, Field, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef, TimeUnit,
};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result as DatafusionResult};
use datafusion::execution::context::{SessionState, TaskContext};
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::{
    display::DisplayFormatType, ExecutionPlan, Partitioning, RecordBatchStream,
    SendableRecordBatchStream, Statistics,
};
use futures::{Stream, StreamExt};
use mongodb::bson::{doc, Document, RawBsonRef, RawDocument, RawDocumentBuf};
use mongodb::{options::ClientOptions, Client, Collection};
use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

/// Similar to arrow's `StructBuilder`, but specific for "shredding" bson
/// records.
///
/// Failures to append either a record or null will put the builder in an
/// undefined state.
pub struct RecordStructBuilder {
    fields: Vec<Field>,
    builders: Vec<Box<dyn ArrayBuilder>>,
    field_index: HashMap<String, usize>,
}

impl RecordStructBuilder {
    pub fn new_with_capacity(fields: Vec<Field>, capacity: usize) -> Result<RecordStructBuilder> {
        let builders = column_builders_for_fields(&fields, capacity)?;
        Self::new_with_builders(fields, builders)
    }

    pub fn new_with_builders(
        fields: Vec<Field>,
        builders: Vec<Box<dyn ArrayBuilder>>,
    ) -> Result<RecordStructBuilder> {
        if fields.len() != builders.len() {
            return Err(MongoError::InvalidArgsForRecordStructBuilder);
        }
        if builders.len() == 0 {
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

                    // Add value to columns.
                    let col = self.builders.get_mut(idx).unwrap(); // Programmer error if this doesn't exist.
                    append_value(val, col.as_mut())?;

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

    pub fn into_fields_and_builders(self) -> (Vec<Field>, Vec<Box<dyn ArrayBuilder>>) {
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
        Arc::new(StructBuilder::new(fields, builders).finish())
    }

    fn finish_cloned(&self) -> ArrayRef {
        let fields = self.fields.clone();
        let arrays: Vec<Arc<dyn Array>> = self.builders.iter().map(|b| b.finish_cloned()).collect();

        let pairs: Vec<(Field, Arc<dyn Array>)> =
            fields.into_iter().zip(arrays.into_iter()).collect();

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

/// Append a value to a column.
///
/// Errors if the value is of an unsupported type.
///
/// Panics if the array builder is not the expected type. This would indicated a
/// programmer error.
fn append_value<'a>(val: RawBsonRef<'a>, col: &mut dyn ArrayBuilder) -> Result<()> {
    match val {
        RawBsonRef::Double(v) => col
            .as_any_mut()
            .downcast_mut::<Float64Builder>()
            .unwrap()
            .append_value(v),
        RawBsonRef::String(v) => col
            .as_any_mut()
            .downcast_mut::<StringBuilder>()
            .unwrap()
            .append_value(v),
        RawBsonRef::Boolean(v) => col
            .as_any_mut()
            .downcast_mut::<BooleanBuilder>()
            .unwrap()
            .append_value(v),
        RawBsonRef::Int32(v) => col
            .as_any_mut()
            .downcast_mut::<Float64Builder>()
            .unwrap()
            .append_value(v as f64),
        RawBsonRef::Int64(v) => col
            .as_any_mut()
            .downcast_mut::<Float64Builder>()
            .unwrap()
            .append_value(v as f64),
        RawBsonRef::Timestamp(v) => col
            .as_any_mut()
            .downcast_mut::<TimestampMillisecondBuilder>() // TODO: Possibly change to nanosecond.
            .unwrap()
            .append_value(v.time as i64),
        RawBsonRef::DateTime(v) => col
            .as_any_mut()
            .downcast_mut::<TimestampMillisecondBuilder>() // TODO: Possibly change to nanosecond.
            .unwrap()
            .append_value(v.timestamp_millis()),
        RawBsonRef::Binary(v) => col
            .as_any_mut()
            .downcast_mut::<BinaryBuilder>()
            .unwrap()
            .append_value(v.bytes), // TODO: Subtype?
        RawBsonRef::ObjectId(v) => col
            .as_any_mut()
            .downcast_mut::<StringBuilder>()
            .unwrap()
            .append_value(v.to_string()),
        RawBsonRef::Document(nested) => {
            let builder = col
                .as_any_mut()
                .downcast_mut::<RecordStructBuilder>()
                .unwrap();
            builder.append_record(nested)?;
        }
        RawBsonRef::Array(_arr) => col
            .as_any_mut()
            .downcast_mut::<StringBuilder>()
            .unwrap()
            .append_value("RAW ARRAY (unimplemented)"),
        RawBsonRef::Decimal128(v) => col
            .as_any_mut()
            .downcast_mut::<Decimal128Builder>()
            .unwrap()
            .append_value(i128::from_le_bytes(v.bytes())),
        _ => return Err(MongoError::UnsupportedBsonType("Other")), // TODO: Match on all types.
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
    fields: &[Field],
    capacity: usize,
) -> Result<Vec<Box<dyn ArrayBuilder>>> {
    let mut cols = Vec::with_capacity(capacity);

    for field in fields {
        let col: Box<dyn ArrayBuilder> = match field.data_type() {
            &DataType::Boolean => Box::new(BooleanBuilder::with_capacity(capacity)),
            &DataType::Int32 => Box::new(Int32Builder::with_capacity(capacity)),
            &DataType::Int64 => Box::new(Int64Builder::with_capacity(capacity)),
            &DataType::Float64 => Box::new(Float64Builder::with_capacity(capacity)),
            &DataType::Timestamp(_, _) => {
                Box::new(TimestampMillisecondBuilder::with_capacity(capacity)) // TODO: Possibly change to nanosecond.
            }
            &DataType::Utf8 => Box::new(StringBuilder::with_capacity(capacity, 10)), // TODO: Can collect avg when inferring schema.
            &DataType::Binary => Box::new(BinaryBuilder::with_capacity(capacity, 10)), // TODO: Can collect avg when inferring schema.
            &DataType::Decimal128(_, _) => Box::new(Decimal128Builder::with_capacity(capacity)), // TODO: Can collect avg when inferring schema.
            &DataType::Struct(ref fields) => {
                let nested = column_builders_for_fields(fields, capacity)?;
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

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use bitvec::order::Lsb0;
use bitvec::vec::BitVec;
use bson::{Bson, RawBsonRef, RawDocumentBuf};
use datafusion::arrow::array::{
    Array,
    ArrayBuilder,
    ArrayRef,
    BinaryBuilder,
    BooleanBuilder,
    Date32Builder,
    Date64Builder,
    Decimal128Builder,
    Float16Builder,
    Float32Builder,
    Float64Builder,
    Int16Builder,
    Int32Builder,
    Int64Builder,
    Int8Builder,
    LargeBinaryBuilder,
    LargeStringBuilder,
    StringBuilder,
    StructArray,
    TimestampMicrosecondBuilder,
    TimestampMillisecondBuilder,
    TimestampNanosecondBuilder,
    TimestampSecondBuilder,
};
use datafusion::arrow::datatypes::{DataType, Field, Fields, TimeUnit};
use mysql_common::bigdecimal::{FromPrimitive, ToPrimitive};

use crate::bson::errors::{BsonError, Result};
use crate::common::util::try_parse_datetime;

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
            return Err(BsonError::RecordStructBuilderInvalidArgs);
        }
        if builders.is_empty() {
            return Err(BsonError::RecordStructBuilderRequiresColumns);
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

    pub fn len(&self) -> usize {
        match self.builders.first() {
            Some(elem) => elem.len(),
            None => 0,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn append_nulls(&mut self) -> Result<()> {
        for (builder, field) in self.builders.iter_mut().zip(self.fields.iter()) {
            append_null(field.data_type(), builder.as_mut())?;
        }
        Ok(())
    }

    pub fn append_record(&mut self, doc: &RawDocumentBuf) -> Result<()> {
        let mut cols_set: BitVec<u8, Lsb0> = BitVec::repeat(false, self.fields.len());

        for item in doc.iter_elements() {
            let elem = item?;

            let idx = *self
                .field_index
                .get(elem.key())
                .ok_or_else(|| BsonError::ColumnNotInInferredSchema(elem.key().to_string()))?;

            if *cols_set.get(idx).unwrap() {
                continue;
            }

            // Add value to columns.
            self.add_value_at_index(idx, Some(elem.value()?))?;

            // Track which columns we've added values to.
            cols_set.set(idx, true);
        }

        // Append nulls to all columns not included in the doc.
        for (idx, did_set) in cols_set.iter().enumerate() {
            if !did_set {
                // Add null...
                self.add_value_at_index(idx, None)?;
            }
        }

        Ok(())
    }

    pub fn append_value(&mut self, doc: &RawDocumentBuf) -> Result<()> {
        let mut cols_set: BitVec<u8, Lsb0> = BitVec::repeat(false, self.fields.len());

        for iter_result in doc.iter_elements() {
            match iter_result {
                Ok(elem) => {
                    if let Some(&idx) = self.field_index.get(elem.key()) {
                        if cols_set.get(idx).is_some_and(|v| v == true) {
                            // If this happens it means that the bson document has a field
                            // name that appears more than once. This is legal and possible to build
                            // with some libraries but isn't forbidden, and (I think?) historically
                            // not (always?) rejected by MongoDB. Regardless "ignoring second
                            // appearances of the key" is a reasonable semantic.
                            continue;
                        }

                        // Add value to columns.
                        self.add_value_at_index(idx, Some(elem.value()?))?;

                        // Track which columns we've added values to.
                        cols_set.set(idx, true);
                    };
                }
                Err(_) => return Err(BsonError::FailedToReadRawBsonDocument),
            }
        }

        // Append nulls to all columns not included in the doc.
        for (idx, did_set) in cols_set.iter().enumerate() {
            if !did_set {
                // Add null...
                self.add_value_at_index(idx, None)?;
            }
        }

        Ok(())
    }

    pub fn into_builders(self) -> Vec<Box<dyn ArrayBuilder>> {
        self.builders
    }

    fn add_value_at_index(&mut self, idx: usize, val: Option<RawBsonRef>) -> Result<()> {
        let typ = self.fields.get(idx).unwrap().data_type(); // Programmer error if data type doesn't exist.
        let col = self.builders.get_mut(idx).unwrap(); // Programmer error if column doesn't exist.

        match val {
            Some(v) => append_value(v, typ, col.as_mut()),
            None => append_null(typ, col.as_mut()),
        }
    }
}

impl ArrayBuilder for RecordStructBuilder {
    fn len(&self) -> usize {
        self.builders.first().unwrap().len()
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn finish(&mut self) -> ArrayRef {
        let fields = std::mem::take(&mut self.fields);
        let builders = std::mem::take(&mut self.builders);
        let arrays = builders.into_iter().map(|mut b| b.finish());

        let pairs: Vec<(Arc<Field>, Arc<dyn Array>)> =
            fields.into_iter().cloned().zip(arrays).collect();

        let array: StructArray = pairs.into();

        Arc::new(array)
    }

    fn finish_cloned(&self) -> ArrayRef {
        let arrays: Vec<Arc<dyn Array>> = self.builders.iter().map(|b| b.finish_cloned()).collect();

        let pairs: Vec<(Arc<Field>, Arc<dyn Array>)> =
            self.fields.iter().cloned().zip(arrays).collect();

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
        // null
        (RawBsonRef::Null, _) => append_null(typ, col)?,
        (RawBsonRef::Undefined, _) => append_null(typ, col)?,

        // Boolean
        (RawBsonRef::Boolean(v), DataType::Boolean) => append_scalar!(BooleanBuilder, col, v),
        (RawBsonRef::Boolean(v), DataType::Int8) => append_scalar!(Int8Builder, col, v.into()),
        (RawBsonRef::Boolean(v), DataType::Int16) => append_scalar!(Int16Builder, col, v.into()),
        (RawBsonRef::Boolean(v), DataType::Int32) => append_scalar!(Int32Builder, col, v.into()),
        (RawBsonRef::Boolean(v), DataType::Int64) => append_scalar!(Int64Builder, col, v.into()),
        (RawBsonRef::Boolean(v), DataType::Float16) => {
            append_scalar!(Float16Builder, col, half::f16::from_f32(v.into()))
        }
        (RawBsonRef::Boolean(v), DataType::Float32) => {
            append_scalar!(Float32Builder, col, v.into())
        }
        (RawBsonRef::Boolean(v), DataType::Float64) => {
            append_scalar!(Float64Builder, col, v.into())
        }
        (RawBsonRef::Boolean(v), DataType::Utf8) => {
            append_scalar!(StringBuilder, col, v.to_string())
        }
        (RawBsonRef::Boolean(v), DataType::LargeUtf8) => {
            append_scalar!(LargeStringBuilder, col, v.to_string())
        }
        (RawBsonRef::Boolean(v), DataType::Binary) => {
            append_scalar!(BinaryBuilder, col, v.into())
        }
        (RawBsonRef::Boolean(v), DataType::LargeBinary) => {
            append_scalar!(LargeBinaryBuilder, col, v.into())
        }

        // Double
        (RawBsonRef::Double(v), DataType::Int8) => append_scalar!(
            Int8Builder,
            col,
            v.to_i8()
                .ok_or_else(|| BsonError::UnexpectedDataTypeForBuilder(typ.to_owned()))?
        ),
        (RawBsonRef::Double(v), DataType::Int16) => {
            append_scalar!(
                Int16Builder,
                col,
                v.to_i16()
                    .ok_or_else(|| BsonError::UnexpectedDataTypeForBuilder(typ.to_owned()))?
            )
        }
        (RawBsonRef::Double(v), DataType::Int32) => append_scalar!(Int32Builder, col, v as i32),
        (RawBsonRef::Double(v), DataType::Int64) => append_scalar!(Int64Builder, col, v as i64),
        (RawBsonRef::Double(v), DataType::Float16) => {
            append_scalar!(Float16Builder, col, half::f16::from_f64(v))
        }
        (RawBsonRef::Double(v), DataType::Float32) => {
            append_scalar!(Float32Builder, col, v as f32)
        }
        (RawBsonRef::Double(v), DataType::Float64) => append_scalar!(Float64Builder, col, v),
        (RawBsonRef::Double(v), DataType::Utf8) => {
            append_scalar!(StringBuilder, col, v.to_string())
        }
        (RawBsonRef::Double(v), DataType::LargeUtf8) => {
            append_scalar!(LargeStringBuilder, col, v.to_string())
        }
        (RawBsonRef::Double(v), DataType::Binary) => {
            append_scalar!(BinaryBuilder, col, v.into())
        }
        (RawBsonRef::Double(v), DataType::LargeBinary) => {
            append_scalar!(LargeBinaryBuilder, col, v.into())
        }

        // Int32
        (RawBsonRef::Int32(v), DataType::Int8) => append_scalar!(Int8Builder, col, v as i8),
        (RawBsonRef::Int32(v), DataType::Int16) => append_scalar!(Int16Builder, col, v as i16),
        (RawBsonRef::Int32(v), DataType::Int32) => append_scalar!(Int32Builder, col, v),
        (RawBsonRef::Int32(v), DataType::Int64) => append_scalar!(Int64Builder, col, v as i64),
        (RawBsonRef::Int32(v), DataType::Float16) => {
            append_scalar!(
                Float16Builder,
                col,
                half::f16::from_i32(v)
                    .ok_or_else(|| BsonError::UnexpectedDataTypeForBuilder(typ.to_owned()))?
            )
        }
        (RawBsonRef::Int32(v), DataType::Float32) => append_scalar!(Float32Builder, col, v as f32),
        (RawBsonRef::Int32(v), DataType::Float64) => append_scalar!(Float64Builder, col, v as f64),
        (RawBsonRef::Int32(v), DataType::Utf8) => {
            append_scalar!(StringBuilder, col, v.to_string())
        }
        (RawBsonRef::Int32(v), DataType::LargeUtf8) => {
            append_scalar!(LargeStringBuilder, col, v.to_string())
        }
        (RawBsonRef::Int32(v), DataType::Binary) => {
            append_scalar!(BinaryBuilder, col, v.into())
        }
        (RawBsonRef::Int32(v), DataType::LargeBinary) => {
            append_scalar!(LargeBinaryBuilder, col, v.into())
        }

        // Int64
        (RawBsonRef::Int64(v), DataType::Int8) => append_scalar!(Int8Builder, col, v as i8),
        (RawBsonRef::Int64(v), DataType::Int16) => append_scalar!(Int16Builder, col, v as i16),
        (RawBsonRef::Int64(v), DataType::Int32) => append_scalar!(Int32Builder, col, v),
        (RawBsonRef::Int64(v), DataType::Int64) => append_scalar!(Int64Builder, col, v as i64),
        (RawBsonRef::Int64(v), DataType::Float16) => {
            append_scalar!(
                Float16Builder,
                col,
                half::f16::from_i64(v)
                    .ok_or_else(|| BsonError::UnexpectedDataTypeForBuilder(typ.to_owned()))?
            )
        }
        (RawBsonRef::Int64(v), DataType::Float32) => append_scalar!(Float32Builder, col, v as f32),
        (RawBsonRef::Int64(v), DataType::Float64) => append_scalar!(Float64Builder, col, v as f64),
        (RawBsonRef::Int64(v), DataType::Utf8) => {
            append_scalar!(StringBuilder, col, v.to_string())
        }
        (RawBsonRef::Int64(v), DataType::LargeUtf8) => {
            append_scalar!(LargeStringBuilder, col, v.to_string())
        }
        (RawBsonRef::Int64(v), DataType::Binary) => {
            append_scalar!(BinaryBuilder, col, v.into())
        }
        (RawBsonRef::Int64(v), DataType::LargeBinary) => {
            append_scalar!(LargeBinaryBuilder, col, v.into())
        }

        // String
        (RawBsonRef::String(v), DataType::Utf8) => append_scalar!(StringBuilder, col, v),
        (RawBsonRef::String(v), DataType::LargeUtf8) => append_scalar!(LargeStringBuilder, col, v),
        (RawBsonRef::String(v), DataType::Boolean) => {
            append_scalar!(BooleanBuilder, col, v.parse().unwrap_or_default())
        }
        (RawBsonRef::String(v), DataType::Int64) => {
            append_scalar!(Int64Builder, col, v.parse().unwrap_or_default())
        }
        (RawBsonRef::String(v), DataType::Int32) => {
            append_scalar!(Int32Builder, col, v.parse().unwrap_or_default())
        }
        (RawBsonRef::String(v), DataType::Int16) => {
            append_scalar!(Int32Builder, col, v.parse().unwrap_or_default())
        }
        (RawBsonRef::String(v), DataType::Int8) => {
            append_scalar!(Int32Builder, col, v.parse().unwrap_or_default())
        }
        (RawBsonRef::String(v), DataType::Float64) => {
            append_scalar!(Float64Builder, col, v.parse().unwrap_or_default())
        }
        (RawBsonRef::String(v), DataType::Float32) => {
            append_scalar!(Float32Builder, col, v.parse().unwrap_or_default())
        }
        (RawBsonRef::String(v), DataType::Float16) => {
            append_scalar!(Float16Builder, col, v.parse().unwrap_or_default())
        }
        (RawBsonRef::String(v), DataType::Date64) => {
            append_scalar!(
                Date64Builder,
                col,
                try_parse_datetime(v)?.timestamp_millis()
            )
        }
        (RawBsonRef::String(v), DataType::Date32) => {
            append_scalar!(
                Date32Builder,
                col,
                try_parse_datetime(v)?.timestamp() as i32
            )
        }
        (RawBsonRef::String(v), DataType::Timestamp(TimeUnit::Millisecond, _)) => {
            append_scalar!(
                TimestampMillisecondBuilder,
                col,
                try_parse_datetime(v)?.timestamp_millis()
            )
        }
        (RawBsonRef::String(v), DataType::Timestamp(TimeUnit::Microsecond, _)) => {
            append_scalar!(
                TimestampMicrosecondBuilder,
                col,
                try_parse_datetime(v)?.timestamp_micros()
            )
        }
        (RawBsonRef::String(v), DataType::Timestamp(TimeUnit::Second, _)) => {
            append_scalar!(
                TimestampSecondBuilder,
                col,
                try_parse_datetime(v)?.timestamp()
            )
        }

        // ObjectId
        (RawBsonRef::ObjectId(v), DataType::Binary) => {
            append_scalar!(BinaryBuilder, col, v.bytes())
        }
        (RawBsonRef::ObjectId(v), DataType::Utf8) => {
            append_scalar!(StringBuilder, col, v.to_string())
        }
        (RawBsonRef::ObjectId(v), DataType::LargeBinary) => {
            append_scalar!(LargeBinaryBuilder, col, v.bytes())
        }
        (RawBsonRef::ObjectId(v), DataType::LargeUtf8) => {
            append_scalar!(LargeStringBuilder, col, v.to_string())
        }

        // Timestamp (internal mongodb type; second specified)
        (RawBsonRef::Timestamp(v), DataType::Timestamp(TimeUnit::Second, _)) => {
            append_scalar!(TimestampSecondBuilder, col, v.time as i64)
        }
        (RawBsonRef::Timestamp(v), DataType::Timestamp(TimeUnit::Millisecond, _)) => {
            append_scalar!(TimestampSecondBuilder, col, v.time as i64 * 1000)
        }
        (RawBsonRef::Timestamp(v), DataType::Timestamp(TimeUnit::Microsecond, _)) => {
            append_scalar!(TimestampSecondBuilder, col, v.time as i64 * 1000 * 1000)
        }
        (RawBsonRef::Timestamp(v), DataType::Date64) => {
            append_scalar!(Date64Builder, col, v.time as i64 * 1000)
        }
        (RawBsonRef::Timestamp(v), DataType::Date32) => {
            append_scalar!(
                Date32Builder,
                col,
                v.time
                    .try_into()
                    .map_err(|_| BsonError::UnhandledElementType(
                        bson::spec::ElementType::Timestamp,
                        DataType::Date32
                    ))?
            )
        }
        (RawBsonRef::Timestamp(v), DataType::Utf8) => {
            append_scalar!(
                StringBuilder,
                col,
                chrono::DateTime::from_timestamp_millis(v.time as i64 * 1000)
                    .ok_or_else(|| BsonError::InvalidValue(v.to_string()))?
                    .to_rfc3339()
            )
        }
        (RawBsonRef::Timestamp(v), DataType::LargeUtf8) => {
            append_scalar!(
                LargeStringBuilder,
                col,
                chrono::DateTime::from_timestamp_millis(v.time as i64 * 1000)
                    .ok_or_else(|| BsonError::InvalidValue(v.to_string()))?
                    .to_rfc3339()
            )
        }

        // Datetime (actual timestamps that you'd actually use in an application)
        (RawBsonRef::DateTime(v), DataType::Timestamp(TimeUnit::Second, _)) => {
            append_scalar!(TimestampSecondBuilder, col, v.timestamp_millis() / 1000)
        }
        (RawBsonRef::DateTime(v), DataType::Timestamp(TimeUnit::Millisecond, _)) => {
            append_scalar!(TimestampMillisecondBuilder, col, v.timestamp_millis())
        }
        (RawBsonRef::DateTime(v), DataType::Timestamp(TimeUnit::Microsecond, _)) => {
            append_scalar!(
                TimestampMicrosecondBuilder,
                col,
                v.timestamp_millis() * 1000
            )
        }
        (RawBsonRef::DateTime(v), DataType::Timestamp(TimeUnit::Nanosecond, _)) => {
            append_scalar!(
                TimestampNanosecondBuilder,
                col,
                v.timestamp_millis() * 1000 * 1000
            )
        }
        (RawBsonRef::DateTime(v), DataType::Date64) => {
            append_scalar!(Date64Builder, col, v.timestamp_millis())
        }
        (RawBsonRef::DateTime(v), DataType::Date32) => {
            append_scalar!(Date32Builder, col, (v.timestamp_millis() / 1000) as i32)
        }
        (RawBsonRef::DateTime(v), DataType::Utf8) => {
            append_scalar!(
                StringBuilder,
                col,
                chrono::DateTime::from_timestamp_millis(v.timestamp_millis())
                    .ok_or_else(|| BsonError::InvalidValue(v.to_string()))?
                    .to_rfc3339()
            )
        }
        (RawBsonRef::DateTime(v), DataType::LargeUtf8) => {
            append_scalar!(
                LargeStringBuilder,
                col,
                chrono::DateTime::from_timestamp_millis(v.timestamp_millis())
                    .ok_or_else(|| BsonError::InvalidValue(v.to_string()))?
                    .to_rfc3339()
            )
        }
        (RawBsonRef::DateTime(v), DataType::Int64) => {
            append_scalar!(Int64Builder, col, v.timestamp_millis())
        }

        // Array
        (RawBsonRef::Document(doc), DataType::Struct(_)) => {
            append_scalar!(RecordStructBuilder, col, &doc.to_raw_document_buf())?
        }
        (RawBsonRef::Document(doc), DataType::Binary) => {
            append_scalar!(BinaryBuilder, col, doc.as_bytes())
        }
        (RawBsonRef::Document(doc), DataType::LargeBinary) => {
            append_scalar!(BinaryBuilder, col, doc.as_bytes())
        }
        (RawBsonRef::Document(doc), DataType::Utf8) => {
            append_scalar!(
                StringBuilder,
                col,
                Bson::Document(bson::Document::from_reader(doc.as_bytes())?)
                    .into_relaxed_extjson()
                    .to_string()
            )
        }
        (RawBsonRef::Document(doc), DataType::LargeUtf8) => {
            append_scalar!(
                LargeStringBuilder,
                col,
                Bson::Document(bson::Document::from_reader(doc.as_bytes())?)
                    .into_relaxed_extjson()
                    .to_string()
            )
        }

        // Array
        (RawBsonRef::Array(doc), DataType::Struct(_)) => append_scalar!(
            RecordStructBuilder,
            col,
            &RawDocumentBuf::from_bytes(doc.as_bytes().into())?
        )?,
        (RawBsonRef::Array(arr), DataType::Binary) => {
            append_scalar!(BinaryBuilder, col, arr.as_bytes())
        }
        (RawBsonRef::Array(arr), DataType::LargeBinary) => {
            append_scalar!(BinaryBuilder, col, arr.as_bytes())
        }
        (RawBsonRef::Array(arr), DataType::Utf8) => {
            append_scalar!(
                StringBuilder,
                col,
                Bson::Array(bson::Array::try_from(arr)?)
                    .into_relaxed_extjson()
                    .to_string()
            )
        }
        (RawBsonRef::Array(arr), DataType::LargeUtf8) => {
            append_scalar!(
                LargeStringBuilder,
                col,
                Bson::Array(bson::Array::try_from(arr)?)
                    .into_relaxed_extjson()
                    .to_string()
            )
        }

        // Decimal128
        (RawBsonRef::Decimal128(v), DataType::Decimal128(_, _)) => {
            append_scalar!(Decimal128Builder, col, i128::from_le_bytes(v.bytes()))
        }

        // Binary
        (RawBsonRef::Binary(v), DataType::Binary) => append_scalar!(BinaryBuilder, col, v.bytes),
        (RawBsonRef::Binary(v), DataType::LargeBinary) => {
            append_scalar!(LargeBinaryBuilder, col, v.bytes)
        }

        (_, DataType::Binary) => append_scalar!(BinaryBuilder, col, bson::ser::to_vec(&val)?),
        (_, DataType::LargeBinary) => {
            append_scalar!(LargeBinaryBuilder, col, bson::ser::to_vec(&val)?)
        }
        (_, DataType::Utf8) => append_scalar!(
            StringBuilder,
            col,
            bson::Bson::try_from(val)?
                .into_relaxed_extjson()
                .to_string()
        ),
        (_, DataType::LargeUtf8) => append_scalar!(
            LargeStringBuilder,
            col,
            bson::Bson::try_from(val)?
                .into_relaxed_extjson()
                .to_string()
        ),

        (bson_ref, dt) => {
            return Err(BsonError::UnhandledElementType(
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
        &DataType::Int8 => col
            .as_any_mut()
            .downcast_mut::<Int8Builder>()
            .unwrap()
            .append_null(),
        &DataType::Int16 => col
            .as_any_mut()
            .downcast_mut::<Int16Builder>()
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
        &DataType::Float32 => col
            .as_any_mut()
            .downcast_mut::<Float32Builder>()
            .unwrap()
            .append_null(),
        &DataType::Float16 => col
            .as_any_mut()
            .downcast_mut::<Float165Builder>()
            .unwrap()
            .append_null(),
        &DataType::Timestamp(TimeUnit::Nanosecond, _) => col
            .as_any_mut()
            .downcast_mut::<TimestampNanosecondBuilder>()
            .unwrap()
            .append_null(),
        &DataType::Timestamp(TimeUnit::Microsecond, _) => col
            .as_any_mut()
            .downcast_mut::<TimestampMillisecondBuilder>()
            .unwrap()
            .append_null(),
        &DataType::Timestamp(TimeUnit::Millisecond, _) => col
            .as_any_mut()
            .downcast_mut::<TimestampMillisecondBuilder>()
            .unwrap()
            .append_null(),
        &DataType::Timestamp(TimeUnit::Second, _) => col
            .as_any_mut()
            .downcast_mut::<TimestampSecondBuilder>()
            .unwrap()
            .append_null(),
        &DataType::Date64 => col
            .as_any_mut()
            .downcast_mut::<Date64Builder>()
            .unwrap()
            .append_null(),
        &DataType::Date32 => col
            .as_any_mut()
            .downcast_mut::<Date32Builder>()
            .unwrap()
            .append_null(),
        &DataType::Utf8 => col
            .as_any_mut()
            .downcast_mut::<StringBuilder>()
            .unwrap()
            .append_null(),
        &DataType::LargeUtf8 => col
            .as_any_mut()
            .downcast_mut::<LargeStringBuilder>()
            .unwrap()
            .append_null(),
        &DataType::Binary => col
            .as_any_mut()
            .downcast_mut::<BinaryBuilder>()
            .unwrap()
            .append_null(),
        &DataType::LargeBinary => col
            .as_any_mut()
            .downcast_mut::<LargeBinaryBuilder>()
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
        other => return Err(BsonError::UnexpectedDataTypeForBuilder(other.clone())),
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
            DataType::Timestamp(TimeUnit::Second, _) => {
                Box::new(TimestampSecondBuilder::with_capacity(capacity))
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                Box::new(TimestampMicrosecondBuilder::with_capacity(capacity))
            }
            DataType::Timestamp(TimeUnit::Millisecond, _) => {
                Box::new(TimestampMillisecondBuilder::with_capacity(capacity))
            }
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                Box::new(TimestampNanosecondBuilder::with_capacity(capacity))
            }
            DataType::Date64 => Box::new(Date64Builder::with_capacity(capacity)),
            DataType::Date32 => Box::new(Date32Builder::with_capacity(capacity)),
            DataType::Utf8 => Box::new(StringBuilder::with_capacity(capacity, 10)), // TODO: Can collect avg when inferring schema.
            DataType::LargeUtf8 => Box::new(LargeStringBuilder::with_capacity(capacity, 10)), // TODO: Can collect avg when inferring schema.
            DataType::Binary => Box::new(BinaryBuilder::with_capacity(capacity, 10)), // TODO: Can collect avg when inferring schema.
            DataType::LargeBinary => Box::new(LargeBinaryBuilder::with_capacity(capacity, 10)), // TODO: Can collect avg when inferring schema.
            DataType::Decimal128(_, _) => Box::new(Decimal128Builder::with_capacity(capacity)), // TODO: Can collect avg when inferring schema.
            DataType::Struct(fields) => {
                let nested = column_builders_for_fields(fields.clone(), capacity)?;
                Box::new(RecordStructBuilder::new_with_builders(
                    fields.clone(),
                    nested,
                )?)
            }
            other => return Err(BsonError::UnexpectedDataTypeForBuilder(other.clone())),
        };

        cols.push(col);
    }

    Ok(cols)
}

#[cfg(test)]
mod test {
    use bson::oid::ObjectId;

    use super::*;

    #[test]
    fn test_duplicate_field_handling() {
        let fields = Fields::from_iter(vec![
            Field::new("_id", DataType::Binary, true),
            Field::new("idx", DataType::Int64, true),
            Field::new("value", DataType::Utf8, true),
        ]);
        let mut rsb = RecordStructBuilder::new_with_capacity(fields, 100).unwrap();
        for idx in 0..100 {
            let mut buf = bson::RawDocumentBuf::new();

            buf.append("_id", ObjectId::new());
            buf.append("idx", idx as i64);
            buf.append("value", "first");
            buf.append("value", "second");
            assert_eq!(buf.iter().count(), 4);

            rsb.append_record(&buf).unwrap();
        }
        assert_eq!(rsb.len(), 100);
        for value in rsb
            .builders
            .get_mut(2)
            .unwrap()
            .as_any_mut()
            .downcast_mut::<StringBuilder>()
            .unwrap()
            .finish_cloned()
            .iter()
        {
            let v = value.unwrap();
            assert_eq!(v, "first");
        }
    }

    #[test]
    fn test_unexpected_schema_change() {
        let fields = Fields::from_iter(vec![
            Field::new("_id", DataType::Binary, true),
            Field::new("idx", DataType::Int64, true),
            Field::new("value", DataType::Utf8, true),
        ]);
        let mut rsb = RecordStructBuilder::new_with_capacity(fields, 100).unwrap();
        let mut buf = bson::RawDocumentBuf::new();

        buf.append("_id", ObjectId::new());
        buf.append("idx", 0);
        buf.append("value", "first");
        assert_eq!(buf.iter().count(), 3);

        rsb.append_record(&buf)
            .expect("first record matchex expectations");
        assert_eq!(rsb.len(), 1);

        let mut buf = bson::RawDocumentBuf::new();
        buf.append("index", 1);
        buf.append("values", 3);
        assert_eq!(buf.iter().count(), 2);
        rsb.append_record(&buf.clone())
            .expect_err("for append_record schema changes are an error");
        assert_eq!(rsb.len(), 1);
        rsb.append_value(&buf.clone())
            .expect("project and append should filter out unrequired fields");
        assert_eq!(rsb.len(), 2);

        let mut buf = bson::RawDocumentBuf::new();
        buf.append("_id", ObjectId::new());
        buf.append("index", 1);
        buf.append("values", 3);
        assert_eq!(buf.iter().count(), 3);

        rsb.append_record(&buf)
            .expect_err("for append_record schema changes are an error");
        // the first value was added successfully to another buffer to the rsb grew
        assert_eq!(rsb.len(), 3);

        rsb.append_value(&buf.clone())
            .expect("project and append should filter out unrequired fields");
        assert_eq!(rsb.len(), 4);
    }
}

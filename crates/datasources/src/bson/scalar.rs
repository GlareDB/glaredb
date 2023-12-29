use std::sync::Arc;

use bson::{Bson, Document};
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::scalar::ScalarValue;

use crate::bson::errors::{BsonError, Result, RECURSION_LIMIT};

pub fn from_document(doc: Document) -> Result<ScalarValue> {
    let (fields, values) = unwind_document(0, doc.to_owned().iter())?;
    Ok(ScalarValue::Struct(Some(values), fields.into()))
}

fn unwind_document<'a>(
    depth: usize,
    doc_iter: impl Iterator<Item = (&'a String, &'a Bson)>,
) -> Result<(Vec<Field>, Vec<ScalarValue>)> {
    if depth >= RECURSION_LIMIT {
        return Err(BsonError::RecursionLimitExceeded(RECURSION_LIMIT));
    }

    let (_, size) = doc_iter.size_hint();
    let mut fields = Vec::with_capacity(size.unwrap_or_default());
    let mut values = Vec::with_capacity(size.unwrap_or_default());

    for (key, val) in doc_iter {
        let (dt, sv) = bson_to_arrow(depth, val)?;

        // Assume everything is nullable.
        fields.push(Field::new(key, dt, true));
        values.push(sv)
    }

    Ok((fields, values))
}

fn bson_to_arrow(depth: usize, bson: &Bson) -> Result<(DataType, ScalarValue)> {
    Ok(match bson {
        Bson::Array(elems) => {
            let mut dt: Option<DataType> = None;
            let mut values = Vec::with_capacity(elems.len());
            for elem in elems {
                let (edt, sv) = bson_to_arrow(depth + 1, elem)?;
                if dt.is_none() {
                    Ok(dt.replace(edt))
                } else if !edt.equals_datatype(dt.as_ref().unwrap()) {
                    Err(BsonError::IncorrectType(
                        edt,
                        dt.as_ref().unwrap().to_owned(),
                    ))
                } else {
                    Ok(None)
                }?;

                values.push(sv);
            }

            let feild_ref = Arc::new(Field::new("", dt.unwrap_or(DataType::Null), true));
            (
                DataType::List(feild_ref.clone()),
                ScalarValue::List(Some(values), feild_ref.clone()),
            )
        }
        Bson::Document(nested) => {
            let (fields, values) = unwind_document(depth + 1, nested.iter())?;

            (
                DataType::Struct(fields.clone().into()),
                ScalarValue::Struct(Some(values), fields.clone().into()),
            )
        }
        Bson::String(v) => (DataType::Utf8, ScalarValue::Utf8(Some(v.to_owned()))),
        Bson::Double(v) => (DataType::Float64, ScalarValue::Float64(Some(v.to_owned()))),
        Bson::Boolean(v) => (DataType::Boolean, ScalarValue::Boolean(Some(v.to_owned()))),
        Bson::Null => (DataType::Null, ScalarValue::Null),
        Bson::Undefined => (DataType::Null, ScalarValue::Null),
        Bson::Int32(v) => (DataType::Int32, ScalarValue::Int32(Some(v.to_owned()))),
        Bson::Int64(v) => (DataType::Int64, ScalarValue::Int64(Some(v.to_owned()))),
        Bson::Binary(v) => (
            DataType::Binary,
            ScalarValue::Binary(Some(v.bytes.to_vec().to_owned())),
        ),
        Bson::ObjectId(v) => (
            DataType::Binary,
            ScalarValue::Binary(Some(v.bytes().into_iter().collect())),
        ),
        Bson::DateTime(v) => (
            DataType::Date64,
            ScalarValue::Date64(Some(v.to_owned().timestamp_millis())),
        ),
        Bson::Symbol(v) => (DataType::Utf8, ScalarValue::Utf8(Some(v.to_owned()))),
        Bson::RegularExpression(v) => {
            // TODO: capture the options
            (
                DataType::Utf8,
                ScalarValue::Utf8(Some(v.pattern.to_owned())),
            )
        }
        Bson::JavaScriptCode(v) => (DataType::Utf8, ScalarValue::Utf8(Some(v.to_owned()))),
        Bson::Decimal128(v) => (
            DataType::Decimal128(38, 10),
            ScalarValue::Decimal128(Some(i128::from_le_bytes(v.bytes())), 38, 10),
        ),
        Bson::MaxKey => (DataType::Utf8, ScalarValue::Utf8(None)),
        Bson::MinKey => (DataType::Utf8, ScalarValue::Utf8(None)),
        // Deprecated or MongoDB server intrenal types
        Bson::JavaScriptCodeWithScope(_) => return Err(BsonError::UnspportedType("CodeWithScope")),
        Bson::Timestamp(_) => return Err(BsonError::UnspportedType("OplogTimestamp")),
        Bson::DbPointer(_) => return Err(BsonError::UnspportedType("DbPointer")),
    })
}

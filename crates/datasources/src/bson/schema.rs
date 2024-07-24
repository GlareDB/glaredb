use std::collections::HashMap;
use std::iter::IntoIterator;

use bson::{RawBsonRef, RawDocumentBuf};
use datafusion::arrow::datatypes::{DataType, Field, Schema};

use crate::bson::errors::{BsonError, Result};

/// Recursion limit for inferring the schema for nested documents.
///
/// The MongoDB kernel rejects nesting of greater than 100.
const RECURSION_LIMIT: usize = 100;

pub fn schema_from_document(doc: &RawDocumentBuf) -> Result<Schema> {
    Ok(Schema::new(fields_from_document(0, doc)?))
}

fn fields_from_document(depth: usize, doc: &RawDocumentBuf) -> Result<Vec<Field>> {
    if depth >= RECURSION_LIMIT {
        return Err(BsonError::RecursionLimitExceeded(RECURSION_LIMIT));
    }

    let iter = doc.iter_elements();
    let (_, size) = iter.size_hint();
    let mut fields = Vec::with_capacity(size.unwrap_or_default());

    for item in iter {
        let elem = item?;
        let arrow_typ = bson_to_arrow_type(depth, elem.value()?)?;

        // Assume everything is nullable.
        fields.push(Field::new(elem.key(), arrow_typ, true));
    }

    Ok(fields)
}

fn bson_to_arrow_type(depth: usize, bson: RawBsonRef) -> Result<DataType> {
    Ok(match bson {
        RawBsonRef::Array(doc) => DataType::Struct(
            fields_from_document(
                depth + 1,
                &RawDocumentBuf::from_bytes(doc.as_bytes().into())?,
            )?
            .into(),
        ),
        RawBsonRef::Document(doc) => {
            DataType::Struct(fields_from_document(depth + 1, &doc.to_raw_document_buf())?.into())
        }
        RawBsonRef::String(_) => DataType::Utf8,
        RawBsonRef::Double(_) => DataType::Float64,
        RawBsonRef::Boolean(_) => DataType::Boolean,
        RawBsonRef::Null => DataType::Null,
        RawBsonRef::Undefined => DataType::Null,
        RawBsonRef::Int32(_) => DataType::Int32,
        RawBsonRef::Int64(_) => DataType::Int64,
        RawBsonRef::Binary(_) => DataType::Binary,
        RawBsonRef::ObjectId(_) => DataType::Binary,
        RawBsonRef::DateTime(_) => DataType::Date64,
        RawBsonRef::Symbol(_) => DataType::Utf8,
        RawBsonRef::Decimal128(_) => DataType::Decimal128(38, 10),
        RawBsonRef::RegularExpression(_) => DataType::Utf8,
        RawBsonRef::JavaScriptCode(_) => DataType::Utf8,

        // storing these values (which exist to establish a total
        // order of types for indexing in the MongoDB server,) in
        // documents that GlareDB would interact with is probably
        // always an error.
        RawBsonRef::MaxKey => return Err(BsonError::UnspportedType("maxKey")),
        RawBsonRef::MinKey => return Err(BsonError::UnspportedType("minKey")),

        // Deprecated or MongoDB server intrenal types
        RawBsonRef::JavaScriptCodeWithScope(_) => {
            return Err(BsonError::UnspportedType("CodeWithScope"))
        }
        RawBsonRef::Timestamp(_) => return Err(BsonError::UnspportedType("OplogTimestamp")),
        RawBsonRef::DbPointer(_) => return Err(BsonError::UnspportedType("DbPointer")),
    })
}

#[derive(Debug, Clone)]
struct OrderedField(usize, Field);

pub fn merge_schemas(schemas: impl IntoIterator<Item = Result<Schema>>) -> Result<Schema> {
    let mut fields: HashMap<String, OrderedField> = HashMap::new();

    for schema in schemas.into_iter() {
        let schema = schema?;
        for (idx, field) in schema.fields.into_iter().enumerate() {
            match fields.get_mut(field.name()) {
                Some(existing) => {
                    merge_field(&mut existing.1, field)?;
                }
                None => {
                    fields.insert(
                        field.name().clone(),
                        OrderedField(idx, field.as_ref().clone()),
                    );
                }
            };
        }
    }

    let mut fields: Vec<_> = fields.into_values().collect();
    fields.sort_unstable_by(|a, b| a.0.cmp(&b.0));

    // Collect all fields.
    //
    // Note there's special handling for null types in that we'll just set those
    // to strings since we're able to handle that better when actually building
    // the columns.
    let fields: Vec<_> = fields
        .into_iter()
        .map(|f| {
            if f.1.data_type() == &DataType::Null {
                f.1.with_data_type(DataType::Utf8)
            } else {
                f.1
            }
        })
        .collect();

    Ok(Schema::new(fields))
}

/// Merge fields with best-effort type widening.
fn merge_field(left: &mut Field, right: &Field) -> Result<()> {
    let dt = match (left.data_type(), right.data_type()) {
        (&DataType::Null, right) => right.clone(),
        (&DataType::Int32, &DataType::Int64) => DataType::Int64,
        (&DataType::Int32 | &DataType::Int64 | &DataType::Float64, &DataType::Float64) => {
            DataType::Float64
        }
        (_, &DataType::Utf8) => DataType::Utf8,
        _ => return Ok(()),
    };

    *left = Field::new(left.name(), dt, true);
    Ok(())
}

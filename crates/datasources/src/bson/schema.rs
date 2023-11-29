use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use std::collections::HashMap;

use crate::bson::errors::{BsonError, Result};
use bson::{Bson, Document};

/// Recursion limit for inferring the schema for nested documents.
///
/// The MongoDB kernel rejects nesting of greater than 100.
const RECURSION_LIMIT: usize = 100;

pub fn schema_from_document(doc: &Document) -> Result<Schema> {
    let fields = fields_from_document(0, doc)?;
    Ok(Schema::new(fields))
}

fn fields_from_document(depth: usize, doc: &Document) -> Result<Vec<Field>> {
    if depth >= RECURSION_LIMIT {
        return Err(BsonError::RecursionLimitExceeded(RECURSION_LIMIT));
    }

    let doc_iter = doc.iter();
    let (_, size) = doc_iter.size_hint();
    let mut fields = Vec::with_capacity(size.unwrap_or_default());

    for (key, val) in doc_iter {
        let arrow_typ = bson_to_arrow_type(depth, val)?;

        // Assume everything is nullable.
        fields.push(Field::new(key, arrow_typ, true));
    }

    Ok(fields)
}

fn bson_to_arrow_type(depth: usize, bson: &Bson) -> Result<DataType> {
    let arrow_typ = match bson {
        Bson::Double(_) => DataType::Float64,
        Bson::String(val) => {
            if val.is_empty() {
                // TODO: We'll want to determine if this is something we should
                // keep in. Currently when we load in the test file, we're
                // loading a csv that might have empty values (null). Mongo
                // reads those in and puts them as empty strings.
                //
                // During schema merges, we'll widen "null" types to whatever
                // type we're merging with. This _should_ be more resilient to
                // empty values.
                DataType::Null
            } else {
                DataType::Utf8
            }
        }
        Bson::Array(array_doc) => match array_doc.to_owned().pop() {
            Some(val) => bson_to_arrow_type(0, &val)?,
            None => DataType::Utf8,
        },
        Bson::Document(nested) => {
            let fields = fields_from_document(depth + 1, nested)?;
            DataType::Struct(fields.into())
        }
        Bson::Boolean(_) => DataType::Boolean,
        Bson::Null => DataType::Null,
        Bson::RegularExpression(_) => DataType::Utf8,
        Bson::JavaScriptCode(_) => DataType::Utf8,
        Bson::JavaScriptCodeWithScope(_) => {
            return Err(BsonError::UnsupportedBsonType("CodeWithScope"))
        }
        Bson::Int32(_) => DataType::Float64,
        Bson::Int64(_) => DataType::Float64,
        Bson::Timestamp(_) => return Err(BsonError::UnsupportedBsonType("OplogTimestamp")),
        Bson::Binary(_) => DataType::Binary,
        Bson::ObjectId(_) => DataType::Utf8,
        Bson::DateTime(_) => DataType::Timestamp(TimeUnit::Millisecond, None),
        Bson::Symbol(_) => DataType::Utf8,
        Bson::Decimal128(_) => DataType::Decimal128(38, 10),
        Bson::Undefined => DataType::Null,
        Bson::MaxKey => DataType::Utf8,
        Bson::MinKey => DataType::Utf8,
        Bson::DbPointer(_) => return Err(BsonError::UnsupportedBsonType("DbPointer")),
    };
    Ok(arrow_typ)
}

#[derive(Debug, Clone)]
struct OrderedField(usize, Field);

pub fn merge_schemas(schemas: impl IntoIterator<Item = Schema>) -> Result<Schema> {
    let mut fields: HashMap<String, OrderedField> = HashMap::new();

    for schema in schemas.into_iter() {
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
        (&DataType::Int32 | &DataType::Int64 | &DataType::Float64, &DataType::Float64) => {
            DataType::Float64
        }
        (_, &DataType::Utf8) => DataType::Utf8,
        _ => return Ok(()),
    };

    *left = Field::new(left.name(), dt, true);
    Ok(())
}

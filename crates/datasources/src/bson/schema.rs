use std::collections::HashMap;
use std::iter::IntoIterator;

use bson::{Bson, Document};
use datafusion::arrow::datatypes::{DataType, Field, Schema};

use crate::bson::errors::{BsonError, Result};

/// Recursion limit for inferring the schema for nested documents.
///
/// The MongoDB kernel rejects nesting of greater than 100.
const RECURSION_LIMIT: usize = 100;

pub fn schema_from_document(doc: Document) -> Result<Schema> {
    Ok(Schema::new(fields_from_document(0, doc.iter())?))
}

fn fields_from_document<'a>(
    depth: usize,
    doc_iter: impl Iterator<Item = (&'a String, &'a Bson)>,
) -> Result<Vec<Field>> {
    if depth >= RECURSION_LIMIT {
        return Err(BsonError::RecursionLimitExceeded(RECURSION_LIMIT));
    }

    // let doc_iter = doc.iter();
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
    Ok(match bson {
        Bson::Array(array_doc) => DataType::new_list(
            bson_to_arrow_type(0, &array_doc.to_owned().pop().or(Some(Bson::Null)).unwrap())?,
            true,
        ),
        Bson::Document(nested) => {
            DataType::Struct(fields_from_document(depth + 1, nested.iter())?.into())
        }
        Bson::String(_) => DataType::Utf8,
        Bson::Double(_) => DataType::Float64,
        Bson::Boolean(_) => DataType::Boolean,
        Bson::Null => DataType::Null,
        Bson::Int32(_) => DataType::Float64,
        Bson::Int64(_) => DataType::Float64,
        Bson::Binary(_) => DataType::Binary,
        Bson::ObjectId(_) => DataType::Utf8,
        Bson::DateTime(_) => DataType::Date64,
        Bson::Symbol(_) => DataType::Utf8,
        Bson::Decimal128(_) => DataType::Decimal128(38, 10),
        Bson::Undefined => DataType::Null,
        Bson::RegularExpression(_) => DataType::Utf8,
        Bson::JavaScriptCode(_) => DataType::Utf8,

        // TODO: storing these (which exist to establish a total order
        // of types for indexing in the MongoDB server,) in documents
        // that GlareDB would interact with is probably always an
        // error.
        Bson::MaxKey => DataType::Utf8,
        Bson::MinKey => DataType::Utf8,

        // Deprecated or MongoDB server intrenal types
        Bson::JavaScriptCodeWithScope(_) => return Err(BsonError::UnspportedType("CodeWithScope")),
        Bson::Timestamp(_) => return Err(BsonError::UnspportedType("OplogTimestamp")),
        Bson::DbPointer(_) => return Err(BsonError::UnspportedType("DbPointer")),
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
        (&DataType::Int32 | &DataType::Int64 | &DataType::Float64, &DataType::Float64) => {
            DataType::Float64
        }
        (_, &DataType::Utf8) => DataType::Utf8,
        _ => return Ok(()),
    };

    *left = Field::new(left.name(), dt, true);
    Ok(())
}

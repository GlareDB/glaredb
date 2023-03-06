use crate::errors::{MongoError, Result};
use datafusion::arrow::datatypes::{
    DataType, Field, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef, TimeUnit, UnionMode,
};
use futures::{StreamExt, TryStreamExt};
use mongodb::bson::{doc, Bson, Document};
use mongodb::{options::ClientOptions, Client, Collection};
use std::collections::HashSet;
use tracing::debug;

/// How many documents to sample for schema inferrence.
const SAMPLE_COUNT: usize = 1;

/// Recursion limit for inferring the schema for nested documents.
const RECURSION_LIMIT: usize = 5;

pub struct TableSampler {
    collection: Collection<Document>,
}

impl TableSampler {
    pub fn new(collection: Collection<Document>) -> TableSampler {
        TableSampler { collection }
    }

    #[tracing::instrument(skip(self))]
    pub async fn infer_schema_from_sample(&self) -> Result<ArrowSchema> {
        let sample_pipeline = [doc! {
            "$sample": {"size": SAMPLE_COUNT as i64}
        }];

        let mut cursor = self.collection.aggregate(sample_pipeline, None).await?;

        let mut schemas = Vec::with_capacity(SAMPLE_COUNT);
        while let Some(doc) = cursor.try_next().await? {
            let schema = schema_from_document(&doc)?;
            schemas.push(schema);
        }

        let merged = ArrowSchema::try_merge(schemas).map_err(MongoError::FailedSchemaMerge)?;

        Ok(merged)
    }
}

fn schema_from_document(doc: &Document) -> Result<ArrowSchema> {
    let fields = fields_from_document(0, doc)?;
    Ok(ArrowSchema::new(fields))
}

fn fields_from_document(depth: usize, doc: &Document) -> Result<Vec<Field>> {
    if depth >= RECURSION_LIMIT {
        return Err(MongoError::RecursionLimitExceeded(RECURSION_LIMIT));
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
        Bson::String(_) => DataType::Utf8,
        Bson::Array(_) => DataType::Utf8, // TODO: Proper type
        Bson::Document(nested) => {
            let fields = fields_from_document(depth + 1, nested)?;
            DataType::Struct(fields)
        }
        Bson::Boolean(_) => DataType::Boolean,
        Bson::Null => DataType::Null,
        Bson::RegularExpression(_) => DataType::Utf8,
        Bson::JavaScriptCode(_) => DataType::Utf8,
        Bson::JavaScriptCodeWithScope(_) => DataType::Utf8,
        Bson::Int32(_) => DataType::Float64,
        Bson::Int64(_) => DataType::Float64,
        Bson::Timestamp(_) => DataType::Timestamp(TimeUnit::Second, None),
        Bson::Binary(_) => DataType::Binary, // TODO: Subtype?
        Bson::ObjectId(_) => DataType::Utf8,
        Bson::DateTime(_) => DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".to_string())),
        Bson::Symbol(_) => DataType::Utf8,
        Bson::Decimal128(_) => DataType::Decimal128(38, 9),
        Bson::Undefined => DataType::Null,
        Bson::MaxKey => DataType::Utf8,
        Bson::MinKey => DataType::Utf8,
        Bson::DbPointer(_) => return Err(MongoError::UnsupportedBsonType("DbPointer")),
    };
    Ok(arrow_typ)
}

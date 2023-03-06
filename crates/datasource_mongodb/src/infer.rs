use crate::errors::{MongoError, Result};
use datafusion::arrow::datatypes::{
    DataType, Field, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef, TimeUnit,
};
use futures::{StreamExt, TryStreamExt};
use mongodb::bson::{doc, Bson, Document};
use mongodb::{options::ClientOptions, Client, Collection};

const SAMPLE_COUNT: usize = 10;

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
            "$sample": {"size": SAMPLE_COUNT.to_string()}
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
    let doc_iter = doc.iter();
    let (_, size) = doc_iter.size_hint();
    let mut fields = Vec::with_capacity(size.unwrap_or_default());

    for (key, val) in doc_iter {
        let arrow_typ = match val {
            Bson::Double(_) => DataType::Float64,
            Bson::String(_) => DataType::Utf8,
            Bson::Array(_) => return Err(MongoError::UnsupportedBsonType("Array")),
            Bson::Document(_) => return Err(MongoError::UnsupportedBsonType("Document")),
            Bson::Boolean(_) => DataType::Boolean,
            Bson::Null => return Err(MongoError::UnsupportedBsonType("Null")),
            Bson::RegularExpression(_) => {
                return Err(MongoError::UnsupportedBsonType("RegularExpression"))
            }
            Bson::JavaScriptCode(_) => {
                return Err(MongoError::UnsupportedBsonType("JavaScriptCode"))
            }
            Bson::JavaScriptCodeWithScope(_) => {
                return Err(MongoError::UnsupportedBsonType("JavaScriptCodeWithScope"))
            }
            Bson::Int32(_) => DataType::Int32,
            Bson::Int64(_) => DataType::Int64,
            Bson::Timestamp(_) => DataType::Timestamp(TimeUnit::Second, None),
            Bson::Binary(_) => DataType::Binary, // TODO: Subtype?
            Bson::ObjectId(_) => return Err(MongoError::UnsupportedBsonType("ObjectId")),
            Bson::DateTime(_) => {
                DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".to_string()))
            }
            Bson::Symbol(_) => return Err(MongoError::UnsupportedBsonType("Symbol")),
            Bson::Decimal128(_) => return Err(MongoError::UnsupportedBsonType("Decimal128")),
            Bson::Undefined => return Err(MongoError::UnsupportedBsonType("Undefined")),
            Bson::MaxKey => return Err(MongoError::UnsupportedBsonType("MaxKey")),
            Bson::MinKey => return Err(MongoError::UnsupportedBsonType("MinKey")),
            Bson::DbPointer(_) => return Err(MongoError::UnsupportedBsonType("DbPointer")),
        };

        // Assume everything is nullable.
        fields.push(Field::new(key, arrow_typ, true));
    }

    Ok(ArrowSchema::new(fields))
}

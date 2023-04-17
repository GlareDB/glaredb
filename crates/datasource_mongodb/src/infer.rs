use crate::errors::{MongoError, Result};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema, TimeUnit};
use futures::TryStreamExt;
use mongodb::bson::{doc, Bson, Document};
use mongodb::Collection;
use std::collections::HashMap;

const SAMPLE_PCT: f32 = 0.01;
const MAX_SAMPLE_SIZE: usize = 30;

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
        let count = self.collection.estimated_document_count(None).await?;
        let mut sample_count = (count as f32 * SAMPLE_PCT) as i64;
        if sample_count as usize > MAX_SAMPLE_SIZE {
            sample_count = MAX_SAMPLE_SIZE as i64;
        }
        // Very small table.
        if sample_count == 0 {
            sample_count = MAX_SAMPLE_SIZE as i64;
        }

        let sample_pipeline = [doc! {
            "$sample": {"size": sample_count}
        }];

        let mut cursor = self.collection.aggregate(sample_pipeline, None).await?;

        let mut schemas = Vec::with_capacity(sample_count as usize);
        while let Some(doc) = cursor.try_next().await? {
            let schema = schema_from_document(&doc)?;
            schemas.push(schema);
        }

        // Note that we're not using `try_merge` since that errors on type
        // mismatch. Since mongo is schemaless, we want to be best effort with
        // defining a schema, so we merge schemas in such a way that each field
        // has its data type set to the "widest" type that we encountered.
        let merged = merge_schemas(schemas)?;

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
        Bson::Array(_) => DataType::Utf8, // TODO: Proper type.
        Bson::Document(nested) => {
            let fields = fields_from_document(depth + 1, nested)?;
            DataType::Struct(fields.into())
        }
        Bson::Boolean(_) => DataType::Boolean,
        Bson::Null => DataType::Null,
        Bson::RegularExpression(_) => DataType::Utf8,
        Bson::JavaScriptCode(_) => DataType::Utf8,
        Bson::JavaScriptCodeWithScope(_) => DataType::Utf8,
        Bson::Int32(_) => DataType::Float64,
        Bson::Int64(_) => DataType::Float64,
        Bson::Timestamp(_) => DataType::Timestamp(TimeUnit::Microsecond, None), // TODO: Nanosecond
        Bson::Binary(_) => DataType::Binary,                                    // TODO: Subtype?
        Bson::ObjectId(_) => DataType::Utf8,
        Bson::DateTime(_) => DataType::Timestamp(TimeUnit::Microsecond, None), // TODO: Nanosecond
        Bson::Symbol(_) => DataType::Utf8,
        Bson::Decimal128(_) => DataType::Decimal128(38, 10),
        Bson::Undefined => DataType::Null,
        Bson::MaxKey => DataType::Utf8,
        Bson::MinKey => DataType::Utf8,
        Bson::DbPointer(_) => return Err(MongoError::UnsupportedBsonType("DbPointer")),
    };
    Ok(arrow_typ)
}

#[derive(Debug, Clone)]
struct OrderedField(usize, Field);

fn merge_schemas(schemas: impl IntoIterator<Item = ArrowSchema>) -> Result<ArrowSchema> {
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

    let fields: Vec<_> = fields.into_iter().map(|f| f.1).collect();
    Ok(ArrowSchema::new(fields))
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

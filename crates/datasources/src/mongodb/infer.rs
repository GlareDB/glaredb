use super::errors::Result;
use bson::RawDocumentBuf;
use datafusion::arrow::datatypes::Schema as ArrowSchema;
use futures::TryStreamExt;

use mongodb::bson::{doc, Document};
use mongodb::Collection;

use crate::bson::schema::{merge_schemas, schema_from_document};

const SAMPLE_PCT: f32 = 0.05;

const MAX_SAMPLE_SIZE: usize = 100;
const MIN_SAMPLE_SIZE: usize = 10;

/// Sample a table to allow inferring the table's schema.
pub struct TableSampler {
    collection: Collection<Document>,
}

impl TableSampler {
    pub fn new(collection: Collection<Document>) -> TableSampler {
        TableSampler { collection }
    }

    /// Infer the schema by sampling the table.
    ///
    /// This will map bson value types to arrow data types, attempting to widen
    /// to the widest type encountered. For example, if we encounter an "Int64"
    /// and a "Utf8", the type will be automatically widened to "Utf8" in the
    /// final schema.
    #[tracing::instrument(skip(self))]
    pub async fn infer_schema_from_sample(&self) -> Result<ArrowSchema> {
        let count = self.collection.estimated_document_count(None).await?;
        let sample_count = Self::sample_size(count as usize) as i64;

        let sample_pipeline = [doc! {
            "$sample": {"size": sample_count}
        }];

        let mut cursor = self.collection.aggregate(sample_pipeline, None).await?;

        let mut schemas = Vec::with_capacity(sample_count as usize);
        while let Some(doc) = cursor.try_next().await? {
            let schema = schema_from_document(&RawDocumentBuf::from_document(&doc)?);
            schemas.push(schema);
        }

        // Note that we're not using arrow's `try_merge` since that errors on
        // type mismatch. Since mongo is schemaless, we want to be best effort
        // with defining a schema, so we merge schemas in such a way that each
        // field has its data type set to the "widest" type that we encountered.
        let merged = merge_schemas(schemas)?;

        Ok(merged)
    }

    fn sample_size(doc_count: usize) -> usize {
        let mut sample_count = (doc_count as f32 * SAMPLE_PCT) as usize;
        if sample_count > MAX_SAMPLE_SIZE {
            sample_count = MAX_SAMPLE_SIZE;
        }

        // Very small table.
        if sample_count < MIN_SAMPLE_SIZE {
            sample_count = MIN_SAMPLE_SIZE;
        }

        sample_count
    }
}

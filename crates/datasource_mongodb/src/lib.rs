//! MongoDB as a data source.
pub mod errors;

mod builder;
mod exec;
mod infer;

use crate::errors::Result;
use crate::exec::MongoBsonExec;
use crate::infer::TableSampler;
use async_trait::async_trait;
use datafusion::arrow::datatypes::{Schema as ArrowSchema, SchemaRef as ArrowSchemaRef};
use datafusion::datasource::TableProvider;
use datafusion::error::Result as DatafusionResult;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;
use mongodb::bson::{Document, RawDocumentBuf};
use mongodb::Collection;
use mongodb::{options::ClientOptions, Client};
use std::any::Any;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct MongoAccessInfo {
    pub connection_string: String,
}

#[derive(Debug, Clone)]
pub struct MongoAccessor {
    client: Client,
}

impl MongoAccessor {
    pub async fn connect(info: MongoAccessInfo) -> Result<MongoAccessor> {
        let mut opts = ClientOptions::parse(&info.connection_string).await?;
        opts.app_name = Some("GlareDB (MongoDB Data source)".to_string());
        let client = Client::with_options(opts)?;

        Ok(MongoAccessor { client })
    }

    pub fn into_table_accessor(self, info: MongoTableAccessInfo) -> MongoTableAccessor {
        MongoTableAccessor {
            info,
            client: self.client,
        }
    }
}

#[derive(Debug, Clone)]
pub struct MongoTableAccessInfo {
    pub database: String, // "Schema"
    pub collection: String,
}

#[derive(Debug, Clone)]
pub struct MongoTableAccessor {
    info: MongoTableAccessInfo,
    client: Client,
}

impl MongoTableAccessor {
    /// Validate that we can access the table.
    pub async fn validate(&self) -> Result<()> {
        let _ = self
            .client
            .database(&self.info.database)
            .collection::<Document>(&self.info.collection)
            .estimated_document_count(None)
            .await?;

        Ok(())
    }

    pub async fn into_table_provider(self) -> Result<MongoTableProvider> {
        let collection = self
            .client
            .database(&self.info.database)
            .collection(&self.info.collection);
        let sampler = TableSampler::new(collection);

        let schema = sampler.infer_schema_from_sample().await?;

        Ok(MongoTableProvider {
            schema: Arc::new(schema),
            collection: self
                .client
                .database(&self.info.database)
                .collection(&self.info.collection),
        })
    }
}

pub struct MongoTableProvider {
    schema: Arc<ArrowSchema>,
    collection: Collection<RawDocumentBuf>,
}

#[async_trait]
impl TableProvider for MongoTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filter_pushdown(
        &self,
        _filter: &Expr,
    ) -> DatafusionResult<TableProviderFilterPushDown> {
        Ok(TableProviderFilterPushDown::Inexact)
    }

    async fn scan(
        &self,
        _ctx: &SessionState,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> DatafusionResult<Arc<dyn ExecutionPlan>> {
        // Projection.
        //
        // Note that this projection will only project top-level fields. There
        // is not a way to project nested documents (at least when modelling
        // nested docs as a struct).
        let projected_schema = match projection {
            Some(projection) => Arc::new(self.schema.project(projection)?),
            None => self.schema.clone(),
        };

        // TODO: Filters.

        Ok(Arc::new(MongoBsonExec::new(
            projected_schema,
            self.collection.clone(),
            limit,
        )))
    }
}

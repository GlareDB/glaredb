//! MongoDB as a data source.
pub mod errors;

mod exec;
mod infer;

use crate::errors::{MongoError, Result};
use crate::infer::TableSampler;
use async_trait::async_trait;
use datafusion::arrow::datatypes::{
    DataType, Field, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef, TimeUnit,
};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::ScalarValue;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DatafusionResult};
use datafusion::execution::context::{SessionState, TaskContext};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::{
    display::DisplayFormatType, ExecutionPlan, Partitioning, RecordBatchStream,
    SendableRecordBatchStream, Statistics,
};
use mongodb::bson::{doc, Document};
use mongodb::{options::ClientOptions, Client};
use std::any::Any;
use std::borrow::{Borrow, Cow};
use std::fmt::{self, Write};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::net::TcpStream;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

#[derive(Debug, Clone)]
pub struct MongoAccessInfo {
    pub connection_string: String,
}

#[derive(Debug, Clone)]
pub struct MongoAccessor {
    info: MongoAccessInfo,
    client: Client,
}

impl MongoAccessor {
    pub async fn connect(info: MongoAccessInfo) -> Result<MongoAccessor> {
        let mut opts = ClientOptions::parse(&info.connection_string).await?;
        opts.app_name = Some("GlareDB (MongDB Data source)".to_string());
        let client = Client::with_options(opts)?;

        Ok(MongoAccessor { info, client })
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
        })
    }
}

pub struct MongoTableProvider {
    schema: Arc<ArrowSchema>,
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
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DatafusionResult<Arc<dyn ExecutionPlan>> {
        unimplemented!()
    }
}

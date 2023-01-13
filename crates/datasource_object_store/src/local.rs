use std::any::Any;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::config::ConfigOptions;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DatafusionResult};
use datafusion::execution::context::{SessionState, TaskContext};
use datafusion::logical_expr::{Expr, TableType};
use datafusion::parquet::arrow::ParquetRecordBatchStreamBuilder;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::display::DisplayFormatType;
use datafusion::physical_plan::file_format::{FileScanConfig, ParquetExec};
use datafusion::physical_plan::{
    ExecutionPlan, Partitioning, SendableRecordBatchStream, Statistics,
};
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectStorePath;
use object_store::{ObjectMeta, ObjectStore};
use serde::{Deserialize, Serialize};

use crate::errors::Result;
use crate::parquet::{ParquetObjectReader, SimpleParquetFileReaderFactory};

/// Information needed for accessing an Parquet file on local file system.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalTableAccess {
    pub location: String,
}

#[derive(Debug)]
pub struct LocalAccessor {
    /// Local filesystem  object store access info
    pub store: Arc<dyn ObjectStore>,
    /// Meta information for location/object
    pub meta: Arc<ObjectMeta>,
}

impl LocalAccessor {
    /// Setup accessor for Local file system
    pub async fn new(access: LocalTableAccess) -> Result<Self> {
        let store = Arc::new(LocalFileSystem::new()) as Arc<dyn ObjectStore>;

        let location = ObjectStorePath::from(access.location);
        tracing::trace!(?location, "path to file");
        let meta = Arc::new(store.head(&location).await?);
        //TODO: RUSTOM - REMOVE
        tracing::trace!(?meta, "META info");

        Ok(Self { store, meta })
    }

    pub async fn into_table_provider(self, predicate_pushdown: bool) -> Result<LocalTableProvider> {
        let reader = ParquetObjectReader {
            store: self.store.clone(),
            meta: self.meta.clone(),
            meta_size_hint: None,
        };

        let reader = ParquetRecordBatchStreamBuilder::new(reader).await?;

        let arrow_schema = reader.schema().clone();

        Ok(LocalTableProvider {
            predicate_pushdown,
            accessor: self,
            arrow_schema,
        })
    }
}

pub struct LocalTableProvider {
    predicate_pushdown: bool,
    accessor: LocalAccessor,
    /// Schema for parquet file
    arrow_schema: ArrowSchemaRef,
}

#[async_trait]
impl TableProvider for LocalTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        self.arrow_schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    async fn scan(
        &self,
        _ctx: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DatafusionResult<Arc<dyn ExecutionPlan>> {
        let predicate = if self.predicate_pushdown {
            filters
                .to_vec()
                .into_iter()
                .reduce(|accum, expr| accum.and(expr))
        } else {
            None
        };

        let base_config = FileScanConfig {
            object_store_url: ObjectStoreUrl::local_filesystem(), // to be ignored
            file_schema: self.arrow_schema.clone(),
            file_groups: Vec::new(),
            statistics: Statistics::default(),
            projection: projection.cloned(),
            limit,
            table_partition_cols: Vec::new(),
            output_ordering: None,
            config_options: ConfigOptions::new().into_shareable(),
        };

        let factory = Arc::new(SimpleParquetFileReaderFactory::new(
            self.accessor.store.clone(),
            base_config.clone(),
        ));

        let exec = ParquetExec::new(base_config, predicate, None)
            .with_parquet_file_reader_factory(factory)
            .with_pushdown_filters(self.predicate_pushdown);

        Ok(Arc::new(exec))
    }
}

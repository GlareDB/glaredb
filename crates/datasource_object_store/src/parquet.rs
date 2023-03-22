//! Helpers for handling parquet files.
use std::any::Any;
use std::ops::Range;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::common::ToDFSchema;
use datafusion::datasource::file_format::parquet::fetch_parquet_metadata;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DatafusionResult};
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::TableType;
use datafusion::optimizer::utils::conjunction;
use datafusion::parquet::arrow::async_reader::AsyncFileReader;
use datafusion::parquet::arrow::ParquetRecordBatchStreamBuilder;
use datafusion::parquet::errors::{ParquetError, Result as ParquetResult};
use datafusion::parquet::file::metadata::ParquetMetaData;
use datafusion::physical_expr::create_physical_expr;
use datafusion::physical_plan::file_format::{
    FileMeta, FileScanConfig, ParquetExec, ParquetFileReaderFactory,
};
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::{ExecutionPlan, Statistics};
use datafusion::prelude::Expr;
use futures::future::{BoxFuture, FutureExt, TryFutureExt};
use object_store::{ObjectMeta, ObjectStore};

use crate::errors::Result;
use crate::TableAccessor;

/// Custom `ParquetFileReaderFactory` to provide a custom datasource in
/// `ParquetExec` instead of datafusion Object Store registry
#[derive(Debug)]
pub struct SimpleParquetFileReaderFactory {
    /// Object Store to read from
    store: Arc<dyn ObjectStore>,
}

impl SimpleParquetFileReaderFactory {
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self { store }
    }
}

impl ParquetFileReaderFactory for SimpleParquetFileReaderFactory {
    fn create_reader(
        &self,
        _partition_index: usize,
        file_meta: FileMeta,
        metadata_size_hint: Option<usize>,
        _metrics: &ExecutionPlanMetricsSet,
    ) -> Result<Box<dyn AsyncFileReader + Send>, DataFusionError> {
        Ok(Box::new(ParquetObjectReader {
            store: self.store.clone(),
            meta: Arc::new(file_meta.object_meta),
            meta_size_hint: metadata_size_hint,
        }))
    }
}

/// A Parquet file reader using the `AsyncFileReader` interface.
#[derive(Debug)]
pub struct ParquetObjectReader {
    pub store: Arc<dyn ObjectStore>,
    pub meta: Arc<ObjectMeta>,
    pub meta_size_hint: Option<usize>,
}

/// Implement parquet's `AsyncFileReader` interface.
impl AsyncFileReader for ParquetObjectReader {
    fn get_bytes(&mut self, range: Range<usize>) -> BoxFuture<'_, ParquetResult<Bytes>> {
        self.store
            .get_range(&self.meta.location, range)
            .map_err(|e| ParquetError::General(format!("get bytes: {e}")))
            .boxed()
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<usize>>,
    ) -> BoxFuture<'_, ParquetResult<Vec<Bytes>>> {
        Box::pin(async move {
            self.store
                .get_ranges(&self.meta.location, &ranges)
                .await
                .map_err(|e| ParquetError::General(format!("get ranges: {e}")))
        })
    }

    fn get_metadata(&mut self) -> BoxFuture<'_, ParquetResult<Arc<ParquetMetaData>>> {
        Box::pin(async move {
            let metadata =
                fetch_parquet_metadata(self.store.as_ref(), &self.meta, self.meta_size_hint)
                    .await
                    .map_err(|e| ParquetError::General(format!("fetch metadata: {e}")))?;
            Ok(Arc::new(metadata))
        })
    }
}

/// Table provider for Parquet table
pub struct ParquetTableProvider<T>
where
    T: TableAccessor,
{
    pub(crate) predicate_pushdown: bool,
    pub(crate) accessor: T,
    /// Schema for parquet file
    pub(crate) arrow_schema: ArrowSchemaRef,
}

impl<T> ParquetTableProvider<T>
where
    T: TableAccessor,
{
    pub async fn from_table_accessor(
        accessor: T,
        predicate_pushdown: bool,
    ) -> Result<ParquetTableProvider<T>> {
        let reader = ParquetObjectReader {
            store: accessor.store().clone(),
            meta: accessor.object_meta().clone(),
            meta_size_hint: None,
        };

        let reader = ParquetRecordBatchStreamBuilder::new(reader).await?;

        let arrow_schema = reader.schema().clone();

        Ok(ParquetTableProvider {
            predicate_pushdown,
            accessor,
            arrow_schema,
        })
    }
}

#[async_trait]
impl<T> TableProvider for ParquetTableProvider<T>
where
    T: TableAccessor + 'static,
{
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        self.arrow_schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        ctx: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DatafusionResult<Arc<dyn ExecutionPlan>> {
        let predicate = if self.predicate_pushdown {
            // Adapted from df...
            if let Some(expr) = conjunction(filters.to_vec()) {
                // NOTE: Use the table schema (NOT file schema) here because `expr` may contain references to partition columns.
                let table_df_schema = self.arrow_schema.as_ref().clone().to_dfschema()?;
                let filter = create_physical_expr(
                    &expr,
                    &table_df_schema,
                    &self.arrow_schema,
                    ctx.execution_props(),
                )?;

                Some(filter)
            } else {
                None
            }
        } else {
            None
        };

        let file = self.accessor.object_meta().as_ref().clone().into();

        let base_config = FileScanConfig {
            // `object_store_url` will be ignored as we are providing a
            // `SimpleParquetFileReaderFactory` to `ParquetExec` to use instead of the
            // datafusion object store registry.
            object_store_url: ObjectStoreUrl::local_filesystem(),
            file_schema: self.arrow_schema.clone(),
            file_groups: vec![vec![file]],
            statistics: Statistics::default(),
            projection: projection.cloned(),
            limit,
            table_partition_cols: Vec::new(),
            output_ordering: None,
            infinite_source: false,
        };

        let factory = Arc::new(SimpleParquetFileReaderFactory::new(
            self.accessor.store().clone(),
        ));

        let exec = ParquetExec::new(base_config, predicate, None)
            .with_parquet_file_reader_factory(factory)
            .with_pushdown_filters(self.predicate_pushdown);

        Ok(Arc::new(exec))
    }
}

use std::{any::Any, fmt, ops::Deref, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    arrow::datatypes::SchemaRef as ArrowSchemaRef,
    datasource::TableProvider,
    error::{DataFusionError, Result as DatafusionResult},
    execution::context::{SessionState, TaskContext},
    logical_expr::{Expr, TableType},
    parquet::arrow::ParquetRecordBatchStreamBuilder,
    physical_expr::PhysicalSortExpr,
    physical_plan::{
        display::DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream,
        Statistics,
    },
};
use object_store::{
    gcp::GoogleCloudStorageBuilder, path::Path as ObjectStorePath, ObjectMeta, ObjectStore,
};
use serde::{Deserialize, Serialize};

use crate::{
    errors::Result,
    parquet::{ParquetFileStream, ParquetObjectReader, ParquetOpener, StreamState},
};

/// Information needed for accessing an external Parquet file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GcsTableAccess {
    /// GCS object store bucket name
    pub bucket_name: String,
    /// GCS object store service account credentials
    pub service_account_path: String,
    /// GCS object store table location
    pub location: String,
}

#[derive(Debug)]
pub struct GcsAccessor {
    access: GcsTableAccess,
    /// Object store access information
    pub object_store: Arc<dyn ObjectStore>,
}

impl GcsAccessor {
    /// Setup accessor for GCS
    pub async fn new(access: GcsTableAccess) -> Result<Self> {
        let object_store = Arc::new(
            GoogleCloudStorageBuilder::new()
                .with_service_account_path(access.service_account_path.clone())
                .with_bucket_name(access.bucket_name.clone())
                .build()?,
        ) as Arc<dyn ObjectStore>;

        Ok(Self {
            access,
            object_store,
        })
    }

    pub async fn into_table_provider(self, _predicate_pushdown: bool) -> Result<GcsTableProvider> {
        let location = ObjectStorePath::from(self.access.location.clone());
        let meta = self.object_store.head(&location).await?;

        let reader = ParquetObjectReader {
            store: self.object_store.clone(),
            meta: meta.clone(),
            meta_size_hint: None,
        };

        let reader = ParquetRecordBatchStreamBuilder::new(reader).await?;

        let arrow_schema = reader.schema().clone();

        Ok(GcsTableProvider {
            _predicate_pushdown,
            accessor: Arc::new(self),
            arrow_schema,
            meta: Arc::new(meta),
        })
    }
}

pub struct GcsTableProvider {
    _predicate_pushdown: bool,
    accessor: Arc<GcsAccessor>,
    arrow_schema: ArrowSchemaRef,
    /// Object store access information
    meta: Arc<ObjectMeta>,
}

#[async_trait]
impl TableProvider for GcsTableProvider {
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
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DatafusionResult<Arc<dyn ExecutionPlan>> {
        let exec = GcsExec {
            accessor: self.accessor.clone(),
            arrow_schema: self.arrow_schema.clone(),
            meta: self.meta.clone(),
        };
        let exec = Arc::new(exec) as Arc<dyn ExecutionPlan>;
        Ok(exec)
    }
}

/// Copy data from the source Postgres table using the binary copy protocol.
#[derive(Debug)]
struct GcsExec {
    accessor: Arc<GcsAccessor>,
    arrow_schema: ArrowSchemaRef,
    meta: Arc<ObjectMeta>,
}

impl ExecutionPlan for GcsExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        self.arrow_schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DatafusionResult<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Execution(
            "cannot replace children for BinaryCopyExec".to_string(),
        ))
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DatafusionResult<SendableRecordBatchStream> {
        let meta = self.meta.deref().clone();
        let opener = ParquetOpener {
            store: self.accessor.object_store.clone(),
            meta,
            meta_size_hint: None,
        };

        let stream = ParquetFileStream {
            schema: self.arrow_schema.clone(),
            opener,
            state: StreamState::Idle,
        };

        Ok(Box::pin(stream))
    }

    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "GcsExec: schema={}", self.arrow_schema)
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

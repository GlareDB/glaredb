use std::{any::Any, fmt, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    arrow::datatypes::SchemaRef as ArrowSchemaRef,
    datasource::TableProvider,
    error::{DataFusionError, Result as DatafusionResult},
    execution::context::{SessionState, TaskContext},
    logical_expr::{Expr, TableType},
    physical_expr::PhysicalSortExpr,
    physical_plan::{
        display::DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream,
        Statistics,
    },
};
use object_store::{gcp::GoogleCloudStorageBuilder, ObjectStore};
use serde::{Deserialize, Serialize};

use crate::errors::Result;

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
        todo!()
    }
}

pub struct GcsTableProvider {
    predicate_pushdown: bool,
    accessor: Arc<GcsAccessor>,
    arrow_schema: ArrowSchemaRef,
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
        todo!()
    }
}

/// Copy data from the source Postgres table using the binary copy protocol.
#[derive(Debug)]
struct GcsExec {
    arrow_schema: ArrowSchemaRef,
    // opener: StreamOpener,
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
        todo!()
    }

    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "GcsExec: schema={}", self.arrow_schema)
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

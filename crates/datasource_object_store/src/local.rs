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
use object_store::{local::LocalFileSystem, ObjectStore};
use serde::{Deserialize, Serialize};

use crate::errors::Result;

/// Information needed for accessing an external Parquet file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalTableAccess {
    /// Path to local file
    pub file: String,
}

#[derive(Debug)]
pub struct LocalAccessor {
    access: LocalTableAccess,
    /// Object store access information
    pub object_store: Arc<dyn ObjectStore>,
}

impl LocalAccessor {
    /// Setup accessor for GCS
    pub async fn new(access: LocalTableAccess) -> Result<Self> {
        let object_store = Arc::new(LocalFileSystem::new()) as Arc<dyn ObjectStore>;

        Ok(Self {
            access,
            object_store,
        })
    }

    pub async fn into_table_provider(
        self,
        _predicate_pushdown: bool,
    ) -> Result<LocalTableProvider> {
        todo!()
    }
}

pub struct LocalTableProvider {
    predicate_pushdown: bool,
    accessor: Arc<LocalAccessor>,
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
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DatafusionResult<Arc<dyn ExecutionPlan>> {
        todo!()
    }
}

/// Copy data from the source Postgres table using the binary copy protocol.
#[derive(Debug)]
struct LocalExec {
    arrow_schema: ArrowSchemaRef,
    // opener: StreamOpener,
}

impl ExecutionPlan for LocalExec {
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
        write!(f, "LocalExec: schema={}", self.arrow_schema)
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

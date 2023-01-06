use std::any::Any;
use std::fmt;
use std::ops::Deref;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DatafusionResult};
use datafusion::execution::context::{SessionState, TaskContext};
use datafusion::logical_expr::{Expr, TableType};
use datafusion::parquet::arrow::ParquetRecordBatchStreamBuilder;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::display::DisplayFormatType;
use datafusion::physical_plan::{
    ExecutionPlan, Partitioning, SendableRecordBatchStream, Statistics,
};
use object_store::local::LocalFileSystem;
use object_store::{ObjectMeta, ObjectStore};
use serde::{Deserialize, Serialize};

use crate::errors::Result;
use crate::parquet::{ParquetFileStream, ParquetObjectReader, ParquetOpener, StreamState};

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

    pub async fn into_table_provider(self, predicate_pushdown: bool) -> Result<LocalTableProvider> {
        let location = object_store::path::Path::from(self.access.file.clone());
        let meta = self.object_store.head(&location).await?;

        let reader = ParquetObjectReader {
            store: self.object_store.clone(),
            meta: meta.clone(),
            meta_size_hint: None,
        };

        let reader = ParquetRecordBatchStreamBuilder::new(reader).await?;

        let arrow_schema = reader.schema().clone();

        Ok(LocalTableProvider {
            _predicate_pushdown: predicate_pushdown,
            accessor: Arc::new(self),
            arrow_schema,
            meta: Arc::new(meta),
        })
    }
}

pub struct LocalTableProvider {
    _predicate_pushdown: bool,
    accessor: Arc<LocalAccessor>,
    /// Schema for parquet file
    arrow_schema: ArrowSchemaRef,
    /// Object store access information
    meta: Arc<ObjectMeta>,
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
        TableType::Base
    }

    async fn scan(
        &self,
        _ctx: &SessionState,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DatafusionResult<Arc<dyn ExecutionPlan>> {
        let exec = LocalExec {
            accessor: self.accessor.clone(),
            arrow_schema: self.arrow_schema.clone(),
            meta: self.meta.clone(),
        };
        let exec = Arc::new(exec) as Arc<dyn ExecutionPlan>;
        Ok(exec)
    }
}

#[derive(Debug)]
struct LocalExec {
    accessor: Arc<LocalAccessor>,
    arrow_schema: ArrowSchemaRef,
    meta: Arc<ObjectMeta>,
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
            "cannot replace children for ParquetExec".to_string(),
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
        write!(f, "ParquetExec: schema={}", self.arrow_schema)
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

use std::{any::Any, fmt, sync::Arc};

use async_trait::async_trait;
use datafusion::arrow::datatypes::{
    DataType, Field, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef,
};
use datafusion::{
    datasource::{file_format::parquet::ParquetFormat, listing::ListingTableUrl, TableProvider},
    error::{DataFusionError, Result as DatafusionResult},
    execution::context::{SessionState, TaskContext},
    logical_expr::{Expr, TableType},
    parquet::arrow::{
        arrow_reader::ParquetRecordBatchReaderBuilder, ParquetRecordBatchStreamBuilder,
    },
    physical_expr::PhysicalSortExpr,
    physical_plan::{
        display::DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream,
        Statistics,
    },
    prelude::ParquetReadOptions,
    row::accessor,
};
use object_store::{local::LocalFileSystem, ObjectStore};
use serde::{Deserialize, Serialize};
use tokio::fs::File;

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

    pub async fn into_table_provider(self, predicate_pushdown: bool) -> Result<LocalTableProvider> {
        let file = File::open(&self.access.file).await.unwrap();

        let arrow_schema = ParquetRecordBatchStreamBuilder::new(file)
            .await?
            .schema()
            .clone();

        Ok(LocalTableProvider {
            predicate_pushdown,
            accessor: Arc::new(self),
            arrow_schema,
        })
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
        TableType::Base
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

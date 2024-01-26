//! Table functions for triggering system-related functionality. Users are
//! unlikely to use these, but there's no harm if they do.
pub mod cache_external_tables;
pub mod remove_delta_tables;

use std::any::Any;
use std::fmt;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use cache_external_tables::CacheExternalDatabaseTablesOperation;
use datafusion::arrow::array::{Date64Builder, StringBuilder};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::context::SessionState;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::TableType;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs,
    DisplayFormatType,
    ExecutionPlan,
    Partitioning,
    SendableRecordBatchStream,
    Statistics,
};
use datafusion::prelude::Expr;
use futures::stream;
use once_cell::sync::Lazy;

use self::remove_delta_tables::DeleteDeltaTablesOperation;

/// A system operation can execute an arbitrary operation.
///
/// This should be focused on operations that do not require user interactions
/// (e.g. background operations like optimizing tables or running caching jobs).
#[derive(Clone)]
pub enum SystemOperation {
    CacheExternalTables(CacheExternalDatabaseTablesOperation),
    DeleteDeltaTables(DeleteDeltaTablesOperation),
}

impl SystemOperation {
    /// Name of the operation. Used for debugging as well as generating
    /// appropriate errors.
    pub fn name(&self) -> &'static str {
        match self {
            Self::CacheExternalTables(inner) => inner.name(),
            Self::DeleteDeltaTables(_) => DeleteDeltaTablesOperation::NAME,
        }
    }

    /// Create a boxed future for executing the operation.
    ///
    /// Accepts a datafusion context that's expected to have
    /// `NativeTabelStorage`.
    pub async fn execute(&self, context: Arc<TaskContext>) -> Result<(), DataFusionError> {
        match self {
            Self::CacheExternalTables(inner) => inner.execute(context).await?,
            Self::DeleteDeltaTables(inner) => inner.execute(context).await?,
        }
        Ok(())
    }
}

/// Schema of the output batch once the operation completes.
pub static SYSTEM_OPERATION_PHYSICAL_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("system_operation_name", DataType::Utf8, false),
        Field::new("started_at", DataType::Date64, false),
        Field::new("finished_at", DataType::Date64, false),
    ]))
});

/// Helper table provider to create an appropirate exec.
pub struct SystemOperationTableProvider {
    pub operation: SystemOperation,
}

#[async_trait]
impl TableProvider for SystemOperationTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Arc<Schema> {
        SYSTEM_OPERATION_PHYSICAL_SCHEMA.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &SessionState,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(SystemOperationExec {
            operation: self.operation.clone(),
            projection: projection.cloned(),
        }))
    }
}

/// A generic system operation plan.
///
/// Note that this isn't currently serializable over rpc. This is assuming that
/// the plan is created by calling a table function (which happens) on the
/// remote, and the execution plan is created from the resulting table provider.
///
/// We may want to look at making this serializable in the future (and callable
/// outside of a table func).
pub struct SystemOperationExec {
    operation: SystemOperation,
    projection: Option<Vec<usize>>,
}
impl SystemOperationExec {
    /// Create a new system operation exec.
    pub fn new(operation: SystemOperation) -> Self {
        Self {
            operation,
            projection: None,
        }
    }
}

impl ExecutionPlan for SystemOperationExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Arc<Schema> {
        match &self.projection {
            Some(proj) => Arc::new(SYSTEM_OPERATION_PHYSICAL_SCHEMA.project(proj).unwrap()),
            None => SYSTEM_OPERATION_PHYSICAL_SCHEMA.clone(),
        }
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
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Plan(
            "Cannot change children for SystemOperationExec".to_string(),
        ))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(
                "SystemOperationExec only supports 1 partition".to_string(),
            ));
        }

        let name = self.operation.name();
        let projection = self.projection.clone();
        let op = self.operation.clone();

        let out = stream::once(async move {
            let start = SystemTime::now();
            op.execute(context).await?;
            let end = SystemTime::now();

            let mut name_arr = StringBuilder::new();
            let mut start_arr = Date64Builder::new();
            let mut end_arr = Date64Builder::new();

            name_arr.append_value(name);
            start_arr.append_value(start.duration_since(UNIX_EPOCH).unwrap().as_millis() as i64);
            end_arr.append_value(end.duration_since(UNIX_EPOCH).unwrap().as_millis() as i64);

            let batch = RecordBatch::try_new(
                SYSTEM_OPERATION_PHYSICAL_SCHEMA.clone(),
                vec![
                    Arc::new(name_arr.finish()),
                    Arc::new(start_arr.finish()),
                    Arc::new(end_arr.finish()),
                ],
            )
            .unwrap();

            match &projection {
                Some(proj) => Ok(batch.project(proj).unwrap()),
                None => Ok(batch),
            }
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(self.schema(), out)))
    }

    fn statistics(&self) -> DataFusionResult<Statistics> {
        Ok(Statistics::new_unknown(self.schema().as_ref()))
    }
}

impl DisplayAs for SystemOperationExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SystemOperationExec: {}", self.operation.name())
    }
}

impl fmt::Debug for SystemOperationExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SystemOperationExec")
            .field("operation", &self.operation.name())
            .finish()
    }
}

use async_trait::async_trait;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::context::SessionState;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::TableType;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::{
    stream::RecordBatchStreamAdapter, DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning,
    SendableRecordBatchStream, Statistics,
};
use datafusion::prelude::Expr;
use datafusion_ext::system::SystemOperation;
use futures::{stream, Future};
use parking_lot::Mutex;
use std::any::Any;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;

use super::{new_operation_batch, GENERIC_OPERATION_PHYSICAL_SCHEMA};

/// Helper table provider for create an appropirate exec.
#[derive(Debug)]
pub struct SystemOperationTableProvider {
    pub operation: Arc<dyn SystemOperation>,
}

#[async_trait]
impl TableProvider for SystemOperationTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Arc<Schema> {
        GENERIC_OPERATION_PHYSICAL_SCHEMA.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &SessionState,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(SystemOperationExec {
            operation: self.operation.clone(),
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
#[derive(Debug)]
pub struct SystemOperationExec {
    pub operation: Arc<dyn SystemOperation>,
}

impl ExecutionPlan for SystemOperationExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Arc<Schema> {
        GENERIC_OPERATION_PHYSICAL_SCHEMA.clone()
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

        let fut = self.operation.create_future(context);
        let out = stream::once(async move {
            let _ = fut.await?;
            Ok(new_operation_batch("system_op"))
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(self.schema(), out)))
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

impl DisplayAs for SystemOperationExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SystemOperationExec: {}", self.operation.name())
    }
}

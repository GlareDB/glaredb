use datafusion::arrow::datatypes::Schema as ArrowSchema;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::expressions::PhysicalSortExpr;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream,
    Statistics,
};
use std::any::Any;
use std::fmt;
use std::sync::Arc;
use tonic::Streaming;
use uuid::Uuid;

use super::client::RemoteClient;
use crate::extension_codec::GlareDBExtensionCodec;

/// Send the results of an execution to the remote service.
#[derive(Debug, Clone)]
pub struct BroadcastJoinSendExec {
    input: Arc<dyn ExecutionPlan>,
    client: RemoteClient,
}

impl ExecutionPlan for BroadcastJoinSendExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Arc<ArrowSchema> {
        self.input.schema()
    }

    fn output_partitioning(&self) -> Partitioning {
        self.input.output_partitioning()
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        self.input.output_ordering()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        unimplemented!()
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(
                "RemoteLocalExec only supports 1 partition".to_string(),
            ));
        }

        unimplemented!()
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

impl DisplayAs for BroadcastJoinSendExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "BroadcastJoinSendExec")
    }
}

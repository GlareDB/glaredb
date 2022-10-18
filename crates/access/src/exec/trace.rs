use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::error::Result as ArrowResult;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result as DatafusionResult};
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::{
    display::DisplayFormatType, expressions::PhysicalSortExpr, ExecutionPlan, Partitioning,
    RecordBatchStream, SendableRecordBatchStream, Statistics,
};
use futures::{ready, stream::StreamExt, Stream};
use std::any::Any;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tracing::trace;

/// Trace results of a single execution plan.
#[derive(Debug)]
pub struct SingleOpaqueTraceExec {
    child: Arc<dyn ExecutionPlan>,
}

impl SingleOpaqueTraceExec {
    pub fn new(child: Arc<dyn ExecutionPlan>) -> SingleOpaqueTraceExec {
        SingleOpaqueTraceExec { child }
    }
}

impl ExecutionPlan for SingleOpaqueTraceExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.child.schema()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn relies_on_input_order(&self) -> bool {
        false
    }

    fn maintains_input_order(&self) -> bool {
        true
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.child.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DatafusionResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Execution(format!(
                "invalid number of children for tracing execution: {:?}",
                children
            )));
        }
        let child = children[0].clone();
        Ok(Arc::new(SingleOpaqueTraceExec::new(child)))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DatafusionResult<SendableRecordBatchStream> {
        let stream = self.child.execute(partition, context)?;
        trace!("execution stream create");
        Ok(Box::pin(TraceStream {
            schema: self.schema(),
            stream,
        }))
    }

    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SingleOpaqueTraceExec")
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

pub struct TraceStream {
    schema: SchemaRef,
    stream: SendableRecordBatchStream,
}

impl Stream for TraceStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match ready!(self.stream.poll_next_unpin(cx)) {
            Some(result) => {
                trace!(?result, "execution stream result");
                Poll::Ready(Some(result))
            }
            None => {
                trace!("execution stream complete");
                Poll::Ready(None)
            }
        }
    }
}

impl RecordBatchStream for TraceStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

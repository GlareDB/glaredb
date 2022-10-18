use crate::errors::{internal, Result};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::error::Result as ArrowResult;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::Result as DatafusionResult;
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::{
    display::DisplayFormatType, expressions::PhysicalSortExpr, ExecutionPlan, Partitioning,
    RecordBatchStream, SendableRecordBatchStream, Statistics,
};
use futures::stream::{self, Select, Stream, StreamExt};
use std::any::Any;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

// TODO: Provide optional limit as well.
#[derive(Debug)]
pub struct SelectUnorderedExec {
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
}

impl SelectUnorderedExec {
    pub fn new(left: Arc<dyn ExecutionPlan>, right: Arc<dyn ExecutionPlan>) -> Result<Self> {
        if left.schema() != right.schema() {
            return Err(internal!(
                "schemas do not match; left: {}, right: {}",
                left.schema(),
                right.schema()
            ));
        }
        Ok(SelectUnorderedExec { left, right })
    }
}

impl ExecutionPlan for SelectUnorderedExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.left.schema()
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
        false
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.left.clone(), self.right.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DatafusionResult<Arc<dyn ExecutionPlan>> {
        // Purposely ignore new children. `children` above use both by EXPLAINs
        // and the optimizer. The optimizer doesn't know anything about this
        // node, so just ignore it.
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DatafusionResult<SendableRecordBatchStream> {
        let left = self.left.execute(partition, context.clone())?;
        let right = self.right.execute(partition, context)?;
        Ok(Box::pin(SelectBatchStream {
            schema: self.schema(),
            inner: stream::select(left, right),
        }))
    }

    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SelectUnorderedExec",)
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

struct SelectBatchStream {
    schema: SchemaRef,
    inner: Select<SendableRecordBatchStream, SendableRecordBatchStream>,
}

impl Stream for SelectBatchStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.poll_next_unpin(cx)
    }
}

impl RecordBatchStream for SelectBatchStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

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
use futures::stream::{self, SelectAll, Stream, StreamExt};
use std::any::Any;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

/// Select batches from all children in any order.
// TODO: Provide optional limit as well.
#[derive(Debug)]
pub struct SelectUnorderedExec {
    children: Vec<Arc<dyn ExecutionPlan>>,
}

impl SelectUnorderedExec {
    /// Create a new executor selecting from any number of children.
    ///
    /// All children must produce batches with the same schema.
    pub fn new(children: impl IntoIterator<Item = Arc<dyn ExecutionPlan>>) -> Result<Self> {
        let mut iter = children.into_iter();
        let (lower, _) = iter.size_hint();
        let mut children = Vec::with_capacity(lower);

        let first = iter
            .next()
            .ok_or_else(|| internal!("select unordered must have at least one child"))?;
        let schema = first.schema();
        children.push(first);

        for child in iter {
            if child.schema() != schema {
                return Err(internal!("schema mismatch between children"));
            }
            children.push(child);
        }

        Ok(SelectUnorderedExec { children })
    }
}

impl ExecutionPlan for SelectUnorderedExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        // Always have at least one child.
        self.children.first().unwrap().schema()
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
        self.children.clone()
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
        let streams = self
            .children
            .iter()
            .map(|child| child.as_ref().execute(partition, context.clone()))
            .collect::<DatafusionResult<Vec<_>>>()?;
        let stream = stream::select_all(streams);
        Ok(Box::pin(SelectBatchStream {
            schema: self.schema(),
            inner: stream,
        }))
    }

    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "SelectUnorderedExec: num_children={}",
            self.children.len()
        )
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

struct SelectBatchStream {
    schema: SchemaRef,
    inner: SelectAll<SendableRecordBatchStream>,
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

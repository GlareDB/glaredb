use std::any::Any;
use std::fmt;
use std::sync::{Arc, Mutex};

use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::common::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::expressions::PhysicalSortExpr;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    DisplayAs,
    DisplayFormatType,
    ExecutionPlan,
    Partitioning,
    SendableRecordBatchStream,
    Statistics,
};

pub struct StreamExecPlan {
    stream: Mutex<Option<SendableRecordBatchStream>>,
    schema: Arc<Schema>,
    metrics: ExecutionPlanMetricsSet,
}

impl StreamExecPlan {
    pub fn new(schema: Arc<Schema>, stream: Mutex<Option<SendableRecordBatchStream>>) -> Self {
        Self {
            stream,
            schema,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl DisplayAs for StreamExecPlan {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "StreamExecPlan")
    }
}

impl fmt::Debug for StreamExecPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "StreamExecPlan: {:?}", self.schema)
    }
}

impl ExecutionPlan for StreamExecPlan {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
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
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            Err(DataFusionError::Execution(
                "cannot replace children for StreamExecPlan".to_string(),
            ))
        }
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        Ok(self.stream.lock().unwrap().take().unwrap())
    }

    fn statistics(&self) -> datafusion::error::Result<Statistics> {
        Ok(Statistics::new_unknown(self.schema().as_ref()))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

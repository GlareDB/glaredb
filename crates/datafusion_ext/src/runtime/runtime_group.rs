use datafusion::arrow::datatypes::Schema;
use datafusion::error::Result as DataFusionResult;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream,
    Statistics,
};
use protogen::metastore::types::catalog::RuntimePreference;
use std::any::Any;
use std::fmt;
use std::sync::Arc;

/// An execution plan with an associated runtime preference.
///
/// This does not alter execution of the plan in any way, and is just a way for
/// us to provide information during optimization.
#[derive(Debug, Clone)]
pub struct RuntimeGroupExec {
    pub preference: RuntimePreference,
    pub child: Arc<dyn ExecutionPlan>,
}

impl RuntimeGroupExec {
    pub fn new(preference: RuntimePreference, child: Arc<dyn ExecutionPlan>) -> Self {
        Self { preference, child }
    }
}

impl ExecutionPlan for RuntimeGroupExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Arc<Schema> {
        self.child.schema()
    }

    fn output_partitioning(&self) -> Partitioning {
        self.child.output_partitioning()
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        self.child.output_ordering()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.child.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self {
            preference: self.preference,
            child: children[0].clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        self.child.execute(partition, context)
    }

    fn statistics(&self) -> DataFusionResult<Statistics> {
        self.child.statistics()
    }

    fn metrics(&self) -> Option<MetricsSet> {
        None
    }
}

impl DisplayAs for RuntimeGroupExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "RuntimeGroupExec: runtime_preference={}",
            self.preference.as_str(),
        )
    }
}

use datafusion::{
    datasource::{MemTable, TableProvider},
    execution::{context::SessionState, TaskContext},
    physical_plan::{coalesce_partitions::CoalescePartitionsExec, SendableRecordBatchStream},
};
use futures::StreamExt;

use crate::{metastore::catalog::TempCatalog, planner::logical_plan::OwnedFullObjectReference};

use super::*;

#[derive(Debug, Clone)]
pub struct CreateTempTableExec {
    pub tbl_reference: OwnedFullObjectReference,
    pub if_not_exists: bool,
    pub arrow_schema: SchemaRef,
    pub source: Option<Arc<dyn ExecutionPlan>>,
}

impl ExecutionPlan for CreateTempTableExec {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        GENERIC_OPERATION_PHYSICAL_SCHEMA.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        match &self.source {
            Some(source) => vec![source.clone()],
            None => vec![],
        }
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(CreateTempTableExec {
            tbl_reference: self.tbl_reference.clone(),
            if_not_exists: self.if_not_exists,
            arrow_schema: self.arrow_schema.clone(),
            source: children.get(0).cloned(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(
                "CreateTempTableExec only supports 1 partition".to_string(),
            ));
        }

        let stream = stream::once(create_temp_table(self.clone(), context));
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

impl DisplayAs for CreateTempTableExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "CreateTempTableExec")
    }
}

async fn create_temp_table(
    plan: CreateTempTableExec,
    context: Arc<TaskContext>,
) -> DataFusionResult<RecordBatch> {
    let temp_objects = context
        .session_config()
        .get_extension::<TempCatalog>()
        .unwrap();

    if temp_objects
        .resolve_temp_table(&plan.tbl_reference.name)
        .is_some()
    {
        if plan.if_not_exists {
            return Ok(RecordBatch::new_empty(Arc::new(Schema::empty())));
        }
        return Err(DataFusionError::Execution(format!(
            "Duplicate object name: '{}' already exists",
            plan.tbl_reference.name.clone().into_owned()
        )));
    }

    let schema = plan.arrow_schema;
    let data = RecordBatch::new_empty(schema.clone());
    let table = Arc::new(MemTable::try_new(schema, vec![vec![data]])?);
    temp_objects.put_temp_table(plan.tbl_reference.name.into_owned(), table.clone());

    if let Some(source) = plan.source {
        let source: Arc<dyn ExecutionPlan> = match source.output_partitioning().partition_count() {
            1 => source,
            _ => {
                // merge into a single partition
                Arc::new(CoalescePartitionsExec::new(source))
            }
        };

        let state =
            SessionState::with_config_rt(context.session_config().clone(), context.runtime_env());

        let exec = table.insert_into(&state, source).await?;
        let mut stream = exec.execute(0, context)?;

        // Drain stream to write everything.
        while let Some(res) = stream.next().await {
            let _ = res?;
        }
    }

    Ok(new_operation_batch("create_table"))
}

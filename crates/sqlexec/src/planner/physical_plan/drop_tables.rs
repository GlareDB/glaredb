use crate::metastore::catalog::CatalogMutator;
use crate::planner::logical_plan::OwnedFullObjectReference;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::{
    stream::RecordBatchStreamAdapter, DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning,
    SendableRecordBatchStream, Statistics,
};
use futures::stream;
use protogen::metastore::types::service::{self, Mutation};
use std::any::Any;
use std::fmt;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct DropTablesExec {
    pub catalog_version: u64,
    pub references: Vec<OwnedFullObjectReference>,
    pub if_exists: bool,
}

impl ExecutionPlan for DropTablesExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Arc<Schema> {
        Arc::new(Schema::empty())
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
            "Cannot change children for DropTablesExec".to_string(),
        ))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(
                "DropTablesExec only supports 1 partition".to_string(),
            ));
        }

        let mutator = context
            .session_config()
            .get_extension::<CatalogMutator>()
            .expect("context should have catalog mutator");

        let stream = stream::once(drop_tables(mutator, self.clone()));

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

impl DisplayAs for DropTablesExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "DropTablesExec")
    }
}

async fn drop_tables(
    mutator: Arc<CatalogMutator>,
    plan: DropTablesExec,
) -> DataFusionResult<RecordBatch> {
    let mut drops = Vec::with_capacity(plan.references.len());
    // let mut jobs = Vec::with_capacity(plan.names.len());
    // let mut temp_table_drops = Vec::with_capacity(plan.references.len());

    for r in plan.references {
        // if let Ok(table) = self.resolve_temp_table_ref(r.clone()) {
        //     // This is a temp table.
        //     temp_table_drops.push(table);
        //     continue;
        // }

        // let (database, schema, name) = self.resolve_table_ref(r)?;

        // if let Some(table_entry) = self.catalog.resolve_native_table(&database, &schema, &name)
        // {
        //     let job: Arc<dyn BgJob> =
        //         BackgroundJobDeleteTable::new(self.tables.clone(), table_entry.clone());
        //     jobs.push(job);
        // }

        drops.push(Mutation::DropObject(service::DropObject {
            schema: r.schema.into_owned(),
            name: r.name.into_owned(),
            if_exists: plan.if_exists,
        }));
    }
    mutator
        .mutate(plan.catalog_version, drops)
        .await
        .map_err(|e| DataFusionError::Execution(format!("failed to drop tables: {e}")))?;

    // // Drop the session (temp) tables after catalog has mutated successfully
    // // since this step is not going to error. Ideally, we should have transactions
    // // here, but this works beautifully for now.
    // for temp_table in temp_table_drops {
    //     self.temp_objects.drop_table(&temp_table);
    // }

    // // Run background jobs _after_ tables get removed from the catalog.
    // //
    // // Note: If/when we have transactions, background jobs should be stored
    // // on the session until transaction commit.
    // self.background_jobs.add_many(jobs)?;

    Ok(RecordBatch::new_empty(Arc::new(Schema::empty())))
}

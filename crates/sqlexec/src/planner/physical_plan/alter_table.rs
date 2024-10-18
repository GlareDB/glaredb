use std::any::Any;
use std::fmt;
use std::sync::Arc;

use catalog::mutator::CatalogMutator;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::TaskContext;
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
use datasources::native::access::NativeTableStorage;
use futures::stream;
use protogen::metastore::types::catalog::CatalogEntry;
use protogen::metastore::types::service::{self, AlterTableOperation, Mutation};

use super::{new_operation_batch, GENERIC_OPERATION_PHYSICAL_SCHEMA};

#[derive(Debug, Clone)]
pub struct AlterTableExec {
    pub catalog_version: u64,
    pub schema: String,
    pub name: String,
    pub operation: AlterTableOperation,
}

impl ExecutionPlan for AlterTableExec {
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
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            Err(DataFusionError::Plan(
                "Cannot change children for AlterTableRenameExec".to_string(),
            ))
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(
                "AlterTableExec only supports 1 partition".to_string(),
            ));
        }

        let mutator = context
            .session_config()
            .get_extension::<CatalogMutator>()
            .expect("context should have catalog mutator");

        let storage = context
            .session_config()
            .get_extension::<NativeTableStorage>()
            .unwrap();

        let stream = stream::once(alter_table(mutator, self.clone(), storage));

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }

    fn statistics(&self) -> DataFusionResult<Statistics> {
        Ok(Statistics::new_unknown(self.schema().as_ref()))
    }
}

impl DisplayAs for AlterTableExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "AlterTableExec")
    }
}

async fn alter_table(
    mutator: Arc<CatalogMutator>,
    plan: AlterTableExec,
    storage: Arc<NativeTableStorage>,
) -> DataFusionResult<RecordBatch> {
    // TODO: Error if schemas between references differ.
    let new_state = mutator
        .mutate(
            plan.catalog_version,
            [Mutation::AlterTable(service::AlterTable {
                schema: plan.schema,
                name: plan.name.clone(),
                operation: plan.operation.clone(),
            })],
        )
        .await
        .map_err(|e| DataFusionError::Execution(format!("failed to alter table: {e}")))?;

    // Table was successfully altered. We can re-name delta table now.
    if let AlterTableOperation::RenameColumn { .. } = plan.operation {
        let updated_table = new_state
            .entries
            .iter()
            .find_map(|(_, e)| {
                if let CatalogEntry::Table(t) = e {
                    if t.meta.name == plan.name {
                        Some(t)
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .unwrap();

        let _updated_table = storage.load_table(updated_table).await.map_err(|e| {
            DataFusionError::Execution(format!("unable to load table '{}': {}", plan.name, e))
        })?;

        todo!("actually update the table with new schema...");
    }

    mutator
        .commit_state(plan.catalog_version, new_state.as_ref().clone())
        .await
        .map_err(|e| DataFusionError::Execution(format!("failed to commit state: {e}")))?;

    Ok(new_operation_batch("alter_table"))
}

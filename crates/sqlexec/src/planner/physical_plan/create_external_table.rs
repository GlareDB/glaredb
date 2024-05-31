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
use futures::stream;
use protogen::metastore::types::options::{InternalColumnDefinition, TableOptionsV0};
use protogen::metastore::types::service::{self, Mutation};

use super::{new_operation_batch, GENERIC_OPERATION_PHYSICAL_SCHEMA};
use crate::planner::logical_plan::OwnedFullObjectReference;

#[derive(Debug, Clone)]
pub struct CreateExternalTableExec {
    pub catalog_version: u64,
    pub tbl_reference: OwnedFullObjectReference,
    pub or_replace: bool,
    pub if_not_exists: bool,
    pub table_options: TableOptionsV0,
    pub tunnel: Option<String>,
    pub table_schema: Option<Schema>,
}

impl ExecutionPlan for CreateExternalTableExec {
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
                "Cannot change children for CreateExternalTableExec".to_string(),
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
                "CreateExternalTableExec only supports 1 partition".to_string(),
            ));
        }

        let mutator = context
            .session_config()
            .get_extension::<CatalogMutator>()
            .expect("context should have catalog mutator");

        let stream = stream::once(create_external_table(mutator, self.clone()));

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }

    fn statistics(&self) -> DataFusionResult<Statistics> {
        Ok(Statistics::new_unknown(self.schema().as_ref()))
    }
}

impl DisplayAs for CreateExternalTableExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CreateExternalTableExec")
    }
}

async fn create_external_table(
    mutator: Arc<CatalogMutator>,
    plan: CreateExternalTableExec,
) -> DataFusionResult<RecordBatch> {
    // We dont want to tightly couple the metastore types with the arrow types
    // so we convert the arrow schema to metastore schema
    let columns = plan.table_schema.map(|schema| {
        InternalColumnDefinition::from_arrow_fields(schema.fields()).collect::<Vec<_>>()
    });

    mutator
        .mutate_and_commit(
            plan.catalog_version,
            [Mutation::CreateExternalTable(
                service::CreateExternalTable {
                    schema: plan.tbl_reference.schema.into_owned(),
                    name: plan.tbl_reference.name.into_owned(),
                    options: plan.table_options,
                    or_replace: plan.or_replace,
                    if_not_exists: plan.if_not_exists,
                    tunnel: plan.tunnel,
                    columns,
                },
            )],
        )
        .await
        .map_err(|e| DataFusionError::Execution(format!("failed to create external table: {e}")))?;

    Ok(new_operation_batch("create_table"))
}

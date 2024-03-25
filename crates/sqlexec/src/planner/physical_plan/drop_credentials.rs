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
use protogen::metastore::types::service::{self, Mutation};

use super::{
    new_operation_batch,
    new_operation_with_deprecation_batch,
    GENERIC_OPERATION_PHYSICAL_SCHEMA,
    GENERIC_OPERATION_WITH_DEPRECATION_PHYSICAL_SCHEMA,
};

const DEPRECATION_MESSAGE: &str = "`DROP CREDENTIALS` is deprecated and will be removed in a future release. Use `DROP CREDENTIAL` instead.";

#[derive(Debug, Clone)]
pub struct DropCredentialsExec {
    pub catalog_version: u64,
    pub names: Vec<String>,
    pub if_exists: bool,
    /// If true, show a deprecation warning when the operation is executed.
    pub show_deprecation_warning: bool,
}

impl ExecutionPlan for DropCredentialsExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Arc<Schema> {
        if self.show_deprecation_warning {
            GENERIC_OPERATION_WITH_DEPRECATION_PHYSICAL_SCHEMA.clone()
        } else {
            GENERIC_OPERATION_PHYSICAL_SCHEMA.clone()
        }
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
                "Cannot change children for DropCredentialsExec".to_string(),
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
                "DropCredentialsExec only supports 1 partition".to_string(),
            ));
        }

        let mutator = context
            .session_config()
            .get_extension::<CatalogMutator>()
            .expect("context should have catalog mutator");

        let stream = stream::once(drop_credentials(mutator, self.clone()));

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }

    fn statistics(&self) -> DataFusionResult<Statistics> {
        Ok(Statistics::new_unknown(self.schema().as_ref()))
    }
}

impl DisplayAs for DropCredentialsExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "DropCredentialsExec")
    }
}

async fn drop_credentials(
    mutator: Arc<CatalogMutator>,
    plan: DropCredentialsExec,
) -> DataFusionResult<RecordBatch> {
    let drops: Vec<_> = plan
        .names
        .into_iter()
        .map(|name| {
            Mutation::DropCredentials(service::DropCredentials {
                name,
                if_exists: plan.if_exists,
            })
        })
        .collect();

    mutator
        .mutate(plan.catalog_version, drops)
        .await
        .map_err(|e| DataFusionError::Execution(format!("failed to drop credentials: {e}")))?;
    if plan.show_deprecation_warning {
        Ok(new_operation_with_deprecation_batch(
            "drop_credentials",
            DEPRECATION_MESSAGE,
        ))
    } else {
        Ok(new_operation_batch("drop_credentials"))
    }
}

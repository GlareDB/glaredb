use datafusion::arrow::datatypes::Schema;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::{stream::RecordBatchStreamAdapter, DisplayAs, ExecutionPlan};
use futures::stream;
use futures::StreamExt;
use protogen::metastore::types::{options::CredentialsOptions, service, service::Mutation};

use crate::metastore::catalog::CatalogMutator;

#[derive(Clone, Debug)]
pub struct CreateCredentialsExec {
    pub name: String,
    pub options: CredentialsOptions,
    pub comment: String,
}

impl DisplayAs for CreateCredentialsExec {
    fn fmt_as(
        &self,
        t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        todo!()
    }
}

impl ExecutionPlan for CreateCredentialsExec {
    fn as_any(&self) -> &dyn std::any::Any {
        todo!()
    }

    fn schema(&self) -> datafusion::arrow::datatypes::SchemaRef {
        todo!()
    }

    fn output_partitioning(&self) -> datafusion::physical_plan::Partitioning {
        todo!()
    }

    fn output_ordering(&self) -> Option<&[datafusion::physical_expr::PhysicalSortExpr]> {
        todo!()
    }

    fn children(&self) -> Vec<std::sync::Arc<dyn ExecutionPlan>> {
        todo!()
    }

    fn with_new_children(
        self: std::sync::Arc<Self>,
        children: Vec<std::sync::Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<std::sync::Arc<dyn ExecutionPlan>> {
        todo!()
    }

    fn execute(
        &self,
        partition: usize,
        context: std::sync::Arc<datafusion::execution::TaskContext>,
    ) -> datafusion::error::Result<datafusion::physical_plan::SendableRecordBatchStream> {
        let catalog_mutator = context
            .session_config()
            .get_extension::<CatalogMutator>()
            .unwrap();
        let output = stream::once(execute_impl(self, catalog_mutator.as_ref())).boxed();
        // todo (execute output)
        EmptyExec::new(false, Schema::empty().into()).execute(partition, context)
    }

    fn statistics(&self) -> datafusion::physical_plan::Statistics {
        todo!()
    }
}

pub async fn execute_impl(exec: &CreateCredentialsExec, mutator: &CatalogMutator) {
    mutator
        .mutate(
            1u64,
            [Mutation::CreateCredentials(service::CreateCredentials {
                name: exec.name.clone(),
                options: exec.options.clone(),
                comment: exec.comment.clone(),
            })],
        )
        .await
        .unwrap();
}

use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::context::SessionState;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::{
    stream::RecordBatchStreamAdapter, DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning,
    SendableRecordBatchStream, Statistics,
};
use datafusion::scalar::ScalarValue;
use datasources::native::access::NativeTable;
use datasources::native::access::NativeTableStorage;
use futures::{stream, StreamExt};

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use crate::background_jobs::storage::BackgroundJobStorageTracker;
use crate::background_jobs::JobRunner;
use crate::metastore::catalog::CatalogMutator;

use super::remote_scan::ProviderReference;
use super::{new_operation_with_count_batch, GENERIC_OPERATION_AND_COUNT_PHYSICAL_SCHEMA};

#[derive(Debug, Clone)]
pub struct InsertExec {
    pub provider: ProviderReference,
    pub source: Arc<dyn ExecutionPlan>,
}

impl ExecutionPlan for InsertExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Arc<Schema> {
        GENERIC_OPERATION_AND_COUNT_PHYSICAL_SCHEMA.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.source.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(InsertExec {
            provider: self.provider.clone(),
            source: children.get(0).unwrap().clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(
                "InsertExec only supports 1 partition".to_string(),
            ));
        }

        let this = self.clone();
        let stream = stream::once(async move {
            match this.provider {
                ProviderReference::RemoteReference(_) => Err(DataFusionError::Internal(
                    "required table provider, found remote reference to insert".to_string(),
                )),
                ProviderReference::Provider(provider) => {
                    let is_native_table = provider.as_any().downcast_ref::<NativeTable>().is_some();

                    match Self::do_insert(provider, this.source, context.clone()).await {
                        // Add the background storage tracker job when inserting
                        // into a native table.
                        Ok(batch) if is_native_table => {
                            let job_runner = context
                                .session_config()
                                .get_extension::<JobRunner>()
                                .unwrap();

                            let native_storage = context
                                .session_config()
                                .get_extension::<NativeTableStorage>()
                                .unwrap();

                            let catalog_mutator = context
                                .session_config()
                                .get_extension::<CatalogMutator>()
                                .unwrap();

                            job_runner
                                .add(BackgroundJobStorageTracker::new(
                                    native_storage.as_ref().clone(),
                                    catalog_mutator.as_ref().clone(),
                                ))
                                .map_err(|e| {
                                    DataFusionError::Internal(format!(
                                        "unable to start storage tracker job: {e}"
                                    ))
                                })?;

                            Ok(batch)
                        }
                        res => res,
                    }
                }
            }
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

impl DisplayAs for InsertExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "InsertExec")
    }
}

impl InsertExec {
    pub async fn do_insert(
        table: Arc<dyn TableProvider>,
        source: Arc<dyn ExecutionPlan>,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<RecordBatch> {
        let state =
            SessionState::with_config_rt(context.session_config().clone(), context.runtime_env());

        let source = if source.output_partitioning().partition_count() != 1 {
            Arc::new(CoalescePartitionsExec::new(source))
        } else {
            source
        };

        let exec = table.insert_into(&state, source, false).await?;

        let mut stream = exec.execute(0, context)?;

        let mut inserted_rows = 0_u64;
        while let Some(res) = stream.next().await {
            // Drain stream to write everything.
            let res = res?;
            // Each res should have the count of rows inserted.
            let count =
                datafusion::arrow::compute::cast(res.column(0).as_ref(), &DataType::UInt64)?;
            for row_idx in 0..count.len() {
                let s = ScalarValue::try_from_array(&count, row_idx)?;
                match s {
                    ScalarValue::UInt64(Some(v)) => {
                        inserted_rows += v;
                    }
                    _ => unreachable!("scalar value should be of UInt64 type"),
                };
            }
        }

        Ok(new_operation_with_count_batch("insert", inserted_rows))
    }
}

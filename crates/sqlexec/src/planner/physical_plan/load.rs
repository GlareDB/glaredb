use std::any::Any;
use std::fmt;
use std::sync::Arc;

use catalog::mutator::CatalogMutator;
use datafusion::arrow::array::{ArrayRef, BooleanArray, StringArray};
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
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
use once_cell::sync::Lazy;
use protogen::metastore::types::service::{LoadExtension, Mutation};
use sqlbuiltins::functions::{FUNCTION_REGISTRY, SENTENCE_TRANSFORMER_EXTENSION};

pub static LOAD_SCHEMA: Lazy<SchemaRef> = Lazy::new(|| {
    Schema::new(vec![
        Field::new(
            "extension",
            datafusion::arrow::datatypes::DataType::Utf8,
            false,
        ),
        Field::new(
            "loaded",
            datafusion::arrow::datatypes::DataType::Boolean,
            false,
        ),
        Field::new(
            "remote",
            datafusion::arrow::datatypes::DataType::Boolean,
            false,
        ),
    ])
    .into()
});


#[derive(Debug, Clone)]
pub struct LoadExec {
    pub extension: String,
    pub catalog_version: u64,
    pub remote: bool,
    pub should_update_catalog: bool,
}

impl ExecutionPlan for LoadExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Arc<Schema> {
        LOAD_SCHEMA.clone()
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
                "Cannot change children for LoadExec".to_string(),
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
                "LoadExec only supports 1 partition".to_string(),
            ));
        }

        let catalog_mutator = context
            .session_config()
            .get_extension::<CatalogMutator>()
            .unwrap();
        let this = self.clone();
        let stream = stream::once(this.load_extension(catalog_mutator));

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }

    fn statistics(&self) -> DataFusionResult<Statistics> {
        Ok(Statistics::new_unknown(self.schema().as_ref()))
    }
}

impl DisplayAs for LoadExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "LoadExec")
    }
}

impl LoadExec {
    async fn load_extension(self, mutator: Arc<CatalogMutator>) -> DataFusionResult<RecordBatch> {
        if self.should_update_catalog {
            mutator
                .mutate(
                    self.catalog_version,
                    [Mutation::LoadExtension(LoadExtension {
                        extension: self.extension.clone(),
                    })],
                )
                .await
                .map_err(|e| {
                    DataFusionError::Execution(format!("failed to load extension in catalog: {e}"))
                })?;
        } else {
            // hardcoded for now
            FUNCTION_REGISTRY
                .lock()
                .register_extension(&SENTENCE_TRANSFORMER_EXTENSION);
        };


        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec![self.extension.clone()])),
            Arc::new(BooleanArray::from(vec![true])),
            Arc::new(BooleanArray::from(vec![self.remote])),
        ];


        Ok(RecordBatch::try_new(self.schema(), columns).unwrap())
    }
}

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::common::ToDFSchema;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::context::SessionState;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{ident, Cast, Expr};
use datafusion::physical_expr::{create_physical_expr, PhysicalSortExpr};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs,
    DisplayFormatType,
    Distribution,
    ExecutionPlan,
    Partitioning,
    SendableRecordBatchStream,
    Statistics,
};
use deltalake::kernel::StructField;
use deltalake::logstore::LogStore;
use deltalake::operations::write::WriteBuilder;
use deltalake::protocol::SaveMode;
use deltalake::table::state::DeltaTableState;
use futures::StreamExt;

use crate::common::util::{create_count_record_batch, COUNT_SCHEMA};

/// An execution plan for inserting data into a delta table.
#[derive(Debug)]
pub struct NativeTableInsertExec {
    input: Arc<dyn ExecutionPlan>,
    store: Arc<dyn LogStore>,
    snapshot: DeltaTableState,
    save_mode: SaveMode,
}

impl NativeTableInsertExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        store: Arc<dyn LogStore>,
        snapshot: DeltaTableState,
        save_mode: SaveMode,
    ) -> Self {
        NativeTableInsertExec {
            input,
            store,
            snapshot,
            save_mode,
        }
    }
}

impl ExecutionPlan for NativeTableInsertExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        COUNT_SCHEMA.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::UnspecifiedDistribution]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![false]
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self {
            input: children[0].clone(),
            store: self.store.clone(),
            snapshot: self.snapshot.clone(),
            save_mode: self.save_mode.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(
                format!("Invalid requested partition {partition}. NativeTableInsertExec requires a single input partition.")));
        }

        // This is needed since we might be inserting from a plan that includes
        // a client recv exec. That exec requires that we have an appropriate
        // set of extensions.
        let state = SessionState::new_with_config_rt(
            context.session_config().clone(),
            context.runtime_env(),
        );

        let schema = self.input.schema();
        let fields = schema.fields().clone();
        let input_dfschema = schema.to_dfschema()?;
        // delta-rs does not support all data types, so we need to check if the input schema
        // contains any unsupported data types.
        // If it does, we need to cast them to supported data types.
        let mut contains_unsupported_fields = false;
        let projections = fields
            .iter()
            .map(|field| {
                let e = if StructField::try_from(field.as_ref()).is_ok() {
                    ident(field.name())
                } else {
                    contains_unsupported_fields = true;

                    match field.data_type() {
                        DataType::Timestamp(_, _) => Expr::Cast(Cast {
                            expr: Box::new(ident(field.name())),
                            data_type: DataType::Timestamp(
                                datafusion::arrow::datatypes::TimeUnit::Microsecond,
                                None,
                            ),
                        }),

                        dtype => {
                            return Err(DataFusionError::Execution(format!(
                                "Unsupported data type {:?} for field {}",
                                dtype,
                                field.name()
                            )))
                        }
                    }
                };
                let e = create_physical_expr(&e, &input_dfschema, state.execution_props()).unwrap();
                Ok((e, field.name().clone()))
            })
            .collect::<DataFusionResult<Vec<_>>>()?;

        let input = if contains_unsupported_fields {
            Arc::new(ProjectionExec::try_new(projections, self.input.clone())?)
        } else {
            self.input.clone()
        };

        // Allows writing multiple output partitions from the input execution
        // plan.
        //
        // TODO: Possibly try avoiding cloning the snapshot.
        let builder = WriteBuilder::new(self.store.clone(), Some(self.snapshot.clone()))
            .with_input_session_state(state)
            .with_save_mode(self.save_mode.clone())
            .with_input_execution_plan(input.clone());

        let output = futures::stream::once(async move {
            let _ = builder
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            let count = input
                .metrics()
                .map(|metrics| metrics.output_rows().unwrap_or_default())
                .unwrap_or_default();

            Ok(create_count_record_batch(count as u64))
        })
        .boxed();

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            output,
        )))
    }

    fn statistics(&self) -> DataFusionResult<Statistics> {
        Ok(Statistics::new_unknown(self.schema().as_ref()))
    }
}

impl DisplayAs for NativeTableInsertExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                write!(f, "NativeTableInsertExec")
            }
            DisplayFormatType::Verbose => {
                write!(f, "NativeTableInsertExec")
            }
        }
    }
}

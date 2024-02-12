use std::any::Any;
use std::sync::Arc;

use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::FlightClient;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::expressions::PhysicalSortExpr;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    execute_stream,
    DisplayAs,
    DisplayFormatType,
    ExecutionPlan,
    Partitioning,
    SendableRecordBatchStream,
    Statistics,
};
use futures::StreamExt;
use tonic::transport::Channel;

use super::FlightSqlSourceConnectionOptions;
use crate::common::util::create_count_record_batch;

pub struct ExecPlan {
    channel: Channel,
    opts: FlightSqlSourceConnectionOptions,
    input: Arc<dyn ExecutionPlan>,
    metrics: ExecutionPlanMetricsSet,
}

impl ExecPlan {
    pub fn new(
        channel: Channel,
        opts: FlightSqlSourceConnectionOptions,
        input: Arc<dyn ExecutionPlan>,
    ) -> Self {
        Self {
            channel,
            opts,
            input,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    fn get_client(&self) -> Result<FlightClient, FlightError> {
        let mut client = FlightClient::new(self.channel.clone());
        client.add_header("database", self.opts.database.as_str())?;
        client.add_header(
            "authorization",
            format!("Bearer {}", self.opts.token.clone()).as_str(),
        )?;
        Ok(client)
    }
}

impl DisplayAs for ExecPlan {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "FlightInsertExecPlan")
    }
}

impl std::fmt::Debug for ExecPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FlightInsertExecPlan: {:?}", self.schema())
    }
}

impl ExecutionPlan for ExecPlan {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
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
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            Err(DataFusionError::Execution(
                "cannot replace children for FlightInsertExecPlan".to_string(),
            ))
        }
    }

    fn execute(
        &self,
        _partition: usize,
        ctx: Arc<TaskContext>,
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        let stream = execute_stream(self.input.clone(), ctx)?;
        let schema = self.schema().clone();

        let mut client = self
            .get_client()
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema.clone(),
            futures::stream::once(async move {
                // TODO(tycho): nothing here tells the service what
                // table to "put" into at this moment. I think we have
                // to annotate the schema or the client metdata with a
                // "path".
                //
                // There is some complexity here: influx db only
                // supports inserts via normal sql using "do_get"
                // operations; and the signature of do_put is
                // different for flightsql and flightrpc. (The SQL
                // variant is a stream of recordbatch wrappers, and
                // the RPC variant is a stream of results, more
                // closely mirroring SendableRecordBatchStream)

                let enc = FlightDataEncoderBuilder::new()
                    .with_schema(schema.clone())
                    .build(stream.map(|item| match item {
                        Ok(val) => Ok(val), // TODO(tycho): read the num_rows from here
                        Err(e) => Err(FlightError::ExternalError(Box::new(e))),
                    }));

                // TODO(tycho) this is wrong: it counts the number of
                // record batches not the number of rows, which is
                // what this count really should report, but
                // inspecting the encoder is hard for borrowing reasons
                let count = client
                    .do_put(enc)
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?
                    .count()
                    .await as u64;

                Ok::<RecordBatch, DataFusionError>(create_count_record_batch(count))
            }),
        )))
    }

    fn statistics(&self) -> datafusion::error::Result<Statistics> {
        Ok(Statistics::new_unknown(self.schema().as_ref()))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

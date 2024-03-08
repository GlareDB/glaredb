use std::any::Any;
use std::sync::Arc;

use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::sql::client::FlightSqlServiceClient;
use arrow_flight::sql::CommandGetTables;
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

use super::FlightSqlConnectionOptions;
use crate::common::util::{create_count_record_batch, COUNT_SCHEMA};

pub struct ExecPlan {
    channel: Channel,
    opts: FlightSqlConnectionOptions,
    input: Arc<dyn ExecutionPlan>,
    metrics: ExecutionPlanMetricsSet,
}

impl ExecPlan {
    pub fn new(
        channel: Channel,
        opts: FlightSqlConnectionOptions,
        input: Arc<dyn ExecutionPlan>,
    ) -> Self {
        Self {
            channel,
            opts,
            input,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    fn get_client(&self) -> Result<FlightClient, DataFusionError> {
        let mut client = FlightClient::new(self.channel.clone());
        client
            .add_header("database", self.opts.database.as_str())
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        client
            .add_header(
                "authorization",
                format!("Bearer {}", self.opts.token.clone()).as_str(),
            )
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(client)
    }

    fn get_flightsql_client(&self) -> FlightSqlServiceClient<Channel> {
        let token = self.opts.token.clone();
        let db = self.opts.database.clone();
        let mut client = FlightSqlServiceClient::new(self.channel.clone());
        client.set_token(token);
        client.set_header("database", db);
        client
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
        COUNT_SCHEMA.clone()
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

        let db = self.opts.database.clone();
        let table = self.opts.table.clone();

        let mut client = self.get_client()?;
        let mut fsqlcli = self.get_flightsql_client();


        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema.clone(),
            futures::stream::once(async move {
                let catalogs = fsqlcli
                    .get_catalogs()
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                let descriptor = fsqlcli
                    .get_tables(CommandGetTables {
                        catalog: Some(catalogs.to_string()),
                        db_schema_filter_pattern: Some(db),
                        table_name_filter_pattern: Some(table.clone()),
                        include_schema: false,
                        table_types: vec!["TABLE".to_string(), "VIEW".to_string()],
                    })
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?
                    .flight_descriptor
                    .ok_or_else(|| {
                        DataFusionError::Internal(format!("could not find table {}", table.clone()))
                    })?;


                // TODO(tycho): There is some complexity here: influx
                // db only supports inserts via normal sql using
                // "do_get" operations and the signature of do_put is
                // different for flightsql and flightrpc. (The SQL
                // variant is a stream of recordbatch wrappers, and
                // the RPC variant is a stream of results of those
                // wrappers, more closely mirroring
                // SendableRecordBatchStream)

                let enc = FlightDataEncoderBuilder::new()
                    .with_flight_descriptor(Some(descriptor))
                    .with_schema(schema.clone())
                    .build(stream.map(|item| match item {
                        Ok(val) => Ok(val),
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
                    .await;


                Ok::<RecordBatch, DataFusionError>(create_count_record_batch(count as u64))
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

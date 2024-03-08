use std::any::Any;
use std::sync::Arc;

use arrow_flight::sql::client::FlightSqlServiceClient;
use async_stream::stream;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::expressions::PhysicalSortExpr;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs,
    DisplayFormatType,
    ExecutionPlan,
    Partitioning,
    SendableRecordBatchStream,
    Statistics,
};
use futures::StreamExt;
use tonic::transport::Channel;

pub struct ExecPlan {
    id: uuid::Uuid,
    client: FlightSqlServiceClient<Channel>,
    schema: SchemaRef,
    input: String,
    metrics: ExecutionPlanMetricsSet,
}

impl ExecPlan {
    pub fn new(
        id: uuid::Uuid,
        schema: SchemaRef,
        client: FlightSqlServiceClient<Channel>,
        query: impl ToString,
    ) -> Self {
        Self {
            id,
            client,
            schema: schema.clone(),
            input: query.to_string(),
            metrics: ExecutionPlanMetricsSet::new(),
        }
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
        self.schema.clone()
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
        _ctx: Arc<TaskContext>,
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        let schema = self.schema.clone();
        let mut client = self.client.clone();
        let query = self.input.clone();
        let id = self.id.into_bytes().to_vec().into();

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema.clone(),
            stream! {
                let info = client.execute(query, Some(id)).await.map_err(|e| {
                    DataFusionError::External(Box::new(e))
                })?;

                let flight_endpoint = info.endpoint.get(0).ok_or_else(|| {
                    DataFusionError::Internal("could not resolve flightsql ticket".to_string())
                })?;

                let ticket = flight_endpoint.ticket.as_ref().ok_or_else(|| {
                    DataFusionError::Internal("missing flightsql ticket".to_string())
                })?;

                let mut items = client.do_get(ticket.to_owned()).await?;

                while let Some(it) = items.next().await {
                    yield it.map_err(|e| {
                        DataFusionError::External(Box::new(e))
                    });
                }
            },
        )))
    }

    fn statistics(&self) -> datafusion::error::Result<Statistics> {
        Ok(Statistics::new_unknown(self.schema().as_ref()))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

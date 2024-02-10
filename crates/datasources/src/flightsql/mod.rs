use std::any::Any;
use std::sync::Arc;

use arrow_flight::decode::{FlightDataDecoder, FlightRecordBatchStream};
use arrow_flight::encode::FlightDataEncoder;
use arrow_flight::sql::client::FlightSqlServiceClient;
use arrow_flight::sql::CommandGetDbSchemas;
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::{execute_stream, ExecutionPlan};
use datafusion_ext::errors::ExtensionError;
use tonic::transport::{Channel, Endpoint};

#[derive(Debug)]
pub struct FlightSqlSourceConnectionOptions {
    pub uri: String,
    pub database: String,
    pub token: String,
}

pub struct FlightSqlSourceProvider {
    client: arrow_flight::sql::client::FlightSqlServiceClient<Channel>,
    schema: SchemaRef,
}

impl FlightSqlSourceProvider {
    pub async fn try_new(opts: FlightSqlSourceConnectionOptions) -> Result<Self, ExtensionError> {
        let mut client = FlightSqlServiceClient::new(
            Endpoint::from_shared(opts.uri)
                .map_err(|e| ExtensionError::String(e.to_string()))?
                .connect()
                .await
                .map_err(|e| ExtensionError::String(e.to_string()))?,
        );
        client.set_token(opts.token);
        client.set_header("database", opts.database.as_str());

        let catalogs = client.get_catalogs().await?;
        let schema = Arc::new(
            client
                .get_db_schemas(CommandGetDbSchemas {
                    catalog: Some(catalogs.to_string()),
                    db_schema_filter_pattern: Some(opts.database),
                })
                .await?
                .try_decode_schema()?,
        );

        Ok(Self { client, schema })
    }
}

#[async_trait]
impl TableProvider for FlightSqlSourceProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filter_pushdown(
        &self,
        _filter: &Expr,
    ) -> Result<TableProviderFilterPushDown, DataFusionError> {
        Ok(TableProviderFilterPushDown::Exact)
    }

    async fn scan(
        &self,
        _ctx: &SessionState,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let id = uuid::Uuid::new_v4();
        // TODO: convert this from exprs using the same logic as in
        // the clickhouse implementation.
        let query = "select *".to_string();

        let info = self
            .client
            .execute(query, Some(id.into_bytes().to_vec().into()))
            .await?;
        let flight_endpoint = info.endpoint.get(0).ok_or_else(|| {
            DataFusionError::Internal("could not resolve flightsql ticket".to_string())
        })?;
        let ticket = flight_endpoint
            .ticket
            .ok_or_else(|| DataFusionError::Internal("missing flightsql ticket".to_string()))?;

        let stream =
            FlightRecordBatchStream::new(FlightDataDecoder::new(self.client.do_get(ticket).await?));

        // TODO(tycho)
        todo!("convert this operation into execution stream")
    }

    async fn insert_into(
        &self,
        state: &SessionState,
        input: Arc<dyn ExecutionPlan>,
        _overwrite: bool,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let id = uuid::Uuid::new_v4().into_bytes();

        let mut stream = FlightDataEncoder::try_from(execute_stream(_input, state.task_ctx())?)?;

        let res = self.client.do_put(stream).await?;
        // TODO(tycho)
        todo!("convert this operation into execution stream")
    }
}

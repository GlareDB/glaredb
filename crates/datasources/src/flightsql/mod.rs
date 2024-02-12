mod insert;
mod scan;

use std::any::Any;
use std::sync::Arc;

use arrow_flight::sql::client::FlightSqlServiceClient;
use arrow_flight::sql::CommandGetDbSchemas;
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_ext::errors::ExtensionError;
use tonic::transport::{Channel, Endpoint};

use crate::common::query;
use crate::common::util::Datasource;

// use arrow_flight::FlightClient;

// use arrow_flight::error::FlightError;
// use arrow_flight::encode::FlightDataEncoderBuilder;
// use datafusion::physical_plan::execute_stream;
// use futures::StreamExt;

#[derive(Debug, Clone)]
pub struct FlightSqlSourceConnectionOptions {
    pub uri: String,
    pub database: String,
    pub table: String,
    pub token: String,
}

pub struct FlightSqlSourceProvider {
    channel: Channel,
    schema: SchemaRef,
    opts: FlightSqlSourceConnectionOptions,
}

impl FlightSqlSourceProvider {
    pub async fn try_new(opts: FlightSqlSourceConnectionOptions) -> Result<Self, ExtensionError> {
        let uri = opts.uri.clone();
        let channel = Endpoint::from_shared(uri)
            .map_err(|e| ExtensionError::String(e.to_string()))?
            .connect()
            .await
            .map_err(|e| ExtensionError::String(e.to_string()))?;


        let db = opts.database.clone();
        let mut client = Self::make_client(opts.clone(), channel.clone());
        let catalogs = client.get_catalogs().await?;
        let schema = Arc::new(
            client
                .get_db_schemas(CommandGetDbSchemas {
                    catalog: Some(catalogs.to_string()),
                    db_schema_filter_pattern: Some(db),
                })
                .await?
                .try_decode_schema()?,
        );

        Ok(Self {
            channel,
            schema,
            opts,
        })
    }

    fn get_client(&self) -> FlightSqlServiceClient<Channel> {
        FlightSqlSourceProvider::make_client(self.opts.clone(), self.channel.clone())
    }

    fn make_client(
        opts: FlightSqlSourceConnectionOptions,
        endpoint: Channel,
    ) -> FlightSqlServiceClient<Channel> {
        let token = opts.token;
        let db = opts.database;
        let mut client = FlightSqlServiceClient::new(endpoint);
        client.set_token(token);
        client.set_header("database", db);

        client
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
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let id = uuid::Uuid::new_v4();

        let projected_schema = match projection {
            Some(projection) => Arc::new(self.schema.project(projection)?),
            None => self.schema.clone(),
        };

        Ok(Arc::new(scan::ExecPlan::new(
            id,
            projected_schema.clone(),
            self.get_client(),
            query::from_exprs(
                Datasource::FlightSql,
                self.opts.table.clone(),
                projected_schema.clone(),
                filters,
                limit,
            )?,
        )))
    }

    async fn insert_into(
        &self,
        _state: &SessionState,
        input: Arc<dyn ExecutionPlan>,
        _overwrite: bool,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        Ok(Arc::new(insert::ExecPlan::new(
            self.channel.clone(),
            self.opts.clone(),
            input,
        )))
    }
}

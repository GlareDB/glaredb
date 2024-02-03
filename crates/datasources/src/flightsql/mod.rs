use arrow_flight::sql::{
    client::FlightSqlServiceClient, CommandGetDbSchemas, CommandGetTables,
};
use datafusion_ext::errors::ExtensionError;
use datafusion::logical_expr::{Expr};
use tonic::transport::Endpoint;

#[derive(Debug, Default)]
pub struct FlightSourceConnectionOptions {
    database: String,
    token: String,
    host: String,
}

async fn main() -> Result<bool, ExtensionError> {
    let opts = FlightSourceConnectionOptions::default();

    let conn = Endpoint::from_shared(opts.host)
        .map_err(|e| ExtensionError::String(e.to_string()))?
        .connect()
        .await
        .map_err(|e| ExtensionError::String(e.to_string()))?;

    let mut client = FlightSqlServiceClient::new(conn);
    client.set_token(opts.token);
    client.set_header("database", opts.database.as_str());

    let catalogs = client.get_catalogs().await?;
    let _schema = client
        .get_db_schemas(CommandGetDbSchemas {
            catalog: Some(catalogs.to_string()),
            db_schema_filter_pattern: Some(opts.database),
        })
        .await?
        .try_decode_schema()?;

    let projection: Option<&Vec<usize>> = None;

    let filters: &[Expr] = &[];
    filters.

    client.execute(query, transaction_id)

    Ok(false)
}

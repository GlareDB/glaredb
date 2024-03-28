use anyhow::Result;
use arrow_flight::sql::client::FlightSqlServiceClient;
use futures::StreamExt;
use pgrepr::format::Format;
use pgrepr::scalar::Scalar;
use pgrepr::types::arrow_to_pg_type;
use rpcsrv::flight::handler::FLIGHTSQL_DATABASE_HEADER;
use sqlexec::errors::ExecError;
use sqllogictest::{AsyncDB, DBOutput, DefaultColumnType};
use tokio_postgres::types::private::BytesMut;
use tokio_postgres::Config;
use tonic::async_trait;
use tonic::transport::{Channel, Endpoint};
use uuid::Uuid;

#[derive(Clone)]
pub struct FlightSqlTestClient {
    pub client: FlightSqlServiceClient<Channel>,
}

impl FlightSqlTestClient {
    pub async fn new(config: &Config) -> Result<Self> {
        let port = config.get_ports().first().unwrap();
        let addr = format!("http://0.0.0.0:{port}");
        let conn = Endpoint::new(addr)?.connect().await?;
        let dbid: Uuid = config.get_dbname().unwrap().parse().unwrap();

        let mut client = FlightSqlServiceClient::new(conn);
        client.set_header(FLIGHTSQL_DATABASE_HEADER, dbid.to_string());
        Ok(FlightSqlTestClient { client })
    }
}

#[async_trait]
impl AsyncDB for FlightSqlTestClient {
    type Error = sqlexec::errors::ExecError;
    type ColumnType = DefaultColumnType;
    async fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
        let mut output = Vec::new();
        let mut num_columns = 0;

        let mut client = self.client.clone();
        let ticket = client.execute(sql.to_string(), None).await?;
        let ticket = ticket
            .endpoint
            .first()
            .ok_or_else(|| ExecError::String("The server should support this".to_string()))?
            .clone();
        let ticket = ticket.ticket.unwrap();
        let mut stream = client.do_get(ticket).await?;

        // all the remaining stream messages should be dictionary and record batches
        while let Some(batch) = stream.next().await {
            let batch = batch.map_err(|e| {
                Self::Error::String(format!("error getting batch from flight: {e}"))
            })?;

            if num_columns == 0 {
                num_columns = batch.num_columns();
            }

            for row_idx in 0..batch.num_rows() {
                let mut row_output = Vec::with_capacity(num_columns);

                for col in batch.columns() {
                    let pg_type = arrow_to_pg_type(col.data_type(), None);
                    let scalar = Scalar::try_from_array(col, row_idx, &pg_type)?;

                    if scalar.is_null() {
                        row_output.push("NULL".to_string());
                    } else {
                        let mut buf = BytesMut::new();
                        scalar.encode_with_format(Format::Text, &mut buf)?;

                        if buf.is_empty() {
                            row_output.push("(empty)".to_string())
                        } else {
                            let scalar = String::from_utf8(buf.to_vec()).map_err(|e| {
                                ExecError::Internal(format!(
                                    "invalid text formatted result from pg encoder: {e}"
                                ))
                            })?;
                            row_output.push(scalar.trim().to_owned());
                        }
                    }
                }
                output.push(row_output);
            }
        }
        if output.is_empty() && num_columns == 0 {
            Ok(DBOutput::StatementComplete(0))
        } else {
            Ok(DBOutput::Rows {
                types: vec![DefaultColumnType::Text; num_columns],
                rows: output,
            })
        }
    }
}

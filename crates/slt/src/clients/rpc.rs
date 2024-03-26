use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use datafusion_ext::vars::SessionVars;
use futures::StreamExt;
use metastore::util::MetastoreClientMode;
use pgrepr::format::Format;
use pgrepr::scalar::Scalar;
use pgrepr::types::arrow_to_pg_type;
use sqlexec::engine::{Engine, EngineStorageConfig, SessionStorageConfig, TrackedSession};
use sqlexec::errors::ExecError;
use sqlexec::remote::client::RemoteClient;
use sqlexec::session::ExecutionResult;
use sqllogictest::{AsyncDB, DBOutput, DefaultColumnType};
use telemetry::Tracker;
use tokio::sync::Mutex;
use tokio_postgres::types::private::BytesMut;
use tokio_postgres::Config;
use tonic::async_trait;
use uuid::Uuid;

#[derive(Clone)]
pub struct RpcTestClient {
    pub session: Arc<Mutex<TrackedSession>>,
    _engine: Arc<Engine>,
}

impl RpcTestClient {
    pub async fn new(data_dir: PathBuf, config: &Config) -> Result<Self> {
        let metastore = MetastoreClientMode::LocalInMemory.into_client().await?;
        let storage = EngineStorageConfig::try_from_path_buf(&data_dir)?;
        let engine = Engine::new(metastore, storage, Arc::new(Tracker::Nop), None).await?;
        let port = config.get_ports().first().unwrap();

        let addr = format!("http://0.0.0.0:{port}");
        let remote_client = RemoteClient::connect(addr.parse().unwrap()).await?;
        let mut session = engine
            .new_local_session_context(SessionVars::default(), SessionStorageConfig::default())
            .await?;
        let test_id = Uuid::new_v4();
        session
            .attach_remote_session(remote_client, Some(test_id))
            .await?;
        Ok(RpcTestClient {
            session: Arc::new(Mutex::new(session)),
            _engine: Arc::new(engine),
        })
    }
}

#[async_trait]
impl AsyncDB for RpcTestClient {
    type Error = sqlexec::errors::ExecError;
    type ColumnType = DefaultColumnType;
    async fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
        let mut output = Vec::new();
        let mut num_columns = 0;
        let RpcTestClient { session, .. } = self;

        let mut session = session.lock().await;
        const UNNAMED: String = String::new();
        let statements = session.parse_query(sql)?;

        for stmt in statements {
            session.prepare_statement(UNNAMED, stmt, Vec::new()).await?;
            let prepared = session.get_prepared_statement(&UNNAMED)?;
            let num_fields = prepared.output_fields().map(|f| f.len()).unwrap_or(0);
            session.bind_statement(
                UNNAMED,
                &UNNAMED,
                Vec::new(),
                vec![Format::Text; num_fields],
            )?;
            let stream = session.execute_portal(&UNNAMED, 0).await?;

            match stream {
                ExecutionResult::Query { stream, .. } => {
                    let batches = stream
                        .collect::<Vec<_>>()
                        .await
                        .into_iter()
                        .collect::<Result<Vec<_>, _>>()?;

                    for batch in batches {
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
                                        let scalar =
                                            String::from_utf8(buf.to_vec()).map_err(|e| {
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
                }
                ExecutionResult::Error(e) => return Err(e.into()),
                _ => (),
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

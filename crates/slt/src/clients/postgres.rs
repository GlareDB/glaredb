use std::ops::Deref;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use sqlexec::errors::ExecError;
use sqllogictest::{AsyncDB, DBOutput, DefaultColumnType};
use tokio::sync::{oneshot, Mutex};
use tokio_postgres::{Client, Config, NoTls, SimpleQueryMessage};
use tonic::async_trait;

#[derive(Clone)]
pub struct PgTestClient {
    client: Arc<Client>,
    conn_err_rx: Arc<Mutex<oneshot::Receiver<Result<(), tokio_postgres::Error>>>>,
}

impl Deref for PgTestClient {
    type Target = Client;
    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

impl PgTestClient {
    pub async fn new(client_config: &Config) -> Result<Self> {
        let (client, conn) = client_config.connect(NoTls).await?;
        let (conn_err_tx, conn_err_rx) = oneshot::channel();
        tokio::spawn(async move { conn_err_tx.send(conn.await) });
        Ok(Self {
            client: Arc::new(client),
            conn_err_rx: Arc::new(Mutex::new(conn_err_rx)),
        })
    }

    pub(super) async fn close(&self) -> Result<()> {
        let PgTestClient { conn_err_rx, .. } = self;
        let mut conn_err_rx = conn_err_rx.lock().await;

        if let Ok(result) = conn_err_rx.try_recv() {
            // Handle connection error
            match result {
                Ok(()) => Err(anyhow!("Client connection unexpectedly closed")),
                Err(err) => Err(anyhow!("Client connection errored: {err}")),
            }
        } else {
            Ok(())
        }
    }
}

#[async_trait]
impl AsyncDB for PgTestClient {
    type Error = sqlexec::errors::ExecError;
    type ColumnType = DefaultColumnType;
    async fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
        let mut output = Vec::new();
        let mut num_columns = 0;

        let rows = self
            .simple_query(sql)
            .await
            .map_err(|e| ExecError::Internal(format!("cannot execute simple query: {e}")))?;
        for row in rows {
            match row {
                SimpleQueryMessage::Row(row) => {
                    num_columns = row.len();
                    let mut row_output = Vec::with_capacity(row.len());
                    for i in 0..row.len() {
                        match row.get(i) {
                            Some(v) => {
                                if v.is_empty() {
                                    row_output.push("(empty)".to_string());
                                } else {
                                    row_output.push(v.to_string().trim().to_owned());
                                }
                            }
                            None => row_output.push("NULL".to_string()),
                        }
                    }
                    output.push(row_output);
                }
                SimpleQueryMessage::CommandComplete(_) => {}
                SimpleQueryMessage::RowDescription(_) => {}
                msg => unreachable!("message: '{msg:?}'"),
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

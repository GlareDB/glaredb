use crate::codec::{FramedConn, PgCodec};
use crate::errors::{PgSrvError, Result};
use crate::messages::{
    BackendMessage, ErrorResponse, FieldDescription, FrontendMessage, StartupMessage,
    TransactionStatus,
};
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::StreamExt;
use sqlexec::{
    engine::Engine,
    executor::{ExecutionResult, Executor},
    session::Session,
};
use std::collections::HashMap;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tracing::trace;

/// Default parameters to send to the frontend on startup. Existing postgres
/// drivers may expect these in the server response on startup.
///
/// See https://www.postgresql.org/docs/current/runtime-config-preset.html for
/// other parameters we may want to provide.
///
/// Some parameters  will eventually be provided at runtime.
const DEFAULT_READ_ONLY_PARAMS: &[(&str, &str)] = &[("server_version", "0.0.0")];

/// A wrapper around a sqlengine that implements the Postgres frontend/backend
/// protocol.
pub struct Handler {
    engine: Engine,
}

impl Handler {
    pub fn new(engine: Engine) -> Handler {
        Handler { engine }
    }

    /// Handle an incoming connection.
    pub async fn handle_connection<C>(&self, mut conn: C) -> Result<()>
    where
        C: AsyncRead + AsyncWrite + Unpin,
    {
        let startup = PgCodec::decode_startup_from_conn(&mut conn).await?;
        trace!(?startup, "received startup message");

        match startup {
            StartupMessage::StartupRequest { params, .. } => {
                self.begin(conn, params).await?;
            }
            StartupMessage::SSLRequest { .. } => {
                // 'N' for not supported, 'S' for supported.
                //
                // No SSL support for now, send back not supported and try
                // reading in a new startup message.
                conn.write_u8(b'N').await?;

                // Frontend should continue on with an unencrypted connection
                // (or exit).
                let startup = PgCodec::decode_startup_from_conn(&mut conn).await?;
                match startup {
                    StartupMessage::StartupRequest { params, .. } => {
                        self.begin(conn, params).await?
                    }
                    other => return Err(PgSrvError::UnexpectedStartupMessage(other)),
                }
            }
            StartupMessage::CancelRequest { .. } => {
                trace!("received cancel request");
                // TODO: Properly handle requests to cancel sessions.

                // Note that we should not respond to this request.
            }
        }

        Ok(())
    }

    /// Runs the postgres protocol for a connection to completion.
    async fn begin<C>(&self, conn: C, params: HashMap<String, String>) -> Result<()>
    where
        C: AsyncRead + AsyncWrite + Unpin,
    {
        trace!("starting protocol with params: {:?}", params);

        let mut framed = FramedConn::new(conn);

        // TODO: Check username, password, database.

        framed
            .send(BackendMessage::AuthenticationCleartextPassword)
            .await?;
        let msg = framed.read().await?;
        match msg {
            Some(FrontendMessage::PasswordMessage { password }) => {
                trace!(%password, "received password");
                framed.send(BackendMessage::AuthenticationOk).await?;
            }
            Some(other) => return Err(PgSrvError::UnexpectedFrontendMessage(other)), // TODO: Send error.
            None => return Ok(()),
        }

        let sess = match self.engine.new_session() {
            Ok(sess) => sess,
            Err(e) => {
                framed
                    .send(
                        ErrorResponse::fatal_internal(format!("failed to open session: {}", e))
                            .into(),
                    )
                    .await?;
                return Err(e.into());
            }
        };

        // Send server parameters.
        for (key, val) in DEFAULT_READ_ONLY_PARAMS {
            framed
                .send(BackendMessage::ParameterStatus {
                    key: key.to_string(),
                    val: val.to_string(),
                })
                .await?;
        }

        let cs = ClientSession::new(sess, framed);
        cs.run().await
    }
}

struct ClientSession<C> {
    conn: FramedConn<C>,
    session: Session, // TODO: Make this a trait for stubbability?
}

impl<C> ClientSession<C>
where
    C: AsyncRead + AsyncWrite + Unpin,
{
    fn new(session: Session, conn: FramedConn<C>) -> Self {
        ClientSession { session, conn }
    }

    async fn run(mut self) -> Result<()> {
        self.ready_for_query().await?;
        loop {
            let msg = self.conn.read().await?;

            match msg {
                Some(FrontendMessage::Query { sql }) => self.query(sql).await?,
                Some(other) => {
                    self.conn
                        .send(
                            ErrorResponse::feature_not_supported(format!(
                                "unsupported frontend message: {:?}",
                                other
                            ))
                            .into(),
                        )
                        .await?;
                    self.ready_for_query().await?;
                }
                None => {
                    trace!("connection closed");
                    return Ok(());
                }
            }
        }
    }

    async fn ready_for_query(&mut self) -> Result<()> {
        // TODO: Proper status.
        self.conn
            .send(BackendMessage::ReadyForQuery(TransactionStatus::Idle))
            .await
    }

    async fn query(&mut self, sql: String) -> Result<()> {
        trace!(%sql, "received query");

        let session = &mut self.session;
        let conn = &mut self.conn;
        let mut executor = Executor::new(&sql, session)?;
        // Determines if we send back an empty query response.
        let num_statements = executor.statements_remaining();

        // Iterate over all statements to completion, returning on the first
        // error.
        while let Some(result) = executor.execute_next().await {
            let result = match result {
                Ok(result) => result,
                Err(e) => {
                    self.conn
                        .send(
                            ErrorResponse::error_internal(format!("failed to execute: {:?}", e))
                                .into(),
                        )
                        .await?;
                    return self.ready_for_query().await;
                }
            };

            match result {
                ExecutionResult::Query { stream } => {
                    Self::stream_batch(conn, stream).await?;
                    Self::command_complete(conn, "SELECT").await?
                }
                ExecutionResult::Begin => Self::command_complete(conn, "BEGIN").await?,
                ExecutionResult::Commit => Self::command_complete(conn, "COMMIT").await?,
                ExecutionResult::Rollback => Self::command_complete(conn, "ROLLBACK").await?,
                ExecutionResult::WriteSuccess => Self::command_complete(conn, "INSERT").await?,
                ExecutionResult::CreateTable => {
                    Self::command_complete(conn, "CREATE_TABLE").await?
                }
                ExecutionResult::SetLocal => Self::command_complete(conn, "SET").await?,
            }
        }

        if num_statements == 0 {
            self.conn.send(BackendMessage::EmptyQueryResponse).await?;
        }

        self.ready_for_query().await
    }

    async fn stream_batch(
        conn: &mut FramedConn<C>,
        mut stream: SendableRecordBatchStream,
    ) -> Result<()> {
        let schema = stream.schema();
        let fields: Vec<_> = schema
            .fields
            .iter()
            .map(|field| FieldDescription::new_named(field.name()))
            .collect();
        conn.send(BackendMessage::RowDescription(fields)).await?;

        while let Some(result) = stream.next().await {
            let batch = result?;
            for row_idx in 0..batch.num_rows() {
                // Clone is cheapish here, all columns behind an arc.
                conn.send(BackendMessage::DataRow(batch.clone(), row_idx))
                    .await?;
            }
        }
        Ok(())
    }

    async fn command_complete(conn: &mut FramedConn<C>, tag: impl Into<String>) -> Result<()> {
        conn.send(BackendMessage::CommandComplete { tag: tag.into() })
            .await
    }
}

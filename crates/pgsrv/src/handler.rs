use crate::codec::{FramedConn, PgCodec};
use crate::errors::{PgSrvError, Result};
use crate::messages::{
    BackendMessage, DescribeObjectType, ErrorResponse, FieldDescription, FrontendMessage,
    StartupMessage, TransactionStatus,
};
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::StreamExt;
use sqlexec::logical_plan::LogicalPlan;
use sqlexec::{
    engine::Engine,
    executor::{ExecutionResult, Executor},
    session::Session,
};
use std::collections::HashMap;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tracing::{trace, warn};

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

    error_state: bool,
}

impl<C> ClientSession<C>
where
    C: AsyncRead + AsyncWrite + Unpin,
{
    fn new(session: Session, conn: FramedConn<C>) -> Self {
        ClientSession {
            session,
            conn,
            error_state: false,
        }
    }

    async fn run(mut self) -> Result<()> {
        self.ready_for_query().await?;
        loop {
            let msg = self.conn.read().await?;
            // If we're in an error state, we should only process Sync messages.
            // Until this is received, we should discard all incoming messages
            if self.error_state {
                match msg {
                    Some(FrontendMessage::Sync) => {
                        self.clear_error();
                        self.ready_for_query().await?;
                        continue;
                    }
                    Some(other) => {
                        tracing::warn!(?other, "discarding message");
                    }
                    None => {
                        tracing::debug!("connection closed");
                        return Ok(());
                    }
                }
            } else {
                // Execute messages as normal if not in an error state.
                match msg {
                    Some(FrontendMessage::Query { sql }) => self.query(sql).await?,
                    Some(FrontendMessage::Parse {
                        name,
                        sql,
                        param_types,
                    }) => self.parse(name, sql, param_types).await?,
                    Some(FrontendMessage::Bind {
                        portal,
                        statement,
                        param_formats,
                        param_values,
                        result_formats,
                    }) => {
                        self.bind(
                            portal,
                            statement,
                            param_formats,
                            param_values,
                            result_formats,
                        )
                        .await?
                    }
                    Some(FrontendMessage::Describe { object_type, name }) => {
                        self.describe(object_type, name).await?
                    }
                    Some(FrontendMessage::Execute { portal, max_rows }) => {
                        self.execute(portal, max_rows).await?
                    }
                    Some(FrontendMessage::Sync) => self.sync().await?,
                    Some(other) => {
                        warn!(?other, "unsupported frontend message");
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

            Self::send_result(conn, result).await?;
        }

        if num_statements == 0 {
            self.conn.send(BackendMessage::EmptyQueryResponse).await?;
        }

        self.ready_for_query().await
    }

    /// Parse the provided SQL statement and store it in the session.
    async fn parse(&mut self, name: String, sql: String, param_types: Vec<i32>) -> Result<()> {
        let session = &mut self.session;
        let conn = &mut self.conn;

        // an empty name selectss the unnamed prepared statement
        let name = if name.is_empty() { None } else { Some(name) };

        trace!(?name, %sql, ?param_types, "received parse");

        session.create_prepared_statement(name, sql, param_types)?;

        conn.send(BackendMessage::ParseComplete).await?;

        Ok(())
    }

    async fn bind(
        &mut self,
        portal: String,
        statement: String,
        param_formats: Vec<i16>,
        param_values: Vec<Option<Vec<u8>>>,
        result_formats: Vec<i16>,
    ) -> Result<()> {
        let portal_name = if portal.is_empty() {
            None
        } else {
            Some(portal)
        };
        let statement_name = if statement.is_empty() {
            None
        } else {
            Some(statement)
        };

        // param_formats can be empty, in which case all parameters (if any) are assumed to be text
        // or it may have one entry, in which case all parameters are assumed to be of that format
        // or it may have one entry per parameter, in which case each parameter is assumed to be of that format
        // each code must be 0 (text) or 1 (binary)
        let param_formats = if param_formats.is_empty() {
            if param_values.is_empty() {
                vec![]
            } else {
                vec![0]
            }
        } else if param_formats.len() == 1 {
            vec![param_formats[0]; param_values.len()]
        } else {
            param_formats
        };

        trace!(
            ?portal_name,
            ?statement_name,
            ?param_formats,
            ?param_values,
            ?result_formats,
            "received bind"
        );

        let session = &mut self.session;
        let conn = &mut self.conn;

        session.bind_prepared_statement(
            portal_name,
            statement_name,
            param_formats,
            param_values,
            result_formats,
        )?;

        conn.send(BackendMessage::BindComplete).await?;

        Ok(())
    }

    async fn describe(&mut self, object_type: DescribeObjectType, name: String) -> Result<()> {
        let session = &mut self.session;
        let conn = &mut self.conn;

        let name = if name.is_empty() { None } else { Some(name) };

        trace!(?name, ?object_type, "received describe");

        match object_type {
            DescribeObjectType::Statement => match session.get_prepared_statement(&name) {
                Some(statement) => {
                    // The Describe message statement variant returns a ParameterDescription message describing
                    // the parameters needed by the statement, followed by a RowDescription message describing the
                    // rows that will be returned when the statement is eventually executed.
                    // If the statement will not return rows, then a NoData message is returned.
                    conn.send(BackendMessage::ParameterDescription(
                        statement.param_types.clone(),
                    ))
                    .await?;

                    // TODO: return RowDescription if query will return rows
                    conn.send(BackendMessage::NoData).await?;
                }
                None => {
                    self.conn
                        .send(
                            ErrorResponse::error_internal(format!(
                                "unknown prepared statement: {:?}",
                                name
                            ))
                            .into(),
                        )
                        .await?;
                }
            },
            DescribeObjectType::Portal => {
                // Describe (portal variant) returns a RowDescription message describing the rows
                // that will be returned. If the portal contains a query that returns no rows, then
                // a NoData message is returned instead.
                match session.get_portal(&name) {
                    Some(portal) => match &portal.plan {
                        LogicalPlan::Ddl(_) => {
                            self.conn.send(BackendMessage::NoData).await?;
                        }
                        LogicalPlan::Write(_) => {
                            todo!("return portal describe response for Write");
                        }
                        LogicalPlan::Query(df_plan) => {
                            let schema = df_plan.schema();
                            let fields: Vec<_> = schema
                                .fields()
                                .iter()
                                .map(|field| FieldDescription::new_named(field.name()))
                                .collect();
                            conn.send(BackendMessage::RowDescription(fields)).await?;
                        }
                        LogicalPlan::Transaction(_) => {
                            todo!("return portal describe response for Transaction");
                        }
                        LogicalPlan::Runtime => {
                            todo!("return portal describe response for Runtime");
                        }
                    },
                    None => {
                        self.conn
                            .send(
                                ErrorResponse::error_internal(format!(
                                    "unknown portal: {:?}",
                                    name
                                ))
                                .into(),
                            )
                            .await?;
                    }
                }
            }
        }

        Ok(())
    }

    async fn execute(&mut self, portal: String, max_rows: i32) -> Result<()> {
        let portal_name = if portal.is_empty() {
            None
        } else {
            Some(portal)
        };

        let session = &mut self.session;
        let conn = &mut self.conn;

        trace!(?portal_name, ?max_rows, "received execute");

        let result = session.execute_portal(&portal_name, max_rows).await?;
        Self::send_result(conn, result).await?;

        Ok(())
    }

    async fn sync(&mut self) -> Result<()> {
        trace!("received sync");

        self.ready_for_query().await
    }

    async fn send_result(conn: &mut FramedConn<C>, result: ExecutionResult) -> Result<()> {
        match result {
            ExecutionResult::Query { stream } => {
                Self::stream_batch(conn, stream).await?;
                Self::command_complete(conn, "SELECT").await?
            }
            ExecutionResult::Begin => Self::command_complete(conn, "BEGIN").await?,
            ExecutionResult::Commit => Self::command_complete(conn, "COMMIT").await?,
            ExecutionResult::Rollback => Self::command_complete(conn, "ROLLBACK").await?,
            ExecutionResult::WriteSuccess => Self::command_complete(conn, "INSERT").await?,
            ExecutionResult::CreateTable => Self::command_complete(conn, "CREATE TABLE").await?,
            ExecutionResult::SetLocal => Self::command_complete(conn, "SET").await?,
            ExecutionResult::DropTables => Self::command_complete(conn, "DROP TABLE").await?,
        }
        Ok(())
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

    fn set_error(&mut self) {
        self.error_state = true;
    }

    fn clear_error(&mut self) {
        self.error_state = false;
    }
}

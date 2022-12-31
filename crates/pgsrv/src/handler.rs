use crate::codec::server::{FramedConn, PgCodec};
use crate::errors::{PgSrvError, Result};
use crate::messages::{
    BackendMessage, DescribeObjectType, ErrorResponse, FieldDescription, FrontendMessage, SqlState,
    StartupMessage, TransactionStatus,
};
use crate::ssl::{Connection, SslConfig};
use datafusion::arrow::datatypes::Schema as ArrowSchema;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::StreamExt;
use pgrepr::format::Format;
use sqlexec::{
    engine::Engine,
    parser::{self, StatementWithExtensions},
    session::{ExecutionResult, Session},
};
use std::collections::HashMap;
use std::collections::VecDeque;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tracing::{debug, trace, warn};

/// A wrapper around a SQL engine that implements the Postgres frontend/backend
/// protocol.
pub struct ProtocolHandler {
    engine: Engine,
    ssl_conf: Option<SslConfig>,
}

impl ProtocolHandler {
    pub fn new(engine: Engine) -> ProtocolHandler {
        ProtocolHandler {
            engine,
            // TODO: Allow specifying SSL/TLS on the GlareDB side as well. I
            // want to hold off on doing that until we have a shared config
            // between the proxy and GlareDB.
            ssl_conf: None,
        }
    }

    pub async fn handle_connection<C>(&self, conn: C) -> Result<()>
    where
        C: AsyncRead + AsyncWrite + Unpin,
    {
        let mut conn = Connection::new_unencrypted(conn);
        loop {
            let startup = PgCodec::decode_startup_from_conn(&mut conn).await?;
            trace!(?startup, "received startup message (local)");

            match startup {
                StartupMessage::StartupRequest { params, .. } => {
                    self.begin(conn, params).await?;
                    return Ok(());
                }
                StartupMessage::SSLRequest { .. } => {
                    conn = match (conn, &self.ssl_conf) {
                        (Connection::Unencrypted(mut conn), Some(conf)) => {
                            trace!("accepting ssl request");
                            // SSL supported, send back that we support it and
                            // start encrypting.
                            conn.write_all(&[b'S']).await?;
                            Connection::new_encrypted(conn, conf).await?
                        }
                        (mut conn, _) => {
                            trace!("rejecting ssl request");
                            // SSL not supported (or the connection is already
                            // wrapped). Reject and continue.
                            conn.write_all(&[b'N']).await?;
                            conn
                        }
                    }
                }
                StartupMessage::CancelRequest { .. } => {
                    self.cancel(conn).await?;
                    return Ok(());
                }
            }
        }
    }

    /// Runs the postgres protocol for a connection to completion.
    async fn begin<C>(&self, conn: Connection<C>, params: HashMap<String, String>) -> Result<()>
    where
        C: AsyncRead + AsyncWrite + Unpin,
    {
        trace!("starting protocol with params: {:?}", params);

        let mut framed = FramedConn::new(conn);

        framed
            .send(BackendMessage::AuthenticationCleartextPassword)
            .await?;
        let msg = framed.read().await?;
        match msg {
            Some(FrontendMessage::PasswordMessage { password }) => {
                framed.send(BackendMessage::AuthenticationOk).await?;
            }
            Some(other) => return Err(PgSrvError::UnexpectedFrontendMessage(other)), // TODO: Send error.
            None => return Ok(()),
        }

        let mut sess = match self.engine.new_session() {
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

        // Set params provided on startup.
        //
        // Note that we're ignoring unknown params.
        let vars = sess.get_session_vars_mut();
        for (key, val) in &params {
            if let Err(e) = vars.set(key, val) {
                trace!(%e, %key, %val, "unable to set session variable from startup param");
            }
        }

        // Send server parameters.
        let msgs: Vec<_> = sess
            .get_session_vars()
            .startup_vars_iter()
            .map(|var| BackendMessage::ParameterStatus {
                key: var.name().to_string(),
                val: var.formatted_value(),
            })
            .collect();
        for msg in msgs {
            framed.send(msg).await?;
        }

        let cs = ClientSession::new(sess, framed);
        cs.run().await
    }

    /// Cancel a connection.
    ///
    /// Unimplemented. The protocol states that there's no guarantee that
    /// anything is actually canceled, so no-op is fine for now.
    async fn cancel<C>(&self, _conn: Connection<C>) -> Result<()>
    where
        C: AsyncRead + AsyncWrite + Unpin,
    {
        trace!("cancel received (local)");
        Ok(())
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

    /// Send an error response to the client.
    async fn send_error(&mut self, err: ErrorResponse) -> Result<()> {
        self.conn.send(err.into()).await?;
        Ok(())
    }

    async fn ready_for_query(&mut self) -> Result<()> {
        // TODO: Proper status.
        self.conn
            .send(BackendMessage::ReadyForQuery(TransactionStatus::Idle))
            .await
    }

    async fn query(&mut self, sql: String) -> Result<()> {
        let session = &mut self.session;
        let conn = &mut self.conn;

        let stmts = match parse_sql(&sql) {
            Ok(stmts) => stmts,
            Err(e) => {
                self.send_error(e.into()).await?;
                return self.ready_for_query().await;
            }
        };

        // Determines if we send back an empty query response.
        let num_statements = stmts.len();

        for stmt in stmts {
            // TODO: Ensure in transaction.

            // Note everything is using unnamed portals/prepared statements.

            const UNNAMED: String = String::new();

            // Parse...
            if let Err(e) = session.prepare_statement(UNNAMED, Some(stmt), Vec::new()) {
                self.send_error(e.into()).await?;
                return self.ready_for_query().await;
            };

            // Describe...
            let stmt = match session.get_prepared_statement(&UNNAMED) {
                Ok(stmt) => stmt.clone(),
                Err(e) => {
                    self.send_error(e.into()).await?;
                    return self.ready_for_query().await;
                }
            };

            // Bind...
            let result_formats = match extend_formats(
                Vec::new(),
                stmt.output_schema()
                    .and_then(|schema| Some(schema.fields().len()))
                    .unwrap_or(0),
            ) {
                Ok(v) => v,
                Err(e) => {
                    self.send_error(e.into()).await?;
                    return self.ready_for_query().await;
                }
            };
            if let Err(e) =
                session.bind_statement(UNNAMED, &UNNAMED, Vec::new(), Vec::new(), result_formats)
            {
                self.send_error(e.into()).await?;
                return self.ready_for_query().await;
            }

            // Maybe row description...
            //
            // Only send back a row description if the statement produces
            // output.
            if let Some(schema) = stmt.output_schema() {
                Self::send_row_descriptor(conn, schema).await?;
            }

            // Execute...
            let result = match session.execute_portal(&UNNAMED, 0).await {
                Ok(result) => result,
                Err(e) => {
                    self.send_error(e.into()).await?;
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
        // TODO: Ensure in transaction.

        let mut stmts = match parse_sql(&sql) {
            Ok(stmts) => stmts,
            Err(e) => return self.send_error(e).await,
        };

        // Can only have one statement per parse.
        if stmts.len() > 1 {
            return self
                .send_error(ErrorResponse::error_internal(
                    "cannot parse multiple statements",
                ))
                .await;
        }

        // TODO: Check if in failed transaction.

        // Store statement for future use.
        match self
            .session
            .prepare_statement(name, stmts.pop_front(), param_types)
        {
            Ok(_) => self.conn.send(BackendMessage::ParseComplete).await,
            Err(e) => self.send_error(e.into()).await,
        }
    }

    /// Bind to a prepared statement.
    async fn bind(
        &mut self,
        portal: String,
        statement: String,
        param_formats: Vec<Format>,
        param_values: Vec<Option<Vec<u8>>>,
        result_formats: Vec<Format>,
    ) -> Result<()> {
        // TODO: Ensure in transaction.

        // Check for the statement.
        let stmt = match self.session.get_prepared_statement(&statement) {
            Ok(stmt) => stmt,
            Err(e) => return self.send_error(e.into()).await,
        };

        // TODO: Check and parse param formats and values.

        // Extend out the result formats.
        let result_formats = match extend_formats(
            result_formats,
            stmt.output_schema()
                .and_then(|schema| Some(schema.fields().len()))
                .unwrap_or(0),
        ) {
            Ok(formats) => formats,
            Err(e) => return self.send_error(e).await,
        };

        match self.session.bind_statement(
            portal,
            &statement,
            param_formats,
            param_values,
            result_formats,
        ) {
            Ok(_) => self.conn.send(BackendMessage::BindComplete).await,
            Err(e) => self.send_error(e.into()).await,
        }
    }

    async fn describe(&mut self, object_type: DescribeObjectType, name: String) -> Result<()> {
        // TODO: Ensure in a transaction.

        let conn = &mut self.conn;
        match object_type {
            DescribeObjectType::Statement => match self.session.get_prepared_statement(&name) {
                Ok(stmt) => {
                    // TODO: We don't accept parameters yet. So just send back
                    // an empty paramaters list.
                    conn.send(BackendMessage::ParameterDescription(Vec::new()))
                        .await?;

                    // Send back row description.
                    match stmt.output_schema() {
                        Some(schema) => Self::send_row_descriptor(conn, schema).await?,
                        None => self.conn.send(BackendMessage::NoData).await?,
                    }

                    Ok(())
                }
                Err(e) => self.send_error(e.into()).await,
            },
            DescribeObjectType::Portal => match self.session.get_portal(&name) {
                Ok(portal) => {
                    // Send back row description.
                    match portal.output_schema() {
                        Some(schema) => Self::send_row_descriptor(conn, schema).await?,
                        None => self.conn.send(BackendMessage::NoData).await?,
                    }
                    Ok(())
                }
                Err(e) => self.send_error(e.into()).await,
            },
        }
    }

    async fn execute(&mut self, portal: String, max_rows: i32) -> Result<()> {
        // TODO: Ensure in transaction.

        let conn = &mut self.conn;
        let result = self.session.execute_portal(&portal, max_rows).await?;
        Self::send_result(conn, result).await?;

        Ok(())
    }

    async fn sync(&mut self) -> Result<()> {
        self.ready_for_query().await
    }

    async fn send_result(conn: &mut FramedConn<C>, result: ExecutionResult) -> Result<()> {
        match result {
            ExecutionResult::Query { stream } => {
                let num_rows = Self::stream_batch(conn, stream).await?;
                Self::command_complete(conn, format!("SELECT {}", num_rows)).await?
            }
            ExecutionResult::EmptyQuery => conn.send(BackendMessage::EmptyQueryResponse).await?,
            ExecutionResult::Begin => Self::command_complete(conn, "BEGIN").await?,
            ExecutionResult::Commit => Self::command_complete(conn, "COMMIT").await?,
            ExecutionResult::Rollback => Self::command_complete(conn, "ROLLBACK").await?,
            ExecutionResult::WriteSuccess => Self::command_complete(conn, "INSERT").await?,
            ExecutionResult::CreateTable => Self::command_complete(conn, "CREATE TABLE").await?,
            ExecutionResult::CreateSchema => Self::command_complete(conn, "CREATE SCHEMA").await?,
            ExecutionResult::SetLocal => Self::command_complete(conn, "SET").await?,
            ExecutionResult::DropTables => Self::command_complete(conn, "DROP TABLE").await?,
            ExecutionResult::DropSchemas => Self::command_complete(conn, "DROP SCHEMA").await?,
        }
        Ok(())
    }

    /// Convert an arrow schema into a row descriptor and send it to the client.
    // TODO: Provide formats too.
    async fn send_row_descriptor(conn: &mut FramedConn<C>, schema: &ArrowSchema) -> Result<()> {
        let fields: Vec<_> = schema
            .fields
            .iter()
            .map(|field| FieldDescription::new_named(field.name()))
            .collect();
        conn.send(BackendMessage::RowDescription(fields)).await?;
        Ok(())
    }

    /// Streams the batch to the client, returned the total number of rows sent.
    async fn stream_batch(
        conn: &mut FramedConn<C>,
        mut stream: SendableRecordBatchStream,
    ) -> Result<usize> {
        let mut num_rows = 0;
        while let Some(result) = stream.next().await {
            let batch = result?;
            num_rows += batch.num_rows();
            for row_idx in 0..batch.num_rows() {
                // Clone is cheapish here, all columns behind an arc.
                conn.send(BackendMessage::DataRow(batch.clone(), row_idx))
                    .await?;
            }
        }
        Ok(num_rows)
    }

    async fn command_complete(conn: &mut FramedConn<C>, tag: impl Into<String>) -> Result<()> {
        conn.send(BackendMessage::CommandComplete { tag: tag.into() })
            .await
    }
}

/// Parse a sql string, returning an error response if failed to parse.
fn parse_sql(sql: &str) -> Result<VecDeque<StatementWithExtensions>, ErrorResponse> {
    parser::parse_sql(sql).map_err(|e| ErrorResponse::error(SqlState::SyntaxError, e.to_string()))
}

/// Extend a vector of format codes to the desired size.
///
/// See the doc for the `Bind` message for more.
fn extend_formats(formats: Vec<Format>, num: usize) -> Result<Vec<Format>, ErrorResponse> {
    Ok(match formats.len() {
        0 => vec![Format::Text; num], // Everything defaults to text,
        1 => vec![formats[0]; num],   // Use the singly specified format for everything.
        len if len == num => formats,
        len => {
            return Err(ErrorResponse::error_internal(format!(
                "invalid number for format specifiers, got: {}, expected: {}",
                len, num,
            )))
        }
    })
}

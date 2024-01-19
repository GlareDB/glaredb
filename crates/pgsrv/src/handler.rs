use crate::auth::{LocalAuthenticator, PasswordMode};
use crate::codec::server::{FramedConn, PgCodec};
use crate::errors::{PgSrvError, Result};
use crate::messages::{
    BackendMessage, DescribeObjectType, ErrorResponse, FieldDescriptionBuilder, FrontendMessage,
    StartupMessage, TransactionStatus,
};
use crate::proxy::{
    ProxyKey, GLAREDB_DATABASE_ID_KEY, GLAREDB_GCS_STORAGE_BUCKET_KEY,
    GLAREDB_MAX_CREDENTIALS_COUNT_KEY, GLAREDB_MAX_DATASOURCE_COUNT_KEY,
    GLAREDB_MAX_TUNNEL_COUNT_KEY, GLAREDB_MEMORY_LIMIT_BYTES_KEY, GLAREDB_USER_ID_KEY,
};
use crate::ssl::{Connection, SslConfig};
use datafusion::arrow::datatypes::DataType;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::scalar::ScalarValue;
use datafusion::variable::VarType;
use datafusion_ext::vars::{Dialect, SessionVars};
use futures::StreamExt;
use pgrepr::format::Format;
use pgrepr::scalar::Scalar;
use sqlexec::context::local::{OutputFields, Portal, PreparedStatement};
use sqlexec::engine::SessionStorageConfig;
use sqlexec::{
    engine::Engine,
    parser::{self, StatementWithExtensions},
    session::{ExecutionResult, Session},
};
use std::collections::HashMap;
use std::collections::VecDeque;
use std::ops::DerefMut;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio_postgres::types::Type as PgType;
use tracing::{debug, debug_span, warn, Instrument};
use uuid::Uuid;

pub struct ProtocolHandlerConfig {
    /// Authenticor to use on the server side.
    pub authenticator: Box<dyn LocalAuthenticator>,
    /// SSL configuration to use on the server side.
    pub ssl_conf: Option<SslConfig>,
    /// If the server should be configured for integration tests. This is only
    /// applicable for local databases.
    pub integration_testing: bool,
}

/// A wrapper around a SQL engine that implements the Postgres frontend/backend
/// protocol.
pub struct ProtocolHandler {
    engine: Arc<Engine>,
    conf: ProtocolHandlerConfig,
}

impl ProtocolHandler {
    pub fn new(engine: Arc<Engine>, conf: ProtocolHandlerConfig) -> Self {
        ProtocolHandler { engine, conf }
    }

    pub async fn handle_connection<C>(&self, id: Uuid, conn: C) -> Result<()>
    where
        C: AsyncRead + AsyncWrite + Unpin,
    {
        let mut conn = Connection::new_unencrypted(conn);
        loop {
            let startup = PgCodec::decode_startup_from_conn(&mut conn).await?;
            debug!(?startup, "received startup message (local)");

            match startup {
                StartupMessage::StartupRequest { params, .. } => {
                    self.begin(id, conn, params).await?;
                    return Ok(());
                }
                StartupMessage::SSLRequest { .. } => {
                    conn = match (conn, &self.conf.ssl_conf) {
                        (Connection::Unencrypted(mut conn), Some(conf)) => {
                            debug!("accepting ssl request");
                            // SSL supported, send back that we support it and
                            // start encrypting.
                            conn.write_all(&[b'S']).await?;
                            Connection::new_encrypted(conn, conf.config.clone()).await?
                        }
                        (mut conn, _) => {
                            debug!("rejecting ssl request");
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

    /// Read a value from the startup params that's been placed by pgsrv.
    ///
    /// This will also write any errors to the connection.
    async fn read_proxy_key_val<C, V, K>(
        &self,
        framed: &mut FramedConn<C>,
        key: &K,
        params: &HashMap<String, String>,
    ) -> Result<V>
    where
        C: AsyncRead + AsyncWrite + Unpin,
        K: ProxyKey<V>,
    {
        match key.value_from_params(params) {
            Ok(v) => Ok(v),
            Err(e) => {
                let resp = ErrorResponse::from(&e);
                framed.send(resp.into()).await?;
                // Technicall a client error, but the most likely cause is
                // misconfiguration on our end, go ahead and return the error so
                // it gets logged.
                Err(e)
            }
        }
    }

    /// Whether the server should be configured for integration testing.
    fn is_integration_testing_enabled(&self) -> bool {
        self.conf.integration_testing
    }

    /// Runs the postgres protocol for a connection to completion.
    async fn begin<C>(
        &self,
        conn_id: Uuid,
        conn: Connection<C>,
        params: HashMap<String, String>,
    ) -> Result<()>
    where
        C: AsyncRead + AsyncWrite + Unpin,
    {
        debug!("starting protocol with params: {:?}", params);

        let mut framed = FramedConn::new(conn);

        // Get params.
        // TODO: Possibly just serialize these into a single key on the proxy
        // side and deserialize here?
        let db_id = self
            .read_proxy_key_val(&mut framed, &GLAREDB_DATABASE_ID_KEY, &params)
            .await?;
        let is_cloud_instance = !db_id.is_nil();

        let user_id = self
            .read_proxy_key_val(&mut framed, &GLAREDB_USER_ID_KEY, &params)
            .await?;
        let max_datasource_count = self
            .read_proxy_key_val(&mut framed, &GLAREDB_MAX_DATASOURCE_COUNT_KEY, &params)
            .await?;
        let memory_limit_bytes = self
            .read_proxy_key_val(&mut framed, &GLAREDB_MEMORY_LIMIT_BYTES_KEY, &params)
            .await?;
        let max_tunnel_count = self
            .read_proxy_key_val(&mut framed, &GLAREDB_MAX_TUNNEL_COUNT_KEY, &params)
            .await?;
        let max_credentials_count = self
            .read_proxy_key_val(&mut framed, &GLAREDB_MAX_CREDENTIALS_COUNT_KEY, &params)
            .await?;

        let storage_bucket = params.get(GLAREDB_GCS_STORAGE_BUCKET_KEY).cloned();

        // Standard postgres params. These values are used only for informational purposes.
        let user_name = params.get("user").cloned().unwrap_or_default();
        let database_name = params.get("database").cloned().unwrap_or_default();
        let db_id = if self.is_integration_testing_enabled() {
            // When in integration testing mode, try to get the database ID from dbname.
            database_name.parse::<Uuid>().unwrap_or(db_id)
        } else {
            db_id
        };

        // Handle password.
        match self.conf.authenticator.password_mode() {
            PasswordMode::RequireCleartext => {
                framed
                    .send(BackendMessage::AuthenticationCleartextPassword)
                    .await?;
                let msg = framed.read().await?;
                match msg {
                    Some(FrontendMessage::PasswordMessage { password }) => {
                        match self.conf.authenticator.authenticate(
                            &user_name,
                            &password,
                            &database_name,
                        ) {
                            Ok(sess) => sess,
                            Err(e) => {
                                framed
                                    .send(
                                        ErrorResponse::fatal_internal(format!(
                                            "Failed to authenticate: {}",
                                            e
                                        ))
                                        .into(),
                                    )
                                    .await?;
                                return Err(e);
                            }
                        }
                        framed.send(BackendMessage::AuthenticationOk).await?;
                    }
                    Some(other) => {
                        // TODO: Send error.
                        return Err(PgSrvError::UnexpectedFrontendMessage(Box::new(other)));
                    }
                    None => return Ok(()),
                }
            }
            PasswordMode::NoPassword { drop_auth_messages } => {
                if drop_auth_messages {
                    // Send the message to frontend to ask for an auth message.
                    // We will drop this message later on.
                    framed
                        .send(BackendMessage::AuthenticationCleartextPassword)
                        .await?;

                    // Read the auth message from the frontend. This will be
                    // ignored.
                    let msg = framed.peek().await?;
                    match msg {
                        Some(msg) if msg.is_auth_message() => {
                            let dropped = framed.read().await?; // Drop auth message.
                            warn!(?dropped, "dropping authentication message");
                        }
                        Some(_msg) => (), // We peeked a message not related to auth.
                        None => return Ok(()), // Connection closed
                    }
                }

                // Nothin to do.
                framed.send(BackendMessage::AuthenticationOk).await?;
            }
        }
        let mut vars = SessionVars::default()
            .with_user_id(user_id, VarType::System)
            .with_user_name(user_name, VarType::System)
            .with_connection_id(conn_id, VarType::System)
            .with_database_id(db_id, VarType::System)
            .with_database_name(database_name, VarType::System)
            .with_max_datasource_count(max_datasource_count, VarType::System)
            .with_memory_limit_bytes(memory_limit_bytes, VarType::System)
            .with_max_tunnel_count(max_tunnel_count, VarType::System)
            .with_max_credentials_count(max_credentials_count, VarType::System)
            .with_is_cloud_instance(is_cloud_instance, VarType::System);

        // Set other params provided on startup. Note that these are all set as
        // the "user" since these include values set in options.
        //
        // Note that we're ignoring unknown params, or params that we're unable
        // to set as a user.
        for (key, val) in &params {
            if let Err(e) = vars.set(key, val, VarType::UserDefined) {
                debug!(%e, %key, %val, "unable to set session variable from startup param");
            }
        }

        let sess = match self
            .engine
            .new_local_session_context(
                vars,
                SessionStorageConfig {
                    gcs_bucket: storage_bucket,
                },
            )
            .await
        {
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
        let msgs: Vec<_> = sess
            .get_session_vars()
            .read()
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
        debug!("cancel received (local)");
        Ok(())
    }
}

struct ClientSession<C, S> {
    conn: FramedConn<C>,
    session: S,
}

/// This helper macro is used so we can call some `get_*` methods on the
/// session and maybe do some processing over it.
///
/// The motivation to write this macro is that during a query, we don't want to
/// return `Err(...)` in case of non-connection errors.
macro_rules! session_do {
    ($client:ident, $sess:ident, $get_fn:ident, $name:expr, $do:expr) => {
        match $sess.$get_fn($name) {
            Ok(v) => $do(v),
            Err(e) => {
                $client.send_error(e.into()).await?;
                return $client.ready_for_query().await;
            }
        }
    };
}

impl<C, S> ClientSession<C, S>
where
    C: AsyncRead + AsyncWrite + Unpin,
    S: DerefMut<Target = Session>,
{
    fn new(session: S, conn: FramedConn<C>) -> Self {
        ClientSession { session, conn }
    }

    async fn run(mut self) -> Result<()> {
        self.ready_for_query().await?;
        loop {
            let msg = self.conn.read().await?;

            let msg = match msg {
                Some(msg) => msg,
                None => {
                    // No message received, connection closed.
                    debug!("connection closed");
                    return Ok(());
                }
            };

            let span = debug_span!("pg_protocol_message", name = msg.name());
            span.follows_from(tracing::Span::current());

            match msg {
                FrontendMessage::Query { sql } => self.query(sql).instrument(span).await?,
                FrontendMessage::Parse {
                    name,
                    sql,
                    param_types,
                } => self.parse(name, sql, param_types).instrument(span).await?,
                FrontendMessage::Bind {
                    portal,
                    statement,
                    param_formats,
                    param_values,
                    result_formats,
                } => {
                    self.bind(
                        portal,
                        statement,
                        param_formats,
                        param_values,
                        result_formats,
                    )
                    .instrument(span)
                    .await?
                }
                FrontendMessage::Describe { object_type, name } => {
                    self.describe(object_type, name).instrument(span).await?
                }
                FrontendMessage::Execute { portal, max_rows } => {
                    self.execute(portal, max_rows).instrument(span).await?
                }
                FrontendMessage::Close { object_type, name } => {
                    self.close_object(object_type, name)
                        .instrument(span)
                        .await?
                }
                FrontendMessage::Sync => self.sync().instrument(span).await?,
                FrontendMessage::Flush => self.flush().instrument(span).await?,
                FrontendMessage::Terminate => return Ok(()),
                other => {
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
            }
        }
    }

    /// Send an error response to the client.
    async fn send_error(&mut self, err: ErrorResponse) -> Result<()> {
        self.conn.send(err.into()).await?;
        Ok(())
    }

    async fn ready_for_query(&mut self) -> Result<()> {
        // Display notice messages before indicating we're ready for the next
        // query. The pg protocol does not presribe a specific flow for notice
        // messages, and so frontends should be capable of handling notices at
        // any point in the message flow.
        for notice in self.session.take_notices() {
            self.conn
                .send(BackendMessage::NoticeResponse(notice))
                .await?;
        }

        // TODO: Proper status.
        self.conn
            .send(BackendMessage::ReadyForQuery(TransactionStatus::Idle))
            .await?;
        self.flush().await
    }

    /// Run the simple query flow.
    ///
    /// Note that this should only returns errors related to the underlying
    /// connection. All errors resulting from query execution should be sent to
    /// client following by a "ready for query".
    async fn query(&mut self, sql: String) -> Result<()> {
        let session = &mut self.session;
        let conn = &mut self.conn;

        let stmts = match parse_sql(session.get_session_vars(), &sql) {
            Ok(stmts) => stmts,
            Err(e) => {
                self.send_error(e).await?;
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
            if let Err(e) = session.prepare_statement(UNNAMED, stmt, Vec::new()).await {
                self.send_error(e.into()).await?;
                return self.ready_for_query().await;
            };

            // Describe statement and get number of fields...
            fn get_num_fields(s: &PreparedStatement) -> usize {
                s.output_fields().map(|f| f.len()).unwrap_or(0)
            }
            let num_fields = session_do!(
                self,
                session,
                get_prepared_statement,
                &UNNAMED,
                get_num_fields
            );

            // Bind...
            if let Err(e) =
                session.bind_statement(UNNAMED, &UNNAMED, Vec::new(), all_text_formats(num_fields))
            {
                self.send_error(e.into()).await?;
                return self.ready_for_query().await;
            }

            // Execute...
            let stream = match session.execute_portal(&UNNAMED, 0).await {
                Ok(stream) => stream,
                Err(e) => {
                    self.send_error(e.into()).await?;
                    return self.ready_for_query().await;
                }
            };

            // If we're returning data (SELECT), send back the output fields
            // before sending back actual data.
            if let ExecutionResult::Query { .. } = stream {
                let output_fields =
                    session_do!(self, session, get_portal, &UNNAMED, Portal::output_fields);
                if let Some(fields) = output_fields {
                    Self::send_row_descriptor(conn, fields).await?;
                }
            }

            Self::send_result(
                conn,
                stream,
                session_do!(self, session, get_portal, &UNNAMED, get_encoding_state),
            )
            .await?;
        }

        if num_statements == 0 {
            self.conn.send(BackendMessage::EmptyQueryResponse).await?;
        }

        self.ready_for_query().await
    }

    /// Parse the provided SQL statement and store it in the session.
    async fn parse(&mut self, name: String, sql: String, param_types: Vec<i32>) -> Result<()> {
        // TODO: Ensure in transaction.
        let vars = self.session.get_session_vars();
        let mut stmts = match parse_sql(vars, &sql) {
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
            .await
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

        // Read scalars for query parameters.
        let scalars = match stmt.input_paramaters() {
            Some(types) => match decode_param_scalars(param_formats, param_values, types) {
                Ok(scalars) => scalars,
                Err(e) => return self.send_error(e).await,
            },
            None => Vec::new(), // Would only happen with an empty query.
        };

        // Extend out the result formats.
        let result_formats = match extend_formats(
            result_formats,
            stmt.output_fields().map(|fields| fields.len()).unwrap_or(0),
        ) {
            Ok(formats) => formats,
            Err(e) => return self.send_error(e).await,
        };

        match self
            .session
            .bind_statement(portal, &statement, scalars, result_formats)
        {
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
                    match stmt.output_fields() {
                        Some(fields) => {
                            // When fields are extracted from a prepared
                            // statement, default format is applied, i.e., Text
                            // which is exactly what the protocol dictates.
                            //
                            // See: https://www.postgresql.org/docs/15/protocol-flow.html
                            // > Note that since Bind has not yet been issued,
                            // > the formats to be used for returned columns
                            // > are not yet known to the backend; the format
                            // > code fields in the RowDescription message will
                            // > be zeroes in this case.
                            Self::send_row_descriptor(conn, fields).await?
                        }
                        None => self.conn.send(BackendMessage::NoData).await?,
                    }

                    Ok(())
                }
                Err(e) => self.send_error(e.into()).await,
            },
            DescribeObjectType::Portal => match self.session.get_portal(&name) {
                Ok(portal) => {
                    // Send back row description.
                    match portal.output_fields() {
                        Some(fields) => Self::send_row_descriptor(conn, fields).await?,
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
        let session = &mut self.session;
        let stream = match session.execute_portal(&portal, max_rows).await {
            Ok(r) => r,
            Err(e) => return self.send_error(e.into()).await,
        };

        // TODO: This seems to be missing sending back row description. Is it
        // needed? If not, a comment needs to go here.

        Self::send_result(
            conn,
            stream,
            session_do!(self, session, get_portal, &portal, get_encoding_state),
        )
        .await
    }

    async fn close_object(&mut self, object_type: DescribeObjectType, name: String) -> Result<()> {
        match object_type {
            DescribeObjectType::Statement => self.session.remove_prepared_statement(&name),
            DescribeObjectType::Portal => self.session.remove_portal(&name),
        }
        self.conn.send(BackendMessage::CloseComplete).await
    }

    async fn sync(&mut self) -> Result<()> {
        self.ready_for_query().await
    }

    async fn flush(&mut self) -> Result<()> {
        self.conn.flush().await?;
        Ok(())
    }

    async fn send_result(
        conn: &mut FramedConn<C>,
        stream: ExecutionResult,
        encoding_state: Vec<(PgType, Format)>,
    ) -> Result<()> {
        match stream {
            ExecutionResult::Error(e) => return Err(e.into()),
            ExecutionResult::Query { stream, .. } => {
                if let Some(num_rows) = Self::stream_batch(conn, stream, encoding_state).await? {
                    Self::command_complete(conn, format!("SELECT {}", num_rows)).await?;
                }
            }
            ExecutionResult::EmptyQuery => conn.send(BackendMessage::EmptyQueryResponse).await?,
            ExecutionResult::Begin => Self::command_complete(conn, "BEGIN").await?,
            ExecutionResult::Commit => Self::command_complete(conn, "COMMIT").await?,
            ExecutionResult::Rollback => Self::command_complete(conn, "ROLLBACK").await?,
            ExecutionResult::InsertSuccess { rows_inserted } => {
                // Format is 'INSERT <oid> <num_inserted>'. Oid will always be
                // zero according to postgres docs.
                Self::command_complete(conn, format!("INSERT 0 {rows_inserted}")).await?
            }
            ExecutionResult::CopySuccess => Self::command_complete(conn, "COPY").await?,
            ExecutionResult::DeleteSuccess { deleted_rows } => {
                Self::command_complete(conn, format!("DELETE {}", deleted_rows)).await?
            }
            ExecutionResult::UpdateSuccess { updated_rows } => {
                Self::command_complete(conn, format!("UPDATE {}", updated_rows)).await?
            }
            ExecutionResult::CreateTable => Self::command_complete(conn, "CREATE TABLE").await?,
            ExecutionResult::CreateDatabase => {
                Self::command_complete(conn, "CREATE DATABASE").await?
            }
            ExecutionResult::CreateTunnel => Self::command_complete(conn, "CREATE TUNNEL").await?,
            ExecutionResult::CreateCredential => {
                Self::command_complete(conn, "CREATE CREDENTIAL").await?
            }
            ExecutionResult::CreateCredentials => {
                Self::command_complete(
                    conn,
                    "CREATE CREDENTIALS\nDEPRECATION WARNING.USE `CREATE CREDENTIAL`.",
                )
                .await?
            }
            ExecutionResult::CreateSchema => Self::command_complete(conn, "CREATE SCHEMA").await?,
            ExecutionResult::CreateView => Self::command_complete(conn, "CREATE VIEW").await?,
            ExecutionResult::AlterTable => Self::command_complete(conn, "ALTER TABLE").await?,
            ExecutionResult::AlterDatabase => {
                Self::command_complete(conn, "ALTER DATABASE").await?
            }
            ExecutionResult::AlterTunnelRotateKeys => {
                Self::command_complete(conn, "ALTER TUNNEL").await?
            }
            ExecutionResult::Set => Self::command_complete(conn, "SET").await?,
            ExecutionResult::DropTables => Self::command_complete(conn, "DROP TABLE").await?,
            ExecutionResult::DropViews => Self::command_complete(conn, "DROP VIEW").await?,
            ExecutionResult::DropSchemas => Self::command_complete(conn, "DROP SCHEMA").await?,
            ExecutionResult::DropDatabase => Self::command_complete(conn, "DROP DATABASE").await?,
            ExecutionResult::DropTunnel => Self::command_complete(conn, "DROP TUNNEL").await?,
            ExecutionResult::DropCredentials => {
                Self::command_complete(conn, "DROP CREDENTIALS").await?
            }
        };
        Ok(())
    }

    /// Convert an arrow schema into a row descriptor and send it to the client.
    async fn send_row_descriptor(conn: &mut FramedConn<C>, fields: OutputFields<'_>) -> Result<()> {
        let mut row_description = Vec::with_capacity(fields.len());
        for f in fields {
            let desc = FieldDescriptionBuilder::new(f.name)
                .with_type(f.pg_type)
                .with_format(*f.format)
                .build()?;
            row_description.push(desc);
        }
        conn.send(BackendMessage::RowDescription(row_description))
            .await?;
        Ok(())
    }

    /// Streams the batch to the client, returns an optional total number of
    /// rows sent. `None` rows sent means that an error response was sent.
    async fn stream_batch(
        conn: &mut FramedConn<C>,
        mut stream: SendableRecordBatchStream,
        encoding_state: Vec<(PgType, Format)>,
    ) -> Result<Option<usize>> {
        conn.set_encoding_state(encoding_state);
        let mut num_rows = 0;
        while let Some(result) = stream.next().await {
            let batch = match result {
                Ok(r) => r,
                Err(e) => {
                    conn.send(
                        ErrorResponse::error(
                            pgrepr::notice::SqlState::InternalError,
                            e.to_string(),
                        )
                        .into(),
                    )
                    .await?;
                    return Ok(None);
                }
            };
            num_rows += batch.num_rows();
            for row_idx in 0..batch.num_rows() {
                // Clone is cheapish here, all columns behind an arc.
                conn.send(BackendMessage::DataRow(batch.clone(), row_idx))
                    .await?;
            }
        }
        Ok(Some(num_rows))
    }

    async fn command_complete(conn: &mut FramedConn<C>, tag: impl Into<String>) -> Result<()> {
        conn.send(BackendMessage::CommandComplete { tag: tag.into() })
            .await
    }
}

/// Parse a sql string, returning an error response if failed to parse.
fn parse_sql(
    session_vars: SessionVars,
    sql: &str,
) -> Result<VecDeque<StatementWithExtensions>, ErrorResponse> {
    match session_vars.dialect() {
        Dialect::Prql => parser::parse_prql(sql),
        Dialect::Sql => parser::parse_sql(sql),
    }
    .map_err(|e| ErrorResponse::error(pgrepr::notice::SqlState::SyntaxError, e.to_string()))
}

/// Decodes inputs for a prepared query into the appropriate scalar values.
fn decode_param_scalars(
    param_formats: Vec<Format>,
    param_values: Vec<Option<Vec<u8>>>,
    types: &HashMap<String, Option<(PgType, DataType)>>,
) -> Result<Vec<ScalarValue>, ErrorResponse> {
    let param_formats = extend_formats(param_formats, param_values.len())?;

    if param_values.len() != types.len() {
        return Err(ErrorResponse::error_internal(format!(
            "Invalid number of values provided. Expected: {}, got: {}",
            types.len(),
            param_values.len(),
        )));
    }

    let mut scalars = Vec::with_capacity(param_values.len());
    for (idx, (val, format)) in param_values
        .into_iter()
        .zip(param_formats.into_iter())
        .enumerate()
    {
        // Parameter types keyed by '$n'.
        let str_id = format!("${}", idx + 1);

        let typ = types.get(&str_id).ok_or_else(|| {
            ErrorResponse::error_internal(format!(
                "Missing type for param value at index {}, input types: {:?}",
                idx, types
            ))
        })?;

        match typ {
            Some(typ) => {
                let scalar = {
                    match val.as_deref() {
                        None => ScalarValue::Null,
                        Some(v) => Scalar::decode_with_format(format, v, &typ.0)?
                            .into_datafusion(&typ.1)?,
                    }
                };
                scalars.push(scalar);
            }
            None => {
                return Err(ErrorResponse::error_internal(format!(
                    "Unknown type at index {}, input types: {:?}",
                    idx, types
                )))
            }
        }
    }

    Ok(scalars)
}

/// Returns a vector with all the formats extended to the default "text".
fn all_text_formats(num: usize) -> Vec<Format> {
    extend_formats(Vec::new(), num).unwrap()
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

/// Returns the encoding state, i.e., postgres type and format from the portal.
fn get_encoding_state(portal: &Portal) -> Vec<(PgType, Format)> {
    match portal.output_fields() {
        None => Vec::new(),
        Some(fields) => fields
            .map(|field| (field.pg_type.to_owned(), field.format.to_owned()))
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_params_success() {
        // Success test cases for decoding params.

        struct TestCase {
            values: Vec<Option<Vec<u8>>>,
            types: Vec<(&'static str, Option<(PgType, DataType)>)>,
            expected: Vec<ScalarValue>,
        }

        let test_cases = vec![
            // No params.
            TestCase {
                values: Vec::new(),
                types: Vec::new(),
                expected: Vec::new(),
            },
            // One param of type int64.
            TestCase {
                values: vec![Some(vec![49])],
                types: vec![("$1", Some((PgType::INT8, DataType::Int64)))],
                expected: vec![ScalarValue::Int64(Some(1))],
            },
            // Two params param of type string.
            TestCase {
                values: vec![Some(vec![49, 48]), Some(vec![50, 48])],
                types: vec![
                    ("$1", Some((PgType::TEXT, DataType::Utf8))),
                    ("$2", Some((PgType::TEXT, DataType::Utf8))),
                ],
                expected: vec![
                    ScalarValue::Utf8(Some("10".to_string())),
                    ScalarValue::Utf8(Some("20".to_string())),
                ],
            },
        ];

        for test_case in test_cases {
            let types: HashMap<_, _> = test_case
                .types
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect();

            let scalars = decode_param_scalars(Vec::new(), test_case.values, &types).unwrap();
            assert_eq!(test_case.expected, scalars);
        }
    }

    #[test]
    fn decode_params_fail() {
        // Failure test cases for decoding params (all cases should result in an
        // error).

        struct TestCase {
            values: Vec<Option<Vec<u8>>>,
            types: Vec<(&'static str, Option<(PgType, DataType)>)>,
        }

        let test_cases = vec![
            // Params provided, none expected.
            TestCase {
                values: vec![Some(vec![49])],
                types: Vec::new(),
            },
            // No params provided, one expected.
            TestCase {
                values: Vec::new(),
                types: vec![("$1", Some((PgType::INT8, DataType::Int64)))],
            },
        ];

        for test_case in test_cases {
            let types: HashMap<_, _> = test_case
                .types
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect();

            decode_param_scalars(Vec::new(), test_case.values, &types).unwrap_err();
        }
    }
}

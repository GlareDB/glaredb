use crate::codec::{FramedConn, PgCodec};
use crate::errors::{PgSrvError, Result};
use crate::messages::{
    BackendMessage, ErrorResponse, FieldDescription, FrontendMessage, NoticeResponse,
    StartupMessage, TransactionStatus,
};
use crate::types::PgValue;
use lemur::execute::stream::source::DataSource;
use lemur::repr::df::DataFrame;
use lemur::repr::expr::ExplainRelationExpr;
use sqlengine::engine::{Engine, ExecutionResult, Session};
use std::collections::HashMap;
use tokio::io::{AsyncRead, AsyncWrite};
use tracing::trace;

pub struct Handler<S> {
    engine: Engine<S>,
}

impl<S: DataSource> Handler<S> {
    pub fn new(engine: Engine<S>) -> Handler<S> {
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
            StartupMessage::Startup { version, params } => {
                self.begin(conn, params).await?;
            }
        }

        Ok(())
    }

    /// Runs the postgres protocol for a connection to completion.
    async fn begin<C>(&self, conn: C, params: HashMap<String, String>) -> Result<()>
    where
        C: AsyncRead + AsyncWrite + Unpin,
    {
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

        // TODO: When we open a session, get the transaction status from that.
        framed
            .send(BackendMessage::ReadyForQuery(TransactionStatus::Idle))
            .await?;

        let sess = match self.engine.begin_session() {
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

        let cs = ClientSession::new(sess, framed);
        cs.run().await
    }
}

struct ClientSession<C, S: DataSource> {
    conn: FramedConn<C>,
    session: Session<S>, // TODO: Make this a trait for stubbability?
}

impl<C, S> ClientSession<C, S>
where
    C: AsyncRead + AsyncWrite + Unpin,
    S: DataSource,
{
    fn new(session: Session<S>, conn: FramedConn<C>) -> Self {
        ClientSession { session, conn }
    }

    async fn run(mut self) -> Result<()> {
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
        // TODO: This needs some refactoring for starting an implicit
        // transaction for multiple statements. Also might give us a chance to
        // return a data frame stream over holding everything in memory.
        let results = match self.session.execute_query(&sql).await {
            Ok(results) => results,
            Err(e) => {
                self.conn
                    .send(
                        ErrorResponse::error_interanl(format!("failed to execute: {:?}", e)).into(),
                    )
                    .await?;
                return self.ready_for_query().await;
            }
        };
        let num_results = results.len();

        for result in results.into_iter() {
            match result {
                ExecutionResult::Query { df } => {
                    self.send_dataframe(df).await?;
                    self.command_complete("SELECT").await?
                }
                ExecutionResult::Begin => self.command_complete("BEGIN").await?,
                ExecutionResult::Commit => self.command_complete("COMMIT").await?,
                ExecutionResult::Rollback => self.command_complete("ROLLBACK").await?,
                ExecutionResult::WriteSuccess => self.command_complete("INSERT").await?,
                ExecutionResult::CreateTable => self.command_complete("CREATE_TABLE").await?,
                ExecutionResult::Explain(explain) => {
                    self.send_explain(explain).await?;
                    self.command_complete("EXPLAIN").await?;
                }
                other => {
                    self.conn
                        .send(
                            ErrorResponse::feature_not_supported(format!(
                                "execution result: {:?}",
                                other
                            ))
                            .into(),
                        )
                        .await?
                }
            }
        }

        if num_results == 0 {
            self.conn.send(BackendMessage::EmptyQueryResponse).await?;
        }

        self.ready_for_query().await
    }

    // TODO: Stream
    async fn send_dataframe(&mut self, df: DataFrame) -> Result<()> {
        let fields: Vec<_> = (0..df.arity()).map(|_| FieldDescription::new()).collect();
        self.conn
            .send(BackendMessage::RowDescription(fields))
            .await?;

        for row in df.iter_row_refs() {
            self.conn
                .send(BackendMessage::DataRow(
                    row.values
                        .into_iter()
                        .map(PgValue::from_value_ref)
                        .collect(),
                ))
                .await?;
        }

        Ok(())
    }

    async fn send_explain(&mut self, explain: ExplainRelationExpr) -> Result<()> {
        self.conn
            .send(BackendMessage::RowDescription(
                vec![FieldDescription::new()],
            ))
            .await?;
        self.conn
            .send(BackendMessage::DataRow(vec![PgValue::Text(
                explain.to_string(),
            )]))
            .await?;
        Ok(())
    }

    async fn command_complete(&mut self, tag: impl Into<String>) -> Result<()> {
        self.conn
            .send(BackendMessage::CommandComplete { tag: tag.into() })
            .await
    }
}

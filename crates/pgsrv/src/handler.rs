use crate::codec::{FramedConn, PgCodec};
use crate::errors::{PgSrvError, Result};
use crate::messages::{
    BackendMessage, ErrorResponse, FrontendMessage, NoticeResponse, StartupMessage,
    TransactionStatus,
};
use lemur::execute::stream::source::DataSource;
use sqlengine::engine::{Engine, Session};
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
        self.ready_for_query().await
    }
}

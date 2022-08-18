use crate::codec::{FramedConn, PgCodec};
use crate::errors::{PgSrvError, Result};
use crate::messages::{BackendMessage, FrontendMessage, StartupMessage};
use std::collections::HashMap;
use tokio::io::{AsyncRead, AsyncWrite};
use tracing::trace;

pub struct Handler {}

impl Handler {
    pub fn new() -> Handler {
        Handler {}
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
                Self::run(conn, params).await?;
            }
        }

        Ok(())
    }

    /// Runs the postgres protocol for a connection to completion.
    async fn run<C>(conn: C, params: HashMap<String, String>) -> Result<()>
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

        framed.send(BackendMessage::ReadyForQuery).await?;

        loop {
            let msg = framed.read().await?;

            match msg {
                Some(FrontendMessage::Query { sql }) => trace!(%sql, "received query"),
                Some(other) => unimplemented!("frontend msg: {:?}", other),
                None => {
                    trace!("connection closed");
                    return Ok(());
                }
            }
        }
    }
}

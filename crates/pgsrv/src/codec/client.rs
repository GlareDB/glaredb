use crate::errors::{PgSrvError, Result};
use bytes::{Buf, BufMut, BytesMut};
use bytesutil::{BufStringMut, Cursor};
use datafusion::scalar::ScalarValue;
use futures::{SinkExt, TryStreamExt};
use ioutil::write::InfallibleWrite;
use pgrepr::messages::{
    BackendMessage, FrontendMessage, StartupMessage, TransactionStatus, VERSION_CANCEL,
    VERSION_SSL, VERSION_V3,
};
use std::collections::HashMap;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite};
use tokio_util::codec::{Decoder, Encoder, Framed};
use tracing::trace;

pub struct FramedClientConn<C> {
    conn: Framed<C, PgClientCodec>,
}

impl<C> FramedClientConn<C>
where
    C: AsyncRead + AsyncWrite + Unpin,
{
    pub fn new(conn: C) -> Self {
        Self {
            conn: Framed::new(conn, PgClientCodec),
        }
    }

    /// Reads a single, complete backend message.
    ///
    /// Returns `None` once the underlying connection terminates.
    pub async fn read(&mut self) -> Result<Option<BackendMessage>> {
        let msg = self.conn.try_next().await?;
        match &msg {
            Some(msg) => trace!(?msg, "read message"),
            None => trace!("read message (None)"),
        };
        Ok(msg)
    }

    /// Sends a single startup message to the underlying connection.
    pub async fn send_startup(&mut self, msg: StartupMessage) -> Result<()> {
        trace!(?msg, "sending message");
        self.conn.send(msg).await
    }

    /// Sends a single frontend message to the underlying connection.
    pub async fn send(&mut self, msg: FrontendMessage) -> Result<()> {
        trace!(?msg, "sending message");
        self.conn.send(msg).await
    }
}


/// A client codec for the postgres protocol.
/// This is the reverse of the `PgCodec` which is used by the server.
pub struct PgClientCodec;

impl Encoder<StartupMessage> for PgClientCodec {
    type Error = PgSrvError;

    fn encode(&mut self, item: StartupMessage, dst: &mut BytesMut) -> Result<(), Self::Error> {
        match item {
            StartupMessage::SSLRequest {
                version,
            } => {
                todo!("encode<StartupMessage::SSLRequest>")
            }
            StartupMessage::StartupRequest {
                version,
                params,
            } => {
                todo!("encode<StartupMessage::StartupRequest>")
            }
            StartupMessage::CancelRequest {
                version,
            } => {
                todo!("encode<StartupMessage::CancelRequest>")
            }
        }
    }
}

impl Encoder<FrontendMessage> for PgClientCodec {
    type Error = PgSrvError;

    fn encode(&mut self, item: FrontendMessage, dst: &mut BytesMut) -> Result<(), Self::Error> {
        todo!("encode<FrontendMessage>")
    }
}

impl Decoder for PgClientCodec {
    type Item = BackendMessage;
    type Error = PgSrvError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        todo!("decode")
    }
}

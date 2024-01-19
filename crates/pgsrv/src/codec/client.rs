use bytes::{Buf, BufMut, BytesMut};
use bytesutil::{BufStringMut, Cursor};
use futures::{SinkExt, TryStreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::{Decoder, Encoder, Framed};
use tracing::trace;

use crate::errors::{PgSrvError, Result};
use crate::messages::{BackendMessage, FrontendMessage, StartupMessage};
use crate::ssl::Connection;

pub struct FramedClientConn<C> {
    conn: Framed<Connection<C>, PgClientCodec>,
}

impl<C> FramedClientConn<C>
where
    C: AsyncRead + AsyncWrite + Unpin,
{
    pub fn new(conn: Connection<C>) -> Self {
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
    #[allow(dead_code)]
    pub async fn send(&mut self, msg: FrontendMessage) -> Result<()> {
        trace!(?msg, "sending message");
        self.conn.send(msg).await
    }

    /// Consumes the `FramedClientConn`, returning the underlying `Framed`
    pub fn into_inner(self) -> Framed<Connection<C>, PgClientCodec> {
        self.conn
    }
}

/// A client codec for the postgres protocol.
/// This is the reverse of the `PgCodec` which is used by the server.
pub struct PgClientCodec;

impl PgClientCodec {
    fn decode_authentication(buf: &mut Cursor<'_>) -> Result<BackendMessage> {
        let auth_type = buf.get_i32();

        match auth_type {
            0 => Ok(BackendMessage::AuthenticationOk),
            3 => Ok(BackendMessage::AuthenticationCleartextPassword),
            _ => unimplemented!("auth type {}", auth_type),
        }
    }
}

impl Encoder<StartupMessage> for PgClientCodec {
    type Error = PgSrvError;

    fn encode(&mut self, item: StartupMessage, dst: &mut BytesMut) -> Result<(), Self::Error> {
        match item {
            StartupMessage::SSLRequest { version: _ } => {
                todo!("encode<StartupMessage::SSLRequest>")
            }
            StartupMessage::StartupRequest { version, params } => {
                // determine the length of the message
                let mut len = 4; // message length indicator
                len += 4; // protocol version
                len += params
                    .iter()
                    .map(|(k, v)| k.len() + v.len() + 2)
                    .sum::<usize>();
                len += 1; // trailing zero

                // write the message
                dst.reserve(len);
                dst.put_i32(len as i32);
                dst.put_i32(version);
                for (k, v) in params {
                    // put the key and values as null-terminated strings
                    dst.put_slice(k.as_bytes());
                    dst.put_u8(0);
                    dst.put_slice(v.as_bytes());
                    dst.put_u8(0);
                }

                // null-terminate the message
                dst.put_u8(0);

                Ok(())
            }
            StartupMessage::CancelRequest { version: _ } => {
                todo!("encode<StartupMessage::CancelRequest>")
            }
        }
    }
}

impl Encoder<FrontendMessage> for PgClientCodec {
    type Error = PgSrvError;

    fn encode(&mut self, item: FrontendMessage, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let byte = match &item {
            FrontendMessage::PasswordMessage { .. } => b'p',
            other => unimplemented!("encode<FrontendMessage>::{:?}", other),
        };
        dst.put_u8(byte);

        // Length placeholder.
        let len_idx = dst.len();
        dst.put_u32(0);

        match item {
            FrontendMessage::PasswordMessage { password } => {
                dst.put_cstring(&password);
            }
            other => unimplemented!("encode<FrontendMessage>::{:?}", other),
        }

        let msg_len = dst.len() - len_idx;
        let msg_len = i32::try_from(msg_len).map_err(|_| PgSrvError::MsgTooLarge(msg_len))?;
        dst[len_idx..len_idx + 4].copy_from_slice(&i32::to_be_bytes(msg_len));

        Ok(())
    }
}

impl Decoder for PgClientCodec {
    type Error = PgSrvError;
    type Item = BackendMessage;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        // Every message has a type byte, and an i32 for msg length. Return
        // early if we don't even have those available.
        if src.len() < 5 {
            return Ok(None);
        }

        // Index directly into the buffer to avoid advancing the cursor forward.
        let msg_type = src[0];
        let msg_len = i32::from_be_bytes(src[1..5].try_into().unwrap()) as usize;

        // Not enough bytes to read the full message yet.
        if src.len() < msg_len + 1 {
            src.reserve(msg_len + 1 - src.len());
            return Ok(None);
        }

        let buf = src.split_to(msg_len + 1);
        let mut buf = Cursor::new(&buf);
        buf.advance(5);

        let msg = match msg_type {
            b'R' => Self::decode_authentication(&mut buf)?,
            other => return Err(PgSrvError::InvalidMsgType(other)),
        };

        Ok(Some(msg))
    }
}

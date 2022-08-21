use crate::errors::{PgSrvError, Result};
use crate::messages::{
    BackendMessage, FrontendMessage, StartupMessage, TransactionStatus, VERSION,
};
use crate::types::PgValue;
use bytes::{Buf, BufMut, BytesMut};
use futures::{Sink, SinkExt, Stream, StreamExt, TryStreamExt};
use ioutil::fmt::HexBuf;
use ioutil::write::InfallibleWrite;
use std::collections::HashMap;
use std::str;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite};
use tokio_util::codec::{Decoder, Encoder, Framed};
use tracing::trace;

/// A connection that can encode and decode postgres protocol messages.
pub struct FramedConn<C> {
    conn: Framed<C, PgCodec>, // TODO: Buffer.
}

impl<C> FramedConn<C>
where
    C: AsyncRead + AsyncWrite + Unpin,
{
    /// Create a new framed connection.
    pub fn new(conn: C) -> Self {
        FramedConn {
            conn: Framed::new(conn, PgCodec),
        }
    }

    /// Reads a single, complete frontend message.
    ///
    /// Returns `None` once the underlying connection terminates.
    pub async fn read(&mut self) -> Result<Option<FrontendMessage>> {
        let msg = self.conn.try_next().await?;
        match &msg {
            Some(msg) => trace!(?msg, "read message"),
            None => trace!("read message (None)"),
        };
        Ok(msg)
    }

    /// Sends a single backend message to the underlying connection.
    pub async fn send(&mut self, msg: BackendMessage) -> Result<()> {
        trace!(?msg, "sending message");
        self.conn.send(msg).await
    }
}

trait BufStringMut: BufMut {
    /// Put a null-terminated string in the buffer.
    fn put_cstring(&mut self, s: &str);
}

impl<B: BufMut> BufStringMut for B {
    fn put_cstring(&mut self, s: &str) {
        self.put(s.as_bytes());
        self.put_u8(0);
    }
}

#[derive(Debug)]
struct Cursor<'a> {
    buf: &'a [u8],
}

impl<'a> Cursor<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Cursor { buf }
    }

    fn read_cstring(&mut self) -> Result<&'a str> {
        match self.buf.iter().position(|b| *b == 0) {
            Some(pos) => {
                let s = str::from_utf8(&self.buf[0..pos]).unwrap();
                self.advance(pos + 1);
                Ok(s)
            }
            None => Err(PgSrvError::MissingNullByte),
        }
    }

    fn next_is_null_byte(&self) -> bool {
        self.buf.len() > 0 && self.buf[0] == 0
    }
}

impl<'a> Buf for Cursor<'a> {
    fn remaining(&self) -> usize {
        self.buf.len()
    }

    fn chunk(&self) -> &[u8] {
        self.buf
    }

    fn advance(&mut self, cnt: usize) {
        self.buf = &self.buf[cnt..]
    }
}

pub struct PgCodec;

impl PgCodec {
    /// Decode a startup message from some underlying connection.
    ///
    /// Note that this falls outside the typical flow for decoding frontend
    /// messages.
    pub async fn decode_startup_from_conn<C>(conn: &mut C) -> Result<StartupMessage>
    where
        C: AsyncRead + Unpin,
    {
        let msg_len = conn.read_i32().await? as usize;
        let mut buf = BytesMut::new();
        buf.resize(msg_len - 4, 0);
        conn.read_exact(&mut buf).await?;

        let mut buf = Cursor::new(&buf);
        let version = buf.get_i32();
        if version != VERSION {
            return Err(PgSrvError::InvalidProtocolVersion(version));
        }

        let mut params = HashMap::new();
        while buf.remaining() > 0 && !buf.next_is_null_byte() {
            let key = buf.read_cstring()?.to_string();
            let val = buf.read_cstring()?.to_string();
            params.insert(key, val);
        }

        Ok(StartupMessage::Startup { version, params })
    }

    fn decode_query(buf: &mut Cursor<'_>) -> Result<FrontendMessage> {
        Ok(FrontendMessage::Query {
            sql: buf.read_cstring()?.to_string(),
        })
    }

    fn decode_password(buf: &mut Cursor<'_>) -> Result<FrontendMessage> {
        Ok(FrontendMessage::PasswordMessage {
            password: buf.read_cstring()?.to_string(),
        })
    }

    fn encode_value_as_text(val: PgValue, buf: &mut BytesMut) -> Result<()> {
        if matches!(val, PgValue::Null) {
            buf.put_i32(-1);
            return Ok(());
        }

        // Write placeholder length.
        let len_idx = buf.len();
        buf.put_i32(0);

        match val {
            PgValue::Null => unreachable!(), // Checked above.
            PgValue::Bool(v) => write!(buf, "{}", if v { "t" } else { "f" }),
            PgValue::Int2(v) => write!(buf, "{}", v),
            PgValue::Int4(v) => write!(buf, "{}", v),
            PgValue::Text(v) => buf.write_str(&v),
            PgValue::Bytea(v) => write!(buf, "{:#x}", HexBuf(&v)),
        };

        // Note the value of length does not include itself.
        let val_len = buf.len() - len_idx - 4;
        let val_len = i32::try_from(val_len).map_err(|_| PgSrvError::MsgTooLarge(val_len))?;
        buf[len_idx..len_idx + 4].copy_from_slice(&i32::to_be_bytes(val_len));

        Ok(())
    }
}

impl Encoder<BackendMessage> for PgCodec {
    type Error = PgSrvError;

    fn encode(&mut self, item: BackendMessage, dst: &mut BytesMut) -> Result<()> {
        let byte = match &item {
            BackendMessage::AuthenticationOk => b'R',
            BackendMessage::AuthenticationCleartextPassword => b'R',
            BackendMessage::EmptyQueryResponse => b'I',
            BackendMessage::ReadyForQuery(_) => b'Z',
            BackendMessage::CommandComplete { .. } => b'C',
            BackendMessage::RowDescription(_) => b'T',
            BackendMessage::ErrorResponse(_) => b'E',
            BackendMessage::NoticeResponse(_) => b'N',
            _ => unimplemented!(),
        };
        dst.put_u8(byte);

        // Length placeholder.
        let len_idx = dst.len();
        dst.put_u32(0);

        match item {
            BackendMessage::AuthenticationOk => dst.put_i32(0),
            BackendMessage::AuthenticationCleartextPassword => dst.put_i32(3),
            BackendMessage::EmptyQueryResponse => (),
            BackendMessage::ReadyForQuery(status) => match status {
                TransactionStatus::Idle => dst.put_u8(b'I'),
                TransactionStatus::InBlock => dst.put_u8(b'T'),
                TransactionStatus::Failed => dst.put_u8(b'E'),
            },
            BackendMessage::CommandComplete { tag } => dst.put_cstring(&tag),
            BackendMessage::RowDescription(descs) => {
                dst.put_i16(descs.len() as i16); // TODO: Check
                for desc in descs.into_iter() {
                    dst.put_cstring(&desc.name);
                    dst.put_i32(desc.table_id);
                    dst.put_i16(desc.col_id);
                    dst.put_i32(desc.obj_id);
                    dst.put_i16(desc.type_size);
                    dst.put_i32(desc.type_mod);
                    dst.put_i16(desc.format);
                }
            }
            BackendMessage::ErrorResponse(error) => {
                // See https://www.postgresql.org/docs/current/protocol-error-fields.html

                // Severity
                dst.put_u8(b'S');
                dst.put_cstring(error.severity.as_str());
                dst.put_u8(b'V');
                dst.put_cstring(error.severity.as_str());

                // SQLSTATE error code
                dst.put_u8(b'C');
                dst.put_cstring(error.code.as_code_str());

                // Message
                dst.put_u8(b'M');
                dst.put_cstring(&error.message);

                // Terminate message.
                dst.put_u8(0);
            }
            BackendMessage::NoticeResponse(notice) => {
                // Pretty much the same as an error response.
                dst.put_u8(b'S');
                dst.put_cstring(notice.severity.as_str());
                dst.put_u8(b'V');
                dst.put_cstring(notice.severity.as_str());
                dst.put_u8(b'C');
                dst.put_cstring(notice.code.as_code_str());
                dst.put_u8(b'M');
                dst.put_cstring(&notice.message);
                dst.put_u8(0);
            }
            // BackendMessage::RowDescription
            _ => unimplemented!(),
        }

        let msg_len = dst.len() - len_idx;
        let msg_len = i32::try_from(msg_len).map_err(|_| PgSrvError::MsgTooLarge(msg_len))?;
        dst[len_idx..len_idx + 4].copy_from_slice(&i32::to_be_bytes(msg_len));

        Ok(())
    }
}

impl Decoder for PgCodec {
    type Item = FrontendMessage;
    type Error = PgSrvError;

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
        if src.len() < msg_len - 1 {
            src.reserve(msg_len - src.len());
            return Ok(None);
        }

        let buf = src.split_to(msg_len + 1);
        let mut buf = Cursor::new(&buf);
        buf.advance(5);

        let msg = match msg_type {
            b'Q' => Self::decode_query(&mut buf)?,
            b'p' => Self::decode_password(&mut buf)?,
            other => return Err(PgSrvError::InvalidMsgType(other)),
        };

        Ok(Some(msg))
    }
}

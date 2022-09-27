use crate::errors::{PgSrvError, Result};
use crate::messages::{
    BackendMessage, FrontendMessage, StartupMessage, TransactionStatus, VERSION_CANCEL,
    VERSION_SSL, VERSION_V3,
};
use bytes::{Buf, BufMut, BytesMut};
use datafusion::scalar::ScalarValue;
use futures::{SinkExt, TryStreamExt};
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
        !self.buf.is_empty() && self.buf[0] == 0
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

        match version {
            VERSION_V3 => (), // Continue with normal startup flow.
            VERSION_SSL => return Ok(StartupMessage::SSLRequest { version }),
            VERSION_CANCEL => return Ok(StartupMessage::CancelRequest { version }),
            other => return Err(PgSrvError::InvalidProtocolVersion(other)),
        }

        let mut params = HashMap::new();
        while buf.remaining() > 0 && !buf.next_is_null_byte() {
            let key = buf.read_cstring()?.to_string();
            let val = buf.read_cstring()?.to_string();
            params.insert(key, val);
        }

        Ok(StartupMessage::StartupRequest { version, params })
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

    fn decode_parse(buf: &mut Cursor<'_>) -> Result<FrontendMessage> {
        let name = buf.read_cstring()?.to_string();
        let sql = buf.read_cstring()?.to_string();
        let num_params = buf.get_i16() as usize;
        let mut param_types = Vec::with_capacity(num_params);
        for _ in 0..num_params {
            param_types.push(buf.get_i32());
        }
        Ok(FrontendMessage::Parse {
            name,
            sql,
            param_types,
        })
    }

    fn decode_bind(buf: &mut Cursor<'_>) -> Result<FrontendMessage> {
        let portal = buf.read_cstring()?.to_string();
        let statement = buf.read_cstring()?.to_string();

        let num_params = buf.get_i16() as usize;
        let mut param_formats = Vec::with_capacity(num_params);
        for _ in 0..num_params {
            param_formats.push(buf.get_i16());
        }

        let num_values = buf.get_i16() as usize; // must match num_params
        let mut param_values = Vec::with_capacity(num_values);
        for _ in 0..num_values {
            let len = buf.get_i32();
            if len == -1 {
                param_values.push(None);
            } else {
                let mut val = vec![0; len as usize];
                buf.copy_to_slice(&mut val);
                param_values.push(Some(val));
            }
        }

        let num_params = buf.get_i16() as usize;
        let mut result_formats = Vec::with_capacity(num_params);
        for _ in 0..num_params {
            result_formats.push(buf.get_i16());
        }

        Ok(FrontendMessage::Bind {
            portal,
            statement,
            param_formats,
            param_values,
            result_formats,
        })
    }

    fn decode_describe(buf: &mut Cursor<'_>) -> Result<FrontendMessage> {
        let object_type = buf.get_u8().try_into()?;
        let name = buf.read_cstring()?.to_string();

        Ok(FrontendMessage::Describe { object_type, name })
    }

    fn decode_execute(buf: &mut Cursor<'_>) -> Result<FrontendMessage> {
        let portal = buf.read_cstring()?.to_string();
        let max_rows = buf.get_i32();
        Ok(FrontendMessage::Execute { portal, max_rows })
    }

    fn decode_sync(_buf: &mut Cursor<'_>) -> Result<FrontendMessage> {
        Ok(FrontendMessage::Sync)
    }

    fn decode_terminate(_buf: &mut Cursor<'_>) -> Result<FrontendMessage> {
        Ok(FrontendMessage::Terminate)
    }

    fn encode_scalar_as_text(scalar: ScalarValue, buf: &mut BytesMut) -> Result<()> {
        if scalar.is_null() {
            buf.put_i32(-1);
            return Ok(());
        }

        // Write placeholder length.
        let len_idx = buf.len();
        buf.put_i32(0);

        match scalar {
            ScalarValue::Boolean(Some(v)) => write!(buf, "{}", if v { "t" } else { "f" }),
            scalar => write!(buf, "{}", scalar), // Note this won't write null, that's checked above.
        }

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
            BackendMessage::ParameterStatus { .. } => b'S',
            BackendMessage::ReadyForQuery(_) => b'Z',
            BackendMessage::CommandComplete { .. } => b'C',
            BackendMessage::RowDescription(_) => b'T',
            BackendMessage::DataRow(_, _) => b'D',
            BackendMessage::ErrorResponse(_) => b'E',
            BackendMessage::NoticeResponse(_) => b'N',
            BackendMessage::ParseComplete => b'1',
            BackendMessage::BindComplete => b'2',
            BackendMessage::NoData => b'n',
        };
        dst.put_u8(byte);

        // Length placeholder.
        let len_idx = dst.len();
        dst.put_u32(0);

        match item {
            BackendMessage::AuthenticationOk => dst.put_i32(0),
            BackendMessage::AuthenticationCleartextPassword => dst.put_i32(3),
            BackendMessage::EmptyQueryResponse => (),
            BackendMessage::ParseComplete => (),
            BackendMessage::BindComplete => (),
            BackendMessage::NoData => (),
            BackendMessage::ParameterStatus { key, val } => {
                dst.put_cstring(&key);
                dst.put_cstring(&val);
            }
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
            BackendMessage::DataRow(batch, row_idx) => {
                dst.put_i16(batch.num_columns() as i16); // TODO: Check.
                for col in batch.columns().iter() {
                    let scalar = ScalarValue::try_from_array(col, row_idx)?;
                    Self::encode_scalar_as_text(scalar, dst)?;
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
        if src.len() < msg_len + 1 {
            src.reserve(msg_len + 1 - src.len());
            return Ok(None);
        }

        let buf = src.split_to(msg_len + 1);
        let mut buf = Cursor::new(&buf);
        buf.advance(5);

        let msg = match msg_type {
            b'Q' => Self::decode_query(&mut buf)?,
            b'p' => Self::decode_password(&mut buf)?,
            b'P' => Self::decode_parse(&mut buf)?,
            b'B' => Self::decode_bind(&mut buf)?,
            b'D' => Self::decode_describe(&mut buf)?,
            b'E' => Self::decode_execute(&mut buf)?,
            b'S' => Self::decode_sync(&mut buf)?,
            // X - Terminate
            b'X' => return Ok(None),
            other => return Err(PgSrvError::InvalidMsgType(other)),
        };

        Ok(Some(msg))
    }
}

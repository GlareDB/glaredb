use crate::errors::{PgSrvError, Result};
use crate::messages::{
    BackendMessage, FrontendMessage, StartupMessage, TransactionStatus, VERSION_CANCEL,
    VERSION_SSL, VERSION_V3,
};
use crate::ssl::Connection;
use bytes::{Buf, BufMut, BytesMut};
use bytesutil::{BufStringMut, Cursor};
use futures::{sink::Buffer, SinkExt, TryStreamExt};
use pgrepr::format::Format;
use pgrepr::scalar::Scalar;
use std::collections::HashMap;
use std::mem::size_of_val;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite};
use tokio_postgres::types::Type as PgType;
use tokio_util::codec::{Decoder, Encoder, Framed};
use tracing::{debug, trace};

/// A connection that can encode and decode postgres protocol messages.
pub struct FramedConn<C> {
    conn: Buffer<Framed<Connection<C>, PgCodec>, BackendMessage>,
}

impl<C> FramedConn<C>
where
    C: AsyncRead + AsyncWrite + Unpin,
{
    /// Create a new framed connection.
    pub fn new(conn: Connection<C>) -> Self {
        FramedConn {
            conn: Framed::new(conn, PgCodec::new()).buffer(16),
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

    /// Flush the connection.
    pub async fn flush(&mut self) -> Result<()> {
        self.conn.flush().await?;
        Ok(())
    }

    /// Consumes the `FramedConn`, returning the underlying `Framed`
    ///
    /// The connection should be flushed before calling this.
    // TODO: This should probably just return the connection itself.
    pub fn into_inner(self) -> Framed<Connection<C>, PgCodec> {
        self.conn.into_inner()
    }

    /// Sets the encoding state for current connection.
    pub fn set_encoding_state(&mut self, s: Vec<(PgType, Format)>) {
        self.conn.get_mut().codec_mut().encoding_state = s;
    }
}

pub struct PgCodec {
    encoding_state: Vec<(PgType, Format)>,
}

impl PgCodec {
    fn new() -> Self {
        Self {
            encoding_state: Vec::new(),
        }
    }

    /// Decode a startup message from some underlying connection.
    ///
    /// Note that this falls outside the typical flow for decoding frontend
    /// messages.
    pub async fn decode_startup_from_conn<C>(conn: &mut C) -> Result<StartupMessage>
    where
        C: AsyncRead + Unpin,
    {
        let msg_len = conn.read_i32().await?;
        let version = conn.read_i32().await?;
        debug!(msg_len, version, "startup connection version");

        match version {
            VERSION_V3 => (), // Continue with normal startup flow.
            VERSION_SSL => return Ok(StartupMessage::SSLRequest { version }),
            VERSION_CANCEL => return Ok(StartupMessage::CancelRequest { version }),
            other => return Err(PgSrvError::InvalidProtocolVersion(other)),
        }

        let min_buf_len = size_of_val(&msg_len) + size_of_val(&version);
        // In the case the message `version` matches VERSION_V3 however is not a startup message
        // and the `msg_len` could be either an invalid conversion or less than `min_buf_len`
        let msg_len: usize = match msg_len.try_into() {
            Ok(len) if len < min_buf_len => Err(PgSrvError::InvalidMsgLength(msg_len)),
            Ok(len) => Ok(len),
            Err(_) => Err(PgSrvError::InvalidMsgLength(msg_len)),
        }?;
        let remaning_msg_len = msg_len - min_buf_len;
        let mut buf = BytesMut::zeroed(remaning_msg_len);
        conn.read_exact(&mut buf).await?;
        debug!(?buf, "startup connection message");

        // Generate map of parameters
        let mut buf = Cursor::new(&buf);
        let mut params = HashMap::new();
        while buf.remaining() > 0 && !buf.peek_next_is_null() {
            let key = buf.read_cstring()?.to_string();
            let val = buf.read_cstring()?.to_string();
            params.insert(key, val);
        }
        debug!(?params, "startup connection params");

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

        // Input param formats.
        //
        // Valid lengths may be:
        // 0 -> Use default format for all inputs (text)
        // 1 -> Use this one format for all inputs
        // n -> Individually specified formats for each input.
        let num_params = buf.get_i16() as usize;
        let mut param_formats = Vec::with_capacity(num_params);
        for _ in 0..num_params {
            let format: Format = buf.get_i16().try_into()?;
            param_formats.push(format);
        }

        // Input param values.
        let num_values = buf.get_i16() as usize;
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
            let format: Format = buf.get_i16().try_into()?;
            result_formats.push(format);
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

    fn decode_close(buf: &mut Cursor<'_>) -> Result<FrontendMessage> {
        let object_type = buf.get_u8().try_into()?;
        let name = buf.read_cstring()?.to_string();

        Ok(FrontendMessage::Close { object_type, name })
    }

    fn decode_sync(_buf: &mut Cursor<'_>) -> Result<FrontendMessage> {
        Ok(FrontendMessage::Sync)
    }

    fn decode_flush(_buf: &mut Cursor<'_>) -> Result<FrontendMessage> {
        Ok(FrontendMessage::Flush)
    }

    fn decode_terminate(_buf: &mut Cursor<'_>) -> Result<FrontendMessage> {
        Ok(FrontendMessage::Terminate)
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
            BackendMessage::DataRow(..) => b'D',
            BackendMessage::ErrorResponse(_) => b'E',
            BackendMessage::NoticeResponse(_) => b'N',
            BackendMessage::ParseComplete => b'1',
            BackendMessage::BindComplete => b'2',
            BackendMessage::CloseComplete => b'3',
            BackendMessage::NoData => b'n',
            BackendMessage::ParameterDescription(_) => b't',
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
            BackendMessage::CloseComplete => (),
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
                    dst.put_i32(desc.type_oid);
                    dst.put_i16(desc.type_size);
                    dst.put_i32(desc.type_mod);
                    dst.put_i16(desc.format);
                }
            }
            BackendMessage::DataRow(batch, row_idx) => {
                dst.put_i16(batch.num_columns() as i16); // TODO: Check.
                for (col, (pg_type, format)) in
                    batch.columns().iter().zip(self.encoding_state.iter())
                {
                    let scalar = Scalar::try_from_array(col, row_idx, pg_type)?;

                    if scalar.is_null() {
                        dst.put_i32(-1);
                    } else {
                        // Write a placeholder length.
                        let len_idx = dst.len();
                        dst.put_i32(0);

                        scalar.encode_with_format(*format, dst)?;

                        // Note the value of length does not include itself.
                        let val_len = dst.len() - len_idx - 4;
                        let val_len = i32::try_from(val_len)
                            .map_err(|_| PgSrvError::MessageTooLarge(val_len))?;
                        dst[len_idx..len_idx + 4].copy_from_slice(&i32::to_be_bytes(val_len));
                    }
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
            BackendMessage::ParameterDescription(descs) => {
                dst.put_i16(descs.len() as i16);
                for desc in descs.into_iter() {
                    dst.put_i32(desc);
                }
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
            b'C' => Self::decode_close(&mut buf)?,
            b'S' => Self::decode_sync(&mut buf)?,
            b'H' => Self::decode_flush(&mut buf)?,
            b'X' => Self::decode_terminate(&mut buf)?,
            other => return Err(PgSrvError::InvalidMsgType(other)),
        };

        Ok(Some(msg))
    }
}

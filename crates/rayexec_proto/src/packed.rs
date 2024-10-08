//! Simple packed encoding of protobuf messages.
//!
//! Length-prefixed encoding into a byte buffer for arbitrary messages. Message
//! schemas are runtime only concepts are defined by the order and types of
//! messages encoded into the buffer.

use prost::Message;
use rayexec_error::{RayexecError, Result, ResultExt};

#[derive(Debug)]
pub struct PackedEncoder<'a> {
    buf: &'a mut Vec<u8>,
}

impl<'a> PackedEncoder<'a> {
    /// Create a new packed encoder that will write the provided buffer.
    ///
    /// Encoded messages will start at the end of the buffer to enable multiple
    /// instantiations of the encoder without clobbering previously written
    /// data.
    pub fn new(buf: &'a mut Vec<u8>) -> Self {
        PackedEncoder { buf }
    }

    /// Encode a message into the buffer.
    pub fn encode_next<M: Message>(&mut self, msg: &M) -> Result<()> {
        let msg_len = msg.encoded_len();
        let mut buf_start = self.buf.len();
        self.buf.resize(self.buf.len() + 8 + msg_len, 0);

        self.buf[buf_start..buf_start + 8].copy_from_slice(&(msg_len as u64).to_le_bytes());

        buf_start += 8;

        let mut buf = &mut self.buf[buf_start..buf_start + msg_len];
        msg.encode(&mut buf).context("failed to encode message")?;

        Ok(())
    }
}

#[derive(Debug)]
pub struct PackedDecoder<'a> {
    buf: &'a [u8],
    n: usize,
}

impl<'a> PackedDecoder<'a> {
    /// Create a new packed decoder that will read from the provided buffer.
    pub fn new(buf: &'a [u8]) -> Self {
        PackedDecoder { buf, n: 0 }
    }

    /// Decode the next message from the buffer.
    pub fn decode_next<M: Message + Default>(&mut self) -> Result<M> {
        let msg_len_buf: [u8; 8] = self
            .buf
            .get(self.n..self.n + 8)
            .ok_or_else(|| RayexecError::new("buffer too small to contain message"))?
            .try_into()
            .unwrap();

        let msg_len = u64::from_le_bytes(msg_len_buf) as usize;

        self.n += 8;

        let buf = &self.buf[self.n..self.n + msg_len];
        let msg = M::decode(buf).context("failed to decode message")?;

        self.n += msg_len;

        Ok(msg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generated::schema::{data_type, DataType, DecimalTypeMeta, EmptyMeta, Field};

    #[test]
    fn single_message() {
        let field = Field {
            name: "hello".to_string(),
            datatype: Some(DataType {
                value: Some(data_type::Value::TypeInt16(EmptyMeta {})),
            }),
            nullable: true,
        };

        let mut buf = Vec::new();
        {
            let mut encoder = PackedEncoder::new(&mut buf);
            encoder.encode_next(&field).unwrap();
        }

        let mut decoder = PackedDecoder::new(&buf);
        let msg: Field = decoder.decode_next().unwrap();

        assert_eq!(field, msg);
        assert_eq!("hello", msg.name);
    }

    #[test]
    fn multiple_messages() {
        let field = Field {
            name: "hello".to_string(),
            datatype: Some(DataType {
                value: Some(data_type::Value::TypeInt16(EmptyMeta {})),
            }),
            nullable: true,
        };
        let decimal_meta = DecimalTypeMeta {
            precision: 19,
            scale: 8,
        };

        let mut buf = Vec::new();
        {
            let mut encoder = PackedEncoder::new(&mut buf);
            encoder.encode_next(&field).unwrap();
            encoder.encode_next(&decimal_meta).unwrap();
        }

        let mut decoder = PackedDecoder::new(&buf);
        let msg1: Field = decoder.decode_next().unwrap();
        let msg2: DecimalTypeMeta = decoder.decode_next().unwrap();

        assert_eq!(field, msg1);
        assert_eq!(decimal_meta, msg2);
        assert_eq!(19, msg2.precision);
    }
}

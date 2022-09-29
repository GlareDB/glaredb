//! Extensions to various byte traits.
use bytes::{Buf, BufMut};
use std::io;
use std::str;

pub trait BufStringMut: BufMut {
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
pub struct Cursor<'a> {
    buf: &'a [u8],
}

impl<'a> Cursor<'a> {
    /// Creates a new cursor from the provided buffer.
    pub fn new(buf: &'a [u8]) -> Self {
        Cursor { buf }
    }

    /// Reads a null-terminated string from the buffer.
    ///
    /// Returns an error if the end of the buffer is reached without
    /// encountering a null byte.
    ///
    /// # Panics
    ///
    /// Panics if the string is not valid utf8.
    pub fn read_cstring(&mut self) -> Result<&'a str, io::Error> {
        match self.buf.iter().position(|b| *b == 0) {
            Some(pos) => {
                let s = str::from_utf8(&self.buf[0..pos]).unwrap();
                self.advance(pos + 1);
                Ok(s)
            }
            None => Err(io::ErrorKind::UnexpectedEof.into()),
        }
    }

    /// Checks if the next byte is a null byte. Does not advance the cursor.
    pub fn peek_next_is_null(&self) -> bool {
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

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn can_read_string() {
        let mut buf = BytesMut::new();
        buf.put_cstring("hello");
        buf.put_u8(b'!');

        let buf = buf.freeze();
        let mut cursor = Cursor::new(&buf[..]);
        let out = cursor.read_cstring().unwrap();

        assert_eq!("hello", out)
    }

    #[test]
    fn missing_null_byte() {
        let mut buf = BytesMut::new();
        buf.put_u8(b'!');

        let buf = buf.freeze();
        let mut cursor = Cursor::new(&buf[..]);
        let _ = cursor.read_cstring().expect_err("no null byte");
    }
}

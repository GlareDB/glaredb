use std::fmt;

/// Encodes a buffer inot a hex string.
pub fn encode(buf: impl AsRef<[u8]>) -> String {
    let mut s = String::with_capacity(buf.as_ref().len() * 2);
    buf.write_hex(&mut s).unwrap();
    s
}

pub trait WriteHex {
    /// Writes a lowercase hex string of self into `w`.
    fn write_hex<W: fmt::Write>(&self, w: &mut W) -> fmt::Result;
}

impl<T: AsRef<[u8]>> WriteHex for T {
    fn write_hex<W: fmt::Write>(&self, w: &mut W) -> fmt::Result {
        const CHARS: &[u8; 16] = b"0123456789abcdef";

        for &b in self.as_ref() {
            w.write_char(CHARS[((b >> 4) & 0xF) as usize] as char)?;
            w.write_char(CHARS[(b & 0xF) as usize] as char)?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_hello_world() {
        assert_eq!(encode("Hello world!"), "48656c6c6f20776f726c6421");
    }
}

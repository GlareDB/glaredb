use std::fmt;

// TODO: Scalar functions for this

pub trait WriteHex {
    /// Writes a lowercase hex string of self into `w`.
    fn write_hex<W: fmt::Write>(&self, w: &mut W) -> fmt::Result;

    /// Writes the hex string prefixed by '\x'.
    fn write_hex_with_escape<W: fmt::Write>(&self, w: &mut W) -> fmt::Result {
        w.write_str("\\x")?;
        self.write_hex(w)
    }
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
    fn write_hello_world() {
        let mut s = String::new();
        "Hello world!".write_hex(&mut s).unwrap();
        assert_eq!(s, "48656c6c6f20776f726c6421");
    }

    #[test]
    fn write_hello_world_with_escape_string() {
        let mut s = String::new();
        "Hello world!".write_hex_with_escape(&mut s).unwrap();
        assert_eq!(s, "\\x48656c6c6f20776f726c6421");
    }
}

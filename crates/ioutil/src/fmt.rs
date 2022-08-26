use std::fmt;

/// A wrapper type around a binary slice implementing hex formatting.
#[derive(Debug)]
pub struct HexBuf<'a>(pub &'a [u8]);

impl<'a> fmt::LowerHex for HexBuf<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if f.alternate() {
            write!(f, "0x")?;
        }
        for b in self.0 {
            write!(f, "{:x}", b)?;
        }
        Ok(())
    }
}

impl<'a> fmt::UpperHex for HexBuf<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if f.alternate() {
            write!(f, "0X")?;
        }
        for b in self.0 {
            write!(f, "{:X}", b)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple() {
        let out = format!("{:x}", HexBuf(&[255, 254]));
        assert_eq!("fffe", out);

        let out = format!("{:X}", HexBuf(&[255, 254]));
        assert_eq!("FFFE", out);
    }

    #[test]
    fn alternate() {
        let out = format!("{:#X}", HexBuf(&[255, 254]));
        assert_eq!("0XFFFE", out);
    }
}

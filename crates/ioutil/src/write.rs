use bytes::BufMut;
use std::fmt;

/// A trait representing infallible writes to some underlying buffer.
///
/// The methods provided by this trait are the same as the methods provided by
/// `std::fmt::Write` except that they do not return a result.
///
/// Types that implment this trait can be used with the `write!` macro without
/// having to check for an error.
pub trait InfallibleWrite {
    /// Writes a string slice into this writer.
    fn write_str(&mut self, s: &str);

    /// Glue for usage of the `write!` macro with implementors of this trait.
    fn write_fmt(&mut self, args: fmt::Arguments<'_>);

    /// Writes a char into this writer.
    fn write_char(&mut self, c: char);
}

impl<B: BufMut + fmt::Write> InfallibleWrite for B {
    fn write_str(&mut self, s: &str) {
        self.put(s.as_bytes())
    }

    fn write_fmt(&mut self, args: fmt::Arguments<'_>) {
        fmt::Write::write_fmt(self, args).expect("BufMut does not return a Result");
    }

    fn write_char(&mut self, c: char) {
        self.put(c.encode_utf8(&mut [0; 4]).as_bytes())
    }
}

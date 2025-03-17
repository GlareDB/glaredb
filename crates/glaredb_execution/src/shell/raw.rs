use std::io;

/// A wrapper around a writer that will replace '\n' with '\r\n' when writing
/// bytes.
#[derive(Debug)]
pub struct RawTerminalWriter<'a, W> {
    pub writer: &'a mut W,
}

impl<'a, W: io::Write> RawTerminalWriter<'a, W> {
    pub const fn new(writer: &'a mut W) -> Self {
        RawTerminalWriter { writer }
    }
}

impl<W: io::Write> io::Write for RawTerminalWriter<'_, W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        write_raw(self.writer, buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

fn write_raw(writer: &mut impl io::Write, buf: &[u8]) -> io::Result<usize> {
    let mut current = buf;
    let mut n = 0;

    while !current.is_empty() {
        let pos = current
            .iter()
            .position(|&b| b == b'\n')
            .unwrap_or(current.len());

        // Write the part that doesn't have a new line.
        let to_write = &current[0..pos];
        writer.write_all(to_write)?;
        n += to_write.len();

        if pos == current.len() {
            return Ok(n);
        }

        // Insert a '\r\n'
        writer.write_all(b"\r\n")?;
        n += 1; // This should only account for '\n', user doesn't care/know about the '\r'.

        // Update current, skipping the '\n'
        current = &current[pos + 1..];
    }

    Ok(n)
}

#[cfg(test)]
mod tests {
    use io::Write;

    use super::*;

    #[test]
    fn no_newlines() {
        let mut dest = Vec::new();

        let n = RawTerminalWriter::new(&mut dest).write(b"hello").unwrap();

        assert_eq!(5, n);
        assert_eq!(b"hello".as_slice(), &dest);
    }

    #[test]
    fn newline_in_middle() {
        let mut dest = Vec::new();

        let n = RawTerminalWriter::new(&mut dest).write(b"he\nllo").unwrap();

        assert_eq!(6, n);
        assert_eq!(b"he\r\nllo".as_slice(), &dest);
    }

    #[test]
    fn newline_at_start() {
        let mut dest = Vec::new();

        let n = RawTerminalWriter::new(&mut dest).write(b"\nhello").unwrap();

        assert_eq!(6, n);
        assert_eq!(b"\r\nhello".as_slice(), &dest);
    }

    #[test]
    fn newline_at_end() {
        let mut dest = Vec::new();

        let n = RawTerminalWriter::new(&mut dest).write(b"hello\n").unwrap();

        assert_eq!(6, n);
        assert_eq!(b"hello\r\n".as_slice(), &dest);
    }

    #[test]
    fn multiple_newlines() {
        let mut dest = Vec::new();

        let n = RawTerminalWriter::new(&mut dest)
            .write(b"h\nello\n")
            .unwrap();

        assert_eq!(7, n);
        assert_eq!(b"h\r\nello\r\n".as_slice(), &dest);
    }

    #[test]
    fn single_newline() {
        let mut dest = Vec::new();

        let n = RawTerminalWriter::new(&mut dest).write(b"\n").unwrap();

        assert_eq!(1, n);
        assert_eq!(b"\r\n".as_slice(), &dest);
    }

    #[test]
    fn only_newlines() {
        let mut dest = Vec::new();

        let n = RawTerminalWriter::new(&mut dest).write(b"\n\n\n").unwrap();

        assert_eq!(3, n);
        assert_eq!(b"\r\n\r\n\r\n".as_slice(), &dest);
    }

    #[test]
    fn inner_writer_does_not_conume_everything() {
        struct PickyWriter {
            val: Vec<u8>,
        }

        impl io::Write for PickyWriter {
            fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
                if buf.len() > 0 {
                    self.val.push(buf[0]);
                    Ok(1)
                } else {
                    Ok(0)
                }
            }

            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }

        let mut inner = PickyWriter { val: Vec::new() };

        let n = RawTerminalWriter::new(&mut inner)
            .write(b"h\nello\n")
            .unwrap();

        assert_eq!(7, n);
        assert_eq!(b"h\r\nello\r\n".as_slice(), &inner.val);
    }
}

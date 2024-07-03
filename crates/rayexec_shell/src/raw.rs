use std::io;

/// A wrapper around a writer that will replace '\n' with '\r\n' when writing
/// bytes.
#[derive(Debug)]
pub struct RawTerminalWriter<'a, W> {
    pub writer: &'a mut W,
}

impl<'a, W: io::Write> RawTerminalWriter<'a, W> {
    pub fn new(writer: &'a mut W) -> Self {
        RawTerminalWriter { writer }
    }
}

impl<'a, W: io::Write> io::Write for RawTerminalWriter<'a, W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut current = buf;
        let mut n = 0;

        while !current.is_empty() {
            let pos = current
                .iter()
                .position(|&b| b == b'\n')
                .unwrap_or(current.len());

            // Write the part that doesn't have a new line.
            let to_write = &current[0..pos];
            n += self.writer.write(to_write)?;

            if pos == current.len() {
                return Ok(n);
            }

            // Insert a '\r\n'
            self.writer.write_all(&[b'\r', b'\n'])?;
            n += 1; // This should only account for '\n', user doesn't care/know about the '\r'.

            // Update current, skipping the '\n'
            current = &current[pos + 1..];
        }

        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

#[cfg(test)]
mod tests {
    use io::Write;

    use super::*;

    #[test]
    fn no_newlines() {
        let mut dest = Vec::new();

        let n = RawTerminalWriter::new(&mut dest)
            .write(&[b'h', b'e', b'l', b'l', b'o'])
            .unwrap();

        assert_eq!(5, n);
        assert_eq!(vec![b'h', b'e', b'l', b'l', b'o'], dest);
    }

    #[test]
    fn newline_in_middle() {
        let mut dest = Vec::new();

        let n = RawTerminalWriter::new(&mut dest)
            .write(&[b'h', b'e', b'\n', b'l', b'l', b'o'])
            .unwrap();

        assert_eq!(6, n);
        assert_eq!(vec![b'h', b'e', b'\r', b'\n', b'l', b'l', b'o'], dest);
    }

    #[test]
    fn newline_at_end() {
        let mut dest = Vec::new();

        let n = RawTerminalWriter::new(&mut dest)
            .write(&[b'h', b'e', b'l', b'l', b'o', b'\n'])
            .unwrap();

        assert_eq!(6, n);
        assert_eq!(vec![b'h', b'e', b'l', b'l', b'o', b'\r', b'\n'], dest);
    }

    #[test]
    fn multiple_newlines() {
        let mut dest = Vec::new();

        let n = RawTerminalWriter::new(&mut dest)
            .write(&[b'h', b'\n', b'e', b'l', b'l', b'o', b'\n'])
            .unwrap();

        assert_eq!(7, n);
        assert_eq!(
            vec![b'h', b'\r', b'\n', b'e', b'l', b'l', b'o', b'\r', b'\n'],
            dest
        );
    }
}

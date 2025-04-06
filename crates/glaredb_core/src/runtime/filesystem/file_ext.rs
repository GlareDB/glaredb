use std::pin::Pin;
use std::task::{Context, Poll};

use glaredb_error::Result;

use super::File;

/// Extension trait for `File` providing some utility async methods.
pub trait FileExt: File {
    /// Read into the provided buffer, returning the amount of bytes read.
    ///
    /// `0` may be returned on EOF, or if the provided buffer's length is zero.
    fn read(&mut self, buf: &mut [u8]) -> Read<'_, Self>;

    /// Read from the file until we've either filled up the given buffer, or we
    /// reach the end of the file.
    fn read_fill(&mut self, buf: &mut [u8]) -> ReadFill<'_, Self>;
}

#[derive(Debug)]
pub struct Read<'a, F: File + ?Sized> {
    file: &'a mut F,
    buf: &'a mut [u8],
}

impl<'a, F> Future for Read<'a, F>
where
    F: File + ?Sized,
{
    type Output = Result<usize>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        this.file.poll_read(cx, this.buf)
    }
}

#[derive(Debug)]
pub struct ReadFill<'a, F: File + ?Sized> {
    file: &'a mut F,
    read_count: usize,
    buf: &'a mut [u8],
}

impl<'a, F> Future for ReadFill<'a, F>
where
    F: File + ?Sized,
{
    type Output = Result<usize>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        loop {
            let read_buf = &mut this.buf[this.read_count..];
            if read_buf.is_empty() {
                // Reached the end of the buffer.
                return Poll::Ready(Ok(this.read_count));
            }

            let n = match this.file.poll_read(cx, read_buf) {
                Poll::Ready(Ok(n)) => n,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => return Poll::Pending,
            };

            this.read_count += n;

            if n == 0 {
                // Reached the end of the file.
                return Poll::Ready(Ok(this.read_count));
            }

            // Keep looping, trying to read as much as we can until pending or
            // end of file/buffer.
        }
    }
}

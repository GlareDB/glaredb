use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::FutureExt;
use glaredb_error::{DbError, Result};

use super::FileHandle;

/// Extension trait for `File` providing some utility async methods.
pub trait FileExt: FileHandle {
    /// Read into the provided buffer, returning the amount of bytes read.
    ///
    /// `0` may be returned on EOF, or if the provided buffer's length is zero.
    fn read<'a>(&'a mut self, buf: &'a mut [u8]) -> Read<'a, Self> {
        Read { file: self, buf }
    }

    /// Read from the file until we've either filled up the given buffer, or we
    /// reach the end of the file.
    #[must_use]
    fn read_fill<'a>(&'a mut self, buf: &'a mut [u8]) -> ReadFill<'a, Self> {
        ReadFill {
            file: self,
            read_count: 0,
            buf,
        }
    }

    /// Completely fill the buffering before resolving.
    ///
    /// Errors if we reach the end of the file before filling the buffer.
    #[must_use]
    fn read_exact<'a>(&'a mut self, buf: &'a mut [u8]) -> ReadExact<'a, Self> {
        ReadExact {
            fill: ReadFill {
                file: self,
                read_count: 0,
                buf,
            },
        }
    }

    /// Set the seek position for the file.
    #[must_use]
    fn seek(&mut self, seek: io::SeekFrom) -> Seek<'_, Self> {
        Seek { seek, file: self }
    }
}

impl<F> FileExt for F where F: FileHandle {}

#[derive(Debug)]
pub struct Read<'a, F: FileHandle + ?Sized> {
    file: &'a mut F,
    buf: &'a mut [u8],
}

impl<F> Future for Read<'_, F>
where
    F: FileHandle + ?Sized,
{
    type Output = Result<usize>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        this.file.poll_read(cx, this.buf)
    }
}

#[derive(Debug)]
pub struct ReadFill<'a, F: FileHandle + ?Sized> {
    file: &'a mut F,
    read_count: usize,
    buf: &'a mut [u8],
}

impl<F> Future for ReadFill<'_, F>
where
    F: FileHandle + ?Sized,
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

#[derive(Debug)]
pub struct ReadExact<'a, F: FileHandle + ?Sized> {
    fill: ReadFill<'a, F>,
}

impl<F> Future for ReadExact<'_, F>
where
    F: FileHandle + ?Sized,
{
    type Output = Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.fill.poll_unpin(cx) {
            Poll::Ready(Ok(n)) => {
                if n == self.fill.buf.len() {
                    Poll::Ready(Ok(()))
                } else {
                    Poll::Ready(Err(DbError::new(format!(
                        "Unexpected EOF, read {} bytes, expected to read {} bytes",
                        n,
                        self.fill.buf.len()
                    ))))
                }
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[derive(Debug)]
pub struct Seek<'a, F: FileHandle + ?Sized> {
    seek: io::SeekFrom,
    file: &'a mut F,
}

impl<F> Future for Seek<'_, F>
where
    F: FileHandle + ?Sized,
{
    type Output = Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let seek = self.seek;
        self.file.poll_seek(cx, seek)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::buffer_manager::DefaultBufferManager;
    use crate::runtime::filesystem::memory::MemoryFileHandle;
    use crate::util::future::block_on;

    #[test]
    fn read_fill_small_buffer() {
        let mut handle = MemoryFileHandle::from_bytes(&DefaultBufferManager, b"hello").unwrap();
        let mut out = vec![0; 4];

        let count = block_on(handle.read_fill(&mut out)).unwrap();
        assert_eq!(4, count);
        assert_eq!(b"hell", &out[0..4]);
    }

    #[test]
    fn read_fill_small_file() {
        let mut handle = MemoryFileHandle::from_bytes(&DefaultBufferManager, b"hello").unwrap();
        let mut out = vec![0; 10];

        let count = block_on(handle.read_fill(&mut out)).unwrap();
        assert_eq!(5, count);
        assert_eq!(b"hello", &out[0..5]);
    }
}

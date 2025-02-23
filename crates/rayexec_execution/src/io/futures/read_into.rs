use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use rayexec_error::Result;

use crate::io::file::AsyncReadStream;

/// A future that reads from the stream into a buffer.
///
/// This will return the number of bytes written to the buffer, which may be
/// less than the length of the buffer if the stream completes sooner.
///
/// The stream can continue to be used after this future complete.
pub struct ReadInto<'a, S> {
    stream: &'a mut S,
    buf: &'a mut [u8],
    count: usize,
}

impl<'a, S> ReadInto<'a, S>
where
    S: AsyncReadStream,
{
    /// Create a new future for reading into the provided buffer.
    ///
    /// This will write to the start of the buffer.
    pub fn new(stream: &'a mut S, buf: &'a mut [u8]) -> Self {
        ReadInto {
            stream,
            buf,
            count: 0,
        }
    }

    /// Return the amount written to the buffer.
    ///
    /// This can be used to resume reading into a buffer.
    pub fn amount_written(&self) -> usize {
        self.count
    }

    /// Resume reading into a buffer.
    ///
    /// This must be provided the same stream and buffer that were used for the
    /// initial create.
    ///
    /// `count` should be the value retried by `amount_written`.
    pub fn resume(stream: &'a mut S, buf: &'a mut [u8], count: usize) -> Self {
        ReadInto { stream, buf, count }
    }
}

impl<'a, S> Future for ReadInto<'a, S>
where
    S: AsyncReadStream,
{
    type Output = Result<usize>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;

        loop {
            if this.count >= this.buf.len() {
                // TODO: Should this error instead?
                return Poll::Ready(Ok(self.count));
            }

            let buf = &mut this.buf[this.count..];

            match this.stream.poll_read(cx, buf) {
                Ok(Poll::Ready(Some(count))) => {
                    this.count += count;
                    // Keep trying to read more.
                    continue;
                }
                Ok(Poll::Ready(None)) => return Poll::Ready(Ok(self.count)),
                Ok(Poll::Pending) => return Poll::Pending,
                Err(e) => return Poll::Ready(Err(e)),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::FutureExt;
    use stdutil::task::noop_context;

    use super::*;

    #[derive(Debug)]
    struct TestReadStream {
        /// Value to write to the buffer repeatedly.
        pub value: u8,
        /// Remaining number of bytes to write before "terminating".
        pub remaining: usize,
    }

    impl TestReadStream {
        pub fn new(value: u8, count: usize) -> Self {
            TestReadStream {
                value,
                remaining: count,
            }
        }
    }

    impl AsyncReadStream for TestReadStream {
        fn poll_read(
            self: &mut Self,
            _cx: &mut Context,
            buf: &mut [u8],
        ) -> Result<Poll<Option<usize>>> {
            if self.remaining == 0 {
                return Ok(Poll::Ready(None));
            }

            let count = usize::min(self.remaining, buf.len());
            buf[0..count].fill(self.value);
            self.remaining -= count;

            Ok(Poll::Ready(Some(count)))
        }
    }

    #[test]
    fn read_small_buffer() {
        let mut buf = vec![0; 4];

        let mut stream = TestReadStream::new(8, 100);
        let poll = ReadInto::new(&mut stream, &mut buf).poll_unpin(&mut noop_context());

        match poll {
            Poll::Ready(result) => {
                let count = result.unwrap();
                assert_eq!(4, count);
            }
            _ => panic!("unexpected poll"),
        }

        assert_eq!(&[8, 8, 8, 8], buf.as_slice());
    }

    #[test]
    fn read_stream_terminates() {
        let mut buf = vec![0; 4];

        let mut stream = TestReadStream::new(8, 2);
        let poll = ReadInto::new(&mut stream, &mut buf).poll_unpin(&mut noop_context());

        match poll {
            Poll::Ready(result) => {
                let count = result.unwrap();
                assert_eq!(2, count);
            }
            _ => panic!("unexpected poll"),
        }

        assert_eq!(&[8, 8, 0, 0], buf.as_slice());
    }
}

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use rayexec_error::Result;

use crate::exp::AsyncReadStream;

/// A future that reads from the stream into a buffer.
///
/// This will return the number of bytes written to the buffer, which may be
/// less than the length of the buffer if the stream completes sooner.
///
/// The stream can continue to be used after this future complete.
pub struct ReadInto<'a> {
    stream: &'a mut Pin<Box<dyn AsyncReadStream>>,
    buf: &'a mut [u8],
    count: usize,
}

impl<'a> ReadInto<'a> {
    pub fn new(stream: &'a mut Pin<Box<dyn AsyncReadStream>>, buf: &'a mut [u8]) -> Self {
        ReadInto {
            stream,
            buf,
            count: 0,
        }
    }
}

impl<'a> Future for ReadInto<'a> {
    type Output = Result<usize>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;

        loop {
            if this.count >= this.buf.len() {
                return Poll::Ready(Ok(self.count));
            }

            let buf = &mut this.buf[this.count..];

            match this.stream.as_mut().poll_read(cx, buf) {
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
    use std::sync::Arc;
    use std::task::Waker;

    use futures::FutureExt;

    use super::*;
    use crate::exp::testutil::{NoopWaker, TestReadStream};

    #[test]
    fn read_small_buffer() {
        let mut buf = vec![0; 4];

        let mut stream = TestReadStream::new_pinned(8, 100);
        let waker: Waker = Arc::new(NoopWaker).into();
        let mut cx = Context::from_waker(&waker);
        let poll = ReadInto::new(&mut stream, &mut buf).poll_unpin(&mut cx);

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

        let mut stream = TestReadStream::new_pinned(8, 2);
        let waker: Waker = Arc::new(NoopWaker).into();
        let mut cx = Context::from_waker(&waker);
        let poll = ReadInto::new(&mut stream, &mut buf).poll_unpin(&mut cx);

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

use std::fmt::Debug;
use std::pin::Pin;
use std::task::{Context, Poll};

use glaredb_error::Result;

use crate::access::AccessConfig;

pub trait FileProvider: Sync + Send + Debug {
    /// Configuration for accessing a single file source.
    ///
    /// This should include location, credentials, etc for accessing the file.
    type AccessConfig: AccessConfig;

    /// File type to use for reads.
    type ReadFile: FileSource;

    /// Open a file for reading.
    fn open_for_read(&self, conf: &Self::AccessConfig) -> Result<Self::ReadFile>;
}

/// Describes accessing some "file" object.
///
/// All methods accept a mut reference to ensure exclusive access even if the
/// underlying "file" does not require it (e.g. file descriptors technically
/// don't need mut access, but the descriptors get updated on reads/seeks). This
/// doesn't guard against misuse, but close enough.
pub trait FileSource: Sync + Send + Debug {
    type ReadStream: AsyncReadStream;
    type ReadRangeStream: AsyncReadStream;

    /// Reads the file as a stream, with the stream starting at the beginning.
    fn read(&mut self) -> Self::ReadStream;

    /// Reads a range of bytes from the file as a stream.
    fn read_range(&mut self, start: usize, len: usize) -> Self::ReadRangeStream;
}

pub trait AsyncReadStream: Debug + Sync + Send {
    /// Polls the stream to read data into `buf`.
    ///
    /// Each poll will start writing at the beginning of the buffer.
    ///
    /// `Poll::Ready(Some(n))` indicates `n` bytes were written to the buffer.
    /// Should continue polling to read more data.
    ///
    /// `Poll::Ready(None)` indicates the stream is complete. No data is written
    /// to the buffer.
    fn poll_read(&mut self, cx: &mut Context, buf: &mut [u8]) -> Result<Poll<Option<usize>>>;
}

pub trait AsyncWriteSink: Send {
    /// Writes the contents of `buf` to the sink.
    ///
    /// Returns the number of bytes from `buf` that were written. If this is
    /// less than the length of `buf`, then `buf` should be sliced to the
    /// remaining bytes and provided on the next poll.
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context, buf: &[u8]) -> Result<Poll<usize>>;

    /// Flushes data to the sink, ensuring it's been written.
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context) -> Result<Poll<()>>;
}

// #[cfg(test)]
// pub(crate) mod testutil {
//     use super::*;

//     #[derive(Debug)]
//     pub struct TestReadStream {
//         /// Value to write to the buffer repeatedly.
//         pub value: u8,
//         /// Remaining number of bytes to write before "terminating".
//         pub remaining: usize,
//     }

//     impl TestReadStream {
//         pub fn new(value: u8, count: usize) -> Self {
//             TestReadStream {
//                 value,
//                 remaining: count,
//             }
//         }
//     }

//     impl AsyncReadStream for TestReadStream {
//         fn poll_read(
//             self: &mut Self,
//             _cx: &mut Context,
//             buf: &mut [u8],
//         ) -> Result<Poll<Option<usize>>> {
//             if self.remaining == 0 {
//                 return Ok(Poll::Ready(None));
//             }

//             let count = usize::min(self.remaining, buf.len());
//             buf[0..count].fill(self.value);
//             self.remaining -= count;

//             Ok(Poll::Ready(Some(count)))
//         }
//     }
// }

//! Traits for opening files.
//!
//! Note that many of the "async" traits do no accept a `Pin<&mut Self>` which
//! is normal for futures as those traits require external state management.
//! They cannot be used in async contexts without helpers.

use std::fmt::Debug;
use std::future::Future;
use std::task::{Context, Poll};

use glaredb_error::Result;

use super::access::AccessConfig;

/// Open files for reads and writes.
pub trait FileOpener: Debug + Sync + Send {
    /// Configuration for accessing a single file source.
    ///
    /// This should include location, credentials, etc for accessing the file.
    type AccessConfig: AccessConfig;

    /// File type to use for reads.
    type ReadFile: FileSource;

    /// Lists all files with the given prefix.
    fn list_prefix(&self, prefix: &str) -> impl Future<Output = Result<Vec<String>>> + Send;

    /// Open a file for reading.
    fn open_for_read(&self, conf: &Self::AccessConfig) -> Result<Self::ReadFile>;
}

/// Describes accessing some "file" object.
///
/// All methods accept a mut reference to ensure exclusive access even if the
/// underlying "file" does not require it (e.g. file descriptors technically
/// don't need mut access, but the descriptors get updated on reads/seeks). This
/// doesn't guard against misuse, but close enough.
pub trait FileSource: Debug + Sync + Send {
    type ReadStream: AsyncReadStream;
    type ReadRangeStream: AsyncReadStream;

    fn size(&self) -> impl Future<Output = Result<usize>> + Send;

    /// Reads the file as a stream, with the stream starting at the beginning.
    fn read(&mut self) -> Self::ReadStream;

    /// Reads a range of bytes from the file as a stream.
    fn read_range(&mut self, start: usize, len: usize) -> Self::ReadRangeStream;
}

pub trait AsyncReadStream: Debug + Sync + Send {
    /// Polls the stream to read data into `buf`.
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
    fn poll_write(&mut self, cx: &mut Context, buf: &[u8]) -> Result<Poll<usize>>;

    /// Flushes data to the sink, ensuring it's been written.
    fn poll_flush(&mut self, cx: &mut Context) -> Result<Poll<()>>;
}

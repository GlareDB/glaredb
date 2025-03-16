use std::sync::Arc;
use std::task::{Context, Poll};

use rayexec_error::{RayexecError, Result};

use super::file::{AsyncReadStream, FileSource};
use crate::buffer::buffer_manager::AsRawBufferManager;
use crate::buffer::typed::ByteBuffer;

#[derive(Debug)]
pub struct MemoryFileSource {
    // TODO: Try to move "cloneability" into the buffer itself.
    //
    // This would also help with the `OwnedOrShared` type for array buffers.
    data: Arc<ByteBuffer>,
}

impl MemoryFileSource {
    /// Create a new memory file source.
    ///
    /// Mostly useful for testing.
    pub fn new(manager: &impl AsRawBufferManager, bytes: impl AsRef<[u8]>) -> Result<Self> {
        let bytes = bytes.as_ref();
        let cap = bytes.len();

        let mut buffer = ByteBuffer::try_with_capacity(manager, cap)?;
        buffer.as_slice_mut()[0..cap].copy_from_slice(bytes);

        Ok(MemoryFileSource {
            data: Arc::new(buffer),
        })
    }
}

impl FileSource for MemoryFileSource {
    type ReadStream = MemoryFileRead;
    type ReadRangeStream = MemoryFileRead;

    async fn size(&self) -> Result<usize> {
        Ok(self.data.capacity())
    }

    fn read(&mut self) -> Self::ReadStream {
        MemoryFileRead {
            idx: 0,
            remaining: self.data.capacity(),
            data: self.data.clone(),
        }
    }

    fn read_range(&mut self, start: usize, len: usize) -> Self::ReadRangeStream {
        MemoryFileRead {
            idx: start,
            remaining: len,
            data: self.data.clone(),
        }
    }
}

#[derive(Debug)]
pub struct MemoryFileRead {
    idx: usize,
    remaining: usize,
    data: Arc<ByteBuffer>,
}

impl AsyncReadStream for MemoryFileRead {
    fn poll_read(&mut self, _cx: &mut Context, buf: &mut [u8]) -> Result<Poll<Option<usize>>> {
        if self.remaining == 0 {
            return Ok(Poll::Ready(None));
        }

        if (self.data.capacity() - self.idx) < self.remaining {
            return Err(RayexecError::new("Invalid range"));
        }

        let count = usize::min(buf.len(), self.remaining);

        let src = &self.data.as_slice()[self.idx..(self.idx + count)];
        let dest = &mut buf[..count];
        dest.copy_from_slice(src);

        self.idx += count;
        self.remaining -= count;

        Ok(Poll::Ready(Some(count)))
    }
}

#[cfg(test)]
mod tests {
    use stdutil::future::block_on;

    use super::*;
    use crate::buffer::buffer_manager::NopBufferManager;
    use crate::io::futures::read_into::ReadInto;

    #[test]
    fn memory_size() {
        let source = MemoryFileSource::new(&NopBufferManager, [0, 1, 2, 3]).unwrap();
        let size = block_on(source.size()).unwrap();
        assert_eq!(4, size);
    }

    #[test]
    fn memory_read_full() {
        let mut source = MemoryFileSource::new(&NopBufferManager, [0, 1, 2, 3]).unwrap();
        let mut buf = [0; 4];

        let mut stream = source.read();
        let count = block_on(ReadInto::new(&mut stream, &mut buf)).unwrap();

        assert_eq!(4, count);
        assert_eq!([0, 1, 2, 3], buf);
    }

    #[test]
    fn memory_read_range() {
        let mut source = MemoryFileSource::new(&NopBufferManager, [0, 1, 2, 3]).unwrap();
        let mut buf = [0; 4];

        let mut stream = source.read_range(1, 2);
        let count = block_on(ReadInto::new(&mut stream, &mut buf)).unwrap();

        assert_eq!(2, count);
        assert_eq!(&[1, 2], &buf[0..2]);
    }
}

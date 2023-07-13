pub mod csv;
pub mod json;
pub mod parquet;

use std::io::{self, Write};
use std::sync::Arc;

/// A simple buffer to aid in converting writers to async writers. It's expected
/// that the lock has no contention.
#[derive(Clone)]
pub struct SharedBuffer {
    pub buffer: Arc<futures::lock::Mutex<Vec<u8>>>,
}

impl SharedBuffer {
    /// Create a new buffer with capacity.
    pub fn with_capacity(cap: usize) -> Self {
        SharedBuffer {
            buffer: Arc::new(futures::lock::Mutex::new(Vec::with_capacity(cap))),
        }
    }
}

impl Write for SharedBuffer {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut buffer = self.buffer.try_lock().unwrap();
        Write::write(&mut *buffer, buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        let mut buffer = self.buffer.try_lock().unwrap();
        Write::flush(&mut *buffer)
    }
}

//! Transaction timestamp utilities.
use std::sync::atomic::{AtomicU64, Ordering};

pub trait IntoByteVec {
    fn into_vec(&self) -> Vec<u8>;
}

pub trait TimestampGen {
    /// The timestamp returned. Must maintain order when serialized into bytes.
    type Timestamp: IntoByteVec;

    type Error: std::fmt::Debug;

    /// Get the next timestamp to use.
    fn next(&self) -> Result<Self::Timestamp, Self::Error>;
}

impl IntoByteVec for u64 {
    fn into_vec(&self) -> Vec<u8> {
        self.to_be_bytes().to_vec()
    }
}

/// A timestamp generator suitable for use on a single machine.
///
/// Every timestamp is a u64 serialized to its big-endian representation.
#[derive(Debug)]
pub struct SingleNodeTimestampGen {
    ts: AtomicU64,
}

impl SingleNodeTimestampGen {
    pub fn new(start: u64) -> Self {
        SingleNodeTimestampGen {
            ts: AtomicU64::new(start),
        }
    }
}

impl TimestampGen for SingleNodeTimestampGen {
    type Timestamp = u64;
    type Error = std::convert::Infallible;

    fn next(&self) -> Result<Self::Timestamp, Self::Error> {
        Ok(self.ts.fetch_add(1, Ordering::Relaxed))
    }
}

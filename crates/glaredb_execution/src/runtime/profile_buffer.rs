use std::sync::Arc;

use glaredb_error::{DbError, Result};
use parking_lot::Mutex;

use crate::catalog::profile::PartitionPipelineProfile;

/// Responsible for writing the parition pipeline's profile at a specific index
/// in the profile buffer.
#[derive(Debug)]
pub struct ProfileSink {
    /// Index to write to.
    idx: usize,
    /// The profiles.
    inner: Arc<Mutex<ProfileBufferInner>>,
}

impl ProfileSink {
    pub fn put(&self, profile: PartitionPipelineProfile) {
        let mut inner = self.inner.lock();
        if inner.consumed {
            // Profiles were already taken. Avoid panicking and just drop.
            return;
        }
        inner.profiles[self.idx] = Some(profile)
    }
}

#[derive(Debug)]
pub struct ProfileBuffer {
    inner: Arc<Mutex<ProfileBufferInner>>,
}

#[derive(Debug)]
struct ProfileBufferInner {
    /// If the profiles have been consumed by the query handle.
    consumed: bool,
    profiles: Vec<Option<PartitionPipelineProfile>>,
}

impl ProfileBuffer {
    pub fn new(partition_pipeline_count: usize) -> (Self, ProfileSinkGenerator) {
        let profiles = (0..partition_pipeline_count).map(|_| None).collect();
        let inner = Arc::new(Mutex::new(ProfileBufferInner {
            consumed: false,
            profiles,
        }));

        let generator = ProfileSinkGenerator {
            inner: inner.clone(),
            curr: 0,
            count: partition_pipeline_count,
        };

        let buffer = ProfileBuffer { inner };

        (buffer, generator)
    }

    pub(crate) fn take_profiles(&self) -> Result<Vec<Option<PartitionPipelineProfile>>> {
        let mut inner = self.inner.lock();
        if inner.consumed {
            return Err(DbError::new(
                "Cannot take execution profiles more than once",
            ));
        }

        let profiles = std::mem::take(&mut inner.profiles);
        inner.consumed = true;

        Ok(profiles)
    }
}

/// Generates sinks for partition pipelines to write their execution profile to.
#[derive(Debug)]
pub struct ProfileSinkGenerator {
    inner: Arc<Mutex<ProfileBufferInner>>,
    curr: usize,
    count: usize,
}

impl Iterator for ProfileSinkGenerator {
    type Item = ProfileSink;

    fn next(&mut self) -> Option<Self::Item> {
        if self.curr >= self.count {
            return None;
        }

        let sink = ProfileSink {
            idx: self.curr,
            inner: self.inner.clone(),
        };

        self.curr += 1;

        Some(sink)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let rem = self.count - self.curr;
        (rem, Some(rem))
    }
}

impl ExactSizeIterator for ProfileSinkGenerator {}

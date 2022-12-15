//! Configuration for background jobs.
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Default, Debug, Deserialize, Serialize)]
pub struct BackgroundConfig {
    /// Storage reporting background job. Note that if cloud communication is
    /// disabled, this will only log out the total storage used.
    pub storage_reporting: JobConfig,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct JobConfig {
    /// Interval between job runs.
    pub interval: Duration,
}

impl Default for JobConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(30),
        }
    }
}

//! Configuration for background jobs.
use serde::Deserialize;
use std::time::Duration;

#[derive(Debug, Deserialize)]
pub struct BackgroundConfig {
    /// Storage reporting background job. Note that if cloud communication is
    /// disabled, this will only log out the total storage used.
    pub storage_reporting: JobConfig,
}

impl Default for BackgroundConfig {
    fn default() -> Self {
        BackgroundConfig {
            storage_reporting: JobConfig {
                interval: Duration::from_secs(30),
            },
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct JobConfig {
    /// Interval between job runs.
    pub interval: Duration,
}

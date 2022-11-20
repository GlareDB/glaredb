//! Configuration for communicating with Cloud.
use serde::Deserialize;
use std::time::Duration;

#[derive(Debug, Default, Deserialize)]
pub struct CloudConfig {
    /// Whether or not Cloud communication is enabled.
    pub enabled: bool,
    /// Root url to use for api calls.
    pub api_url: String,
    /// Path to ping to ensure GlareDB can communicate with the cloud service.
    pub ping_path: String,
    /// Request timeout.
    pub timeout: Duration,
}

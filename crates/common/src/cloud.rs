//! Configuration for communicating with Cloud.
use serde::{Deserialize, Serialize};
use std::time::Duration;

// TODO: Add `system_api_key` field so the cloud client can authenticate with
// Cloud.
#[derive(Debug, Deserialize, Serialize)]
pub struct CloudConfig {
    /// Whether or not Cloud communication is enabled.
    pub enabled: bool,
    /// Root url to use for api calls.
    pub api_url: String,
    /// Path to ping to ensure GlareDB can communicate with the cloud service.
    pub ping_path: String,
    /// System API Key authorizes the GlareDB system/background jobs with a
    /// database identifier on Cloud. This key is used for reporting meta
    /// about a database, such as storage usage and eventually limits.
    pub system_api_key: String,
    /// Request timeout.
    pub timeout: Duration,
}

impl Default for CloudConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            api_url: String::from("https://qa.glaredb.com"),
            ping_path: String::from("/healthz"),
            system_api_key: Default::default(),
            timeout: Duration::from_secs(15),
        }
    }
}

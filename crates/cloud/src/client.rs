use crate::errors::{CloudError, Result};
use common::cloud::CloudConfig;
use reqwest::Client;
use serde::Serialize;

const REPORT_STORAGE_ENDPOINT: &str = "/api/internal/databases/usage";

/// Client to the Cloud service.
#[derive(Debug)]
pub struct CloudClient {
    conf: CloudConfig,
    http: Client,
}

impl CloudClient {
    /// Try to create a new cloud client from the provided configuration.
    ///
    /// Errors if cloud communication is disabled or if the client cannot
    /// communicate with the Cloud service.
    pub async fn try_from_config(conf: CloudConfig) -> Result<CloudClient> {
        if !conf.enabled {
            return Err(CloudError::CloudCommsDisabled);
        }
        let http = Client::builder().timeout(conf.timeout).build()?;
        let client = CloudClient { conf, http };
        client.ping().await?;
        Ok(client)
    }

    /// Report storage usage to Cloud.
    pub async fn report_usage(&self, usage_bytes: u64) -> Result<()> {
        #[derive(Serialize)]
        struct Body {
            usage_bytes: u64,
        }
        let res = self
            .http
            .put(self.conf.api_url.clone() + REPORT_STORAGE_ENDPOINT)
            .header("Authorization", "Basic 6tCvEVBkD91q4KhjGVtT")
            .header("X-System-Token", &self.conf.system_api_key)
            .json(&Body { usage_bytes })
            .send()
            .await?;
        if res.status().as_u16() != 204 {
            let text = res.text().await?;
            return Err(CloudError::UnexpectedResponse(text));
        }
        Ok(())
    }

    async fn ping(&self) -> Result<()> {
        let _ = self
            .http
            .get(format!("{}/{}", self.conf.api_url, self.conf.ping_path))
            .send()
            .await?;
        Ok(())
    }
}

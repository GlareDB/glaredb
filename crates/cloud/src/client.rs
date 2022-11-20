use crate::errors::{CloudError, Result};
use common::cloud::CloudConfig;
use reqwest::Client;

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

    async fn ping(&self) -> Result<()> {
        let _ = self
            .http
            .get(format!("{}/{}", self.conf.api_url, self.conf.ping_path))
            .send()
            .await?;
        Ok(())
    }
}

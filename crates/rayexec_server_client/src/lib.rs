//! Client and type definitions to connecting to a server.
//!
//! This exists a separate crate to make the dependency graph easier to manage.
//! Specifically the execution crate and binding crates (CLI, wasm) should import
//! this crate. The server crate should import this crate and the execution crate.
pub mod types;

use rayexec_error::{RayexecError, Result, ResultExt};
use reqwest::StatusCode;
use types::MessageOwned;
use url::Url;

pub const API_VERSION: usize = 0;

pub const ENDPOINTS: Endpoints = Endpoints {
    healthz: "/healthz",
    rpc_hybrid_run: "/rpc/v0/hybrid/run",
    rpc_hybrid_push: "/rpc/v0/hybrid/push_batch",
    rpc_hybrid_pull: "/rpc/v0/hybrid/pull_batch",
};

#[derive(Debug)]
pub struct Endpoints {
    pub healthz: &'static str,
    pub rpc_hybrid_run: &'static str,
    pub rpc_hybrid_push: &'static str,
    pub rpc_hybrid_pull: &'static str,
}

#[derive(Debug, Clone)]
pub struct HybridClient {
    /// Client used to connect to remote server.
    ///
    /// Note that this isn't using our `HttpClient` trait as that's specific to
    /// reading (and eventually writing) files. Adding in arbitrary requests
    /// would complicate the interface.
    ///
    /// However some care needs to be taken with where this gets called and
    /// ensuring it can compile for wasm.
    ///
    /// This _must_ be used where we would be in the context of a tokio runtime
    /// otherwise reqwest will unfortunately panic. While tokio isn't a
    /// requirement when compiling for wasm, the code path should be common
    /// between both.
    ///
    /// Eventually Sean will write a client using just hyper allowing for
    /// whatever async runtime we want to use.
    client: reqwest::Client,

    /// URL of server we're "connected" to.
    url: Url,
}

impl HybridClient {
    pub fn new(url: Url) -> Self {
        HybridClient {
            client: reqwest::Client::default(),
            url,
        }
    }

    pub async fn ping(&self) -> Result<()> {
        let url = self
            .url
            .join(ENDPOINTS.healthz)
            .context("failed to parse healthz url")?;
        let resp = self
            .client
            .get(url)
            .send()
            .await
            .context("failed to send request")?;

        if resp.status() != StatusCode::OK {
            return Err(RayexecError::new(format!(
                "Expected 200 from healthz, got {}",
                resp.status().as_u16()
            )));
        }

        Ok(())
    }

    pub async fn do_rpc_request(&self, endpoint: &str, msg: MessageOwned) -> Result<MessageOwned> {
        let url = self.url.join(endpoint).unwrap();

        let resp = self
            .client
            .post(url)
            .json(&msg)
            .send()
            .await
            .context("failed to send request")?;

        if resp.status() != StatusCode::OK {
            return Err(RayexecError::new(format!(
                "Expected 200 from healthz, got {}",
                resp.status().as_u16()
            )));
        }

        let msg: MessageOwned = resp.json().await.context("failed to deserialize json")?;

        Ok(msg)
    }
}

use std::fmt::Debug;
use std::sync::Arc;

use glaredb_error::{DbError, Result};
use rayexec_io::http::HttpClient;

use super::time::RuntimeInstant;
use crate::io::file::FileOpener;

/// IO dependendencies.
pub trait IoRuntime: Debug + Sync + Send + Clone + 'static {
    type HttpClient: HttpClient;
    type FileProvider: FileOpener;
    type TokioHandle: TokioHandlerProvider;
    type Instant: RuntimeInstant; // TODO: Should this be on the runtime?

    /// Returns a file provider.
    fn file_provider(&self) -> Arc<Self::FileProvider>;

    /// Returns an http client. Freely cloneable.
    fn http_client(&self) -> Self::HttpClient;

    /// Return a handle to a tokio runtime if this execution runtime has a tokio
    /// runtime configured.
    ///
    /// This is needed because our native execution runtime does not depend on
    /// tokio, but certain libraries and drivers that we want to use have an
    /// unavoidable dependency on tokio.
    ///
    /// Data sources should error if they require tokio and if this returns
    /// None.
    fn tokio_handle(&self) -> &Self::TokioHandle;
}

pub trait TokioHandlerProvider {
    fn handle_opt(&self) -> Option<tokio::runtime::Handle>;

    fn handle(&self) -> Result<tokio::runtime::Handle> {
        self.handle_opt()
            .ok_or_else(|| DbError::new("Tokio runtime not configured"))
    }
}

#[derive(Debug)]
pub struct OptionalTokioRuntime(Option<tokio::runtime::Runtime>);

impl OptionalTokioRuntime {
    pub fn new(runtime: Option<tokio::runtime::Runtime>) -> Self {
        OptionalTokioRuntime(runtime)
    }
}

impl TokioHandlerProvider for OptionalTokioRuntime {
    fn handle_opt(&self) -> Option<tokio::runtime::Handle> {
        self.0.as_ref().map(|t| t.handle().clone())
    }
}

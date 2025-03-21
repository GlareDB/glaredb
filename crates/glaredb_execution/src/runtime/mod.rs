pub mod handle;
pub mod profile_buffer;
pub mod time;

use std::fmt::Debug;
use std::sync::Arc;

use glaredb_error::{DbError, Result};
use handle::QueryHandle;
use rayexec_io::http::HttpClient;
use time::RuntimeInstant;

use crate::execution::partition_pipeline::ExecutablePartitionPipeline;
use crate::io::file::FileOpener;

/// How pipelines get executed on a single node.
///
/// This is trait only concerns itself with the low-level execution of
/// pipelines. Higher level concepts like hybrid and distributed execution build
/// on top of these traits.
///
/// This will likely only ever have two implementations; one for when we're
/// executing "natively" (running pipelines on a thread pool), and one for wasm.
pub trait PipelineExecutor: Debug + Sync + Send + Clone {
    /// Number of partitions to default to when executing.
    fn default_partitions(&self) -> usize;

    /// Spawn execution of multiple pipelines for a query.
    ///
    /// A query handle will be returned allowing for canceling and dumping a
    /// query.
    ///
    /// When execution encounters an unrecoverable error, the error will be
    /// written to the provided error sink. Recoverable errors should be handled
    /// internally.
    ///
    /// This must not block.
    fn spawn_pipelines(
        &self,
        pipelines: Vec<ExecutablePartitionPipeline>,
        errors: Arc<dyn ErrorSink>,
    ) -> Arc<dyn QueryHandle>;
}

/// Runtime dependendencies.
pub trait Runtime: Debug + Sync + Send + Clone + 'static {
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

pub trait ErrorSink: Debug + Sync + Send {
    /// Push an error.
    fn set_error(&self, error: DbError);
}

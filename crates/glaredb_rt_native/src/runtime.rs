use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use glaredb_core::arrays::scalar::ScalarValue;
use glaredb_core::execution::partition_pipeline::ExecutablePartitionPipeline;
use glaredb_core::io::access::AccessConfig;
use glaredb_core::io::file::FileOpener;
use glaredb_core::runtime::io::{IoRuntime, OptionalTokioRuntime, TokioHandlerProvider};
use glaredb_core::runtime::pipeline::{ErrorSink, PipelineRuntime, QueryHandle};
use glaredb_error::{Result, ResultExt, not_implemented};

use crate::filesystem::LocalFile;
use crate::http::TokioWrappedHttpClient;
use crate::threaded::ThreadedScheduler;
use crate::time::NativeInstant;

/// Inner behavior of the execution runtime.
// TODO: Single-threaded scheduler to run our SLTs on to ensure no operators
// block without making progress. Would not be used for anything else.
pub trait Scheduler: Sync + Send + Debug + Sized + Clone {
    type Handle: QueryHandle;

    fn try_new(num_threads: usize) -> Result<Self>;

    fn num_threads(&self) -> usize;

    fn spawn_pipelines(
        &self,
        pipelines: Vec<ExecutablePartitionPipeline>,
        errors: Arc<dyn ErrorSink>,
    ) -> Self::Handle;
}

#[derive(Debug, Clone)]
pub struct NativeExecutor<S: Scheduler>(S);

impl<S: Scheduler> NativeExecutor<S> {
    pub fn try_new() -> Result<Self> {
        let threads = num_cpus::get();
        Ok(NativeExecutor(S::try_new(threads)?))
    }

    pub fn try_new_with_num_threads(num_threads: usize) -> Result<Self> {
        Ok(NativeExecutor(S::try_new(num_threads)?))
    }
}

impl<S: Scheduler + 'static> PipelineRuntime for NativeExecutor<S> {
    fn default_partitions(&self) -> usize {
        self.0.num_threads()
    }

    fn spawn_pipelines(
        &self,
        pipelines: Vec<ExecutablePartitionPipeline>,
        errors: Arc<dyn ErrorSink>,
    ) -> Arc<dyn QueryHandle> {
        let handle = self.0.spawn_pipelines(pipelines, errors);
        Arc::new(handle)
    }
}

pub type ThreadedNativeExecutor = NativeExecutor<ThreadedScheduler>;

#[derive(Debug, Clone)]
pub struct NativeRuntime {
    tokio: Arc<OptionalTokioRuntime>,
}

impl NativeRuntime {
    pub fn with_default_tokio() -> Result<Self> {
        // TODO: I had to change this to multi threaded since there was a
        // deadlock with current_thread and a single worker. I _think_ this is
        // because in main we're using the tokio runtime + block_on.
        let tokio = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_io()
            .enable_time()
            .thread_name("rayexec_tokio")
            .build()
            .context("Failed to build tokio runtime")?;

        Ok(NativeRuntime {
            tokio: Arc::new(OptionalTokioRuntime::new(Some(tokio))),
        })
    }
}

impl IoRuntime for NativeRuntime {
    type HttpClient = TokioWrappedHttpClient;
    type FileProvider = NativeFileProvider;
    type TokioHandle = OptionalTokioRuntime;
    type Instant = NativeInstant;

    fn file_provider(&self) -> Arc<Self::FileProvider> {
        Arc::new(NativeFileProvider {
            _handle: self.tokio.handle_opt(),
        })
    }

    fn http_client(&self) -> Self::HttpClient {
        // TODO: Currently not possible to construct a native runtime without
        // tokio, but it is optional...
        TokioWrappedHttpClient::new(reqwest::Client::default(), self.tokio.handle().unwrap())
    }

    fn tokio_handle(&self) -> &Self::TokioHandle {
        self.tokio.as_ref()
    }
}

#[derive(Debug, Clone)]
pub struct NativeFileProvider {
    /// For http (reqwest).
    ///
    /// If we don't have it, we return an error when attempting to access an
    /// http file.
    _handle: Option<tokio::runtime::Handle>,
}

#[derive(Debug)]
pub struct StubAccess {}

impl AccessConfig for StubAccess {
    fn from_options(
        _unnamed: &[ScalarValue],
        _named: &HashMap<String, ScalarValue>,
    ) -> Result<Self> {
        not_implemented!("access from args")
    }
}

impl FileOpener for NativeFileProvider {
    type AccessConfig = StubAccess;
    type ReadFile = LocalFile;

    async fn list_prefix(&self, _prefix: &str) -> Result<Vec<String>> {
        Ok(Vec::new())
    }

    fn open_for_read(&self, _conf: &Self::AccessConfig) -> Result<Self::ReadFile> {
        not_implemented!("open for read")
    }
}

use std::fmt::Debug;
use std::sync::Arc;

use futures::{
    stream::{self, BoxStream},
    StreamExt,
};
use rayexec_error::{not_implemented, RayexecError, Result, ResultExt};
use rayexec_execution::{
    execution::query_graph::QueryGraph,
    runtime::{ErrorSink, ExecutionRuntime, QueryHandle},
};
use rayexec_io::{
    http::HttpClientReader,
    location::{AccessConfig, FileLocation},
    s3::{S3Client, S3Location},
    FileProvider, FileSink, FileSource,
};

use crate::{
    filesystem::LocalFileSystemProvider, http::TokioWrappedHttpClient, threaded::ThreadedScheduler,
};

/// Inner behavior of the execution runtime.
// TODO: Single-threaded scheduler to run our SLTs on to ensure no operators
// block without making progress. Would not be used for anything else.
pub trait Scheduler: Sync + Send + Debug + Sized {
    type Handle: QueryHandle;

    fn try_new() -> Result<Self>;

    fn spawn_query_graph(
        &self,
        query_graph: QueryGraph,
        errors: Arc<dyn ErrorSink>,
    ) -> Self::Handle;
}

pub type ThreadedExecutionRuntime = NativeExecutionRuntime<ThreadedScheduler>;

/// Execution runtime that makes use of native threads and thread pools.
///
/// May optionally be configured with a tokio runtime _in addition_ to the
/// actual execution scheduler.
#[derive(Debug)]
pub struct NativeExecutionRuntime<S: Scheduler> {
    /// Scheduler for executing queries.
    scheduler: S,

    /// Optional tokio runtime that this execution runtime can be configured
    /// with.
    tokio: Option<Arc<tokio::runtime::Runtime>>,
}

impl<S: Scheduler> NativeExecutionRuntime<S> {
    pub fn try_new() -> Result<Self> {
        Ok(NativeExecutionRuntime {
            scheduler: S::try_new()?,
            tokio: None,
        })
    }

    pub fn with_tokio(mut self, tokio: Arc<tokio::runtime::Runtime>) -> Self {
        self.tokio = Some(tokio);
        self
    }

    pub fn with_default_tokio(mut self) -> Result<Self> {
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
        self.tokio = Some(Arc::new(tokio));
        Ok(self)
    }
}

impl<S: Scheduler + 'static> ExecutionRuntime for NativeExecutionRuntime<S> {
    fn spawn_query_graph(
        &self,
        query_graph: QueryGraph,
        errors: Arc<dyn ErrorSink>,
    ) -> Box<dyn QueryHandle> {
        let handle = self.scheduler.spawn_query_graph(query_graph, errors);
        Box::new(handle) as _
    }

    fn tokio_handle(&self) -> Option<tokio::runtime::Handle> {
        self.tokio.as_ref().map(|rt| rt.handle().clone())
    }

    fn file_provider(&self) -> Arc<dyn FileProvider> {
        Arc::new(NativeFileProvider {
            handle: self.tokio_handle(),
        })
    }
}

#[derive(Debug, Clone)]
pub struct NativeFileProvider {
    /// For http (reqwest).
    ///
    /// If we don't have it, we return an error when attempting to access an
    /// http file.
    handle: Option<tokio::runtime::Handle>,
}

impl FileProvider for NativeFileProvider {
    fn file_source(
        &self,
        location: FileLocation,
        config: &AccessConfig,
    ) -> Result<Box<dyn FileSource>> {
        match (location, config, self.handle.as_ref()) {
            (FileLocation::Url(url), AccessConfig::None, Some(handle)) => {
                let client =
                    TokioWrappedHttpClient::new(reqwest::Client::default(), handle.clone());
                Ok(Box::new(HttpClientReader::new(client, url)))
            }
            (
                FileLocation::Url(url),
                AccessConfig::S3 {
                    credentials,
                    region,
                },
                Some(handle),
            ) => {
                let client = S3Client::new(
                    TokioWrappedHttpClient::new(reqwest::Client::default(), handle.clone()),
                    credentials.clone(),
                );
                let location = S3Location::from_url(url, region)?;
                let reader = client.file_source(location, region)?;
                Ok(reader)
            }
            (FileLocation::Url(_), _, None) => Err(RayexecError::new(
                "Cannot create http client, missing tokio runtime",
            )),
            (FileLocation::Path(path), _, _) => LocalFileSystemProvider.file_source(&path),
        }
    }

    fn file_sink(
        &self,
        location: FileLocation,
        _config: &AccessConfig,
    ) -> Result<Box<dyn FileSink>> {
        match (location, self.handle.as_ref()) {
            (FileLocation::Url(_url), _) => not_implemented!("http sink native"),
            (FileLocation::Path(path), _) => LocalFileSystemProvider.file_sink(&path),
        }
    }

    fn list_prefix(
        &self,
        prefix: FileLocation,
        config: &AccessConfig,
    ) -> BoxStream<'static, Result<Vec<String>>> {
        match (prefix, config, self.handle.as_ref()) {
            (
                FileLocation::Url(url),
                AccessConfig::S3 {
                    credentials,
                    region,
                },
                Some(handle),
            ) => {
                let client = S3Client::new(
                    TokioWrappedHttpClient::new(reqwest::Client::default(), handle.clone()),
                    credentials.clone(),
                );
                let location = S3Location::from_url(url, region).unwrap(); // TODO
                let stream = client.list_prefix(location, region);
                stream.boxed()
            }
            (FileLocation::Url(_), _, _) => Box::pin(stream::once(async move {
                Err(RayexecError::new("Cannot list for http file sources"))
            })),
            (FileLocation::Path(path), _, _) => Box::pin(stream::once(async move {
                LocalFileSystemProvider.list_prefix(&path)
            })),
        }
    }
}

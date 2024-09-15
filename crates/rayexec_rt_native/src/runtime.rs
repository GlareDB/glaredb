use std::fmt::Debug;
use std::sync::Arc;

use futures::{
    stream::{self, BoxStream},
    StreamExt,
};
use rayexec_error::{not_implemented, RayexecError, Result, ResultExt};
use rayexec_execution::{
    execution::executable::pipeline::{ExecutablePartitionPipeline, ExecutablePipeline},
    runtime::{
        handle::QueryHandle, ErrorSink, OptionalTokioRuntime, PipelineExecutor, Runtime,
        TokioHandlerProvider,
    },
};
use rayexec_io::{
    http::HttpClientReader,
    location::{AccessConfig, FileLocation},
    s3::{S3Client, S3Location},
    FileProvider, FileSink, FileSource,
};

use crate::{
    filesystem::LocalFileSystemProvider, http::TokioWrappedHttpClient, threaded::ThreadedScheduler,
    time::NativeInstant,
};

/// Inner behavior of the execution runtime.
// TODO: Single-threaded scheduler to run our SLTs on to ensure no operators
// block without making progress. Would not be used for anything else.
pub trait Scheduler: Sync + Send + Debug + Sized + Clone {
    type Handle: QueryHandle;

    fn try_new(num_threads: usize) -> Result<Self>;

    fn spawn_pipelines<P>(&self, pipelines: P, errors: Arc<dyn ErrorSink>) -> Self::Handle
    where
        P: IntoIterator<Item = ExecutablePartitionPipeline>;
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

impl<S: Scheduler + 'static> PipelineExecutor for NativeExecutor<S> {
    fn spawn_pipelines(
        &self,
        pipelines: Vec<ExecutablePipeline>,
        errors: Arc<dyn ErrorSink>,
    ) -> Box<dyn QueryHandle> {
        let handle = self.0.spawn_pipelines(
            pipelines
                .into_iter()
                .flat_map(|pipeline| pipeline.into_partition_pipeline_iter()),
            errors,
        );
        Box::new(handle)
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

impl Runtime for NativeRuntime {
    type HttpClient = TokioWrappedHttpClient;
    type FileProvider = NativeFileProvider;
    type TokioHandle = OptionalTokioRuntime;
    type Instant = NativeInstant;

    fn file_provider(&self) -> Arc<Self::FileProvider> {
        Arc::new(NativeFileProvider {
            handle: self.tokio.handle_opt(),
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

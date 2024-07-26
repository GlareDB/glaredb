use futures::{
    stream::{self, BoxStream},
    StreamExt,
};
use parking_lot::Mutex;
use rayexec_error::{not_implemented, RayexecError, Result};
use rayexec_execution::{
    execution::{pipeline::PartitionPipeline, query_graph::QueryGraph},
    runtime::{
        dump::QueryDump, ErrorSink, PipelineExecutor, QueryHandle, Runtime, TokioHandlerProvider,
    },
};
use rayexec_io::{
    http::HttpClientReader,
    location::{AccessConfig, FileLocation},
    memory::MemoryFileSystem,
    s3::{S3Client, S3Location},
    FileProvider, FileSink, FileSource,
};
use std::{
    collections::BTreeMap,
    sync::Arc,
    task::{Context, Poll, Wake, Waker},
};
use tracing::debug;
use wasm_bindgen_futures::spawn_local;

use crate::http::WasmHttpClient;

#[derive(Debug, Clone)]
pub struct WasmRuntime {
    // TODO: Remove Arc? Arc already used internally for the memory fs.
    pub(crate) fs: Arc<MemoryFileSystem>,
}

impl WasmRuntime {
    pub fn try_new() -> Result<Self> {
        Ok(WasmRuntime {
            fs: Arc::new(MemoryFileSystem::default()),
        })
    }
}

impl Runtime for WasmRuntime {
    type HttpClient = WasmHttpClient;
    type FileProvider = WasmFileProvider;
    type TokioHandle = MissingTokioHandle;

    fn file_provider(&self) -> Arc<Self::FileProvider> {
        // TODO: Could probably remove this arc.
        Arc::new(WasmFileProvider {
            fs: self.fs.clone(),
        })
    }

    fn http_client(&self) -> Self::HttpClient {
        WasmHttpClient::new(reqwest::Client::default())
    }

    fn tokio_handle(&self) -> &Self::TokioHandle {
        &MissingTokioHandle
    }
}

#[derive(Debug, Clone, Copy)]
pub struct MissingTokioHandle;

impl TokioHandlerProvider for MissingTokioHandle {
    fn handle_opt(&self) -> Option<tokio::runtime::Handle> {
        None
    }
}

/// Execution scheduler for wasm.
///
/// This implementation works on a single thread which each pipeline task being
/// spawned local to the thread (using js promises under the hood).
#[derive(Debug, Clone)]
pub struct WasmExecutor;

impl PipelineExecutor for WasmExecutor {
    fn spawn_query_graph(
        &self,
        query_graph: QueryGraph,
        errors: Arc<dyn ErrorSink>,
    ) -> Box<dyn QueryHandle> {
        debug!("spawning query graph on wasm runtime");

        let states: Vec<_> = query_graph
            .into_partition_pipeline_iter()
            .map(|pipeline| WasmTaskState {
                errors: errors.clone(),
                pipeline: Arc::new(Mutex::new(pipeline)),
            })
            .collect();

        // TODO: Put references into query handle to allow canceling.

        for state in states {
            spawn_local(async move { state.execute() })
        }

        Box::new(WasmQueryHandle {})
    }
}

#[derive(Debug, Clone)]
pub struct WasmFileProvider {
    fs: Arc<MemoryFileSystem>,
}

impl FileProvider for WasmFileProvider {
    fn file_source(
        &self,
        location: FileLocation,
        config: &AccessConfig,
    ) -> Result<Box<dyn FileSource>> {
        match (location, config) {
            (FileLocation::Url(url), AccessConfig::None) => {
                let client = WasmHttpClient::new(reqwest::Client::default());
                Ok(Box::new(HttpClientReader::new(client, url)))
            }
            (
                FileLocation::Url(url),
                AccessConfig::S3 {
                    credentials,
                    region,
                },
            ) => {
                let client = S3Client::new(
                    WasmHttpClient::new(reqwest::Client::default()),
                    credentials.clone(),
                );
                let location = S3Location::from_url(url, region)?;
                let reader = client.file_source(location, region)?;
                Ok(reader)
            }
            (FileLocation::Path(path), _) => self.fs.file_source(&path),
        }
    }

    fn file_sink(
        &self,
        location: FileLocation,
        _config: &AccessConfig,
    ) -> Result<Box<dyn FileSink>> {
        match location {
            FileLocation::Url(_url) => not_implemented!("http sink wasm"),
            FileLocation::Path(path) => self.fs.file_sink(&path),
        }
    }

    fn list_prefix(
        &self,
        prefix: FileLocation,
        config: &AccessConfig,
    ) -> BoxStream<'static, Result<Vec<String>>> {
        match (prefix, config) {
            (
                FileLocation::Url(url),
                AccessConfig::S3 {
                    credentials,
                    region,
                },
            ) => {
                let client = S3Client::new(
                    WasmHttpClient::new(reqwest::Client::default()),
                    credentials.clone(),
                );
                let location = S3Location::from_url(url, region).unwrap(); // TODO
                let stream = client.list_prefix(location, region);
                stream.boxed()
            }
            (FileLocation::Url(_), AccessConfig::None) => Box::pin(stream::once(async move {
                Err(RayexecError::new("Cannot list for http file sources"))
            })),
            (FileLocation::Path(_), _) => {
                stream::once(async move { not_implemented!("wasm list fs") }).boxed()
            }
        }
    }
}

#[derive(Debug, Clone)]
struct WasmTaskState {
    errors: Arc<dyn ErrorSink>,
    pipeline: Arc<Mutex<PartitionPipeline>>,
}

impl WasmTaskState {
    fn execute(&self) {
        let state = self.clone();
        let waker: Waker = Arc::new(WasmWaker { state }).into();
        let mut cx = Context::from_waker(&waker);

        let mut pipeline = self.pipeline.lock();
        loop {
            match pipeline.poll_execute(&mut cx) {
                Poll::Ready(Some(Ok(()))) => {
                    continue;
                }
                Poll::Ready(Some(Err(e))) => {
                    self.errors.push_error(e);
                    return;
                }
                Poll::Pending => {
                    return;
                }
                Poll::Ready(None) => {
                    return;
                }
            }
        }
    }
}

#[derive(Debug)]
pub struct WasmQueryHandle {}

impl QueryHandle for WasmQueryHandle {
    fn cancel(&self) {
        // TODO
    }

    fn dump(&self) -> QueryDump {
        // TODO
        QueryDump {
            pipelines: BTreeMap::new(),
        }
    }
}

#[derive(Debug)]
struct WasmWaker {
    state: WasmTaskState,
}

impl Wake for WasmWaker {
    fn wake_by_ref(self: &Arc<Self>) {
        self.clone().wake()
    }

    fn wake(self: Arc<Self>) {
        spawn_local(async move { self.state.execute() })
    }
}

use std::collections::HashMap;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use futures::future::BoxFuture;
use glaredb_error::{Result, not_implemented};
use glaredb_execution::arrays::scalar::ScalarValue;
use glaredb_execution::execution::partition_pipeline::ExecutablePartitionPipeline;
use glaredb_execution::io::access::AccessConfig;
use glaredb_execution::io::file::FileOpener;
use glaredb_execution::io::memory::MemoryFileSource;
use glaredb_execution::runtime::handle::{ExecutionProfileData, QueryHandle};
use glaredb_execution::runtime::{ErrorSink, PipelineExecutor, Runtime, TokioHandlerProvider};
use parking_lot::Mutex;
use tracing::debug;
use wasm_bindgen_futures::spawn_local;

use crate::http::WasmHttpClient;
use crate::time::PerformanceInstant;

#[derive(Debug, Clone)]
pub struct WasmRuntime {
    // TODO: Shared memory fs
}

impl WasmRuntime {
    pub fn try_new() -> Result<Self> {
        Ok(WasmRuntime {})
    }
}

impl Runtime for WasmRuntime {
    type HttpClient = WasmHttpClient;
    type FileProvider = WasmFileProvider;
    type TokioHandle = MissingTokioHandle;
    type Instant = PerformanceInstant;

    fn file_provider(&self) -> Arc<Self::FileProvider> {
        // TODO: Could probably remove this arc.
        Arc::new(WasmFileProvider {})
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
    fn default_partitions(&self) -> usize {
        1
    }

    fn spawn_pipelines(
        &self,
        pipelines: Vec<ExecutablePartitionPipeline>,
        errors: Arc<dyn ErrorSink>,
    ) -> Arc<dyn QueryHandle> {
        debug!("spawning query graph on wasm runtime");

        let states: Vec<_> = pipelines
            .into_iter()
            .map(|pipeline| WasmTaskState {
                errors: errors.clone(),
                pipeline: Arc::new(Mutex::new(pipeline)),
            })
            .collect();

        // TODO: Put references into query handle to allow canceling.

        for state in &states {
            let state = state.clone();
            spawn_local(async move { state.execute() })
        }

        Arc::new(WasmQueryHandle { states })
    }
}

#[derive(Debug, Clone)]
struct WasmTaskState {
    errors: Arc<dyn ErrorSink>,
    pipeline: Arc<Mutex<ExecutablePartitionPipeline>>,
}

impl WasmTaskState {
    fn execute(&self) {
        let state = self.clone();
        let waker: Waker = Arc::new(WasmWaker { state }).into();
        let mut cx = Context::from_waker(&waker);

        let mut pipeline = self.pipeline.lock();
        match pipeline.poll_execute::<PerformanceInstant>(&mut cx) {
            Poll::Ready(Ok(())) => {
                // Pushing through the pipeline was successful.
            }
            Poll::Ready(Err(e)) => {
                self.errors.set_error(e);
            }
            Poll::Pending => {
                // Exit the loop. Waker was already stored in the pending
                // sink/source, we'll be woken back up when there's more
                // this operator chain can start executing.
            }
        }
    }
}

#[derive(Debug)]
pub struct WasmQueryHandle {
    #[allow(unused)]
    states: Vec<WasmTaskState>,
}

impl QueryHandle for WasmQueryHandle {
    fn cancel(&self) {
        // TODO
    }

    fn generate_execution_profile_data(&self) -> BoxFuture<'_, Result<ExecutionProfileData>> {
        Box::pin(async { not_implemented!("execution profile data") })
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

#[derive(Debug, Clone)]
pub struct WasmFileProvider {}

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

impl FileOpener for WasmFileProvider {
    type AccessConfig = StubAccess;
    type ReadFile = MemoryFileSource;

    async fn list_prefix(&self, _prefix: &str) -> Result<Vec<String>> {
        Ok(Vec::new())
    }

    fn open_for_read(&self, _conf: &Self::AccessConfig) -> Result<Self::ReadFile> {
        not_implemented!("open for read")
    }
}

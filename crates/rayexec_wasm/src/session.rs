use std::rc::Rc;

use glaredb_error::RayexecError;
use glaredb_execution::arrays::batch::Batch;
use glaredb_execution::arrays::field::ColumnSchema;
use glaredb_execution::engine::single_user::SingleUserEngine;
use tracing::trace;
use wasm_bindgen::prelude::*;

use crate::errors::Result;
use crate::runtime::{WasmExecutor, WasmRuntime};

#[wasm_bindgen]
#[derive(Debug)]
pub struct WasmSession {
    #[allow(unused)]
    pub(crate) runtime: WasmRuntime,
    #[allow(unused)]
    pub(crate) engine: SingleUserEngine<WasmExecutor, WasmRuntime>,
}

#[wasm_bindgen]
impl WasmSession {
    pub fn try_new() -> Result<WasmSession> {
        let runtime = WasmRuntime::try_new()?;
        let engine = SingleUserEngine::try_new(WasmExecutor, runtime.clone())?;

        Ok(WasmSession { runtime, engine })
    }

    pub async fn query(&self, _sql: &str) -> Result<WasmQueryResults> {
        Err(RayexecError::new("query not implemented").into())
    }

    // TODO: This copies `content`. Not sure if there's a good way to get around
    // that.
    //
    // Unsure if the clippy lint failure is because content is unused right now.
    // If for sure needs to be boxed to cross the wasm boundary.
    #[allow(clippy::boxed_local)]
    pub fn register_file(&self, name: String, _content: Box<[u8]>) -> Result<()> {
        trace!(%name, "registering local file with runtime");
        Err(RayexecError::new("register file not implemented").into())
    }

    /// Return a list of registered file names.
    ///
    /// Names will be sorted alphabetically.
    pub fn list_local_files(&self) -> Vec<String> {
        Vec::new()
    }

    pub async fn connect_hybrid(&self, _connection_string: String) -> Result<()> {
        Err(RayexecError::new("connect hybrid not implemented").into())
    }

    pub fn version(&self) -> String {
        env!("CARGO_PKG_VERSION").to_string()
    }
}

/// Wrapper around result tables.
///
/// This intermediate type is needed since wasm_bindgen can't generate the
/// appropriate type for `Result<Vec<T>, E>`, otherwise we'd just return these
/// tables directly.
#[wasm_bindgen]
#[derive(Debug)]
#[allow(unused)]
pub struct WasmQueryResults(pub(crate) Vec<Rc<WasmQueryResult>>);

impl WasmQueryResults {}

#[wasm_bindgen]
#[derive(Debug)]
#[allow(unused)]
pub struct WasmQueryResult {
    pub(crate) schema: ColumnSchema,
    pub(crate) batches: Vec<Batch>,
}

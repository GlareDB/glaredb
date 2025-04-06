use std::rc::Rc;

use ext_csv::extension::CsvExtension;
use ext_parquet::extension::ParquetExtension;
use ext_spark::SparkExtension;
use glaredb_core::arrays::batch::Batch;
use glaredb_core::arrays::field::ColumnSchema;
use glaredb_core::engine::single_user::SingleUserEngine;
use glaredb_error::DbError;
use tracing::trace;
use wasm_bindgen::prelude::*;

use crate::errors::Result;
use crate::runtime::{WasmExecutor, WasmSystemRuntime};

#[wasm_bindgen]
#[derive(Debug)]
pub struct WasmSession {
    #[allow(unused)]
    pub(crate) runtime: WasmSystemRuntime,
    #[allow(unused)]
    pub(crate) engine: SingleUserEngine<WasmExecutor, WasmSystemRuntime>,
}

#[wasm_bindgen]
impl WasmSession {
    pub fn try_new() -> Result<WasmSession> {
        let runtime = WasmSystemRuntime::try_new()?;
        let engine = SingleUserEngine::try_new(WasmExecutor, runtime.clone())?;
        engine.register_extension(SparkExtension)?;
        engine.register_extension(CsvExtension)?;
        engine.register_extension(ParquetExtension)?;

        Ok(WasmSession { runtime, engine })
    }

    pub async fn query(&self, _sql: &str) -> Result<WasmQueryResults> {
        Err(DbError::new("query not implemented").into())
    }

    // TODO: This copies `content`. Not sure if there's a good way to get around
    // that.
    //
    // Unsure if the clippy lint failure is because content is unused right now.
    // If for sure needs to be boxed to cross the wasm boundary.
    #[allow(clippy::boxed_local)]
    pub fn register_file(&self, name: String, _content: Box<[u8]>) -> Result<()> {
        trace!(%name, "registering local file with runtime");
        Err(DbError::new("register file not implemented").into())
    }

    /// Return a list of registered file names.
    ///
    /// Names will be sorted alphabetically.
    pub fn list_local_files(&self) -> Vec<String> {
        Vec::new()
    }

    pub async fn connect_hybrid(&self, _connection_string: String) -> Result<()> {
        Err(DbError::new("connect hybrid not implemented").into())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_session() {
        // Ensure we can create a session.
        //
        // The reason for this test case is to ensure we can create the system
        // catalog and registery any extensions without erroring.
        //
        // It does not test that we can actually run anything (since running
        // would attempt to create promises).
        let _ = WasmSession::try_new().unwrap();
    }
}

use std::{path::PathBuf, rc::Rc, sync::Arc};

use crate::{errors::Result, runtime::WasmExecutionRuntime};
use rayexec_bullet::format::{FormatOptions, Formatter};
use rayexec_error::RayexecError;
use rayexec_execution::datasource::{DataSourceRegistry, MemoryDataSource};
use rayexec_parquet::ParquetDataSource;
use rayexec_shell::session::{ResultTable, SingleUserEngine};
use tracing::trace;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
#[derive(Debug)]
pub struct WasmSession {
    pub(crate) runtime: Arc<WasmExecutionRuntime>,
    pub(crate) engine: SingleUserEngine,
}

#[wasm_bindgen]
impl WasmSession {
    pub fn try_new() -> Result<WasmSession> {
        let runtime = Arc::new(WasmExecutionRuntime::try_new()?);
        let registry = DataSourceRegistry::default()
            .with_datasource("memory", Box::new(MemoryDataSource))?
            .with_datasource("parquet", Box::new(ParquetDataSource))?;

        let engine = SingleUserEngine::new_with_runtime(runtime.clone(), registry)?;

        Ok(WasmSession { runtime, engine })
    }

    pub async fn sql(&self, sql: &str) -> Result<WasmResultTables> {
        let tables = self
            .engine
            .sql(sql)
            .await?
            .into_iter()
            .map(Rc::new)
            .collect();
        Ok(WasmResultTables(tables))
    }

    // TODO: This copies `content`. Not sure if there's a good way to get around
    // that.
    pub fn register_file(&self, name: String, content: Box<[u8]>) -> Result<()> {
        trace!(%name, "registering local file with runtime");
        self.runtime
            .fs
            .register_file(&PathBuf::from(name), content.into())?;
        Ok(())
    }

    /// Return a list of registered file names.
    ///
    /// Names will be sorted alphabetically.
    pub fn list_local_files(&self) -> Vec<String> {
        let mut names = self.runtime.fs.list_files();
        names.sort();
        names
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
pub struct WasmResultTables(pub(crate) Vec<Rc<ResultTable>>);

#[wasm_bindgen]
impl WasmResultTables {
    pub fn get_tables(&self) -> Vec<WasmResultTable> {
        self.0
            .iter()
            .map(|table| {
                let mut first_row_indices = Vec::with_capacity(table.batches.len());
                let mut curr_idx = 0;
                for batch in &table.batches {
                    first_row_indices.push(curr_idx);
                    curr_idx += batch.num_rows();
                }

                WasmResultTable {
                    table: table.clone(),
                    first_row_indices,
                }
            })
            .collect()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[wasm_bindgen]
#[derive(Debug)]
pub struct WasmResultTable {
    /// Result table for a single query. The result table may contain more than
    /// one batch.
    pub(crate) table: Rc<ResultTable>,
    /// Row indices for where each batch starts.
    ///
    /// For example, the 0th batch has a row start index of 0, the 1st batch
    /// will have an index of 0+len(batches[0]), and so on.
    ///
    /// Storing these indices allow us to not have to concat batches into a
    /// single large batch.
    pub(crate) first_row_indices: Vec<usize>,
}

#[wasm_bindgen]
impl WasmResultTable {
    pub fn column_names(&self) -> Vec<String> {
        self.table
            .schema
            .fields
            .iter()
            .map(|f| f.name.clone())
            .collect()
    }

    pub fn num_rows(&self) -> usize {
        self.table.batches.iter().map(|b| b.num_rows()).sum()
    }

    pub fn format_cell(&self, col: usize, row: usize) -> Result<String> {
        const FORMATTER: Formatter = Formatter::new(FormatOptions::new());

        let batch_idx = self
            .first_row_indices
            .iter()
            .position(|&idx| row >= idx)
            .unwrap();

        let arr = self.table.batches[batch_idx]
            .column(col)
            .ok_or_else(|| RayexecError::new(format!("Column index {col} out of range")))?;

        let v = FORMATTER
            .format_array_value(arr, row - self.first_row_indices[batch_idx])
            .ok_or_else(|| RayexecError::new(format!("Row index {row} out of range")))?;

        Ok(v.to_string())
    }
}

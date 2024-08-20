use std::{path::PathBuf, rc::Rc, sync::Arc};

use crate::{
    errors::Result,
    runtime::{WasmExecutor, WasmRuntime},
};
use rayexec_bullet::{
    array::Array,
    format::{FormatOptions, Formatter},
};
use rayexec_csv::CsvDataSource;
use rayexec_delta::DeltaDataSource;
use rayexec_error::RayexecError;
use rayexec_execution::datasource::{DataSourceBuilder, DataSourceRegistry, MemoryDataSource};
use rayexec_parquet::ParquetDataSource;
use rayexec_shell::session::{ResultTable, SingleUserEngine};
use tracing::trace;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
#[derive(Debug)]
pub struct WasmSession {
    pub(crate) runtime: WasmRuntime,
    pub(crate) engine: SingleUserEngine<WasmExecutor, WasmRuntime>,
}

#[wasm_bindgen]
impl WasmSession {
    pub fn try_new() -> Result<WasmSession> {
        let runtime = WasmRuntime::try_new()?;
        let registry = DataSourceRegistry::default()
            .with_datasource("memory", Box::new(MemoryDataSource))?
            .with_datasource("parquet", ParquetDataSource::initialize(runtime.clone()))?
            .with_datasource("csv", CsvDataSource::initialize(runtime.clone()))?
            .with_datasource("delta", DeltaDataSource::initialize(runtime.clone()))?;

        let engine = SingleUserEngine::try_new(WasmExecutor, runtime.clone(), registry)?;

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

    pub async fn connect_hybrid(&self, connection_string: String) -> Result<()> {
        self.engine.connect_hybrid(connection_string).await?;
        Ok(())
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
            .map(|table| WasmResultTable::new(table.clone()))
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
}

#[wasm_bindgen]
impl WasmResultTable {
    /// Wraps a result table for wasm.
    ///
    /// Generates first row indices for each batch in the result table.
    fn new(table: Rc<ResultTable>) -> Self {
        WasmResultTable { table }
    }

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

    #[inline]
    fn batch_row(&self, mut row: usize) -> (usize, usize) {
        for (batch_idx, batch) in self.table.batches.iter().enumerate() {
            if row < batch.num_rows() {
                return (batch_idx, row);
            }
            row -= batch.num_rows();
        }
        (0, 0)
    }

    pub fn column(&self, column: &str) -> Result<WasmArray> {
        let col_idx = self
            .table
            .schema
            .fields
            .iter()
            .position(|f| f.name == column)
            .ok_or_else(|| {
                RayexecError::new(format!(
                    "Unable to find column with name '{column}' in results table"
                ))
            })?;

        let arrays = self
            .table
            .batches
            .iter()
            .map(|b| b.column(col_idx).expect("column to exist").clone())
            .collect();

        Ok(WasmArray { arrays })
    }

    pub fn format_cell(&self, col: usize, row: usize) -> Result<String> {
        const FORMATTER: Formatter = Formatter::new(FormatOptions::new());

        let (batch_idx, row) = self.batch_row(row);

        let arr = self.table.batches[batch_idx]
            .column(col)
            .ok_or_else(|| RayexecError::new(format!("Column index {col} out of range")))?;

        let v = FORMATTER
            .format_array_value(arr, row)
            .ok_or_else(|| RayexecError::new(format!("Row index {row} out of range")))?;

        Ok(v.to_string())
    }
}

#[wasm_bindgen]
pub struct WasmArray {
    arrays: Vec<Arc<Array>>,
}

#[wasm_bindgen]
impl WasmArray {
    pub fn len(&self) -> usize {
        self.arrays.iter().map(|a| a.len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn find_array_for_row(&self, mut row: usize) -> Result<(&Array, usize)> {
        for array in self.arrays.iter() {
            if row < array.len() {
                return Ok((array, row));
            }
            row -= array.len();
        }
        Err(RayexecError::new(format!("Row index {row} out of bounds")).into())
    }

    pub fn value_as_string(&self, row_idx: usize) -> Result<Option<String>> {
        const FORMATTER: Formatter = Formatter::new(FormatOptions::new());

        let (arr, row_idx) = self.find_array_for_row(row_idx)?;
        let valid = arr.is_valid(row_idx).expect("row in bounds");

        if valid {
            Ok(Some(
                FORMATTER
                    .format_array_value(arr, row_idx)
                    .unwrap()
                    .to_string(),
            ))
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use rayexec_bullet::{
        array::{Array, Int32Array},
        batch::Batch,
        datatype::DataType,
        field::{Field, Schema},
    };

    use super::*;

    #[test]
    fn format_cells() {
        let table = ResultTable {
            schema: Schema::new([Field::new("c1", DataType::Int32, true)]),
            batches: vec![
                Batch::try_new([Array::Int32(Int32Array::from_iter([0, 1, 2, 3]))]).unwrap(),
                Batch::try_new([Array::Int32(Int32Array::from_iter([4, 5]))]).unwrap(),
                Batch::try_new([Array::Int32(Int32Array::from_iter([6, 7, 8, 9, 10]))]).unwrap(),
            ],
        };

        let table = WasmResultTable::new(Rc::new(table));

        // From first batch.
        assert_eq!("0", table.format_cell(0, 0).unwrap());
        assert_eq!("1", table.format_cell(0, 1).unwrap());

        // From second batch.
        assert_eq!("4", table.format_cell(0, 4).unwrap());
        assert_eq!("5", table.format_cell(0, 5).unwrap());

        // Last batch.
        assert_eq!("6", table.format_cell(0, 6).unwrap());
        assert_eq!("10", table.format_cell(0, 10).unwrap());
    }
}
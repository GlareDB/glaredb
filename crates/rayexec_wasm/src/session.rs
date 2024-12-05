use std::path::PathBuf;
use std::rc::Rc;

use rayexec_bullet::format::{FormatOptions, Formatter};
use rayexec_csv::CsvDataSource;
use rayexec_delta::DeltaDataSource;
use rayexec_execution::datasource::{DataSourceBuilder, DataSourceRegistry, MemoryDataSource};
use rayexec_iceberg::IcebergDataSource;
use rayexec_parquet::ParquetDataSource;
use rayexec_shell::result_table::{MaterializedColumn, MaterializedResultTable};
use rayexec_shell::session::SingleUserEngine;
use rayexec_unity_catalog::UnityCatalogDataSource;
use tracing::trace;
use wasm_bindgen::prelude::*;

use crate::errors::Result;
use crate::runtime::{WasmExecutor, WasmRuntime};

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
            .with_datasource("delta", DeltaDataSource::initialize(runtime.clone()))?
            .with_datasource("unity", UnityCatalogDataSource::initialize(runtime.clone()))?
            .with_datasource("iceberg", IcebergDataSource::initialize(runtime.clone()))?;

        let engine = SingleUserEngine::try_new(WasmExecutor, runtime.clone(), registry)?;

        Ok(WasmSession { runtime, engine })
    }

    pub async fn query(&self, sql: &str) -> Result<WasmMaterializedResultTables> {
        let pending_queries = self.engine.session().query_many(sql)?;
        let mut tables = Vec::with_capacity(pending_queries.len());

        for pending in pending_queries {
            let table = pending.execute().await?.collect().await?;
            tables.push(Rc::new(table));
        }

        Ok(WasmMaterializedResultTables(tables))
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
pub struct WasmMaterializedResultTables(pub(crate) Vec<Rc<MaterializedResultTable>>);

#[wasm_bindgen]
impl WasmMaterializedResultTables {
    pub fn get_tables(&self) -> Vec<WasmMaterializedResultTable> {
        self.0
            .iter()
            .map(|table| WasmMaterializedResultTable(table.clone()))
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
pub struct WasmMaterializedResultTable(pub(crate) Rc<MaterializedResultTable>);

#[wasm_bindgen]
impl WasmMaterializedResultTable {
    pub fn column_names(&self) -> Vec<String> {
        self.0
            .schema()
            .fields
            .iter()
            .map(|f| f.name.clone())
            .collect()
    }

    pub fn num_rows(&self) -> usize {
        self.0.num_rows()
    }

    pub fn column(&self, column: &str) -> Result<WasmMaterializedColumn> {
        Ok(WasmMaterializedColumn(self.0.column_by_name(column)?))
    }

    pub fn format_cell(&self, col: usize, row: usize) -> Result<String> {
        const FORMATTER: Formatter = Formatter::new(FormatOptions::new());
        let v = self.0.with_cell(
            |arr, row| {
                FORMATTER
                    .format_array_value(arr, row)
                    .map(|v| v.to_string())
            },
            col,
            row,
        )?;

        Ok(v)
    }
}

#[wasm_bindgen]
pub struct WasmMaterializedColumn(pub(crate) MaterializedColumn);

#[wasm_bindgen]
impl WasmMaterializedColumn {
    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn value_as_string(&self, row_idx: usize) -> Result<Option<String>> {
        const FORMATTER: Formatter = Formatter::new(FormatOptions::new());
        let v = self.0.with_row(
            |arr, row| {
                let valid = arr.is_valid(row).expect("row in bounds");
                if valid {
                    Ok(Some(
                        FORMATTER.format_array_value(arr, row).unwrap().to_string(),
                    ))
                } else {
                    Ok(None)
                }
            },
            row_idx,
        )?;

        Ok(v)
    }
}

#[cfg(test)]
mod tests {
    use rayexec_bullet::array::Array;
    use rayexec_bullet::batch::Batch;
    use rayexec_bullet::datatype::DataType;
    use rayexec_bullet::field::{Field, Schema};

    use super::*;

    #[test]
    fn format_cells() {
        let table = MaterializedResultTable::try_new(
            Schema::new([Field::new("c1", DataType::Int32, true)]),
            [
                Batch::try_new([Array::from_iter([0, 1, 2, 3])]).unwrap(),
                Batch::try_new([Array::from_iter([4, 5])]).unwrap(),
                Batch::try_new([Array::from_iter([6, 7, 8, 9, 10])]).unwrap(),
            ],
        )
        .unwrap();

        let table = WasmMaterializedResultTable(Rc::new(table));

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

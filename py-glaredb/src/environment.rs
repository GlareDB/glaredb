use datafusion::datasource::MemTable;
use datafusion::{
    arrow::{pyarrow::PyArrowType, record_batch::RecordBatch},
    datasource::TableProvider,
};
use pyo3::{prelude::*, types::PyType};
use sqlexec::environment::EnvironmentReader;
use std::sync::Arc;

/// Read polars dataframes from the python environment.
#[derive(Debug, Clone, Copy)]
pub struct PyEnvironmentReader;

impl EnvironmentReader for PyEnvironmentReader {
    fn resolve_table(
        &self,
        name: &str,
    ) -> Result<Option<Arc<dyn TableProvider>>, Box<dyn std::error::Error + Send + Sync>> {
        Python::with_gil(|py| {
            // Currently assuming any error getting the variable just means
            // there's no variable with that name.
            let var = match py.eval(name, None, None) {
                Ok(var) => var,
                Err(_) => return Ok(None),
            };

            if let Some(table) = resolve_polars(py, var).map_err(Box::new)? {
                return Ok(Some(table));
            }

            // TODO: Other data frame types.

            // Variable isn't one of the data frames that we support.
            Ok(None)
        })
    }
}

/// Try to resolve a variable as a polars data frame.
///
/// Returns `Ok(None)` if the variable isn't a polars data frame.
fn resolve_polars(py: Python, var: &PyAny) -> PyResult<Option<Arc<dyn TableProvider>>> {
    let polars_type: &PyType = py
        .import("polars")?
        .getattr("DataFrame")?
        .downcast()
        .unwrap();

    if !var.is_instance(polars_type).unwrap() {
        return Ok(None);
    }

    let arrow = var.call_method0("to_arrow").unwrap();
    let batches = arrow.call_method0("to_batches").unwrap();
    let batches = batches.extract::<PyArrowType<Vec<RecordBatch>>>().unwrap();
    let batches = batches.0;

    let schema = batches[0].schema();

    let table = MemTable::try_new(schema, vec![batches]).unwrap();

    Ok(Some(Arc::new(table) as Arc<dyn TableProvider>))
}

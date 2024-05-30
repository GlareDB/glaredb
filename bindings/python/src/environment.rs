use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::pyarrow::PyArrowType;
use datafusion::datasource::{MemTable, TableProvider};
use pyo3::prelude::*;
use pyo3::types::{IntoPyDict, PyTuple, PyType};
use glaredb::EnvironmentReader;

use crate::execution::PyExecutionOutput;

/// Read polars dataframes from the python environment.
#[derive(Debug, Clone, Copy)]
pub struct PyEnvironmentReader;

impl EnvironmentReader for PyEnvironmentReader {
    fn resolve_table(
        &self,
        name: &str,
    ) -> Result<Option<Arc<dyn TableProvider>>, Box<dyn std::error::Error + Send + Sync>> {
        Python::with_gil(|py| {
            let var = match get_stack_locals(py, name) {
                Ok(Some(var)) => var,
                _ => return Ok(None),
            };

            // since the resolve functions will err if the library is uninstalled,
            // dont `try` the results, we want to move on next resolver if this one errs.
            if let Ok(Some(table)) = resolve_polars(py, var) {
                return Ok(Some(table));
            }
            if let Ok(Some(table)) = resolve_polars_lazy(py, var) {
                return Ok(Some(table));
            }
            if let Ok(Some(table)) = resolve_pandas(py, var) {
                return Ok(Some(table));
            }
            if let Ok(Some(tbl)) = resolve_logical_plan(py, var) {
                return Ok(Some(tbl));
            }

            Ok(None)
        })
    }
}

/// Search for a python variable in the current frame, or any parent frame.
fn get_stack_locals<'py>(py: Python<'py>, name: &str) -> PyResult<Option<&'py PyAny>> {
    let mut current_frame = py.import("inspect")?.getattr("currentframe")?.call0()?;

    loop {
        if !current_frame.hasattr("f_locals")? {
            // No more local scope to search. Do a final check in the global
            // scope since we may have been called at the top-level.
            if let Ok(var) = current_frame.getattr("f_globals")?.get_item(name) {
                return Ok(Some(var));
            }

            // Nothing found.
            return Ok(None);
        }

        // Search locals.
        if let Ok(var) = current_frame.getattr("f_locals")?.get_item(name) {
            return Ok(Some(var));
        }

        // Search globals.
        if let Ok(var) = current_frame.getattr("f_globals")?.get_item(name) {
            return Ok(Some(var));
        }

        // Go back a frame.
        current_frame = current_frame.getattr("f_back")?;
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

/// Try to resolve a variable as a polars lazy data frame.
///
/// Returns `Ok(None)` if the variable isn't a lazy polars data frame.
fn resolve_polars_lazy(py: Python, var: &PyAny) -> PyResult<Option<Arc<dyn TableProvider>>> {
    // Polars seems to lack streaming output for lazy frames in the python api.
    // So we collect it all in memory!

    let polars_type: &PyType = py
        .import("polars")?
        .getattr("LazyFrame")?
        .downcast()
        .unwrap();

    if !var.is_instance(polars_type).unwrap() {
        return Ok(None);
    }

    let kwargs = &[("streaming", true)];
    let df = var.call_method("collect", (), Some(kwargs.into_py_dict(py)))?;

    let arrow = df.call_method0("to_arrow").unwrap();
    let batches = arrow.call_method0("to_batches").unwrap();
    let batches = batches.extract::<PyArrowType<Vec<RecordBatch>>>().unwrap();
    let batches = batches.0;

    let schema = batches[0].schema();

    let table = MemTable::try_new(schema, vec![batches]).unwrap();

    Ok(Some(Arc::new(table) as Arc<dyn TableProvider>))
}

/// Try to resolve a variable as a pandas data frame.
///
/// Returns `Ok(None)` if the variable isn't a pandas data frame.
fn resolve_pandas(py: Python, var: &PyAny) -> PyResult<Option<Arc<dyn TableProvider>>> {
    let pandas_type: &PyType = py
        .import("pandas")?
        .getattr("DataFrame")?
        .downcast()
        .unwrap();

    if !var.is_instance(pandas_type).unwrap() {
        return Ok(None);
    }

    let args = PyTuple::new(py, [var]);
    let table: PyObject = py
        .import("pyarrow")?
        .getattr("Table")?
        .call_method1("from_pandas", args)?
        .into();

    let batches = table.call_method0(py, "to_batches")?;
    let batches = batches.extract::<PyArrowType<Vec<RecordBatch>>>(py)?;
    let batches = batches.0;

    let schema = batches[0].schema();

    let table = MemTable::try_new(schema, vec![batches]).unwrap();

    Ok(Some(Arc::new(table) as Arc<dyn TableProvider>))
}

fn resolve_logical_plan(_py: Python, var: &PyAny) -> PyResult<Option<Arc<dyn TableProvider>>> {
    let exec: PyExecutionOutput = var.extract()?;
    Ok(Some(Arc::new(exec) as Arc<dyn TableProvider>))
}

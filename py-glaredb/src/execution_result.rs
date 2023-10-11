use crate::util::pyprint;
use anyhow::Result;
use arrow_util::pretty::pretty_format_batches;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::pyarrow::ToPyArrow;
use datafusion::arrow::record_batch::RecordBatch;
use futures::StreamExt;
use pyo3::{exceptions::PyRuntimeError, prelude::*, types::PyTuple};
use sqlexec::session::ExecutionResult;
use std::sync::Arc;

use crate::runtime::wait_for_future;

/// The result of an executed query.
#[pyclass]
pub struct PyExecutionResult(pub ExecutionResult);

#[pymethods]
impl PyExecutionResult {
    /// Convert to Arrow Table
    /// Collect the batches and pass to Arrow Table
    #[allow(clippy::wrong_self_convention)] // this is consistent with other python API's
    pub fn to_arrow(&mut self, py: Python) -> PyResult<PyObject> {
        let (batches, schema) = to_arrow_batches_and_schema(&mut self.0, py)?;

        Python::with_gil(|py| {
            // Instantiate pyarrow Table object and use its from_batches method
            let table_class = py.import("pyarrow")?.getattr("Table")?;
            let args = PyTuple::new(py, &[batches, schema]);
            let table: PyObject = table_class.call_method1("from_batches", args)?.into();
            Ok(table)
        })
    }

    #[allow(clippy::wrong_self_convention)] // this is consistent with other python API's
    pub fn to_polars(&mut self, py: Python) -> PyResult<PyObject> {
        let (batches, schema) = to_arrow_batches_and_schema(&mut self.0, py)?;

        Python::with_gil(|py| {
            let table_class = py.import("pyarrow")?.getattr("Table")?;
            let args = PyTuple::new(py, &[batches, schema]);
            let table: PyObject = table_class.call_method1("from_batches", args)?.into();

            let table_class = py.import("polars")?.getattr("DataFrame")?;
            let args = PyTuple::new(py, &[table]);
            let result = table_class.call1(args)?.into();
            Ok(result)
        })
    }

    #[allow(clippy::wrong_self_convention)] // this is consistent with other python API's
    pub fn to_pandas(&mut self, py: Python) -> PyResult<PyObject> {
        let (batches, schema) = to_arrow_batches_and_schema(&mut self.0, py)?;

        Python::with_gil(|py| {
            let table_class = py.import("pyarrow")?.getattr("Table")?;
            let args = PyTuple::new(py, &[batches, schema]);
            let table: PyObject = table_class.call_method1("from_batches", args)?.into();

            let result = table.call_method0(py, "to_pandas")?;
            Ok(result)
        })
    }

    pub fn execute(&mut self, py: Python) -> PyResult<()> {
        match &mut self.0 {
            ExecutionResult::Query { stream, .. } => wait_for_future(py, async move {
                while let Some(r) = stream.next().await {
                    let _ = r?;
                }
                Ok(())
            }),
            _ => Ok(()),
        }
    }

    pub fn show(&mut self, py: Python) -> PyResult<()> {
        print_batch(&mut self.0, py)?;
        Ok(())
    }
}

fn to_arrow_batches_and_schema(
    result: &mut ExecutionResult,
    py: Python<'_>,
) -> PyResult<(PyObject, PyObject)> {
    match result {
        ExecutionResult::Query { stream, .. } => {
            let batches: Result<Vec<RecordBatch>> = wait_for_future(py, async move {
                Ok(stream
                    .collect::<Vec<_>>()
                    .await
                    .into_iter()
                    .collect::<Result<Vec<_>, _>>()?)
            });

            let batches = batches
                .map_err(|e| PyRuntimeError::new_err(format!("unhandled exception: {:?}", &e)))?;
            let schema = batches[0].schema().to_pyarrow(py)?;

            // TODO: currently we are iterating twice due to the GIL lock
            // we can't use `to_pyarrow` within an async block.
            let batches = batches
                .into_iter()
                .map(|batch| batch.to_pyarrow(py))
                .collect::<Result<Vec<_>, _>>()?
                .to_object(py);

            Ok((batches, schema))
        }
        _ => {
            // TODO: Figure out the schema we actually want to use.
            let schema = Arc::new(Schema::empty());
            let batches = vec![RecordBatch::new_empty(schema.clone()).to_pyarrow(py)]
                .into_iter()
                .collect::<Result<Vec<_>, _>>()?
                .to_object(py);
            Ok((batches, schema.to_pyarrow(py)?))
        }
    }
}

fn print_batch(result: &mut ExecutionResult, py: Python<'_>) -> PyResult<()> {
    match result {
        ExecutionResult::Query { stream, .. } => {
            let schema = stream.schema();
            let batches = wait_for_future(py, async move {
                stream
                    .collect::<Vec<_>>()
                    .await
                    .into_iter()
                    .collect::<Result<Vec<RecordBatch>, _>>()
            })?;

            let disp = pretty_format_batches(&schema, &batches, None, None)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

            pyprint(disp, py)
        }
        _ => Err(PyRuntimeError::new_err("Not able to show executed result")),
    }
}

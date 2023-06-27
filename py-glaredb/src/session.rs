use anyhow::Result;
use datafusion::arrow::record_batch::RecordBatch;
use futures::StreamExt;
use pgrepr::format::Format;
use pyo3::{exceptions::PyRuntimeError, prelude::*, types::PyTuple};
use sqlexec::{
    engine::{Engine, TrackedSession},
    parser,
    session::ExecutionResult,
};

use crate::{error::PyGlareDbError, runtime::wait_for_future};

#[pyclass]
pub struct LocalSession {
    pub(super) sess: TrackedSession,
    pub(super) _engine: Engine, // Avoid dropping
}

#[pyclass]
pub struct PyExecutionResult(ExecutionResult);

impl From<ExecutionResult> for PyExecutionResult {
    fn from(result: ExecutionResult) -> Self {
        Self(result)
    }
}

#[pymethods]
impl LocalSession {
    fn sql(&mut self, py: Python<'_>, query: &str) -> PyResult<PyExecutionResult> {
        const UNNAMED: String = String::new();

        let mut statements = parser::parse_sql(query).map_err(PyGlareDbError::from)?;

        wait_for_future(py, async move {
            match statements.len() {
                0 => todo!(),
                1 => {
                    let stmt = statements.pop_front().unwrap();

                    self.sess
                        .prepare_statement(UNNAMED, Some(stmt), Vec::new())
                        .await
                        .map_err(PyGlareDbError::from)?;
                    let prepared = self
                        .sess
                        .get_prepared_statement(&UNNAMED)
                        .map_err(PyGlareDbError::from)?;
                    let num_fields = prepared.output_fields().map(|f| f.len()).unwrap_or(0);
                    self.sess
                        .bind_statement(
                            UNNAMED,
                            &UNNAMED,
                            Vec::new(),
                            vec![Format::Text; num_fields],
                        )
                        .map_err(PyGlareDbError::from)?;
                    Ok(self
                        .sess
                        .execute_portal(&UNNAMED, 0)
                        .await
                        .map_err(PyGlareDbError::from)?
                        .into())
                }
                _ => {
                    todo!()
                }
            }
        })
    }
}
fn to_arrow_batches_and_schema(
    result: &mut ExecutionResult,
    py: Python<'_>,
) -> PyResult<(PyObject, PyObject)> {
    use datafusion::arrow::pyarrow::PyArrowConvert;
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
        _ => todo!(),
    }
}

#[pymethods]
impl PyExecutionResult {
    /// Convert to Arrow Table
    /// Collect the batches and pass to Arrow Table
    #[allow(clippy::wrong_self_convention)] // this is consistent with other python API's
    fn to_arrow(&mut self, py: Python) -> PyResult<PyObject> {
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
    fn to_polars(&mut self, py: Python) -> PyResult<PyObject> {
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
}

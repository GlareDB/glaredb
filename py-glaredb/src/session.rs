use crate::util::pyprint;
use anyhow::Result;
use arrow_util::pretty::pretty_format_batches;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::{datatypes::Schema, pyarrow::ToPyArrow};
use futures::lock::Mutex;
use futures::StreamExt;
use pgrepr::format::Format;
use pyo3::{exceptions::PyRuntimeError, prelude::*, types::PyTuple};
use sqlexec::session::ExecutionStream;
use sqlexec::{
    engine::{Engine, TrackedSession},
    parser,
    session::ExecutionResult,
};
use std::sync::Arc;

pub(super) type PyTrackedSession = Arc<Mutex<TrackedSession>>;

use crate::{error::PyGlareDbError, logical_plan::PyLogicalPlan, runtime::wait_for_future};

#[pyclass]
pub struct LocalSession {
    pub(super) sess: PyTrackedSession,
    pub(super) engine: Engine,
}

#[pyclass]
pub struct PyExecutionStream {
    pub inner: ExecutionStream,
    pub result: ExecutionResult,
}

#[pymethods]
impl LocalSession {
    fn sql(&mut self, py: Python<'_>, query: &str) -> PyResult<PyLogicalPlan> {
        let cloned_sess = self.sess.clone();
        wait_for_future(py, async move {
            let mut sess = self.sess.lock().await;
            Ok(sess
                .sql_to_lp(query)
                .await
                .map(|lp| PyLogicalPlan::new(lp, cloned_sess))
                .map_err(PyGlareDbError::from)?)
        })
    }

    fn execute(&mut self, py: Python<'_>, query: &str) -> PyResult<PyExecutionStream> {
        const UNNAMED: String = String::new();

        let mut statements = parser::parse_sql(query).map_err(PyGlareDbError::from)?;

        wait_for_future(py, async move {
            let mut sess = self.sess.lock().await;

            match statements.len() {
                0 => todo!(),
                1 => {
                    let stmt = statements.pop_front().unwrap();

                    println!("statement: {stmt}");

                    sess.prepare_statement(UNNAMED, Some(stmt), Vec::new())
                        .await
                        .map_err(PyGlareDbError::from)?;

                    let prepared = sess
                        .get_prepared_statement(&UNNAMED)
                        .map_err(PyGlareDbError::from)?;
                    let num_fields = prepared.output_fields().map(|f| f.len()).unwrap_or(0);
                    sess.bind_statement(
                        UNNAMED,
                        &UNNAMED,
                        Vec::new(),
                        vec![Format::Text; num_fields],
                    )
                    .map_err(PyGlareDbError::from)?;
                    let mut stream = sess
                        .execute_portal(&UNNAMED, 0)
                        .await
                        .map_err(PyGlareDbError::from)?;

                    let result = stream.inspect_result().await;

                    Ok(PyExecutionStream {
                        inner: stream,
                        result,
                    })
                }
                _ => {
                    todo!()
                }
            }
        })
    }

    fn close(&mut self, py: Python<'_>) -> PyResult<()> {
        wait_for_future(py, async move {
            if let Err(err) = self.sess.lock().await.close().await {
                eprintln!("unable to close the existing session: {err}");
            }
            Ok(self.engine.shutdown().await.map_err(PyGlareDbError::from)?)
        })
    }
}

fn to_arrow_batches_and_schema(
    stream: &mut ExecutionStream,
    py: Python<'_>,
) -> PyResult<(PyObject, PyObject)> {
    let batches: Result<Vec<RecordBatch>> = wait_for_future(py, async move {
        Ok(stream
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?)
    });

    let batches =
        batches.map_err(|e| PyRuntimeError::new_err(format!("unhandled exception: {:?}", &e)))?;
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

fn print_batch(stream: &mut ExecutionStream, py: Python<'_>) -> PyResult<()> {
    let batches = wait_for_future(py, async move {
        stream
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<RecordBatch>, _>>()
    })?;

    let disp = pretty_format_batches(&batches, None, None, None)
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

    pyprint(disp, py)?;

    Ok(())
}

#[pymethods]
impl PyExecutionStream {
    /// Convert to Arrow Table
    /// Collect the batches and pass to Arrow Table
    #[allow(clippy::wrong_self_convention)] // this is consistent with other python API's
    pub fn to_arrow(&mut self, py: Python) -> PyResult<PyObject> {
        let (batches, schema) = to_arrow_batches_and_schema(&mut self.inner, py)?;

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
        let (batches, schema) = to_arrow_batches_and_schema(&mut self.inner, py)?;

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
        let (batches, schema) = to_arrow_batches_and_schema(&mut self.inner, py)?;

        Python::with_gil(|py| {
            let table_class = py.import("pyarrow")?.getattr("Table")?;
            let args = PyTuple::new(py, &[batches, schema]);
            let table: PyObject = table_class.call_method1("from_batches", args)?.into();

            let result = table.call_method0(py, "to_pandas")?;
            Ok(result)
        })
    }

    pub fn show(&mut self, py: Python) -> PyResult<()> {
        print_batch(&mut self.inner, py)?;
        Ok(())
    }
}

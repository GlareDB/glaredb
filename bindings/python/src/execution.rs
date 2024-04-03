use std::fmt::Debug;
use std::sync::{Arc, Mutex};

use arrow_util::pretty;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::pyarrow::ToPyArrow;
use futures::StreamExt;
use glaredb::{DataFusionError, RecordBatch, SendableRecordBatchStream};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyTuple;

use crate::error::PyGlareDbError;
use crate::runtime::wait_for_future;
use crate::util::pyprint;

#[pyclass]
#[derive(Clone)]
pub struct PyExecution {
    pub(crate) schema: SchemaRef,
    pub(crate) stream: Arc<Mutex<Option<SendableRecordBatchStream>>>,
}

impl Debug for PyExecution {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PyExecution{:?}", self.schema.clone())
    }
}


impl From<SendableRecordBatchStream> for PyExecution {
    fn from(stream: SendableRecordBatchStream) -> Self {
        Self {
            schema: stream.schema().clone(),
            stream: Arc::new(Mutex::new(Some(stream))),
        }
    }
}


#[pymethods]
impl PyExecution {
    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("PyExecution{:#?}", self.schema))
    }

    /// Convert to Arrow Table
    /// Collect the batches and pass to Arrow Table
    pub fn to_arrow(&mut self, py: Python) -> PyResult<PyObject> {
        let (batches, schema) = self.get_batches_and_schema(py)?;

        Python::with_gil(|py| {
            // Instantiate pyarrow Table object and use its from_batches method
            let table_class = py.import("pyarrow")?.getattr("Table")?;
            let args = PyTuple::new(py, &[batches, schema]);
            let table: PyObject = table_class.call_method1("from_batches", args)?.into();
            Ok(table)
        })
    }

    pub fn to_polars(&mut self, py: Python) -> PyResult<PyObject> {
        let (batches, schema) = self.get_batches_and_schema(py)?;

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

    pub fn to_pandas(&mut self, py: Python) -> PyResult<PyObject> {
        let (batches, schema) = self.get_batches_and_schema(py)?;

        Python::with_gil(|py| {
            let table_class = py.import("pyarrow")?.getattr("Table")?;
            let args = PyTuple::new(py, &[batches, schema]);
            let table: PyObject = table_class.call_method1("from_batches", args)?.into();

            let result = table.call_method0(py, "to_pandas")?;
            Ok(result)
        })
    }

    pub fn execute(&mut self, py: Python) -> PyResult<()> {
        let lp = self.clone();
        wait_for_future(py, async move {
            let mut stream = lp.stream.lock().unwrap().take().unwrap();
            while let Some(r) = stream.next().await {
                let _ = r?;
            }
            Ok(())
        })
    }

    pub fn show(&mut self, py: Python) -> PyResult<()> {
        let lp = self.clone();
        let batches = wait_for_future(py, async move {
            let mut stream = lp.stream.lock().unwrap().take().unwrap();
            let mut out = Vec::new();
            while let Some(batch) = stream.next().await {
                out.push(batch?)
            }
            Ok::<Vec<glaredb::RecordBatch>, DataFusionError>(out)
        })?;

        let disp = pretty::pretty_format_batches(
            &self.schema.clone(),
            &batches,
            Some(terminal_util::term_width()),
            None,
        )
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

        let _ = pyprint(disp, py);

        Ok(())
    }

    fn get_batches_and_schema(&self, py: Python) -> PyResult<(PyObject, PyObject)> {
        let lp = self.clone();
        let batches: Vec<RecordBatch> = wait_for_future(py, async move {
            let stream = lp.stream.lock().unwrap().take().unwrap();
            stream
                .collect::<Vec<Result<RecordBatch, DataFusionError>>>()
                .await
                .into_iter()
                .collect::<Result<Vec<RecordBatch>, DataFusionError>>()
                .map_err(PyGlareDbError::from)
        })?;
        let batches = batches
            .into_iter()
            .map(|rb| rb.to_pyarrow(py))
            .collect::<Result<Vec<_>, _>>()?
            .to_object(py);

        let schema = self.schema.clone().to_pyarrow(py)?;

        Ok((batches, schema))
    }
}

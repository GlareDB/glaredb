use std::any::Any;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};

use arrow_util::pretty;
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::pyarrow::ToPyArrow;
use datafusion::datasource::TableProvider;
use datafusion::execution::context::SessionState;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::streaming::{PartitionStream, StreamingTableExec};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::Expr;
use futures::StreamExt;
use glaredb::{DataFusionError, RecordBatch, SendableRecordBatchStream};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyTuple;

use crate::connection::Connection;
use crate::error::PyGlareDbError;
use crate::runtime::wait_for_future;
use crate::util::pyprint;

#[derive(Debug, Clone)]
pub(crate) enum OperationType {
    Sql,
    Prql,
    Execute,
}

#[pyclass]
#[derive(Clone)]
pub struct PyExecution {
    pub(crate) db: Connection,
    pub(crate) query: String,
    pub(crate) op: OperationType,
    pub(crate) schema: SchemaRef,
    pub(crate) stream: Arc<Mutex<Option<SendableRecordBatchStream>>>,
}


impl Debug for PyExecution {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PyExecution({:?}){:?}", self.op, self.schema.clone())
    }
}

impl PyExecution {
    pub(crate) fn get_stream(
        &mut self,
        py: Python,
    ) -> Result<SendableRecordBatchStream, PyGlareDbError> {
        match self.take_stream() {
            Some(stream) => Ok(stream),
            None => self.reexec(py),
        }
    }

    pub(crate) fn resolve_table(&self, py: Python<'_>) -> PyResult<PyTable> {
        let stream = self.stream.lock().unwrap();
        if stream.is_some() {
            Ok(PyTable {
                inner: Arc::new(Mutex::new(self.clone())),
                schema: self.schema.clone(),
            })
        } else {
            let mut exec = self.clone();
            exec.stream = Arc::new(Mutex::new(Some(exec.reexec(py)?)));
            Ok(PyTable {
                inner: Arc::new(Mutex::new(exec)),
                schema: self.schema.clone(),
            })
        }
    }

    fn reexec(&mut self, py: Python<'_>) -> Result<SendableRecordBatchStream, PyGlareDbError> {
        match self.op {
            OperationType::Sql => self.db.sql(py, &self.query)?.resolve_stream(),
            OperationType::Prql => self.db.prql(py, &self.query)?.resolve_stream(),
            OperationType::Execute => self.db.execute(py, &self.query)?.resolve_stream(),
        }
    }

    fn resolve_stream(&self) -> Result<SendableRecordBatchStream, PyGlareDbError> {
        self.take_stream()
            .map(Ok)
            .unwrap_or_else(|| Err(PyGlareDbError::new("data is not accessible")))
    }

    fn take_stream(&self) -> Option<SendableRecordBatchStream> {
        self.stream.clone().lock().unwrap().take()
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
        let mut stream = self.clone().get_stream(py)?;
        wait_for_future(py, async move {
            while let Some(r) = stream.next().await {
                let _ = r?;
            }
            Ok(())
        })
    }

    pub fn show(&mut self, py: Python) -> PyResult<()> {
        let mut stream = self.clone().get_stream(py)?;
        let batches = wait_for_future(py, async move {
            let mut out = Vec::new();
            while let Some(batch) = stream.next().await {
                out.push(batch?)
            }
            Ok::<Vec<RecordBatch>, PyGlareDbError>(out)
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
        let stream = self.clone().get_stream(py)?;
        let batches: Vec<RecordBatch> = wait_for_future(py, async move {
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


#[pyclass]
pub(crate) struct PyTable {
    schema: SchemaRef,
    inner: Arc<Mutex<PyExecution>>,
}


// just a wrapper around the stream so that we can compose multiple subqueries
#[async_trait]
impl TableProvider for PyTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn supports_filter_pushdown(
        &self,
        _filter: &Expr,
    ) -> Result<TableProviderFilterPushDown, DataFusionError> {
        Ok(TableProviderFilterPushDown::Inexact)
    }

    async fn scan(
        &self,
        _ctx: &SessionState,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        Ok(Arc::new(StreamingTableExec::try_new(
            self.schema.clone(),
            vec![Arc::new(PyPartition {
                schema: self.schema.clone(),
                exec: self.inner.clone(),
            })],
            projection,
            None,
            false,
        )?))
    }
}

struct PyPartition {
    schema: SchemaRef,
    exec: Arc<Mutex<PyExecution>>,
}

impl PartitionStream for PyPartition {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        self.exec.lock().unwrap().take_stream().unwrap()
    }
}

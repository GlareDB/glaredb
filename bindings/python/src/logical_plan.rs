use std::any::Any;
use std::fmt::Debug;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use arrow_util::pretty;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::pyarrow::ToPyArrow;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DatafusionResult};
use datafusion::execution::context::SessionState;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::streaming::{PartitionStream, StreamingTableExec};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::Expr;
use futures::{Stream, StreamExt};
use glaredb::{RecordBatch, SendableRecordBatchStream};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyTuple;

use crate::error::PyGlareDbError;
use crate::runtime::wait_for_future;
use crate::util::pyprint;

#[pyclass]
#[derive(Clone)]
pub struct PyLogicalPlan {
    schema: SchemaRef,
    stream: Arc<Mutex<Option<SendableRecordBatchStream>>>,
}

// impl Clone for PyLogicalPlan {
//     fn clone(&self) -> Self {
//         Self {
//             schema: self.schema.clone(),
//             stream: self.stream,
//         }
//     }
// }

impl Debug for PyLogicalPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PyLogicalPlan{:?}", self.schema.clone())
    }
}


impl From<SendableRecordBatchStream> for PyLogicalPlan {
    fn from(stream: SendableRecordBatchStream) -> Self {
        Self {
            schema: stream.schema().clone(),
            stream: Arc::new(Mutex::new(Some(stream))),
        }
    }
}


#[pymethods]
impl PyLogicalPlan {
    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("PyLogicalPlan{:#?}", self.schema))
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

#[pyclass]
pub(crate) struct PyTable {
    schema: SchemaRef,
    inner: Mutex<PyLogicalPlan>,
}

impl From<PyLogicalPlan> for PyTable {
    fn from(lp: PyLogicalPlan) -> Self {
        Self {
            schema: lp.schema.clone(),
            inner: Mutex::new(lp),
        }
    }
}

// just a wrapper around the stream so that we can compose multiple subqueries
#[async_trait::async_trait]
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
    ) -> DatafusionResult<TableProviderFilterPushDown> {
        Ok(TableProviderFilterPushDown::Inexact)
    }

    async fn scan(
        &self,
        _ctx: &SessionState,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let stream = self.inner.lock().unwrap().stream.clone();
        Ok(Arc::new(StreamingTableExec::try_new(
            self.schema.clone(),
            vec![Arc::new(PyPartition {
                schema: self.schema.clone(),
                inner: stream.clone(),
            })],
            projection,
            None,
            false,
        )?))
    }
}

struct LockedStreamAdapter {
    inner: Arc<Mutex<SendableRecordBatchStream>>,
}

impl Stream for LockedStreamAdapter {
    type Item = Result<RecordBatch, DataFusionError>;

    // Required method
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut inner = self.inner.lock().unwrap();
        inner.poll_next_unpin(cx)
    }
}

struct PyPartition {
    schema: SchemaRef,
    inner: Arc<Mutex<Option<SendableRecordBatchStream>>>,
}

impl PartitionStream for PyPartition {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        self.inner.lock().unwrap().take().unwrap()
    }
}

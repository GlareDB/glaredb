use std::any::Any;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};

use arrow_util::pretty;
use async_trait::async_trait;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::pyarrow::ToPyArrow;
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::execution::context::SessionState;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::{PartitionStream, StreamingTableExec};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::Expr;
use futures::StreamExt;
use glaredb::{Operation, RecordBatch, SendableRecordBatchStream};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyTuple;

use crate::error::PyGlareDbError;
use crate::runtime::wait_for_future;
use crate::util::pyprint;

#[pyclass]
#[derive(Debug, Clone)]
pub struct PyExecutionOutput {
    op: Arc<Mutex<Operation>>,
}

impl From<Operation> for PyExecutionOutput {
    fn from(op: Operation) -> Self {
        Self {
            op: Arc::new(Mutex::new(op)),
        }
    }
}

#[pymethods]
impl PyExecutionOutput {
    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("PyExecution{:#?}", self.op))
    }

    /// Convert to Arrow Table
    /// Collect the batches and pass to Arrow Table
    pub fn to_arrow(&mut self, py: Python) -> PyResult<PyObject> {
        let (schema, batches) = self.get_schema_and_batches(py)?;

        Python::with_gil(|py| {
            // Instantiate pyarrow Table object and use its from_batches method
            let table_class = py.import("pyarrow")?.getattr("Table")?;
            let args = PyTuple::new(py, &[batches, schema]);
            let table: PyObject = table_class.call_method1("from_batches", args)?.into();
            Ok(table)
        })
    }

    pub fn to_polars(&mut self, py: Python) -> PyResult<PyObject> {
        let (schema, batches) = self.get_schema_and_batches(py)?;

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
        let (schema, batches) = self.get_schema_and_batches(py)?;

        Python::with_gil(|py| {
            let table_class = py.import("pyarrow")?.getattr("Table")?;
            let args = PyTuple::new(py, &[batches, schema]);
            let table: PyObject = table_class.call_method1("from_batches", args)?.into();

            let result = table.call_method0(py, "to_pandas")?;
            Ok(result)
        })
    }

    pub fn execute(&self, py: Python) -> PyResult<()> {
        let mut stream = self.op.lock().unwrap().call();

        wait_for_future(py, async move {
            while let Some(r) = stream.next().await {
                let _ = r?;
            }
            Ok(())
        })
    }

    pub fn show(&mut self, py: Python) -> PyResult<()> {
        let (schema, batches) = self.resolve_operation(py)?;

        let disp = pretty::pretty_format_batches(
            &schema,
            &batches,
            Some(terminal_util::term_width()),
            None,
        )
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

        let _ = pyprint(disp, py);

        Ok(())
    }
}

impl PyExecutionOutput {
    fn get_schema_and_batches(&self, py: Python) -> PyResult<(PyObject, PyObject)> {
        let (schema, batches) = self.resolve_operation(py)?;

        let batches = batches
            .into_iter()
            .map(|rb| rb.to_pyarrow(py))
            .collect::<Result<Vec<_>, _>>()?;

        Ok((schema.to_pyarrow(py)?, batches.to_object(py)))
    }

    fn resolve_operation(
        &self,
        py: Python,
    ) -> Result<(Arc<Schema>, Vec<RecordBatch>), PyGlareDbError> {
        let batches = self.resolve_batches(py)?;

        let schema = if batches.is_empty() {
            Arc::new(Schema::empty())
        } else {
            batches.first().unwrap().schema()
        };

        Ok((schema, batches))
    }

    fn resolve_batches(&self, py: Python) -> Result<Vec<RecordBatch>, PyGlareDbError> {
        let mut stream = self.op.lock().unwrap().call();

        wait_for_future(py, async move {
            let mut out = Vec::new();
            while let Some(batch) = stream.next().await {
                out.push(batch.map_err(glaredb::Error::from)?)
            }
            Ok(out)
        })
    }
}


// just a wrapper around the stream so that we can compose multiple subqueries
#[async_trait]
impl TableProvider for PyExecutionOutput {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.op
            .lock()
            .unwrap()
            .schema()
            .expect("table must be resolved before use")
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
        let op = self.op.lock().unwrap();
        let schema = op.schema().expect("table must be resolved");
        Ok(Arc::new(StreamingTableExec::try_new(
            schema.clone(),
            vec![Arc::new(PyPartition {
                schema: schema.clone(),
                exec: self.op.clone(),
            })],
            projection,
            None,
            false,
        )?))
    }
}

struct PyPartition {
    schema: SchemaRef,
    exec: Arc<Mutex<Operation>>,
}

impl PartitionStream for PyPartition {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let mut op = self.exec.lock().unwrap();

        Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            op.call(),
        ))
    }
}

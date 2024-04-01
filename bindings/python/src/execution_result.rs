use std::sync::Arc;

use anyhow::Result;
use arrow_util::pretty;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::pyarrow::ToPyArrow;
use datafusion::arrow::record_batch::RecordBatch;
use futures::StreamExt;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyTuple;
use sqlexec::session::ExecutionResult;

use crate::runtime::wait_for_future;
use crate::util::pyprint;

/// The result of an executed query.
#[pyclass]
pub struct PyExecutionResult {
    stream: glaredb::SendableRecordBatchStream,
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

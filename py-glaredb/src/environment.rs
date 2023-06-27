use anyhow::Result;
use async_trait::async_trait;
use datafusion::datasource::{DefaultTableSource, MemTable};
use datafusion::error::Result as DataFusionResult;
use datafusion::logical_expr::TableProviderFilterPushDown;
use datafusion::{
    arrow::{
        datatypes::SchemaRef,
        pyarrow::{PyArrowConvert, PyArrowType},
        record_batch::RecordBatch,
    },
    datasource::TableProvider,
    execution::context::SessionState,
    logical_expr::{TableSource, TableType},
    physical_plan::ExecutionPlan,
    prelude::Expr,
};
use pyo3::{
    exceptions::PyRuntimeError,
    prelude::*,
    types::{PyTuple, PyType},
};
use sqlexec::{
    engine::{Engine, TrackedSession},
    environment::EnvironmentReader,
    parser,
    session::ExecutionResult,
};
use std::any::Any;
use std::sync::Arc;

/// Read polars dataframes from the python environment.
#[derive(Debug, Clone, Copy)]
pub struct PolarsReader;

impl EnvironmentReader for PolarsReader {
    fn resolve_table(
        &self,
        name: &str,
    ) -> Result<Option<Arc<dyn TableProvider>>, Box<dyn std::error::Error + Send + Sync>> {
        Python::with_gil(|py| {
            let var = py.eval(name, None, None).unwrap();
            let polars_type: &PyType = py
                .import("polars.dataframe.frame")
                .unwrap()
                .getattr("DataFrame")
                .unwrap()
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
        })
    }
}

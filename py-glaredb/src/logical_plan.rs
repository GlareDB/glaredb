use std::{any::Any, sync::Arc};

use datafusion::{
    arrow::datatypes::SchemaRef,
    datasource::TableProvider,
    execution::context::SessionState,
    logical_expr::{LogicalPlanBuilder, TableProviderFilterPushDown, TableType},
    physical_plan::ExecutionPlan,
    prelude::Expr,
};
use pyo3::prelude::*;
use sqlexec::LogicalPlan;

use crate::{
    error::PyGlareDbError,
    runtime::wait_for_future,
    session::{PyExecutionResult, PyTrackedSession},
};
use datafusion::error::Result as DatafusionResult;

#[pyclass]
#[derive(Clone, Debug)]
pub struct PyLogicalPlan {
    pub(super) lp: LogicalPlan,
    pub(super) session: PyTrackedSession,
}

impl PyLogicalPlan {
    pub(super) fn new(lp: LogicalPlan, session: PyTrackedSession) -> Self {
        Self { lp, session }
    }

    fn collect(&self, py: Python) -> PyResult<PyExecutionResult> {
        wait_for_future(py, async move {
            let mut sess = self.session.lock().await;
            let exec_res = sess
                .execute_inner(self.lp.clone())
                .await
                .map_err(PyGlareDbError::from)?;
            Ok(exec_res.into())
        })
    }
}

#[pymethods]
impl PyLogicalPlan {
    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("{:#?}", self.lp))
    }

    fn explain(&self) -> PyResult<String> {
        Ok(format!("{:?}", self.lp))
    }
    fn to_arrow(&self, py: Python) -> PyResult<PyObject> {
        self.collect(py)?.to_arrow(py)
    }
    fn to_polars(&self, py: Python) -> PyResult<PyObject> {
        self.collect(py)?.to_polars(py)
    }
    fn to_pandas(&self, py: Python) -> PyResult<PyObject> {
        self.collect(py)?.to_pandas(py)
    }
    fn show(&self, py: Python, show_metadata: bool, n_cols: u64) -> PyResult<()> {
        self.collect(py)?.show(py, show_metadata, n_cols)
    }
}

// just a wrapper around the logical plan so that we can compose multiple subqueries
#[async_trait::async_trait]
impl TableProvider for PyLogicalPlan {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        let lp = self.lp.clone().try_into_datafusion_plan().unwrap();
        let s = lp.schema().as_ref().clone();
        SchemaRef::new(s.into())
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
        ctx: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DatafusionResult<Arc<dyn ExecutionPlan>> {
        let lp = self.lp.clone().try_into_datafusion_plan().unwrap();
        let mut builder = LogicalPlanBuilder::from(lp);
        for filter in filters {
            builder = builder.filter(filter.clone())?;
        }

        if let Some(limit) = limit {
            builder = builder.limit(limit, None)?;
        }
        if let Some(proj) = projection {
            builder = builder.select(proj.clone())?;
        }
        let lp = builder.build()?;

        ctx.create_physical_plan(&lp).await
    }
}

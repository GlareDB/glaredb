use pyo3::prelude::*;
use sqlexec::LogicalPlan;

use crate::{
    error::PyGlareDbError,
    runtime::wait_for_future,
    session::{PyExecutionResult, PyTrackedSession},
};

#[pyclass]
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
}

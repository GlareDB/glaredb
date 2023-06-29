use sqlexec::LogicalPlan;

use pyo3::prelude::*;

use std::sync::Arc;

#[pyclass]
pub struct PyLogicalPlan {
    pub(super) lp: Arc<LogicalPlan>,
}

impl From<LogicalPlan> for PyLogicalPlan {
    fn from(lp: LogicalPlan) -> Self {
        Self { lp: Arc::new(lp) }
    }
}

#[pymethods]
impl PyLogicalPlan {
    fn __repr__(&self) -> String {
        format!("{:?}", self.lp)
    }
    
}

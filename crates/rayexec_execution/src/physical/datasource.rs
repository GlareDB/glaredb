use crate::planner::explainable::Explainable;
use arrow_schema::Schema;
use rayexec_error::{RayexecError, Result};
use std::fmt::Debug;
use std::sync::Arc;

pub trait DataSource: Explainable + Debug {
    /// Get the schema of the underlying data source.
    fn schema(&self) -> Arc<Schema>;
}

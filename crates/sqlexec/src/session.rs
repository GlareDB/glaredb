use crate::errors::{internal, ExecError, Result};
use datafusion::arrow::datatypes::DataType;
use datafusion::catalog::catalog::CatalogList;
use datafusion::error::Result as DfResult;
use datafusion::execution::context::{SessionConfig, SessionState, TaskContext};
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::logical_expr::{AggregateUDF, ScalarUDF, TableSource};
use datafusion::optimizer::{self, optimizer::Optimizer};
use datafusion::physical_optimizer::{self, optimizer::PhysicalOptimizerRule};
use datafusion::sql::{planner::ContextProvider, TableReference};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, trace};
use uuid::Uuid;

const DEFAULT_SCHEMA: &str = "public";

/// A per-client user session.
///
/// This type acts as the bridge between datafusion planning/execution and the
/// rest of the system.
pub struct Session {
    /// Unique session identifier.
    id: String,

    /// Datafusion config for query planning and execution.
    config: SessionConfig,

    /// Datafusion runtime environment.
    runtime: Arc<RuntimeEnv>,
    // TODO: Transaction context goes here.
}

impl Session {
    /// Create a new session.
    ///
    /// Note that the catalog name will be constant throughout the lifetime of
    /// the session, and should match the name for the "cloud database"
    /// instance.
    pub fn new(catalog_name: impl Into<String>, runtime: Arc<RuntimeEnv>) -> Session {
        let id = Uuid::new_v4().to_string();
        let config = SessionConfig::default()
            .create_default_catalog_and_schema(true)
            .with_information_schema(true)
            .with_default_catalog_and_schema(catalog_name, DEFAULT_SCHEMA);

        Session {
            id,
            config,
            runtime,
        }
    }
}

/// Information provided during planning.
impl ContextProvider for Session {
    fn get_table_provider(&self, name: TableReference) -> DfResult<Arc<dyn TableSource>> {
        todo!()
    }

    fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>> {
        None
    }

    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>> {
        None
    }

    fn get_variable_type(&self, variable_names: &[String]) -> Option<DataType> {
        None
    }
}

/// Produce a task context from the session. A task context are used during
/// physical execution.
impl From<&Session> for TaskContext {
    fn from(session: &Session) -> Self {
        let task_id = Uuid::new_v4().to_string();
        // Note that we don't have access to the fields directly, so we don't
        // have a clean way of just passing in the config.
        //
        // TODO: Open up pr against datafusion to allow creating a task context
        // with a given config.
        TaskContext::new(
            task_id,
            session.id.clone(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            session.runtime.clone(),
        )
    }
}

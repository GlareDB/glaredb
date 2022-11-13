use datafusion::execution::context::{SessionConfig, SessionState, TaskContext};
use datafusion::execution::runtime_env::RuntimeEnv;
use std::sync::Arc;

/// Context for a session used during execution.
pub struct SessionContext {
    /// Datafusion session state used for planning and execution.
    ///
    /// This session state makes a ton of assumptions, try to keep usage of it
    /// to a minimum and ensure interactions with this are well-defined.
    df_state: SessionState,
}

impl SessionContext {
    pub fn new() -> SessionContext {
        // TODO: Pass in datafusion runtime env.

        // NOTE: We handle catalog/schema defaults and information schemas
        // ourselves.
        let config = SessionConfig::default()
            .create_default_catalog_and_schema(false)
            .with_information_schema(false);

        let runtime = Arc::new(RuntimeEnv::default());
        let state = SessionState::with_config_rt(config, runtime);

        // TODO: Put catalog list on state?

        SessionContext { df_state: state }
    }

    /// Get a datafusion task context to use for physical plan execution.
    pub fn task_context(&self) -> Arc<TaskContext> {
        Arc::new(TaskContext::from(&self.df_state))
    }

    pub fn get_df_state(&self) -> &SessionState {
        &self.df_state
    }
}

use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::context::{SessionConfig, SessionState, TaskContext};
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::sql::planner::ContextProvider;
use datafusion::sql::TableReference;
use jsoncat::catalog::Catalog;
use std::sync::Arc;

/// Context for a session used during execution.
pub struct SessionContext {
    catalog: Arc<Catalog>,
    /// Datafusion session state used for planning and execution.
    ///
    /// This session state makes a ton of assumptions, try to keep usage of it
    /// to a minimum and ensure interactions with this are well-defined.
    df_state: SessionState,
}

impl SessionContext {
    pub fn new(catalog: Arc<Catalog>) -> SessionContext {
        // TODO: Pass in datafusion runtime env.

        // NOTE: We handle catalog/schema defaults and information schemas
        // ourselves.
        let config = SessionConfig::default()
            .create_default_catalog_and_schema(false)
            .with_information_schema(false);

        let runtime = Arc::new(RuntimeEnv::default());
        let state = SessionState::with_config_rt(config, runtime);

        // // TODO: We don't need to create the builtin functions everytime.
        // let funcs = vec![version_func()];
        // for func in funcs {
        //     state.scalar_functions.insert(func.name.clone(), func);
        // }

        // Note that we do not replace the default catalog list on the state. We
        // should never be referencing it during planning or execution.
        //
        // Ideally we can reduce the need to rely on datafusion's session state
        // as much as possible. It makes way too many assumptions.

        SessionContext {
            catalog,
            df_state: state,
        }
    }

    /// Get a datafusion task context to use for physical plan execution.
    pub fn task_context(&self) -> Arc<TaskContext> {
        Arc::new(TaskContext::from(&self.df_state))
    }

    /// Get a datafusion session state.
    pub fn get_df_state(&self) -> &SessionState {
        &self.df_state
    }
}

// impl ContextProvider for SessionContext {
//     fn get_table_provider(&self, name: TableReference) -> DataFusionResult<Arc<dyn TableSource>> {
//         todo!()
//     }

//     fn get_function_meta(&self, name: &str) -> Option<Arc<datafusion::logical_expr::ScalarUDF>> {
//         todo!()
//     }

//     fn get_aggregate_meta(
//         &self,
//         name: &str,
//     ) -> Option<Arc<datafusion::logical_expr::AggregateUDF>> {
//         todo!()
//     }

//     fn get_variable_type(
//         &self,
//         variable_names: &[String],
//     ) -> Option<datafusion::arrow::datatypes::DataType> {
//         todo!()
//     }
// }

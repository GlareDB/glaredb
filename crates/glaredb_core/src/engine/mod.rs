pub mod query_result;
pub mod session;
pub mod single_user;

use std::sync::Arc;

use glaredb_error::{OptionExt, Result};
use session::Session;

use crate::catalog::context::{DatabaseContext, SYSTEM_CATALOG};
use crate::catalog::create::{
    CreateAggregateFunctionInfo,
    CreateScalarFunctionInfo,
    CreateSchemaInfo,
    CreateTableFunctionInfo,
    OnConflict,
};
use crate::catalog::database::{AccessMode, Database};
use crate::catalog::system::{DEFAULT_SCHEMA, new_system_catalog};
use crate::catalog::{Catalog, Schema};
use crate::extension::Extension;
use crate::runtime::executor::PipelineExecutor;
use crate::runtime::io::IoRuntime;
use crate::storage::storage_manager::StorageManager;

#[derive(Debug)]
pub struct Engine<P: PipelineExecutor, R: IoRuntime> {
    system_catalog: Arc<Database>,
    executor: P,
    runtime: R,
}

impl<P, R> Engine<P, R>
where
    P: PipelineExecutor,
    R: IoRuntime,
{
    pub fn new(executor: P, runtime: R) -> Result<Self> {
        let system_catalog = Arc::new(Database {
            name: SYSTEM_CATALOG.to_string(),
            mode: AccessMode::ReadOnly,
            catalog: Arc::new(new_system_catalog()?),
            storage: Arc::new(StorageManager::empty()),
            attach_info: None,
        });

        Ok(Engine {
            system_catalog,
            executor,
            runtime,
        })
    }

    /// Creates a new database context that contains only the system catalog and
    /// a temporary catalog.
    ///
    /// This should be the base of all session catalogs.
    pub fn new_base_database_context(&self) -> Result<DatabaseContext> {
        DatabaseContext::new(self.system_catalog.clone())
    }

    /// Create a new session.
    pub fn new_session(&self) -> Result<Session<P, R>> {
        let context = self.new_base_database_context()?;
        Ok(Session::new(
            context,
            self.executor.clone(),
            self.runtime.clone(),
        ))
    }

    /// Register a new extension for this engine.
    pub fn register_extension<E>(&self, ext: E) -> Result<()>
    where
        E: Extension,
    {
        let schema = match E::FUNCTION_NAMESPACE {
            Some(namespace) => {
                // Create a new schema for these functions.
                self.system_catalog
                    .catalog
                    .create_schema(&CreateSchemaInfo {
                        name: namespace.to_string(),
                        on_conflict: OnConflict::Error,
                    })?
            }
            None => {
                // Use the default schema.
                self.system_catalog
                    .catalog
                    .get_schema(DEFAULT_SCHEMA)?
                    .required("default schema")?
            }
        };

        // Register scalar functions.
        for scalar in ext.scalar_functions() {
            schema.create_scalar_function(&CreateScalarFunctionInfo {
                name: scalar.name.to_string(),
                implementation: *scalar,
                on_conflict: OnConflict::Error,
            })?;

            for alias in scalar.aliases {
                schema.create_scalar_function(&CreateScalarFunctionInfo {
                    name: alias.to_string(),
                    implementation: *scalar,
                    on_conflict: OnConflict::Error,
                })?;
            }
        }

        // Register aggregate functions.
        for agg in ext.aggregate_functions() {
            schema.create_aggregate_function(&CreateAggregateFunctionInfo {
                name: agg.name.to_string(),
                implementation: *agg,
                on_conflict: OnConflict::Error,
            })?;

            for alias in agg.aliases {
                schema.create_aggregate_function(&CreateAggregateFunctionInfo {
                    name: alias.to_string(),
                    implementation: *agg,
                    on_conflict: OnConflict::Error,
                })?;
            }
        }

        // Register table functions.
        for table_func in ext.table_functions() {
            schema.create_table_function(&CreateTableFunctionInfo {
                name: table_func.name.to_string(),
                implementation: *table_func,
                on_conflict: OnConflict::Error,
            })?;

            for alias in table_func.aliases {
                schema.create_table_function(&CreateTableFunctionInfo {
                    name: alias.to_string(),
                    implementation: *table_func,
                    on_conflict: OnConflict::Error,
                })?;
            }
        }

        // TODO: File handlers

        Ok(())
    }
}

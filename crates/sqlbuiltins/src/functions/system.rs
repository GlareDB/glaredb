//! Table functions for triggering system-related functionality. Users are
//! unlikely to use these, but there's no harm if they do.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::DataType;
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Signature, Volatility};
use datafusion_ext::errors::{ExtensionError, Result};
use datafusion_ext::functions::{FuncParamValue, TableFunc, TableFuncContextProvider};
use datafusion_ext::system::SystemOperation;
use datasources::native::access::NativeTableStorage;
use datasources::postgres::{PostgresAccess, PostgresTableProvider, PostgresTableProviderConfig};
use futures::Future;
use protogen::metastore::types::catalog::RuntimePreference;

#[derive(Debug, Clone, Copy)]
pub struct CacheExternalDatabaseTables;

#[async_trait]
impl TableFunc for CacheExternalDatabaseTables {
    fn runtime_preference(&self) -> RuntimePreference {
        RuntimePreference::Remote
    }

    fn name(&self) -> &str {
        "cache_external_database_tables"
    }

    fn signature(&self) -> Option<Signature> {
        Some(Signature::uniform(0, Vec::new(), Volatility::Stable))
    }

    async fn create_provider(
        &self,
        context: &dyn TableFuncContextProvider,
        _args: Vec<FuncParamValue>,
        _opts: HashMap<String, FuncParamValue>,
    ) -> Result<Arc<dyn TableProvider>> {
        unimplemented!()
    }
}

#[derive(Debug)]
struct CacheExternalDatabaseTablesOperation {}

impl SystemOperation for CacheExternalDatabaseTablesOperation {
    fn name(&self) -> &'static str {
        "cache_external_database_tables"
    }

    fn create_future(
        &self,
        context: Arc<TaskContext>,
    ) -> Pin<Box<dyn Future<Output = Result<(), DataFusionError>> + Send>> {
        let fut = async move { Ok(()) };

        Box::pin(fut)
    }
}

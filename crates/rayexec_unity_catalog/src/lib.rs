pub mod catalog;
pub mod connection;
pub mod functions;
pub mod rest;

use std::collections::HashMap;
use std::sync::Arc;

use catalog::UnityCatalog;
use connection::{UnityCatalogConnection, CATALOG_OPTION_KEY, ENDPOINT_OPTION_KEY};
use functions::{ListSchemasOperation, ListTablesOperation, UnityObjects};
use futures::future::BoxFuture;
use rayexec_error::Result;
use rayexec_execution::arrays::scalar::OwnedScalarValue;
use rayexec_execution::datasource::{
    take_option,
    DataSource,
    DataSourceBuilder,
    DataSourceConnection,
};
use rayexec_execution::functions::table::TableFunction;
use rayexec_execution::runtime::Runtime;
use rayexec_execution::storage::memory::MemoryTableStorage;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnityCatalogDataSource<R: Runtime> {
    runtime: R,
}

impl<R: Runtime> DataSourceBuilder<R> for UnityCatalogDataSource<R> {
    fn initialize(runtime: R) -> Box<dyn DataSource> {
        Box::new(UnityCatalogDataSource { runtime })
    }
}

impl<R: Runtime> DataSource for UnityCatalogDataSource<R> {
    fn connect(
        &self,
        mut options: HashMap<String, OwnedScalarValue>,
    ) -> BoxFuture<'_, Result<DataSourceConnection>> {
        Box::pin(async move {
            let endpoint = take_option(ENDPOINT_OPTION_KEY, &mut options)?.try_into_string()?;
            let catalog_name = take_option(CATALOG_OPTION_KEY, &mut options)?.try_into_string()?;

            let conn =
                UnityCatalogConnection::connect(self.runtime.clone(), &endpoint, &catalog_name)
                    .await?;
            Ok(DataSourceConnection {
                catalog_storage: Some(Arc::new(UnityCatalog::new(conn))),
                table_storage: Arc::new(MemoryTableStorage::default()), // TODO: remove this
            })
        })
    }

    fn initialize_table_functions(&self) -> Vec<Box<dyn TableFunction>> {
        vec![
            // `unity_list_schemas`
            Box::new(UnityObjects::<_, ListSchemasOperation>::new(
                self.runtime.clone(),
            )),
            // `unity_list_tables`
            Box::new(UnityObjects::<_, ListTablesOperation>::new(
                self.runtime.clone(),
            )),
        ]
    }
}

pub mod catalog;
pub mod connection;
pub mod functions;
pub mod rest;

use std::collections::HashMap;
use std::sync::Arc;

use catalog::UnityCatalog;
use connection::UnityCatalogConnection;
use futures::future::BoxFuture;
use rayexec_bullet::scalar::OwnedScalarValue;
use rayexec_error::Result;
use rayexec_execution::datasource::{DataSource, DataSourceBuilder, DataSourceConnection};
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
        options: HashMap<String, OwnedScalarValue>,
    ) -> BoxFuture<'_, Result<DataSourceConnection>> {
        Box::pin(async move {
            let conn = UnityCatalogConnection::connect(self.runtime.clone(), options).await?;
            Ok(DataSourceConnection {
                catalog_storage: Some(Arc::new(UnityCatalog::new(conn))),
                table_storage: Arc::new(MemoryTableStorage::default()), // TODO: remove this
            })
        })
    }
}

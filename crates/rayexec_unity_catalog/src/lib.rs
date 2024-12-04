pub mod connection;
pub mod rest;

use std::collections::HashMap;

use futures::future::BoxFuture;
use rayexec_bullet::scalar::OwnedScalarValue;
use rayexec_error::Result;
use rayexec_execution::datasource::{DataSource, DataSourceBuilder, DataSourceConnection};
use rayexec_execution::runtime::Runtime;

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
        _options: HashMap<String, OwnedScalarValue>,
    ) -> BoxFuture<'_, Result<DataSourceConnection>> {
        unimplemented!()
    }
}

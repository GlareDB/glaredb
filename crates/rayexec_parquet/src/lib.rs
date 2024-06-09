pub mod read_parquet;

use futures::future::BoxFuture;
use rayexec_bullet::scalar::OwnedScalarValue;
use rayexec_error::{RayexecError, Result};
use rayexec_execution::{
    database::catalog::Catalog, datasource::DataSource, engine::EngineRuntime,
    functions::table::GenericTableFunction,
};
use read_parquet::ReadParquet;
use std::{collections::HashMap, sync::Arc};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParquetDataSource;

impl DataSource for ParquetDataSource {
    fn create_catalog(
        &self,
        _runtime: &Arc<EngineRuntime>,
        _options: HashMap<String, OwnedScalarValue>,
    ) -> BoxFuture<Result<Box<dyn Catalog>>> {
        Box::pin(async {
            Err(RayexecError::new(
                "Parquet data source cannot be used to create a catalog",
            ))
        })
    }

    fn initialize_table_functions(&self) -> Vec<Box<dyn GenericTableFunction>> {
        vec![Box::new(ReadParquet)]
    }
}

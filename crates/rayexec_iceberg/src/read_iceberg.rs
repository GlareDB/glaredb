use std::sync::Arc;

use futures::future::BoxFuture;
use rayexec_bullet::field::Schema;
use rayexec_error::{RayexecError, Result};
use rayexec_execution::database::DatabaseContext;
use rayexec_execution::functions::table::{PlannedTableFunction, TableFunction, TableFunctionArgs};
use rayexec_execution::runtime::Runtime;
use rayexec_execution::storage::table_storage::DataTable;
use rayexec_io::location::{AccessConfig, FileLocation};

use crate::table::Table;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadIceberg<R: Runtime> {
    pub(crate) runtime: R,
}

impl<R: Runtime> TableFunction for ReadIceberg<R> {
    fn name(&self) -> &'static str {
        "read_iceberg"
    }

    fn aliases(&self) -> &'static [&'static str] {
        &["iceberg_scan"]
    }

    fn plan_and_initialize(
        &self,
        context: &DatabaseContext,
        args: TableFunctionArgs,
    ) -> BoxFuture<'_, Result<Box<dyn PlannedTableFunction>>> {
        unimplemented!()
    }

    fn decode_state(&self, state: &[u8]) -> Result<Box<dyn PlannedTableFunction>> {
        unimplemented!()
    }
}

#[derive(Debug, Clone)]
struct ReadIcebergState {
    location: FileLocation,
    conf: AccessConfig,
    schema: Schema,
    table: Option<Arc<Table>>, // Populate on re-init if needed.
}

#[derive(Debug, Clone)]
pub struct ReadIcebergImpl<R: Runtime> {
    func: ReadIceberg<R>,
    state: ReadIcebergState,
}

impl<R: Runtime> ReadIcebergImpl<R> {
    async fn initialize(
        func: ReadIceberg<R>,
        args: TableFunctionArgs,
    ) -> Result<Box<dyn PlannedTableFunction>> {
        let (location, conf) = args.try_location_and_access_config()?;
        let provider = func.runtime.file_provider();

        let table = Table::load(location.clone(), provider, conf.clone()).await?;
        let schema = table.schema()?;

        Ok(Box::new(ReadIcebergImpl {
            func,
            state: ReadIcebergState {
                location,
                conf,
                schema,
                table: Some(Arc::new(table)),
            },
        }))
    }
}

impl<R: Runtime> PlannedTableFunction for ReadIcebergImpl<R> {
    fn reinitialize(&self) -> BoxFuture<Result<()>> {
        // TODO: See delta
        unimplemented!()
    }

    fn table_function(&self) -> &dyn TableFunction {
        &self.func
    }

    fn encode_state(&self, state: &mut Vec<u8>) -> Result<()> {
        unimplemented!()
    }

    fn schema(&self) -> Schema {
        self.state.schema.clone()
    }

    fn datatable(&self) -> Result<Box<dyn DataTable>> {
        let table = match self.state.table.as_ref() {
            Some(table) => table.clone(),
            None => return Err(RayexecError::new("Iceberg table not initialized")),
        };

        unimplemented!()
    }
}

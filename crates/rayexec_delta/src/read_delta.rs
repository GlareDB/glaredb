use futures::future::BoxFuture;
use rayexec_bullet::field::Schema;
use rayexec_error::{RayexecError, Result};
use rayexec_execution::{
    database::table::DataTable,
    functions::table::{PlannedTableFunction, TableFunction, TableFunctionArgs},
    runtime::ExecutionRuntime,
};
use rayexec_io::location::{AccessConfig, FileLocation};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::{datatable::DeltaDataTable, protocol::table::Table};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadDelta;

impl TableFunction for ReadDelta {
    fn name(&self) -> &'static str {
        "read_delta"
    }

    fn aliases(&self) -> &'static [&'static str] {
        &["delta_scan"]
    }

    fn plan_and_initialize<'a>(
        &'a self,
        runtime: &'a Arc<dyn ExecutionRuntime>,
        args: TableFunctionArgs,
    ) -> BoxFuture<'a, Result<Box<dyn PlannedTableFunction>>> {
        Box::pin(async move { ReadDeltaImpl::initialize(runtime.as_ref(), args).await })
    }

    fn state_deserialize(
        &self,
        deserializer: &mut dyn erased_serde::Deserializer,
    ) -> Result<Box<dyn PlannedTableFunction>> {
        Ok(Box::new(ReadDeltaImpl::deserialize(deserializer)?))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadDeltaImpl {
    location: FileLocation,
    conf: AccessConfig,
    schema: Schema,
    #[serde(skip)]
    table: Option<Arc<Table>>, // Populate on re-init if needed.
}

impl ReadDeltaImpl {
    async fn initialize(
        runtime: &dyn ExecutionRuntime,
        args: TableFunctionArgs,
    ) -> Result<Box<dyn PlannedTableFunction>> {
        let (location, conf) = args.try_location_and_access_config()?;

        let provider = runtime.file_provider();

        let table = Table::load(location.clone(), provider, conf.clone()).await?;
        let schema = table.table_schema()?;

        Ok(Box::new(ReadDeltaImpl {
            location,
            conf,
            schema,
            table: Some(Arc::new(table)),
        }))
    }
}

impl PlannedTableFunction for ReadDeltaImpl {
    fn reinitialize(&self, _runtime: &Arc<dyn ExecutionRuntime>) -> BoxFuture<Result<()>> {
        // TODO: Reinit table.
        // TODO: Needs mut
        unimplemented!()
    }

    fn serializable_state(&self) -> &dyn erased_serde::Serialize {
        self
    }

    fn table_function(&self) -> &dyn TableFunction {
        &ReadDelta
    }

    fn schema(&self) -> Schema {
        self.schema.clone()
    }

    fn datatable(&self, _runtime: &Arc<dyn ExecutionRuntime>) -> Result<Box<dyn DataTable>> {
        let table = match self.table.as_ref() {
            Some(table) => table.clone(),
            None => return Err(RayexecError::new("Delta table not initialized")),
        };

        Ok(Box::new(DeltaDataTable { table }))
    }
}

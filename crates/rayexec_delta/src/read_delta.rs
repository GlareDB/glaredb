use futures::future::BoxFuture;
use rayexec_bullet::field::Schema;
use rayexec_error::{RayexecError, Result};
use rayexec_execution::{
    database::table::DataTable,
    functions::table::{PlannedTableFunction, TableFunction, TableFunctionArgs},
    runtime::Runtime,
};
use rayexec_io::location::{AccessConfig, FileLocation};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::{datatable::DeltaDataTable, protocol::table::Table};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadDelta<R: Runtime> {
    pub(crate) runtime: R,
}

impl<R: Runtime> TableFunction for ReadDelta<R> {
    fn name(&self) -> &'static str {
        "read_delta"
    }

    fn aliases(&self) -> &'static [&'static str] {
        &["delta_scan"]
    }

    fn plan_and_initialize(
        &self,
        args: TableFunctionArgs,
    ) -> BoxFuture<'_, Result<Box<dyn PlannedTableFunction>>> {
        Box::pin(async move { ReadDeltaImpl::initialize(self.clone(), args).await })
    }

    fn state_deserialize(
        &self,
        deserializer: &mut dyn erased_serde::Deserializer,
    ) -> Result<Box<dyn PlannedTableFunction>> {
        let state = ReadDeltaState::deserialize(deserializer)?;
        Ok(Box::new(ReadDeltaImpl {
            func: self.clone(),
            state,
        }))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ReadDeltaState {
    location: FileLocation,
    conf: AccessConfig,
    schema: Schema,
    #[serde(skip)]
    table: Option<Arc<Table>>, // Populate on re-init if needed.
}

#[derive(Debug, Clone)]
pub struct ReadDeltaImpl<R: Runtime> {
    func: ReadDelta<R>,
    state: ReadDeltaState,
}

impl<R: Runtime> ReadDeltaImpl<R> {
    async fn initialize(
        func: ReadDelta<R>,
        args: TableFunctionArgs,
    ) -> Result<Box<dyn PlannedTableFunction>> {
        let (location, conf) = args.try_location_and_access_config()?;

        let provider = func.runtime.file_provider();

        let table = Table::load(location.clone(), provider, conf.clone()).await?;
        let schema = table.table_schema()?;

        Ok(Box::new(ReadDeltaImpl {
            func,
            state: ReadDeltaState {
                location,
                conf,
                schema,
                table: Some(Arc::new(table)),
            },
        }))
    }
}

impl<R: Runtime> PlannedTableFunction for ReadDeltaImpl<R> {
    fn reinitialize(&self) -> BoxFuture<Result<()>> {
        // TODO: Reinit table.
        // TODO: Needs mut
        unimplemented!()
    }

    fn serializable_state(&self) -> &dyn erased_serde::Serialize {
        &self.state
    }

    fn table_function(&self) -> &dyn TableFunction {
        &self.func
    }

    fn schema(&self) -> Schema {
        self.state.schema.clone()
    }

    fn datatable(&self) -> Result<Box<dyn DataTable>> {
        let table = match self.state.table.as_ref() {
            Some(table) => table.clone(),
            None => return Err(RayexecError::new("Delta table not initialized")),
        };

        Ok(Box::new(DeltaDataTable { table }))
    }
}

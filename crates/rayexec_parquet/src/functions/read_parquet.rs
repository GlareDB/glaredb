use futures::future::BoxFuture;
use rayexec_bullet::field::Schema;
use rayexec_error::{RayexecError, Result};
use rayexec_execution::{
    database::table::DataTable,
    functions::table::{PlannedTableFunction, TableFunction, TableFunctionArgs},
    runtime::Runtime,
};
use rayexec_io::location::{AccessConfig, FileLocation};
use rayexec_io::FileProvider;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::{metadata::Metadata, schema::from_parquet_schema};

use super::datatable::RowGroupPartitionedDataTable;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadParquet<R: Runtime> {
    pub(crate) runtime: R,
}

impl<R: Runtime> TableFunction for ReadParquet<R> {
    fn name(&self) -> &'static str {
        "read_parquet"
    }

    fn aliases(&self) -> &'static [&'static str] {
        &["parquet_scan"]
    }

    fn plan_and_initialize(
        &self,
        args: TableFunctionArgs,
    ) -> BoxFuture<'_, Result<Box<dyn PlannedTableFunction>>> {
        Box::pin(ReadParquetImpl::initialize(self.clone(), args))
    }

    fn state_deserialize(
        &self,
        deserializer: &mut dyn erased_serde::Deserializer,
    ) -> Result<Box<dyn PlannedTableFunction>> {
        let state = ReadParquetState::deserialize(deserializer)?;
        Ok(Box::new(ReadParquetImpl {
            func: self.clone(),
            state,
        }))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ReadParquetState {
    location: FileLocation,
    conf: AccessConfig,
    // TODO: Not sure what we want to do here. We could put
    // Serialize/Deserialize macros on everything, but I'm not sure how
    // deep/wide that would go.
    #[serde(skip)]
    metadata: Option<Arc<Metadata>>,
    schema: Schema,
}

#[derive(Debug, Clone)]
pub struct ReadParquetImpl<R: Runtime> {
    func: ReadParquet<R>,
    state: ReadParquetState,
}

impl<R: Runtime> ReadParquetImpl<R> {
    async fn initialize(
        func: ReadParquet<R>,
        args: TableFunctionArgs,
    ) -> Result<Box<dyn PlannedTableFunction>> {
        let (location, conf) = args.try_location_and_access_config()?;
        let mut source = func
            .runtime
            .file_provider()
            .file_source(location.clone(), &conf)?;

        let size = source.size().await?;

        let metadata = Metadata::load_from(source.as_mut(), size).await?;
        let schema = from_parquet_schema(metadata.parquet_metadata.file_metadata().schema_descr())?;

        Ok(Box::new(Self {
            func,
            state: ReadParquetState {
                location,
                conf,
                metadata: Some(Arc::new(metadata)),
                schema,
            },
        }))
    }
}

impl<R: Runtime> PlannedTableFunction for ReadParquetImpl<R> {
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
        let metadata = match self.state.metadata.as_ref().cloned() {
            Some(metadata) => metadata,
            None => return Err(RayexecError::new("Missing parquet metadata on state")),
        };

        Ok(Box::new(RowGroupPartitionedDataTable {
            metadata,
            schema: self.state.schema.clone(),
            location: self.state.location.clone(),
            conf: self.state.conf.clone(),
            runtime: self.func.runtime.clone(),
        }))
    }
}

use futures::future::BoxFuture;
use rayexec_bullet::field::Schema;
use rayexec_error::{RayexecError, Result};
use rayexec_execution::{
    database::table::DataTable,
    functions::table::{
        check_named_args_is_empty, PlannedTableFunction, TableFunction, TableFunctionArgs,
    },
    runtime::ExecutionRuntime,
};
use rayexec_io::FileLocation;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::{metadata::Metadata, schema::convert_schema};

use super::datatable::RowGroupPartitionedDataTable;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadParquet;

impl TableFunction for ReadParquet {
    fn name(&self) -> &'static str {
        "read_parquet"
    }

    fn aliases(&self) -> &'static [&'static str] {
        &["parquet_scan"]
    }

    fn plan_and_initialize<'a>(
        &'a self,
        runtime: &'a Arc<dyn ExecutionRuntime>,
        args: TableFunctionArgs,
    ) -> BoxFuture<'a, Result<Box<dyn PlannedTableFunction>>> {
        Box::pin(ReadParquetImpl::initialize(runtime.as_ref(), args))
    }

    fn state_deserialize(
        &self,
        deserializer: &mut dyn erased_serde::Deserializer,
    ) -> Result<Box<dyn PlannedTableFunction>> {
        Ok(Box::new(ReadParquetImpl::deserialize(deserializer)?))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadParquetImpl {
    location: FileLocation,
    // TODO: Not sure what we want to do here. We could put
    // Serialize/Deserialize macros on everything, but I'm not sure how
    // deep/wide that would go.
    #[serde(skip)]
    metadata: Option<Arc<Metadata>>,
    schema: Schema,
}

impl ReadParquetImpl {
    async fn initialize(
        runtime: &dyn ExecutionRuntime,
        mut args: TableFunctionArgs,
    ) -> Result<Box<dyn PlannedTableFunction>> {
        check_named_args_is_empty(&ReadParquet, &args)?;
        if args.positional.len() != 1 {
            return Err(RayexecError::new("Expected one argument"));
        }

        // TODO: Glob, dispatch to object storage/http impls

        let location = args.positional.pop().unwrap().try_into_string()?;
        let location = FileLocation::parse(&location);

        let mut source = runtime.file_provider().file_source(location.clone())?;

        let size = source.size().await?;

        let metadata = Metadata::load_from(source.as_mut(), size).await?;
        let schema = convert_schema(metadata.parquet_metadata.file_metadata().schema_descr())?;

        Ok(Box::new(Self {
            location,
            metadata: Some(Arc::new(metadata)),
            schema,
        }))
    }
}

impl PlannedTableFunction for ReadParquetImpl {
    fn serializable_state(&self) -> &dyn erased_serde::Serialize {
        self
    }

    fn table_function(&self) -> &dyn TableFunction {
        &ReadParquet
    }

    fn schema(&self) -> Schema {
        self.schema.clone()
    }

    fn datatable(&self, runtime: &Arc<dyn ExecutionRuntime>) -> Result<Box<dyn DataTable>> {
        let metadata = match self.metadata.as_ref().cloned() {
            Some(metadata) => metadata,
            None => return Err(RayexecError::new("Missing parquet metadata on state")),
        };

        Ok(Box::new(RowGroupPartitionedDataTable {
            metadata,
            schema: self.schema.clone(),
            location: self.location.clone(),
            runtime: runtime.clone(),
        }))
    }
}

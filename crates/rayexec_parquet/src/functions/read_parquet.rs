use futures::future::BoxFuture;
use rayexec_bullet::field::Schema;
use rayexec_error::{RayexecError, Result};
use rayexec_execution::{
    database::DatabaseContext,
    functions::table::{PlannedTableFunction, TableFunction, TableFunctionArgs},
    runtime::Runtime,
    storage::table_storage::DataTable,
};
use rayexec_io::location::{AccessConfig, FileLocation};
use rayexec_io::FileProvider;
use rayexec_proto::{
    packed::{PackedDecoder, PackedEncoder},
    ProtoConv,
};
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
        _context: &DatabaseContext,
        args: TableFunctionArgs,
    ) -> BoxFuture<'_, Result<Box<dyn PlannedTableFunction>>> {
        Box::pin(ReadParquetImpl::initialize(self.clone(), args))
    }

    fn decode_state(&self, state: &[u8]) -> Result<Box<dyn PlannedTableFunction>> {
        Ok(Box::new(ReadParquetImpl {
            func: self.clone(),
            state: ReadParquetState::decode(state)?,
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
    //
    // Could also just keep the bytes around and put those directly in the
    // encoded state.
    #[serde(skip)]
    metadata: Option<Arc<Metadata>>,
    schema: Schema,
}

impl ReadParquetState {
    fn encode(&self, buf: &mut Vec<u8>) -> Result<()> {
        let mut packed = PackedEncoder::new(buf);
        packed.encode_next(&self.location.to_proto()?)?;
        packed.encode_next(&self.conf.to_proto()?)?;
        packed.encode_next(&self.schema.to_proto()?)?;
        Ok(())
    }

    fn decode(buf: &[u8]) -> Result<Self> {
        let mut packed = PackedDecoder::new(buf);
        let location = FileLocation::from_proto(packed.decode_next()?)?;
        let conf = AccessConfig::from_proto(packed.decode_next()?)?;
        let schema = Schema::from_proto(packed.decode_next()?)?;
        Ok(ReadParquetState {
            location,
            conf,
            schema,
            metadata: None,
        })
    }
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
    fn table_function(&self) -> &dyn TableFunction {
        &self.func
    }

    fn schema(&self) -> Schema {
        self.state.schema.clone()
    }

    fn encode_state(&self, state: &mut Vec<u8>) -> Result<()> {
        self.state.encode(state)
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
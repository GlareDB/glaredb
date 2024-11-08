use std::sync::Arc;

use bytes::Bytes;
use futures::future::BoxFuture;
use rayexec_bullet::field::Schema;
use rayexec_error::Result;
use rayexec_execution::database::DatabaseContext;
use rayexec_execution::functions::table::{PlannedTableFunction, TableFunction, TableFunctionArgs};
use rayexec_execution::logical::statistics::StatisticsValue;
use rayexec_execution::runtime::Runtime;
use rayexec_execution::storage::table_storage::DataTable;
use rayexec_io::location::{AccessConfig, FileLocation};
use rayexec_io::FileProvider;
use rayexec_proto::packed::{PackedDecoder, PackedEncoder};
use rayexec_proto::ProtoConv;

use super::datatable::RowGroupPartitionedDataTable;
use crate::metadata::Metadata;
use crate::schema::from_parquet_schema;

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

#[derive(Debug, Clone)]
struct ReadParquetState {
    location: FileLocation,
    conf: AccessConfig,
    metadata: Arc<Metadata>,
    schema: Schema,
}

impl ReadParquetState {
    fn encode(&self, buf: &mut Vec<u8>) -> Result<()> {
        let mut packed = PackedEncoder::new(buf);
        packed.encode_next(&self.location.to_proto()?)?;
        packed.encode_next(&self.conf.to_proto()?)?;
        packed.encode_next(&self.metadata.metadata_buffer)?;
        packed.encode_next(&self.schema.to_proto()?)?;
        Ok(())
    }

    fn decode(buf: &[u8]) -> Result<Self> {
        let mut packed = PackedDecoder::new(buf);
        let location = FileLocation::from_proto(packed.decode_next()?)?;
        let conf = AccessConfig::from_proto(packed.decode_next()?)?;
        let metadata_buffer: Bytes = packed.decode_next()?;
        let schema = Schema::from_proto(packed.decode_next()?)?;

        let metadata = Arc::new(Metadata::try_from_buffer(metadata_buffer)?);

        Ok(ReadParquetState {
            location,
            conf,
            schema,
            metadata,
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

        let metadata = Metadata::new_from_source(source.as_mut(), size).await?;
        let schema = from_parquet_schema(metadata.decoded_metadata.file_metadata().schema_descr())?;

        Ok(Box::new(Self {
            func,
            state: ReadParquetState {
                location,
                conf,
                metadata: Arc::new(metadata),
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

    fn cardinality(&self) -> StatisticsValue<usize> {
        let num_rows = self
            .state
            .metadata
            .decoded_metadata
            .row_groups()
            .iter()
            .map(|g| g.num_rows())
            .sum::<i64>() as usize;

        StatisticsValue::Exact(num_rows)
    }

    fn datatable(&self) -> Result<Box<dyn DataTable>> {
        Ok(Box::new(RowGroupPartitionedDataTable {
            metadata: self.state.metadata.clone(),
            schema: self.state.schema.clone(),
            location: self.state.location.clone(),
            conf: self.state.conf.clone(),
            runtime: self.func.runtime.clone(),
        }))
    }
}

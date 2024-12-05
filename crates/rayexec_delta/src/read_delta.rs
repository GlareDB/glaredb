use std::sync::Arc;

use futures::future::BoxFuture;
use rayexec_bullet::field::Schema;
use rayexec_error::{RayexecError, Result};
use rayexec_execution::database::DatabaseContext;
use rayexec_execution::functions::table::{PlannedTableFunction, TableFunction, TableFunctionArgs};
use rayexec_execution::runtime::Runtime;
use rayexec_execution::storage::table_storage::DataTable;
use rayexec_io::location::{AccessConfig, FileLocation};
use rayexec_proto::packed::{PackedDecoder, PackedEncoder};
use rayexec_proto::ProtoConv;

use crate::datatable::DeltaDataTable;
use crate::protocol::table::Table;

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

    fn plan_and_initialize<'a>(
        &self,
        _context: &'a DatabaseContext,
        args: TableFunctionArgs,
    ) -> BoxFuture<'a, Result<Box<dyn PlannedTableFunction>>> {
        let func = self.clone();
        Box::pin(async move { ReadDeltaImpl::initialize(func, args).await })
    }

    fn decode_state(&self, state: &[u8]) -> Result<Box<dyn PlannedTableFunction>> {
        Ok(Box::new(ReadDeltaImpl {
            func: self.clone(),
            state: ReadDeltaState::decode(state)?,
        }))
    }
}

#[derive(Debug, Clone)]
struct ReadDeltaState {
    location: FileLocation,
    conf: AccessConfig,
    schema: Schema,
    table: Option<Arc<Table>>, // Populate on re-init if needed.
}

impl ReadDeltaState {
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
        Ok(ReadDeltaState {
            location,
            conf,
            schema,
            table: None,
        })
    }
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

    fn table_function(&self) -> &dyn TableFunction {
        &self.func
    }

    fn encode_state(&self, state: &mut Vec<u8>) -> Result<()> {
        self.state.encode(state)
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

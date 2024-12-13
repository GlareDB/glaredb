use std::sync::Arc;

use futures::future::BoxFuture;
use rayexec_bullet::datatype::DataTypeId;
use rayexec_bullet::field::Schema;
use rayexec_error::{not_implemented, RayexecError, Result};
use rayexec_execution::database::DatabaseContext;
use rayexec_execution::functions::table::inputs::TableFunctionInputs;
use rayexec_execution::functions::table::{PlannedTableFunction2, TableFunction};
use rayexec_execution::functions::{FunctionInfo, Signature};
use rayexec_execution::runtime::Runtime;
use rayexec_execution::storage::table_storage::DataTable;
use rayexec_io::location::{AccessConfig, FileLocation};

use crate::datatable::IcebergDataTable;
use crate::table::Table;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadIceberg<R: Runtime> {
    pub(crate) runtime: R,
}

impl<R: Runtime> FunctionInfo for ReadIceberg<R> {
    fn name(&self) -> &'static str {
        "read_iceberg"
    }

    fn aliases(&self) -> &'static [&'static str] {
        &["iceberg_scan"]
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            positional_args: &[DataTypeId::Utf8],
            variadic_arg: None,
            return_type: DataTypeId::Any,
        }]
    }
}

impl<R: Runtime> TableFunction for ReadIceberg<R> {
    fn plan_and_initialize<'a>(
        &self,
        _context: &'a DatabaseContext,
        args: TableFunctionInputs,
    ) -> BoxFuture<'a, Result<Box<dyn PlannedTableFunction2>>> {
        let func = self.clone();
        Box::pin(async move { ReadIcebergImpl::initialize(func, args).await })
    }

    fn decode_state(&self, _state: &[u8]) -> Result<Box<dyn PlannedTableFunction2>> {
        // TODO
        not_implemented!("decode iceberg state")
    }
}

#[derive(Debug, Clone)]
struct ReadIcebergState {
    _location: FileLocation,
    _conf: AccessConfig,
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
        args: TableFunctionInputs,
    ) -> Result<Box<dyn PlannedTableFunction2>> {
        let (location, conf) = args.try_location_and_access_config()?;
        let provider = func.runtime.file_provider();

        // TODO: Fetch stats, use during planning.
        let table = Table::load(location.clone(), provider, conf.clone()).await?;
        let schema = table.schema()?;

        Ok(Box::new(ReadIcebergImpl {
            func,
            state: ReadIcebergState {
                _location: location,
                _conf: conf,
                schema,
                table: Some(Arc::new(table)),
            },
        }))
    }
}

impl<R: Runtime> PlannedTableFunction2 for ReadIcebergImpl<R> {
    fn reinitialize(&self) -> BoxFuture<Result<()>> {
        // TODO: See delta
        Box::pin(async move { not_implemented!("reinit iceberg state") })
    }

    fn table_function(&self) -> &dyn TableFunction {
        &self.func
    }

    fn encode_state(&self, _state: &mut Vec<u8>) -> Result<()> {
        not_implemented!("encode iceberg state")
    }

    fn schema(&self) -> Schema {
        self.state.schema.clone()
    }

    fn datatable(&self) -> Result<Box<dyn DataTable>> {
        let table = match self.state.table.as_ref() {
            Some(table) => table.clone(),
            None => return Err(RayexecError::new("Iceberg table not initialized")),
        };

        Ok(Box::new(IcebergDataTable { table }))
    }
}

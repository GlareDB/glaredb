use futures::future::BoxFuture;
use info::{TableInfo, TPCH_TABLES};
use rayexec_bullet::field::Schema;
use rayexec_error::{RayexecError, Result};
use rayexec_execution::database::DatabaseContext;
use rayexec_execution::functions::table::{PlannedTableFunction, TableFunction, TableFunctionArgs};
use rayexec_execution::runtime::Runtime;
use rayexec_execution::storage::table_storage::DataTable;

pub mod info;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TpchGen<R: Runtime> {
    pub(crate) runtime: R,
}

impl<R: Runtime> TableFunction for TpchGen<R> {
    fn name(&self) -> &'static str {
        "tpch_gen"
    }

    fn plan_and_initialize(
        &self,
        _context: &DatabaseContext,
        args: TableFunctionArgs,
    ) -> BoxFuture<'_, Result<Box<dyn PlannedTableFunction>>> {
        Box::pin(async move { TpchTableGen::initialize(self.clone(), args) })
    }

    fn decode_state(&self, state: &[u8]) -> Result<Box<dyn PlannedTableFunction>> {
        unimplemented!()
    }
}

#[derive(Debug, Clone)]
pub struct TpchTableGen<R: Runtime> {
    table: TableInfo,
    scale: f64,
    func: TpchGen<R>,
}

impl<R: Runtime> TpchTableGen<R> {
    fn initialize(
        func: TpchGen<R>,
        args: TableFunctionArgs,
    ) -> Result<Box<dyn PlannedTableFunction>> {
        if args.positional.len() != 1 {
            return Err(RayexecError::new("Expected exactly one table name"));
        }

        let table_name = args.positional[0].clone().try_into_string()?.to_lowercase();
        let table = Self::find_table(&table_name)?;

        // TODO: Scale factor and stuff

        Ok(Box::new(TpchTableGen {
            table,
            scale: 1.0,
            func,
        }))
    }

    fn find_table(name: &str) -> Result<TableInfo> {
        TPCH_TABLES
            .iter()
            .find(|tbl| tbl.name == name)
            .copied()
            .ok_or_else(|| {
                RayexecError::new(format!(
                    "'{name}' not a valid TPC-H table, expected one of {}",
                    TPCH_TABLES
                        .iter()
                        .map(|tbl| tbl.name)
                        .collect::<Vec<_>>()
                        .join(", "),
                ))
            })
    }
}

impl<R: Runtime> PlannedTableFunction for TpchTableGen<R> {
    fn table_function(&self) -> &dyn TableFunction {
        &self.func
    }

    fn schema(&self) -> Schema {
        self.table.schema()
    }

    fn encode_state(&self, state: &mut Vec<u8>) -> Result<()> {
        unimplemented!()
    }

    fn datatable(&self) -> Result<Box<dyn DataTable>> {
        unimplemented!()
    }
}

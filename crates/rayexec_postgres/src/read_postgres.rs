use futures::future::BoxFuture;
use rayexec_bullet::field::Schema;
use rayexec_error::{OptionExt, RayexecError, Result};
use rayexec_execution::database::DatabaseContext;
use rayexec_execution::functions::table::{PlannedTableFunction, TableFunction, TableFunctionArgs};
use rayexec_execution::runtime::Runtime;
use rayexec_execution::storage::table_storage::DataTable;
use rayexec_proto::packed::{PackedDecoder, PackedEncoder};
use rayexec_proto::ProtoConv;
use serde::{Deserialize, Serialize};

use crate::{PostgresClient, PostgresDataTable};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadPostgres<R: Runtime> {
    pub(crate) runtime: R,
}

impl<R: Runtime> TableFunction for ReadPostgres<R> {
    fn name(&self) -> &'static str {
        "read_postgres"
    }

    fn plan_and_initialize(
        &self,
        _context: &DatabaseContext,
        args: TableFunctionArgs,
    ) -> BoxFuture<'_, Result<Box<dyn PlannedTableFunction>>> {
        Box::pin(ReadPostgresImpl::initialize(self.clone(), args))
    }

    fn decode_state(&self, state: &[u8]) -> Result<Box<dyn PlannedTableFunction>> {
        Ok(Box::new(ReadPostgresImpl {
            func: self.clone(),
            state: ReadPostgresState::decode(state)?,
            client: None,
        }))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ReadPostgresState {
    conn_str: String,
    schema: String,
    table: String,
    table_schema: Schema,
}

impl ReadPostgresState {
    fn encode(&self, buf: &mut Vec<u8>) -> Result<()> {
        let mut packed = PackedEncoder::new(buf);
        packed.encode_next(&self.conn_str)?;
        packed.encode_next(&self.schema)?;
        packed.encode_next(&self.table)?;
        packed.encode_next(&self.table_schema.to_proto()?)?;
        Ok(())
    }

    fn decode(buf: &[u8]) -> Result<Self> {
        let mut packed = PackedDecoder::new(buf);
        Ok(ReadPostgresState {
            conn_str: packed.decode_next()?,
            schema: packed.decode_next()?,
            table: packed.decode_next()?,
            table_schema: Schema::from_proto(packed.decode_next()?)?,
        })
    }
}

#[derive(Debug, Clone)]
struct ReadPostgresImpl<R: Runtime> {
    func: ReadPostgres<R>,
    state: ReadPostgresState,
    client: Option<PostgresClient>,
}

impl<R> ReadPostgresImpl<R>
where
    R: Runtime,
{
    async fn initialize(
        func: ReadPostgres<R>,
        args: TableFunctionArgs,
    ) -> Result<Box<dyn PlannedTableFunction>> {
        if !args.named.is_empty() {
            return Err(RayexecError::new(
                "read_postgres does not accept named arguments",
            ));
        }
        if args.positional.len() != 3 {
            return Err(RayexecError::new("read_postgres requires 3 arguments"));
        }

        let mut args = args.clone();
        let table = args.positional.pop().unwrap().try_into_string()?;
        let schema = args.positional.pop().unwrap().try_into_string()?;
        let conn_str = args.positional.pop().unwrap().try_into_string()?;

        let client = PostgresClient::connect(&conn_str, &func.runtime).await?;

        let fields = match client.get_fields_and_types(&schema, &table).await? {
            Some((fields, _)) => fields,
            None => return Err(RayexecError::new("Table not found")),
        };

        let table_schema = Schema::new(fields);

        Ok(Box::new(ReadPostgresImpl {
            func,
            state: ReadPostgresState {
                conn_str,
                schema,
                table,
                table_schema,
            },
            client: Some(client),
        }))
    }
}

impl<R> PlannedTableFunction for ReadPostgresImpl<R>
where
    R: Runtime,
{
    fn table_function(&self) -> &dyn TableFunction {
        &self.func
    }

    fn schema(&self) -> Schema {
        self.state.table_schema.clone()
    }

    fn encode_state(&self, state: &mut Vec<u8>) -> Result<()> {
        self.state.encode(state)
    }

    fn datatable(&self) -> Result<Box<dyn DataTable>> {
        Ok(Box::new(PostgresDataTable {
            client: self.client.as_ref().required("postgres client")?.clone(),
            schema: self.state.schema.clone(),
            table: self.state.table.clone(),
        }))
    }
}

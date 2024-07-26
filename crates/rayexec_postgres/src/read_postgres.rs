use futures::future::BoxFuture;
use rayexec_bullet::field::Schema;
use rayexec_error::{RayexecError, Result};
use rayexec_execution::{
    database::table::DataTable,
    functions::table::{PlannedTableFunction, TableFunction, TableFunctionArgs},
    runtime::Runtime,
};
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
        args: TableFunctionArgs,
    ) -> BoxFuture<'_, Result<Box<dyn PlannedTableFunction>>> {
        Box::pin(ReadPostgresImpl::initialize(self.clone(), args))
    }

    fn state_deserialize(
        &self,
        deserializer: &mut dyn erased_serde::Deserializer,
    ) -> Result<Box<dyn PlannedTableFunction>> {
        let state = ReadPostgresState::deserialize(deserializer)?;
        Ok(Box::new(ReadPostgresImpl {
            func: self.clone(),
            state,
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct ReadPostgresImpl<R: Runtime> {
    func: ReadPostgres<R>,
    state: ReadPostgresState,
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
        }))
    }
}

impl<R> PlannedTableFunction for ReadPostgresImpl<R>
where
    R: Runtime,
{
    fn serializable_state(&self) -> &dyn erased_serde::Serialize {
        &self.state
    }

    fn table_function(&self) -> &dyn TableFunction {
        &self.func
    }

    fn schema(&self) -> Schema {
        self.state.table_schema.clone()
    }

    fn datatable(&self) -> Result<Box<dyn DataTable>> {
        Ok(Box::new(PostgresDataTable {
            runtime: self.func.runtime.clone(),
            conn_str: self.state.conn_str.clone(),
            schema: self.state.schema.clone(),
            table: self.state.table.clone(),
        }))
    }
}

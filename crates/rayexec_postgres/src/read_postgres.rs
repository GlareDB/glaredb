use futures::future::BoxFuture;
use rayexec_bullet::field::Schema;
use rayexec_error::{RayexecError, Result};
use rayexec_execution::{
    database::table::DataTable,
    functions::table::{
        GenericTableFunction, InitializedTableFunction, SpecializedTableFunction, TableFunctionArgs,
    },
    runtime::ExecutionRuntime,
};
use std::sync::Arc;

use crate::{PostgresClient, PostgresDataTable};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadPostgres;

impl GenericTableFunction for ReadPostgres {
    fn name(&self) -> &'static str {
        "read_postgres"
    }

    fn specialize(&self, args: TableFunctionArgs) -> Result<Box<dyn SpecializedTableFunction>> {
        let args = ReadPostgresArgs::try_from_table_args(args)?;
        Ok(Box::new(args))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ReadPostgresArgs {
    conn_str: String,
    schema: String,
    table: String,
}

impl ReadPostgresArgs {
    fn try_from_table_args(args: TableFunctionArgs) -> Result<Self> {
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

        Ok(ReadPostgresArgs {
            conn_str,
            schema,
            table,
        })
    }
}

impl SpecializedTableFunction for ReadPostgresArgs {
    fn name(&self) -> &'static str {
        "read_postgres"
    }

    fn initialize(
        self: Box<Self>,
        runtime: &Arc<dyn ExecutionRuntime>,
    ) -> BoxFuture<Result<Box<dyn InitializedTableFunction>>> {
        let client = PostgresClient::connect(self.conn_str.clone(), runtime.as_ref());
        Box::pin(async move {
            let client = client.await?;
            let fields = match client
                .get_fields_and_types(&self.schema, &self.table)
                .await?
            {
                Some((fields, _)) => fields,
                None => return Err(RayexecError::new("Table not found")),
            };

            let schema = Schema::new(fields);

            Ok(Box::new(ReadPostgresImpl {
                args: *self,
                _client: client,
                table_schema: schema,
            }) as _)
        })
    }
}

#[derive(Debug, Clone)]
pub struct ReadPostgresImpl {
    args: ReadPostgresArgs,
    _client: PostgresClient,
    table_schema: Schema,
}

impl InitializedTableFunction for ReadPostgresImpl {
    fn specialized(&self) -> &dyn SpecializedTableFunction {
        &self.args
    }

    fn schema(&self) -> Schema {
        self.table_schema.clone()
    }

    fn datatable(&self, runtime: &Arc<dyn ExecutionRuntime>) -> Result<Box<dyn DataTable>> {
        Ok(Box::new(PostgresDataTable {
            runtime: runtime.clone(),
            conn_str: self.args.conn_str.clone(),
            schema: self.args.schema.clone(),
            table: self.args.table.clone(),
        }))
    }
}

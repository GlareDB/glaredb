use futures::future::BoxFuture;
use rayexec_bullet::field::Schema;
use rayexec_error::{RayexecError, Result};
use rayexec_execution::{
    database::table::DataTable,
    engine::EngineRuntime,
    functions::table::{GenericTableFunction, SpecializedTableFunction, TableFunctionArgs},
};
use std::sync::Arc;

use crate::{PostgresClient, PostgresDataTable};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadPostgres;

impl GenericTableFunction for ReadPostgres {
    fn name(&self) -> &'static str {
        "read_postgres"
    }

    fn specialize(&self, args: &TableFunctionArgs) -> Result<Box<dyn SpecializedTableFunction>> {
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

        Ok(Box::new(ReadPostgresImpl {
            conn_str,
            schema,
            table,
        }))
    }
}

#[derive(Debug, Clone)]
pub struct ReadPostgresImpl {
    conn_str: String,
    schema: String,
    table: String,
}

impl SpecializedTableFunction for ReadPostgresImpl {
    fn schema<'a>(&'a mut self, runtime: &'a EngineRuntime) -> BoxFuture<Result<Schema>> {
        let client = PostgresClient::connect(&self.conn_str, runtime);
        Box::pin(async {
            let client = client.await?;
            let fields = match client
                .get_fields_and_types(&self.schema, &self.table)
                .await?
            {
                Some((fields, _)) => fields,
                None => return Err(RayexecError::new("Table not found")),
            };

            Ok(Schema::new(fields))
        })
    }

    fn datatable(&mut self, runtime: &Arc<EngineRuntime>) -> Result<Box<dyn DataTable>> {
        Ok(Box::new(PostgresDataTable {
            runtime: runtime.clone(),
            conn_str: self.conn_str.clone(),
            schema: self.schema.clone(),
            table: self.table.clone(),
        }))
    }
}

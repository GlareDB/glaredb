use futures::future::BoxFuture;
use rayexec_bullet::field::Schema;
use rayexec_error::{RayexecError, Result};
use rayexec_execution::{
    database::table::DataTable,
    functions::table::{PlannedTableFunction, TableFunction, TableFunctionArgs},
    runtime::ExecutionRuntime,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::{PostgresClient, PostgresDataTable};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadPostgres;

impl TableFunction for ReadPostgres {
    fn name(&self) -> &'static str {
        "read_postgres"
    }

    fn plan_and_initialize<'a>(
        &'a self,
        runtime: &'a Arc<dyn ExecutionRuntime>,
        args: TableFunctionArgs,
    ) -> BoxFuture<'a, Result<Box<dyn PlannedTableFunction>>> {
        Box::pin(ReadPostgresImpl::initialize(runtime.as_ref(), args))
    }

    fn state_deserialize(
        &self,
        deserializer: &mut dyn erased_serde::Deserializer,
    ) -> Result<Box<dyn PlannedTableFunction>> {
        Ok(Box::new(ReadPostgresImpl::deserialize(deserializer)?))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ReadPostgresImpl {
    conn_str: String,
    schema: String,
    table: String,
    table_schema: Schema,
}

impl ReadPostgresImpl {
    async fn initialize(
        runtime: &dyn ExecutionRuntime,
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

        let client = PostgresClient::connect(&conn_str, runtime).await?;

        let fields = match client.get_fields_and_types(&schema, &table).await? {
            Some((fields, _)) => fields,
            None => return Err(RayexecError::new("Table not found")),
        };

        let table_schema = Schema::new(fields);

        Ok(Box::new(ReadPostgresImpl {
            conn_str,
            schema,
            table,
            table_schema,
        }))
    }
}

impl PlannedTableFunction for ReadPostgresImpl {
    fn serializable_state(&self) -> &dyn erased_serde::Serialize {
        self
    }

    fn table_function(&self) -> &dyn TableFunction {
        &ReadPostgres
    }

    fn schema(&self) -> Schema {
        self.table_schema.clone()
    }

    fn datatable(&self, runtime: &Arc<dyn ExecutionRuntime>) -> Result<Box<dyn DataTable>> {
        Ok(Box::new(PostgresDataTable {
            runtime: runtime.clone(),
            conn_str: self.conn_str.clone(),
            schema: self.schema.clone(),
            table: self.table.clone(),
        }))
    }
}

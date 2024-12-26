use std::collections::HashMap;
use std::sync::Arc;

use futures::future::BoxFuture;
use futures::FutureExt;
use rayexec_error::{RayexecError, Result};
use rayexec_execution::arrays::datatype::DataTypeId;
use rayexec_execution::arrays::field::Schema;
use rayexec_execution::arrays::scalar::OwnedScalarValue;
use rayexec_execution::database::DatabaseContext;
use rayexec_execution::expr;
use rayexec_execution::functions::table::{
    PlannedTableFunction,
    ScanPlanner,
    TableFunction,
    TableFunctionImpl,
    TableFunctionPlanner,
};
use rayexec_execution::functions::{FunctionInfo, Signature};
use rayexec_execution::logical::statistics::StatisticsValue;
use rayexec_execution::runtime::Runtime;

use crate::{PostgresClient, PostgresDataTable};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadPostgres<R: Runtime> {
    pub(crate) runtime: R,
}

impl<R: Runtime> FunctionInfo for ReadPostgres<R> {
    fn name(&self) -> &'static str {
        "read_postgres"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            positional_args: &[DataTypeId::Utf8, DataTypeId::Utf8, DataTypeId::Utf8],
            variadic_arg: None,
            return_type: DataTypeId::Any,
            doc: None,
        }]
    }
}

impl<R: Runtime> TableFunction for ReadPostgres<R> {
    fn planner(&self) -> TableFunctionPlanner {
        TableFunctionPlanner::Scan(self)
    }
}

impl<R: Runtime> ScanPlanner for ReadPostgres<R> {
    fn plan<'a>(
        &self,
        context: &'a DatabaseContext,
        positional_inputs: Vec<OwnedScalarValue>,
        named_inputs: HashMap<String, OwnedScalarValue>,
    ) -> BoxFuture<'a, Result<PlannedTableFunction>> {
        Self::plan_inner(self.clone(), context, positional_inputs, named_inputs).boxed()
    }
}

impl<R: Runtime> ReadPostgres<R> {
    async fn plan_inner(
        self,
        _context: &DatabaseContext,
        positional_inputs: Vec<OwnedScalarValue>,
        named_inputs: HashMap<String, OwnedScalarValue>,
    ) -> Result<PlannedTableFunction> {
        if !named_inputs.is_empty() {
            return Err(RayexecError::new(
                "read_postgres does not accept named arguments",
            ));
        }
        if positional_inputs.len() != 3 {
            return Err(RayexecError::new("read_postgres requires 3 arguments"));
        }

        let conn_str = positional_inputs.first().unwrap().try_as_str()?;
        let schema = positional_inputs.get(1).unwrap().try_as_str()?;
        let table = positional_inputs.get(2).unwrap().try_as_str()?;

        let client = PostgresClient::connect(conn_str, &self.runtime).await?;

        let fields = match client.get_fields_and_types(schema, table).await? {
            Some((fields, _)) => fields,
            None => return Err(RayexecError::new("Table not found")),
        };

        let table_schema = Schema::new(fields);

        let datatable = PostgresDataTable {
            client,
            schema: schema.to_string(),
            table: table.to_string(),
        };

        Ok(PlannedTableFunction {
            function: Box::new(self),
            positional_inputs: positional_inputs.into_iter().map(expr::lit).collect(),
            named_inputs,
            function_impl: TableFunctionImpl::Scan(Arc::new(datatable)),
            cardinality: StatisticsValue::Unknown,
            schema: table_schema,
        })
    }
}

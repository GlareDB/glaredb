use std::collections::HashMap;
use std::sync::Arc;

use futures::future::BoxFuture;
use futures::FutureExt;
use rayexec_bullet::datatype::DataTypeId;
use rayexec_bullet::scalar::OwnedScalarValue;
use rayexec_error::Result;
use rayexec_execution::database::DatabaseContext;
use rayexec_execution::expr;
use rayexec_execution::functions::table::{
    try_location_and_access_config_from_args,
    PlannedTableFunction,
    ScanPlanner,
    TableFunction,
    TableFunctionImpl,
    TableFunctionPlanner,
};
use rayexec_execution::functions::{FunctionInfo, Signature};
use rayexec_execution::logical::statistics::StatisticsValue;
use rayexec_execution::runtime::Runtime;

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
    fn planner(&self) -> TableFunctionPlanner {
        TableFunctionPlanner::Scan(self)
    }
}

impl<R: Runtime> ScanPlanner for ReadIceberg<R> {
    fn plan<'a>(
        &self,
        context: &'a DatabaseContext,
        positional_inputs: Vec<OwnedScalarValue>,
        named_inputs: HashMap<String, OwnedScalarValue>,
    ) -> BoxFuture<'a, Result<PlannedTableFunction>> {
        Self::plan_inner(self.clone(), context, positional_inputs, named_inputs).boxed()
    }
}

impl<R: Runtime> ReadIceberg<R> {
    async fn plan_inner<'a>(
        self,
        _context: &'a DatabaseContext,
        positional_inputs: Vec<OwnedScalarValue>,
        named_inputs: HashMap<String, OwnedScalarValue>,
    ) -> Result<PlannedTableFunction> {
        let (location, conf) =
            try_location_and_access_config_from_args(&self, &positional_inputs, &named_inputs)?;
        let provider = self.runtime.file_provider();

        // TODO: Fetch stats, use during planning.
        let table = Table::load(location.clone(), provider, conf.clone()).await?;
        let schema = table.schema()?;

        Ok(PlannedTableFunction {
            function: Box::new(self),
            positional_inputs: positional_inputs.into_iter().map(expr::lit).collect(),
            named_inputs,
            function_impl: TableFunctionImpl::Scan(Arc::new(IcebergDataTable {
                table: Arc::new(table), // TODO: Arc Arc
            })),
            cardinality: StatisticsValue::Unknown,
            schema,
        })
    }
}

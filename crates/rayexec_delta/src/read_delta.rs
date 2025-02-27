use std::collections::HashMap;
use std::sync::Arc;

use futures::future::BoxFuture;
use futures::FutureExt;
use rayexec_error::Result;
use rayexec_execution::arrays::datatype::DataTypeId;
use rayexec_execution::arrays::scalar::ScalarValue;
use rayexec_execution::database::DatabaseContext;
use rayexec_execution::expr;
use rayexec_execution::functions::table::{
    try_location_and_access_config_from_args,
    PlannedTableFunction2,
    ScanPlanner2,
    TableFunction2,
    TableFunctionImpl2,
    TableFunctionPlanner2,
};
use rayexec_execution::functions::{FunctionInfo, Signature};
use rayexec_execution::logical::statistics::StatisticsValue;
use rayexec_execution::runtime::Runtime;

use crate::datatable::DeltaDataTable;
use crate::protocol::table::Table;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadDelta<R: Runtime> {
    pub(crate) runtime: R,
}

impl<R: Runtime> FunctionInfo for ReadDelta<R> {
    fn name(&self) -> &'static str {
        "read_delta"
    }

    fn aliases(&self) -> &'static [&'static str] {
        &["delta_scan"]
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            positional_args: &[DataTypeId::Utf8],
            variadic_arg: None,
            return_type: DataTypeId::Any,
            doc: None,
        }]
    }
}

impl<R: Runtime> TableFunction2 for ReadDelta<R> {
    fn planner(&self) -> TableFunctionPlanner2 {
        TableFunctionPlanner2::Scan(self)
    }
}

impl<R: Runtime> ScanPlanner2 for ReadDelta<R> {
    fn plan<'a>(
        &self,
        context: &'a DatabaseContext,
        positional_inputs: Vec<ScalarValue>,
        named_inputs: HashMap<String, ScalarValue>,
    ) -> BoxFuture<'a, Result<PlannedTableFunction2>> {
        Self::plan_inner(self.clone(), context, positional_inputs, named_inputs).boxed()
    }
}

impl<R: Runtime> ReadDelta<R> {
    async fn plan_inner(
        self,
        _context: &DatabaseContext,
        positional_inputs: Vec<ScalarValue>,
        named_inputs: HashMap<String, ScalarValue>,
    ) -> Result<PlannedTableFunction2> {
        let (location, conf) =
            try_location_and_access_config_from_args(&self, &positional_inputs, &named_inputs)?;

        let provider = self.runtime.file_provider();

        let table = Table::load(location.clone(), provider, conf.clone()).await?;
        let schema = table.table_schema()?;

        Ok(PlannedTableFunction2 {
            function: Box::new(self),
            positional: positional_inputs.into_iter().map(expr::lit).collect(),
            named: named_inputs,
            function_impl: TableFunctionImpl2::Scan(Arc::new(DeltaDataTable {
                table: Arc::new(table), // TODO: Arc Arc
            })),
            cardinality: StatisticsValue::Unknown,
            schema,
        })
    }
}

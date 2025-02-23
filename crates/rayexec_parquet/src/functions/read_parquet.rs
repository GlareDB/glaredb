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
    PlannedTableFunction,
    ScanPlanner,
    TableFunction,
    TableFunctionImpl,
    TableFunctionPlanner,
};
use rayexec_execution::functions::{FunctionInfo, Signature};
use rayexec_execution::logical::statistics::StatisticsValue;
use rayexec_execution::runtime::Runtime;
use rayexec_io::FileProvider2;

use super::datatable::RowGroupPartitionedDataTable;
use crate::metadata::Metadata;
use crate::schema::from_parquet_schema;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadParquet<R: Runtime> {
    pub(crate) runtime: R,
}

impl<R: Runtime> FunctionInfo for ReadParquet<R> {
    fn name(&self) -> &'static str {
        "read_parquet"
    }

    fn aliases(&self) -> &'static [&'static str] {
        &["parquet_scan"]
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

impl<R: Runtime> TableFunction for ReadParquet<R> {
    fn planner(&self) -> TableFunctionPlanner {
        TableFunctionPlanner::Scan(self)
    }
}

impl<R: Runtime> ScanPlanner for ReadParquet<R> {
    fn plan<'a>(
        &self,
        context: &'a DatabaseContext,
        positional_inputs: Vec<ScalarValue>,
        named_inputs: HashMap<String, ScalarValue>,
    ) -> BoxFuture<'a, Result<PlannedTableFunction>> {
        Self::plan_inner(self.clone(), context, positional_inputs, named_inputs).boxed()
    }
}

impl<R: Runtime> ReadParquet<R> {
    async fn plan_inner(
        self,
        _context: &DatabaseContext,
        positional_inputs: Vec<ScalarValue>,
        named_inputs: HashMap<String, ScalarValue>,
    ) -> Result<PlannedTableFunction> {
        let (location, conf) =
            try_location_and_access_config_from_args(&self, &positional_inputs, &named_inputs)?;

        unimplemented!()
        // let mut source = self
        //     .runtime
        //     .file_provider()
        //     .file_source(location.clone(), &conf)?;

        // let size = source.size().await?;

        // let metadata = Metadata::new_from_source(source.as_mut(), size).await?;
        // let schema = from_parquet_schema(metadata.decoded_metadata.file_metadata().schema_descr())?;

        // let num_rows = metadata
        //     .decoded_metadata
        //     .row_groups()
        //     .iter()
        //     .map(|g| g.num_rows())
        //     .sum::<i64>() as usize;

        // let datatable = RowGroupPartitionedDataTable {
        //     metadata: Arc::new(metadata),
        //     schema: schema.clone(),
        //     location,
        //     conf,
        //     runtime: self.runtime.clone(),
        // };

        // Ok(PlannedTableFunction {
        //     function: Box::new(self),
        //     positional_inputs: positional_inputs.into_iter().map(expr::lit).collect(),
        //     named_inputs,
        //     function_impl: TableFunctionImpl::Scan(Arc::new(datatable)),
        //     cardinality: StatisticsValue::Exact(num_rows),
        //     schema,
        // })
    }
}

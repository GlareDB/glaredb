use std::collections::HashMap;
use std::sync::Arc;

use futures::future::BoxFuture;
use futures::{FutureExt, StreamExt};
use rayexec_bullet::datatype::DataTypeId;
use rayexec_bullet::scalar::OwnedScalarValue;
use rayexec_error::{RayexecError, Result};
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
use rayexec_io::{FileProvider, FileSource};

use crate::datatable::SingleFileCsvDataTable;
use crate::decoder::{CsvDecoder, DecoderState};
use crate::reader::{CsvSchema, DialectOptions};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadCsv<R: Runtime> {
    pub(crate) runtime: R,
}

impl<R: Runtime> FunctionInfo for ReadCsv<R> {
    fn name(&self) -> &'static str {
        "read_csv"
    }

    fn aliases(&self) -> &'static [&'static str] {
        &["csv_scan"]
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            positional_args: &[DataTypeId::Utf8],
            variadic_arg: None,
            return_type: DataTypeId::Any,
        }]
    }
}

impl<R: Runtime> TableFunction for ReadCsv<R> {
    fn planner(&self) -> TableFunctionPlanner {
        TableFunctionPlanner::Scan(self)
    }
}

impl<R: Runtime> ScanPlanner for ReadCsv<R> {
    fn plan<'a>(
        &self,
        context: &'a DatabaseContext,
        positional_inputs: Vec<OwnedScalarValue>,
        named_inputs: HashMap<String, OwnedScalarValue>,
    ) -> BoxFuture<'a, Result<PlannedTableFunction>> {
        Self::plan_inner(self.clone(), context, positional_inputs, named_inputs).boxed()
    }
}

impl<R: Runtime> ReadCsv<R> {
    async fn plan_inner<'a>(
        self: Self,
        _context: &'a DatabaseContext,
        positional_inputs: Vec<OwnedScalarValue>,
        named_inputs: HashMap<String, OwnedScalarValue>,
    ) -> Result<PlannedTableFunction> {
        let (location, conf) =
            try_location_and_access_config_from_args(&self, &positional_inputs, &named_inputs)?;

        let mut source = self
            .runtime
            .file_provider()
            .file_source(location.clone(), &conf)?;

        let mut stream = source.read_stream();
        // TODO: Actually make sure this is a sufficient size to infer from.
        // TODO: This throws away the buffer after inferring.
        let infer_buf = match stream.next().await {
            Some(result) => {
                const INFER_SIZE: usize = 1024;
                let buf = result?;
                if buf.len() > INFER_SIZE {
                    buf.slice(0..INFER_SIZE)
                } else {
                    buf
                }
            }
            None => return Err(RayexecError::new("Stream returned no data")),
        };

        let dialect = DialectOptions::infer_from_sample(&infer_buf)?;
        let mut decoder = CsvDecoder::new(dialect);
        let mut state = DecoderState::default();
        let _ = decoder.decode(&infer_buf, &mut state)?;
        let completed = state.completed_records();
        let csv_schema = CsvSchema::infer_from_records(completed)?;

        let schema = csv_schema.schema.clone();

        let datatable = SingleFileCsvDataTable {
            options: dialect,
            csv_schema,
            location,
            conf,
            runtime: self.runtime.clone(),
        };

        Ok(PlannedTableFunction {
            function: Box::new(self),
            positional_inputs: positional_inputs.into_iter().map(expr::lit).collect(),
            named_inputs,
            function_impl: TableFunctionImpl::Scan(Arc::new(datatable)),
            cardinality: StatisticsValue::Unknown,
            schema,
        })
    }
}

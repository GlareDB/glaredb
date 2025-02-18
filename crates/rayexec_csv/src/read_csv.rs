use std::collections::HashMap;
use std::sync::Arc;

use futures::future::BoxFuture;
use futures::{FutureExt, StreamExt};
use rayexec_error::{RayexecError, Result};
use rayexec_execution::arrays::datatype::DataTypeId;
use rayexec_execution::arrays::scalar::OwnedScalarValue;
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
use rayexec_io::exp::FileProvider;
use rayexec_io::future::read_into::ReadInto;
use rayexec_io::{FileProvider2, FileSource};

use crate::decoder::{ByteRecords, CsvDecoder};
use crate::dialect::DialectOptions;
use crate::scan::SingleFileCsvScan;

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
            doc: None,
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
    async fn plan_inner(
        self,
        _context: &DatabaseContext,
        positional_inputs: Vec<OwnedScalarValue>,
        named_inputs: HashMap<String, OwnedScalarValue>,
    ) -> Result<PlannedTableFunction> {
        let (location, conf) =
            try_location_and_access_config_from_args(&self, &positional_inputs, &named_inputs)?;

        let mut source = self
            .runtime
            .file_provider()
            .file_source(location.clone(), &conf)?;

        let mut stream = source.read();

        // TODO: Actually make sure this is a sufficient size to infer from.
        //
        // TODO: This throws away the buffer after inferring. We could instead
        // keep both the buffer and stream on the returned table function impl,
        // which would let us continue to read where we left off.
        const READ_BUFFER_SIZE: usize = 1024;
        let mut buf = vec![0; READ_BUFFER_SIZE];
        let count = ReadInto::new(&mut stream, &mut buf).await?;

        let buf = &buf[0..count];
        let mut output = ByteRecords::with_buffer_capacity(READ_BUFFER_SIZE);

        let dialect = DialectOptions::infer_from_sample(buf, &mut output).unwrap_or_default();
        let mut decoder = CsvDecoder::new(dialect);
        // let mut state = DecoderState::default();
        // let _ = decoder.decode(buf, &mut state)?;
        // let completed = state.completed_records();
        unimplemented!()
        // let csv_schema = CsvSchema::infer_from_records(completed)?;

        // let schema = csv_schema.schema.clone();

        // let scan = SingleFileCsvScan {
        //     options: dialect,
        //     csv_schema,
        //     location,
        //     conf,
        //     runtime: self.runtime.clone(),
        // };

        // Ok(PlannedTableFunction {
        //     function: Box::new(self),
        //     positional_inputs: positional_inputs.into_iter().map(expr::lit).collect(),
        //     named_inputs,
        //     function_impl: TableFunctionImpl::new_scan(scan),
        //     cardinality: StatisticsValue::Unknown,
        //     schema,
        // })
    }
}

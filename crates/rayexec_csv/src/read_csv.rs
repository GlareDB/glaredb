use std::collections::HashMap;

use futures::future::BoxFuture;
use futures::FutureExt;
use rayexec_error::Result;
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

use crate::decoder::{ByteRecords, CsvDecoder};
use crate::dialect::DialectOptions;
use crate::scan::{FileScan, SingleFileCsvScan};
use crate::schema::CsvSchema;

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

        // Read a sample from the stream to attempt to infer options.
        //
        // TODO: Actually make sure this is a sufficient size to infer from.
        const READ_BUFFER_SIZE: usize = 16 * 1024;
        let mut buf = vec![0; READ_BUFFER_SIZE];
        let count = ReadInto::new(&mut stream, &mut buf).await?;

        let infer_buf = &buf[0..count];
        let mut output = ByteRecords::with_buffer_capacity(READ_BUFFER_SIZE);

        // Infer the options.
        let dialect = DialectOptions::infer_from_sample(infer_buf, &mut output).unwrap_or_default();

        let mut decoder = CsvDecoder::new(dialect);
        decoder.decode(infer_buf, &mut output);

        // Infer the schema.
        let schema = CsvSchema::infer_from_records(&output)?;

        // Note that we're creating the scan with the same buffers/streams we
        // used for inferring. When we create the reader, `output` will already
        // have decoded records that the reader will process. And then it'll
        // move on to pulling from the stream which will continue where we
        // stopped.
        let scan = SingleFileCsvScan {
            inner: Some(FileScan {
                stream,
                skip_header: schema.has_header,
                read_buffer: buf,
                decoder,
                output,
            }),
        };

        Ok(PlannedTableFunction {
            function: Box::new(self),
            positional_inputs: positional_inputs.into_iter().map(expr::lit).collect(),
            named_inputs,
            function_impl: TableFunctionImpl::new_scan(scan),
            cardinality: StatisticsValue::Unknown,
            schema: schema.schema,
        })
    }
}

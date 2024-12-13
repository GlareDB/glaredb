use futures::future::BoxFuture;
use futures::StreamExt;
use rayexec_bullet::datatype::DataTypeId;
use rayexec_bullet::field::Schema;
use rayexec_error::{RayexecError, Result};
use rayexec_execution::database::DatabaseContext;
use rayexec_execution::functions::table::inputs::TableFunctionInputs;
use rayexec_execution::functions::table::{PlannedTableFunction2, TableFunction};
use rayexec_execution::functions::{FunctionInfo, Signature};
use rayexec_execution::runtime::Runtime;
use rayexec_execution::storage::table_storage::DataTable;
use rayexec_io::location::{AccessConfig, FileLocation};
use rayexec_io::{FileProvider, FileSource};
use rayexec_proto::packed::{PackedDecoder, PackedEncoder};
use rayexec_proto::ProtoConv;
use serde::{Deserialize, Serialize};

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
    fn plan_and_initialize<'a>(
        &self,
        _context: &'a DatabaseContext,
        args: TableFunctionInputs,
    ) -> BoxFuture<'a, Result<Box<dyn PlannedTableFunction2>>> {
        Box::pin(ReadCsvImpl::initialize(self.clone(), args))
    }

    fn decode_state(&self, state: &[u8]) -> Result<Box<dyn PlannedTableFunction2>> {
        let state = ReadCsvState::decode(state)?;
        Ok(Box::new(ReadCsvImpl {
            func: self.clone(),
            state,
        }))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ReadCsvState {
    location: FileLocation,
    conf: AccessConfig,
    csv_schema: CsvSchema,
    dialect: DialectOptions,
}

impl ReadCsvState {
    fn encode(&self, buf: &mut Vec<u8>) -> Result<()> {
        let mut packed = PackedEncoder::new(buf);
        packed.encode_next(&self.location.to_proto()?)?;
        packed.encode_next(&self.conf.to_proto()?)?;
        packed.encode_next(&self.csv_schema.schema.to_proto()?)?;
        packed.encode_next(&self.csv_schema.has_header)?;
        packed.encode_next(&self.csv_schema.has_header)?;
        packed.encode_next(&(self.dialect.delimiter as i32))?;
        packed.encode_next(&(self.dialect.quote as i32))?;
        Ok(())
    }

    fn decode(buf: &[u8]) -> Result<Self> {
        let mut packed = PackedDecoder::new(buf);
        let location = FileLocation::from_proto(packed.decode_next()?)?;
        let conf = AccessConfig::from_proto(packed.decode_next()?)?;
        let schema = Schema::from_proto(packed.decode_next()?)?;
        let has_header: bool = packed.decode_next()?;
        let delimiter: i32 = packed.decode_next()?;
        let quote: i32 = packed.decode_next()?;

        Ok(ReadCsvState {
            location,
            conf,
            csv_schema: CsvSchema { schema, has_header },
            dialect: DialectOptions {
                delimiter: delimiter as u8,
                quote: quote as u8,
            },
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ReadCsvImpl<R: Runtime> {
    func: ReadCsv<R>,
    state: ReadCsvState,
}

impl<R: Runtime> ReadCsvImpl<R> {
    async fn initialize(
        func: ReadCsv<R>,
        args: TableFunctionInputs,
    ) -> Result<Box<dyn PlannedTableFunction2>> {
        let (location, conf) = args.try_location_and_access_config()?;

        let mut source = func
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

        Ok(Box::new(Self {
            func,
            state: ReadCsvState {
                location,
                conf,
                dialect,
                csv_schema,
            },
        }))
    }
}

impl<R: Runtime> PlannedTableFunction2 for ReadCsvImpl<R> {
    fn table_function(&self) -> &dyn TableFunction {
        &self.func
    }

    fn encode_state(&self, state: &mut Vec<u8>) -> Result<()> {
        self.state.encode(state)
    }

    fn schema(&self) -> Schema {
        self.state.csv_schema.schema.clone()
    }

    fn datatable(&self) -> Result<Box<dyn DataTable>> {
        Ok(Box::new(SingleFileCsvDataTable {
            options: self.state.dialect,
            csv_schema: self.state.csv_schema.clone(),
            location: self.state.location.clone(),
            conf: self.state.conf.clone(),
            runtime: self.func.runtime.clone(),
        }))
    }
}

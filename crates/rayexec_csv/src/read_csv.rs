use futures::{future::BoxFuture, StreamExt};
use rayexec_bullet::field::Schema;
use rayexec_error::{RayexecError, Result};
use rayexec_execution::{
    database::table::DataTable,
    functions::table::{PlannedTableFunction, TableFunction, TableFunctionArgs},
    runtime::Runtime,
};
use rayexec_io::{
    location::{AccessConfig, FileLocation},
    FileProvider, FileSource,
};
use serde::{Deserialize, Serialize};

use crate::{
    datatable::SingleFileCsvDataTable,
    decoder::{CsvDecoder, DecoderState},
    reader::{CsvSchema, DialectOptions},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadCsv<R: Runtime> {
    pub(crate) runtime: R,
}

impl<R: Runtime> TableFunction for ReadCsv<R> {
    fn name(&self) -> &'static str {
        "read_csv"
    }

    fn aliases(&self) -> &'static [&'static str] {
        &["csv_scan"]
    }

    fn plan_and_initialize(
        &self,
        args: TableFunctionArgs,
    ) -> BoxFuture<'_, Result<Box<dyn PlannedTableFunction>>> {
        Box::pin(ReadCsvImpl::initialize(self.clone(), args))
    }

    fn state_deserialize(
        &self,
        deserializer: &mut dyn erased_serde::Deserializer,
    ) -> Result<Box<dyn PlannedTableFunction>> {
        let state = ReadCsvState::deserialize(deserializer)?;
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct ReadCsvImpl<R: Runtime> {
    func: ReadCsv<R>,
    state: ReadCsvState,
}

impl<R: Runtime> ReadCsvImpl<R> {
    async fn initialize(
        func: ReadCsv<R>,
        args: TableFunctionArgs,
    ) -> Result<Box<dyn PlannedTableFunction>> {
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

impl<R: Runtime> PlannedTableFunction for ReadCsvImpl<R> {
    fn serializable_state(&self) -> &dyn erased_serde::Serialize {
        &self.state
    }

    fn table_function(&self) -> &dyn TableFunction {
        &self.func
    }

    fn schema(&self) -> Schema {
        Schema {
            fields: self.state.csv_schema.fields.clone(),
        }
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

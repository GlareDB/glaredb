use futures::{future::BoxFuture, StreamExt};
use rayexec_bullet::field::Schema;
use rayexec_error::{RayexecError, Result};
use rayexec_execution::{
    database::table::DataTable,
    functions::table::{PlannedTableFunction, TableFunction, TableFunctionArgs},
    runtime::ExecutionRuntime,
};
use rayexec_io::{
    location::{AccessConfig, FileLocation},
    FileSource,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::{
    datatable::SingleFileCsvDataTable,
    decoder::{CsvDecoder, DecoderState},
    reader::{CsvSchema, DialectOptions},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadCsv;

impl TableFunction for ReadCsv {
    fn name(&self) -> &'static str {
        "read_csv"
    }

    fn aliases(&self) -> &'static [&'static str] {
        &["csv_scan"]
    }

    fn plan_and_initialize<'a>(
        &'a self,
        runtime: &'a Arc<dyn ExecutionRuntime>,
        args: TableFunctionArgs,
    ) -> BoxFuture<'a, Result<Box<dyn PlannedTableFunction>>> {
        Box::pin(ReadCsvImpl::initialize(runtime.as_ref(), args))
    }

    fn state_deserialize(
        &self,
        deserializer: &mut dyn erased_serde::Deserializer,
    ) -> Result<Box<dyn PlannedTableFunction>> {
        Ok(Box::new(ReadCsvImpl::deserialize(deserializer)?))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ReadCsvImpl {
    location: FileLocation,
    conf: AccessConfig,
    csv_schema: CsvSchema,
    dialect: DialectOptions,
}

impl ReadCsvImpl {
    async fn initialize(
        runtime: &dyn ExecutionRuntime,
        args: TableFunctionArgs,
    ) -> Result<Box<dyn PlannedTableFunction>> {
        let (location, conf) = args.try_location_and_access_config()?;

        let mut source = runtime
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
            location,
            conf,
            dialect,
            csv_schema,
        }))
    }
}

impl PlannedTableFunction for ReadCsvImpl {
    fn serializable_state(&self) -> &dyn erased_serde::Serialize {
        self
    }

    fn table_function(&self) -> &dyn TableFunction {
        &ReadCsv
    }

    fn schema(&self) -> Schema {
        Schema {
            fields: self.csv_schema.fields.clone(),
        }
    }

    fn datatable(&self, runtime: &Arc<dyn ExecutionRuntime>) -> Result<Box<dyn DataTable>> {
        Ok(Box::new(SingleFileCsvDataTable {
            options: self.dialect,
            csv_schema: self.csv_schema.clone(),
            location: self.location.clone(),
            conf: self.conf.clone(),
            runtime: runtime.clone(),
        }))
    }
}

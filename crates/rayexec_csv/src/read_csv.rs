use futures::{future::BoxFuture, StreamExt};
use rayexec_bullet::field::Schema;
use rayexec_error::{not_implemented, RayexecError, Result};
use rayexec_execution::{
    database::table::DataTable,
    functions::table::{
        check_named_args_is_empty, GenericTableFunction, InitializedTableFunction,
        SpecializedTableFunction, TableFunctionArgs,
    },
    runtime::ExecutionRuntime,
};
use rayexec_io::{filesystem::FileSystemProvider, AsyncReader};
use std::{path::PathBuf, sync::Arc};
use url::Url;

use crate::{
    datatable::{ReaderBuilder, SingleFileCsvDataTable},
    decoder::{CsvDecoder, DecoderState},
    reader::{CsvSchema, DialectOptions},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadCsv;

impl GenericTableFunction for ReadCsv {
    fn name(&self) -> &'static str {
        "read_csv"
    }

    fn aliases(&self) -> &'static [&'static str] {
        &["csv_scan"]
    }

    fn specialize(&self, mut args: TableFunctionArgs) -> Result<Box<dyn SpecializedTableFunction>> {
        check_named_args_is_empty(self, &args)?;
        if args.positional.len() != 1 {
            return Err(RayexecError::new("Expected one argument"));
        }

        // TODO: Glob, dispatch to object storage/http impls

        let path = args.positional.pop().unwrap().try_into_string()?;

        match Url::parse(&path) {
            Ok(_) => not_implemented!("remote csv"),
            Err(_) => {
                // Assume file.
                Ok(Box::new(ReadCsvLocal {
                    path: PathBuf::from(path),
                }))
            }
        }
    }
}

#[derive(Debug, Clone)]
struct ReadCsvLocal {
    path: PathBuf,
}

impl SpecializedTableFunction for ReadCsvLocal {
    fn name(&self) -> &'static str {
        "read_csv_local"
    }

    fn initialize(
        self: Box<Self>,
        runtime: &Arc<dyn ExecutionRuntime>,
    ) -> BoxFuture<Result<Box<dyn InitializedTableFunction>>> {
        Box::pin(async move { self.initialize_inner(runtime.as_ref()).await })
    }
}

impl ReadCsvLocal {
    async fn initialize_inner(
        self,
        runtime: &dyn ExecutionRuntime,
    ) -> Result<Box<dyn InitializedTableFunction>> {
        let fs = runtime.filesystem()?;
        let mut file = fs.reader(&self.path)?;

        let mut stream = file.read_stream();
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

        let options = DialectOptions::infer_from_sample(&infer_buf)?;
        let mut decoder = CsvDecoder::new(options);
        let mut state = DecoderState::default();
        let _ = decoder.decode(&infer_buf, &mut state)?;
        let completed = state.completed_records();
        let schema = CsvSchema::infer_from_records(completed)?;

        Ok(Box::new(InitializedLocalCsvFunction {
            specialized: self,
            options,
            csv_schema: schema,
        }))
    }
}

#[derive(Debug)]
struct FilesystemReaderBuilder {
    filesystem: Arc<dyn FileSystemProvider>,
    path: PathBuf,
}

impl ReaderBuilder for FilesystemReaderBuilder {
    fn new_reader(&self) -> Result<Box<dyn AsyncReader>> {
        let file = self.filesystem.reader(&self.path)?;
        Ok(Box::new(file))
    }
}

#[derive(Debug, Clone)]
struct InitializedLocalCsvFunction {
    specialized: ReadCsvLocal,
    options: DialectOptions,
    csv_schema: CsvSchema,
}

impl InitializedTableFunction for InitializedLocalCsvFunction {
    fn specialized(&self) -> &dyn SpecializedTableFunction {
        &self.specialized
    }

    fn schema(&self) -> Schema {
        Schema {
            fields: self.csv_schema.fields.clone(),
        }
    }

    fn datatable(&self, runtime: &Arc<dyn ExecutionRuntime>) -> Result<Box<dyn DataTable>> {
        Ok(Box::new(SingleFileCsvDataTable {
            options: self.options,
            csv_schema: self.csv_schema.clone(),
            reader_builder: Box::new(FilesystemReaderBuilder {
                filesystem: runtime.filesystem()?,
                path: self.specialized.path.clone(),
            }),
        }))
    }
}

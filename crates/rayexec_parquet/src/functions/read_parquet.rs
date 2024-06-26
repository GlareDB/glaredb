use rayexec_error::{RayexecError, Result};
use rayexec_execution::functions::table::{
    check_named_args_is_empty, GenericTableFunction, SpecializedTableFunction, TableFunctionArgs,
};
use std::path::PathBuf;
use url::Url;

use super::{read_parquet_http::ReadParquetHttp, read_parquet_local::ReadParquetLocal};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadParquet;

impl GenericTableFunction for ReadParquet {
    fn name(&self) -> &'static str {
        "read_parquet"
    }

    fn aliases(&self) -> &'static [&'static str] {
        &["parquet_scan"]
    }

    fn specialize(&self, mut args: TableFunctionArgs) -> Result<Box<dyn SpecializedTableFunction>> {
        check_named_args_is_empty(self, &args)?;
        if args.positional.len() != 1 {
            return Err(RayexecError::new("Expected one argument"));
        }

        // TODO: Glob, dispatch to object storage/http impls

        let path = args.positional.pop().unwrap().try_into_string()?;

        match Url::parse(&path) {
            Ok(url) => match url.scheme() {
                "http" | "https" => Ok(Box::new(ReadParquetHttp { url })),
                "file" => Ok(Box::new(ReadParquetLocal {
                    path: PathBuf::from(path),
                })),
                other => Err(RayexecError::new(format!("Unrecognized scheme: '{other}'"))),
            },
            Err(_) => {
                // Assume file.
                Ok(Box::new(ReadParquetLocal {
                    path: PathBuf::from(path),
                }))
            }
        }
    }
}

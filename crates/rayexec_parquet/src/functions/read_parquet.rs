use rayexec_error::{RayexecError, Result};
use rayexec_execution::functions::table::{
    check_named_args_is_empty, GenericTableFunction, SpecializedTableFunction, TableFunctionArgs,
};
use std::path::PathBuf;

use super::read_parquet_local::ReadParquetLocal;

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

        let path = PathBuf::from(args.positional.pop().unwrap().try_into_string()?);

        Ok(Box::new(ReadParquetLocal { path }))
    }
}

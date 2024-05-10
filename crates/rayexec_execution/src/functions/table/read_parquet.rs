use super::{BoundTableFunctionOld, TableFunctionArgs, TableFunctionOld};
use rayexec_error::{RayexecError, Result};

#[derive(Debug, Clone, Copy)]
pub struct ReadParquet;

impl TableFunctionOld for ReadParquet {
    fn name(&self) -> &str {
        "read_parquet"
    }

    fn bind(&self, args: TableFunctionArgs) -> Result<Box<dyn BoundTableFunctionOld>> {
        unimplemented!()
    }
}

struct ReadParquetLocal {
    path: String, // For explain
}

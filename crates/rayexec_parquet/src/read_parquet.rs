use rayexec_error::Result;
use rayexec_execution::functions::table::{
    GenericTableFunction, SpecializedTableFunction, TableFunctionArgs,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadParquet;

impl GenericTableFunction for ReadParquet {
    fn name(&self) -> &'static str {
        "read_parquet"
    }

    fn aliases(&self) -> &'static [&'static str] {
        &["parquet_scan"]
    }

    fn specialize(&self, _args: &TableFunctionArgs) -> Result<Box<dyn SpecializedTableFunction>> {
        unimplemented!()
    }
}

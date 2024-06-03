use rayexec_error::Result;

use super::{GenericTableFunction, SpecializedTableFunction, TableFunctionArgs};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GenerateSeries;

impl GenericTableFunction for GenerateSeries {
    fn name(&self) -> &str {
        "generate_series"
    }

    fn specialize(&self, _args: &TableFunctionArgs) -> Result<Box<dyn SpecializedTableFunction>> {
        unimplemented!()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Range {
    pub start: i64,
    pub stop: i64,
    pub step: i64,
}

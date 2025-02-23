use std::fmt::Debug;

use rayexec_error::{not_implemented, Result};

use crate::arrays::datatype::DataTypeId;
use crate::expr::Expression;
use crate::functions::scalar::{PlannedScalarFunction2, ScalarFunction2};
use crate::functions::{FunctionInfo, Signature};
use crate::logical::binder::table_list::TableList;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StructPack;

impl FunctionInfo for StructPack {
    fn name(&self) -> &'static str {
        "struct_pack"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            positional_args: &[DataTypeId::Struct],
            variadic_arg: None,
            return_type: DataTypeId::Struct,
            doc: None,
        }]
    }
}

impl ScalarFunction2 for StructPack {
    fn plan(
        &self,
        _table_list: &TableList,
        _inputs: Vec<Expression>,
    ) -> Result<PlannedScalarFunction2> {
        not_implemented!("struct pack")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StructExtract;

impl FunctionInfo for StructExtract {
    fn name(&self) -> &'static str {
        "struct_extract"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            positional_args: &[DataTypeId::Struct],
            variadic_arg: None,
            return_type: DataTypeId::Any,
            doc: None,
        }]
    }
}

impl ScalarFunction2 for StructExtract {
    fn plan(
        &self,
        _table_list: &TableList,
        _inputs: Vec<Expression>,
    ) -> Result<PlannedScalarFunction2> {
        not_implemented!("struct extract")
    }
}

use std::fmt::Debug;

use rayexec_bullet::datatype::DataTypeId;
use rayexec_error::{not_implemented, Result};

use crate::expr::Expression;
use crate::functions::scalar::{PlannedScalarFunction, ScalarFunction};
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
        }]
    }
}

impl ScalarFunction for StructPack {
    fn plan(
        &self,
        _table_list: &TableList,
        _inputs: Vec<Expression>,
    ) -> Result<PlannedScalarFunction> {
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
        }]
    }
}

impl ScalarFunction for StructExtract {
    fn plan(
        &self,
        _table_list: &TableList,
        _inputs: Vec<Expression>,
    ) -> Result<PlannedScalarFunction> {
        not_implemented!("struct extract")
    }
}

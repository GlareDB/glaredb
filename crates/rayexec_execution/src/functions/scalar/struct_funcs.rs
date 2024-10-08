use std::fmt::Debug;

use rayexec_bullet::array::Array;
use rayexec_bullet::datatype::{DataType, DataTypeId};
use rayexec_error::Result;

use super::{PlannedScalarFunction, ScalarFunction};
use crate::functions::{FunctionInfo, Signature};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StructPack;

impl FunctionInfo for StructPack {
    fn name(&self) -> &'static str {
        "struct_pack"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            input: &[DataTypeId::Struct],
            variadic: None,
            return_type: DataTypeId::Struct,
        }]
    }
}

impl ScalarFunction for StructPack {
    fn decode_state(&self, _state: &[u8]) -> Result<Box<dyn PlannedScalarFunction>> {
        unimplemented!()
    }

    fn plan_from_datatypes(&self, _inputs: &[DataType]) -> Result<Box<dyn PlannedScalarFunction>> {
        Ok(Box::new(StructPackDynamic))
    }
}

/// Creates a struct array from some input arrays.
///
/// Key and values arrays are alternating.
///
/// It's assumed key arrays are string arrays containing all of the same value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StructPackDynamic;

impl PlannedScalarFunction for StructPackDynamic {
    fn scalar_function(&self) -> &dyn ScalarFunction {
        &StructPack
    }

    fn encode_state(&self, _state: &mut Vec<u8>) -> Result<()> {
        unimplemented!()
    }

    fn return_type(&self) -> DataType {
        unimplemented!()
    }

    fn execute(&self, _inputs: &[&Array]) -> Result<Array> {
        unimplemented!()
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
            input: &[DataTypeId::Struct],
            variadic: None,
            return_type: DataTypeId::Any,
        }]
    }
}

impl ScalarFunction for StructExtract {
    fn decode_state(&self, _state: &[u8]) -> Result<Box<dyn PlannedScalarFunction>> {
        unimplemented!()
    }

    fn plan_from_datatypes(&self, _inputs: &[DataType]) -> Result<Box<dyn PlannedScalarFunction>> {
        unimplemented!()
    }
}
